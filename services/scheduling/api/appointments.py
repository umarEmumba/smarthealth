"""Booking routes (plan 02-06; SCHED-01, SCHED-02, SCHED-04; D-07, D-08, D-18).

    POST /api/appointments                   PATIENT, FRONT_DESK, ADMIN   202 Accepted
    GET  /api/appointments/{appointment_id}  PATIENT (own), FRONT_DESK (own clinic), ADMIN
    GET  /api/appointments                   same scope rules, with filters
                                             (`?flagged=true`: schedule-change flagged, M2)
    POST /api/appointments/{id}/cancel       same scope rules   202 / 200 no-op  (plan 02-08)
    POST /api/appointments/{id}/reschedule   same scope rules   202              (plan 02-08)

Booking is asynchronous (D-07). The POST does everything that must be atomic in
ONE local transaction (`appointment_repo.accept_booking`: the idempotency claim,
then the `pending` appointment, arbitrated by Postgres), commits, and only then
starts the Temporal booking saga as `booking-{appointment_id}` on
`scheduling-tq` (`libs.temporal.start_once`). It answers 202 with the
appointment id, its current status and a status URL (also the `Location`
header); the appointment becomes `confirmed` (or `failed` with a reason) when
the saga finishes.

Idempotency (D-08, SCHED-04): `Idempotency-Key` is a required header
(1-128 chars of `[A-Za-z0-9._:-]`, else 400). The canonical body hash
(`slot_id`, resolved `patient_id`, `coverage_ref`) decides: the same key with the
same body replays the same appointment id (with its CURRENT status); the same
key with another body is 422. Both are answered BEFORE any slot check, so a
replay of a booking whose slot is now taken still replays. A different key for
a taken slot is 409 ("slot not available"), and the same patient in an
overlapping window is 409 too.
A slot whose start time has passed is refused with 422 (plan 02.1-07 S6),
answered after the scope check and before the status check, with nothing
written.

If Temporal is unreachable when starting, the booking is still accepted (202,
`pending`, `workflow_started_at` NULL) and `booking.workflow_start_deferred` is
logged; the starter sweeper (plan 02-07) starts it later (research A8,
Pitfall 4). The start happens inside the request span, so the API request, the
workflow and its activities form one trace (D-18).

Scope (D-18, T-02-28): a PATIENT books only for the patient profile linked to
their own login in their clinic (resolved through Patient's by-user lookup;
any other `patient_id` is 403). FRONT_DESK acts only in their clinic: a slot or
appointment elsewhere gets the same 404 as a missing one. ADMIN acts anywhere.

PHI: `coverage_ref` is stored for the billing activity only; it is never
logged, audited, returned or put in workflow input. Audit records field names.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from libs.auth.service_tokens import ServiceTokenProvider, ServiceTokenUnavailable
from libs.db.mongo import get_mongo
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger
from libs.service_client import UpstreamServiceUnavailable
from libs.temporal import start_once
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling import references
from services.scheduling.api.deps import get_service_tokens, get_temporal_client
from services.scheduling.db import get_db
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    BOOKING_WORKFLOW,
    CANCEL_WORKFLOW,
    NO_REFUND,
    REFUND,
    RESCHEDULE_WORKFLOW,
    BookingInput,
    CancelInput,
    RescheduleInput,
    booking_workflow_id,
    cancel_workflow_id,
    reschedule_workflow_id,
)
from services.scheduling.domain.enums import AppointmentStatus, SlotStatus
from services.scheduling.domain.errors import IdempotencyKeyReused, PatientOverlap, SlotTaken
from services.scheduling.domain.schemas import (
    AppointmentAccepted,
    AppointmentHistoryOut,
    AppointmentOut,
    BookingRequest,
    CancelRequest,
    ChangeAccepted,
    RescheduleRequest,
)
from services.scheduling.repository import appointment_repo, slot_repo
from services.scheduling.repository.idempotency_repo import get_idempotency_record
from services.scheduling.repository.models import Appointment

router = APIRouter(prefix="/api/appointments", tags=["appointments"])

log = get_logger(__name__)

BOOKING_ROLES = (Role.PATIENT, Role.FRONT_DESK, Role.ADMIN)
IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
CREATE_FIELDS = ["slot_id", "patient_id", "provider_id", "status", "coverage_ref"]
# Upper bound on starting the saga inside the request (the gateway's upstream
# timeout is 10 s); past it the start is deferred to the sweeper.
START_TIMEOUT_SECONDS = 5.0
LIST_LIMIT_MAX = 200


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _slot_conflict() -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail="slot not available")


def _user_clinic(user: AuthenticatedUser) -> uuid.UUID | None:
    if user.clinic_id is None:
        return None
    try:
        return uuid.UUID(user.clinic_id)
    except ValueError:
        return None


def _in_scope(user: AuthenticatedUser, clinic_id: uuid.UUID) -> bool:
    """D-18: ADMIN anywhere; everyone else only in their own clinic."""
    return user.role == Role.ADMIN or _user_clinic(user) == clinic_id


def request_hash(slot_id: uuid.UUID, patient_id: uuid.UUID, coverage_ref: str | None) -> str:
    """SHA-256 of the canonical booking body (D-08). Only the hash is stored."""
    canonical = json.dumps(
        {"slot_id": str(slot_id), "patient_id": str(patient_id), "coverage_ref": coverage_ref},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _own_patient_id(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, user: AuthenticatedUser
) -> uuid.UUID | None:
    """The patient profile linked to a PATIENT's login in their clinic."""
    clinic_id = _user_clinic(user)
    if clinic_id is None:
        return None
    try:
        user_id = uuid.UUID(user.sub)
    except ValueError:
        return None
    try:
        patient = await references.get_patient_by_user(http, tokens, user_id, clinic_id)
    except UpstreamServiceUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"{exc.service} service unavailable") from exc
    except ServiceTokenUnavailable as exc:
        raise HTTPException(status_code=503, detail="identity service unavailable") from exc
    if patient is None or not patient.get("is_active", False):
        return None
    return uuid.UUID(str(patient["id"]))


def _accepted(response: Response, appointment_id: uuid.UUID, current_status: str) -> AppointmentAccepted:
    status_url = f"/api/appointments/{appointment_id}"
    response.headers["Location"] = status_url
    return AppointmentAccepted(
        appointment_id=appointment_id, status=current_status, status_url=status_url
    )


async def _current_status(db: AsyncSession, appointment_id: uuid.UUID) -> str:
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if appointment is None:  # pragma: no cover - the key row references a committed appointment
        return AppointmentStatus.PENDING
    await db.refresh(appointment)
    return appointment.status


async def _start_saga(
    db: AsyncSession, temporal, booking: BookingInput, appointment_id: uuid.UUID
) -> None:
    """Start `booking-{id}` exactly once, then set `workflow_started_at`. Any
    Temporal failure is logged and deferred to the starter sweeper."""
    try:
        await asyncio.wait_for(
            start_once(
                temporal,
                BOOKING_WORKFLOW,
                booking,
                workflow_id=booking_workflow_id(str(appointment_id)),
                task_queue=SCHEDULING_TASK_QUEUE,
            ),
            timeout=START_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 - accepted anyway; the sweeper recovers (A8)
        log.warning(
            "booking.workflow_start_deferred",
            appointment_id=str(appointment_id),
            error=type(exc).__name__,
        )
        return
    await appointment_repo.mark_workflow_started(db, appointment_id=appointment_id)
    await db.commit()
    log.info("booking.workflow_started", appointment_id=str(appointment_id))


@router.post("", response_model=AppointmentAccepted, status_code=202)
async def create_appointment(
    payload: BookingRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    user: AuthenticatedUser = Depends(require_role(*BOOKING_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
    temporal=Depends(get_temporal_client),
) -> AppointmentAccepted:
    if idempotency_key is None or not IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key header required (1-128 chars of A-Z a-z 0-9 . _ : -)",
        )
    caller_id = uuid.UUID(user.sub)

    # Who is being booked (T-02-28).
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or (payload.patient_id is not None and payload.patient_id != own):
            log.info("booking.refused", reason="not_own_patient")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="patients may only book for themselves",
            )
        patient_id = own
    else:
        if payload.patient_id is None:
            raise HTTPException(
                status_code=422, detail="patient_id is required"
            )
        patient_id = payload.patient_id

    body_hash = request_hash(payload.slot_id, patient_id, payload.coverage_ref)

    # Replay / key reuse is answered BEFORE any slot check (D-08).
    existing = await get_idempotency_record(
        db, caller_user_id=caller_id, key=idempotency_key, live_only=True
    )
    if existing is not None:
        stored_hash, stored_id = existing
        if stored_hash != body_hash:
            log.info("booking.idempotency_mismatch")
            raise HTTPException(
                status_code=422,
                detail="Idempotency-Key was already used with a different request body",
            )
        current = await _current_status(db, stored_id)
        log.info("booking.replayed", appointment_id=str(stored_id), status=current)
        return _accepted(response, stored_id, current)

    slot = await slot_repo.get_slot(db, slot_id=payload.slot_id)
    if slot is None or not _in_scope(user, slot.clinic_id):
        raise _not_found()
    if slot.start_time <= datetime.now(timezone.utc):
        # S6 (plan 02.1-07): a slot that has already started cannot be booked.
        # Nothing has been written yet (only the idempotency read above), and
        # this precedes the status check so a past AND booked slot is 422, not
        # 409. The saga's validate step repeats the check with the database
        # clock for a slot that starts while the booking is pending.
        log.info("booking.refused", slot_id=str(payload.slot_id), reason="slot_in_past")
        raise HTTPException(status_code=422, detail="slot has already started")
    if slot.status != SlotStatus.AVAILABLE.value:
        # Early answer only; the database arbiters below are the real guard.
        raise _slot_conflict()
    clinic_id, provider_id = slot.clinic_id, slot.provider_id
    slot_start, slot_end = slot.start_time, slot.end_time

    appointment_id = uuid.uuid4()
    try:
        accepted = await appointment_repo.accept_booking(
            db,
            appointment_id=appointment_id,
            caller_user_id=caller_id,
            key=idempotency_key,
            request_hash=body_hash,
            clinic_id=clinic_id,
            patient_id=patient_id,
            provider_id=provider_id,
            slot_id=payload.slot_id,
            slot_start=slot_start,
            slot_end=slot_end,
            coverage_ref=payload.coverage_ref,
            created_by=caller_id,
        )
    except SlotTaken as exc:
        await db.rollback()
        log.info("booking.refused", slot_id=str(payload.slot_id), reason="slot_taken")
        raise _slot_conflict() from exc
    except PatientOverlap as exc:
        await db.rollback()
        log.info("booking.refused", slot_id=str(payload.slot_id), reason="patient_overlap")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="patient has an overlapping appointment",
        ) from exc
    except IdempotencyKeyReused as exc:
        await db.rollback()
        log.info("booking.idempotency_mismatch")
        raise HTTPException(
            status_code=422,
            detail="Idempotency-Key was already used with a different request body",
        ) from exc
    except BaseException:
        await db.rollback()
        raise

    if accepted.replayed:
        # A concurrent request with the same key and body won the race.
        await db.rollback()
        current = await _current_status(db, accepted.appointment_id)
        log.info("booking.replayed", appointment_id=str(accepted.appointment_id), status=current)
        return _accepted(response, accepted.appointment_id, current)

    await db.commit()
    log.info(
        "booking.accepted",
        appointment_id=str(appointment_id),
        slot_id=str(payload.slot_id),
        clinic_id=str(clinic_id),
    )
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="appointment.create",
        entity_type="appointment",
        entity_id=str(appointment_id),
        clinic_id=str(clinic_id),
        outcome="success",
        changed_fields=CREATE_FIELDS,
    )

    await _start_saga(
        db,
        temporal,
        BookingInput(
            appointment_id=str(appointment_id),
            clinic_id=str(clinic_id),
            patient_id=str(patient_id),
            provider_id=str(provider_id),
            slot_id=str(payload.slot_id),
        ),
        appointment_id,
    )
    return _accepted(response, appointment_id, AppointmentStatus.PENDING)


def _to_out(appointment: Appointment, history: list | None = None) -> AppointmentOut:
    return AppointmentOut(
        id=appointment.id,
        clinic_id=appointment.clinic_id,
        patient_id=appointment.patient_id,
        provider_id=appointment.provider_id,
        slot_id=appointment.slot_id,
        slot_start=appointment.slot_start,
        slot_end=appointment.slot_end,
        status=appointment.status,
        failure_reason=appointment.failure_reason,
        version=appointment.version,
        created_at=appointment.created_at,
        updated_at=appointment.updated_at,
        schedule_change_flagged=appointment.schedule_change_flagged_at is not None,
        schedule_change_reason=appointment.schedule_change_reason,
        history=[
            AppointmentHistoryOut(
                from_status=h.from_status,
                to_status=h.to_status,
                reason=h.reason,
                created_at=h.created_at,
            )
            for h in (history or [])
        ],
    )


@router.get("/{appointment_id}", response_model=AppointmentOut)
async def get_appointment(
    appointment_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*BOOKING_ROLES)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> AppointmentOut:
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if appointment is None or not _in_scope(user, appointment.clinic_id):
        raise _not_found()
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or own != appointment.patient_id:
            raise _not_found()
    history = await appointment_repo.get_history(db, appointment_id)
    return _to_out(appointment, history)


@router.get("", response_model=list[AppointmentOut])
async def list_appointments(
    clinic_id: uuid.UUID | None = None,
    patient_id: uuid.UUID | None = None,
    provider_id: uuid.UUID | None = None,
    appointment_status: str | None = Query(default=None, alias="status"),
    start: datetime | None = None,
    end: datetime | None = None,
    # M2 (plan 02.1-08): true = only appointments a schedule change flagged.
    flagged: bool | None = None,
    limit: int = Query(default=100, ge=1, le=LIST_LIMIT_MAX),
    user: AuthenticatedUser = Depends(require_role(*BOOKING_ROLES)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> list[AppointmentOut]:
    if appointment_status is not None and appointment_status not in AppointmentStatus.ALL:
        raise HTTPException(status_code=422, detail="unknown appointment status")
    for bound in (start, end):
        if bound is not None and bound.tzinfo is None:
            raise HTTPException(status_code=422, detail="start and end must include a UTC offset")

    if user.role != Role.ADMIN:
        own_clinic = _user_clinic(user)
        if own_clinic is None:
            return []
        if clinic_id is not None and clinic_id != own_clinic:
            return []
        clinic_id = own_clinic
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or (patient_id is not None and patient_id != own):
            return []
        patient_id = own

    rows = await appointment_repo.list_appointments(
        db,
        clinic_id=clinic_id,
        patient_id=patient_id,
        status=appointment_status,
        provider_id=provider_id,
        start=start,
        end=end,
        limit=limit,
        flagged=flagged,
    )
    return [_to_out(a) for a in rows]


# --- plan 02-08: cancel and reschedule (SCHED-05, SCHED-06; D-13, D-14) --------
#
# Both are Temporal workflows started under `{op}-{id}-{version}`
# (`start_once`): a repeat while one runs attaches to it, a repeat after it
# closed is a no-op. Time-dependent policy (started? inside the refund
# cutoff?) is decided HERE, in the request, so workflow history stays
# deterministic (research A11). A `pending` appointment is refused with 409
# "booking in progress" (its booking saga still owns it, research Open
# Question 3). Unlike booking, nothing is committed before the start, so an
# unreachable Temporal is a 503 the client may retry.


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def _change_accepted(
    response: Response, appointment_id: uuid.UUID, current_status: str, operation: str
) -> ChangeAccepted:
    status_url = f"/api/appointments/{appointment_id}"
    response.headers["Location"] = status_url
    return ChangeAccepted(
        appointment_id=appointment_id,
        status=current_status,
        status_url=status_url,
        operation=operation,
    )


async def _scoped_appointment(
    db: AsyncSession,
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    user: AuthenticatedUser,
    appointment_id: uuid.UUID,
) -> Appointment:
    """The appointment if the caller may act on it, else the same 404 as a
    missing one (T-02-40): PATIENT own only, staff own clinic, ADMIN anywhere."""
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if appointment is None or not _in_scope(user, appointment.clinic_id):
        raise _not_found()
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or own != appointment.patient_id:
            raise _not_found()
    return appointment


# M1 (plan 02.1-01): once the patient has checked in, or the visit is over, the
# appointment can no longer be cancelled or rescheduled (it would free a slot
# that was used and trigger a refund). Named so the client can tell why.
_REFUSED_BY_STATE = {
    AppointmentStatus.CHECKED_IN: "appointment already checked in",
    AppointmentStatus.COMPLETED: "appointment already completed",
    AppointmentStatus.NO_SHOW: "appointment was a no-show",
}


def _refuse_unchangeable(appointment: Appointment, now: datetime, operation: str) -> None:
    if appointment.status == AppointmentStatus.PENDING:
        raise _conflict("booking in progress")
    if appointment.status in _REFUSED_BY_STATE:
        raise _conflict(_REFUSED_BY_STATE[appointment.status])
    if appointment.status != AppointmentStatus.CONFIRMED:
        raise _conflict(f"appointment not {'cancellable' if operation == 'cancel' else 'reschedulable'}")
    if appointment.slot_start <= now:
        raise _conflict("appointment already started")


async def _start_change(temporal, workflow: str, arg, *, workflow_id: str) -> None:
    try:
        await asyncio.wait_for(
            start_once(temporal, workflow, arg, workflow_id=workflow_id, task_queue=SCHEDULING_TASK_QUEUE),
            timeout=START_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001 - nothing committed yet; the client retries
        log.warning("appointment.change_start_failed", workflow=workflow, error=type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="workflow service unavailable"
        ) from exc


@router.post("/{appointment_id}/cancel", response_model=ChangeAccepted, status_code=202)
async def cancel_appointment(
    appointment_id: uuid.UUID,
    response: Response,
    payload: CancelRequest | None = None,
    user: AuthenticatedUser = Depends(require_role(*BOOKING_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
    temporal=Depends(get_temporal_client),
) -> ChangeAccepted:
    payload = payload or CancelRequest()
    appointment = await _scoped_appointment(db, http, tokens, user, appointment_id)
    if payload.waive_cutoff and user.role == Role.PATIENT:
        # T-02-39: only staff may waive the refund cutoff.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="only staff may waive the cancel cutoff"
        )
    if appointment.status == AppointmentStatus.CANCELLED:
        response.status_code = status.HTTP_200_OK
        log.info("appointment.cancel_noop", appointment_id=str(appointment_id))
        return _change_accepted(response, appointment_id, AppointmentStatus.CANCELLED, "cancel")

    now = datetime.now(timezone.utc)
    _refuse_unchangeable(appointment, now, "cancel")
    before_cutoff = appointment.slot_start - now >= timedelta(hours=settings.cancel_cutoff_hours)
    refund_policy = REFUND if (payload.waive_cutoff or before_cutoff) else NO_REFUND
    clinic_id, slot_id, version = appointment.clinic_id, appointment.slot_id, appointment.version

    await _start_change(
        temporal,
        CANCEL_WORKFLOW,
        CancelInput(
            appointment_id=str(appointment_id),
            clinic_id=str(clinic_id),
            slot_id=str(slot_id),
            refund_policy=refund_policy,
            actor=f"user:{user.sub}",
        ),
        workflow_id=cancel_workflow_id(str(appointment_id), version),
    )
    log.info(
        "appointment.cancel_requested",
        appointment_id=str(appointment_id),
        refund_policy=refund_policy,
        waived=payload.waive_cutoff,
    )
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="appointment.cancel_requested",
        entity_type="appointment",
        entity_id=str(appointment_id),
        clinic_id=str(clinic_id),
        outcome="success",
        changed_fields=["status"],
    )
    return _change_accepted(response, appointment_id, appointment.status, "cancel")


@router.post("/{appointment_id}/reschedule", response_model=ChangeAccepted, status_code=202)
async def reschedule_appointment(
    appointment_id: uuid.UUID,
    payload: RescheduleRequest,
    response: Response,
    user: AuthenticatedUser = Depends(require_role(*BOOKING_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
    temporal=Depends(get_temporal_client),
) -> ChangeAccepted:
    appointment = await _scoped_appointment(db, http, tokens, user, appointment_id)
    now = datetime.now(timezone.utc)
    _refuse_unchangeable(appointment, now, "reschedule")

    new_slot = await slot_repo.get_slot(db, slot_id=payload.new_slot_id)
    if new_slot is None or new_slot.clinic_id != appointment.clinic_id:
        raise _not_found()
    if new_slot.provider_id != appointment.provider_id:
        raise HTTPException(status_code=422, detail="reschedule must keep the same provider")
    if new_slot.status != SlotStatus.AVAILABLE.value or new_slot.start_time <= now:
        # Early answer only; the hold and the swap's arbiters are the real guard.
        raise _slot_conflict()

    clinic_id, old_slot_id, version = appointment.clinic_id, appointment.slot_id, appointment.version
    await _start_change(
        temporal,
        RESCHEDULE_WORKFLOW,
        RescheduleInput(
            appointment_id=str(appointment_id),
            clinic_id=str(clinic_id),
            old_slot_id=str(old_slot_id),
            new_slot_id=str(payload.new_slot_id),
            actor=f"user:{user.sub}",
        ),
        workflow_id=reschedule_workflow_id(str(appointment_id), version),
    )
    log.info(
        "appointment.reschedule_requested",
        appointment_id=str(appointment_id),
        new_slot_id=str(payload.new_slot_id),
    )
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="appointment.reschedule_requested",
        entity_type="appointment",
        entity_id=str(appointment_id),
        clinic_id=str(clinic_id),
        outcome="success",
        changed_fields=["slot_id"],
    )
    return _change_accepted(response, appointment_id, appointment.status, "reschedule")
