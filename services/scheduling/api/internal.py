"""Scheduling's internal appointment lookup for Visit (plan 02-10, D-06, D-15).

    GET /internal/appointments/{appointment_id}?clinic_id= -> AppointmentRef

Visit calls this at check-in to learn whether the appointment is `confirmed`
and whose it is (patient, provider). Design rules (T-02-53):
  * Not routed by the gateway (no `/internal` prefix in its table) and excluded
    from OpenAPI (`include_in_schema=False`).
  * Callers: a Visit SERVICE token whose `aud` is `scheduling`, or ADMIN /
    FRONT_DESK user tokens (`require_internal_caller`). Any other service is
    403; a token for another audience, or none, is 401.
  * `clinic_id` is REQUIRED (422 without it): the caller states the clinic it
    acts for and Scheduling re-applies it. A missing appointment, one in
    another clinic, and (for user callers) one outside the caller's own
    clinic all get the IDENTICAL 404.
  * Ids, slot times and status only (`AppointmentRef`): never the coverage
    reference. Reads are not audited; the log line carries ids and outcome.

Plan 02.1-01 (M1) adds the two state transitions Visit drives:

    POST /internal/appointments/{appointment_id}/check-in?clinic_id=
    POST /internal/appointments/{appointment_id}/complete?clinic_id=

Visit SERVICE tokens with `aud=scheduling` ONLY: no user role is allowed, so
every user token (admin included) is 403, another service is 403 and a token
for another audience is 401. Staff reach check-in through Visit's public
route, which owns the visit record. Same clinic rule and identical 404 as the
lookup. Both are idempotent (a repeat answers 200 with the current status and
writes nothing) and 409 when the appointment is in the wrong state; a change
that applied writes one audit entry (field names only, dedupe key per
appointment and action).
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.db.mongo import get_mongo
from libs.observability.logging import get_logger
from services.scheduling.config import settings
from services.scheduling.db import get_db
from services.scheduling.domain.schemas import AppointmentRef
from services.scheduling.repository import appointment_repo

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _ref(appointment) -> AppointmentRef:
    return AppointmentRef(
        id=appointment.id,
        clinic_id=appointment.clinic_id,
        patient_id=appointment.patient_id,
        provider_id=appointment.provider_id,
        slot_id=appointment.slot_id,
        slot_start=appointment.slot_start,
        slot_end=appointment.slot_end,
        status=appointment.status,
    )


@router.get("/appointments/{appointment_id}", response_model=AppointmentRef)
async def lookup_appointment(
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID = Query(...),
    caller: InternalCaller = Depends(
        require_internal_caller(
            Role.ADMIN,
            Role.FRONT_DESK,
            audience="scheduling",
            allowed_services=frozenset({"visit"}),
        )
    ),
    db: AsyncSession = Depends(get_db),
) -> AppointmentRef:
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if (
        appointment is None
        or appointment.clinic_id != clinic_id
        or not clinic_scope_permits(caller, appointment.clinic_id)
    ):
        log.info(
            "internal.appointment_lookup", appointment_id=str(appointment_id), outcome="not_found"
        )
        raise _not_found()

    log.info(
        "internal.appointment_lookup",
        appointment_id=str(appointment.id),
        outcome="found",
        status=appointment.status,
    )
    return _ref(appointment)


# --- plan 02.1-01 (M1): state transitions driven by Visit ------------------------

_VISIT_ONLY = require_internal_caller(audience="scheduling", allowed_services=frozenset({"visit"}))

CHECK_IN_FIELDS = ["status"]


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


async def _scoped_for_transition(
    db: AsyncSession, appointment_id: uuid.UUID, clinic_id: uuid.UUID, caller: InternalCaller
):
    """The appointment, or the identical 404 when it is missing, in another
    clinic, or outside the caller's clinic scope (same rule as the lookup)."""
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if (
        appointment is None
        or appointment.clinic_id != clinic_id
        or not clinic_scope_permits(caller, appointment.clinic_id)
    ):
        raise _not_found()
    return appointment


async def _audit_transition(
    mongo: AsyncMongoClient, caller: InternalCaller, appointment, *, action: str, step: str
) -> None:
    await write_audit(
        mongo,
        actor_id=caller.sub,
        actor_role="service",
        action=action,
        entity_type="appointment",
        entity_id=str(appointment.id),
        clinic_id=str(appointment.clinic_id),
        outcome="success",
        changed_fields=CHECK_IN_FIELDS,
        dedupe_key=f"appointment:{appointment.id}:{step}",
    )


@router.post("/appointments/{appointment_id}/check-in", response_model=AppointmentRef)
async def check_in_appointment(
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID = Query(...),
    caller: InternalCaller = Depends(_VISIT_ONLY),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> AppointmentRef:
    """`confirmed -> checked_in` inside the check-in window (DB clock)."""
    appointment = await _scoped_for_transition(db, appointment_id, clinic_id, caller)

    outcome = await appointment_repo.check_in_appointment(
        db,
        appointment_id=appointment_id,
        actor=caller.sub,
        opens_before_minutes=settings.checkin_opens_before_minutes,
        closes_after_end_minutes=settings.checkin_closes_after_end_minutes,
    )
    log.info("internal.appointment_check_in", appointment_id=str(appointment_id), outcome=outcome)
    if outcome == "not_found":  # pragma: no cover - read just above; never deleted
        await db.rollback()
        raise _not_found()
    if outcome == "not_confirmed":
        await db.rollback()
        raise _conflict("appointment not confirmed")
    if outcome == "too_early":
        await db.rollback()
        raise _conflict("check-in window not open")
    if outcome == "too_late":
        await db.rollback()
        raise _conflict("check-in window closed")
    if outcome == "already":
        await db.rollback()
        await db.refresh(appointment)
        return _ref(appointment)

    await db.commit()
    await db.refresh(appointment)
    await _audit_transition(mongo, caller, appointment, action="appointment.check_in", step="check_in")
    return _ref(appointment)
