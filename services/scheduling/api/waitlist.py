"""Waitlist routes (plan 02-09; SCHED-07, D-12, D-18).

    POST /api/waitlist                     PATIENT (self), FRONT_DESK (own clinic), ADMIN   201
    GET  /api/waitlist                     same scope rules, filters provider_id/status/patient_id
    GET  /api/waitlist/{entry_id}          same scope rules
    POST /api/waitlist/{entry_id}/cancel   same scope rules   200 (repeat: 200 no-op)

The PROVIDER role is refused everywhere (403, research RBAC matrix).

An entry names a provider and an optional window (`window_start` /
`window_end`, open ends allowed). When a booked slot of that provider is freed
by a cancel or a reschedule, `WaitlistAdvanceWorkflow` auto-books the OLDEST
waiting entry whose window contains the slot through the normal booking saga
(D-12). One patient holds at most one active (`waiting`/`matched`) entry per
provider: the partial unique index `uq_waitlist_active_patient_provider` is
the arbiter (409).

Scope (D-18, T-02-44): a PATIENT acts only for the patient profile linked to
their own login in their clinic (Patient's by-user lookup; another
`patient_id` is 403). FRONT_DESK acts only in their clinic: a provider, patient
or entry elsewhere gets the same 404 as a missing one. ADMIN acts anywhere.
Reference reads that cannot reach their owner are 503 before any write.

PHI: `coverage_ref` is stored for the auto-booking only; it is never returned,
logged or audited (audit records field names).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pymongo import AsyncMongoClient
from sqlalchemy.exc import IntegrityError
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
from services.scheduling import references
from services.scheduling.api.appointments import _in_scope, _own_patient_id, _user_clinic
from services.scheduling.api.deps import get_service_tokens
from services.scheduling.db import get_db
from services.scheduling.domain.enums import WaitlistStatus
from services.scheduling.domain.schemas import WaitlistCreate, WaitlistEntryOut
from services.scheduling.repository import waitlist_repo
from services.scheduling.repository.models import WaitlistEntry

router = APIRouter(prefix="/api/waitlist", tags=["waitlist"])

log = get_logger(__name__)

WAITLIST_ROLES = (Role.PATIENT, Role.FRONT_DESK, Role.ADMIN)
CREATE_FIELDS = ["provider_id", "patient_id", "window_start", "window_end", "coverage_ref", "status"]
LIST_LIMIT_MAX = 200


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _unavailable(exc: Exception) -> HTTPException:
    if isinstance(exc, UpstreamServiceUnavailable):
        return HTTPException(status_code=503, detail=f"{exc.service} service unavailable")
    return HTTPException(status_code=503, detail="identity service unavailable")


def _to_out(entry: WaitlistEntry) -> WaitlistEntryOut:
    return WaitlistEntryOut(
        id=entry.id,
        clinic_id=entry.clinic_id,
        provider_id=entry.provider_id,
        patient_id=entry.patient_id,
        window_start=entry.window_start,
        window_end=entry.window_end,
        status=entry.status,
        failure_reason=entry.failure_reason,
        matched_appointment_id=entry.matched_appointment_id,
        booked_appointment_id=entry.booked_appointment_id,
        attempts=entry.attempts,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
    )


async def _provider_clinic(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, provider_id: uuid.UUID
) -> uuid.UUID | None:
    """provider -> department -> clinic; None when any link is missing or
    inactive (the caller answers the identical 404)."""
    try:
        provider = await references.get_provider_ref(http, tokens, provider_id)
        if provider is None or not provider.get("is_active", False):
            return None
        department = await references.get_department_ref(
            http, tokens, uuid.UUID(str(provider["department_id"]))
        )
        if department is None or not department.get("is_active", True):
            return None
        clinic_id = uuid.UUID(str(department["clinic_id"]))
        clinic = await references.get_clinic_ref(http, tokens, clinic_id)
    except (UpstreamServiceUnavailable, ServiceTokenUnavailable) as exc:
        raise _unavailable(exc) from exc
    if clinic is None or not clinic.get("is_active", False):
        return None
    return clinic_id


async def _scoped_entry(
    db: AsyncSession,
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    user: AuthenticatedUser,
    entry_id: uuid.UUID,
) -> WaitlistEntry:
    """The entry if the caller may see it, else the same 404 as a missing one."""
    entry = await waitlist_repo.get_entry(db, entry_id)
    if entry is None or not _in_scope(user, entry.clinic_id):
        raise _not_found()
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or own != entry.patient_id:
            raise _not_found()
    return entry


@router.post("", response_model=WaitlistEntryOut, status_code=201)
async def join_waitlist(
    payload: WaitlistCreate,
    user: AuthenticatedUser = Depends(require_role(*WAITLIST_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> WaitlistEntryOut:
    now = datetime.now(timezone.utc)
    if payload.window_end is not None and payload.window_end <= now:
        raise HTTPException(status_code=422, detail="window_end must be in the future")

    # Who is waiting (T-02-44).
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or (payload.patient_id is not None and payload.patient_id != own):
            log.info("waitlist.refused", reason="not_own_patient")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="patients may only join a waitlist for themselves",
            )
        patient_id = own
    else:
        if payload.patient_id is None:
            raise HTTPException(status_code=422, detail="patient_id is required")
        patient_id = payload.patient_id

    clinic_id = await _provider_clinic(http, tokens, payload.provider_id)
    if clinic_id is None or not _in_scope(user, clinic_id):
        raise _not_found()
    if user.role != Role.PATIENT:
        try:
            patient = await references.get_patient_ref(http, tokens, patient_id, clinic_id)
        except (UpstreamServiceUnavailable, ServiceTokenUnavailable) as exc:
            raise _unavailable(exc) from exc
        if patient is None or not patient.get("is_active", False):
            raise _not_found()

    try:
        entry = await waitlist_repo.create_entry(
            db,
            clinic_id=clinic_id,
            provider_id=payload.provider_id,
            patient_id=patient_id,
            window_start=payload.window_start,
            window_end=payload.window_end,
            coverage_ref=payload.coverage_ref,
            created_by=uuid.UUID(user.sub),
        )
    except IntegrityError as exc:
        await db.rollback()
        if waitlist_repo.ACTIVE_ENTRY_CONSTRAINT in str(exc.orig):
            log.info("waitlist.refused", provider_id=str(payload.provider_id), reason="duplicate")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="patient already on this provider's waitlist",
            ) from exc
        raise
    out = _to_out(entry)
    await db.commit()
    log.info("waitlist.created", entry_id=str(out.id), provider_id=str(out.provider_id))
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="waitlist.create",
        entity_type="waitlist_entry",
        entity_id=str(out.id),
        clinic_id=str(clinic_id),
        outcome="success",
        changed_fields=CREATE_FIELDS,
    )
    return out


@router.get("", response_model=list[WaitlistEntryOut])
async def list_waitlist(
    clinic_id: uuid.UUID | None = None,
    provider_id: uuid.UUID | None = None,
    patient_id: uuid.UUID | None = None,
    entry_status: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=LIST_LIMIT_MAX),
    user: AuthenticatedUser = Depends(require_role(*WAITLIST_ROLES)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> list[WaitlistEntryOut]:
    if entry_status is not None and entry_status not in WaitlistStatus.ALL:
        raise HTTPException(status_code=422, detail="unknown waitlist status")
    if user.role != Role.ADMIN:
        own_clinic = _user_clinic(user)
        if own_clinic is None or (clinic_id is not None and clinic_id != own_clinic):
            return []
        clinic_id = own_clinic
    if user.role == Role.PATIENT:
        own = await _own_patient_id(http, tokens, user)
        if own is None or (patient_id is not None and patient_id != own):
            return []
        patient_id = own
    rows = await waitlist_repo.list_entries(
        db,
        clinic_id=clinic_id,
        provider_id=provider_id,
        patient_id=patient_id,
        status=entry_status,
        limit=limit,
    )
    return [_to_out(e) for e in rows]


@router.get("/{entry_id}", response_model=WaitlistEntryOut)
async def get_waitlist_entry(
    entry_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*WAITLIST_ROLES)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> WaitlistEntryOut:
    return _to_out(await _scoped_entry(db, http, tokens, user, entry_id))


@router.post("/{entry_id}/cancel", response_model=WaitlistEntryOut)
async def cancel_waitlist_entry(
    entry_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*WAITLIST_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> WaitlistEntryOut:
    entry = await _scoped_entry(db, http, tokens, user, entry_id)
    clinic_id = entry.clinic_id
    outcome = await waitlist_repo.cancel_entry(db, entry_id=entry_id, actor=f"user:{user.sub}")
    if outcome == "not_cancellable":
        await db.rollback()
        await db.refresh(entry)
        detail = (
            "match in progress"
            if entry.status == WaitlistStatus.MATCHED
            else "waitlist entry not cancellable"
        )
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)
    await db.commit()
    await db.refresh(entry)
    out = _to_out(entry)
    log.info("waitlist.cancel", entry_id=str(entry_id), outcome=outcome)
    if outcome == "cancelled":
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="waitlist.cancel",
            entity_type="waitlist_entry",
            entity_id=str(entry_id),
            clinic_id=str(clinic_id),
            outcome="success",
            changed_fields=["status"],
        )
    return out
