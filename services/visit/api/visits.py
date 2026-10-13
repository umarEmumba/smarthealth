"""Visit's public routes, reached through the gateway (plan 02-10, D-15, D-17, D-18).

    POST /api/visits                     check in a CONFIRMED appointment (front-desk, admin)
    GET  /api/visits/{visit_id}          the visit plus its state history
    POST /api/visits/{visit_id}/start    checked_in -> in_progress
    POST /api/visits/{visit_id}/complete in_progress -> completed

Check-in (VISIT-01, D-15):
  * Front-desk acts in its own clinic (from the token); a body `clinic_id`
    naming another clinic is the identical 404. Admin must name `clinic_id`
    (422 without it).
  * A visit that already exists for the appointment is returned as-is (200),
    without asking Scheduling again: check-in is idempotent.
  * Otherwise Scheduling is asked over an aud=scheduling service token whether
    the appointment exists in that clinic (missing / other clinic -> 404) and
    is `confirmed` (else 409 "appointment not confirmed"). Scheduling or
    Identity unreachable -> 503 before anything is written (fail closed).
  * `INSERT ... ON CONFLICT (appointment_id) DO NOTHING` arbitrates concurrent
    check-ins: one 201 (audited `visit.check_in`), the rest 200.

Transitions (VISIT-02/03, D-15): the visit's provider (resolved from the
caller's login through Provider's by-user lookup, aud=provider), or
front-desk/admin within the clinic. A provider acting on another provider's
visit gets the identical 404 a missing visit gets; Provider unreachable is 503
with nothing changed. `transition_visit` is a conditional UPDATE plus a
history row in one transaction: applied -> commit, then audit
`visit.start` / `visit.complete` LAST; the current or an already-passed target
-> 200 no-op (no history, no audit); a skipped state -> 409. Completion leaves
`downstream_started_at` NULL for plan 02-11.

Clinic scope (D-18): front-desk sees only its own clinic; anything outside it
answers exactly like a missing visit (404 "not found"). Responses carry ids,
statuses and timestamps only. Reads are not audited (existing convention);
audit entries carry field NAMES only and are written after the commit.
"""

from __future__ import annotations

import uuid

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Response, status
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
from services.visit.api.deps import get_service_tokens
from services.visit.db import get_db
from services.visit.domain.schemas import (
    CheckInRequest,
    VisitDetailOut,
    VisitHistoryOut,
    VisitOut,
)
from services.visit.domain.states import PREVIOUS, TransitionOutcome, VisitStatus
from services.visit.references import get_appointment_ref, get_provider_by_user
from services.visit.repository import visit_repo
from services.visit.repository.models import Visit

router = APIRouter(prefix="/api/visits", tags=["visits"])

log = get_logger(__name__)

STAFF_ROLES = (Role.FRONT_DESK, Role.ADMIN)
VISIT_ROLES = (Role.PROVIDER, Role.FRONT_DESK, Role.ADMIN)
ENTITY_TYPE = "visit"
APPOINTMENT_CONFIRMED = "confirmed"


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _user_clinic(user: AuthenticatedUser) -> uuid.UUID | None:
    if user.clinic_id is None:
        return None
    try:
        return uuid.UUID(user.clinic_id)
    except ValueError:
        return None


def _staff_in_scope(user: AuthenticatedUser, clinic_id: uuid.UUID) -> bool:
    """ADMIN anywhere; front-desk only in its own clinic."""
    return user.role == Role.ADMIN or _user_clinic(user) == clinic_id


def _to_out(visit: Visit) -> VisitOut:
    return VisitOut(
        id=visit.id,
        appointment_id=visit.appointment_id,
        clinic_id=visit.clinic_id,
        patient_id=visit.patient_id,
        provider_id=visit.provider_id,
        status=visit.status,
        checked_in_at=visit.checked_in_at,
        started_at=visit.started_at,
        completed_at=visit.completed_at,
    )


async def _detail(db: AsyncSession, visit: Visit) -> VisitDetailOut:
    history = await visit_repo.get_history(db, visit.id)
    return VisitDetailOut(
        **_to_out(visit).model_dump(),
        history=[
            VisitHistoryOut(from_status=h.from_status, to_status=h.to_status, created_at=h.created_at)
            for h in history
        ],
    )


async def _audit(
    mongo: AsyncMongoClient, user: AuthenticatedUser, visit: Visit, action: str
) -> None:
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action=action,
        entity_type=ENTITY_TYPE,
        entity_id=str(visit.id),
        clinic_id=str(visit.clinic_id),
        outcome="success",
        changed_fields=["status"],
        # One entry per (visit, action) even if an applied write were replayed.
        dedupe_key=f"visit-{visit.id}:{action}",
    )


def _unavailable(service: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"{service} service unavailable"
    )


@router.post("", response_model=VisitOut, status_code=status.HTTP_201_CREATED)
async def check_in(
    response: Response,
    body: CheckInRequest = Body(...),
    user: AuthenticatedUser = Depends(require_role(*STAFF_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> VisitOut:
    if user.role == Role.ADMIN:
        if body.clinic_id is None:
            raise HTTPException(status_code=422, detail="clinic_id is required for admin")
        clinic_id = body.clinic_id
    else:
        own = _user_clinic(user)
        if own is None or (body.clinic_id is not None and body.clinic_id != own):
            raise _not_found()
        clinic_id = own

    existing = await visit_repo.get_by_appointment(db, body.appointment_id)
    if existing is not None:
        if existing.clinic_id != clinic_id:
            raise _not_found()
        response.status_code = status.HTTP_200_OK
        return _to_out(existing)

    try:
        appointment = await get_appointment_ref(
            http, tokens, appointment_id=body.appointment_id, clinic_id=clinic_id
        )
    except (UpstreamServiceUnavailable, ServiceTokenUnavailable) as exc:
        log.warning("visit.check_in_refused", reason="scheduling_unavailable", error=type(exc).__name__)
        raise _unavailable("scheduling") from exc
    if appointment is None or uuid.UUID(appointment["clinic_id"]) != clinic_id:
        raise _not_found()
    if appointment["status"] != APPOINTMENT_CONFIRMED:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="appointment not confirmed")

    visit, created = await visit_repo.create_visit(
        db,
        appointment_id=body.appointment_id,
        clinic_id=clinic_id,
        patient_id=uuid.UUID(appointment["patient_id"]),
        provider_id=uuid.UUID(appointment["provider_id"]),
        created_by=uuid.UUID(user.sub),
        actor=f"user:{user.sub}",
    )
    await db.commit()
    log.info("visit.check_in", visit_id=str(visit.id), created=created)
    if not created:
        response.status_code = status.HTTP_200_OK
        return _to_out(visit)
    await _audit(mongo, user, visit, "visit.check_in")
    return _to_out(visit)


async def _visible_visit(
    db: AsyncSession,
    user: AuthenticatedUser,
    visit_id: uuid.UUID,
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
) -> Visit:
    """The visit if `user` may act on it, else the identical 404.

    A PROVIDER caller is resolved to its provider profile FIRST (before the
    visit is even read), so the answer never depends on whether the visit
    exists in some other clinic or belongs to someone else."""
    if user.role == Role.PROVIDER:
        try:
            provider = await get_provider_by_user(http, tokens, user_id=uuid.UUID(user.sub))
        except (UpstreamServiceUnavailable, ServiceTokenUnavailable) as exc:
            log.warning("visit.provider_lookup_failed", error=type(exc).__name__)
            raise _unavailable("provider") from exc
        visit = await visit_repo.get_visit(db, visit_id)
        if provider is None or visit is None or str(visit.provider_id) != str(provider["id"]):
            raise _not_found()
        return visit

    visit = await visit_repo.get_visit(db, visit_id)
    if visit is None or not _staff_in_scope(user, visit.clinic_id):
        raise _not_found()
    return visit


@router.get("/{visit_id}", response_model=VisitDetailOut)
async def get_visit(
    visit_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*VISIT_ROLES)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> VisitDetailOut:
    visit = await _visible_visit(db, user, visit_id, http, tokens)
    return await _detail(db, visit)


async def _transition(
    *,
    visit_id: uuid.UUID,
    target: str,
    action: str,
    user: AuthenticatedUser,
    db: AsyncSession,
    mongo: AsyncMongoClient,
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
) -> VisitOut:
    await _visible_visit(db, user, visit_id, http, tokens)
    try:
        outcome = await visit_repo.transition_visit(
            db, visit_id=visit_id, target=target, actor=f"user:{user.sub}"
        )
    except IntegrityError:
        # Belt and braces: the conditional UPDATE already serialises movers, so
        # `uq_visit_history_visit_to` should never fire. If it does, nothing of
        # this request is kept and it answers like any other duplicate.
        await db.rollback()
        log.warning("visit.duplicate_history_suppressed", visit_id=str(visit_id), target=target)
        outcome = TransitionOutcome.NOOP

    if outcome == TransitionOutcome.APPLIED:
        await db.commit()
    else:
        await db.rollback()
    if outcome == TransitionOutcome.SKIPPED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"visit must be {PREVIOUS[target]} before {target}",
        )

    visit = await visit_repo.get_visit(db, visit_id)
    assert visit is not None  # visits are never deleted
    log.info("visit.transition", visit_id=str(visit_id), target=target, outcome=outcome)
    if outcome == TransitionOutcome.APPLIED:
        await _audit(mongo, user, visit, action)
    return _to_out(visit)


@router.post("/{visit_id}/start", response_model=VisitOut)
async def start_visit(
    visit_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*VISIT_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> VisitOut:
    return await _transition(
        visit_id=visit_id,
        target=VisitStatus.IN_PROGRESS,
        action="visit.start",
        user=user,
        db=db,
        mongo=mongo,
        http=http,
        tokens=tokens,
    )


@router.post("/{visit_id}/complete", response_model=VisitOut)
async def complete_visit(
    visit_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*VISIT_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> VisitOut:
    return await _transition(
        visit_id=visit_id,
        target=VisitStatus.COMPLETED,
        action="visit.complete",
        user=user,
        db=db,
        mongo=mongo,
        http=http,
        tokens=tokens,
    )
