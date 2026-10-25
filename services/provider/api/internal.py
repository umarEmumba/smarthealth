"""Provider's internal integrity lookup for other services (T-01.1-31).

Scheduling (Phase 2) will call this before accepting a `provider_slots.provider_id`,
since that column can no longer be a foreign key across the service boundary:

    GET /internal/providers/{provider_id} -> {id, department_id, is_active}

Plan 02-03 (research G3) adds two more lookups:

    GET /internal/providers/by-user/{user_id}        -> ProviderRef (Scheduling, Visit; D-15)
    GET /internal/providers/{provider_id}/availability -> [AvailabilityRef] (Scheduling; D-01)

Plan 02.1-05 (M2) adds the schedule-change feed Scheduling drains:

    GET  /internal/schedule-changes?limit=             -> [ScheduleChangeRef] (undelivered, oldest first)
    POST /internal/schedule-changes/{event_id}/ack     -> {id, delivered: true}

Events are an outbox written by Provider's repository in the same transaction as the
change; a pending event stays in the feed until acknowledged, so nothing is lost if the
consumer or Temporal is down when the change is made. Only a Scheduling service token
(`aud=provider`) or an ADMIN user token is accepted; the payload carries ids, the
change type and a timestamp only.

The availability list includes INACTIVE templates (`is_active: false`); slot
generation decides what to do with them. `by-user` is declared before
`{provider_id}` so the literal segment is matched first.

Design rules:
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated callers only: ADMIN user tokens, or a Scheduling/Visit service
    token whose `aud` is `provider` (`require_internal_caller`, D-06). Any other
    role or service is 403; no token, or a service token for another audience, is 401.
  * INACTIVE providers are REPORTED (`is_active: false`), never hidden: the
    caller decides what "inactive" means for it.
  * An unknown id is the same 404 for every caller. The answer carries ids and a
    flag only: no name, no specialties, no account link.
  * Reads are not audited (Phase 1 does not audit reads); the log line carries
    ids only. Provider makes no outbound call from here.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.provider.db import get_db
from services.provider.repository import provider_repo

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


class ProviderRef(BaseModel):
    id: uuid.UUID
    department_id: uuid.UUID
    is_active: bool


class AvailabilityRef(BaseModel):
    id: uuid.UUID
    department_id: uuid.UUID
    day_of_week: int
    start_time: time
    end_time: time
    slot_duration_minutes: int
    effective_from: date
    effective_to: date | None = None
    is_active: bool


class ScheduleChangeRef(BaseModel):
    id: uuid.UUID
    provider_id: uuid.UUID
    change_type: str
    template_id: uuid.UUID | None = None
    created_at: datetime


class ScheduleChangeAck(BaseModel):
    id: uuid.UUID
    delivered: bool


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


# Declared BEFORE `/providers/{provider_id}` so the literal `by-user` segment wins.
@router.get("/providers/by-user/{user_id}", response_model=ProviderRef)
async def lookup_provider_by_user(
    user_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling", "visit"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> ProviderRef:
    provider = await provider_repo.get_provider_by_user(db, user_id)
    if provider is None:
        log.info("internal.provider_lookup", by="user", user_id=str(user_id), outcome="not_found")
        raise _not_found()

    log.info("internal.provider_lookup", by="user", provider_id=str(provider.id), outcome="found")
    return ProviderRef(
        id=provider.id, department_id=provider.department_id, is_active=provider.is_active
    )


@router.get("/providers/{provider_id}/availability", response_model=list[AvailabilityRef])
async def lookup_provider_availability(
    provider_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> list[AvailabilityRef]:
    provider = await provider_repo.get_provider(db, provider_id)
    if provider is None:
        log.info("internal.provider_availability_lookup", provider_id=str(provider_id), outcome="not_found")
        raise _not_found()

    templates = await provider_repo.list_availability(db, provider.id, include_inactive=True)
    log.info(
        "internal.provider_availability_lookup",
        provider_id=str(provider.id),
        count=len(templates),
        outcome="found",
    )
    return [
        AvailabilityRef(
            id=t.id,
            department_id=t.department_id,
            day_of_week=t.day_of_week,
            start_time=t.start_time,
            end_time=t.end_time,
            slot_duration_minutes=t.slot_duration_minutes,
            effective_from=t.effective_from,
            effective_to=t.effective_to,
            is_active=t.is_active,
        )
        for t in templates
    ]


@router.get("/providers/{provider_id}", response_model=ProviderRef)
async def lookup_provider(
    provider_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling", "visit"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> ProviderRef:
    provider = await provider_repo.get_provider(db, provider_id)
    if provider is None:
        log.info("internal.provider_lookup", provider_id=str(provider_id), outcome="not_found")
        raise _not_found()

    log.info("internal.provider_lookup", provider_id=str(provider.id), outcome="found")
    return ProviderRef(
        id=provider.id, department_id=provider.department_id, is_active=provider.is_active
    )


@router.get("/schedule-changes", response_model=list[ScheduleChangeRef])
async def list_schedule_changes(
    limit: int = Query(default=100, ge=1, le=200),
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> list[ScheduleChangeRef]:
    events = await provider_repo.list_pending_changes(db, limit=limit)
    log.info("internal.schedule_changes_feed", count=len(events), outcome="listed")
    return [
        ScheduleChangeRef(
            id=e.id,
            provider_id=e.provider_id,
            change_type=e.change_type,
            template_id=e.template_id,
            created_at=e.created_at,
        )
        for e in events
    ]


@router.post("/schedule-changes/{event_id}/ack", response_model=ScheduleChangeAck)
async def acknowledge_schedule_change(
    event_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> ScheduleChangeAck:
    outcome = await provider_repo.mark_change_delivered(db, event_id)
    log.info("internal.schedule_change_ack", event_id=str(event_id), outcome=outcome)
    if outcome == "not_found":
        raise _not_found()
    return ScheduleChangeAck(id=event_id, delivered=True)
