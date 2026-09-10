"""Availability-template router (PPL-03) — provider/admin, audited,
weekly templates only.

RESEARCH Decision 2 (the most consequential Phase 1 decision): this
router persists recurring WEEKLY AvailabilityTemplate rows (config/rules)
only — it never generates concrete bookable-slot rows. Slot generation
against these templates is Phase 2's job, against the already-proven
GiST exclusion constraint on the (currently empty) concrete-slot table
from Plan 02. This router does not import the concrete-slot ORM class
and does not insert into the concrete-slot table anywhere.

Times (`start_time`/`end_time`) are naive TIME values that MUST resolve
against the owning provider's clinic's `Clinic.timezone` — NEVER assume
UTC or server-local time (RESEARCH Pitfall 3). This router does not
resolve a concrete calendar instant; it only validates and persists the
template. The clinic's timezone is reachable via
`provider.department.clinic.timezone` for any future consumer (Phase 2)
that needs to resolve an instant from this template.

Gap-closure Plan 01-07 adds `PATCH .../{template_id}` (partial update)
and `POST .../{template_id}/deactivate` (soft-delete, NEVER a row
delete) — both enforce the SAME CR-02 provider-ownership check as
create/list above (PROVIDER role may only manage their OWN provider_id;
ADMIN may act on any provider_id). `GET` list excludes inactive
templates by default; `?include_inactive=true` includes them. A PATCH
that would leave the window inconsistent (end_time<=start_time after
merging with existing values) is rejected 422, exactly like create.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.observability.logging import get_logger
from services.identity.domain.enums import Role
from services.identity.domain.schemas import AvailabilityTemplateUpdate
from services.identity.repository import people_repo
from services.identity.security.current_user import AuthenticatedUser, user_id_as_uuid
from services.identity.security.rbac import require_role
from smarthealth.db import get_db, get_mongo

router = APIRouter(prefix="/api/providers/{provider_id}/availability", tags=["availability"])

log = get_logger(__name__)


class AvailabilityTemplateCreate(BaseModel):
    """Validates a weekly availability window before it is ever persisted.

    NOTE (RESEARCH Pitfall 3): `start_time`/`end_time` are naive TIME
    values. They resolve against the owning provider's clinic's
    `Clinic.timezone` (IANA tz name) — NOT UTC, NOT server-local time.
    Phase 2's slot-generation job is the consumer that performs that
    resolution; this schema only validates the template's internal
    consistency (day_of_week range, end after start, positive duration,
    effective_from <= effective_to).
    """

    day_of_week: int = Field(ge=0, le=6, description="0=Monday .. 6=Sunday")
    start_time: time
    end_time: time
    slot_duration_minutes: int = Field(default=30, gt=0)
    effective_from: date
    effective_to: date | None = None

    @model_validator(mode="after")
    def _validate_window(self) -> "AvailabilityTemplateCreate":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be strictly after start_time")
        if self.effective_to is not None and self.effective_from > self.effective_to:
            raise ValueError("effective_from must be on or before effective_to")
        return self


class AvailabilityTemplateOut(BaseModel):
    id: uuid.UUID
    provider_id: uuid.UUID
    department_id: uuid.UUID
    day_of_week: int
    start_time: time
    end_time: time
    slot_duration_minutes: int
    effective_from: date
    effective_to: date | None = None
    is_active: bool
    deactivated_at: datetime | None = None


def _to_out(template) -> AvailabilityTemplateOut:
    return AvailabilityTemplateOut(
        id=template.id,
        provider_id=template.provider_id,
        department_id=template.department_id,
        day_of_week=template.day_of_week,
        start_time=template.start_time,
        end_time=template.end_time,
        slot_duration_minutes=template.slot_duration_minutes,
        effective_from=template.effective_from,
        effective_to=template.effective_to,
        is_active=template.is_active,
        deactivated_at=template.deactivated_at,
    )


def _assert_provider_owner_or_admin(
    user: AuthenticatedUser, provider
) -> None:
    """CR-02: a PROVIDER may only manage their OWN availability — mirrors
    the identical check in create_availability_template/list_availability.
    """
    if user.role == Role.PROVIDER and provider.user_id != user_id_as_uuid(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="cannot manage another provider's availability",
        )


@router.post("", response_model=AvailabilityTemplateOut, status_code=status.HTTP_201_CREATED)
async def create_availability_template(
    provider_id: uuid.UUID,
    payload: AvailabilityTemplateCreate,
    user: AuthenticatedUser = Depends(require_role(Role.PROVIDER, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> AvailabilityTemplateOut:
    provider = await people_repo.get_provider(db, provider_id)
    if provider is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.create",
            entity_type="availability_template",
            entity_id="unknown",
            clinic_id=None,
            outcome="failure",
        )
        log.info("availability.create.provider_not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    # CR-02: a PROVIDER may only manage their OWN availability — Provider.user_id
    # must match the authenticated caller. ADMIN may act on any provider_id.
    if user.role == Role.PROVIDER and provider.user_id != user_id_as_uuid(user):
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.create",
            entity_type="availability_template",
            entity_id=str(provider_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info(
            "availability.create.forbidden_not_owner",
            provider_id=str(provider_id),
            user_id=user.sub,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="cannot manage another provider's availability",
        )

    template = await people_repo.create_availability_template(
        db,
        provider_id=provider_id,
        department_id=provider.department_id,
        day_of_week=payload.day_of_week,
        start_time=payload.start_time,
        end_time=payload.end_time,
        slot_duration_minutes=payload.slot_duration_minutes,
        effective_from=payload.effective_from,
        effective_to=payload.effective_to,
    )

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="availability.create",
        entity_type="availability_template",
        entity_id=str(template.id),
        clinic_id=None,
        outcome="success",
        changed_fields=["day_of_week", "start_time", "end_time", "slot_duration_minutes"],
    )
    log.info(
        "availability.create.success",
        availability_template_id=str(template.id),
        provider_id=str(provider_id),
    )

    return _to_out(template)


@router.get("", response_model=list[AvailabilityTemplateOut])
async def list_availability(
    provider_id: uuid.UUID,
    include_inactive: bool = False,
    user: AuthenticatedUser = Depends(require_role(Role.PROVIDER, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> list[AvailabilityTemplateOut]:
    provider = await people_repo.get_provider(db, provider_id)
    if provider is None:
        # IN-06: audit the 404 (provider-not-found) branch for consistency
        # with the sibling create_availability_template's 404 branch and
        # with this route's own 403 (ownership) branch below — this
        # codebase's design principle is that denied/failed attempts are
        # ALSO audited, not only successful mutations/reads.
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.list",
            entity_type="availability_template",
            entity_id="unknown",
            clinic_id=None,
            outcome="failure",
        )
        log.info("availability.list.provider_not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    # CR-02: a PROVIDER may only read their OWN availability — see the
    # matching check in create_availability_template for rationale.
    if user.role == Role.PROVIDER and provider.user_id != user_id_as_uuid(user):
        log.info(
            "availability.list.forbidden_not_owner",
            provider_id=str(provider_id),
            user_id=user.sub,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="cannot view another provider's availability",
        )

    templates = await people_repo.list_availability(
        db, provider_id, include_inactive=include_inactive
    )
    return [_to_out(t) for t in templates]


@router.patch("/{template_id}", response_model=AvailabilityTemplateOut)
async def update_availability_template(
    provider_id: uuid.UUID,
    template_id: uuid.UUID,
    payload: AvailabilityTemplateUpdate,
    user: AuthenticatedUser = Depends(require_role(Role.PROVIDER, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> AvailabilityTemplateOut:
    provider = await people_repo.get_provider(db, provider_id)
    if provider is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.update",
            entity_type="availability_template",
            entity_id=str(template_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info("availability.update.provider_not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    _assert_provider_owner_or_admin(user, provider)

    existing = await people_repo.get_availability_template(db, template_id, provider_id)
    if existing is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.update",
            entity_type="availability_template",
            entity_id=str(template_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info(
            "availability.update.not_found", template_id=str(template_id), provider_id=str(provider_id)
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="availability template not found")

    # Validate the RESULTING window (existing values merged with the
    # partial update) — a PATCH that supplies only start_time and leaves
    # end_time untouched must not silently create an inverted window.
    resulting_start = payload.start_time if payload.start_time is not None else existing.start_time
    resulting_end = payload.end_time if payload.end_time is not None else existing.end_time
    if resulting_end <= resulting_start:
        raise HTTPException(
            status_code=422,
            detail="end_time must be strictly after start_time",
        )
    resulting_from = (
        payload.effective_from if payload.effective_from is not None else existing.effective_from
    )
    resulting_to = (
        payload.effective_to if payload.effective_to is not None else existing.effective_to
    )
    if resulting_to is not None and resulting_from > resulting_to:
        raise HTTPException(
            status_code=422,
            detail="effective_from must be on or before effective_to",
        )

    template = await people_repo.update_availability_template(
        db,
        template_id,
        provider_id,
        day_of_week=payload.day_of_week,
        start_time=payload.start_time,
        end_time=payload.end_time,
        slot_duration_minutes=payload.slot_duration_minutes,
        effective_from=payload.effective_from,
        effective_to=payload.effective_to,
    )

    changed_fields = list(payload.model_fields_set)
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="availability.update",
        entity_type="availability_template",
        entity_id=str(template.id),
        clinic_id=None,
        outcome="success",
        changed_fields=changed_fields,
    )
    log.info(
        "availability.update.success",
        availability_template_id=str(template.id),
        provider_id=str(provider_id),
    )

    return _to_out(template)


@router.post("/{template_id}/deactivate", response_model=AvailabilityTemplateOut)
async def deactivate_availability_template(
    provider_id: uuid.UUID,
    template_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.PROVIDER, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> AvailabilityTemplateOut:
    provider = await people_repo.get_provider(db, provider_id)
    if provider is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.deactivate",
            entity_type="availability_template",
            entity_id=str(template_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info("availability.deactivate.provider_not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    _assert_provider_owner_or_admin(user, provider)

    template = await people_repo.deactivate_availability_template(db, template_id, provider_id)
    if template is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="availability.deactivate",
            entity_type="availability_template",
            entity_id=str(template_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info(
            "availability.deactivate.not_found",
            template_id=str(template_id),
            provider_id=str(provider_id),
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="availability template not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="availability.deactivate",
        entity_type="availability_template",
        entity_id=str(template.id),
        clinic_id=None,
        outcome="success",
    )
    log.info(
        "availability.deactivate.success",
        availability_template_id=str(template.id),
        provider_id=str(provider_id),
    )

    return _to_out(template)
