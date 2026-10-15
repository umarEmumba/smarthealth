"""Provider CRUD router (PPL-02) — admin-only, audited, department-scoped.

`ProviderCreate` requires `department_id` (the ORG-03 scope-chain root:
provider -> department -> clinic) and `specialties` (>=1 name, RESEARCH
must_haves — "one or more specialties"). The department belongs to Organization,
so a provider is created ONLY under a department Organization confirms exists and
is active, checked synchronously BEFORE any write (D-05): a missing or inactive
department is a 422 (the Phase 1 404 is superseded), Organization being unable to
answer is a 503, and in both cases nothing is written. Reads return this
service's own rows with the department as a bare id — never enriched with another
service's data (D-07) — and deactivating a department never cascades (D-06).

Gap-closure Plan 01-07 adds `PATCH /api/providers/{provider_id}` (partial
update — `specialties`, when supplied, REPLACES the full set via the same
get-or-create-by-name path `create_provider` uses) and `POST
/api/providers/{provider_id}/deactivate` (soft-delete, NEVER a row
delete) — both admin-only. `GET` list excludes inactive providers by
default; `?include_inactive=true` includes them.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
import httpx
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from libs.db.mongo import get_mongo
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger
from libs.service_client import (
    ReferencedResourceInvalid,
    UpstreamServiceUnavailable,
    reference_error_detail,
    validate_department,
    validate_user,
)
from services.provider.db import get_db
from services.provider.domain.schemas import ProviderUpdate
from services.provider.repository import provider_repo

router = APIRouter(prefix="/api/providers", tags=["providers"])

log = get_logger(__name__)


class ProviderCreate(BaseModel):
    department_id: uuid.UUID
    full_name: str = Field(min_length=1, max_length=255)
    specialties: list[str] = Field(min_length=1, description="One or more specialty names")
    user_id: uuid.UUID | None = None


class SpecialtyOut(BaseModel):
    id: uuid.UUID
    name: str


class ProviderOut(BaseModel):
    id: uuid.UUID
    department_id: uuid.UUID
    full_name: str
    specialties: list[SpecialtyOut]
    user_id: uuid.UUID | None = None
    is_active: bool
    deactivated_at: datetime | None = None


def _to_out(provider) -> ProviderOut:
    return ProviderOut(
        id=provider.id,
        department_id=provider.department_id,
        full_name=provider.full_name,
        specialties=[SpecialtyOut(id=s.id, name=s.name) for s in provider.specialties],
        user_id=provider.user_id,
        is_active=provider.is_active,
        deactivated_at=provider.deactivated_at,
    )


async def _audit_create_refused(mongo: AsyncMongoClient, user: AuthenticatedUser) -> None:
    """Failure audit for a create refused by a reference check (no field values)."""
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="provider.create",
        entity_type="provider",
        entity_id="unknown",
        clinic_id=None,
        outcome="failure",
    )


async def _validate_references_or_refuse(
    http: httpx.AsyncClient,
    mongo: AsyncMongoClient,
    user: AuthenticatedUser,
    payload: ProviderCreate,
) -> None:
    """D-05: check the cross-service references BEFORE any write.

    `department_id` belongs to Organization and, when supplied, `user_id` belongs
    to Identity. A missing/inactive target is a 422, an owner that cannot answer
    cleanly is a 503; either way the refusal is audited as a failure and nothing
    has been written yet, so there is never a half-created provider. The calls
    carry the id only.
    """
    try:
        await validate_department(http, payload.department_id, bearer_token=user.raw_token)
    except ReferencedResourceInvalid as exc:
        await _audit_create_refused(mongo, user)
        log.info("provider.create.department_rejected", department_id=str(payload.department_id))
        raise HTTPException(status_code=422, detail=reference_error_detail(exc)) from exc
    except UpstreamServiceUnavailable as exc:
        await _audit_create_refused(mongo, user)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="organization service unavailable",
        ) from exc

    if payload.user_id is None:
        return
    try:
        await validate_user(http, payload.user_id, bearer_token=user.raw_token)
    except ReferencedResourceInvalid as exc:
        await _audit_create_refused(mongo, user)
        log.info("provider.create.user_rejected", department_id=str(payload.department_id))
        raise HTTPException(status_code=422, detail=reference_error_detail(exc)) from exc
    except UpstreamServiceUnavailable as exc:
        await _audit_create_refused(mongo, user)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="identity service unavailable",
        ) from exc


@router.post("", response_model=ProviderOut, status_code=status.HTTP_201_CREATED)
async def create_provider(
    payload: ProviderCreate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
) -> ProviderOut:
    await _validate_references_or_refuse(http, mongo, user, payload)

    try:
        provider = await provider_repo.create_provider(
            db,
            department_id=payload.department_id,
            full_name=payload.full_name,
            specialty_names=payload.specialties,
            user_id=payload.user_id,
        )
    except IntegrityError as exc:
        # The only constraint a validated create can trip is the one-profile-per-
        # account unique on user_id (two creates racing for the same account); the
        # database constraint is the arbiter, not an application lock.
        await db.rollback()
        if payload.user_id is None:
            raise
        await _audit_create_refused(mongo, user)
        log.info("provider.create.account_already_linked")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="account is already linked to a provider profile",
        ) from exc

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="provider.create",
        entity_type="provider",
        entity_id=str(provider.id),
        clinic_id=None,
        outcome="success",
        changed_fields=["full_name", "specialties"],
    )
    log.info(
        "provider.create.success",
        provider_id=str(provider.id),
        department_id=str(provider.department_id),
    )

    return _to_out(provider)


@router.get("", response_model=list[ProviderOut])
async def list_providers(
    department_id: uuid.UUID,
    include_inactive: bool = False,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> list[ProviderOut]:
    providers = await provider_repo.list_providers(
        db, department_id, include_inactive=include_inactive
    )
    return [_to_out(p) for p in providers]


@router.get("/{provider_id}", response_model=ProviderOut)
async def get_provider(
    provider_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> ProviderOut:
    provider = await provider_repo.get_provider(db, provider_id)
    if provider is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")
    return _to_out(provider)


@router.patch("/{provider_id}", response_model=ProviderOut)
async def update_provider(
    provider_id: uuid.UUID,
    payload: ProviderUpdate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ProviderOut:
    provider = await provider_repo.update_provider(
        db, provider_id, full_name=payload.full_name, specialty_names=payload.specialties
    )

    if provider is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="provider.update",
            entity_type="provider",
            entity_id=str(provider_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info("provider.update.not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    changed_fields = list(payload.model_fields_set)
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="provider.update",
        entity_type="provider",
        entity_id=str(provider.id),
        clinic_id=None,
        outcome="success",
        changed_fields=changed_fields,
    )
    log.info("provider.update.success", provider_id=str(provider.id))

    return _to_out(provider)


@router.post("/{provider_id}/deactivate", response_model=ProviderOut)
async def deactivate_provider(
    provider_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ProviderOut:
    provider = await provider_repo.deactivate_provider(db, provider_id)

    if provider is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="provider.deactivate",
            entity_type="provider",
            entity_id=str(provider_id),
            clinic_id=None,
            outcome="failure",
        )
        log.info("provider.deactivate.not_found", provider_id=str(provider_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="provider not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="provider.deactivate",
        entity_type="provider",
        entity_id=str(provider.id),
        clinic_id=None,
        outcome="success",
    )
    log.info("provider.deactivate.success", provider_id=str(provider.id))

    return _to_out(provider)
