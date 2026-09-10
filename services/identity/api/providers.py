"""Provider CRUD router (PPL-02) — admin-only, audited, department-scoped.

`ProviderCreate` requires `department_id` (the ORG-03 scope-chain root:
provider -> department -> clinic) and `specialties` (>=1 name, RESEARCH
must_haves — "one or more specialties"). A provider is created ONLY under
an existing department; a non-existent `department_id` is rejected 404
with no orphan row (mirroring `org_repo.create_department`'s missing-
parent pattern), never a surfaced FK IntegrityError.

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
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.observability.logging import get_logger
from services.identity.domain.enums import Role
from services.identity.domain.schemas import ProviderUpdate
from services.identity.repository import people_repo
from services.identity.security.current_user import AuthenticatedUser
from services.identity.security.rbac import require_role
from smarthealth.db import get_db, get_mongo

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


@router.post("", response_model=ProviderOut, status_code=status.HTTP_201_CREATED)
async def create_provider(
    payload: ProviderCreate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ProviderOut:
    provider = await people_repo.create_provider(
        db,
        department_id=payload.department_id,
        full_name=payload.full_name,
        specialty_names=payload.specialties,
        user_id=payload.user_id,
    )

    if provider is None:
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
        log.info("provider.create.department_not_found", department_id=str(payload.department_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="department not found")

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
    providers = await people_repo.list_providers(
        db, department_id, include_inactive=include_inactive
    )
    return [_to_out(p) for p in providers]


@router.get("/{provider_id}", response_model=ProviderOut)
async def get_provider(
    provider_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> ProviderOut:
    provider = await people_repo.get_provider(db, provider_id)
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
    provider = await people_repo.update_provider(
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
    provider = await people_repo.deactivate_provider(db, provider_id)

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
