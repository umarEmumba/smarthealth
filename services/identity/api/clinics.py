"""Clinic CRUD router (ORG-01) — admin-only mutations, audited.

POST /api/clinics and the list/detail GET routes reuse the tracer's
(Plan 03) proven primitives: `require_role(Role.ADMIN)` for the write
guard, `write_audit` for the shared audit helper, and `get_current_user`/
`get_db` for auth + DB access. No new architecture — this is Wave-4
horizontal expansion onto an already-proven skeleton.

Gap-closure Plan 01-07 adds `PATCH /api/clinics/{clinic_id}` (partial
update, `ClinicUpdate`) and `POST /api/clinics/{clinic_id}/deactivate`
(soft-delete — is_active=false + deactivated_at=now(), NEVER a row
delete). `GET /api/clinics` excludes inactive clinics by default; pass
`?include_inactive=true` to include them. `GET /api/clinics/{id}` always
returns the clinic regardless of active status, so its is_active value
stays visible.
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
from services.identity.domain.schemas import ClinicUpdate
from services.identity.repository import org_repo
from services.identity.security.current_user import AuthenticatedUser
from services.identity.security.rbac import require_role
from smarthealth.db import get_db, get_mongo

router = APIRouter(prefix="/api/clinics", tags=["clinics"])

log = get_logger(__name__)


class ClinicCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    timezone: str = Field(min_length=1, max_length=64, description="IANA tz name, e.g. America/New_York")


class ClinicOut(BaseModel):
    id: uuid.UUID
    name: str
    timezone: str
    is_active: bool
    deactivated_at: datetime | None = None


def _to_out(clinic) -> ClinicOut:
    return ClinicOut(
        id=clinic.id,
        name=clinic.name,
        timezone=clinic.timezone,
        is_active=clinic.is_active,
        deactivated_at=clinic.deactivated_at,
    )


@router.post("", response_model=ClinicOut, status_code=status.HTTP_201_CREATED)
async def create_clinic(
    payload: ClinicCreate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ClinicOut:
    clinic = await org_repo.create_clinic(db, name=payload.name, timezone=payload.timezone)

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="clinic.create",
        entity_type="clinic",
        entity_id=str(clinic.id),
        clinic_id=str(clinic.id),
        outcome="success",
    )
    log.info("clinic.create.success", clinic_id=str(clinic.id))

    return _to_out(clinic)


@router.get("", response_model=list[ClinicOut])
async def list_clinics(
    include_inactive: bool = False,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> list[ClinicOut]:
    clinics = await org_repo.list_clinics(db, include_inactive=include_inactive)
    return [_to_out(c) for c in clinics]


@router.get("/{clinic_id}", response_model=ClinicOut)
async def get_clinic(
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> ClinicOut:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")
    return _to_out(clinic)


@router.patch("/{clinic_id}", response_model=ClinicOut)
async def update_clinic(
    clinic_id: uuid.UUID,
    payload: ClinicUpdate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ClinicOut:
    clinic = await org_repo.update_clinic(
        db, clinic_id, name=payload.name, timezone=payload.timezone
    )

    if clinic is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="clinic.update",
            entity_type="clinic",
            entity_id=str(clinic_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info("clinic.update.not_found", clinic_id=str(clinic_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    changed_fields = list(payload.model_fields_set)
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="clinic.update",
        entity_type="clinic",
        entity_id=str(clinic.id),
        clinic_id=str(clinic.id),
        outcome="success",
        changed_fields=changed_fields,
    )
    log.info("clinic.update.success", clinic_id=str(clinic.id))

    return _to_out(clinic)


@router.post("/{clinic_id}/deactivate", response_model=ClinicOut)
async def deactivate_clinic(
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> ClinicOut:
    clinic = await org_repo.deactivate_clinic(db, clinic_id)

    if clinic is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="clinic.deactivate",
            entity_type="clinic",
            entity_id=str(clinic_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info("clinic.deactivate.not_found", clinic_id=str(clinic_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="clinic.deactivate",
        entity_type="clinic",
        entity_id=str(clinic.id),
        clinic_id=str(clinic.id),
        outcome="success",
    )
    log.info("clinic.deactivate.success", clinic_id=str(clinic.id))

    return _to_out(clinic)
