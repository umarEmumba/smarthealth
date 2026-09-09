"""Clinic CRUD router (ORG-01) — admin-only mutations, audited.

POST /api/clinics and the list/detail GET routes reuse the tracer's
(Plan 03) proven primitives: `require_role(Role.ADMIN)` for the write
guard, `write_audit` for the shared audit helper, and `get_current_user`/
`get_db` for auth + DB access. No new architecture — this is Wave-4
horizontal expansion onto an already-proven skeleton.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.observability.logging import get_logger
from services.identity.domain.enums import Role
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

    return ClinicOut(id=clinic.id, name=clinic.name, timezone=clinic.timezone)


@router.get("", response_model=list[ClinicOut])
async def list_clinics(
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> list[ClinicOut]:
    clinics = await org_repo.list_clinics(db)
    return [ClinicOut(id=c.id, name=c.name, timezone=c.timezone) for c in clinics]


@router.get("/{clinic_id}", response_model=ClinicOut)
async def get_clinic(
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> ClinicOut:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")
    return ClinicOut(id=clinic.id, name=clinic.name, timezone=clinic.timezone)
