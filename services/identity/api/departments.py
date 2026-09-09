"""Department CRUD router, nested under a clinic (ORG-02/03) — admin-only
mutations, audited, clinic-scoped.

`create_department`/`list_departments` always take `clinic_id` explicitly
via `org_repo` (RESEARCH Pitfall 1) — there is no id-only department
lookup path in this router or its repository. A department created under
a non-existent clinic id is rejected 404 with no orphan row written
(T-04-04); the clinic existence check happens in `org_repo.create_department`
before any INSERT is issued.
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

router = APIRouter(prefix="/api/clinics/{clinic_id}/departments", tags=["departments"])

log = get_logger(__name__)


class DepartmentCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)


class DepartmentOut(BaseModel):
    id: uuid.UUID
    clinic_id: uuid.UUID
    name: str


@router.post("", response_model=DepartmentOut, status_code=status.HTTP_201_CREATED)
async def create_department(
    clinic_id: uuid.UUID,
    payload: DepartmentCreate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> DepartmentOut:
    department = await org_repo.create_department(db, clinic_id=clinic_id, name=payload.name)

    if department is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="department.create",
            entity_type="department",
            entity_id="unknown",
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info("department.create.clinic_not_found", clinic_id=str(clinic_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="department.create",
        entity_type="department",
        entity_id=str(department.id),
        clinic_id=str(clinic_id),
        outcome="success",
    )
    log.info("department.create.success", department_id=str(department.id), clinic_id=str(clinic_id))

    return DepartmentOut(id=department.id, clinic_id=department.clinic_id, name=department.name)


@router.get("", response_model=list[DepartmentOut])
async def list_departments(
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> list[DepartmentOut]:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    departments = await org_repo.list_departments(db, clinic_id)
    return [DepartmentOut(id=d.id, clinic_id=d.clinic_id, name=d.name) for d in departments]
