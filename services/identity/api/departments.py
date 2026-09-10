"""Department CRUD router, nested under a clinic (ORG-02/03) — admin-only
mutations, audited, clinic-scoped.

`create_department`/`list_departments` always take `clinic_id` explicitly
via `org_repo` (RESEARCH Pitfall 1) — there is no id-only department
lookup path in this router or its repository. A department created under
a non-existent clinic id is rejected 404 with no orphan row written
(T-04-04); the clinic existence check happens in `org_repo.create_department`
before any INSERT is issued.

Gap-closure Plan 01-07 adds `PATCH .../{department_id}` (partial update)
and `POST .../{department_id}/deactivate` (soft-delete, NEVER a row
delete) — both clinic-scoped via `org_repo.get_department_scoped`, so a
department id from a different clinic 404s rather than being editable.
`GET` list excludes inactive departments by default; `?include_inactive=
true` includes them.
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
from services.identity.domain.schemas import DepartmentUpdate
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
    is_active: bool
    deactivated_at: datetime | None = None


def _to_out(department) -> DepartmentOut:
    return DepartmentOut(
        id=department.id,
        clinic_id=department.clinic_id,
        name=department.name,
        is_active=department.is_active,
        deactivated_at=department.deactivated_at,
    )


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

    return _to_out(department)


@router.get("", response_model=list[DepartmentOut])
async def list_departments(
    clinic_id: uuid.UUID,
    include_inactive: bool = False,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> list[DepartmentOut]:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    departments = await org_repo.list_departments(db, clinic_id, include_inactive=include_inactive)
    return [_to_out(d) for d in departments]


@router.patch("/{department_id}", response_model=DepartmentOut)
async def update_department(
    clinic_id: uuid.UUID,
    department_id: uuid.UUID,
    payload: DepartmentUpdate,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> DepartmentOut:
    department = await org_repo.update_department(
        db, department_id, clinic_id, name=payload.name
    )

    if department is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="department.update",
            entity_type="department",
            entity_id=str(department_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info(
            "department.update.not_found", department_id=str(department_id), clinic_id=str(clinic_id)
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="department not found")

    changed_fields = list(payload.model_fields_set)
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="department.update",
        entity_type="department",
        entity_id=str(department.id),
        clinic_id=str(clinic_id),
        outcome="success",
        changed_fields=changed_fields,
    )
    log.info("department.update.success", department_id=str(department.id), clinic_id=str(clinic_id))

    return _to_out(department)


@router.post("/{department_id}/deactivate", response_model=DepartmentOut)
async def deactivate_department(
    clinic_id: uuid.UUID,
    department_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> DepartmentOut:
    department = await org_repo.deactivate_department(db, department_id, clinic_id)

    if department is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="department.deactivate",
            entity_type="department",
            entity_id=str(department_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info(
            "department.deactivate.not_found",
            department_id=str(department_id),
            clinic_id=str(clinic_id),
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="department not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="department.deactivate",
        entity_type="department",
        entity_id=str(department.id),
        clinic_id=str(clinic_id),
        outcome="success",
    )
    log.info(
        "department.deactivate.success", department_id=str(department.id), clinic_id=str(clinic_id)
    )

    return _to_out(department)
