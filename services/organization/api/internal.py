"""Internal reference lookups for other services (D-05 consumer side, D-06).

Identity, Patient and Provider call these at write time before accepting a
reference to a clinic or department, forwarding the original caller's token:

    GET /internal/clinics/{clinic_id}         -> {id, is_active}
    GET /internal/departments/{department_id} -> {id, clinic_id, is_active}

Design rules (STRIDE T-01.1-14):
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated, role-allowlisted callers only (`require_internal_caller`).
  * A missing id and an out-of-clinic-scope id return the IDENTICAL 404, so
    existence cannot be probed across clinics.
  * INACTIVE rows are reported (`is_active: false`), never hidden: the caller
    turns "inactive" into a 422 (RESEARCH Pitfall 5, D-06). That is why the
    id-only `org_repo.get_department` (no `is_active` filter) is used here.
  * Minimal fields only; reads are not audited (Phase 1 does not audit reads).
  * Organization makes no outbound call from here or anywhere (no reverse
    check, no cascade — D-06).
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.organization.db import get_db
from services.organization.repository import org_repo

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


class ClinicRef(BaseModel):
    id: uuid.UUID
    is_active: bool


class DepartmentRef(BaseModel):
    id: uuid.UUID
    clinic_id: uuid.UUID
    is_active: bool


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


@router.get("/clinics/{clinic_id}", response_model=ClinicRef)
async def lookup_clinic(
    clinic_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, Role.FRONT_DESK, audience="organization", allowed_services=frozenset({"identity"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> ClinicRef:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None or not clinic_scope_permits(caller, clinic.id):
        log.info("internal.clinic_lookup", clinic_id=str(clinic_id), outcome="not_found")
        raise _not_found()

    log.info("internal.clinic_lookup", clinic_id=str(clinic.id), outcome="found")
    return ClinicRef(id=clinic.id, is_active=clinic.is_active)


@router.get("/departments/{department_id}", response_model=DepartmentRef)
async def lookup_department(
    department_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, Role.PROVIDER, audience="organization", allowed_services=frozenset())
    ),
    db: AsyncSession = Depends(get_db),
) -> DepartmentRef:
    department = await org_repo.get_department(db, department_id)
    if department is None or not clinic_scope_permits(caller, department.clinic_id):
        log.info("internal.department_lookup", department_id=str(department_id), outcome="not_found")
        raise _not_found()

    log.info("internal.department_lookup", department_id=str(department.id), outcome="found")
    return DepartmentRef(
        id=department.id, clinic_id=department.clinic_id, is_active=department.is_active
    )
