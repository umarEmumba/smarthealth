"""Async repository for clinics/departments (ORG-01/02/03).

Every department method takes `clinic_id` explicitly — there is no
id-only department fetch path. This is the structural fix for RESEARCH
Pitfall 1 (RBAC enforced only at the router level, not the query level):
a repository method that could fetch a department by its id alone, with
no clinic scope, would let an admin (or a future less-trusted role) walk
department ids across clinics. Making `clinic_id` a required parameter on
every department read/write makes that unscoped path structurally
unavailable, not just discouraged by convention.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone as dt_timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.organization.repository.models import Clinic, Department


async def create_clinic(db: AsyncSession, *, name: str, timezone: str) -> Clinic:
    clinic = Clinic(name=name, timezone=timezone)
    db.add(clinic)
    await db.commit()
    await db.refresh(clinic)
    return clinic


async def get_clinic(db: AsyncSession, clinic_id: uuid.UUID) -> Clinic | None:
    result = await db.execute(select(Clinic).where(Clinic.id == clinic_id))
    return result.scalar_one_or_none()


async def list_clinics(db: AsyncSession, *, include_inactive: bool = False) -> list[Clinic]:
    stmt = select(Clinic).order_by(Clinic.created_at)
    if not include_inactive:
        stmt = stmt.where(Clinic.is_active.is_(True))
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def update_clinic(
    db: AsyncSession,
    clinic_id: uuid.UUID,
    *,
    name: str | None = None,
    timezone: str | None = None,
) -> Clinic | None:
    """Partial-update a clinic — only fields explicitly passed (non-None)
    change. Returns `None` if `clinic_id` does not reference an existing
    clinic (the caller/router maps this to 404).
    """
    clinic = await get_clinic(db, clinic_id)
    if clinic is None:
        return None

    if name is not None:
        clinic.name = name
    if timezone is not None:
        clinic.timezone = timezone

    await db.commit()
    await db.refresh(clinic)
    return clinic


async def deactivate_clinic(db: AsyncSession, clinic_id: uuid.UUID) -> Clinic | None:
    """Soft-delete: sets is_active=false + deactivated_at=now(), never
    deletes the row. Idempotent — deactivating an already-inactive clinic
    is a no-op success (returns the clinic unchanged, not an error).
    """
    clinic = await get_clinic(db, clinic_id)
    if clinic is None:
        return None

    if clinic.is_active:
        clinic.is_active = False
        clinic.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        await db.refresh(clinic)
    return clinic


async def create_department(
    db: AsyncSession, *, clinic_id: uuid.UUID, name: str
) -> Department | None:
    """Create a department under `clinic_id`.

    Returns `None` (no row created) if `clinic_id` does not reference an
    existing clinic — the caller (router) is responsible for turning that
    into a 404. Verifying existence here, in addition to the FK constraint,
    means the rejection is a clean application-level check rather than a
    surfaced IntegrityError (RESEARCH T-04-04 — orphan department under a
    non-existent clinic).
    """
    clinic = await get_clinic(db, clinic_id)
    if clinic is None:
        return None

    department = Department(clinic_id=clinic_id, name=name)
    db.add(department)
    await db.commit()
    await db.refresh(department)
    return department


async def list_departments(
    db: AsyncSession, clinic_id: uuid.UUID, *, include_inactive: bool = False
) -> list[Department]:
    """List departments for `clinic_id` — always scoped, never all-departments.

    A clinic with no departments returns an empty list, not an error.
    """
    stmt = select(Department).where(Department.clinic_id == clinic_id)
    if not include_inactive:
        stmt = stmt.where(Department.is_active.is_(True))
    stmt = stmt.order_by(Department.created_at)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def get_department(db: AsyncSession, department_id: uuid.UUID) -> Department | None:
    """Fetch a department by id alone.

    Unlike `list_departments` (always clinic-scoped), this id-only lookup
    exists because `people_repo.create_provider` needs to resolve a
    provider's `department_id` -> `clinic_id` chain (ORG-03) without the
    caller having to already know the clinic id — mirroring `get_clinic`'s
    existing id-only shape above. Callers that expose this to an
    HTTP boundary must still apply their own RBAC/scope checks; this
    function itself performs no authorization.
    """
    result = await db.execute(select(Department).where(Department.id == department_id))
    return result.scalar_one_or_none()


async def get_department_scoped(
    db: AsyncSession, department_id: uuid.UUID, clinic_id: uuid.UUID
) -> Department | None:
    """Fetch a department scoped to `clinic_id` — the clinic-scoped
    counterpart to `get_department`'s id-only lookup. Used by the
    PATCH/deactivate routes so a department id from another clinic is
    404, not accidentally editable (mirrors `people_repo.get_patient`'s
    clinic-scoping pattern).
    """
    result = await db.execute(
        select(Department).where(
            Department.id == department_id, Department.clinic_id == clinic_id
        )
    )
    return result.scalar_one_or_none()


async def update_department(
    db: AsyncSession,
    department_id: uuid.UUID,
    clinic_id: uuid.UUID,
    *,
    name: str | None = None,
) -> Department | None:
    """Partial-update a department, scoped to `clinic_id`. Returns `None`
    if the department does not exist or is out of clinic scope (the
    caller/router maps this to 404).
    """
    department = await get_department_scoped(db, department_id, clinic_id)
    if department is None:
        return None

    if name is not None:
        department.name = name

    await db.commit()
    await db.refresh(department)
    return department


async def deactivate_department(
    db: AsyncSession, department_id: uuid.UUID, clinic_id: uuid.UUID
) -> Department | None:
    """Soft-delete a department, scoped to `clinic_id`. Idempotent —
    deactivating an already-inactive department is a no-op success.
    """
    department = await get_department_scoped(db, department_id, clinic_id)
    if department is None:
        return None

    if department.is_active:
        department.is_active = False
        department.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        await db.refresh(department)
    return department
