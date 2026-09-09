"""Async repository for patients/providers/specialties/availability — the
highest-PHI-density repository in Phase 1.

Every patient method requires `clinic_id` explicitly (RESEARCH Pitfall 1 —
no id-only patient lookup path exists, structurally closing cross-clinic
enumeration, same pattern as `org_repo.py`'s department methods). Every
provider method is reached through its `department_id` (ORG-03 scope
chain: provider -> department -> clinic).

PHI note: this module reads/writes `Patient.full_name`/`date_of_birth`/
`phone`/`address` (PHI-bearing columns per models.py). Nothing in this
module ever logs those values — callers (routers) are responsible for
passing only field NAMES to `write_audit`, never PHI values (RESEARCH PHI
boundary, enforced by discipline + tests, same convention as Plan 03).
"""

from __future__ import annotations

import uuid
from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.identity.repository.models import (
    AvailabilityTemplate,
    Patient,
    Provider,
    Specialty,
)


async def create_patient(
    db: AsyncSession,
    *,
    clinic_id: uuid.UUID,
    full_name: str,
    date_of_birth: date,
    phone: str | None = None,
    address: str | None = None,
    user_id: uuid.UUID | None = None,
) -> Patient:
    """Create a patient operational profile under `clinic_id`.

    `user_id` is optional (PPL-01 / RESEARCH Open Question 1) — a
    front-desk-created profile need not have a linked auth account.
    """
    patient = Patient(
        clinic_id=clinic_id,
        full_name=full_name,
        date_of_birth=date_of_birth,
        phone=phone,
        address=address,
        user_id=user_id,
    )
    db.add(patient)
    await db.commit()
    await db.refresh(patient)
    return patient


async def get_patient(
    db: AsyncSession, patient_id: uuid.UUID, clinic_id: uuid.UUID
) -> Patient | None:
    """Fetch a patient scoped to `clinic_id` — no id-only lookup path
    exists anywhere in this repository (RESEARCH Pitfall 1 / T-05-03).
    """
    result = await db.execute(
        select(Patient).where(Patient.id == patient_id, Patient.clinic_id == clinic_id)
    )
    return result.scalar_one_or_none()


async def list_patients(db: AsyncSession, clinic_id: uuid.UUID) -> list[Patient]:
    """List patients for `clinic_id` — always scoped, never all-patients."""
    result = await db.execute(
        select(Patient).where(Patient.clinic_id == clinic_id).order_by(Patient.created_at)
    )
    return list(result.scalars().all())


async def get_specialties_by_ids(
    db: AsyncSession, specialty_ids: list[uuid.UUID]
) -> list[Specialty]:
    if not specialty_ids:
        return []
    result = await db.execute(select(Specialty).where(Specialty.id.in_(specialty_ids)))
    return list(result.scalars().all())


async def get_or_create_specialty(db: AsyncSession, name: str) -> Specialty:
    """Look up a specialty by name, creating it if it doesn't exist yet.

    Specialties have no admin-facing "manage specialties" endpoint in this
    plan's scope (PPL-02 only requires a provider to carry >=1 specialty) —
    get-or-create by name keeps provider registration self-contained
    without a separate specialty-management surface.
    """
    result = await db.execute(select(Specialty).where(Specialty.name == name))
    specialty = result.scalar_one_or_none()
    if specialty is not None:
        return specialty

    specialty = Specialty(name=name)
    db.add(specialty)
    await db.commit()
    await db.refresh(specialty)
    return specialty


async def create_provider(
    db: AsyncSession,
    *,
    department_id: uuid.UUID,
    full_name: str,
    specialty_names: list[str],
    user_id: uuid.UUID | None = None,
) -> Provider | None:
    """Create a provider under `department_id`, attaching one-or-more
    specialties by name (get-or-create, see `get_or_create_specialty`).

    Returns `None` if `department_id` does not reference an existing
    department — the caller (router) maps this to 404, mirroring
    `org_repo.create_department`'s missing-parent pattern.
    """
    from services.identity.repository.org_repo import get_department  # noqa: PLC0415

    department = await get_department(db, department_id)
    if department is None:
        return None

    specialties = [await get_or_create_specialty(db, name) for name in specialty_names]

    provider = Provider(
        department_id=department_id,
        full_name=full_name,
        user_id=user_id,
        specialties=specialties,
    )
    db.add(provider)
    await db.commit()
    await db.refresh(provider, attribute_names=["specialties"])
    return provider


async def get_provider(db: AsyncSession, provider_id: uuid.UUID) -> Provider | None:
    result = await db.execute(
        select(Provider).where(Provider.id == provider_id)
    )
    provider = result.scalar_one_or_none()
    if provider is not None:
        # Force-load specialties within this session (avoids a lazy-load
        # attempt outside an async context on the returned ORM object).
        _ = provider.specialties
    return provider


async def list_providers(
    db: AsyncSession, department_id: uuid.UUID | None = None
) -> list[Provider]:
    """List providers, optionally scoped to a single department.

    `department_id=None` lists across all departments — reserved for
    admin-only call sites (the provider router guards writes to ADMIN;
    a future cross-department admin listing view could use this, but
    this plan's router always passes an explicit `department_id`).
    """
    stmt = select(Provider).order_by(Provider.created_at)
    if department_id is not None:
        stmt = select(Provider).where(Provider.department_id == department_id).order_by(
            Provider.created_at
        )
    result = await db.execute(stmt)
    providers = list(result.scalars().all())
    for provider in providers:
        _ = provider.specialties
    return providers


async def create_availability_template(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    department_id: uuid.UUID,
    day_of_week: int,
    start_time,
    end_time,
    slot_duration_minutes: int,
    effective_from: date,
    effective_to: date | None = None,
) -> AvailabilityTemplate:
    """Persist a weekly availability template row.

    Validation (day_of_week range, end_time > start_time, positive
    duration, effective_from <= effective_to) happens in the router's
    Pydantic schema BEFORE this is called — this function trusts its
    caller and only persists (RESEARCH Decision 2: templates only, no
    concrete provider_slots rows are generated here).
    """
    template = AvailabilityTemplate(
        provider_id=provider_id,
        department_id=department_id,
        day_of_week=day_of_week,
        start_time=start_time,
        end_time=end_time,
        slot_duration_minutes=slot_duration_minutes,
        effective_from=effective_from,
        effective_to=effective_to,
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return template


async def list_availability(
    db: AsyncSession, provider_id: uuid.UUID
) -> list[AvailabilityTemplate]:
    result = await db.execute(
        select(AvailabilityTemplate)
        .where(AvailabilityTemplate.provider_id == provider_id)
        .order_by(AvailabilityTemplate.day_of_week, AvailabilityTemplate.start_time)
    )
    return list(result.scalars().all())
