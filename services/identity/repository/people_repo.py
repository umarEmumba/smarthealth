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
from datetime import date, datetime, time, timezone as dt_timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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


async def list_patients(
    db: AsyncSession, clinic_id: uuid.UUID, *, include_inactive: bool = False
) -> list[Patient]:
    """List patients for `clinic_id` — always scoped, never all-patients."""
    stmt = select(Patient).where(Patient.clinic_id == clinic_id)
    if not include_inactive:
        stmt = stmt.where(Patient.is_active.is_(True))
    stmt = stmt.order_by(Patient.created_at)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def update_patient(
    db: AsyncSession,
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID,
    *,
    full_name: str | None = None,
    date_of_birth: date | None = None,
    phone: str | None = None,
    address: str | None = None,
) -> Patient | None:
    """Partial-update a patient, scoped to `clinic_id` (no id-only lookup
    path — RESEARCH Pitfall 1, same as `get_patient`). Returns `None` if
    the patient does not exist or is out of clinic scope.
    """
    patient = await get_patient(db, patient_id, clinic_id)
    if patient is None:
        return None

    if full_name is not None:
        patient.full_name = full_name
    if date_of_birth is not None:
        patient.date_of_birth = date_of_birth
    if phone is not None:
        patient.phone = phone
    if address is not None:
        patient.address = address

    await db.commit()
    await db.refresh(patient)
    return patient


async def deactivate_patient(
    db: AsyncSession, patient_id: uuid.UUID, clinic_id: uuid.UUID
) -> Patient | None:
    """Soft-delete a patient, scoped to `clinic_id`. Idempotent —
    deactivating an already-inactive patient is a no-op success.
    """
    patient = await get_patient(db, patient_id, clinic_id)
    if patient is None:
        return None

    if patient.is_active:
        patient.is_active = False
        patient.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        await db.refresh(patient)
    return patient


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

    # Re-select with `specialties` eagerly loaded (selectinload) instead of
    # touching the lazy relationship attribute post-commit — an async
    # lazy-load triggered outside the greenlet context SQLAlchemy's async
    # ORM uses to bridge to its sync core raises
    # `MissingGreenlet: greenlet_spawn has not been called` (confirmed via
    # this plan's own first test run). Eager-loading via a fresh SELECT is
    # the correct async-ORM pattern, not a workaround.
    return await get_provider(db, provider.id)


async def get_provider(db: AsyncSession, provider_id: uuid.UUID) -> Provider | None:
    result = await db.execute(
        select(Provider)
        .where(Provider.id == provider_id)
        .options(selectinload(Provider.specialties))
    )
    return result.scalar_one_or_none()


async def list_providers(
    db: AsyncSession,
    department_id: uuid.UUID | None = None,
    *,
    include_inactive: bool = False,
) -> list[Provider]:
    """List providers, optionally scoped to a single department.

    `department_id=None` lists across all departments — reserved for
    admin-only call sites (the provider router guards writes to ADMIN;
    a future cross-department admin listing view could use this, but
    this plan's router always passes an explicit `department_id`).
    """
    stmt = select(Provider).options(selectinload(Provider.specialties)).order_by(
        Provider.created_at
    )
    if department_id is not None:
        stmt = stmt.where(Provider.department_id == department_id)
    if not include_inactive:
        stmt = stmt.where(Provider.is_active.is_(True))
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def update_provider(
    db: AsyncSession,
    provider_id: uuid.UUID,
    *,
    full_name: str | None = None,
    specialty_names: list[str] | None = None,
) -> Provider | None:
    """Partial-update a provider. `specialty_names`, when supplied, REPLACES
    the provider's full specialty set (get-or-create by name, same as
    `create_provider` — no separate specialty-management endpoint exists).
    Returns `None` if `provider_id` does not exist.
    """
    provider = await get_provider(db, provider_id)
    if provider is None:
        return None

    if full_name is not None:
        provider.full_name = full_name
    if specialty_names is not None:
        provider.specialties = [
            await get_or_create_specialty(db, name) for name in specialty_names
        ]

    await db.commit()

    # Re-select with `specialties` eagerly loaded — see create_provider's
    # docstring for why a post-commit lazy-load is unsafe here.
    return await get_provider(db, provider.id)


async def deactivate_provider(db: AsyncSession, provider_id: uuid.UUID) -> Provider | None:
    """Soft-delete a provider. Idempotent — deactivating an already-inactive
    provider is a no-op success.
    """
    provider = await get_provider(db, provider_id)
    if provider is None:
        return None

    if provider.is_active:
        provider.is_active = False
        provider.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        provider = await get_provider(db, provider_id)
    return provider


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
    db: AsyncSession, provider_id: uuid.UUID, *, include_inactive: bool = False
) -> list[AvailabilityTemplate]:
    stmt = select(AvailabilityTemplate).where(AvailabilityTemplate.provider_id == provider_id)
    if not include_inactive:
        stmt = stmt.where(AvailabilityTemplate.is_active.is_(True))
    stmt = stmt.order_by(AvailabilityTemplate.day_of_week, AvailabilityTemplate.start_time)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def get_availability_template(
    db: AsyncSession, template_id: uuid.UUID, provider_id: uuid.UUID
) -> AvailabilityTemplate | None:
    """Fetch a single availability template scoped to `provider_id` — the
    provider-ownership PATCH/deactivate routes need to load-then-check
    ownership on the parent provider, so this stays id+provider scoped
    rather than id-only (mirrors the clinic/department scoping pattern).
    """
    result = await db.execute(
        select(AvailabilityTemplate).where(
            AvailabilityTemplate.id == template_id,
            AvailabilityTemplate.provider_id == provider_id,
        )
    )
    return result.scalar_one_or_none()


async def update_availability_template(
    db: AsyncSession,
    template_id: uuid.UUID,
    provider_id: uuid.UUID,
    *,
    day_of_week: int | None = None,
    start_time: time | None = None,
    end_time: time | None = None,
    slot_duration_minutes: int | None = None,
    effective_from: date | None = None,
    effective_to: date | None = None,
) -> AvailabilityTemplate | None:
    """Partial-update an availability template, scoped to `provider_id`.

    Window-consistency validation (end after start, effective_from <=
    effective_to) against the RESULTING row (existing values merged with
    the supplied partial update) happens in the router BEFORE this is
    called — mirrors `create_availability_template`'s trust-the-caller
    contract. Returns `None` if the template does not exist or does not
    belong to `provider_id`.
    """
    template = await get_availability_template(db, template_id, provider_id)
    if template is None:
        return None

    if day_of_week is not None:
        template.day_of_week = day_of_week
    if start_time is not None:
        template.start_time = start_time
    if end_time is not None:
        template.end_time = end_time
    if slot_duration_minutes is not None:
        template.slot_duration_minutes = slot_duration_minutes
    if effective_from is not None:
        template.effective_from = effective_from
    if effective_to is not None:
        template.effective_to = effective_to

    await db.commit()
    await db.refresh(template)
    return template


async def deactivate_availability_template(
    db: AsyncSession, template_id: uuid.UUID, provider_id: uuid.UUID
) -> AvailabilityTemplate | None:
    """Soft-delete an availability template, scoped to `provider_id`.
    Idempotent — deactivating an already-inactive template is a no-op
    success.
    """
    template = await get_availability_template(db, template_id, provider_id)
    if template is None:
        return None

    if template.is_active:
        template.is_active = False
        template.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        await db.refresh(template)
    return template
