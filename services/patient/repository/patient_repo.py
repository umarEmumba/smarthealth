"""Async repository for patient profiles — the highest-PHI-density repository.

Every method requires `clinic_id` explicitly (RESEARCH Pitfall 1 — no id-only
patient lookup path exists, structurally closing cross-clinic enumeration, same
pattern as Organization's department methods).

PHI note: this module reads/writes `Patient.full_name`/`date_of_birth`/`phone`/
`address` (PHI-bearing columns per models.py). Nothing in this module ever logs
those values — callers (routers) are responsible for passing only field NAMES
to `write_audit`, never PHI values.

Behaviour is carried over unchanged from the Phase 1 monolith (reference commit
7ce4490); only the import of `Patient` changed.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone as dt_timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.patient.repository.models import Patient


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

    `user_id` is optional (PPL-01) — a front-desk-created profile need not have
    a linked auth account.
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
    """Fetch a patient scoped to `clinic_id` — no id-only lookup path exists
    anywhere in this repository (RESEARCH Pitfall 1 / T-05-03).
    """
    result = await db.execute(
        select(Patient).where(Patient.id == patient_id, Patient.clinic_id == clinic_id)
    )
    return result.scalar_one_or_none()


async def get_patient_by_user(
    db: AsyncSession, user_id: uuid.UUID, clinic_id: uuid.UUID
) -> Patient | None:
    """Fetch the patient profile linked to login account `user_id`, scoped to
    `clinic_id` — like `get_patient`, there is no unscoped variant.
    """
    result = await db.execute(
        select(Patient).where(Patient.user_id == user_id, Patient.clinic_id == clinic_id)
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
    """Partial-update a patient, scoped to `clinic_id` (no id-only lookup path —
    same as `get_patient`). Returns `None` if the patient does not exist or is
    out of clinic scope.
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
    """Soft-delete a patient, scoped to `clinic_id`. Idempotent — deactivating
    an already-inactive patient is a no-op success.
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
