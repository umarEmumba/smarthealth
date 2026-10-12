"""Patient CRUD router (PPL-01) — front-desk/admin only, audited, PHI-safe.

The highest-PHI-density router in Phase 1. `PatientCreate` carries
operational-profile PHI fields (full_name/date_of_birth/phone/address).
NONE of those values are ever passed to `write_audit`'s `changed_fields`
or to any log/span call in this module — only field NAMES and
identifiers (patient_id/clinic_id) ever appear in audit/log/trace output
(RESEARCH PHI-Safe Structured Logging section; T-05-01).

A patient profile (this router) is distinct from a patient's auth
identity (`users`, IAM-01) — `patients.user_id` is nullable (RESEARCH
Open Question 1). This route is guarded to FRONT_DESK/ADMIN only; a
patient-role token cannot self-register a profile via this staff route
(T-05-02).

Every patient method in `patient_repo.py` requires `clinic_id` explicitly
— there is no id-only patient lookup path (RESEARCH Pitfall 1 / T-05-03).

Cross-service references (D-05): `patients.clinic_id` is owned by Organization
and is validated at write time through the shared reference client before any
row is written (missing/inactive -> 422, owner cannot answer cleanly -> 503, in
both cases NOTHING is written). Those internal calls carry ids only — never a
PHI value (T-01.1-25). Reads return this service's own rows and never enrich
them with another service's data (D-07).

Gap-closure Plan 01-07 adds `PATCH /api/patients/{patient_id}` (partial
update, PHI-safe audit of field NAMES only) and `POST
/api/patients/{patient_id}/deactivate` (soft-delete, NEVER a row delete)
— both front-desk/admin, clinic-scoped identically to the create/read
routes above. `GET` list excludes inactive patients by default;
`?include_inactive=true` includes them.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from libs.db.mongo import get_mongo
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger
from libs.service_client import (
    ReferencedResourceInvalid,
    UpstreamServiceUnavailable,
    lookup_clinic,
    reference_error_detail,
    validate_clinic,
)
from services.patient.db import get_db
from services.patient.domain.schemas import PatientUpdate
from services.patient.repository import patient_repo

router = APIRouter(prefix="/api/patients", tags=["patients"])

log = get_logger(__name__)

_PATIENT_PROFILE_FIELDS = ["full_name", "date_of_birth", "phone", "address"]


class PatientCreate(BaseModel):
    """PHI-bearing request payload — never log/audit these field VALUES."""

    clinic_id: uuid.UUID
    full_name: str = Field(min_length=1, max_length=255)
    date_of_birth: date
    phone: str | None = Field(default=None, max_length=32)
    address: str | None = Field(default=None, max_length=2000)
    user_id: uuid.UUID | None = None


class PatientOut(BaseModel):
    id: uuid.UUID
    clinic_id: uuid.UUID
    full_name: str
    date_of_birth: date
    phone: str | None = None
    address: str | None = None
    user_id: uuid.UUID | None = None
    is_active: bool
    deactivated_at: datetime | None = None


def _assert_clinic_scope(user: AuthenticatedUser, clinic_id: uuid.UUID) -> None:
    """WR-02: a non-ADMIN caller may only act on their OWN clinic_id.

    `AuthenticatedUser.clinic_id` is carried in the access-token claims.
    Without this check, a FRONT_DESK user scoped to Clinic A could pass
    Clinic B's `clinic_id` and read/create patient PHI there — the
    repository-level "no id-only lookup" mitigation only closes
    cross-clinic *enumeration by id alone*, not this cross-clinic access
    path when the caller already knows another clinic's UUID.
    """
    if user.role == Role.ADMIN:
        return
    if user.clinic_id is None or uuid.UUID(user.clinic_id) != clinic_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="cannot access another clinic's patients",
        )


def _to_out(patient) -> PatientOut:
    return PatientOut(
        id=patient.id,
        clinic_id=patient.clinic_id,
        full_name=patient.full_name,
        date_of_birth=patient.date_of_birth,
        phone=patient.phone,
        address=patient.address,
        user_id=patient.user_id,
        is_active=patient.is_active,
        deactivated_at=patient.deactivated_at,
    )


async def _audit_create_refused(
    mongo: AsyncMongoClient, user: AuthenticatedUser, clinic_id: uuid.UUID
) -> None:
    """Failure audit for a create refused by a reference check (no PHI, no field values)."""
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="patient.create",
        entity_type="patient",
        entity_id="unknown",
        clinic_id=str(clinic_id),
        outcome="failure",
    )


@router.post("", response_model=PatientOut, status_code=status.HTTP_201_CREATED)
async def create_patient(
    payload: PatientCreate,
    user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
) -> PatientOut:
    _assert_clinic_scope(user, payload.clinic_id)

    # D-05: the clinic belongs to Organization. Validate it BEFORE any write so a
    # refused create leaves no row behind. Only the clinic id leaves this service.
    try:
        await validate_clinic(http, payload.clinic_id, bearer_token=user.raw_token)
    except ReferencedResourceInvalid as exc:
        await _audit_create_refused(mongo, user, payload.clinic_id)
        raise HTTPException(status_code=422, detail=reference_error_detail(exc)) from exc
    except UpstreamServiceUnavailable as exc:
        await _audit_create_refused(mongo, user, payload.clinic_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="organization service unavailable",
        ) from exc

    patient = await patient_repo.create_patient(
        db,
        clinic_id=payload.clinic_id,
        full_name=payload.full_name,
        date_of_birth=payload.date_of_birth,
        phone=payload.phone,
        address=payload.address,
        user_id=payload.user_id,
    )

    # PHI-safe: only field NAMES are recorded, never the PHI values above.
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="patient.create",
        entity_type="patient",
        entity_id=str(patient.id),
        clinic_id=str(patient.clinic_id),
        outcome="success",
        changed_fields=_PATIENT_PROFILE_FIELDS,
    )
    log.info("patient.create.success", patient_id=str(patient.id), clinic_id=str(patient.clinic_id))

    return _to_out(patient)


@router.get("", response_model=list[PatientOut])
async def list_patients(
    clinic_id: uuid.UUID,
    include_inactive: bool = False,
    user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
) -> list[PatientOut]:
    _assert_clinic_scope(user, clinic_id)

    # WR-01: consistent with list_departments — distinguish "clinic exists,
    # 0 patients" from "clinic_id doesn't exist" via an explicit 404. The clinic
    # belongs to Organization, so it is asked (id only). This only validates the
    # query's scope parameter; the answer is never merged into the response (D-07),
    # and an inactive clinic still lists its patients (D-06, no cascade on reads).
    try:
        clinic = await lookup_clinic(http, clinic_id, bearer_token=user.raw_token)
    except UpstreamServiceUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="organization service unavailable",
        ) from exc
    if clinic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="clinic not found")

    patients = await patient_repo.list_patients(db, clinic_id, include_inactive=include_inactive)
    return [_to_out(p) for p in patients]


@router.get("/{patient_id}", response_model=PatientOut)
async def get_patient(
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> PatientOut:
    _assert_clinic_scope(user, clinic_id)

    patient = await patient_repo.get_patient(db, patient_id, clinic_id)
    if patient is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="patient not found")
    return _to_out(patient)


@router.patch("/{patient_id}", response_model=PatientOut)
async def update_patient(
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID,
    payload: PatientUpdate,
    user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> PatientOut:
    _assert_clinic_scope(user, clinic_id)

    patient = await patient_repo.update_patient(
        db,
        patient_id,
        clinic_id,
        full_name=payload.full_name,
        date_of_birth=payload.date_of_birth,
        phone=payload.phone,
        address=payload.address,
    )

    if patient is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="patient.update",
            entity_type="patient",
            entity_id=str(patient_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info("patient.update.not_found", patient_id=str(patient_id), clinic_id=str(clinic_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="patient not found")

    # PHI-safe: only field NAMES are recorded, never the PHI values above.
    changed_fields = list(payload.model_fields_set)
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="patient.update",
        entity_type="patient",
        entity_id=str(patient.id),
        clinic_id=str(patient.clinic_id),
        outcome="success",
        changed_fields=changed_fields,
    )
    log.info("patient.update.success", patient_id=str(patient.id), clinic_id=str(patient.clinic_id))

    return _to_out(patient)


@router.post("/{patient_id}/deactivate", response_model=PatientOut)
async def deactivate_patient(
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> PatientOut:
    _assert_clinic_scope(user, clinic_id)

    patient = await patient_repo.deactivate_patient(db, patient_id, clinic_id)

    if patient is None:
        await write_audit(
            mongo,
            actor_id=user.sub,
            actor_role=user.role.value,
            action="patient.deactivate",
            entity_type="patient",
            entity_id=str(patient_id),
            clinic_id=str(clinic_id),
            outcome="failure",
        )
        log.info(
            "patient.deactivate.not_found", patient_id=str(patient_id), clinic_id=str(clinic_id)
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="patient not found")

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="patient.deactivate",
        entity_type="patient",
        entity_id=str(patient.id),
        clinic_id=str(patient.clinic_id),
        outcome="success",
    )
    log.info("patient.deactivate.success", patient_id=str(patient.id), clinic_id=str(patient.clinic_id))

    return _to_out(patient)
