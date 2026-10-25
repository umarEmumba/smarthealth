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
`?include_inactive=true` includes them. The list is paginated (C5): `limit`
1..200 (default 50), `offset`, a stable `(created_at, id)` order and the grand
total in the `X-Total-Count` header.

Plan 02.1-12 (C4) adds the one patient-facing read, `GET /api/patients/me`: a
logged-in PATIENT reads their own profile. The route has no id parameter, so
there is no way to name another patient: the account comes from the verified
token's `sub` and the clinic from its clinic claim, and the profile is found
through the same by-user lookup Scheduling uses. A missing clinic claim, an
account with no linked profile, a deactivated profile and a profile in another
clinic are all the same 404, so the route never says which of them it was. It
is declared BEFORE `/{patient_id}` so the literal segment wins, returns the
fields the staff read already returns, and logs the patient id only.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.current_user import AuthenticatedUser, user_id_as_uuid
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from libs.db.mongo import get_mongo
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger
from libs.pagination import TOTAL_COUNT_HEADER, Page, page_params
from libs.service_client import (
    ReferencedResourceInvalid,
    UpstreamServiceUnavailable,
    lookup_clinic,
    reference_error_detail,
    validate_clinic,
    validate_user,
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


async def _validate_references_or_refuse(
    http: httpx.AsyncClient,
    mongo: AsyncMongoClient,
    user: AuthenticatedUser,
    payload: PatientCreate,
) -> None:
    """D-05: check both cross-service references BEFORE any write.

    `clinic_id` belongs to Organization and, when supplied, `user_id` belongs to
    Identity. A missing/inactive/out-of-scope target is a 422, an owner that
    cannot answer cleanly is a 503; either way the refusal is audited as a
    failure and nothing has been written yet, so there is never a half-created
    profile. The calls carry the ids only — never a PHI field (T-01.1-25).
    """
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

    if payload.user_id is None:
        return
    try:
        await validate_user(http, payload.user_id, bearer_token=user.raw_token)
    except ReferencedResourceInvalid as exc:
        await _audit_create_refused(mongo, user, payload.clinic_id)
        raise HTTPException(status_code=422, detail=reference_error_detail(exc)) from exc
    except UpstreamServiceUnavailable as exc:
        await _audit_create_refused(mongo, user, payload.clinic_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="identity service unavailable",
        ) from exc


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

    await _validate_references_or_refuse(http, mongo, user, payload)

    try:
        patient = await patient_repo.create_patient(
            db,
            clinic_id=payload.clinic_id,
            full_name=payload.full_name,
            date_of_birth=payload.date_of_birth,
            phone=payload.phone,
            address=payload.address,
            user_id=payload.user_id,
        )
    except IntegrityError as exc:
        # The only constraint a validated create can trip is the one-profile-per-
        # account unique on user_id (two creates racing for the same account).
        await db.rollback()
        if payload.user_id is None:
            raise
        await _audit_create_refused(mongo, user, payload.clinic_id)
        log.info("patient.create.account_already_linked", clinic_id=str(payload.clinic_id))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="account is already linked to a patient profile",
        ) from exc

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
    response: Response,
    include_inactive: bool = False,
    page: Page = Depends(page_params),
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

    patients = await patient_repo.list_patients(
        db, clinic_id, include_inactive=include_inactive, limit=page.limit, offset=page.offset
    )
    total = await patient_repo.count_patients(db, clinic_id, include_inactive=include_inactive)
    response.headers[TOTAL_COUNT_HEADER] = str(total)
    return [_to_out(p) for p in patients]


def _own_profile_not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="patient profile not found")


# Declared BEFORE `/{patient_id}` so the literal `me` segment is matched first.
@router.get("/me", response_model=PatientOut)
async def get_my_profile(
    user: AuthenticatedUser = Depends(require_role(Role.PATIENT)),
    db: AsyncSession = Depends(get_db),
) -> PatientOut:
    """The caller's own patient profile (C4). Identity comes only from the token."""
    if user.clinic_id is None:
        raise _own_profile_not_found()
    try:
        clinic_id = uuid.UUID(user.clinic_id)
        user_id = user_id_as_uuid(user)
    except ValueError:
        raise _own_profile_not_found() from None

    patient = await patient_repo.get_patient_by_user(db, user_id, clinic_id)
    if patient is None or not patient.is_active:
        raise _own_profile_not_found()

    # Ids only: never the name, date of birth, phone or address.
    log.info("patient.me", patient_id=str(patient.id))
    return _to_out(patient)


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
