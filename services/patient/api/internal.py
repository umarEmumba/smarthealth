"""Patient's internal reference lookups for other services (plan 02-03, D-06, D-18).

Scheduling's booking validation (D-09 step 1) confirms that a patient exists, is
active and belongs to the clinic being booked:

    GET /internal/patients/{patient_id}?clinic_id=      -> {id, clinic_id, user_id, is_active}
    GET /internal/patients/by-user/{user_id}?clinic_id= -> same shape

Design rules (STRIDE T-02-13, T-02-14, T-02-16):
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated callers only: ADMIN / FRONT_DESK user tokens, or a Scheduling
    service token whose `aud` is `patient` (`require_internal_caller`). Any other
    service is 403; no token, or a service token for another audience, is 401.
  * `clinic_id` is REQUIRED: a service token carries no clinic claim, so clinic
    scope is re-applied from the explicit parameter (D-06). User callers must
    additionally be in scope for that clinic (`clinic_scope_permits`).
  * Missing, out-of-clinic and out-of-scope all return the IDENTICAL 404, so
    existence cannot be probed across clinics.
  * INACTIVE patients are REPORTED (`is_active: false`), never hidden.
  * The answer carries ids and the active flag only — never any profile field.
    Reads are not audited (existing convention); log lines carry ids and outcome.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.patient.db import get_db
from services.patient.repository import patient_repo
from services.patient.repository.models import Patient

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


class PatientRef(BaseModel):
    id: uuid.UUID
    clinic_id: uuid.UUID
    user_id: uuid.UUID | None = None
    is_active: bool


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _to_ref(patient: Patient) -> PatientRef:
    return PatientRef(
        id=patient.id,
        clinic_id=patient.clinic_id,
        user_id=patient.user_id,
        is_active=patient.is_active,
    )


# Declared BEFORE `/patients/{patient_id}` so the literal `by-user` segment wins.
@router.get("/patients/by-user/{user_id}", response_model=PatientRef)
async def lookup_patient_by_user(
    user_id: uuid.UUID,
    clinic_id: uuid.UUID = Query(...),
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, Role.FRONT_DESK, audience="patient", allowed_services=frozenset({"scheduling"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> PatientRef:
    patient = None
    if clinic_scope_permits(caller, clinic_id):
        patient = await patient_repo.get_patient_by_user(db, user_id, clinic_id)
    if patient is None:
        log.info(
            "internal.patient_lookup", by="user", user_id=str(user_id), clinic_id=str(clinic_id), outcome="not_found"
        )
        raise _not_found()

    log.info("internal.patient_lookup", by="user", patient_id=str(patient.id), clinic_id=str(clinic_id), outcome="found")
    return _to_ref(patient)


@router.get("/patients/{patient_id}", response_model=PatientRef)
async def lookup_patient(
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID = Query(...),
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, Role.FRONT_DESK, audience="patient", allowed_services=frozenset({"scheduling"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> PatientRef:
    patient = None
    if clinic_scope_permits(caller, clinic_id):
        patient = await patient_repo.get_patient(db, patient_id, clinic_id)
    if patient is None:
        log.info(
            "internal.patient_lookup", by="id", patient_id=str(patient_id), clinic_id=str(clinic_id), outcome="not_found"
        )
        raise _not_found()

    log.info("internal.patient_lookup", by="id", patient_id=str(patient.id), clinic_id=str(clinic_id), outcome="found")
    return _to_ref(patient)
