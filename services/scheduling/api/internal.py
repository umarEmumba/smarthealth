"""Scheduling's internal appointment lookup for Visit (plan 02-10, D-06, D-15).

    GET /internal/appointments/{appointment_id}?clinic_id= -> AppointmentRef

Visit calls this at check-in to learn whether the appointment is `confirmed`
and whose it is (patient, provider). Design rules (T-02-53):
  * Not routed by the gateway (no `/internal` prefix in its table) and excluded
    from OpenAPI (`include_in_schema=False`).
  * Callers: a Visit SERVICE token whose `aud` is `scheduling`, or ADMIN /
    FRONT_DESK user tokens (`require_internal_caller`). Any other service is
    403; a token for another audience, or none, is 401.
  * `clinic_id` is REQUIRED (422 without it): the caller states the clinic it
    acts for and Scheduling re-applies it. A missing appointment, one in
    another clinic, and (for user callers) one outside the caller's own
    clinic all get the IDENTICAL 404.
  * Ids, slot times and status only (`AppointmentRef`): never the coverage
    reference. Reads are not audited; the log line carries ids and outcome.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.scheduling.db import get_db
from services.scheduling.domain.schemas import AppointmentRef
from services.scheduling.repository import appointment_repo

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


@router.get("/appointments/{appointment_id}", response_model=AppointmentRef)
async def lookup_appointment(
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID = Query(...),
    caller: InternalCaller = Depends(
        require_internal_caller(
            Role.ADMIN,
            Role.FRONT_DESK,
            audience="scheduling",
            allowed_services=frozenset({"visit"}),
        )
    ),
    db: AsyncSession = Depends(get_db),
) -> AppointmentRef:
    appointment = await appointment_repo.get_appointment(db, appointment_id)
    if (
        appointment is None
        or appointment.clinic_id != clinic_id
        or not clinic_scope_permits(caller, appointment.clinic_id)
    ):
        log.info(
            "internal.appointment_lookup", appointment_id=str(appointment_id), outcome="not_found"
        )
        raise _not_found()

    log.info(
        "internal.appointment_lookup",
        appointment_id=str(appointment.id),
        outcome="found",
        status=appointment.status,
    )
    return AppointmentRef(
        id=appointment.id,
        clinic_id=appointment.clinic_id,
        patient_id=appointment.patient_id,
        provider_id=appointment.provider_id,
        slot_id=appointment.slot_id,
        slot_start=appointment.slot_start,
        slot_end=appointment.slot_end,
        status=appointment.status,
    )
