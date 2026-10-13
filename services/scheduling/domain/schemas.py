"""Scheduling's public request/response models (plans 02-04, 02-06).

Slot payloads carry ids and UTC instants only: no names, no PHI, and no data
enriched from another service (Phase 1.1 D-07).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field


class SlotGenerateRequest(BaseModel):
    provider_id: uuid.UUID
    # Inclusive local dates, read in the provider's clinic timezone.
    from_date: date
    to_date: date


class SlotGenerateResponse(BaseModel):
    # Rows actually inserted / candidate slots the templates produced. A
    # repeated request reports created == 0.
    created: int
    considered: int


class SlotOut(BaseModel):
    id: uuid.UUID
    provider_id: uuid.UUID
    clinic_id: uuid.UUID
    department_id: uuid.UUID
    start_time: datetime
    end_time: datetime
    status: str


# --- plan 02-06: booking ------------------------------------------------------


class BookingRequest(BaseModel):
    slot_id: uuid.UUID
    # Staff must name the patient; a PATIENT books for themselves (resolved
    # from their login) and may only repeat their own id here.
    patient_id: uuid.UUID | None = None
    # Opaque billing reference: stored on the appointment for the billing
    # activity, never logged, audited, echoed back or put in workflow I/O.
    coverage_ref: str | None = Field(default=None, max_length=64)


class AppointmentAccepted(BaseModel):
    appointment_id: uuid.UUID
    status: str
    status_url: str


class AppointmentHistoryOut(BaseModel):
    from_status: str | None
    to_status: str
    reason: str | None
    created_at: datetime


class AppointmentOut(BaseModel):
    id: uuid.UUID
    clinic_id: uuid.UUID
    patient_id: uuid.UUID
    provider_id: uuid.UUID
    slot_id: uuid.UUID
    slot_start: datetime
    slot_end: datetime
    status: str
    failure_reason: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    # Filled on the single-appointment read; empty in list responses.
    history: list[AppointmentHistoryOut] = Field(default_factory=list)


# --- plan 02-08: cancel and reschedule ----------------------------------------


class CancelRequest(BaseModel):
    # Staff (FRONT_DESK/ADMIN) may waive the refund cutoff (D-13); a PATIENT
    # asking for it gets 403.
    waive_cutoff: bool = False


class RescheduleRequest(BaseModel):
    new_slot_id: uuid.UUID


class ChangeAccepted(BaseModel):
    appointment_id: uuid.UUID
    # The appointment's CURRENT status ("cancelled" on a cancel no-op).
    status: str
    status_url: str
    # "cancel" or "reschedule"
    operation: str
