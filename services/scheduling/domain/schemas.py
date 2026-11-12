"""Scheduling's public request/response models (plans 02-04, 02-06).

Slot payloads carry ids and UTC instants only: no names, no PHI, and no data
enriched from another service (Phase 1.1 D-07).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field, model_validator


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
    # M2 (plan 02.1-08): True when a provider schedule change removed the
    # appointment's slot. The appointment stays confirmed; staff cancel or
    # reschedule it (a reschedule clears the flag). `schedule_change_reason` is a
    # fixed code (template_updated, template_deactivated, provider_deactivated),
    # None when not flagged.
    schedule_change_flagged: bool = False
    schedule_change_reason: str | None = None
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


# --- plan 02-09: waitlist -----------------------------------------------------


class WaitlistCreate(BaseModel):
    provider_id: uuid.UUID
    # Staff must name the patient; a PATIENT joins for themselves (resolved
    # from their login) and may only repeat their own id here.
    patient_id: uuid.UUID | None = None
    # Optional window (tz-aware); NULL ends are open. The auto-booked slot
    # must lie entirely inside it.
    window_start: datetime | None = None
    window_end: datetime | None = None
    # Opaque billing reference for the auto-booking: stored, never echoed
    # back, logged, audited or put in workflow I/O.
    coverage_ref: str | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def _window_is_ordered(self) -> "WaitlistCreate":
        for bound in (self.window_start, self.window_end):
            if bound is not None and bound.tzinfo is None:
                raise ValueError("window_start and window_end must include a UTC offset")
        if (
            self.window_start is not None
            and self.window_end is not None
            and self.window_end <= self.window_start
        ):
            raise ValueError("window_end must be after window_start")
        return self


class WaitlistEntryOut(BaseModel):
    """Ids, window and statuses only: never the coverage reference."""

    id: uuid.UUID
    clinic_id: uuid.UUID
    provider_id: uuid.UUID
    patient_id: uuid.UUID
    window_start: datetime | None
    window_end: datetime | None
    status: str
    failure_reason: str | None
    matched_appointment_id: uuid.UUID | None
    booked_appointment_id: uuid.UUID | None
    attempts: int
    created_at: datetime
    updated_at: datetime


# --- plan 02-10: internal appointment lookup for Visit ----------------------


class AppointmentRef(BaseModel):
    """What Visit needs to check an appointment in (D-15): ids, slot times and
    status only. No coverage reference, no failure reason, no history."""

    id: uuid.UUID
    clinic_id: uuid.UUID
    patient_id: uuid.UUID
    provider_id: uuid.UUID
    slot_id: uuid.UUID
    slot_start: datetime
    slot_end: datetime
    status: str
