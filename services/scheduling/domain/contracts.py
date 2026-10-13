"""Scheduling's Temporal workflow contracts (plan 02-06, D-05, D-06).

Pure on purpose: this module imports only `dataclasses` and `typing`, so the
workflow sandbox can load it and the API (starter) and worker share one
definition.

Rules every contract here follows (research Pitfall 1, T-02-27, T-02-30):

* fields are plain `str` (ids as strings, outcomes and reasons as plain
  strings) or, for counts, plain `int` - never a str-subclass Enum, which Temporal's default JSON converter
  round-trips as a list of characters;
* ids only: no PHI, no coverage reference (activities load it from the
  appointment row), no caller JWT (activities authenticate with service
  tokens). Workflow histories are durable and operator-visible.

Activities are referenced by NAME (`ACT_*`), so workflow code never imports the
activity implementations (which pull in SQLAlchemy and httpx).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Workflow type name (also the class's @workflow.defn name).
BOOKING_WORKFLOW = "BookingWorkflow"

# Activity names (D-09 order: validate -> reserve -> billing -> notify -> confirm;
# compensations: release, void; terminal failure write: mark_failed).
ACT_VALIDATE = "booking.validate"
ACT_RESERVE = "booking.reserve_slot"
ACT_BILLING = "booking.billing_precheck"
ACT_NOTIFY = "booking.schedule_notification"
ACT_CONFIRM = "booking.confirm"
ACT_RELEASE = "booking.release_slot"
ACT_VOID = "booking.void_billing"
ACT_MARK_FAILED = "booking.mark_failed"

# Maintenance workflows run by Temporal Schedules (plan 02-07; D-03, research
# G7 / Pattern 8): the hold reaper and the pending-booking starter.
HOLD_REAPER_WORKFLOW = "HoldReaperWorkflow"
PENDING_STARTER_WORKFLOW = "PendingBookingStarterWorkflow"
ACT_EXPIRE_HOLDS = "maintenance.expire_holds"
ACT_START_PENDING = "maintenance.start_pending_bookings"

OUTCOME_CONFIRMED = "confirmed"
OUTCOME_FAILED = "failed"


def booking_workflow_id(appointment_id: str) -> str:
    """Deterministic booking workflow ID (D-06): one saga per appointment."""
    return f"booking-{appointment_id}"


@dataclass
class BookingInput:
    appointment_id: str
    clinic_id: str
    patient_id: str
    provider_id: str
    slot_id: str


@dataclass
class BookingResult:
    # "confirmed" or "failed"
    outcome: str
    # Machine-readable reason when failed (an ApplicationError type such as
    # "BillingRejected", or "activity_timeout" / "activity_failed").
    reason: Optional[str] = None


@dataclass
class MarkFailedInput:
    appointment_id: str
    reason: str


@dataclass
class MaintenanceResult:
    # How many items one maintenance run handled (holds released, or booking
    # workflows started). A plain int: a count, never an id or a value.
    processed: int
