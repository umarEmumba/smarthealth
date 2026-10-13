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

# Cancel and reschedule workflows (plan 02-08; D-13, D-14). Their activities
# live in `activities/changes.py` and are named `change.*`. The change
# notification is a SEPARATE activity from the booking one (`ACT_NOTIFY`), so
# BookingWorkflow's command sequence and its recorded histories stay untouched.
CANCEL_WORKFLOW = "CancelWorkflow"
RESCHEDULE_WORKFLOW = "RescheduleWorkflow"
ACT_CANCEL_APPOINTMENT = "change.cancel_appointment"
ACT_BILLING_CANCEL = "change.billing_cancel"
ACT_HOLD_NEW = "change.hold_new_slot"
ACT_SWAP = "change.swap_slot"
ACT_RELEASE_NEW = "change.release_new_hold"
ACT_BILLING_RELINK = "change.billing_relink"
ACT_RECORD_OUTCOME = "change.record_outcome"
ACT_NOTIFY_CHANGE = "change.schedule_notification"

# Refund policy, decided in the API request (research A11) so the workflow
# never reads the clock.
REFUND = "refund"
NO_REFUND = "no_refund"

OUTCOME_CANCELLED = "cancelled"
OUTCOME_RESCHEDULED = "rescheduled"
# Reason on a successful change whose billing step did not land in its budget.
BILLING_PENDING = "billing_pending"

NOTIFY_CANCELLED = "booking_cancelled"
NOTIFY_RESCHEDULED = "booking_rescheduled"


def booking_workflow_id(appointment_id: str) -> str:
    """Deterministic booking workflow ID (D-06): one saga per appointment."""
    return f"booking-{appointment_id}"


def cancel_workflow_id(appointment_id: str, version: int) -> str:
    """One cancel per appointment version (D-06 counter scheme, research
    Pattern 2): a repeat while it runs attaches, a repeat after it closed is a
    no-op."""
    return f"cancel-{appointment_id}-{version}"


def reschedule_workflow_id(appointment_id: str, version: int) -> str:
    """One reschedule per appointment version. A successful swap and a
    recorded failure both bump the version, so the next attempt gets a new id."""
    return f"reschedule-{appointment_id}-{version}"


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


@dataclass
class CancelInput:
    appointment_id: str
    clinic_id: str
    # The slot the appointment held when the cancel was requested (for audit);
    # the repository releases whatever slot the appointment points at.
    slot_id: str
    # REFUND or NO_REFUND, decided in the API request (D-13).
    refund_policy: str
    # Who asked, as "user:<id>" (ids only).
    actor: str


@dataclass
class RescheduleInput:
    appointment_id: str
    clinic_id: str
    old_slot_id: str
    new_slot_id: str
    actor: str


@dataclass
class ChangeResult:
    # "cancelled", "rescheduled" or "failed"
    outcome: str
    # Failure reason (an ApplicationError type), or "billing_pending" on a
    # change whose billing step did not land.
    reason: Optional[str] = None


@dataclass
class NotifyInput:
    appointment_id: str
    clinic_id: str
    # "booking_cancelled" or "booking_rescheduled"
    notification_type: str


@dataclass
class ChangeOutcomeInput:
    appointment_id: str
    clinic_id: str
    # History note (`reason` column, from_status == to_status), e.g.
    # "reschedule_failed:SlotUnavailable".
    note: str
    # Audit action for the note, e.g. "appointment.reschedule_failed".
    audit_action: str
    # 1 when the note must bump the appointment version (a failed reschedule
    # frees the next attempt's workflow id), else 0. A plain int flag.
    bump_version: int = 0
