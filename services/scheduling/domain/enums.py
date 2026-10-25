"""Scheduling's domain enums.

`SlotStatus` backs the `provider_slots` exclusion constraint's WHERE clause
(`status <> 'cancelled'`) — see `services/scheduling/repository/models.py`.
"""

from __future__ import annotations

from enum import Enum


class SlotStatus(str, Enum):
    """Lifecycle states for a `provider_slots` row.

    The table exists, empty, from this phase on; Phase 2's booking saga is what
    transitions slots through this lifecycle.
    """

    AVAILABLE = "available"
    HELD = "held"
    BOOKED = "booked"
    CANCELLED = "cancelled"


class AppointmentStatus:
    """String constants for `appointments.status` (D-02, D-09; M1).

    Deliberately a plain class of `str` attributes, NOT a `(str, Enum)`: these
    values travel through Temporal workflow/activity dataclasses in plan 02-06,
    and Temporal's default JSON converter round-trips a `(str, Enum)` field as
    a list of characters (research Pitfall 1). Plain strings round-trip intact.

    Lifecycle (M1, plan 02.1-01):

        pending   -> confirmed | failed
        confirmed -> checked_in | no_show | cancelled
        checked_in -> completed

    Phase 2 kept `completed` out of the vocabulary because the active-slot
    unique index covered `pending`/`confirmed` only, so a status outside that
    predicate would free the slot for rebooking (research Open Question 4).
    That is now answered by widening the predicate: `ACTIVE` lists every status
    that occupies a slot, so a checked_in, completed or no_show appointment
    keeps its slot reserved forever, and cancel/reschedule (which act only on
    `confirmed`) can never free it. `checked_in` and `completed` are reached
    only through Scheduling's own conditional transitions (Visit calls the
    internal routes; it never writes Scheduling's table).
    """

    PENDING = "pending"
    CONFIRMED = "confirmed"
    CHECKED_IN = "checked_in"
    COMPLETED = "completed"
    NO_SHOW = "no_show"
    CANCELLED = "cancelled"
    FAILED = "failed"

    # Every status that holds a slot (the `uq_appointments_active_slot` and
    # `ex_appointments_patient_no_overlap` predicates).
    ACTIVE = ("pending", "confirmed", "checked_in", "completed", "no_show")
    # Statuses past the booking saga: the booking is real. A compensation or a
    # sweeper must never treat these as an in-flight booking.
    COMMITTED = ("confirmed", "checked_in", "completed", "no_show")
    ALL = (
        "pending",
        "confirmed",
        "checked_in",
        "completed",
        "no_show",
        "cancelled",
        "failed",
    )


class FailureReason:
    """Machine-readable `appointments.failure_reason` vocabulary (D-09).

    The saga stores the failing activity's `ApplicationError.type` verbatim
    (plan 02-06), so the business reasons use the error-type spelling.
    """

    SLOT_UNAVAILABLE = "SlotUnavailable"
    BILLING_REJECTED = "BillingRejected"
    REFERENCE_INVALID = "ReferenceInvalid"
    HOLD_EXPIRED = "hold_expired"
    CONFIRM_LOST_HOLD = "ConfirmLostHold"
    # S6 (plan 02.1-07): the slot started while the booking was pending.
    SLOT_IN_PAST = "SlotInPast"
    ACTIVITY_TIMEOUT = "activity_timeout"
    ACTIVITY_FAILED = "activity_failed"
    # S4 (plan 02.1-11): the booking workflow is gone (terminated, timed out or
    # closed without writing a terminal state) and the appointment was still
    # `pending`; the reconciler failed it, released its hold and voided billing.
    WORKFLOW_LOST = "workflow_lost"


class WaitlistStatus:
    """String constants for `waitlist_entries.status` (plan 02-09, D-12).

    A plain class of `str` attributes like `AppointmentStatus` (research
    Pitfall 1). `waiting -> matched` is the claim (FOR UPDATE SKIP LOCKED);
    a matched entry ends `booked`, `failed` (with a reason) or goes back to
    `waiting` when its booking lost the slot race. `cancelled` is the
    patient/staff withdrawal of a waiting entry.
    """

    WAITING = "waiting"
    MATCHED = "matched"
    BOOKED = "booked"
    FAILED = "failed"
    CANCELLED = "cancelled"

    # The statuses covered by `uq_waitlist_active_patient_provider`.
    ACTIVE = ("waiting", "matched")
    ALL = ("waiting", "matched", "booked", "failed", "cancelled")
