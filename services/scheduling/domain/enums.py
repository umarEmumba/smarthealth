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
    """String constants for `appointments.status` (D-02, D-09).

    Deliberately a plain class of `str` attributes, NOT a `(str, Enum)`: these
    values travel through Temporal workflow/activity dataclasses in plan 02-06,
    and Temporal's default JSON converter round-trips a `(str, Enum)` field as
    a list of characters (research Pitfall 1). Plain strings round-trip intact.

    There is intentionally no `completed` status: the active-slot unique index
    covers `pending`/`confirmed` only, so a status outside that predicate would
    free the slot for rebooking (research Open Question 4). Visit completion is
    tracked by the Visit service, not here.
    """

    PENDING = "pending"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    # The statuses that hold a slot (the `uq_appointments_active_slot` and
    # `ex_appointments_patient_no_overlap` predicates).
    ACTIVE = ("pending", "confirmed")
    ALL = ("pending", "confirmed", "cancelled", "failed")


class FailureReason:
    """Machine-readable `appointments.failure_reason` vocabulary (D-09)."""

    SLOT_UNAVAILABLE = "SlotUnavailable"
    BILLING_REJECTED = "BillingRejected"
    REFERENCE_INVALID = "ReferenceInvalid"
    HOLD_EXPIRED = "hold_expired"
    CONFIRM_LOST_HOLD = "confirm_lost_hold"
