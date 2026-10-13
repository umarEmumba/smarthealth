"""Scheduling's business-outcome exceptions.

These are raised by the repository layer when a database arbiter (a unique
index, an exclusion constraint, the idempotency-key primary key) rejects a
write. Callers map them to client-visible outcomes (409 / 422) or to saga
compensation; none of them is a retryable infrastructure failure.

Messages carry no PHI: at most an opaque UUID.
"""

from __future__ import annotations


class SchedulingError(Exception):
    """Base class for Scheduling business outcomes."""


class SlotTaken(SchedulingError):
    """The slot already has an active (pending/confirmed) appointment
    (`uq_appointments_active_slot`, D-02)."""


class PatientOverlap(SchedulingError):
    """The patient already holds an active appointment overlapping this window
    (`ex_appointments_patient_no_overlap`)."""


class IdempotencyKeyReused(SchedulingError):
    """The same `(caller, Idempotency-Key)` was used with a different request
    body (D-08: maps to 422)."""


class SlotNotBookable(SchedulingError):
    """The slot does not exist or is not in a bookable state."""
