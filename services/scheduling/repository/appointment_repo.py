"""Appointment writes: the one-transaction accept step and the retry-safe
appointment state steps (D-02, D-08, D-09).

Every function takes the caller's `AsyncSession`, flushes, and never commits:
the caller (API route or Temporal activity) owns the transaction. Nothing here
deletes a row; compensation is a forward transition plus an
`appointment_status_history` row.

Business outcomes are decided by Postgres arbiters, not by read-then-write
checks: `uq_appointments_active_slot` (one active appointment per slot),
`ex_appointments_patient_no_overlap` (one patient, no overlapping active
appointments) and the `idempotency_keys` primary key (duplicate requests).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.domain.enums import AppointmentStatus
from services.scheduling.domain.errors import IdempotencyKeyReused, PatientOverlap, SlotTaken
from services.scheduling.repository.idempotency_repo import (
    claim_idempotency_key,
    get_idempotency_record,
)
from services.scheduling.repository.models import Appointment, AppointmentStatusHistory

ACTIVE_SLOT_CONSTRAINT = "uq_appointments_active_slot"
PATIENT_OVERLAP_CONSTRAINT = "ex_appointments_patient_no_overlap"

# Default actor for transitions driven by the booking saga.
WORKFLOW_ACTOR = "system:workflow"


@dataclass(frozen=True, slots=True)
class AcceptResult:
    """Outcome of `accept_booking`: the appointment that answers this request,
    and whether it was created by an earlier request with the same key."""

    appointment_id: uuid.UUID
    replayed: bool


def _violates(exc: IntegrityError, constraint_name: str) -> bool:
    return constraint_name in str(exc.orig)


def is_slot_taken(exc: IntegrityError) -> bool:
    """True when `exc` is the active-slot unique index rejecting a second
    active appointment for a slot (research Pitfall 9: every path that can hit
    the index maps it to a business outcome through this one helper)."""
    return _violates(exc, ACTIVE_SLOT_CONSTRAINT)


def is_patient_overlap(exc: IntegrityError) -> bool:
    """True when `exc` is the patient-overlap exclusion constraint."""
    return _violates(exc, PATIENT_OVERLAP_CONSTRAINT)


async def append_status_history(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    from_status: str | None,
    to_status: str,
    actor: str,
    reason: str | None = None,
) -> None:
    """Append one transition row (append-only; never updated or deleted)."""
    db.add(
        AppointmentStatusHistory(
            appointment_id=appointment_id,
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            actor=actor,
        )
    )
    await db.flush()


async def accept_booking(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    caller_user_id: uuid.UUID,
    key: str,
    request_hash: str,
    clinic_id: uuid.UUID,
    patient_id: uuid.UUID,
    provider_id: uuid.UUID,
    slot_id: uuid.UUID,
    slot_start: datetime,
    slot_end: datetime,
    coverage_ref: str | None,
    created_by: uuid.UUID,
) -> AcceptResult:
    """Claim the idempotency key, then insert the `pending` appointment, in the
    caller's transaction (research Pattern 1).

    * Key already claimed with the same body -> `AcceptResult(stored, replayed=True)`.
    * Key already claimed with another body -> `IdempotencyKeyReused`.
    * Slot already actively booked -> `SlotTaken`; patient already booked in an
      overlapping window -> `PatientOverlap`.

    On `SlotTaken`/`PatientOverlap` the transaction is aborted (no SAVEPOINT is
    used), so the caller MUST roll back; that also discards the key claim, so a
    losing request leaves no idempotency row behind and may be retried.
    """
    owner = await claim_idempotency_key(
        db,
        caller_user_id=caller_user_id,
        key=key,
        request_hash=request_hash,
        appointment_id=appointment_id,
    )
    if owner is None:
        record = await get_idempotency_record(db, caller_user_id=caller_user_id, key=key)
        if record is None:  # pragma: no cover - rows are never deleted
            raise RuntimeError("idempotency key conflicted but no record is visible")
        stored_hash, stored_appointment_id = record
        if stored_hash != request_hash:
            raise IdempotencyKeyReused()
        return AcceptResult(appointment_id=stored_appointment_id, replayed=True)

    db.add(
        Appointment(
            id=appointment_id,
            clinic_id=clinic_id,
            patient_id=patient_id,
            provider_id=provider_id,
            slot_id=slot_id,
            slot_start=slot_start,
            slot_end=slot_end,
            status=AppointmentStatus.PENDING,
            coverage_ref=coverage_ref,
            created_by=created_by,
        )
    )
    db.add(
        AppointmentStatusHistory(
            appointment_id=appointment_id,
            from_status=None,
            to_status=AppointmentStatus.PENDING,
            actor=f"user:{created_by}",
        )
    )
    try:
        await db.flush()
    except IntegrityError as exc:
        if is_slot_taken(exc):
            raise SlotTaken() from exc
        if is_patient_overlap(exc):
            raise PatientOverlap() from exc
        raise
    return AcceptResult(appointment_id=appointment_id, replayed=False)


async def confirm_booking(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    slot_id: uuid.UUID,
    actor: str = WORKFLOW_ACTOR,
) -> bool:
    """Final saga step: slot `held` -> `booked` and appointment `pending` ->
    `confirmed`, in the caller's single transaction.

    True when the booking is now confirmed (including a repeat: no second
    history row). False when the hold was lost (reaped, or taken by another
    appointment) or the appointment is no longer pending; then nothing changes
    and the saga compensates (research Pitfall 5).

    The appointment row is locked first so the two updates cannot be split by
    a concurrent transition of the same appointment.
    """
    current = await db.scalar(
        text("SELECT status FROM appointments WHERE id = :a FOR UPDATE"), {"a": appointment_id}
    )
    if current == AppointmentStatus.CONFIRMED:
        booked = await db.scalar(
            text(
                "SELECT 1 FROM provider_slots "
                "WHERE id = :s AND status = 'booked' AND held_by_appointment_id = :a"
            ),
            {"s": slot_id, "a": appointment_id},
        )
        return booked is not None
    if current != AppointmentStatus.PENDING:
        return False

    slot = await db.scalar(
        text(
            """
            UPDATE provider_slots
               SET status = 'booked', held_until = NULL
             WHERE id = :s AND status = 'held' AND held_by_appointment_id = :a
            RETURNING id
            """
        ),
        {"s": slot_id, "a": appointment_id},
    )
    if slot is None:
        return False

    confirmed = await db.scalar(
        text(
            """
            UPDATE appointments
               SET status = 'confirmed', version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'pending'
            RETURNING id
            """
        ),
        {"a": appointment_id},
    )
    if confirmed is None:  # pragma: no cover - the row is locked as pending above
        raise RuntimeError("appointment left 'pending' while locked")
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.PENDING,
        to_status=AppointmentStatus.CONFIRMED,
        actor=actor,
    )
    return True


async def mark_appointment_failed(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    reason: str,
    actor: str,
) -> bool:
    """Compensation: `pending` -> `failed` with a machine-readable reason
    (`FailureReason`), plus one history row. A forward transition, never a
    delete.

    True when the appointment is now failed (a repeat appends no second history
    row); False when it is in another state (e.g. already confirmed), which is
    left untouched.
    """
    failed = await db.scalar(
        text(
            """
            UPDATE appointments
               SET status = 'failed', failure_reason = :r,
                   version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'pending'
            RETURNING id
            """
        ),
        {"a": appointment_id, "r": reason},
    )
    if failed is not None:
        await append_status_history(
            db,
            appointment_id=appointment_id,
            from_status=AppointmentStatus.PENDING,
            to_status=AppointmentStatus.FAILED,
            reason=reason,
            actor=actor,
        )
        return True
    current = await db.scalar(
        text("SELECT status FROM appointments WHERE id = :a"), {"a": appointment_id}
    )
    return current == AppointmentStatus.FAILED
