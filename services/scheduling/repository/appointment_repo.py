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
from typing import Literal

from sqlalchemy import func, select, text
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
    workflow_started: bool = False,
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

    `workflow_started=True` (plan 02-09, the waitlist claim) sets
    `workflow_started_at = now()` on the inserted row in this same
    transaction: the caller (a parent workflow) guarantees the booking saga is
    started as its child, so the pending-booking starter (plan 02-07) must
    never pick the row up and race that child start.
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
            workflow_started_at=func.now() if workflow_started else None,
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
    history row, and a retry after the appointment already progressed to
    checked_in, completed or no_show while its slot is still booked by it, so a
    late retry never reports a lost hold for a real booking; S3). False when
    the hold was lost (reaped, or taken by another appointment) or the
    appointment is no longer pending; then nothing changes and the saga
    compensates (research Pitfall 5).

    The appointment row is locked first so the two updates cannot be split by
    a concurrent transition of the same appointment.
    """
    current = await db.scalar(
        text("SELECT status FROM appointments WHERE id = :a FOR UPDATE"), {"a": appointment_id}
    )
    if current in AppointmentStatus.COMMITTED:
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


# --- plan 02-06: workflow-start marker and reads for /api/appointments --------


async def mark_workflow_started(db: AsyncSession, *, appointment_id: uuid.UUID) -> bool:
    """Record that the booking workflow has been started (research Pattern 8).

    Conditional (`WHERE workflow_started_at IS NULL`), so a repeat or a race
    with the starter sweeper (plan 02-07) is harmless: True when this call set
    the marker. Flushes only; the caller commits.
    """
    marked = await db.scalar(
        text(
            """
            UPDATE appointments
               SET workflow_started_at = now()
             WHERE id = :a AND workflow_started_at IS NULL
            RETURNING id
            """
        ),
        {"a": appointment_id},
    )
    return marked is not None


async def list_unstarted_pending(
    db: AsyncSession, *, older_than_seconds: int, limit: int = 100
) -> list[Appointment]:
    """Pending appointments whose booking workflow was never started (plan
    02-07, research G7 / Pitfall 4): `status = 'pending' AND
    workflow_started_at IS NULL AND created_at < now() - older_than_seconds`,
    oldest first, at most `limit`. Served by the partial index
    `ix_appointments_pending_unstarted`.

    No row lock is taken: the starter calls Temporal per row, and a duplicate
    start is already a no-op (`start_once`, then the conditional
    `mark_workflow_started`). The grace window keeps the starter away from
    bookings the API is starting right now.
    """
    query = (
        select(Appointment)
        .where(
            Appointment.status == AppointmentStatus.PENDING,
            Appointment.workflow_started_at.is_(None),
            Appointment.created_at
            < text("now() - make_interval(secs => :grace)").bindparams(
                grace=float(older_than_seconds)
            ),
        )
        .order_by(Appointment.created_at)
        .limit(limit)
    )
    return list((await db.scalars(query)).all())


async def get_appointment(db: AsyncSession, appointment_id: uuid.UUID) -> Appointment | None:
    return await db.get(Appointment, appointment_id)


async def list_appointments(
    db: AsyncSession,
    *,
    clinic_id: uuid.UUID | None = None,
    patient_id: uuid.UUID | None = None,
    status: str | None = None,
    provider_id: uuid.UUID | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[Appointment]:
    """Appointments matching every given filter, ordered by slot start.

    `start`/`end` bound `slot_start` as `[start, end)`. Scope filters
    (`clinic_id`, `patient_id`) are applied by the caller from the user's role.
    """
    query = select(Appointment)
    if clinic_id is not None:
        query = query.where(Appointment.clinic_id == clinic_id)
    if patient_id is not None:
        query = query.where(Appointment.patient_id == patient_id)
    if status is not None:
        query = query.where(Appointment.status == status)
    if provider_id is not None:
        query = query.where(Appointment.provider_id == provider_id)
    if start is not None:
        query = query.where(Appointment.slot_start >= start)
    if end is not None:
        query = query.where(Appointment.slot_start < end)
    query = query.order_by(Appointment.slot_start, Appointment.id).limit(limit)
    return list((await db.scalars(query)).all())


async def get_history(db: AsyncSession, appointment_id: uuid.UUID) -> list[AppointmentStatusHistory]:
    """The appointment's transitions, oldest first."""
    query = (
        select(AppointmentStatusHistory)
        .where(AppointmentStatusHistory.appointment_id == appointment_id)
        .order_by(AppointmentStatusHistory.created_at, AppointmentStatusHistory.id)
    )
    return list((await db.scalars(query)).all())


# --- plan 02-08: cancel and reschedule (D-13, D-14) -----------------------------

CancelOutcome = Literal["cancelled", "already", "not_cancellable"]
SwapOutcome = Literal["swapped", "already", "rejected"]


async def cancel_appointment(
    db: AsyncSession, *, appointment_id: uuid.UUID, actor: str
) -> CancelOutcome:
    """Appointment `confirmed` -> `cancelled` and its slot `booked` ->
    `available`, in the caller's ONE transaction, plus one history row.

    The slot released is the one the appointment points at NOW (read under the
    row lock), so a reschedule that committed first never leaves its new slot
    booked by a cancelled appointment. "already" when the appointment is
    already cancelled (a retry or repeat: no second history row);
    "not_cancellable" for any other state (pending, failed, missing), which is
    left untouched. A forward transition, never a delete.
    """
    row = (
        await db.execute(
            text("SELECT status, slot_id FROM appointments WHERE id = :a FOR UPDATE"),
            {"a": appointment_id},
        )
    ).one_or_none()
    if row is None:
        return "not_cancellable"
    if row.status == AppointmentStatus.CANCELLED:
        return "already"
    if row.status != AppointmentStatus.CONFIRMED:
        return "not_cancellable"

    await db.execute(
        text(
            """
            UPDATE appointments
               SET status = 'cancelled', version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'confirmed'
            """
        ),
        {"a": appointment_id},
    )
    await db.execute(
        text(
            """
            UPDATE provider_slots
               SET status = 'available', held_until = NULL, held_by_appointment_id = NULL
             WHERE id = :s AND held_by_appointment_id = :a AND status = 'booked'
            """
        ),
        {"s": row.slot_id, "a": appointment_id},
    )
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.CONFIRMED,
        to_status=AppointmentStatus.CANCELLED,
        actor=actor,
    )
    return "cancelled"


RESCHEDULED_REASON = "rescheduled"


async def swap_slot(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    old_slot_id: uuid.UUID,
    new_slot_id: uuid.UUID,
    actor: str,
) -> SwapOutcome:
    """The reschedule swap (D-14, research Pattern 5), in the caller's ONE
    transaction: the new slot `held` (by this appointment) -> `booked`, the
    appointment repointed (`slot_id`, `slot_start`, `slot_end`,
    `version + 1`), the old slot -> `available`, and one
    `confirmed -> confirmed` history row with reason `rescheduled`.

    * "swapped"  - done now.
    * "already"  - the appointment already points at the new slot (a retry):
                   nothing changes, no second history row.
    * "rejected" - the appointment is no longer confirmed on the old slot, the
                   new slot is no longer held by it, or a database arbiter
                   (`uq_appointments_active_slot`,
                   `ex_appointments_patient_no_overlap`) refused the move
                   (research Pitfall 9). The caller MUST roll back (the
                   transaction may be aborted), which leaves the original
                   booking untouched.
    """
    row = (
        await db.execute(
            text("SELECT status, slot_id FROM appointments WHERE id = :a FOR UPDATE"),
            {"a": appointment_id},
        )
    ).one_or_none()
    if row is None or row.status != AppointmentStatus.CONFIRMED:
        return "rejected"
    if row.slot_id == new_slot_id:
        return "already"
    if row.slot_id != old_slot_id:
        return "rejected"

    new_slot = (
        await db.execute(
            text(
                """
                UPDATE provider_slots
                   SET status = 'booked', held_until = NULL
                 WHERE id = :s AND status = 'held' AND held_by_appointment_id = :a
                RETURNING start_time, end_time
                """
            ),
            {"s": new_slot_id, "a": appointment_id},
        )
    ).one_or_none()
    if new_slot is None:
        return "rejected"

    try:
        moved = await db.scalar(
            text(
                """
                UPDATE appointments
                   SET slot_id = :new, slot_start = :st, slot_end = :en,
                       version = version + 1, updated_at = now()
                 WHERE id = :a AND slot_id = :old AND status = 'confirmed'
                RETURNING id
                """
            ),
            {
                "a": appointment_id,
                "old": old_slot_id,
                "new": new_slot_id,
                "st": new_slot.start_time,
                "en": new_slot.end_time,
            },
        )
    except IntegrityError as exc:
        if is_slot_taken(exc) or is_patient_overlap(exc):
            return "rejected"
        raise
    if moved is None:  # pragma: no cover - the row is locked as confirmed on old above
        return "rejected"

    await db.execute(
        text(
            """
            UPDATE provider_slots
               SET status = 'available', held_until = NULL, held_by_appointment_id = NULL
             WHERE id = :s AND held_by_appointment_id = :a AND status = 'booked'
            """
        ),
        {"s": old_slot_id, "a": appointment_id},
    )
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.CONFIRMED,
        to_status=AppointmentStatus.CONFIRMED,
        reason=RESCHEDULED_REASON,
        actor=actor,
    )
    return "swapped"


async def record_history_note(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    reason: str,
    actor: str,
    bump_version: bool = False,
) -> bool:
    """Append a `from_status == to_status` history row carrying `reason` (e.g.
    `reschedule_failed:SlotUnavailable`), so a change that did not happen is
    still visible in the appointment's history (D-09, D-14).

    Idempotent per `(reason, actor)`: activities pass their workflow id as the
    actor, so a retried activity writes one row. `bump_version` also increments
    `appointments.version` (a failed reschedule frees the next attempt's
    workflow id). True when a row was written now; False when it already
    existed or the appointment is missing.
    """
    current = await db.scalar(
        text("SELECT status FROM appointments WHERE id = :a FOR UPDATE"), {"a": appointment_id}
    )
    if current is None:
        return False
    exists = await db.scalar(
        text(
            "SELECT 1 FROM appointment_status_history "
            "WHERE appointment_id = :a AND reason = :r AND actor = :actor LIMIT 1"
        ),
        {"a": appointment_id, "r": reason, "actor": actor},
    )
    if exists is not None:
        return False
    if bump_version:
        await db.execute(
            text(
                "UPDATE appointments SET version = version + 1, updated_at = now() WHERE id = :a"
            ),
            {"a": appointment_id},
        )
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=current,
        to_status=current,
        reason=reason,
        actor=actor,
    )
    return True


# --- plan 02.1-01: check-in, completion and no-show (M1) ------------------------

CheckInOutcome = Literal[
    "checked_in", "already", "not_confirmed", "too_early", "too_late", "not_found"
]


async def check_in_appointment(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    actor: str,
    opens_before_minutes: int,
    closes_after_end_minutes: int,
) -> CheckInOutcome:
    """Appointment `confirmed` -> `checked_in`, only inside the check-in window
    (opens `opens_before_minutes` before `slot_start`, closes
    `closes_after_end_minutes` after `slot_end`), plus one history row.

    The window is judged by the DATABASE clock (`now()` in the locking SELECT),
    never the application's, so a skewed API host cannot widen it. The row is
    locked first and the UPDATE is conditional on `status = 'confirmed'`, so
    concurrent check-ins produce exactly one transition (the rest see
    `already`). The slot stays `booked`: a checked-in appointment keeps it.

    * "checked_in"    - done now.
    * "already"       - already checked in (a repeat: no second history row).
    * "not_confirmed" - any other state (pending, cancelled, completed, ...).
    * "too_early" / "too_late" - confirmed but outside the window; nothing written.
    * "not_found"     - no such appointment.

    Flushes only; the caller commits.
    """
    row = (
        await db.execute(
            text(
                """
                SELECT status,
                       (now() < slot_start - make_interval(mins => :before)) AS too_early,
                       (now() > slot_end + make_interval(mins => :after)) AS too_late
                  FROM appointments
                 WHERE id = :a
                   FOR UPDATE
                """
            ),
            {
                "a": appointment_id,
                "before": opens_before_minutes,
                "after": closes_after_end_minutes,
            },
        )
    ).one_or_none()
    if row is None:
        return "not_found"
    if row.status == AppointmentStatus.CHECKED_IN:
        return "already"
    if row.status != AppointmentStatus.CONFIRMED:
        return "not_confirmed"
    if row.too_early:
        return "too_early"
    if row.too_late:
        return "too_late"

    moved = await db.scalar(
        text(
            """
            UPDATE appointments
               SET status = 'checked_in', version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'confirmed'
            RETURNING id
            """
        ),
        {"a": appointment_id},
    )
    if moved is None:  # pragma: no cover - the row is locked as confirmed above
        return "not_confirmed"
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.CONFIRMED,
        to_status=AppointmentStatus.CHECKED_IN,
        actor=actor,
    )
    return "checked_in"


CompleteOutcome = Literal["completed", "already", "not_checked_in", "not_found"]


async def complete_appointment(
    db: AsyncSession, *, appointment_id: uuid.UUID, actor: str
) -> CompleteOutcome:
    """Appointment `checked_in` -> `completed`, plus one history row.

    Row-locked and conditional on `status = 'checked_in'`. "already" when it is
    already completed (a repeat: no second history row); "not_checked_in" for
    any other state (a visit cannot complete an appointment that never checked
    in); "not_found" when missing. The slot is untouched: a completed
    appointment keeps it. Flushes only; the caller commits.
    """
    current = await db.scalar(
        text("SELECT status FROM appointments WHERE id = :a FOR UPDATE"), {"a": appointment_id}
    )
    if current is None:
        return "not_found"
    if current == AppointmentStatus.COMPLETED:
        return "already"
    if current != AppointmentStatus.CHECKED_IN:
        return "not_checked_in"

    moved = await db.scalar(
        text(
            """
            UPDATE appointments
               SET status = 'completed', version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'checked_in'
            RETURNING id
            """
        ),
        {"a": appointment_id},
    )
    if moved is None:  # pragma: no cover - the row is locked as checked_in above
        return "not_checked_in"
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.CHECKED_IN,
        to_status=AppointmentStatus.COMPLETED,
        actor=actor,
    )
    return "completed"


NoShowOutcome = Literal["no_show", "already", "too_early", "not_confirmed", "not_found"]


async def mark_no_show(
    db: AsyncSession, *, appointment_id: uuid.UUID, actor: str, grace_minutes: int
) -> NoShowOutcome:
    """Appointment `confirmed` -> `no_show`, only once `slot_end + grace` has
    passed on the DATABASE clock, plus one history row.

    "already" for a repeat; "too_early" while the grace period still runs;
    "not_confirmed" for any other state (a checked-in patient is never a
    no-show); "not_found" when missing. The slot is untouched: a no-show
    keeps it reserved. Flushes only; the caller commits.
    """
    row = (
        await db.execute(
            text(
                """
                SELECT status,
                       (now() > slot_end + make_interval(mins => :grace)) AS past_grace
                  FROM appointments
                 WHERE id = :a
                   FOR UPDATE
                """
            ),
            {"a": appointment_id, "grace": grace_minutes},
        )
    ).one_or_none()
    if row is None:
        return "not_found"
    if row.status == AppointmentStatus.NO_SHOW:
        return "already"
    if row.status != AppointmentStatus.CONFIRMED:
        return "not_confirmed"
    if not row.past_grace:
        return "too_early"

    moved = await db.scalar(
        text(
            """
            UPDATE appointments
               SET status = 'no_show', version = version + 1, updated_at = now()
             WHERE id = :a AND status = 'confirmed'
            RETURNING id
            """
        ),
        {"a": appointment_id},
    )
    if moved is None:  # pragma: no cover - the row is locked as confirmed above
        return "not_confirmed"
    await append_status_history(
        db,
        appointment_id=appointment_id,
        from_status=AppointmentStatus.CONFIRMED,
        to_status=AppointmentStatus.NO_SHOW,
        actor=actor,
    )
    return "no_show"


async def list_no_show_candidates(
    db: AsyncSession, *, grace_minutes: int, limit: int
) -> list[Appointment]:
    """Confirmed appointments whose `slot_end + grace` has passed (DB clock),
    oldest first, at most `limit`. No row lock: the sweep (plan 02.1-11)
    re-checks each row through `mark_no_show`."""
    query = (
        select(Appointment)
        .where(
            Appointment.status == AppointmentStatus.CONFIRMED,
            Appointment.slot_end
            < text("now() - make_interval(mins => :grace)").bindparams(grace=grace_minutes),
        )
        .order_by(Appointment.slot_end, Appointment.id)
        .limit(limit)
    )
    return list((await db.scalars(query)).all())
