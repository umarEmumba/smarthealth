"""Retry-safe slot hold steps: hold, release, and the hold reaper (D-03, D-09).

Each step is ONE conditional `UPDATE ... WHERE <expected state> RETURNING`, so
concurrent callers are arbitrated by Postgres row locks and a Temporal activity
can run the same step twice without harm: a repeat by the same appointment is
reported as success, a step whose precondition no longer holds changes nothing
and reports False. Nothing here deletes a row; a released hold is a forward
transition back to `available`.

`held_by_appointment_id` is kept once a slot is `booked`, so a retried hold by
the booking appointment is recognised as already done.

Functions flush through the caller's session and never commit; the caller owns
the transaction.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.domain.enums import FailureReason
from services.scheduling.repository.appointment_repo import mark_appointment_failed

DEFAULT_HOLD_TTL_MINUTES = 10
REAPER_ACTOR = "system:reaper"

_HOLD_SQL = text(
    """
    UPDATE provider_slots
       SET status = 'held',
           held_by_appointment_id = :a,
           held_until = now() + make_interval(mins => :ttl)
     WHERE id = :s
       AND (status = 'available' OR (status = 'held' AND held_by_appointment_id = :a))
    RETURNING id
    """
)

_RELEASE_SQL = text(
    """
    UPDATE provider_slots
       SET status = 'available', held_until = NULL, held_by_appointment_id = NULL
     WHERE id = :s AND status = 'held' AND held_by_appointment_id = :a
    RETURNING id
    """
)


async def hold_slot(
    db: AsyncSession,
    *,
    slot_id: uuid.UUID,
    appointment_id: uuid.UUID,
    ttl_minutes: int = DEFAULT_HOLD_TTL_MINUTES,
) -> bool:
    """Hold an available slot for `appointment_id` until now() + ttl.

    True when the slot is now held (or already held, or already booked) by this
    appointment; False when it is unavailable to it (held or booked by another
    appointment, cancelled, or missing). A repeat by the owner refreshes
    `held_until`, which is harmless.
    """
    held = await db.scalar(_HOLD_SQL, {"s": slot_id, "a": appointment_id, "ttl": ttl_minutes})
    if held is not None:
        return True
    already_booked = await db.scalar(
        text(
            "SELECT 1 FROM provider_slots "
            "WHERE id = :s AND status = 'booked' AND held_by_appointment_id = :a"
        ),
        {"s": slot_id, "a": appointment_id},
    )
    return already_booked is not None


async def release_hold(db: AsyncSession, *, slot_id: uuid.UUID, appointment_id: uuid.UUID) -> bool:
    """Compensation: return a slot held by `appointment_id` to `available`.

    True when the hold was released now or is already gone (the slot is held
    by nobody, e.g. a repeat or after the reaper); False when the slot is held
    or booked by an appointment (another one, or this one after confirm, which
    a hold release must not undo). A False never changes the slot.
    """
    released = await db.scalar(_RELEASE_SQL, {"s": slot_id, "a": appointment_id})
    if released is not None:
        return True
    holder = (
        await db.execute(
            text("SELECT held_by_appointment_id FROM provider_slots WHERE id = :s"),
            {"s": slot_id},
        )
    ).one_or_none()
    return holder is not None and holder.held_by_appointment_id is None


async def expire_holds(db: AsyncSession, *, limit: int = 100) -> list[tuple[uuid.UUID, uuid.UUID | None]]:
    """Reaper: release holds whose `held_until` has passed and fail the owning
    `pending` appointment (`hold_expired`), in the caller's transaction.

    Rows are claimed with `FOR UPDATE SKIP LOCKED`, so concurrent reapers (or a
    saga confirming the same slot right now) never block on or double-process
    a row. Returns the `(slot_id, appointment_id)` pairs released.
    """
    rows = (
        await db.execute(
            text(
                """
                SELECT id, held_by_appointment_id
                  FROM provider_slots
                 WHERE status = 'held' AND held_until < now()
                 ORDER BY held_until
                 LIMIT :limit
                   FOR UPDATE SKIP LOCKED
                """
            ),
            {"limit": limit},
        )
    ).all()

    released: list[tuple[uuid.UUID, uuid.UUID | None]] = []
    for slot_id, appointment_id in rows:
        freed = await db.scalar(
            text(
                """
                UPDATE provider_slots
                   SET status = 'available', held_until = NULL, held_by_appointment_id = NULL
                 WHERE id = :s AND status = 'held' AND held_until < now()
                RETURNING id
                """
            ),
            {"s": slot_id},
        )
        if freed is None:  # pragma: no cover - the row is locked by this transaction
            continue
        if appointment_id is not None:
            await mark_appointment_failed(
                db,
                appointment_id=appointment_id,
                reason=FailureReason.HOLD_EXPIRED,
                actor=REAPER_ACTOR,
            )
        released.append((slot_id, appointment_id))
    return released
