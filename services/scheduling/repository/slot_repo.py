"""Slot steps: retry-safe hold/release/reaper (D-03, D-09) and, since plan
02-04, idempotent generation, search and admin withdrawal (D-01).

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
from collections.abc import Sequence
from datetime import datetime
from typing import Literal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.domain.enums import FailureReason, SlotStatus
from services.scheduling.domain.slot_generation import CandidateSlot
from services.scheduling.repository.appointment_repo import mark_appointment_failed
from services.scheduling.repository.models import ProviderSlot

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


# --- plan 02-04: slot generation, search and withdrawal (D-01) ---------------

# `ON CONFLICT DO NOTHING` with NO conflict target skips a row that trips either
# `uq_provider_slots_provider_start` (same provider + start, whatever its status,
# so a held, booked or withdrawn slot is never re-inserted or touched) or the
# `ex_provider_slots_no_overlap` exclusion constraint (research Pattern 6).
_INSERT_GENERATED_SQL = text(
    """
    INSERT INTO provider_slots (provider_id, clinic_id, department_id, start_time, end_time)
    SELECT :p, :c, g.department_id, g.start_time, g.end_time
      FROM unnest(
               CAST(:departments AS uuid[]),
               CAST(:starts AS timestamptz[]),
               CAST(:ends AS timestamptz[])
           ) AS g(department_id, start_time, end_time)
    ON CONFLICT DO NOTHING
    RETURNING id
    """
)


async def insert_generated_slots(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    clinic_id: uuid.UUID,
    candidates: Sequence[CandidateSlot],
) -> int:
    """Insert the candidates as `available` slots; return how many were new.

    Idempotent: re-running over the same range inserts nothing, and an existing
    slot is never modified. Flushes through the caller's session; the caller
    commits.
    """
    if not candidates:
        return 0
    rows = (
        await db.execute(
            _INSERT_GENERATED_SQL,
            {
                "p": provider_id,
                "c": clinic_id,
                "departments": [c.department_id for c in candidates],
                "starts": [c.start for c in candidates],
                "ends": [c.end for c in candidates],
            },
        )
    ).all()
    return len(rows)


async def search_slots(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    start: datetime,
    end: datetime,
    status: str | None = SlotStatus.AVAILABLE.value,
) -> list[ProviderSlot]:
    """The provider's slots starting in `[start, end)`, ordered by start;
    `status=None` returns every status."""
    query = (
        select(ProviderSlot)
        .where(
            ProviderSlot.provider_id == provider_id,
            ProviderSlot.start_time >= start,
            ProviderSlot.start_time < end,
        )
        .order_by(ProviderSlot.start_time)
    )
    if status is not None:
        query = query.where(ProviderSlot.status == status)
    return list((await db.scalars(query)).all())


async def latest_generated_start(db: AsyncSession, *, provider_id: uuid.UUID) -> datetime | None:
    """Start of the provider's latest slot (any status), or None if none."""
    return await db.scalar(
        select(func.max(ProviderSlot.start_time)).where(ProviderSlot.provider_id == provider_id)
    )


async def get_slot(db: AsyncSession, *, slot_id: uuid.UUID) -> ProviderSlot | None:
    return await db.get(ProviderSlot, slot_id)


WithdrawOutcome = Literal["withdrawn", "already", "not_available"]


async def withdraw_slot(db: AsyncSession, *, slot_id: uuid.UUID) -> WithdrawOutcome:
    """Admin withdrawal of a generated slot (research Pitfall 14).

    One conditional UPDATE `WHERE status = 'available'` (a forward transition to
    `cancelled`, never a delete): "withdrawn" when it changed the row now,
    "already" when the slot was already cancelled (a repeat is a no-op), and
    "not_available" when it is held or booked (or missing). The cancelled slot
    keeps its `(provider_id, start_time)` key, so re-generation never
    resurrects it. Flushes only; the caller commits.
    """
    changed = await db.scalar(
        text(
            "UPDATE provider_slots SET status = 'cancelled' "
            "WHERE id = :s AND status = 'available' RETURNING id"
        ),
        {"s": slot_id},
    )
    if changed is not None:
        return "withdrawn"
    current = await db.scalar(text("SELECT status FROM provider_slots WHERE id = :s"), {"s": slot_id})
    if current == SlotStatus.CANCELLED.value:
        return "already"
    return "not_available"
