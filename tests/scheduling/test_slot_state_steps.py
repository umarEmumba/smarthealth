"""Retry-safe slot and appointment state steps (D-03, D-09, research Code Example 2).

Each step is a conditional UPDATE that a Temporal activity may run more than
once: the second run must be a harmless no-op that still reports success, a
step whose precondition no longer holds must change nothing, and history rows
are appended only for real transitions. Nothing may ever be deleted.

Runs inside the rolled-back `scheduling_db_session`. Hold expiry is simulated
by writing `held_until` in the past explicitly (research Pitfall 8).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.repository.appointment_repo import (
    confirm_booking,
    mark_appointment_failed,
)
from services.scheduling.repository.slot_repo import expire_holds, hold_slot, release_hold

pytestmark = pytest.mark.integration

BASE = datetime(2027, 6, 7, 9, 0, tzinfo=timezone.utc)
REPO_DIR = Path(__file__).resolve().parents[2] / "services" / "scheduling" / "repository"


async def _slot(db: AsyncSession, start: datetime = BASE) -> uuid.UUID:
    return await db.scalar(
        text(
            "INSERT INTO provider_slots "
            "(provider_id, clinic_id, department_id, start_time, end_time) "
            "VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), :s, :e) RETURNING id"
        ),
        {"s": start, "e": start + timedelta(minutes=30)},
    )


async def _pending(db: AsyncSession, slot_id: uuid.UUID, status: str = "pending") -> uuid.UUID:
    return await db.scalar(
        text(
            "INSERT INTO appointments "
            "(id, clinic_id, patient_id, provider_id, slot_id, slot_start, slot_end, status, created_by) "
            "VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), "
            ":sl, :s, :e, :st, gen_random_uuid()) RETURNING id"
        ),
        {"sl": slot_id, "s": BASE, "e": BASE + timedelta(minutes=30), "st": status},
    )


async def _slot_row(db: AsyncSession, slot_id: uuid.UUID):
    return (
        await db.execute(
            text(
                "SELECT status, held_until, held_by_appointment_id, "
                "held_until > now() + interval '9 minutes' AS fresh "
                "FROM provider_slots WHERE id = :s"
            ),
            {"s": slot_id},
        )
    ).one()


async def _appt_row(db: AsyncSession, appointment_id: uuid.UUID):
    return (
        await db.execute(
            text("SELECT status, failure_reason, version FROM appointments WHERE id = :a"),
            {"a": appointment_id},
        )
    ).one()


async def _history(db: AsyncSession, appointment_id: uuid.UUID) -> list[tuple]:
    rows = await db.execute(
        text(
            "SELECT from_status, to_status, reason, actor FROM appointment_status_history "
            "WHERE appointment_id = :a ORDER BY created_at"
        ),
        {"a": appointment_id},
    )
    return [tuple(r) for r in rows]


async def test_hold_available_slot_is_idempotent_for_its_owner(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    owner, other = uuid.uuid4(), uuid.uuid4()

    assert await hold_slot(db, slot_id=slot_id, appointment_id=owner, ttl_minutes=10) is True
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_by_appointment_id, row.fresh) == ("held", owner, True)

    assert await hold_slot(db, slot_id=slot_id, appointment_id=owner, ttl_minutes=10) is True
    assert await hold_slot(db, slot_id=slot_id, appointment_id=other, ttl_minutes=10) is False
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_by_appointment_id) == ("held", owner)


async def test_hold_after_confirm_succeeds_only_for_the_booking_appointment(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    appt = await _pending(db, slot_id)
    assert await hold_slot(db, slot_id=slot_id, appointment_id=appt, ttl_minutes=10)
    assert await confirm_booking(db, appointment_id=appt, slot_id=slot_id)

    # A retried reserve step after confirm is an idempotent success...
    assert await hold_slot(db, slot_id=slot_id, appointment_id=appt, ttl_minutes=10) is True
    # ...but nobody else can take the booked slot, and it stays booked.
    assert await hold_slot(db, slot_id=slot_id, appointment_id=uuid.uuid4(), ttl_minutes=10) is False
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_until, row.held_by_appointment_id) == ("booked", None, appt)


async def test_confirm_books_slot_and_confirms_appointment_once(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    appt = await _pending(db, slot_id)
    await hold_slot(db, slot_id=slot_id, appointment_id=appt, ttl_minutes=10)

    assert await confirm_booking(db, appointment_id=appt, slot_id=slot_id) is True
    assert await confirm_booking(db, appointment_id=appt, slot_id=slot_id) is True

    slot = await _slot_row(db, slot_id)
    assert (slot.status, slot.held_until) == ("booked", None)
    appointment = await _appt_row(db, appt)
    assert (appointment.status, appointment.version) == ("confirmed", 1)
    history = await _history(db, appt)
    assert [(h[0], h[1]) for h in history] == [("pending", "confirmed")]


@pytest.mark.parametrize("reaped_to", ["available", "held_by_other"])
async def test_confirm_after_the_hold_was_lost_changes_nothing(
    scheduling_db_session: AsyncSession, reaped_to: str
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    appt = await _pending(db, slot_id)
    await hold_slot(db, slot_id=slot_id, appointment_id=appt, ttl_minutes=10)
    # The reaper freed the hold (and possibly someone else took it).
    await db.execute(
        text(
            "UPDATE provider_slots SET status = 'available', held_until = NULL, "
            "held_by_appointment_id = NULL WHERE id = :s"
        ),
        {"s": slot_id},
    )
    other = uuid.uuid4()
    if reaped_to == "held_by_other":
        assert await hold_slot(db, slot_id=slot_id, appointment_id=other, ttl_minutes=10)
    before = await _slot_row(db, slot_id)

    assert await confirm_booking(db, appointment_id=appt, slot_id=slot_id) is False

    after = await _slot_row(db, slot_id)
    assert (after.status, after.held_by_appointment_id) == (before.status, before.held_by_appointment_id)
    assert (await _appt_row(db, appt)).status == "pending"
    assert await _history(db, appt) == []


async def test_release_hold_by_owner_is_idempotent(scheduling_db_session: AsyncSession) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    owner = uuid.uuid4()
    await hold_slot(db, slot_id=slot_id, appointment_id=owner, ttl_minutes=10)

    assert await release_hold(db, slot_id=slot_id, appointment_id=owner) is True
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_until, row.held_by_appointment_id) == ("available", None, None)

    assert await release_hold(db, slot_id=slot_id, appointment_id=owner) is True
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_until, row.held_by_appointment_id) == ("available", None, None)


async def test_release_by_non_owner_leaves_the_slot_untouched(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    owner = uuid.uuid4()
    await hold_slot(db, slot_id=slot_id, appointment_id=owner, ttl_minutes=10)

    assert await release_hold(db, slot_id=slot_id, appointment_id=uuid.uuid4()) is False
    row = await _slot_row(db, slot_id)
    assert (row.status, row.held_by_appointment_id) == ("held", owner)


async def test_mark_failed_is_a_forward_idempotent_transition(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    appt = await _pending(db, await _slot(db))

    assert (
        await mark_appointment_failed(db, appointment_id=appt, reason="BillingRejected", actor="system:workflow")
        is True
    )
    assert (
        await mark_appointment_failed(db, appointment_id=appt, reason="BillingRejected", actor="system:workflow")
        is True
    )
    row = await _appt_row(db, appt)
    assert (row.status, row.failure_reason) == ("failed", "BillingRejected")
    assert await _history(db, appt) == [("pending", "failed", "BillingRejected", "system:workflow")]


async def test_mark_failed_refuses_a_confirmed_appointment(scheduling_db_session: AsyncSession) -> None:
    db = scheduling_db_session
    appt = await _pending(db, await _slot(db), status="confirmed")

    assert (
        await mark_appointment_failed(db, appointment_id=appt, reason="BillingRejected", actor="system:workflow")
        is False
    )
    row = await _appt_row(db, appt)
    assert (row.status, row.failure_reason) == ("confirmed", None)
    assert await _history(db, appt) == []


async def test_expire_holds_frees_expired_holds_and_fails_their_appointments(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    expired_slot = await _slot(db, BASE)
    live_slot = await _slot(db, BASE + timedelta(hours=1))
    expired_appt = await _pending(db, expired_slot)
    live_appt = await _pending(db, live_slot)
    await hold_slot(db, slot_id=expired_slot, appointment_id=expired_appt, ttl_minutes=10)
    await hold_slot(db, slot_id=live_slot, appointment_id=live_appt, ttl_minutes=10)
    await db.execute(
        text("UPDATE provider_slots SET held_until = now() - interval '1 minute' WHERE id = :s"),
        {"s": expired_slot},
    )

    expired = await expire_holds(db, limit=1000)

    assert (expired_slot, expired_appt) in expired
    assert all(slot != live_slot for slot, _ in expired)
    slot = await _slot_row(db, expired_slot)
    assert (slot.status, slot.held_until, slot.held_by_appointment_id) == ("available", None, None)
    appointment = await _appt_row(db, expired_appt)
    assert (appointment.status, appointment.failure_reason) == ("failed", "hold_expired")
    assert await _history(db, expired_appt) == [("pending", "failed", "hold_expired", "system:reaper")]

    live = await _slot_row(db, live_slot)
    assert (live.status, live.held_by_appointment_id) == ("held", live_appt)
    assert (await _appt_row(db, live_appt)).status == "pending"

    # A second sweep finds nothing more for these slots.
    again = await expire_holds(db, limit=1000)
    assert all(slot not in (expired_slot, live_slot) for slot, _ in again)


def test_state_steps_never_delete() -> None:
    pattern = re.compile(r"delete\(|DELETE FROM", re.IGNORECASE)
    for name in ("slot_repo.py", "appointment_repo.py", "idempotency_repo.py"):
        source = (REPO_DIR / name).read_text(encoding="utf-8")
        assert not pattern.search(source), name
