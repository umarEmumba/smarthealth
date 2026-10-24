"""Raw-SQL proofs of Scheduling's appointment and slot constraints (D-02, WR-07).

Every statement here is plain SQL, so no application code is involved: the
guarantees come from Postgres alone ("enforced at the data layer", SCHED-03).
Each test runs inside the rolled-back `scheduling_db_session`; an expected
violation is flushed inside `begin_nested()` (a SAVEPOINT) so only that
statement is undone, and the constraint NAME is asserted so the test cannot
pass on an unrelated error.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.integration

BASE = datetime(2027, 4, 5, 9, 0, tzinfo=timezone.utc)


async def _slot(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID | None = None,
    start: datetime = BASE,
    minutes: int = 30,
    status: str = "available",
) -> uuid.UUID:
    return await db.scalar(
        text(
            "INSERT INTO provider_slots "
            "(provider_id, clinic_id, department_id, start_time, end_time, status) "
            "VALUES (:p, :c, :d, :s, :e, :st) RETURNING id"
        ),
        {
            "p": provider_id or uuid.uuid4(),
            "c": uuid.uuid4(),
            "d": uuid.uuid4(),
            "s": start,
            "e": start + timedelta(minutes=minutes),
            "st": status,
        },
    )


async def _appointment(
    db: AsyncSession,
    *,
    slot_id: uuid.UUID,
    patient_id: uuid.UUID | None = None,
    start: datetime = BASE,
    minutes: int = 30,
    status: str = "pending",
) -> uuid.UUID:
    return await db.scalar(
        text(
            "INSERT INTO appointments "
            "(clinic_id, patient_id, provider_id, slot_id, slot_start, slot_end, status, created_by) "
            "VALUES (:c, :pt, :pr, :sl, :s, :e, :st, :cb) RETURNING id"
        ),
        {
            "c": uuid.uuid4(),
            "pt": patient_id or uuid.uuid4(),
            "pr": uuid.uuid4(),
            "sl": slot_id,
            "s": start,
            "e": start + timedelta(minutes=minutes),
            "st": status,
            "cb": uuid.uuid4(),
        },
    )


async def test_second_active_appointment_for_one_slot_is_rejected(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    await _appointment(db, slot_id=slot_id, status="pending")

    for second_status in ("pending", "confirmed"):
        with pytest.raises(IntegrityError) as excinfo:
            async with db.begin_nested():
                await _appointment(db, slot_id=slot_id, status=second_status)
        assert "uq_appointments_active_slot" in str(excinfo.value)


async def test_cancelling_the_active_appointment_frees_the_slot(
    scheduling_db_session: AsyncSession,
) -> None:
    """Leaving the index predicate (no delete) makes the slot bookable again."""
    db = scheduling_db_session
    slot_id = await _slot(db)
    first = await _appointment(db, slot_id=slot_id, status="confirmed")

    await db.execute(text("UPDATE appointments SET status = 'cancelled' WHERE id = :a"), {"a": first})
    await _appointment(db, slot_id=slot_id, status="pending")

    rows = (
        await db.execute(
            text("SELECT status FROM appointments WHERE slot_id = :s ORDER BY created_at"),
            {"s": slot_id},
        )
    ).scalars().all()
    assert sorted(rows) == ["cancelled", "pending"]


async def test_failed_appointments_do_not_occupy_the_slot(scheduling_db_session: AsyncSession) -> None:
    db = scheduling_db_session
    slot_id = await _slot(db)
    await _appointment(db, slot_id=slot_id, status="failed")
    await _appointment(db, slot_id=slot_id, status="failed")
    await _appointment(db, slot_id=slot_id, status="pending")


async def test_patient_cannot_hold_two_overlapping_active_appointments(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    patient_id = uuid.uuid4()
    first_slot = await _slot(db, start=BASE)
    other_slot = await _slot(db, start=BASE + timedelta(minutes=15))  # other provider
    await _appointment(db, slot_id=first_slot, patient_id=patient_id, start=BASE)

    with pytest.raises(IntegrityError) as excinfo:
        async with db.begin_nested():
            await _appointment(
                db, slot_id=other_slot, patient_id=patient_id, start=BASE + timedelta(minutes=15)
            )
    assert "ex_appointments_patient_no_overlap" in str(excinfo.value)


async def test_patient_back_to_back_appointments_are_accepted(
    scheduling_db_session: AsyncSession,
) -> None:
    """tstzrange is half-open: an appointment ending as the next starts does not overlap."""
    db = scheduling_db_session
    patient_id = uuid.uuid4()
    first_slot = await _slot(db, start=BASE)
    next_slot = await _slot(db, start=BASE + timedelta(minutes=30))
    await _appointment(db, slot_id=first_slot, patient_id=patient_id, start=BASE)
    await _appointment(
        db, slot_id=next_slot, patient_id=patient_id, start=BASE + timedelta(minutes=30)
    )


async def test_failed_overlapping_appointment_does_not_block_the_patient(
    scheduling_db_session: AsyncSession,
) -> None:
    db = scheduling_db_session
    patient_id = uuid.uuid4()
    first_slot = await _slot(db, start=BASE)
    other_slot = await _slot(db, start=BASE)
    await _appointment(db, slot_id=first_slot, patient_id=patient_id, status="failed")
    await _appointment(db, slot_id=other_slot, patient_id=patient_id, status="pending")


async def test_zero_length_or_inverted_slot_is_rejected(scheduling_db_session: AsyncSession) -> None:
    db = scheduling_db_session
    for minutes in (0, -30):
        with pytest.raises(IntegrityError) as excinfo:
            async with db.begin_nested():
                await _slot(db, minutes=minutes)
        assert "ck_provider_slots_end_after_start" in str(excinfo.value)


async def test_one_provider_cannot_have_two_slots_with_the_same_start(
    scheduling_db_session: AsyncSession,
) -> None:
    """Even a cancelled twin is rejected (the overlap constraint ignores
    cancelled rows; this natural key does not), so slot generation is idempotent."""
    db = scheduling_db_session
    provider_id = uuid.uuid4()
    await _slot(db, provider_id=provider_id, status="cancelled")

    with pytest.raises(IntegrityError) as excinfo:
        async with db.begin_nested():
            await _slot(db, provider_id=provider_id, minutes=60)
    assert "uq_provider_slots_provider_start" in str(excinfo.value)


async def test_appointment_status_outside_vocabulary_is_rejected(
    scheduling_db_session: AsyncSession,
) -> None:
    """Only the seven statuses of the state machine are valid (M1); `completed`
    is one of them now, so the invalid examples are values outside the set."""
    db = scheduling_db_session
    slot_id = await _slot(db)
    for bad in ("bogus", "booked", "PENDING"):
        with pytest.raises(IntegrityError) as excinfo:
            async with db.begin_nested():
                await _appointment(db, slot_id=slot_id, status=bad)
        assert "ck_appointments_status_valid" in str(excinfo.value)


async def test_checked_in_completed_and_no_show_pass_the_status_check_and_keep_the_slot(
    scheduling_db_session: AsyncSession,
) -> None:
    """`completed` and `no_show` (and `checked_in`) are valid statuses that stay
    inside the active-slot predicate, so each holds its slot (plan 02.1-01)."""
    db = scheduling_db_session
    for status in ("checked_in", "completed", "no_show"):
        slot_id = await _slot(db)
        await _appointment(db, slot_id=slot_id, status=status)
        with pytest.raises(IntegrityError) as excinfo:
            async with db.begin_nested():
                await _appointment(db, slot_id=slot_id, status="pending")
        assert "uq_appointments_active_slot" in str(excinfo.value)


async def test_appointment_must_reference_an_existing_slot(scheduling_db_session: AsyncSession) -> None:
    db = scheduling_db_session
    with pytest.raises(IntegrityError) as excinfo:
        async with db.begin_nested():
            await _appointment(db, slot_id=uuid.uuid4())
    assert "fk_appointments_slot_id" in str(excinfo.value)
