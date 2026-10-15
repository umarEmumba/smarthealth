"""Double-booking proofs on the live Postgres (SCHED-03, D-02, D-19).

Fifty transactions race for ONE slot, each in its own session and each really
committing. Exactly one may win; every loser must fail with `SlotTaken` and
leave nothing behind (its idempotency claim rolls back with it). The guarantee
comes from the `uq_appointments_active_slot` partial unique index, so a variant
that bypasses all application code with raw INSERTs must give the same result.

Parallelism is 50 against a pool of 20 connections: the test proves the
constraint, not pool sizing (research Pitfall 11).

Plan 02-07 adds the HTTP tier (research Code Example 5 b): 20 parallel
bookings through the gateway with the saga running, one 202 and one confirmed
appointment.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.scheduling.domain.errors import SlotTaken
from services.scheduling.repository.appointment_repo import AcceptResult, accept_booking
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot

pytestmark = pytest.mark.integration

PARALLELISM = 50


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def test_fifty_parallel_accepts_produce_exactly_one_active_appointment(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    engine = committed_scheduling_engine
    provider_id, clinic_id = uuid.uuid4(), uuid.uuid4()
    caller_id = uuid.uuid4()
    start = datetime(2027, 3, 1, 9, 0, tzinfo=timezone.utc)
    slot_id = await seed_slot(
        engine, provider_id=provider_id, start=start, clinic_id=clinic_id, department_id=uuid.uuid4()
    )
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def attempt(i: int) -> AcceptResult | Exception:
        try:
            async with factory() as session, session.begin():
                return await accept_booking(
                    session,
                    appointment_id=uuid.uuid4(),
                    caller_user_id=caller_id,
                    key=f"key-{i}",
                    request_hash=_hash(f"body-{i}"),
                    clinic_id=clinic_id,
                    patient_id=uuid.uuid4(),
                    provider_id=provider_id,
                    slot_id=slot_id,
                    slot_start=start,
                    slot_end=start + timedelta(minutes=30),
                    coverage_ref=None,
                    created_by=caller_id,
                )
        except Exception as exc:  # noqa: BLE001 - outcomes are tallied below
            return exc

    try:
        outcomes = await asyncio.gather(*(attempt(i) for i in range(PARALLELISM)))

        winners = [o for o in outcomes if isinstance(o, AcceptResult)]
        losers = [o for o in outcomes if isinstance(o, SlotTaken)]
        unexpected = [o for o in outcomes if not isinstance(o, (AcceptResult, SlotTaken))]
        assert not unexpected, unexpected
        assert len(winners) == 1
        assert len(losers) == PARALLELISM - 1
        assert winners[0].replayed is False

        async with engine.connect() as conn:
            active = await conn.scalar(
                text(
                    "SELECT count(*) FROM appointments "
                    "WHERE slot_id = :s AND status IN ('pending','confirmed')"
                ),
                {"s": slot_id},
            )
            assert active == 1
            # Losers' key claims rolled back with their appointment insert.
            keys = await conn.scalar(
                text("SELECT count(*) FROM idempotency_keys WHERE caller_user_id = :c"),
                {"c": caller_id},
            )
            assert keys == 1
            winner_keys = await conn.scalar(
                text("SELECT count(*) FROM idempotency_keys WHERE appointment_id = :a"),
                {"a": winners[0].appointment_id},
            )
            assert winner_keys == 1
            history = await conn.scalar(
                text(
                    "SELECT count(*) FROM appointment_status_history "
                    "WHERE appointment_id = :a AND from_status IS NULL AND to_status = 'pending'"
                ),
                {"a": winners[0].appointment_id},
            )
            assert history == 1
    finally:
        await cleanup_scheduling_rows(engine, [provider_id])


async def test_fifty_parallel_raw_inserts_leave_exactly_one_active_row(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """Same race with every line of application code bypassed: plain INSERTs,
    each in its own committed transaction. Only the partial unique index stands
    between them and a double booking."""
    from sqlalchemy.exc import IntegrityError  # noqa: PLC0415

    engine = committed_scheduling_engine
    provider_id, clinic_id = uuid.uuid4(), uuid.uuid4()
    start = datetime(2027, 3, 2, 9, 0, tzinfo=timezone.utc)
    slot_id = await seed_slot(
        engine, provider_id=provider_id, start=start, clinic_id=clinic_id, department_id=uuid.uuid4()
    )

    async def raw_insert() -> str:
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO appointments (clinic_id, patient_id, provider_id, slot_id, "
                        "slot_start, slot_end, status, created_by) "
                        "VALUES (:c, gen_random_uuid(), :p, :s, :st, :en, 'pending', gen_random_uuid())"
                    ),
                    {
                        "c": clinic_id,
                        "p": provider_id,
                        "s": slot_id,
                        "st": start,
                        "en": start + timedelta(minutes=30),
                    },
                )
            return "won"
        except IntegrityError as exc:
            assert "uq_appointments_active_slot" in str(exc.orig)
            return "lost"

    try:
        outcomes = await asyncio.gather(*(raw_insert() for _ in range(PARALLELISM)))
        assert outcomes.count("won") == 1
        assert outcomes.count("lost") == PARALLELISM - 1
        async with engine.connect() as conn:
            rows = await conn.scalar(
                text("SELECT count(*) FROM appointments WHERE slot_id = :s"), {"s": slot_id}
            )
        assert rows == 1
    finally:
        await cleanup_scheduling_rows(engine, [provider_id])


# --- plan 02-07: the same proof at the HTTP layer (SCHED-03, D-19) -------------

HTTP_PARALLELISM = 20


async def test_twenty_parallel_http_bookings_for_one_slot_yield_one_202_and_one_confirmed(
    client, booking_world, scheduling_worker, temporal_env, committed_scheduling_engine
) -> None:
    """Research Code Example 5 tier (b): 20 front-desk POST /api/appointments
    through the gateway, distinct Idempotency-Keys and distinct patients of the
    same clinic, all for ONE slot, while a Temporal test worker runs the saga.
    Exactly one request is accepted; the rest are 409; after the winner's saga,
    exactly one appointment for the slot is confirmed and the slot is booked.
    Parallelism stays at 20 (research Pitfall 11); the suite lifts the gateway
    rate limits (tests/conftest.py)."""
    from services.scheduling.domain.contracts import BookingResult, booking_workflow_id  # noqa: PLC0415
    from tests.scheduling.seed import auth  # noqa: PLC0415

    world = booking_world
    patient_ids = [str(world.patient_id)]
    for i in range(HTTP_PARALLELISM - 1):
        created = await client.post(
            "/api/patients",
            json={
                "clinic_id": str(world.clinic_id),
                "full_name": f"Zzyzx Race Synthetic {i}",
                "date_of_birth": "1990-01-01",
            },
            headers=auth(world.front_desk),
        )
        assert created.status_code == 201, created.text
        patient_ids.append(created.json()["id"])

    start, end = world.local_day_bounds(world.last_day)
    slots = await client.get(
        "/api/slots",
        params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
        headers=auth(world.admin),
    )
    slot_id = slots.json()[0]["id"]

    async def book(patient_id: str):
        return await client.post(
            "/api/appointments",
            json={"slot_id": slot_id, "patient_id": patient_id},
            headers=auth(world.front_desk) | {"Idempotency-Key": f"race-{uuid.uuid4().hex}"},
        )

    responses = await asyncio.gather(*(book(p) for p in patient_ids))

    codes = sorted(r.status_code for r in responses)
    assert codes == [202] + [409] * (HTTP_PARALLELISM - 1), [r.text for r in responses if r.status_code not in (202, 409)]
    assert {r.json()["detail"] for r in responses if r.status_code == 409} == {"slot not available"}
    winner = next(r for r in responses if r.status_code == 202).json()["appointment_id"]

    handle = temporal_env.client.get_workflow_handle(booking_workflow_id(winner), result_type=BookingResult)
    async with temporal_env.time_skipping_unlocked():
        assert (await handle.result()).outcome == "confirmed"

    async with committed_scheduling_engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT id, status FROM appointments WHERE slot_id = :s"), {"s": uuid.UUID(slot_id)}
            )
        ).all()
        slot_status = await conn.scalar(
            text("SELECT status FROM provider_slots WHERE id = :s"), {"s": uuid.UUID(slot_id)}
        )
    assert [(str(r.id), r.status) for r in rows] == [(winner, "confirmed")]
    assert slot_status == "booked"
