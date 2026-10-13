"""Double-booking proofs on the live Postgres (SCHED-03, D-02, D-19).

Fifty transactions race for ONE slot, each in its own session and each really
committing. Exactly one may win; every loser must fail with `SlotTaken` and
leave nothing behind (its idempotency claim rolls back with it). The guarantee
comes from the `uq_appointments_active_slot` partial unique index, so a variant
that bypasses all application code with raw INSERTs must give the same result.

Parallelism is 50 against a pool of 20 connections: the test proves the
constraint, not pool sizing (research Pitfall 11).
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
