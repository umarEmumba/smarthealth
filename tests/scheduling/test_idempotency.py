"""Idempotency-key arbitration on the live Postgres (SCHED-04, D-08).

These tests use committed transactions: duplicate requests are separate
transactions that really race, so only the `(caller_user_id, key)` primary key
can arbitrate them. Expiry is exercised by setting `expires_at` in the past
explicitly, because nothing in a test can advance the database clock
(research Pitfall 8).
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.scheduling.domain.errors import IdempotencyKeyReused
from services.scheduling.repository.appointment_repo import AcceptResult, accept_booking
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot

pytestmark = pytest.mark.integration

START = datetime(2027, 5, 3, 10, 0, tzinfo=timezone.utc)


def _hash(body: str) -> str:
    return hashlib.sha256(body.encode()).hexdigest()


class _Booking:
    """One provider's slots plus a helper that runs `accept_booking` in its own
    committed transaction."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.factory = async_sessionmaker(bind=engine, expire_on_commit=False)
        self.provider_id = uuid.uuid4()
        self.clinic_id = uuid.uuid4()
        self.department_id = uuid.uuid4()

    async def slot(self, start: datetime) -> uuid.UUID:
        return await seed_slot(
            self.engine,
            provider_id=self.provider_id,
            start=start,
            clinic_id=self.clinic_id,
            department_id=self.department_id,
        )

    async def accept(
        self,
        *,
        caller: uuid.UUID,
        key: str,
        request_hash: str,
        slot_id: uuid.UUID,
        start: datetime,
        patient_id: uuid.UUID,
    ) -> AcceptResult:
        async with self.factory() as session, session.begin():
            return await accept_booking(
                session,
                appointment_id=uuid.uuid4(),
                caller_user_id=caller,
                key=key,
                request_hash=request_hash,
                clinic_id=self.clinic_id,
                patient_id=patient_id,
                provider_id=self.provider_id,
                slot_id=slot_id,
                slot_start=start,
                slot_end=start + timedelta(minutes=30),
                coverage_ref=None,
                created_by=caller,
            )

    async def appointment_count(self) -> int:
        async with self.engine.connect() as conn:
            return await conn.scalar(
                text("SELECT count(*) FROM appointments WHERE provider_id = :p"),
                {"p": self.provider_id},
            )


async def test_forty_parallel_duplicates_create_one_appointment(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    booking = _Booking(committed_scheduling_engine)
    caller, patient = uuid.uuid4(), uuid.uuid4()
    request_hash = _hash('{"slot":"same"}')
    try:
        slot_id = await booking.slot(START)
        outcomes = await asyncio.gather(
            *(
                booking.accept(
                    caller=caller,
                    key="dup-key",
                    request_hash=request_hash,
                    slot_id=slot_id,
                    start=START,
                    patient_id=patient,
                )
                for _ in range(40)
            ),
            return_exceptions=True,
        )
        assert all(isinstance(o, AcceptResult) for o in outcomes), outcomes
        originals = [o for o in outcomes if not o.replayed]
        replays = [o for o in outcomes if o.replayed]
        assert len(originals) == 1
        assert len(replays) == 39
        assert {o.appointment_id for o in outcomes} == {originals[0].appointment_id}
        assert await booking.appointment_count() == 1
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [booking.provider_id])


async def test_same_key_with_a_different_body_is_rejected(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    booking = _Booking(committed_scheduling_engine)
    caller, patient = uuid.uuid4(), uuid.uuid4()
    try:
        first_slot = await booking.slot(START)
        other_slot = await booking.slot(START + timedelta(hours=1))
        first = await booking.accept(
            caller=caller,
            key="reused",
            request_hash=_hash("body-a"),
            slot_id=first_slot,
            start=START,
            patient_id=patient,
        )
        with pytest.raises(IdempotencyKeyReused):
            await booking.accept(
                caller=caller,
                key="reused",
                request_hash=_hash("body-b"),
                slot_id=other_slot,
                start=START + timedelta(hours=1),
                patient_id=patient,
            )
        assert await booking.appointment_count() == 1

        # The same body still replays the original.
        replay = await booking.accept(
            caller=caller,
            key="reused",
            request_hash=_hash("body-a"),
            slot_id=first_slot,
            start=START,
            patient_id=patient,
        )
        assert replay == AcceptResult(first.appointment_id, replayed=True)
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [booking.provider_id])


async def test_key_scope_is_per_caller(committed_scheduling_engine: AsyncEngine) -> None:
    """Two callers may use the same key string independently."""
    booking = _Booking(committed_scheduling_engine)
    try:
        slot_a = await booking.slot(START)
        slot_b = await booking.slot(START + timedelta(hours=1))
        a = await booking.accept(
            caller=uuid.uuid4(), key="shared", request_hash=_hash("x"),
            slot_id=slot_a, start=START, patient_id=uuid.uuid4(),
        )
        b = await booking.accept(
            caller=uuid.uuid4(), key="shared", request_hash=_hash("x"),
            slot_id=slot_b, start=START + timedelta(hours=1), patient_id=uuid.uuid4(),
        )
        assert not a.replayed and not b.replayed
        assert a.appointment_id != b.appointment_id
        assert await booking.appointment_count() == 2
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [booking.provider_id])


async def test_expired_key_is_reclaimed_lazily_without_delete(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    booking = _Booking(committed_scheduling_engine)
    caller, patient = uuid.uuid4(), uuid.uuid4()
    try:
        first_slot = await booking.slot(START)
        second_slot = await booking.slot(START + timedelta(hours=2))
        first = await booking.accept(
            caller=caller, key="old", request_hash=_hash("first"),
            slot_id=first_slot, start=START, patient_id=patient,
        )
        async with committed_scheduling_engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE idempotency_keys SET expires_at = now() - interval '1 minute' "
                    "WHERE caller_user_id = :c AND key = 'old'"
                ),
                {"c": caller},
            )

        second = await booking.accept(
            caller=caller, key="old", request_hash=_hash("second"),
            slot_id=second_slot, start=START + timedelta(hours=2), patient_id=patient,
        )
        assert second.replayed is False
        assert second.appointment_id != first.appointment_id
        assert await booking.appointment_count() == 2

        async with committed_scheduling_engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT appointment_id, request_hash, expires_at > now() AS live "
                        "FROM idempotency_keys WHERE caller_user_id = :c"
                    ),
                    {"c": caller},
                )
            ).one()
        assert row.appointment_id == second.appointment_id
        assert row.request_hash == _hash("second")
        assert row.live is True
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [booking.provider_id])
