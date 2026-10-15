"""Concurrent slot frees never share a waitlist entry (plan 02-09; SCHED-07,
D-02, D-19, research Pattern 7, Pitfall 11; threat T-02-45).

Proven on the real Postgres with COMMITTED, racing transactions:

* repository level: ten claim transactions (one per freed slot, each with its
  own appointment id) race over three waiting entries for one provider; each
  claims with `FOR UPDATE SKIP LOCKED` and creates its appointment in the same
  transaction. Exactly three win, each entry exactly once.
* end to end: two confirmed appointments of one provider are cancelled at the
  same time through the gateway with one waiting entry whose window covers
  both slots. Both cancels start an advance; the entry is booked exactly once.

Bounded parallelism: one pool of at most 20 connections (Pitfall 11).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from temporalio.testing import WorkflowEnvironment

from services.scheduling.activities.waitlist import waitlist_idempotency_key, waitlist_request_hash
from services.scheduling.domain.contracts import (
    ChangeResult,
    WaitlistAdvanceResult,
    cancel_workflow_id,
    waitlist_advance_workflow_id,
)
from services.scheduling.repository import appointment_repo, waitlist_repo
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, auth
from tests.scheduling.test_cancel import day_slots, slot_status
from tests.scheduling.test_waitlist import add_patient, book_for, get_entry, joined, result_of

pytestmark = pytest.mark.integration

FREED_SLOTS = 10
WAITING_ENTRIES = 3


async def test_parallel_claims_take_each_entry_exactly_once(
    committed_scheduling_engine: AsyncEngine,
) -> None:
    engine = committed_scheduling_engine
    provider_id, clinic_id, department_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    base = (datetime.now(timezone.utc) + timedelta(days=3)).replace(second=0, microsecond=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        slots = []
        for i in range(FREED_SLOTS):
            start = base + timedelta(minutes=30 * i)
            slot_id = await seed_slot(
                engine,
                provider_id=provider_id,
                start=start,
                clinic_id=clinic_id,
                department_id=department_id,
            )
            slots.append((slot_id, start, start + timedelta(minutes=30)))
        entry_ids = []
        async with sessions() as db:
            for _ in range(WAITING_ENTRIES):
                entry = await waitlist_repo.create_entry(
                    db,
                    clinic_id=clinic_id,
                    provider_id=provider_id,
                    patient_id=uuid.uuid4(),
                    window_start=None,
                    window_end=None,
                    coverage_ref=None,
                    created_by=uuid.uuid4(),
                )
                entry_ids.append(entry.id)
            await db.commit()

        barrier = asyncio.Barrier(FREED_SLOTS)

        async def free(slot_id: uuid.UUID, start: datetime, end: datetime) -> uuid.UUID | None:
            appointment_id = uuid.uuid4()
            async with sessions() as db:
                await barrier.wait()
                entry = await waitlist_repo.claim_next_entry(
                    db,
                    provider_id=provider_id,
                    slot_start=start,
                    slot_end=end,
                    appointment_id=appointment_id,
                )
                if entry is None:
                    await db.rollback()
                    return None
                claimed = entry.id
                await appointment_repo.accept_booking(
                    db,
                    appointment_id=appointment_id,
                    caller_user_id=entry.created_by,
                    key=waitlist_idempotency_key(str(appointment_id)),
                    request_hash=waitlist_request_hash(slot_id, entry.patient_id, None),
                    clinic_id=clinic_id,
                    patient_id=entry.patient_id,
                    provider_id=provider_id,
                    slot_id=slot_id,
                    slot_start=start,
                    slot_end=end,
                    coverage_ref=None,
                    created_by=entry.created_by,
                    workflow_started=True,
                )
                # Hold the row lock a moment so the other claims really overlap.
                await asyncio.sleep(0.05)
                await db.commit()
                return claimed

        results = await asyncio.gather(*(free(*slot) for slot in slots))

        winners = [r for r in results if r is not None]
        assert len(winners) == WAITING_ENTRIES
        assert sorted(winners) == sorted(entry_ids)  # each entry exactly once
        assert results.count(None) == FREED_SLOTS - WAITING_ENTRIES
        async with engine.connect() as conn:
            appointments = (
                await conn.execute(
                    text(
                        "SELECT status, workflow_started_at IS NOT NULL FROM appointments "
                        "WHERE provider_id = :p"
                    ),
                    {"p": provider_id},
                )
            ).all()
            entries = (
                await conn.execute(
                    text(
                        "SELECT status, attempts, matched_appointment_id IS NOT NULL "
                        "FROM waitlist_entries WHERE provider_id = :p"
                    ),
                    {"p": provider_id},
                )
            ).all()
        assert sorted(appointments) == [("pending", True)] * WAITING_ENTRIES
        assert sorted(entries) == [("matched", 1, True)] * WAITING_ENTRIES
    finally:
        await cleanup_scheduling_rows(engine, [provider_id])


async def test_two_concurrent_cancels_book_the_entry_exactly_once(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    s1, s2 = (await day_slots(client, world))[:2]
    patient_b, _ = await add_patient(client, world)
    patient_c, _ = await add_patient(client, world)
    first = await book_for(client, temporal_env, world, s1["id"], patient_b)
    second = await book_for(client, temporal_env, world, s2["id"], patient_c)
    # One waiting entry whose (open) window covers both freed slots.
    entry = await joined(client, world.patient, {"provider_id": str(world.provider_id)})

    responses = await asyncio.gather(
        *(
            client.post(
                f"/api/appointments/{a['id']}/cancel", json={}, headers=auth(world.front_desk)
            )
            for a in (first, second)
        )
    )
    assert [r.status_code for r in responses] == [202, 202], [r.text for r in responses]

    outcomes = {}
    for appointment, slot in ((first, s1), (second, s2)):
        cancel_id = cancel_workflow_id(appointment["id"], appointment["version"])
        assert (await result_of(temporal_env, cancel_id, ChangeResult)).outcome == "cancelled"
        advance = await result_of(
            temporal_env, waitlist_advance_workflow_id(cancel_id), WaitlistAdvanceResult
        )
        outcomes[slot["id"]] = advance.outcome

    assert sorted(outcomes.values()) == ["booked", "no_match"]
    booked_slot = next(s for s, o in outcomes.items() if o == "booked")
    other_slot = next(s for s, o in outcomes.items() if o == "no_match")
    after = await get_entry(client, world, entry["id"])
    assert after["status"] == "booked" and after["attempts"] == 1
    async with committed_scheduling_engine.connect() as conn:
        created = (
            await conn.execute(
                text(
                    "SELECT id, slot_id, status FROM appointments "
                    "WHERE provider_id = :p AND patient_id = :pt"
                ),
                {"p": world.provider_id, "pt": world.patient_id},
            )
        ).all()
    assert len(created) == 1
    assert str(created[0].id) == after["booked_appointment_id"]
    assert str(created[0].slot_id) == booked_slot and created[0].status == "confirmed"
    assert await slot_status(client, world, booked_slot) == "booked"
    assert await slot_status(client, world, other_slot) == "available"
