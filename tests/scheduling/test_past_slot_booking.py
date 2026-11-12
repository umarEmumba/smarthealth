"""A slot that has already started cannot be booked (plan 02.1-07; S6, T-02.1-28).

* `POST /api/appointments` answers `422 slot has already started` before
  anything is written (no appointment, no idempotency row, slot untouched). The
  check follows the scope check and the idempotency replay, and precedes the
  slot-status check.
* The saga's validate step repeats the check with the DATABASE clock for a slot
  that starts while a booking is pending: the booking fails with `SlotInPast`
  and no hold is ever taken.

Slots are seeded with SQL. The world's generated slots start today, so a slot
seeded near "now" on the world's provider can overlap one and trip
`ex_provider_slots_no_overlap`; the API-level tests therefore seed on a
throwaway provider id (their saga outcome is irrelevant: only the 202/422 and
what was written matter) and the saga-level tests either use a slot days in the
past or a generated slot. Everything is removed by `cleanup_scheduling_rows`
scoped to the provider ids a test used.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.testing import WorkflowEnvironment

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.domain.contracts import (
    BookingInput,
    BookingResult,
    booking_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, _staff, auth
from tests.scheduling.test_booking_api import book, new_key
from tests.scheduling.test_cancel import day_slots, insert_appointment

pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@pytest_asyncio.fixture
async def throwaway_provider(committed_scheduling_engine: AsyncEngine) -> AsyncIterator[uuid.UUID]:
    provider_id = uuid.uuid4()
    try:
        yield provider_id
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [provider_id])


async def _seed(
    engine: AsyncEngine,
    world: World,
    start: datetime,
    *,
    provider_id: uuid.UUID,
    status: str = "available",
) -> str:
    slot_id = await seed_slot(
        engine,
        provider_id=provider_id,
        start=start,
        clinic_id=world.clinic_id,
        department_id=world.department_id,
        status=status,
    )
    return str(slot_id)


async def _counts(engine: AsyncEngine, provider_id: uuid.UUID, key: str) -> tuple[int, int]:
    """(appointments of `provider_id`, idempotency rows for `key`)."""
    async with engine.connect() as conn:
        appointments = await conn.scalar(
            text("SELECT count(*) FROM appointments WHERE provider_id = :p"),
            {"p": provider_id},
        )
        keys = await conn.scalar(
            text("SELECT count(*) FROM idempotency_keys WHERE key = :k"), {"k": key}
        )
    return appointments, keys


async def _slot(engine: AsyncEngine, slot_id: str) -> tuple[str, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, held_by_appointment_id FROM provider_slots WHERE id = :s"),
                {"s": uuid.UUID(slot_id)},
            )
        ).one()
    return row.status, row.held_by_appointment_id


async def test_a_slot_that_started_an_hour_ago_is_422_and_nothing_is_written(
    client: AsyncClient,
    booking_world: World,
    committed_scheduling_engine: AsyncEngine,
    throwaway_provider: uuid.UUID,
) -> None:
    world = booking_world
    engine = committed_scheduling_engine
    slot_id = await _seed(engine, world, _now() - timedelta(hours=1), provider_id=throwaway_provider)
    key = new_key()
    before = await _counts(engine, throwaway_provider, key)

    resp = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=key
    )

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "slot has already started"
    assert await _counts(engine, throwaway_provider, key) == before == (0, 0)
    assert await _slot(engine, slot_id) == ("available", None)


async def test_the_check_precedes_the_status_check_so_a_past_booked_slot_is_422_not_409(
    client: AsyncClient,
    booking_world: World,
    committed_scheduling_engine: AsyncEngine,
    throwaway_provider: uuid.UUID,
) -> None:
    world = booking_world
    slot_id = await _seed(
        committed_scheduling_engine,
        world,
        _now() - timedelta(hours=2),
        provider_id=throwaway_provider,
        status="booked",
    )

    resp = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=new_key()
    )

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "slot has already started"


async def test_a_slot_starting_in_about_a_minute_is_accepted(
    client: AsyncClient,
    booking_world: World,
    committed_scheduling_engine: AsyncEngine,
    throwaway_provider: uuid.UUID,
) -> None:
    world = booking_world
    slot_id = await _seed(
        committed_scheduling_engine,
        world,
        _now() + timedelta(seconds=90),
        provider_id=throwaway_provider,
    )
    key = new_key()

    resp = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=key
    )

    assert resp.status_code == 202, resp.text
    assert await _counts(committed_scheduling_engine, throwaway_provider, key) == (1, 1)


async def test_a_replay_of_an_accepted_booking_still_answers_after_its_slot_is_in_the_past(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """The idempotency replay is answered BEFORE any slot check (D-08)."""
    world = booking_world
    engine = committed_scheduling_engine
    slot_id = (await day_slots(client, world))[0]["id"]
    key = new_key()
    first = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=key
    )
    assert first.status_code == 202, first.text
    appointment_id = first.json()["appointment_id"]
    handle = temporal_env.client.get_workflow_handle(
        booking_workflow_id(appointment_id), result_type=BookingResult
    )
    async with temporal_env.time_skipping_unlocked():
        assert (await handle.result()).outcome == "confirmed"
    # Time passes: the slot (and the appointment's copy of its window) is now over.
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET start_time = now() - interval '3 days', "
                "end_time = now() - interval '3 days' + interval '30 minutes' WHERE id = :s"
            ),
            {"s": uuid.UUID(slot_id)},
        )

    replay = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=key
    )

    assert replay.status_code == 202, replay.text
    assert replay.json()["appointment_id"] == appointment_id
    assert replay.json()["status"] == "confirmed"
    # A DIFFERENT key for the same (now past) slot is refused as past.
    other = await book(
        client, world.front_desk, slot_id=slot_id, patient_id=str(world.patient_id), key=new_key()
    )
    assert other.status_code == 422, other.text
    assert other.json()["detail"] == "slot has already started"


async def test_a_front_desk_of_another_clinic_still_gets_the_identical_404_for_a_past_slot(
    client: AsyncClient,
    booking_world: World,
    committed_scheduling_engine: AsyncEngine,
    throwaway_provider: uuid.UUID,
) -> None:
    """The scope check stays first: the 422 must not reveal a foreign slot."""
    world = booking_world
    slot_id = await _seed(
        committed_scheduling_engine, world, _now() - timedelta(hours=1), provider_id=throwaway_provider
    )
    other_clinic = await client.post(
        "/api/clinics",
        json={"name": f"Past Clinic {uuid.uuid4().hex[:6]}", "timezone": "UTC"},
        headers=auth(world.admin),
    )
    assert other_clinic.status_code == 201, other_clinic.text
    _, outsider = await _staff(client, world.admin, "front_desk", other_clinic.json()["id"])

    resp = await book(
        client, outsider, slot_id=slot_id, patient_id=str(world.patient_id), key=new_key()
    )

    assert resp.status_code == 404, resp.text


# --- saga validate ----------------------------------------------------------------------


async def test_validate_fails_a_pending_booking_whose_slot_has_started_with_slot_in_past(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    engine = committed_scheduling_engine
    appointment_id, slot_id = await insert_appointment(
        engine, world, status="pending", start=_now() - timedelta(days=3)
    )
    inp = BookingInput(
        appointment_id=appointment_id,
        clinic_id=str(world.clinic_id),
        patient_id=str(world.patient_id),
        provider_id=str(world.provider_id),
        slot_id=slot_id,
    )
    handle = await temporal_env.client.start_workflow(
        BookingWorkflow.run,
        inp,
        id=booking_workflow_id(appointment_id),
        task_queue=SCHEDULING_TASK_QUEUE,
        result_type=BookingResult,
    )
    result = await handle.result()

    assert result == BookingResult(outcome="failed", reason="SlotInPast")
    # No hold was ever taken: the slot is exactly as seeded.
    assert await _slot(engine, slot_id) == ("available", None)
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, failure_reason FROM appointments WHERE id = :a"),
                {"a": uuid.UUID(appointment_id)},
            )
        ).one()
    assert (row.status, row.failure_reason) == ("failed", "SlotInPast")
    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    wf = booking_workflow_id(appointment_id)
    assert await audit.count_documents({"dedupe_key": f"{wf}:reserve_slot"}) == 0
    # Only the final failure write is audited; nothing needed releasing.
    assert await audit.count_documents({"dedupe_key": f"{wf}:release_slot"}) == 0
    # A past slot never reaches the waitlist re-offer (nothing was held).
    child_starts = [
        e
        async for e in handle.fetch_history_events()
        if e.HasField("start_child_workflow_execution_initiated_event_attributes")
    ]
    assert child_starts == []
