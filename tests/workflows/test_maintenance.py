"""Hold reaper and pending-booking starter (plan 02-07 Task 2; D-03, research
G7 / Pattern 8, Pitfalls 4, 5, 8).

The maintenance activities run in isolation through `ActivityEnvironment`
against the real scheduling_db and audit sink. The database's `now()` never
skips with the Temporal test server (Pitfall 8), so expiry and grace windows
are set up with explicit past timestamps instead of waiting.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from libs.db.postgres import get_session_factory
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.activities.maintenance import MaintenanceActivities
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_BILLING,
    BookingInput,
    BookingResult,
    MaintenanceResult,
    booking_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from tests.conftest import harness_apps
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, auth

pytestmark = pytest.mark.integration


# --- helpers -------------------------------------------------------------------


async def _seed_pending(
    engine: AsyncEngine,
    *,
    provider_id: uuid.UUID,
    start: datetime,
    held_until_offset: timedelta,
) -> tuple[uuid.UUID, uuid.UUID]:
    """A slot held by a fresh `pending` appointment until now() + offset."""
    clinic_id = uuid.uuid4()
    slot_id = await seed_slot(
        engine, provider_id=provider_id, start=start, clinic_id=clinic_id, department_id=uuid.uuid4()
    )
    appointment_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
                "slot_start, slot_end, status, created_by) "
                "VALUES (:a, :c, gen_random_uuid(), :p, :s, :st, :en, 'pending', gen_random_uuid())"
            ),
            {
                "a": appointment_id,
                "c": clinic_id,
                "p": provider_id,
                "s": slot_id,
                "st": start,
                "en": start + timedelta(minutes=30),
            },
        )
        await conn.execute(
            text(
                "INSERT INTO appointment_status_history (appointment_id, from_status, to_status, actor) "
                "VALUES (:a, NULL, 'pending', 'test')"
            ),
            {"a": appointment_id},
        )
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = 'held', held_by_appointment_id = :a, "
                "held_until = now() + make_interval(secs => :o) WHERE id = :s"
            ),
            {"a": appointment_id, "s": slot_id, "o": held_until_offset.total_seconds()},
        )
    return slot_id, appointment_id


async def _slot(engine: AsyncEngine, slot_id: uuid.UUID) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text("SELECT status, held_by_appointment_id, held_until FROM provider_slots WHERE id = :s"),
                {"s": slot_id},
            )
        ).one()


async def _appointment(engine: AsyncEngine, appointment_id: uuid.UUID | str) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, failure_reason, workflow_started_at FROM appointments WHERE id = :a"
                ),
                {"a": uuid.UUID(str(appointment_id))},
            )
        ).one()


async def _history(engine: AsyncEngine, appointment_id: uuid.UUID | str) -> list[tuple]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT from_status, to_status, reason, actor FROM appointment_status_history "
                    "WHERE appointment_id = :a ORDER BY created_at, id"
                ),
                {"a": uuid.UUID(str(appointment_id))},
            )
        ).all()
    return [tuple(r) for r in rows]


async def _audit_count(mongo: AsyncMongoClient, dedupe_key: str) -> int:
    return await mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME].count_documents(
        {"dedupe_key": dedupe_key}
    )


def _maintenance(client: Any, mongo: AsyncMongoClient) -> MaintenanceActivities:
    return MaintenanceActivities(get_session_factory(settings.database_url), client, mongo)


@pytest_asyncio.fixture
async def provider_scope(committed_scheduling_engine: AsyncEngine) -> AsyncIterator[uuid.UUID]:
    provider_id = uuid.uuid4()
    try:
        yield provider_id
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [provider_id])


# --- hold reaper -----------------------------------------------------------------


async def test_reaper_releases_expired_holds_and_fails_their_appointments_once(
    committed_scheduling_engine: AsyncEngine,
    provider_scope: uuid.UUID,
    mongo_client: AsyncMongoClient,
) -> None:
    engine = committed_scheduling_engine
    base = datetime(2027, 6, 1, 9, 0, tzinfo=timezone.utc)
    expired_slot, expired_appt = await _seed_pending(
        engine, provider_id=provider_scope, start=base, held_until_offset=timedelta(minutes=-1)
    )
    live_slot, live_appt = await _seed_pending(
        engine,
        provider_id=provider_scope,
        start=base + timedelta(hours=1),
        held_until_offset=timedelta(minutes=10),
    )
    maintenance = _maintenance(None, mongo_client)

    first = await ActivityEnvironment().run(maintenance.expire_holds)

    assert isinstance(first, MaintenanceResult)
    assert first.processed >= 1
    slot = await _slot(engine, expired_slot)
    assert (slot.status, slot.held_by_appointment_id, slot.held_until) == ("available", None, None)
    appointment = await _appointment(engine, expired_appt)
    assert (appointment.status, appointment.failure_reason) == ("failed", "hold_expired")
    assert await _history(engine, expired_appt) == [
        (None, "pending", None, "test"),
        ("pending", "failed", "hold_expired", "system:reaper"),
    ]
    assert await _audit_count(mongo_client, f"reaper:{expired_appt}") == 1

    # The live hold is untouched.
    live = await _slot(engine, live_slot)
    assert (live.status, live.held_by_appointment_id) == ("held", live_appt)
    assert (await _appointment(engine, live_appt)).status == "pending"
    assert await _audit_count(mongo_client, f"reaper:{live_appt}") == 0

    # A second run finds nothing left to do for this pair.
    await ActivityEnvironment().run(maintenance.expire_holds)
    assert await _audit_count(mongo_client, f"reaper:{expired_appt}") == 1
    assert len(await _history(engine, expired_appt)) == 2
    assert (await _slot(engine, live_slot)).status == "held"


async def _book(client: AsyncClient, world: World, slot_id: str) -> str:
    resp = await client.post(
        "/api/appointments",
        json={"slot_id": slot_id, "patient_id": str(world.patient_id)},
        headers=auth(world.front_desk) | {"Idempotency-Key": f"test-{uuid.uuid4().hex}"},
    )
    assert resp.status_code == 202, resp.text
    return resp.json()["appointment_id"]


async def _slots(client: AsyncClient, world: World) -> list[dict]:
    start, end = world.local_day_bounds(world.last_day)
    resp = await client.get(
        "/api/slots",
        params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
        headers=auth(world.admin),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_saga_whose_hold_was_reaped_before_confirm_compensates_and_never_confirms(
    client: AsyncClient,
    booking_world: World,
    temporal_env: WorkflowEnvironment,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """Research Pitfall 5: the reaper wins the race against a live saga. The
    billing pre-check (the last step before confirm since plan 02.1-07 moved the
    notification after it) is wrapped by a stub that runs the real pre-check,
    then expires the hold and runs the real reaper, so `confirm` finds its hold
    gone."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415
    from services.scheduling.activities.booking import SchedulingActivities  # noqa: PLC0415

    world = booking_world
    engine = committed_scheduling_engine
    maintenance = _maintenance(temporal_env.client, mongo_client)

    booking = None  # bound below, once the real activities exist

    @activity.defn(name=ACT_BILLING)
    async def precheck_then_reap(inp: Any) -> None:
        assert booking is not None
        await booking.billing_precheck(BookingInput(**inp))
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE provider_slots SET held_until = now() - interval '1 minute' WHERE id = :s"),
                {"s": uuid.UUID(inp["slot_id"])},
            )
        await maintenance.expire_holds()

    tokens = ServiceTokenProvider(
        "scheduling", load_client_secret(settings.client_secret_path), settings.identity_service_url, http=mesh
    )
    booking = SchedulingActivities(get_session_factory(settings.database_url), mesh, tokens, mongo_client)
    activities = [a for a in booking.all() if a != booking.billing_precheck]
    activities.append(precheck_then_reap)

    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = temporal_env.client
    try:
        async with Worker(
            temporal_env.client,
            task_queue=SCHEDULING_TASK_QUEUE,
            workflows=[BookingWorkflow],
            activities=activities,
        ):
            slot_id = (await _slots(client, world))[0]["id"]
            appointment_id = await _book(client, world, slot_id)
            handle = temporal_env.client.get_workflow_handle(
                booking_workflow_id(appointment_id), result_type=BookingResult
            )
            async with temporal_env.time_skipping_unlocked():
                result = await handle.result()
    finally:
        app.state.temporal_client = previous

    assert result == BookingResult(outcome="failed", reason="ConfirmLostHold")
    appointment = await _appointment(engine, appointment_id)
    # The reaper's failure stands; the saga never confirmed it.
    assert (appointment.status, appointment.failure_reason) == ("failed", "hold_expired")
    assert [(h[0], h[1]) for h in await _history(engine, appointment_id)] == [
        (None, "pending"),
        ("pending", "failed"),
    ]
    slot = await _slot(engine, uuid.UUID(slot_id))
    assert (slot.status, slot.held_by_appointment_id) == ("available", None)
    wf = booking_workflow_id(appointment_id)
    assert await _audit_count(mongo_client, f"{wf}:confirm") == 0
    assert await _audit_count(mongo_client, f"reaper:{appointment_id}") == 1


# --- pending-booking starter -------------------------------------------------------


class _DownTemporal:
    """The API's Temporal client while Temporal is unreachable."""

    async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
        raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")


async def _accept_without_workflow(client: AsyncClient, world: World, slot_id: str) -> str:
    """POST a booking while Temporal is down: committed `pending`, no workflow."""
    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()
    try:
        return await _book(client, world, slot_id)
    finally:
        app.state.temporal_client = previous


async def _age(engine: AsyncEngine, appointment_id: str, seconds: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE appointments SET created_at = now() - make_interval(secs => :s) WHERE id = :a"),
            {"a": uuid.UUID(appointment_id), "s": seconds},
        )


async def test_starter_starts_orphaned_pending_bookings_once_and_respects_the_grace(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    engine = committed_scheduling_engine
    first_slot, second_slot = (await _slots(client, world))[:2]
    orphan = await _accept_without_workflow(client, world, first_slot["id"])
    young = await _accept_without_workflow(client, world, second_slot["id"])
    await _age(engine, orphan, 60)
    await _age(engine, young, 5)
    maintenance = _maintenance(temporal_env.client, mongo_client)

    result = await ActivityEnvironment().run(maintenance.start_pending_bookings)

    assert result.processed >= 1
    marker = (await _appointment(engine, orphan)).workflow_started_at
    assert marker is not None
    handle = temporal_env.client.get_workflow_handle(booking_workflow_id(orphan), result_type=BookingResult)
    async with temporal_env.time_skipping_unlocked():
        assert (await handle.result()).outcome == "confirmed"
    first_run = (await handle.describe()).run_id
    assert (await _appointment(engine, orphan)).status == "confirmed"

    # Inside the grace window: left alone, no workflow.
    assert (await _appointment(engine, young)).workflow_started_at is None
    with pytest.raises(RPCError) as missing:
        await temporal_env.client.get_workflow_handle(booking_workflow_id(young)).describe()
    assert missing.value.status == RPCStatusCode.NOT_FOUND

    # A second run starts nothing new: the marker is set.
    await ActivityEnvironment().run(maintenance.start_pending_bookings)
    assert (await _appointment(engine, orphan)).workflow_started_at == marker
    assert (await handle.describe()).run_id == first_run



async def test_starter_duplicate_start_is_a_no_op_for_running_and_closed_workflows(
    client: AsyncClient,
    booking_world: World,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """The API started the saga but crashed before setting the marker. With no
    worker polling, the run stays open: the starter attaches to it (no second
    run). After the run is closed, a start is refused (REJECT_DUPLICATE) and
    still only restores the marker."""
    from libs.temporal import start_once  # noqa: PLC0415
    from services.scheduling.domain.contracts import BOOKING_WORKFLOW, BookingInput  # noqa: PLC0415

    world = booking_world
    engine = committed_scheduling_engine
    slot = (await _slots(client, world))[0]
    orphan = await _accept_without_workflow(client, world, slot["id"])
    await _age(engine, orphan, 60)
    inp = BookingInput(
        appointment_id=orphan,
        clinic_id=str(world.clinic_id),
        patient_id=str(world.patient_id),
        provider_id=str(world.provider_id),
        slot_id=slot["id"],
    )
    wf = booking_workflow_id(orphan)
    assert await start_once(
        temporal_env.client, BOOKING_WORKFLOW, inp, workflow_id=wf, task_queue=SCHEDULING_TASK_QUEUE
    )
    handle = temporal_env.client.get_workflow_handle(wf)
    first_run = (await handle.describe()).run_id
    maintenance = _maintenance(temporal_env.client, mongo_client)

    await ActivityEnvironment().run(maintenance.start_pending_bookings)

    description = await handle.describe()
    assert description.run_id == first_run
    assert description.status == WorkflowExecutionStatus.RUNNING
    assert (await _appointment(engine, orphan)).workflow_started_at is not None

    # Closed run + lost marker: refused, marker restored, still one run.
    await handle.terminate(reason="test: simulate a closed run")
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE appointments SET workflow_started_at = NULL WHERE id = :a"),
            {"a": uuid.UUID(orphan)},
        )
    await ActivityEnvironment().run(maintenance.start_pending_bookings)
    description = await handle.describe()
    assert description.run_id == first_run
    assert description.status == WorkflowExecutionStatus.TERMINATED
    after = await _appointment(engine, orphan)
    assert after.status == "pending"
    assert after.workflow_started_at is not None
