"""Schedule-change propagation, Scheduling side (plan 02.1-08, mentor item M2).

Provider's durable change feed (plan 02.1-05) is drained by the poller activity,
which starts one `ScheduleChangeWorkflow` per event; the workflow withdraws the
future slots that no longer exist and flags the bookings that sit on them. These
tests drive the whole chain through the gateway, the in-process mesh (real
service tokens, Provider's feed), the real scheduling_db and a time-skipping
Temporal test server running the real worker (`scheduling_worker`).

The feed is global, oldest first and capped, and other tests leave pending events
in the shared database, so every test drains it first (the same autouse fixture as
`tests/provider/test_schedule_change_feed.py`) and every assertion is scoped to
ids the test created.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, time, timedelta
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from libs.service_client import UpstreamServiceUnavailable
from services.scheduling import references
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ChangeResult,
    ScheduleChangeInput,
    ScheduleChangeResult,
    cancel_workflow_id,
    reschedule_workflow_id,
    schedule_change_workflow_id,
)
from tests.conftest import degraded_mesh, service_base_url
from tests.scheduling.conftest import cleanup_scheduling_rows
from tests.scheduling.seed import World, auth, seed_bookable_world
from tests.scheduling.test_cancel import (
    billing_moves,
    book_confirmed,
    change_result,
    day_slots,
    get_appointment,
    insert_appointment,
    slot_status,
)
from tests.scheduling.test_reschedule import reschedule
from tests.temporal_fixtures import build_schedule_change_activities

pytestmark = pytest.mark.integration


# --- feed helpers ---------------------------------------------------------------


async def _scheduling_headers(mesh: httpx.AsyncClient) -> dict[str, str]:
    """A REAL Scheduling service token for `aud=provider`, minted through Identity."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415

    provider = ServiceTokenProvider(
        "scheduling",
        load_client_secret(os.environ["SCHEDULING_CLIENT_SECRET_PATH"]),
        service_base_url("identity"),
        http=mesh,
    )
    return {"Authorization": f"Bearer {await provider.get('provider')}"}


def _feed(path: str = "/internal/schedule-changes") -> str:
    return f"{service_base_url('provider')}{path}"


async def pending_events(mesh: httpx.AsyncClient) -> list[dict[str, Any]]:
    resp = await mesh.get(_feed(), params={"limit": 200}, headers=await _scheduling_headers(mesh))
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest_asyncio.fixture(autouse=True)
async def _drain_feed(mesh: httpx.AsyncClient) -> None:
    """Acknowledge every pending event before each test (test database, no
    consumer running): without it a fresh provider's events would sit beyond the
    feed's page cap after a few runs."""
    headers = await _scheduling_headers(mesh)
    for _ in range(1000):
        pending = await pending_events(mesh)
        if not pending:
            return
        for event in pending:
            ack = await mesh.post(_feed(f"/internal/schedule-changes/{event['id']}/ack"), headers=headers)
            assert ack.status_code == 200, ack.text
    raise AssertionError("feed did not drain")


async def events_for(mesh: httpx.AsyncClient, provider_id: uuid.UUID) -> list[dict[str, Any]]:
    return [e for e in await pending_events(mesh) if e["provider_id"] == str(provider_id)]


async def await_change(env: WorkflowEnvironment, event_id: str) -> ScheduleChangeResult:
    """The result of `schedule-change-{event_id}` (time skipping lets the
    workflow's hold-TTL timer elapse while the test awaits)."""
    handle = env.client.get_workflow_handle(
        schedule_change_workflow_id(event_id), result_type=ScheduleChangeResult
    )
    async with env.time_skipping_unlocked():
        return await handle.result()


async def run_apply(
    activities: Any,
    *,
    event_id: str | None = None,
    provider_id: uuid.UUID,
    change_type: str,
    template_id: uuid.UUID | None = None,
    pass_number: int = 1,
    workflow_id: str | None = None,
) -> ScheduleChangeResult:
    """Run `apply_schedule_change` through `ActivityEnvironment` (it reads the
    workflow id from the activity context for its audit dedupe key)."""
    event_id = event_id or str(uuid.uuid4())
    environment = ActivityEnvironment()
    environment.info = replace(
        environment.info, workflow_id=workflow_id or schedule_change_workflow_id(event_id)
    )
    return await environment.run(
        activities.apply_schedule_change,
        ScheduleChangeInput(
            event_id=event_id,
            provider_id=str(provider_id),
            change_type=change_type,
            template_id=str(template_id) if template_id else None,
            pass_number=pass_number,
        ),
    )


async def deactivate_template(client: AsyncClient, world: World, template_id: uuid.UUID) -> None:
    resp = await client.post(
        f"/api/providers/{world.provider_id}/availability/{template_id}/deactivate",
        headers=auth(world.admin),
    )
    assert resp.status_code == 200, resp.text


def slot_ids_by_status(slots: list[dict]) -> dict[str, str]:
    return {s["id"]: s["status"] for s in slots}


# --- Task 1: the tracer ----------------------------------------------------------


async def test_tracer_template_deactivation_withdraws_slots(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    withdrawn_day = world.last_day
    other_days = [d for d in (world.first_day, world.first_day + timedelta(days=1))]
    template_id = world.template_ids[withdrawn_day.weekday()]

    before = await day_slots(client, world, day=withdrawn_day, status="any")
    assert before and all(s["status"] == "available" for s in before)
    other_before = {
        s["id"]: s["status"]
        for day in other_days
        for s in await day_slots(client, world, day=day, status="any")
    }
    assert other_before and all(status == "available" for status in other_before.values())

    # Provider records the change atomically with the deactivation.
    await deactivate_template(client, world, template_id)
    events = await events_for(mesh, world.provider_id)
    assert [(e["change_type"], e["template_id"]) for e in events] == [
        ("template_deactivated", str(template_id))
    ]
    event_id = events[0]["id"]

    # The poller starts the workflow and acknowledges the event.
    polled = await ActivityEnvironment().run(schedule_changes.poll_schedule_changes)
    assert polled.processed >= 1
    result = await await_change(temporal_env, event_id)

    assert result.withdrawn == len(before)
    assert result.flagged == 0
    after = await day_slots(client, world, day=withdrawn_day, status="any")
    assert [s["status"] for s in after] == ["cancelled"] * len(before)
    # Other weekdays are untouched.
    for day in other_days:
        for slot in await day_slots(client, world, day=day, status="any"):
            assert slot["status"] == "available", (day, slot)
    # Acknowledged: no longer in Provider's feed; and not bookable or searchable.
    assert await events_for(mesh, world.provider_id) == []
    assert await day_slots(client, world, day=withdrawn_day, status="available") == []


# --- Task 2: flags, second pass, regeneration, notification, API --------------------


@pytest_asyncio.fixture
async def sched_engine():
    engine = create_async_engine(settings.database_url)
    try:
        yield engine
    finally:
        await engine.dispose()


async def appointment_row(engine: AsyncEngine, appointment_id: str | uuid.UUID):
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, slot_id, schedule_change_flagged_at, schedule_change_reason, "
                    "version FROM appointments WHERE id = :a"
                ),
                {"a": uuid.UUID(str(appointment_id))},
            )
        ).one()


async def history_reasons(engine: AsyncEngine, appointment_id: str | uuid.UUID) -> list[str | None]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT reason FROM appointment_status_history WHERE appointment_id = :a "
                "ORDER BY created_at, id"
            ),
            {"a": uuid.UUID(str(appointment_id))},
        )
        return [r.reason for r in rows]


async def notification_entries(mongo: AsyncMongoClient, appointment_id: str | uuid.UUID) -> int:
    """`notification.requested` audit entries for the SCHEDULE-CHANGE intent (its
    dedupe key carries `:notify:`; the booking's own entry does not)."""
    return await mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME].count_documents(
        {
            "action": "notification.requested",
            "entity_id": str(appointment_id),
            "dedupe_key": {"$regex": ":notify:"},
        }
    )


async def feed_event(mesh: httpx.AsyncClient, world: World, change_type: str) -> dict[str, Any]:
    events = [e for e in await events_for(mesh, world.provider_id) if e["change_type"] == change_type]
    assert len(events) == 1, events
    return events[0]


async def apply_event(
    activities: Any, event: dict[str, Any], *, pass_number: int = 1
) -> ScheduleChangeResult:
    return await run_apply(
        activities,
        event_id=event["id"],
        provider_id=uuid.UUID(event["provider_id"]),
        change_type=event["change_type"],
        template_id=uuid.UUID(event["template_id"]) if event["template_id"] else None,
        pass_number=pass_number,
    )


async def all_world_slots(client: AsyncClient, world: World) -> list[dict]:
    """Every slot the world generated (any status), across its three days."""
    slots: list[dict] = []
    for offset in range((world.last_day - world.first_day).days + 1):
        slots += await day_slots(client, world, day=world.first_day + timedelta(days=offset), status="any")
    return slots


def stale_start(world: World, hour: int, minute: int = 0) -> datetime:
    """A local time on the last day outside the 09:00-12:00 templates, in the future."""
    return datetime.combine(world.last_day, time(hour, minute), tzinfo=world.tz)


async def patch_template(client: AsyncClient, world: World, template_id: uuid.UUID, body: dict) -> None:
    resp = await client.patch(
        f"/api/providers/{world.provider_id}/availability/{template_id}",
        json=body,
        headers=auth(world.admin),
    )
    assert resp.status_code == 200, resp.text


async def flagged_by_template_edit(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    world: World,
    env: WorkflowEnvironment,
    activities: Any,
) -> tuple[dict, dict, ScheduleChangeResult]:
    """Book the 09:00 and the 10:30 slot of the last day, then move that day's
    template window to 10:00-13:00 and apply the event. The 09:00 booking sits on
    a stale slot (flagged); the 10:30 one on a slot that is still valid.
    Returns (flagged appointment, kept appointment, apply result)."""
    slots = await day_slots(client, world)
    assert [s["start_time"][11:16] for s in slots][:1], slots
    stale, kept_slot = slots[0], slots[3]
    flagged = await book_confirmed(client, env, world, stale["id"])
    kept = await book_confirmed(client, env, world, kept_slot["id"])
    await patch_template(
        client,
        world,
        world.template_ids[world.last_day.weekday()],
        {"start_time": "10:00:00", "end_time": "13:00:00"},
    )
    event = await feed_event(mesh, world, "template_updated")
    result = await apply_event(activities, event)
    return flagged, kept, result


async def test_provider_deactivation_withdraws_available_and_flags_booked(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
    sched_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    booked_slot = (await day_slots(client, world))[0]
    appointment = await book_confirmed(client, temporal_env, world, booked_slot["id"])
    before = await all_world_slots(client, world)
    now = datetime.now(world.tz)
    future_available = [
        s["id"]
        for s in before
        if s["status"] == "available" and datetime.fromisoformat(s["start_time"]) > now
    ]
    past = [s["id"] for s in before if datetime.fromisoformat(s["start_time"]) <= now]

    resp = await client.post(f"/api/providers/{world.provider_id}/deactivate", headers=auth(world.admin))
    assert resp.status_code == 200, resp.text
    event = await feed_event(mesh, world, "provider_deactivated")
    assert event["template_id"] is None
    result = await apply_event(schedule_changes, event)

    assert result == ScheduleChangeResult(withdrawn=len(future_available), flagged=1)
    after = {s["id"]: s["status"] for s in await all_world_slots(client, world)}
    assert all(after[i] == "cancelled" for i in future_available)
    assert after[booked_slot["id"]] == "booked"
    assert all(after[i] == "available" for i in past if i != booked_slot["id"])  # already started: untouched

    row = await appointment_row(sched_engine, appointment["id"])
    # Flagged, never cancelled: still confirmed, still on its slot.
    assert row.status == "confirmed"
    assert str(row.slot_id) == booked_slot["id"] == appointment["slot_id"]
    assert row.schedule_change_flagged_at is not None
    assert row.schedule_change_reason == "provider_deactivated"
    assert row.version == appointment["version"]
    assert (await history_reasons(sched_engine, appointment["id"]))[-1] == (
        "schedule_changed:provider_deactivated"
    )
    assert await notification_entries(mongo_client, appointment["id"]) == 1


async def test_template_edit_withdraws_stale_keeps_matching_generates_new_and_flags_booked(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
    sched_engine: AsyncEngine,
) -> None:
    world = booking_world
    before = {s["start_time"]: s for s in await day_slots(client, world, status="any")}
    assert len(before) == 6  # 09:00-12:00 every 30 minutes

    flagged, kept, result = await flagged_by_template_edit(
        client, mesh, world, temporal_env, schedule_changes
    )

    # Window moved 09:00-12:00 -> 10:00-13:00: 09:00 (booked, flagged) and 09:30
    # (available, withdrawn) are stale; 10:00..11:30 match; 12:00 and 12:30 are new.
    assert result == ScheduleChangeResult(withdrawn=1, flagged=1)
    after = await day_slots(client, world, status="any")
    by_local = {datetime.fromisoformat(s["start_time"]).astimezone(world.tz).strftime("%H:%M"): s for s in after}
    assert sorted(by_local) == ["09:00", "09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30"]
    assert by_local["09:00"]["status"] == "booked"  # kept, its booking flagged
    assert by_local["09:30"]["status"] == "cancelled"  # withdrawn
    assert [by_local[t]["status"] for t in ("10:00", "11:00", "11:30")] == ["available"] * 3
    assert by_local["10:30"]["status"] == "booked"  # the valid booking is untouched
    assert [by_local[t]["status"] for t in ("12:00", "12:30")] == ["available"] * 2  # generated
    for old in before.values():  # existing slots keep their ids
        assert by_local[datetime.fromisoformat(old["start_time"]).astimezone(world.tz).strftime("%H:%M")]["id"] == old["id"]

    stale_row = await appointment_row(sched_engine, flagged["id"])
    assert (stale_row.status, str(stale_row.slot_id)) == ("confirmed", flagged["slot_id"])
    assert stale_row.schedule_change_reason == "template_updated"
    kept_row = await appointment_row(sched_engine, kept["id"])
    assert kept_row.schedule_change_flagged_at is None and kept_row.schedule_change_reason is None


async def test_applying_twice_withdraws_flags_and_notifies_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
    sched_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    flagged, _kept, first = await flagged_by_template_edit(client, mesh, world, temporal_env, schedule_changes)
    assert first == ScheduleChangeResult(withdrawn=1, flagged=1)
    event = await feed_event(mesh, world, "template_updated")
    slots_after_first = {s["id"]: s["status"] for s in await day_slots(client, world, status="any")}
    flagged_at = (await appointment_row(sched_engine, flagged["id"])).schedule_change_flagged_at

    # Pass 2 of the same workflow, and then a repeated poll's second workflow.
    second = await apply_event(schedule_changes, event, pass_number=2)
    third = await run_apply(
        schedule_changes,
        provider_id=world.provider_id,
        change_type="template_updated",
        template_id=world.template_ids[world.last_day.weekday()],
    )

    assert second == third == ScheduleChangeResult(withdrawn=0, flagged=0)
    assert {s["id"]: s["status"] for s in await day_slots(client, world, status="any")} == slots_after_first
    row = await appointment_row(sched_engine, flagged["id"])
    assert row.schedule_change_flagged_at == flagged_at  # the flag is set once
    notes = [r for r in await history_reasons(sched_engine, flagged["id"]) if r and r.startswith("schedule_changed:")]
    assert notes == ["schedule_changed:template_updated"]
    # Exactly one notification.requested entry per flagged appointment.
    assert await notification_entries(mongo_client, flagged["id"]) == 1


async def test_held_slot_is_skipped_in_pass_one_and_flagged_in_pass_two(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    sched_engine: AsyncEngine,
) -> None:
    world = booking_world
    # A slot the templates no longer define (20:00 is outside 09:00-12:00), held
    # by a pending booking that is still in flight.
    appointment_id, slot_id = await insert_appointment(
        sched_engine, world, status="pending", start=stale_start(world, 20), slot_status="held"
    )
    async with sched_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET held_by_appointment_id = :a, "
                "held_until = now() + interval '10 minutes' WHERE id = :s"
            ),
            {"a": uuid.UUID(appointment_id), "s": uuid.UUID(slot_id)},
        )

    first = await run_apply(
        schedule_changes,
        provider_id=world.provider_id,
        change_type="template_updated",
        template_id=world.template_ids[0],
        pass_number=1,
    )

    assert first == ScheduleChangeResult(withdrawn=0, flagged=0)
    assert await slot_status(client, world, slot_id, day=world.last_day) == "held"  # never withdrawn

    # The booking confirms during the hold window (what the saga's confirm does).
    async with sched_engine.begin() as conn:
        await conn.execute(
            text("UPDATE provider_slots SET status = 'booked', held_until = NULL WHERE id = :s"),
            {"s": uuid.UUID(slot_id)},
        )
        await conn.execute(
            text("UPDATE appointments SET status = 'confirmed' WHERE id = :a"),
            {"a": uuid.UUID(appointment_id)},
        )
    second = await run_apply(
        schedule_changes,
        provider_id=world.provider_id,
        change_type="template_updated",
        template_id=world.template_ids[0],
        pass_number=2,
    )

    assert second == ScheduleChangeResult(withdrawn=0, flagged=1)
    row = await appointment_row(sched_engine, appointment_id)
    assert (row.status, str(row.slot_id), row.schedule_change_reason) == (
        "confirmed",
        slot_id,
        "template_updated",
    )
    assert await slot_status(client, world, slot_id, day=world.last_day) == "booked"


async def test_held_slot_whose_booking_failed_is_withdrawn_in_pass_two(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    sched_engine: AsyncEngine,
) -> None:
    world = booking_world
    slot_id = await _seed_stale_slot(sched_engine, world, 20, status="held")

    first = await run_apply(
        schedule_changes, provider_id=world.provider_id, change_type="template_updated", pass_number=1
    )
    assert first == ScheduleChangeResult(withdrawn=0, flagged=0)

    # The hold is released (booking failed / reaped): available again, still stale.
    async with sched_engine.begin() as conn:
        await conn.execute(
            text("UPDATE provider_slots SET status = 'available', held_until = NULL WHERE id = :s"),
            {"s": slot_id},
        )
    second = await run_apply(
        schedule_changes, provider_id=world.provider_id, change_type="template_updated", pass_number=2
    )

    assert second == ScheduleChangeResult(withdrawn=1, flagged=0)
    assert await slot_status(client, world, str(slot_id), day=world.last_day) == "cancelled"


async def _seed_stale_slot(
    engine: AsyncEngine, world: World, hour: int, *, status: str, minute: int = 0
) -> uuid.UUID:
    from tests.scheduling.conftest import seed_slot  # noqa: PLC0415

    return await seed_slot(
        engine,
        provider_id=world.provider_id,
        start=stale_start(world, hour, minute),
        clinic_id=world.clinic_id,
        department_id=world.department_id,
        status=status,
    )


async def test_checked_in_appointment_on_a_stale_slot_is_flagged(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    sched_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    appointment_id, slot_id = await insert_appointment(
        sched_engine, world, status="checked_in", start=stale_start(world, 20), slot_status="booked"
    )

    result = await run_apply(
        schedule_changes, provider_id=world.provider_id, change_type="template_deactivated"
    )

    assert result == ScheduleChangeResult(withdrawn=0, flagged=1)
    row = await appointment_row(sched_engine, appointment_id)
    assert (row.status, str(row.slot_id), row.schedule_change_reason) == (
        "checked_in",
        slot_id,
        "template_deactivated",
    )
    assert await notification_entries(mongo_client, appointment_id) == 1


@pytest.mark.parametrize(
    ("status", "slot_status", "withdrawn"),
    [("cancelled", "available", 1), ("failed", "available", 1), ("completed", "booked", 0)],
)
async def test_cancelled_failed_and_completed_appointments_are_not_flagged(
    booking_world: World,
    schedule_changes: Any,
    sched_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    status: str,
    slot_status: str,
    withdrawn: int,
) -> None:
    world = booking_world
    appointment_id, _slot_id = await insert_appointment(
        sched_engine, world, status=status, start=stale_start(world, 20), slot_status=slot_status
    )

    result = await run_apply(
        schedule_changes, provider_id=world.provider_id, change_type="template_updated"
    )

    assert result == ScheduleChangeResult(withdrawn=withdrawn, flagged=0)
    row = await appointment_row(sched_engine, appointment_id)
    assert (row.status, row.schedule_change_flagged_at, row.schedule_change_reason) == (status, None, None)
    assert await notification_entries(mongo_client, appointment_id) == 0


async def test_held_and_booked_slots_of_an_unknown_provider_are_never_withdrawn(
    schedule_changes: Any, sched_engine: AsyncEngine, client: AsyncClient
) -> None:
    # An unknown provider has no valid slots (empty candidate set): only its
    # AVAILABLE future slots are withdrawn.
    provider_id, clinic_id, department_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    from tests.scheduling.conftest import seed_slot  # noqa: PLC0415

    start = datetime.now(tz=None).astimezone() + timedelta(days=3)
    ids = {}
    try:
        for index, status in enumerate(("available", "held", "booked", "cancelled")):
            ids[status] = await seed_slot(
                sched_engine,
                provider_id=provider_id,
                start=start + timedelta(hours=index),
                clinic_id=clinic_id,
                department_id=department_id,
                status=status,
            )
        result = await run_apply(
            schedule_changes, provider_id=provider_id, change_type="provider_deactivated"
        )
        async with sched_engine.connect() as conn:
            rows = await conn.execute(
                text("SELECT id, status FROM provider_slots WHERE provider_id = :p"), {"p": provider_id}
            )
            statuses = {r.id: r.status for r in rows}
        assert result == ScheduleChangeResult(withdrawn=1, flagged=0)
        assert statuses[ids["available"]] == "cancelled"
        assert statuses[ids["held"]] == "held"
        assert statuses[ids["booked"]] == "booked"
        assert statuses[ids["cancelled"]] == "cancelled"
    finally:
        await cleanup_scheduling_rows(sched_engine, [provider_id])


async def test_api_exposes_the_flag_and_the_flagged_filter_is_scoped(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
    sched_engine: AsyncEngine,
) -> None:
    world = booking_world
    flagged, kept, _ = await flagged_by_template_edit(client, mesh, world, temporal_env, schedule_changes)

    # Single read: staff and the patient (own appointment) see the flag.
    for tokens in (world.front_desk, world.patient, world.admin):
        got = await client.get(f"/api/appointments/{flagged['id']}", headers=auth(tokens))
        assert got.status_code == 200, got.text
        assert got.json()["schedule_change_flagged"] is True
        assert got.json()["schedule_change_reason"] == "template_updated"
        assert got.json()["status"] == "confirmed"
    unflagged = await get_appointment(client, world, kept["id"])
    assert unflagged["schedule_change_flagged"] is False
    assert unflagged["schedule_change_reason"] is None

    # The filter: only flagged appointments, in the caller's scope.
    listed = await client.get(
        "/api/appointments", params={"flagged": "true"}, headers=auth(world.front_desk)
    )
    assert listed.status_code == 200, listed.text
    assert [a["id"] for a in listed.json()] == [flagged["id"]]
    assert listed.json()[0]["schedule_change_flagged"] is True
    patient_view = await client.get(
        "/api/appointments", params={"flagged": "true"}, headers=auth(world.patient)
    )
    assert [a["id"] for a in patient_view.json()] == [flagged["id"]]
    not_flagged = await client.get(
        "/api/appointments", params={"flagged": "false"}, headers=auth(world.front_desk)
    )
    assert [a["id"] for a in not_flagged.json()] == [kept["id"]]

    # No leak across clinics: another clinic's front desk sees nothing of ours.
    other = await seed_bookable_world(client, mesh)
    try:
        foreign = await client.get(
            "/api/appointments", params={"flagged": "true"}, headers=auth(other.front_desk)
        )
        assert foreign.status_code == 200, foreign.text
        assert foreign.json() == []
        foreign_get = await client.get(f"/api/appointments/{flagged['id']}", headers=auth(other.front_desk))
        assert foreign_get.status_code == 404
        # ADMIN is not clinic-scoped and can narrow by provider.
        admin_view = await client.get(
            "/api/appointments",
            params={"flagged": "true", "provider_id": str(world.provider_id)},
            headers=auth(world.admin),
        )
        assert [a["id"] for a in admin_view.json()] == [flagged["id"]]
    finally:
        await cleanup_scheduling_rows(sched_engine, [other.provider_id])


async def test_a_flagged_appointment_can_still_be_cancelled_with_a_refund(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    flagged, _kept, _ = await flagged_by_template_edit(client, mesh, world, temporal_env, schedule_changes)

    resp = await client.post(f"/api/appointments/{flagged['id']}/cancel", json={}, headers=auth(world.patient))

    assert resp.status_code == 202, resp.text
    result = await change_result(temporal_env, cancel_workflow_id(flagged["id"], flagged["version"]))
    assert result == ChangeResult(outcome="cancelled", reason=None)
    after = await get_appointment(client, world, flagged["id"])
    assert after["status"] == "cancelled"
    _moves, billing_status = await billing_moves(client, world, flagged["id"])
    assert billing_status == "refunded"  # the refund path is unchanged
    # The slot the schedule no longer offers is withdrawn, not freed for rebooking.
    assert await slot_status(client, world, flagged["slot_id"], day=world.last_day) == "cancelled"


async def test_rescheduling_a_flagged_appointment_clears_the_flag(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
    sched_engine: AsyncEngine,
) -> None:
    world = booking_world
    flagged, _kept, _ = await flagged_by_template_edit(client, mesh, world, temporal_env, schedule_changes)
    new_slot = next(s for s in await day_slots(client, world) if s["start_time"] != flagged["slot_start"])

    resp = await reschedule(client, world.front_desk, flagged["id"], new_slot["id"])

    assert resp.status_code == 202, resp.text
    result = await change_result(temporal_env, reschedule_workflow_id(flagged["id"], flagged["version"]))
    assert result == ChangeResult(outcome="rescheduled", reason=None)
    after = await get_appointment(client, world, flagged["id"])
    assert after["status"] == "confirmed"
    assert after["slot_id"] == new_slot["id"]
    assert after["schedule_change_flagged"] is False
    assert after["schedule_change_reason"] is None
    row = await appointment_row(sched_engine, flagged["id"])
    assert row.schedule_change_flagged_at is None and row.schedule_change_reason is None
    # The stale slot it left is withdrawn, not offered again.
    assert await slot_status(client, world, flagged["slot_id"], day=world.last_day) == "cancelled"
    # The history keeps the record of the flag.
    assert "schedule_changed:template_updated" in [h["reason"] for h in after["history"]]


async def _deactivate_weekday_template(client: AsyncClient, world: World) -> list[dict]:
    """Deactivate the last day's template; returns that day's slots (all stale now)."""
    slots = await day_slots(client, world)
    await deactivate_template(client, world, world.template_ids[world.last_day.weekday()])
    return slots


async def test_an_unreachable_owner_is_retryable_and_withdraws_nothing(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    slots = await _deactivate_weekday_template(client, world)
    event = await feed_event(mesh, world, "template_deactivated")

    async with degraded_mesh("provider") as degraded:
        blind = build_schedule_change_activities(temporal_env.client, degraded, mongo_client)
        with pytest.raises(UpstreamServiceUnavailable):  # the retryable outcome
            await apply_event(blind, event)

    assert [s["status"] for s in await day_slots(client, world, status="any")] == ["available"] * len(slots)


@pytest.mark.parametrize("missing", ["get_department_ref", "get_clinic_ref"])
async def test_a_missing_department_or_clinic_is_reference_invalid_and_withdraws_nothing(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    schedule_changes: Any,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    world = booking_world
    slots = await _deactivate_weekday_template(client, world)
    event = await feed_event(mesh, world, "template_deactivated")

    async def _gone(*_args: Any, **_kwargs: Any) -> None:
        return None

    with monkeypatch.context() as patched:
        patched.setattr(references, missing, _gone)
        with pytest.raises(ApplicationError) as raised:
            await apply_event(schedule_changes, event)
    assert raised.value.type == "ReferenceInvalid"
    assert raised.value.non_retryable is True
    assert [s["status"] for s in await day_slots(client, world, status="any")] == ["available"] * len(slots)

    # With the references back, the very same event withdraws that day's slots.
    result = await apply_event(schedule_changes, event)
    assert result == ScheduleChangeResult(withdrawn=len(slots), flagged=0)
