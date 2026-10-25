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
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from services.scheduling.domain.contracts import (
    ScheduleChangeInput,
    ScheduleChangeResult,
    schedule_change_workflow_id,
)
from tests.conftest import service_base_url
from tests.scheduling.seed import World, auth
from tests.scheduling.test_cancel import day_slots

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
