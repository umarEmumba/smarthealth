"""A worker killed mid-saga does not lose the booking (plan 02-07 Task 3;
D-19 worker-crash test, research Code Example 3).

Runs against the Compose Temporal server (`TEMPORAL_ADDRESS`, default
localhost:7233), because the recovery depends on real wall-clock timeouts the
time-skipping server would not exercise. The application side is the
in-process mesh with the real scheduling_db, so only Temporal is "real stack".

1. A booking is accepted while the API cannot start workflows (committed
   `pending`, no workflow), then its saga is started on a unique task queue
   `crash-test-<uuid>`.
2. Worker A runs the real activities, except that its billing pre-check hangs
   (it ignores cancellation, like a dead process). Once A is inside that
   activity, A stops polling; its attempt never completes, so the server only
   learns of the loss through the activity's timeout.
3. Worker B, with the real `SchedulingActivities`, polls the same queue. After
   the billing activity's `start_to_close_timeout` (10 s) the server retries it
   on B, and the saga ends `confirmed`. The history shows ONE scheduled
   billing activity whose started attempt is 2.

Total runtime stays under 30 s. Without a reachable Temporal the test skips,
unless `SMARTHEALTH_E2E_REQUIRE_STACK=1`, in which case it fails.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from temporalio import activity
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from libs.db.postgres import get_session_factory
from services.scheduling.activities.booking import SchedulingActivities
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_BILLING,
    BookingInput,
    BookingResult,
    booking_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from tests.conftest import harness_apps
from tests.scheduling.seed import World, auth

pytestmark = pytest.mark.e2e

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")


class _DownTemporal:
    async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
        raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")


async def _compose_temporal() -> Client:
    try:
        return await asyncio.wait_for(Client.connect(TEMPORAL_ADDRESS), timeout=5)
    except Exception as exc:  # noqa: BLE001 - any connect failure means "no stack"
        message = f"Temporal not reachable at {TEMPORAL_ADDRESS} ({type(exc).__name__})"
        if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
            pytest.fail(message)
        pytest.skip(message)


async def _accept_pending(client: AsyncClient, world: World) -> tuple[str, str]:
    start, end = world.local_day_bounds(world.last_day)
    slots = await client.get(
        "/api/slots",
        params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
        headers=auth(world.admin),
    )
    slot_id = slots.json()[0]["id"]
    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()
    try:
        resp = await client.post(
            "/api/appointments",
            json={"slot_id": slot_id, "patient_id": str(world.patient_id)},
            headers=auth(world.front_desk) | {"Idempotency-Key": f"crash-{uuid.uuid4().hex}"},
        )
    finally:
        app.state.temporal_client = previous
    assert resp.status_code == 202, resp.text
    return resp.json()["appointment_id"], slot_id


async def test_worker_killed_mid_activity_does_not_lose_the_booking(
    client: AsyncClient,
    booking_world: World,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
) -> None:
    began = time.monotonic()
    temporal = await _compose_temporal()
    world = booking_world
    appointment_id, slot_id = await _accept_pending(client, world)
    task_queue = f"crash-test-{uuid.uuid4().hex[:12]}"
    workflow_id = booking_workflow_id(appointment_id)

    tokens = ServiceTokenProvider(
        "scheduling", load_client_secret(settings.client_secret_path), settings.identity_service_url, http=mesh
    )
    session_factory = get_session_factory(settings.database_url)
    real = SchedulingActivities(session_factory, mesh, tokens, mongo_client)

    in_billing = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name=ACT_BILLING)
    async def hang_in_billing(inp: Any) -> None:
        # A hung/dead worker: it never finishes this attempt and ignores the
        # cancellation a shutdown sends, so no completion reaches the server.
        in_billing.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    worker_a_activities = [a for a in real.all() if a != real.billing_precheck] + [hang_in_billing]
    worker_a = Worker(
        temporal,
        task_queue=task_queue,
        workflows=[BookingWorkflow],
        activities=worker_a_activities,
    )
    run_a = asyncio.create_task(worker_a.run())
    handle = await temporal.start_workflow(
        BookingWorkflow.run,
        BookingInput(
            appointment_id=appointment_id,
            clinic_id=str(world.clinic_id),
            patient_id=str(world.patient_id),
            provider_id=str(world.provider_id),
            slot_id=slot_id,
        ),
        id=workflow_id,
        task_queue=task_queue,
        result_type=BookingResult,
    )
    stop_a: asyncio.Task | None = None
    try:
        await asyncio.wait_for(in_billing.wait(), timeout=15)
        # "Kill" A: it stops polling now, while its billing attempt stays hung.
        stop_a = asyncio.create_task(worker_a.shutdown())
        # B is a different process in production: give it its own connection
        # (one client may not run two workers on the same task queue).
        temporal_b = await _compose_temporal()
        async with Worker(
            temporal_b,
            task_queue=task_queue,
            workflows=[BookingWorkflow],
            activities=real.all(),
        ):
            result = await asyncio.wait_for(handle.result(), timeout=25)
    finally:
        # Let A's stale attempt end so the process can clean up; the server
        # already gave that attempt up and ignores its late completion.
        release.set()
        if stop_a is None:
            stop_a = asyncio.create_task(worker_a.shutdown())
        await stop_a
        await run_a

    assert result == BookingResult(outcome="confirmed", reason=None)

    scheduled: dict[int, str] = {}
    billing_scheduled = 0
    billing_attempts: list[int] = []
    async for event in handle.fetch_history_events():
        if event.HasField("activity_task_scheduled_event_attributes"):
            name = event.activity_task_scheduled_event_attributes.activity_type.name
            scheduled[event.event_id] = name
            billing_scheduled += name == ACT_BILLING
        if event.HasField("activity_task_started_event_attributes"):
            started = event.activity_task_started_event_attributes
            if scheduled[started.scheduled_event_id] == ACT_BILLING:
                billing_attempts.append(started.attempt)
    # One billing activity, retried: its recorded start is the second attempt
    # (worker B), after worker A's attempt timed out.
    assert billing_scheduled == 1
    assert billing_attempts == [2]

    engine_rows = None
    async with session_factory() as db:
        engine_rows = (
            await db.execute(
                text(
                    "SELECT a.status, s.status AS slot_status FROM appointments a "
                    "JOIN provider_slots s ON s.id = a.slot_id WHERE a.id = :a"
                ),
                {"a": uuid.UUID(appointment_id)},
            )
        ).one()
    assert (engine_rows.status, engine_rows.slot_status) == ("confirmed", "booked")
    assert time.monotonic() - began < 30
