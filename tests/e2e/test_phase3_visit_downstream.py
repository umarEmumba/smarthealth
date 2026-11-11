"""Visit completion -> outbox -> Debezium -> Kafka -> visit-downstream consumer ->
Temporal, on the deployed stack (plan 03-09; EVENT-01, EVENT-02, EVENT-03, ANLY-03,
ANLY-05; D-03).

A visit is checked in, started and completed through the gateway. The completion commits
a `visit.completed` outbox row in visit_db; the `visit-outbox` connector relays it to
`smarthealth.visit.events` keyed by the visit id; the `visit-consumer` container starts
`visit-completed-{visit_id}` and sets `downstream_started_at`. Both effects are asserted
within 15 s of the completion, which is shorter than the still-running sweeper's 30 s
grace (D-03: the sweeper is retired in plan 03-12), so they prove the consumer path.

Plan 03-11 adds the kill-between-commit-and-publish and outage proofs; nothing in this
file stops, kills or restarts a container. Each Kafka read uses a fresh consumer group.
Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_visit_downstream.py -m e2e
"""

from __future__ import annotations

import asyncio
import datetime
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import Client

from libs.events.catalog import topic_for
from libs.events.outbox import event_id_for
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import visit_completed_workflow_id
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    billing_in,
    book,
    book_slot_inside_checkin_window,
    gateway_url,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

VISIT_TOPIC = topic_for("visit.completed")
DOWNSTREAM_DEADLINE_SECONDS = 15.0
TEMPORAL_LOCAL_ADDRESS = "localhost:7233"


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_connectors_running(["visit-outbox"])


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway, patients=1)


@pytest_asyncio.fixture
async def temporal() -> Client:
    return await Client.connect(TEMPORAL_LOCAL_ADDRESS)


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)


async def _downstream_marker(visit_id: str) -> datetime.datetime | None:
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()


async def _complete_visit_through_gateway(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> tuple[str, str, float]:
    """(visit id, appointment id, monotonic time just after the completion response)."""
    slot_id, _ = await book_slot_inside_checkin_window(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    checked_in = await gateway.post(
        "/api/visits", json={"appointment_id": appointment_id}, headers=ew.front_desk
    )
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)
    assert started.status_code == 200, started.text
    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    return visit_id, appointment_id, time.monotonic()


async def _wait_for(check: Any, deadline: float) -> Any:
    """Await `check()` until truthy or the monotonic `deadline`; the last value."""
    value = await check()
    while not value and time.monotonic() < deadline:
        await asyncio.sleep(0.25)
        value = await check()
    return value


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_completed_visit_starts_downstream_through_the_outbox_and_consumer(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    visit_id, appointment_id, completed_at_mono = await _complete_visit_through_gateway(gateway, ew)
    deadline = completed_at_mono + DOWNSTREAM_DEADLINE_SECONDS
    workflow_id = visit_completed_workflow_id(visit_id)

    # 1. the consumer set the marker ...
    marker = await _wait_for(lambda: _downstream_marker(visit_id), deadline)
    assert marker is not None, (
        f"downstream_started_at still NULL {DOWNSTREAM_DEADLINE_SECONDS:.0f} s after the "
        "completion: the visit-downstream consumer did not act on visit.completed"
    )

    # 2. ... and a Temporal execution with the deterministic id exists, on the visit queue.
    async def _describe() -> Any:
        try:
            return await temporal.get_workflow_handle(workflow_id).describe()
        except Exception:  # noqa: BLE001 - not found yet
            return None

    description = await _wait_for(_describe, deadline)
    assert description is not None, f"no Temporal execution {workflow_id} within the deadline"
    assert description.task_queue == VISIT_TASK_QUEUE
    assert time.monotonic() <= deadline + 1.0

    # 3. exactly one visit.completed message, keyed by the visit id, with the timestamps.
    found = await asyncio.to_thread(
        stack.consume_matching,
        VISIT_TOPIC,
        lambda m: m["key"] == visit_id and m["headers"].get("eventType") == "visit.completed",
        60,
        1,
    )
    assert found, f"no visit.completed message for {visit_id} within 60 s"
    # Debezium is at-least-once: count distinct event ids, not messages.
    assert {m["headers"]["id"] for m in found} == {
        str(event_id_for(visit_id, "visit.completed", "completed"))
    }
    value = found[0]["value"]
    assert found[0]["key"] == visit_id
    assert str(value["visit_id"]) == visit_id
    assert str(value["appointment_id"]) == appointment_id
    assert str(value["clinic_id"]) == ew.clinic_id
    for name in ("checked_in_at", "started_at", "completed_at"):
        assert isinstance(value[name], datetime.datetime), name
    assert (
        _aware(value["checked_in_at"])
        <= _aware(value["started_at"])
        <= _aware(value["completed_at"])
    )

    # The downstream fan-out the consumer started still finishes the visit's work.
    await billing_in(gateway, ew, appointment_id, "finalized")
    assert (await appointment_in(gateway, ew, appointment_id, {"completed"}))["status"] == "completed"
