"""Analytics on the deployed stack (plan 03-13; ANLY-01..06, EVENT-03, D-10, D-13).

Patients, appointments and visits are created through the published gateway; their
events travel outbox -> Debezium -> Kafka -> the `analytics-consumer` container ->
analytics_db, and the numbers are read back from `/api/analytics` through the gateway.
Every test uses a fresh clinic (the seeded world), so it asserts on its own clinic's
numbers only; nothing is deleted.

The live consumer group starts at the LATEST offset on its first start (D-13: metrics
count from go-live), pinned at container start, so events produced by these tests after
the container is healthy are always consumed.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_analytics.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import datetime
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio

from tests.e2e import phase3_stack as stack
from tests.scheduling.seed import auth
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
    book_slot_inside_checkin_window,
    day_slots,
    gateway_url,
    poll,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

TRACER_DEADLINE_SECONDS = 60
FLOW_DEADLINE_SECONDS = 90


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_service_healthy("analytics")
    stack.wait_service_healthy("analytics-consumer")


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


async def _summary(gateway: httpx.AsyncClient, ew: E2EWorld) -> dict[str, Any]:
    response = await gateway.get(
        "/api/analytics/summary", params={"clinic_id": ew.clinic_id}, headers=ew.admin
    )
    assert response.status_code == 200, response.text
    return response.json()


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_registered_patients_reach_the_summary_through_the_consumer(
    gateway: httpx.AsyncClient,
) -> None:
    # The seeded world registers one patient; a second is added through the gateway.
    ew = await seed_phase2_world(gateway, patients=2)

    async def reached() -> dict[str, Any] | None:
        body = await _summary(gateway, ew)
        return body if body["total_patients"] == 2 else None

    started = time.monotonic()
    body = await poll(reached, timeout=TRACER_DEADLINE_SECONDS, interval=1.0)
    assert time.monotonic() - started <= TRACER_DEADLINE_SECONDS
    assert body["clinic_id"] == ew.clinic_id
    assert set(body) == {
        "clinic_id",
        "from",
        "to",
        "total_patients",
        "appointments_booked",
        "completed_visits",
        "cancellation_rate",
        "average_wait_seconds",
    }

    # The prefix boundary: `/api/analyticsX` is not the analytics route.
    other = await gateway.get("/api/analyticsX/summary", headers=ew.admin)
    assert other.status_code == 404, other.text


# --- Task 2: all five metrics, role scoping ------------------------------------------


def _window() -> dict[str, str]:
    today = datetime.datetime.now(datetime.UTC).date()
    return {
        "from": (today - datetime.timedelta(days=1)).isoformat(),
        "to": (today + datetime.timedelta(days=1)).isoformat(),
    }


async def test_booked_cancelled_and_completed_flow_shows_in_all_five_metrics(
    gateway: httpx.AsyncClient,
) -> None:
    ew = await seed_phase2_world(gateway, patients=3)
    day_slot_ids = [slot["id"] for slot in (await day_slots(gateway, ew, ew.world.last_day))[:2]]
    assert len(day_slot_ids) == 2
    window_slot_id, _provider = await book_slot_inside_checkin_window(gateway, ew)

    kept = await book(gateway, ew, slot_id=day_slot_ids[0], patient_id=ew.patient_ids[0])
    to_cancel = await book(gateway, ew, slot_id=day_slot_ids[1], patient_id=ew.patient_ids[1])
    visited = await book(gateway, ew, slot_id=window_slot_id, patient_id=ew.patient_ids[2])
    for appointment_id in (kept, to_cancel, visited):
        confirmed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
        assert confirmed["status"] == "confirmed", confirmed

    cancel = await gateway.post(
        f"/api/appointments/{to_cancel}/cancel", json={}, headers=ew.front_desk
    )
    assert cancel.status_code == 202, cancel.text
    await appointment_in(gateway, ew, to_cancel, {"cancelled"})

    checked_in = await gateway.post("/api/visits", json={"appointment_id": visited}, headers=ew.front_desk)
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)
    assert started.status_code == 200, started.text
    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)
    assert completed.status_code == 200, completed.text

    async def reached() -> dict[str, Any] | None:
        response = await gateway.get(
            "/api/analytics/summary",
            params={"clinic_id": ew.clinic_id, **_window()},
            headers=ew.admin,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        done = (
            body["appointments_booked"] == 3
            and body["completed_visits"] == 1
            and body["cancellation_rate"] is not None
            and body["average_wait_seconds"] is not None
        )
        return body if done else None

    body = await poll(reached, timeout=FLOW_DEADLINE_SECONDS, interval=1.0)
    assert body["total_patients"] == 3
    assert body["cancellation_rate"] == pytest.approx(1 / 3, abs=0.001)
    assert body["average_wait_seconds"] >= 0

    series = await gateway.get(
        "/api/analytics/timeseries",
        params={"metric": "appointments_booked", "clinic_id": ew.clinic_id, **_window()},
        headers=ew.front_desk,
    )
    assert series.status_code == 200, series.text
    assert sum(point["value"] for point in series.json()) == 3


async def test_role_and_clinic_scoping_through_the_gateway(gateway: httpx.AsyncClient) -> None:
    ew = await seed_phase2_world(gateway, patients=1)
    summary = "/api/analytics/summary"

    own = await gateway.get(summary, headers=ew.front_desk)  # omitted clinic = its own
    assert own.status_code == 200, own.text
    assert own.json()["clinic_id"] == ew.clinic_id
    other = await gateway.get(summary, params={"clinic_id": str(uuid.uuid4())}, headers=ew.front_desk)
    assert other.status_code == 403, other.text
    any_clinic = await gateway.get(
        summary, params={"clinic_id": str(uuid.uuid4())}, headers=ew.admin
    )
    assert any_clinic.status_code == 200 and any_clinic.json()["total_patients"] == 0

    assert (await gateway.get(summary, headers=ew.provider)).status_code == 403
    assert (await gateway.get(summary, headers=auth(ew.world.patient))).status_code == 403
    assert (await gateway.get(summary)).status_code == 401
