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

import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio

from tests.e2e import phase3_stack as stack
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
