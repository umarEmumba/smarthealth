"""One booking's trace id is findable in the structured logs of every process it touched
(plan 03-17; OBS-03, D-11, T-3-49).

A booking is sent to the published gateway with a W3C `traceparent` of the test's own trace
id T. The booking then travels through Scheduling's API and worker, Billing (pre-check), the
outbox -> Debezium -> Kafka, the notification and analytics consumers and the Celery worker.
`docker compose logs --since <start>` of each of those services must contain a JSON line with
`"trace_id": "<T>"` (structlog adds it from the active span), and none of the lines of those
services may contain the seeded patient's name or date of birth.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_logs.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import datetime
import json
import secrets
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio

from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    day_slots,
    gateway_url,
    poll,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

LOG_DEADLINE_SECONDS = 120
# Compose service -> what the booking does there (documentation for failures).
LOGGING_SERVICES = {
    "scheduling": "booking API request",
    "scheduling-worker": "booking saga activities",
    "billing": "billing pre-check",
    "notification-consumer": "appointment.booked consumed, send published",
    "notification-celery": "notifications.send task",
    "analytics-consumer": "appointment.booked projected",
}
# Synthetic PHI of the seeded world (tests/scheduling/seed.py); never legitimate in a log line.
PHI_VALUES = ("Zzyzx Seed Synthetic", "Zzyzx E2E Synthetic", "1990-01-01")


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    for service in ("notification-consumer", "notification-celery", "analytics-consumer"):
        stack.wait_service_healthy(service)


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway, patients=1)


def _service_logs(service: str, since: datetime.datetime) -> str:
    return stack.compose(
        "logs", "--no-color", "--since", since.strftime("%Y-%m-%dT%H:%M:%SZ"), service, timeout=120
    ).stdout


def _lines_with_trace(logs: str, trace_id: str) -> list[dict[str, Any]]:
    """JSON log records of the service that carry `trace_id`. A line may have a prefix before
    the JSON object (Compose's `service-1  | `, and Celery's own `[time: WARNING/MainProcess] `
    wrapper around the worker's structlog output)."""
    found = []
    for line in logs.splitlines():
        _, _, payload = line.partition("| ")
        if trace_id not in payload or "{" not in payload:
            continue
        try:
            record = json.loads(payload[payload.index("{") :])
        except json.JSONDecodeError:
            continue
        if record.get("trace_id") == trace_id:
            found.append(record)
    return found


async def test_the_booking_trace_id_is_in_the_json_logs_of_every_process_it_touched(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    since = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=5)
    trace_id = secrets.token_hex(16)
    slots = await day_slots(gateway, ew, ew.world.last_day)
    assert slots, "the seeded world has no slot"

    resp = await gateway.post(
        "/api/appointments",
        json={"slot_id": slots[0]["id"], "patient_id": ew.patient_ids[0]},
        headers={
            **ew.front_desk,
            "Idempotency-Key": f"e2e-{uuid.uuid4().hex}",
            "traceparent": f"00-{trace_id}-{secrets.token_hex(8)}-01",
        },
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    logs: dict[str, str] = {}
    missing: list[str] = list(LOGGING_SERVICES)

    async def every_process_logged() -> bool:
        nonlocal missing
        for service in list(missing):
            logs[service] = _service_logs(service, since)
            if _lines_with_trace(logs[service], trace_id):
                missing.remove(service)
        return not missing

    try:
        await poll(every_process_logged, timeout=LOG_DEADLINE_SECONDS, interval=4.0)
    except pytest.fail.Exception:
        pytest.fail(
            f"no JSON log line with trace_id {trace_id} in: "
            + ", ".join(f"{name} ({LOGGING_SERVICES[name]})" for name in missing)
        )

    for service, text in logs.items():
        records = _lines_with_trace(text, trace_id)
        assert all(record.get("span_id") for record in records), service
        for value in PHI_VALUES:
            assert value not in text, f"PHI value {value!r} appears in the logs of {service}"
    print(  # noqa: T201 - evidence for the SUMMARY
        f"\nLOG_TRACE_EVIDENCE trace_id={trace_id} appointment_id={appointment_id} "
        + " ".join(f"{s}={len(_lines_with_trace(t, trace_id))}" for s, t in sorted(logs.items()))
    )
