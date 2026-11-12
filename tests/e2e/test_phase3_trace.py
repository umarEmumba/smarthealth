"""One booking is ONE trace in Jaeger across HTTP, Temporal, Kafka and Celery (plan 03-17;
OBS-02, D-11, roadmap success criterion 4).

A booking is sent to the published gateway with a W3C `traceparent` of the test's own
trace id T. Every process on the path exports its spans over OTLP to Jaeger (published on
localhost:16686), so the Jaeger query API must return trace T with spans from all six
services the booking touches synchronously or through the event pipeline:

    gateway -> scheduling (HTTP, StartWorkflow) -> scheduling-worker (RunWorkflow/activities)
      -> outbox row (traceparent column) -> Debezium -> Kafka header
      -> notification-consumer and analytics-consumer processing spans
      -> notification-celery task span

The Kafka consumer spans must be CHILDREN (a `CHILD_OF` reference to a span that is itself in
trace T), not only linked: a `FOLLOWS_FROM` link alone fails (research Pitfall 6). The Celery
`notifications.send` task span must have a parent inside T as well.

The reminder is a long-lived Temporal timer (plan 03-06): the notification-consumer starts it
inside the same trace, so its start and first workflow task appear in T, but the timer fires
days later. This test reports whichever `notification-temporal-worker` spans exist and does
not require the (future) firing to be in the trace; docs label that long-lived trace honestly.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_trace.py -m e2e
"""

from __future__ import annotations

import os
import secrets
import time
import uuid
from collections import Counter
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

JAEGER_URL = os.environ.get("SMARTHEALTH_JAEGER_URL", "http://localhost:16686").rstrip("/")
TRACE_DEADLINE_SECONDS = 120
REQUIRED_SERVICES = (
    "smarthealth-gateway",
    "smarthealth-scheduling",
    "smarthealth-scheduling-worker",
    "smarthealth-notification-consumer",
    "smarthealth-notification-celery",
    "smarthealth-analytics-consumer",
)
APPOINTMENT_TOPIC_PROCESS = "smarthealth.appointment.events process"
CELERY_TASK_SPAN = "run/notifications.send"


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


@pytest_asyncio.fixture
async def jaeger() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=JAEGER_URL, timeout=20.0) as http:
        try:
            await http.get("/api/services")
        except httpx.TransportError as exc:
            message = f"Jaeger not reachable at {JAEGER_URL} ({type(exc).__name__})"
            if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
                pytest.fail(message)
            pytest.skip(message)
        yield http


def _service_of(trace: dict[str, Any], span: dict[str, Any]) -> str:
    return trace["processes"][span["processID"]]["serviceName"]


def _references(span: dict[str, Any]) -> list[dict[str, str]]:
    return list(span.get("references") or [])


def _has_parent_in(span: dict[str, Any], trace_id: str, span_ids: set[str]) -> bool:
    """A CHILD_OF reference to a span of this very trace (a FOLLOWS_FROM link does not count)."""
    return any(
        ref.get("refType") == "CHILD_OF"
        and ref.get("traceID") == trace_id
        and ref.get("spanID") in span_ids
        for ref in _references(span)
    )


async def _fetch_trace(jaeger: httpx.AsyncClient, trace_id: str) -> dict[str, Any] | None:
    resp = await jaeger.get(f"/api/traces/{trace_id}")
    if resp.status_code == 404:
        return None
    assert resp.status_code == 200, resp.text
    data = resp.json().get("data") or []
    return data[0] if data else None


async def _complete_trace(jaeger: httpx.AsyncClient, trace_id: str) -> dict[str, Any]:
    """Poll Jaeger until every required service has exported its part of the trace."""
    last: set[str] = set()

    async def check() -> dict[str, Any] | None:
        nonlocal last
        trace = await _fetch_trace(jaeger, trace_id)
        if trace is None:
            return None
        services = {_service_of(trace, span) for span in trace["spans"]}
        last = services
        if not set(REQUIRED_SERVICES) <= services:
            return None
        spans = trace["spans"]
        names = {(_service_of(trace, s), s["operationName"]) for s in spans}
        if ("smarthealth-notification-celery", CELERY_TASK_SPAN) not in names:
            return None
        if ("smarthealth-notification-consumer", APPOINTMENT_TOPIC_PROCESS) not in names:
            return None
        return trace

    try:
        return await poll(check, timeout=TRACE_DEADLINE_SECONDS, interval=3.0)
    except pytest.fail.Exception:
        pytest.fail(
            f"trace {trace_id} in Jaeger never had all of {sorted(REQUIRED_SERVICES)} "
            f"(services seen: {sorted(last)})"
        )


async def test_one_booking_is_one_connected_trace_across_http_temporal_kafka_and_celery(
    gateway: httpx.AsyncClient, jaeger: httpx.AsyncClient, ew: E2EWorld
) -> None:
    trace_id = secrets.token_hex(16)
    parent_span = secrets.token_hex(8)
    slots = await day_slots(gateway, ew, ew.world.last_day)
    assert slots, "the seeded world has no slot"

    started = time.time()
    resp = await gateway.post(
        "/api/appointments",
        json={"slot_id": slots[0]["id"], "patient_id": ew.patient_ids[0]},
        headers={
            **ew.front_desk,
            "Idempotency-Key": f"e2e-{uuid.uuid4().hex}",
            "traceparent": f"00-{trace_id}-{parent_span}-01",
        },
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    trace = await _complete_trace(jaeger, trace_id)
    spans: list[dict[str, Any]] = trace["spans"]
    span_ids = {span["spanID"] for span in spans}
    assert {span["traceID"] for span in spans} == {trace_id}

    per_service = Counter(_service_of(trace, span) for span in spans)
    for service in REQUIRED_SERVICES:
        assert per_service[service] >= 1, (service, dict(per_service))

    # Consumer spans: CHILD_OF a span of this trace (the span that wrote the outbox row).
    consumer_spans = [
        span
        for span in spans
        if span["operationName"] == APPOINTMENT_TOPIC_PROCESS
        and _service_of(trace, span)
        in ("smarthealth-notification-consumer", "smarthealth-analytics-consumer")
    ]
    seen = {_service_of(trace, span) for span in consumer_spans}
    assert seen == {"smarthealth-notification-consumer", "smarthealth-analytics-consumer"}, seen
    for span in consumer_spans:
        assert _has_parent_in(span, trace_id, span_ids), (
            f"{_service_of(trace, span)} consumer span is not a CHILD_OF a span of trace "
            f"{trace_id}: references={_references(span)}"
        )
        # It must not be a root of a different trace with only a FOLLOWS_FROM link.
        assert not any(
            ref.get("refType") == "FOLLOWS_FROM" and ref.get("traceID") == trace_id
            for ref in _references(span)
        ), "a link is not accepted where a parent is required"

    # Celery: the task span hangs under the publish span, itself under the consumer span.
    by_id = {span["spanID"]: span for span in spans}
    task = next(
        span
        for span in spans
        if span["operationName"] == CELERY_TASK_SPAN
        and _service_of(trace, span) == "smarthealth-notification-celery"
    )
    assert _has_parent_in(task, trace_id, span_ids), _references(task)
    publish = by_id[next(r["spanID"] for r in _references(task) if r["refType"] == "CHILD_OF")]
    assert publish["operationName"] == "apply_async/notifications.send"
    assert _service_of(trace, publish) == "smarthealth-notification-consumer"
    publish_parent = by_id[next(r["spanID"] for r in _references(publish) if r["refType"] == "CHILD_OF")]
    assert publish_parent["operationName"] == APPOINTMENT_TOPIC_PROCESS

    # Reminder timer: long-lived workflow of the same trace (reported, not required to fire).
    reminder_spans = per_service.get("smarthealth-notification-temporal-worker", 0)
    print(  # noqa: T201 - evidence for the SUMMARY / the human Jaeger check in plan 03-20
        f"\nJAEGER_TRACE_EVIDENCE trace_id={trace_id} appointment_id={appointment_id} "
        f"elapsed_s={time.time() - started:.1f} total_spans={len(spans)} "
        f"per_service={dict(sorted(per_service.items()))} "
        f"reminder_worker_spans_in_trace={reminder_spans}"
    )
