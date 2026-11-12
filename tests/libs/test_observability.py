"""Tracing setup is idempotent, and the critical flows log PHI-free JSON with trace ids.

`setup_tracing` runs once per service app build, and a process may build several apps
(the test harness builds three), so the httpx instrumentation must be idempotent: a
second `instrument()` call on an already-instrumented library only produces a noisy
warning and no value.

Plan 03-17 (OBS-03, D-11, T-3-49) adds the structured-log checks. Each critical flow is
run in process (the harness mesh, the Temporal test server, a real Celery task call, the
consumer base) with stdout captured; every line is parsed as JSON and must carry the
`trace_id` / `span_id` of the active span, and none may contain a seeded patient's name,
date of birth, phone or address. The live counterpart, which finds one booking's trace id
in the logs of each container it touched, is `tests/e2e/test_phase3_logs.py`.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import secrets
import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.testing import WorkflowEnvironment

from libs.events.consumer import EventConsumer, EventEnvelope, Outcome
from libs.events.registry import serializer_for
from libs.observability import otel
from services.notification.tasks import send as send_task
from services.scheduling.domain.contracts import BookingResult, booking_workflow_id
from tests.events.conftest import booked_payload, mock_registry  # noqa: F401
from tests.notification.conftest import (  # noqa: F401  (fixtures reused by name)
    _span_exporter,
    delivery,
    in_memory_spans,
    mongo_db,
    new_request,
)
from tests.scheduling.seed import auth
from tests.temporal_fixtures import running_scheduling_worker

TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
# Synthetic PHI seeded for these tests; none of it may ever reach a log line.
PHI_VALUES = (
    "Zzyzx Seed Synthetic",
    "1990-01-01",
    "Quentin Phi-Probe",
    "1987-03-09",
    "+1-555-0187",
    "31 Phi Probe Lane",
)


def test_setup_tracing_instruments_httpx_once_across_apps(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        otel.setup_tracing(FastAPI(), service_name="smarthealth-test-a")
        otel.setup_tracing(FastAPI(), service_name="smarthealth-test-b")

    assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert otel._HTTPX_INSTRUMENTED is True
    assert not [r for r in caplog.records if "already instrumented" in r.getMessage().lower()]


def test_instrument_httpx_client_spans_is_idempotent() -> None:
    otel.instrument_httpx_client_spans()
    otel.instrument_httpx_client_spans()
    assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert otel._HTTPX_INSTRUMENTED is True


# --- structured logs (plan 03-17, OBS-03) ----------------------------------------------


def _json_lines(output: str) -> list[dict[str, Any]]:
    """Every stdout line that is a JSON object (structlog lines); other lines are ignored."""
    records = []
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _assert_no_phi(output: str) -> None:
    for value in PHI_VALUES:
        assert value not in output, f"PHI value {value!r} reached a log line"


def _lines(records: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [record for record in records if record.get("event") == event]


def _assert_correlated(record: dict[str, Any], trace_id: str | None = None) -> None:
    assert TRACE_ID_RE.match(str(record.get("trace_id"))), f"no trace_id on {record}"
    assert SPAN_ID_RE.match(str(record.get("span_id"))), f"no span_id on {record}"
    if trace_id is not None:
        assert record["trace_id"] == trace_id, f"{record['event']} left trace {trace_id}: {record}"


def _traced(client: Client) -> Client:
    config = client.config()
    config["interceptors"] = [TracingInterceptor()]
    return Client(**config)


async def _booking_logs(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    temporal_env: WorkflowEnvironment,
    world: Any,
    capsys: pytest.CaptureFixture[str],
) -> tuple[str, str, list[dict[str, Any]], str]:
    """Book through the gateway with a known W3C trace id while an extra patient with
    phone and address exists; return (trace id, appointment id, JSON lines, raw output)."""
    extra = await client.post(
        "/api/patients",
        json={
            "clinic_id": str(world.clinic_id),
            "full_name": "Quentin Phi-Probe",
            "date_of_birth": "1987-03-09",
            "phone": "+1-555-0187",
            "address": "31 Phi Probe Lane",
        },
        headers=auth(world.front_desk),
    )
    assert extra.status_code == 201, extra.text

    start, end = world.local_day_bounds(world.last_day)
    found = await client.get(
        "/api/slots",
        params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
        headers=auth(world.admin),
    )
    slot_id = found.json()[0]["id"]
    trace_id = secrets.token_hex(16)
    traced_client = _traced(temporal_env.client)

    async with running_scheduling_worker(traced_client, mesh, mongo_client):
        capsys.readouterr()
        resp = await client.post(
            "/api/appointments",
            json={"slot_id": slot_id, "patient_id": str(world.patient_id)},
            headers={
                **auth(world.front_desk),
                "Idempotency-Key": f"obs-{uuid.uuid4().hex}",
                "traceparent": f"00-{trace_id}-{secrets.token_hex(8)}-01",
            },
        )
        assert resp.status_code == 202, resp.text
        appointment_id = resp.json()["appointment_id"]
        result = await traced_client.get_workflow_handle(
            booking_workflow_id(appointment_id), result_type=BookingResult
        ).result()
        assert result.outcome == "confirmed"

    output = capsys.readouterr().out
    return trace_id, appointment_id, _json_lines(output), output


@pytest.mark.integration
async def test_booking_request_and_saga_log_json_with_the_request_trace_id(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    booking_world: Any,
    temporal_env: WorkflowEnvironment,
    capsys: pytest.CaptureFixture[str],
) -> None:
    trace_id, appointment_id, records, output = await _booking_logs(
        client, mesh, mongo_client, temporal_env, booking_world, capsys
    )

    started = [r for r in _lines(records, "booking.workflow_started") if r.get("appointment_id") == appointment_id]
    assert started, "the booking request logged no JSON line"
    _assert_correlated(started[0], trace_id)
    # The saga's activities run on the worker, under the same trace.
    for event in ("booking.validate", "booking.reserve_slot", "booking.confirm"):
        lines = [r for r in _lines(records, event) if r.get("appointment_id") == appointment_id]
        assert lines, f"the saga step {event} logged no JSON line for the booking"
        _assert_correlated(lines[0], trace_id)
    _assert_no_phi(output)


@pytest.mark.integration
async def test_billing_precheck_logs_json_with_the_booking_trace_id(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    booking_world: Any,
    temporal_env: WorkflowEnvironment,
    capsys: pytest.CaptureFixture[str],
) -> None:
    trace_id, appointment_id, records, output = await _booking_logs(
        client, mesh, mongo_client, temporal_env, booking_world, capsys
    )

    lines = [r for r in _lines(records, "billing.precheck") if r.get("appointment_id") == appointment_id]
    assert lines, "Billing logged no pre-check line for the booking"
    _assert_correlated(lines[0], trace_id)
    assert lines[0]["outcome"] in {"created", "replayed"}
    _assert_no_phi(output)


@pytest.mark.integration
async def test_availability_template_update_logs_json_with_the_request_trace_id(
    client: AsyncClient,
    booking_world: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    world = booking_world
    template_id = world.template_ids[0]
    trace_id = secrets.token_hex(16)
    capsys.readouterr()

    resp = await client.patch(
        f"/api/providers/{world.provider_id}/availability/{template_id}",
        json={"slot_duration_minutes": 30},
        headers={**auth(world.admin), "traceparent": f"00-{trace_id}-{secrets.token_hex(8)}-01"},
    )
    assert resp.status_code == 200, resp.text

    output = capsys.readouterr().out
    lines = [
        r
        for r in _lines(_json_lines(output), "availability.update.success")
        if r.get("availability_template_id") == str(template_id)
    ]
    assert lines, "the availability update logged no JSON line"
    _assert_correlated(lines[0], trace_id)
    assert lines[0]["provider_id"] == str(world.provider_id)
    _assert_no_phi(output)


@pytest.mark.integration
def test_celery_send_logs_json_with_the_task_span_trace_id(
    mongo_db: Any,  # noqa: F811
    in_memory_spans: InMemorySpanExporter,  # noqa: F811
    capsys: pytest.CaptureFixture[str],
) -> None:
    from services.notification.celery_app import instrument_publisher  # noqa: PLC0415

    instrument_publisher()
    request = new_request()
    capsys.readouterr()
    # `apply` runs the real task body in process (eager): the same code a worker runs.
    assert send_task.apply(kwargs=request).get() == "sent"

    output = capsys.readouterr().out
    sent = [r for r in _lines(_json_lines(output), "notification.sent") if r["dedupe_key"] == request["dedupe_key"]]
    assert sent, "the Celery send logged no JSON line"
    _assert_correlated(sent[0])
    run_spans = [s for s in in_memory_spans.get_finished_spans() if s.name == "run/notifications.send"]
    assert run_spans, [s.name for s in in_memory_spans.get_finished_spans()]
    assert sent[0]["trace_id"] == format(run_spans[-1].context.trace_id, "032x")
    assert delivery(mongo_db, request["dedupe_key"])["status"] == "sent"
    _assert_no_phi(output)


class _FakeMessage:
    """The slice of a confluent-kafka message the consumer base reads."""

    def __init__(self, topic: str, value: bytes, headers: list[tuple[str, bytes]]) -> None:
        self._topic, self._value, self._headers = topic, value, headers

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 7

    def key(self) -> bytes:
        return b"key"

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers


@pytest.mark.integration
def test_consumer_processing_logs_json_inside_the_consumer_span(
    mock_registry: Any,  # noqa: F811
    in_memory_spans: InMemorySpanExporter,  # noqa: F811
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del in_memory_spans  # installs the SDK tracer provider
    topic = "smarthealth.appointment.events"
    payload = booked_payload()
    record = {
        "event_id": str(uuid.uuid4()),
        "event_type": "appointment.booked",
        "schema_version": 1,
        "occurred_at": datetime.datetime.now(datetime.UTC),
        "aggregate_id": payload["appointment_id"],
        **payload,
    }
    from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: PLC0415

    value = serializer_for("appointment.booked")(record, SerializationContext(topic, MessageField.VALUE))

    handled: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope) -> None:
        handled.append(envelope)

    consumer = EventConsumer(
        group_id=f"p3test-logs-{uuid.uuid4().hex}",
        topics=[topic],
        handlers={"appointment.booked": handler},
        dlq_topic="smarthealth.dlq.p3test-logs",
        poll_timeout=0.1,
    )
    monkeypatch.setattr(consumer, "_commit", lambda msg: None)  # no broker round trip
    producer_tracer = trace.get_tracer("tests.libs.observability")
    try:
        with producer_tracer.start_as_current_span("producer") as producer:
            traceparent = f"00-{format(producer.get_span_context().trace_id, '032x')}-{format(producer.get_span_context().span_id, '016x')}-01"
        capsys.readouterr()
        outcome = consumer._handle_message(  # noqa: SLF001 - the unit under test
            _FakeMessage(topic, value, [("traceparent", traceparent.encode())])
        )
    finally:
        consumer.close()

    assert outcome is Outcome.PROCESSED and len(handled) == 1
    output = capsys.readouterr().out
    consumed = [r for r in _lines(_json_lines(output), "event.consumed") if r["event_id"] == record["event_id"]]
    assert consumed, "the consumer logged no `event.consumed` JSON line"
    # The line carries the trace of the producer that wrote the outbox row (child span).
    _assert_correlated(consumed[0], traceparent.split("-")[1])
    assert consumed[0]["outcome"] == "processed"
    _assert_no_phi(output)
