"""The consumer base against the live Kafka broker (plan 03-04, Task 1 tracer).

Unique topic and group per test, `mock://` registry: nothing reaches the real
event topics. Covers decode, child-span tracing, commit-after-effect, one
persistent loop and ignoring unhandled event types.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from opentelemetry import propagate, trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from libs.events.consumer import EventConsumer, EventEnvelope, Outcome
from tests.events.conftest import KAFKA_BOOTSTRAP, poll_for, poll_until

pytestmark = pytest.mark.integration

_producer_tracer = trace.get_tracer("tests.events.producer")


def _make(group: str, topic: str, dlq: str, handlers: dict[str, Any], **kw: Any) -> EventConsumer:
    return EventConsumer(
        group_id=group,
        topics=[topic],
        handlers=handlers,
        dlq_topic=dlq,
        poll_timeout=0.5,
        backoff_seconds=0.05,
        **kw,
    )


def _committed(group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group})
    try:
        return probe.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    finally:
        probe.close()


def test_envelope_equals_record_and_offset_is_committed(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    in_memory_spans: InMemorySpanExporter,
) -> None:
    seen: list[EventEnvelope] = []
    handler_spans: list[Any] = []

    async def handler(envelope: EventEnvelope) -> None:
        seen.append(envelope)
        handler_spans.append(trace.get_current_span().get_span_context())

    with _producer_tracer.start_as_current_span("producer") as producer_span:
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        record = produce_event(
            unique_topic, headers=[("traceparent", carrier["traceparent"].encode())]
        )
        producer_ctx = producer_span.get_span_context()

    consumer = _make(unique_group, unique_topic, dlq_topic, {"appointment.booked": handler})
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()

    assert outcomes == [Outcome.PROCESSED]
    (envelope,) = seen
    assert str(envelope.event_id) == record["event_id"]
    assert envelope.event_type == "appointment.booked"
    assert envelope.aggregate_id == record["aggregate_id"]
    assert envelope.schema_version == 1
    assert envelope.topic == unique_topic
    assert (envelope.partition, envelope.offset) == (0, 0)
    assert envelope.occurred_at.replace(microsecond=0) == record["occurred_at"].replace(microsecond=0)
    for field in ("appointment_id", "clinic_id", "patient_id", "provider_id", "slot_id"):
        assert str(envelope.payload[field]) == record[field]

    consumer_spans = [
        s for s in in_memory_spans.get_finished_spans() if s.kind == SpanKind.CONSUMER
    ]
    (span,) = consumer_spans
    assert span.context.trace_id == producer_ctx.trace_id
    assert span.parent is not None and span.parent.span_id == producer_ctx.span_id
    assert span.name == f"{unique_topic} process"
    attrs = span.attributes
    assert attrs["messaging.system"] == "kafka"
    assert attrs["messaging.destination.name"] == unique_topic
    assert attrs["messaging.kafka.consumer.group"] == unique_group
    assert attrs["smarthealth.event_type"] == "appointment.booked"
    assert attrs["smarthealth.event_id"] == record["event_id"]
    # The handler itself ran inside that span (so Celery/Temporal calls inherit it).
    assert handler_spans[0].span_id == span.context.span_id

    # Committed after the effect: the group's committed offset is 1 and a new
    # consumer in the same group receives nothing.
    assert _committed(unique_group, unique_topic) == 1
    again: list[EventEnvelope] = []

    async def recorder(e: EventEnvelope) -> None:
        again.append(e)

    second = _make(unique_group, unique_topic, dlq_topic, {"appointment.booked": recorder})
    try:
        poll_for(second, 4.0)
    finally:
        second.close()
    assert again == []


def test_handlers_share_one_event_loop(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def handler(envelope: EventEnvelope) -> None:
        loops.append(asyncio.get_running_loop())

    produce_event(unique_topic)
    produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, {"appointment.booked": handler})
    try:
        outcomes = poll_until(consumer, want=2)
    finally:
        consumer.close()
    assert outcomes == [Outcome.PROCESSED, Outcome.PROCESSED]
    assert len(loops) == 2 and loops[0] is loops[1]


def test_unhandled_event_type_is_committed_and_ignored(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    from libs.events.consumer import events_consumed_total

    called: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope) -> None:
        called.append(envelope)

    produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, {"appointment.cancelled": handler})
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()
    assert outcomes == [Outcome.IGNORED]
    assert called == []
    assert _committed(unique_group, unique_topic) == 1
    assert (
        events_consumed_total.labels(unique_group, "appointment.booked", "ignored")._value.get()
        == 1.0
    )


def test_missing_traceparent_header_is_processed_as_root_span(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    in_memory_spans: InMemorySpanExporter,
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        return None

    produce_event(unique_topic, headers=[("traceparent", None)])
    consumer = _make(unique_group, unique_topic, dlq_topic, {"appointment.booked": handler})
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()
    assert outcomes == [Outcome.PROCESSED]
    (span,) = [s for s in in_memory_spans.get_finished_spans() if s.kind == SpanKind.CONSUMER]
    assert span.parent is None


def test_extract_context_tolerates_none_and_bad_bytes() -> None:
    from libs.events.tracing import extract_context

    assert extract_context(None) is not None
    assert extract_context([("traceparent", None), ("x", b"\xff\xfe")]) is not None
    ctx = extract_context(
        [("traceparent", b"00-" + b"a" * 32 + b"-" + b"b" * 16 + b"-01")]
    )
    assert trace.get_current_span(ctx).get_span_context().trace_id == int("a" * 32, 16)


def test_confluent_kafka_otel_instrumentation_is_not_installed() -> None:
    import importlib.util

    assert importlib.util.find_spec("opentelemetry.instrumentation.confluent_kafka") is None
