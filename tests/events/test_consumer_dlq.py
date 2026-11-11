"""Poison messages, retries, outages, metrics, heartbeat and shutdown (plan 03-04, Task 2).

Live Kafka with a unique topic, DLQ topic and group per test; `mock://` registry.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import urllib.request
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition

from libs.events.consumer import (
    DeadLetterWriteError,
    EventConsumer,
    EventEnvelope,
    NonRetryableEventError,
    Outcome,
    TransientEventError,
    dlq_events_total,
    event_end_to_end_lag_seconds,
    event_processing_seconds,
    events_consumed_total,
    kafka_consumer_lag,
)
from tests.events.conftest import KAFKA_BOOTSTRAP, poll_until

pytestmark = pytest.mark.integration


def _make(group: str, topic: str, dlq: str, handler: Any, **kw: Any) -> EventConsumer:
    return EventConsumer(
        group_id=group,
        topics=[topic],
        handlers={"appointment.booked": handler},
        dlq_topic=dlq,
        poll_timeout=0.5,
        backoff_seconds=0.02,
        **kw,
    )


def _read_topic(topic: str, want: int, timeout: float = 15.0) -> list[Any]:
    reader = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"p3test-reader-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    reader.subscribe([topic])
    out: list[Any] = []
    deadline = time.monotonic() + timeout
    try:
        while len(out) < want and time.monotonic() < deadline:
            msg = reader.poll(0.5)
            if msg is not None and msg.error() is None:
                out.append(msg)
        if len(out) >= want:  # a short extra window proves there is nothing more
            extra = reader.poll(1.0)
            if extra is not None and extra.error() is None:
                out.append(extra)
    finally:
        reader.close()
    return out


def _headers(msg: Any) -> dict[str, bytes | None]:
    return dict(msg.headers() or [])


def _committed(group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group})
    try:
        return probe.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    finally:
        probe.close()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_undecodable_bytes_go_to_dlq_and_partition_moves_on(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    handled: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope) -> None:
        handled.append(envelope)

    produce_event(
        unique_topic,
        raw_value=b"not avro at all",
        key="poison-key",
        headers=[("traceparent", None), ("custom", b"keep-me")],
    )
    good = produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        outcomes = poll_until(consumer, want=2)
    finally:
        consumer.close()

    assert outcomes == [Outcome.DEAD_LETTERED, Outcome.PROCESSED]
    assert [str(e.event_id) for e in handled] == [good["event_id"]]
    (dead,) = _read_topic(dlq_topic, 1)
    assert dead.key() == b"poison-key"
    assert dead.value() == b"not avro at all"
    headers = _headers(dead)
    assert headers["custom"] == b"keep-me"
    assert headers["dlq.error_type"] == b"DeserializationError"
    assert headers["dlq.consumer_group"] == unique_group.encode()
    assert headers["dlq.original_topic"] == unique_topic.encode()
    assert headers["dlq.original_partition"] == b"0"
    assert headers["dlq.original_offset"] == b"0"
    assert headers["dlq.attempts"] == b"1"
    assert _committed(unique_group, unique_topic) == 2


def test_non_retryable_error_is_dead_lettered_after_one_attempt(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    calls: list[int] = []

    async def handler(envelope: EventEnvelope) -> None:
        calls.append(1)
        raise NonRetryableEventError("VisitNotFound: patient Jane Roe 555-0100")

    record = produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()
    assert outcomes == [Outcome.DEAD_LETTERED]
    assert len(calls) == 1
    (dead,) = _read_topic(dlq_topic, 1)
    headers = _headers(dead)
    assert headers["dlq.error_type"] == b"NonRetryableEventError"
    assert headers["dlq.attempts"] == b"1"
    # Original value is preserved byte for byte; the event id is still decodable by replay.
    assert dead.key() == record["aggregate_id"].encode()


def test_unexpected_error_is_retried_three_times_then_dead_lettered(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    calls: list[int] = []

    async def handler(envelope: EventEnvelope) -> None:
        calls.append(1)
        raise ValueError("secret detail Jane Roe 1980-01-01")

    produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()
    assert outcomes == [Outcome.DEAD_LETTERED]
    assert len(calls) == 3
    (dead,) = _read_topic(dlq_topic, 1)
    headers = _headers(dead)
    assert headers["dlq.attempts"] == b"3"
    assert headers["dlq.error_type"] == b"ValueError"
    # No header (name or value) carries the exception message text.
    blob = b" ".join(
        [k.encode() + b"=" + (v or b"") for k, v in headers.items()]
    )
    assert b"secret detail" not in blob and b"Jane" not in blob
    assert _committed(unique_group, unique_topic) == 1


def test_handler_failing_twice_then_succeeding_is_processed_once(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    calls: list[int] = []

    async def handler(envelope: EventEnvelope) -> None:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("flaky")

    produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        outcomes = poll_until(consumer)
    finally:
        consumer.close()
    assert outcomes == [Outcome.PROCESSED]
    assert len(calls) == 3
    assert _read_topic(dlq_topic, 1, timeout=3.0) == []
    assert _committed(unique_group, unique_topic) == 1


def test_transient_error_is_never_dead_lettered_and_retries_until_it_succeeds(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    calls: list[int] = []

    async def handler(envelope: EventEnvelope) -> None:
        calls.append(1)
        if len(calls) <= 2:
            raise TransientEventError("temporal unavailable")

    produce_event(unique_topic)
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        first = poll_until(consumer)
        assert first == [Outcome.RETRY_LATER]
        # Not committed while the outage lasts.
        assert _committed(unique_group, unique_topic) < 0
        rest = poll_until(consumer, want=2)
    finally:
        consumer.close()
    assert rest == [Outcome.RETRY_LATER, Outcome.PROCESSED]
    assert len(calls) == 3  # the same message delivered three times, handled once
    assert _read_topic(dlq_topic, 1, timeout=3.0) == []
    assert _committed(unique_group, unique_topic) == 1


def test_failed_dlq_write_does_not_commit_and_the_message_is_redelivered(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        raise NonRetryableEventError("bad")

    produce_event(unique_topic)
    broken = _make(
        unique_group,
        unique_topic,
        dlq_topic,
        handler,
        dlq_producer_overrides={
            "bootstrap.servers": "127.0.0.1:1",
            "message.timeout.ms": 1500,
            "socket.timeout.ms": 1000,
        },
        dlq_flush_timeout=3.0,
    )
    try:
        with pytest.raises(DeadLetterWriteError):
            poll_until(broken)
    finally:
        broken.close()
    assert _committed(unique_group, unique_topic) < 0

    # Process restart: a healthy consumer in the same group gets the message again.
    healthy = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        outcomes = poll_until(healthy)
    finally:
        healthy.close()
    assert outcomes == [Outcome.DEAD_LETTERED]
    assert len(_read_topic(dlq_topic, 1)) == 1
    assert _committed(unique_group, unique_topic) == 1


def test_null_traceparent_header_is_processed(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        return None

    produce_event(unique_topic, headers=[("traceparent", None), ("eventType", b"appointment.booked")])
    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    try:
        assert poll_until(consumer) == [Outcome.PROCESSED]
    finally:
        consumer.close()


def test_metrics_samples_and_metrics_endpoint(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        if str(envelope.payload["clinic_id"]) == poison_clinic:
            raise NonRetryableEventError("x")

    poison_clinic = str(uuid.uuid4())
    from tests.events.conftest import booked_payload

    produce_event(unique_topic)
    produce_event(unique_topic, payload=booked_payload(clinic_id=poison_clinic))
    produce_event(unique_topic)
    port = _free_port()
    consumer = _make(unique_group, unique_topic, dlq_topic, handler, metrics_port=port)
    try:
        assert poll_until(consumer, want=2) == [Outcome.PROCESSED, Outcome.DEAD_LETTERED]
        consumer.refresh_lag(force=True)
        # One message is still unread: high watermark 3 minus position 2.
        assert kafka_consumer_lag.labels(unique_group, unique_topic, "0")._value.get() == 1.0
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()  # noqa: S310
    finally:
        consumer.close()

    g = unique_group
    assert events_consumed_total.labels(g, "appointment.booked", "processed")._value.get() == 1.0
    assert events_consumed_total.labels(g, "appointment.booked", "dead_lettered")._value.get() == 1.0
    assert dlq_events_total.labels(g, "NonRetryableEventError")._value.get() == 1.0
    processing = event_processing_seconds.labels(g, "appointment.booked")
    assert sum(b.get() for b in processing._buckets) == 2.0
    lag = event_end_to_end_lag_seconds.labels(g, "appointment.booked")
    assert sum(b.get() for b in lag._buckets) == 2.0
    assert lag._sum.get() >= 0.0
    for name in (
        "events_consumed_total",
        "event_processing_seconds_count",
        "event_end_to_end_lag_seconds_count",
        "dlq_events_total",
        "kafka_consumer_lag",
    ):
        assert f'{name}{{' in body and f'group="{g}"' in body, name


def test_heartbeat_file_advances_while_idle_and_run_stops_cleanly(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    tmp_path: Any,
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        return None

    beat = tmp_path / "heartbeat"
    consumer = _make(unique_group, unique_topic, dlq_topic, handler, heartbeat_path=beat)
    consumer._poll_timeout = 0.3
    stop = threading.Event()
    worker = threading.Thread(target=consumer.run, args=(stop,), daemon=True)
    worker.start()
    try:
        mtimes: set[int] = set()
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if beat.exists():
                mtimes.add(os.stat(beat).st_mtime_ns)
            time.sleep(0.05)
        assert len(mtimes) >= 3, mtimes
    finally:
        stop.set()
        worker.join(timeout=15)
    assert not worker.is_alive()


def test_stop_event_ends_run_without_extra_commit(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
) -> None:
    async def handler(envelope: EventEnvelope) -> None:
        return None

    consumer = _make(unique_group, unique_topic, dlq_topic, handler)
    stop = threading.Event()
    stop.set()
    consumer.run(stop)  # returns immediately and closes
    assert consumer._closed
    assert _committed(unique_group, unique_topic) < 0
