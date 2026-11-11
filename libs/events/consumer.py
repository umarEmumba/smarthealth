"""The one Kafka consumer base for every Phase 3 consumer role (plan 03-04).

Used by visit-downstream, notification-trigger and analytics-projection
(D-08: one consumer group per role, one DLQ topic per group).

Shape (research Pattern 3):

* A SYNCHRONOUS confluent-kafka poll loop (`poll_once`) on the calling thread.
* Handlers are async. They all run on ONE persistent `asyncio.Runner` created at
  construction, never `asyncio.run` per message, because `libs/db/postgres.py`
  caches engines per running loop (research Pitfall 9). Each call passes a
  fresh copy of the current contextvars so the handler sees the consumer span
  (a Runner otherwise reuses the context copied when it was created).
* `enable.auto.commit` is False. An offset is committed synchronously only
  after the handler's effect, after an IGNORE decision, or after a confirmed
  DLQ write (T-3-14).
* The processing span is a CHILD of the producer's span (traceparent header,
  see `libs.events.tracing`), so one booking stays one trace (D-11).
* Poison messages (undecodable bytes, `NonRetryableEventError`, retries
  exhausted) are copied to the group's DLQ topic with error TYPE headers only
  (never exception text) and the partition moves on (EVENT-05, T-3-11).
  `TransientEventError` (an infrastructure outage) is NEVER dead-lettered: the
  offset is not committed, the consumer seeks back to the message and retries
  with capped backoff, so `max.poll.interval.ms` is never exceeded either.
* Prometheus metrics plus a heartbeat file give liveness (OBS-01 groundwork).

Logs carry ids and types only, never a payload (PHI rule, T-3-13).
"""

from __future__ import annotations

import asyncio
import contextvars
import enum
import os
import signal
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from libs.config import shared_settings
from libs.events.registry import deserializer
from libs.events.tracing import extract_context

logger = structlog.get_logger("smarthealth.events.consumer")

DESERIALIZATION_ERROR = "DeserializationError"
UNKNOWN_EVENT_TYPE_LABEL = "unknown"
_LAG_REFRESH_SECONDS = 15.0
_TRANSIENT_BACKOFF_CAP_SECONDS = 30.0

events_consumed_total = Counter(
    "events_consumed_total",
    "Events taken off Kafka by a consumer group, by outcome.",
    ["group", "event_type", "outcome"],
)
event_processing_seconds = Histogram(
    "event_processing_seconds",
    "Handler time per event including in-process retries.",
    ["group", "event_type"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)
event_end_to_end_lag_seconds = Histogram(
    "event_end_to_end_lag_seconds",
    "Seconds from the event's occurred_at to the end of its handling.",
    ["group", "event_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0),
)
dlq_events_total = Counter(
    "dlq_events_total",
    "Messages copied to a dead-letter topic, by error type.",
    ["group", "error_type"],
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "High watermark minus the consumer's position, per partition.",
    ["group", "topic", "partition"],
)

_metrics_ports: set[int] = set()
_metrics_lock = threading.Lock()


class NonRetryableEventError(Exception):
    """Handler signal: this event can never succeed; dead-letter it now."""


class TransientEventError(Exception):
    """Handler signal: an infrastructure outage; retry later, never dead-letter."""


class DeadLetterWriteError(RuntimeError):
    """The DLQ write was not confirmed; the source offset was NOT committed."""


class Outcome(enum.Enum):
    PROCESSED = "processed"
    IGNORED = "ignored"
    DEAD_LETTERED = "dead_lettered"
    RETRY_LATER = "retry_later"


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """A decoded event. `payload` is the full decoded Avro record (envelope
    fields included), so handlers read e.g. `payload["visit_id"]`."""

    event_id: uuid.UUID
    event_type: str
    aggregate_id: str
    occurred_at: datetime
    schema_version: int
    payload: dict[str, Any]
    topic: str
    partition: int
    offset: int


Handler = Callable[[EventEnvelope], Awaitable[None]]


class _PoisonMessageError(Exception):
    """Internal: the message cannot be decoded into an event."""


class _TransientDecodeError(Exception):
    """Internal: decoding failed because the registry is unreachable."""


def _start_metrics_server(port: int) -> None:
    with _metrics_lock:
        if port in _metrics_ports:
            return
        # Internal port: Compose publishes no host mapping for it.
        start_http_server(port)
        _metrics_ports.add(port)


class EventConsumer:
    def __init__(
        self,
        *,
        group_id: str,
        topics: Sequence[str],
        handlers: Mapping[str, Handler],
        dlq_topic: str,
        bootstrap_servers: str | None = None,
        registry_client: SchemaRegistryClient | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        heartbeat_path: str | os.PathLike[str] | None = None,
        metrics_port: int | None = None,
        poll_timeout: float = 1.0,
        consumer_overrides: Mapping[str, Any] | None = None,
        dlq_producer_overrides: Mapping[str, Any] | None = None,
        dlq_flush_timeout: float = 10.0,
    ) -> None:
        self.group_id = group_id
        self.topics = list(topics)
        self.dlq_topic = dlq_topic
        self._handlers = dict(handlers)
        self._max_attempts = max(1, max_attempts)
        self._backoff = backoff_seconds
        self._heartbeat_path = Path(heartbeat_path) if heartbeat_path else None
        self._poll_timeout = poll_timeout
        self._dlq_flush_timeout = dlq_flush_timeout
        self._bootstrap = bootstrap_servers or shared_settings.kafka_bootstrap_servers
        self._dlq_overrides = dict(dlq_producer_overrides or {})
        self._deserializer: AvroDeserializer = (
            AvroDeserializer(registry_client) if registry_client is not None else deserializer()
        )
        self._tracer = trace.get_tracer("smarthealth.events.consumer")
        self._stop = threading.Event()
        self._transient_counts: dict[tuple[str, int, int], int] = {}
        self._last_lag_refresh = 0.0
        self._heartbeat_warned = False
        self._closed = False
        self._dlq_producer: Producer | None = None
        # One persistent loop for every handler call (Pitfall 9).
        self._runner = asyncio.Runner()

        conf: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
        }
        conf.update(consumer_overrides or {})
        self._consumer = Consumer(conf)
        self._consumer.subscribe(self.topics)
        if metrics_port is not None:
            _start_metrics_server(metrics_port)

    # ------------------------------------------------------------------ loop

    def poll_once(self) -> Outcome | None:
        """Poll once; handle at most one message. None when nothing arrived."""
        self._touch_heartbeat()
        msg = self._consumer.poll(self._poll_timeout)
        self.refresh_lag()
        if msg is None:
            return None
        error = msg.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            # Non-fatal broker/transport errors (and unknown-topic while a
            # topic is being created) are retried by librdkafka itself.
            logger.warning("event.consumer_error", group=self.group_id, error_type=str(error.name()))
            return None
        outcome = self._handle_message(msg)
        self._touch_heartbeat()
        return outcome

    def run(self, stop: threading.Event | None = None) -> None:
        """Poll until `stop` is set or SIGTERM/SIGINT arrives, then close."""
        if stop is not None:
            self._stop = stop
        restore = self._install_signal_handlers()
        try:
            while not self._stop.is_set():
                self.poll_once()
        finally:
            restore()
            self.close()

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        """Leave the group (commits nothing extra) and release the loop."""
        if self._closed:
            return
        self._closed = True
        try:
            self._consumer.close()
        finally:
            if self._dlq_producer is not None:
                self._dlq_producer.flush(2)
                self._dlq_producer = None
            self._runner.close()

    def _install_signal_handlers(self) -> Callable[[], None]:
        if threading.current_thread() is not threading.main_thread():
            return lambda: None
        previous = {
            sig: signal.signal(sig, lambda *_: self._stop.set())
            for sig in (signal.SIGTERM, signal.SIGINT)
        }

        def restore() -> None:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

        return restore

    # --------------------------------------------------------------- message

    def _handle_message(self, msg: Any) -> Outcome:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        try:
            envelope = self._decode(msg)
        except _TransientDecodeError:
            return self._retry_later(msg, UNKNOWN_EVENT_TYPE_LABEL)
        except _PoisonMessageError:
            return self._dead_letter(
                msg, DESERIALIZATION_ERROR, attempts=1, event_type=UNKNOWN_EVENT_TYPE_LABEL
            )

        handler = self._handlers.get(envelope.event_type)
        if handler is None:
            self._commit(msg)
            self._record(envelope.event_type, Outcome.IGNORED, envelope)
            return Outcome.IGNORED

        started = time.perf_counter()
        attributes = {
            "messaging.system": "kafka",
            "messaging.destination.name": topic,
            "messaging.kafka.consumer.group": self.group_id,
            "messaging.kafka.message.offset": offset,
            "messaging.destination.partition.id": str(partition),
            "smarthealth.event_type": envelope.event_type,
            "smarthealth.event_id": str(envelope.event_id),
        }
        with self._tracer.start_as_current_span(
            f"{topic} process",
            context=extract_context(msg.headers()),
            kind=SpanKind.CONSUMER,
            attributes=attributes,
        ) as span:
            for attempt in range(1, self._max_attempts + 1):
                try:
                    self._runner.run(handler(envelope), context=contextvars.copy_context())
                except TransientEventError:
                    span.set_status(Status(StatusCode.ERROR, "TransientEventError"))
                    return self._retry_later(msg, envelope.event_type, envelope)
                except NonRetryableEventError as exc:
                    span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
                    return self._finish_dead_letter(
                        msg, type(exc).__name__, attempt, envelope, started
                    )
                except Exception as exc:  # noqa: BLE001 - handler boundary
                    span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
                    if attempt >= self._max_attempts:
                        return self._finish_dead_letter(
                            msg, type(exc).__name__, attempt, envelope, started
                        )
                    time.sleep(self._backoff * 2 ** (attempt - 1))
                else:
                    break
            self._transient_counts.pop((topic, partition, offset), None)
            self._commit(msg)
            # Logged INSIDE the processing span so the `event.consumed` line carries the
            # trace_id / span_id of the booking's trace (OBS-03): outside it the line would
            # have no active span and could not be found by trace id.
            self._record(envelope.event_type, Outcome.PROCESSED, envelope)

        event_processing_seconds.labels(self.group_id, envelope.event_type).observe(
            time.perf_counter() - started
        )
        return Outcome.PROCESSED

    def _decode(self, msg: Any) -> EventEnvelope:
        topic = msg.topic()
        try:
            record = self._deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
        except SerializationError as exc:
            raise _PoisonMessageError from exc
        except SchemaRegistryError as exc:
            status = getattr(exc, "http_status_code", -1)
            if isinstance(status, int) and 400 <= status < 500:
                raise _PoisonMessageError from exc  # unknown schema id: forged or foreign
            raise _TransientDecodeError from exc
        except (httpx.TransportError, ConnectionError, TimeoutError) as exc:
            raise _TransientDecodeError from exc
        except Exception as exc:  # noqa: BLE001 - corrupt Avro body etc.
            raise _PoisonMessageError from exc

        if not isinstance(record, dict):
            raise _PoisonMessageError
        try:
            event_type = record.get("event_type") or self._header(msg, "eventType")
            if not event_type:
                raise _PoisonMessageError
            occurred_at = record["occurred_at"]
            if isinstance(occurred_at, (int, float)):
                occurred_at = datetime.fromtimestamp(occurred_at / 1000, tz=UTC)
            return EventEnvelope(
                event_id=uuid.UUID(str(record["event_id"])),
                event_type=str(event_type),
                aggregate_id=str(record["aggregate_id"]),
                occurred_at=occurred_at,
                schema_version=int(record["schema_version"]),
                payload=record,
                topic=topic,
                partition=msg.partition(),
                offset=msg.offset(),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise _PoisonMessageError from exc

    @staticmethod
    def _header(msg: Any, name: str) -> str | None:
        for key, value in msg.headers() or ():
            if key == name and value is not None:
                return value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
        return None

    # ----------------------------------------------------- commit / retry / DLQ

    def _commit(self, msg: Any) -> None:
        self._consumer.commit(message=msg, asynchronous=False)

    def _retry_later(
        self, msg: Any, event_type: str, envelope: EventEnvelope | None = None
    ) -> Outcome:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        key = (topic, partition, offset)
        count = self._transient_counts.get(key, 0)
        self._transient_counts[key] = count + 1
        self._consumer.seek(TopicPartition(topic, partition, offset))
        self._record(event_type, Outcome.RETRY_LATER, envelope, topic=topic, offset=offset)
        self._sleep(min(_TRANSIENT_BACKOFF_CAP_SECONDS, self._backoff * 2**count))
        return Outcome.RETRY_LATER

    def _sleep(self, seconds: float) -> None:
        """Sleep in slices so the heartbeat stays fresh and stop is honoured."""
        deadline = time.monotonic() + seconds
        while True:
            self._touch_heartbeat()
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._stop.wait(min(remaining, self._poll_timeout)):
                return

    def _finish_dead_letter(
        self,
        msg: Any,
        error_type: str,
        attempts: int,
        envelope: EventEnvelope,
        started: float,
    ) -> Outcome:
        outcome = self._dead_letter(
            msg, error_type, attempts=attempts, event_type=envelope.event_type, envelope=envelope
        )
        event_processing_seconds.labels(self.group_id, envelope.event_type).observe(
            time.perf_counter() - started
        )
        return outcome

    def _dead_letter(
        self,
        msg: Any,
        error_type: str,
        *,
        attempts: int,
        event_type: str,
        envelope: EventEnvelope | None = None,
    ) -> Outcome:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        headers = list(msg.headers() or [])
        headers += [
            ("dlq.error_type", error_type.encode()),
            ("dlq.consumer_group", self.group_id.encode()),
            ("dlq.original_topic", topic.encode()),
            ("dlq.original_partition", str(partition).encode()),
            ("dlq.original_offset", str(offset).encode()),
            ("dlq.attempts", str(attempts).encode()),
        ]
        self._write_dlq(msg.key(), msg.value(), headers)
        self._commit(msg)
        self._transient_counts.pop((topic, partition, offset), None)
        dlq_events_total.labels(self.group_id, error_type).inc()
        logger.error(
            "event.dead_lettered",
            group=self.group_id,
            topic=topic,
            partition=partition,
            offset=offset,
            event_type=event_type,
            error_type=error_type,
        )
        self._record(event_type, Outcome.DEAD_LETTERED, envelope, log=False)
        return Outcome.DEAD_LETTERED

    def _write_dlq(
        self, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes | None]]
    ) -> None:
        if self._dlq_producer is None:
            conf: dict[str, Any] = {
                "bootstrap.servers": self._bootstrap,
                "acks": "all",
                "enable.idempotence": True,
            }
            conf.update(self._dlq_overrides)
            self._dlq_producer = Producer(conf)
        producer = self._dlq_producer
        failures: list[Any] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                failures.append(err)

        try:
            producer.produce(
                self.dlq_topic, key=key, value=value, headers=headers, on_delivery=on_delivery
            )
            remaining = producer.flush(self._dlq_flush_timeout)
        except Exception as exc:
            self._drop_dlq_producer()
            raise DeadLetterWriteError(type(exc).__name__) from exc
        if remaining > 0 or failures:
            self._drop_dlq_producer()
            raise DeadLetterWriteError("DLQ write not confirmed")

    def _drop_dlq_producer(self) -> None:
        """Purge whatever is still queued so a later retry cannot duplicate it."""
        producer, self._dlq_producer = self._dlq_producer, None
        if producer is not None:
            try:
                producer.purge()
                producer.flush(0.5)
            except Exception:  # noqa: BLE001, S110 - best effort
                pass

    # ------------------------------------------------------ metrics / liveness

    def _record(
        self,
        event_type: str,
        outcome: Outcome,
        envelope: EventEnvelope | None,
        *,
        topic: str | None = None,
        offset: int | None = None,
        log: bool = True,
    ) -> None:
        events_consumed_total.labels(self.group_id, event_type, outcome.value).inc()
        if envelope is not None and outcome in (Outcome.PROCESSED, Outcome.DEAD_LETTERED):
            lag = (datetime.now(UTC) - envelope.occurred_at).total_seconds()
            event_end_to_end_lag_seconds.labels(self.group_id, event_type).observe(max(0.0, lag))
        if log:
            logger.info(
                "event.consumed",
                group=self.group_id,
                event_type=event_type,
                event_id=str(envelope.event_id) if envelope else None,
                topic=topic or (envelope.topic if envelope else None),
                offset=offset if offset is not None else (envelope.offset if envelope else None),
                outcome=outcome.value,
            )

    def refresh_lag(self, force: bool = False) -> None:
        """Set `kafka_consumer_lag` from watermarks minus position (every 15 s)."""
        now = time.monotonic()
        if not force and now - self._last_lag_refresh < _LAG_REFRESH_SECONDS:
            return
        self._last_lag_refresh = now
        try:
            assigned = self._consumer.assignment()
            if not assigned:
                return
            for tp in self._consumer.position(assigned):
                low, high = self._consumer.get_watermark_offsets(tp, timeout=2.0)
                position = tp.offset
                if position < 0:
                    committed = self._consumer.committed([TopicPartition(tp.topic, tp.partition)], 2.0)
                    position = committed[0].offset if committed and committed[0].offset >= 0 else low
                kafka_consumer_lag.labels(self.group_id, tp.topic, str(tp.partition)).set(
                    max(0, high - position)
                )
        except (KafkaException, RuntimeError) as exc:
            logger.debug("event.lag_refresh_failed", group=self.group_id, error_type=type(exc).__name__)

    def _touch_heartbeat(self) -> None:
        if self._heartbeat_path is None:
            return
        try:
            self._heartbeat_path.touch()
        except OSError as exc:
            if not self._heartbeat_warned:
                self._heartbeat_warned = True
                logger.warning("event.heartbeat_failed", error_type=type(exc).__name__)


__all__ = [
    "DeadLetterWriteError",
    "EventConsumer",
    "EventEnvelope",
    "Handler",
    "KafkaError",
    "NonRetryableEventError",
    "Outcome",
    "TransientEventError",
    "dlq_events_total",
    "event_end_to_end_lag_seconds",
    "event_processing_seconds",
    "events_consumed_total",
    "kafka_consumer_lag",
]
