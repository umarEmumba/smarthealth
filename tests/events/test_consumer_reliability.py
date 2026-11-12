"""Consumer base reliability: registry back-pressure, the transient cap, commit failures
(plan 03-21; WR-02, WR-03, WR-07, CR-01, EVENT-05, T-3-63..T-3-65).

Most tests drive the REAL `_handle_message` / `_decode` / `_commit` code with the stand-ins
of `tests/events/standins.py` (no broker, no clock waiting); the cap test also runs on the
live broker with a unique topic, DLQ topic and group, like `test_consumer_dlq.py`.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError
from structlog.testing import capture_logs
from temporalio.service import RPCError, RPCStatusCode

from libs.events import classify
from libs.events import consumer as consumer_module
from libs.events.consumer import (
    DESERIALIZATION_ERROR,
    TRANSIENT_EXHAUSTED,
    EventConsumer,
    EventEnvelope,
    Outcome,
    TransientEventError,
    dlq_events_total,
    event_commit_failures_total,
)
from tests.events.conftest import poll_until
from tests.events.standins import (
    FakeMessage,
    StandInKafkaConsumer,
    event_record,
    install,
    stub_decoder,
)

GROUP = "p3test-reliability-group"
EVENT_TYPE = "appointment.booked"
SECRET_TEXT = "Jane Doe 1980-01-01 must never reach a header or a log"
TOPIC = "p3test.reliability.topic"
DLQ = "p3test.reliability.dlq"


class DlqRecorder:
    """Replaces `_write_dlq`: records what would have been produced to the DLQ topic."""

    def __init__(self) -> None:
        self.records: list[tuple[bytes | None, bytes | None, dict[str, bytes | None]]] = []

    def __call__(self, key: bytes | None, value: bytes | None, headers: list[Any]) -> None:
        self.records.append((key, value, dict(headers)))


def make_consumer(
    handlers: dict[str, Any] | None = None,
    stand_in: StandInKafkaConsumer | None = None,
    **kwargs: Any,
) -> tuple[EventConsumer, StandInKafkaConsumer, DlqRecorder]:
    group_id = kwargs.pop("group_id", GROUP)
    consumer = EventConsumer(
        group_id=group_id,
        topics=[TOPIC],
        handlers=handlers or {},
        dlq_topic=DLQ,
        backoff_seconds=0.0,
        **kwargs,
    )
    stand = stand_in or StandInKafkaConsumer()
    install(consumer, stand)
    recorder = DlqRecorder()
    consumer._write_dlq = recorder  # type: ignore[method-assign]  # noqa: SLF001
    return consumer, stand, recorder


def _failing_decoder(error: BaseException) -> Any:
    def decode(_value: Any, _context: Any) -> Any:
        raise error

    return decode


# ------------------------------------------------------------ registry decode (WR-02)


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_registry_back_pressure_and_server_errors_while_decoding_are_retried(status: int) -> None:
    consumer, stand_in, dlq = make_consumer()
    consumer._deserializer = _failing_decoder(SchemaRegistryError(status, 1, "registry"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage(offset=5)) is Outcome.RETRY_LATER  # noqa: SLF001
        assert stand_in.seeks == [(FakeMessage().topic(), 0, 5)]
        assert stand_in.commits == []
        assert dlq.records == []
    finally:
        consumer.close()


def test_a_registry_answer_without_a_status_is_retried() -> None:
    consumer, stand_in, dlq = make_consumer()
    consumer._deserializer = _failing_decoder(SchemaRegistryError(-1, 1, "no status"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage()) is Outcome.RETRY_LATER  # noqa: SLF001
        assert dlq.records == []
        assert stand_in.commits == []
    finally:
        consumer.close()


@pytest.mark.parametrize("status", [404, 422])
def test_other_registry_client_errors_are_dead_lettered_as_poison(status: int) -> None:
    consumer, stand_in, dlq = make_consumer()
    consumer._deserializer = _failing_decoder(SchemaRegistryError(status, 40403, "unknown id"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage(offset=9)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert len(dlq.records) == 1
        assert dlq.records[0][2]["dlq.error_type"] == DESERIALIZATION_ERROR.encode()
        assert stand_in.commits == [9]
    finally:
        consumer.close()


def test_a_serialization_error_is_dead_lettered() -> None:
    consumer, stand_in, dlq = make_consumer()
    consumer._deserializer = _failing_decoder(SerializationError("bad magic byte"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage(offset=2)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert dlq.records[0][2]["dlq.error_type"] == DESERIALIZATION_ERROR.encode()
        assert stand_in.commits == [2]
    finally:
        consumer.close()


def test_corrupt_avro_bytes_are_dead_lettered() -> None:
    consumer, _stand_in, dlq = make_consumer()
    consumer._deserializer = _failing_decoder(ValueError("corrupt body"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage()) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert len(dlq.records) == 1
    finally:
        consumer.close()


# ------------------------------------------------------- transient cap (WR-03, T-3-63)


class FakeClock:
    """The cap's clock (`libs.events.consumer._monotonic`), advanced by the test."""

    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(consumer_module, "_monotonic", fake)
    return fake


def _always_transient(envelope: EventEnvelope) -> Any:
    async def handler(_envelope: EventEnvelope) -> None:
        raise TransientEventError(SECRET_TEXT)

    return handler(envelope)


def _arm(consumer: EventConsumer) -> None:
    consumer._deserializer = stub_decoder(event_record(EVENT_TYPE))  # type: ignore[assignment]  # noqa: SLF001


def _header_text(headers: dict[str, bytes | None]) -> str:
    return " ".join(value.decode() for value in headers.values() if value is not None)


def test_the_cap_needs_both_thresholds_and_then_dead_letters_as_transient_exhausted(
    clock: FakeClock,
) -> None:
    group = f"p3test-cap-{uuid.uuid4().hex}"
    consumer, stand_in, dlq = make_consumer(
        {EVENT_TYPE: _always_transient},
        max_transient_attempts=3,
        max_transient_seconds=100.0,
        group_id=group,
    )
    _arm(consumer)
    before = dlq_events_total.labels(group, TRANSIENT_EXHAUSTED)._value.get()  # noqa: SLF001
    try:
        # Far above the attempt floor, below the time cap: held, never dead-lettered.
        for _ in range(8):
            assert consumer._handle_message(FakeMessage(offset=4)) is Outcome.RETRY_LATER  # noqa: SLF001
            clock.now += 10.0  # 80 s in total: under the 100 s cap
        assert dlq.records == [] and stand_in.commits == []
        assert len(stand_in.seeks) == 8
        # The time cap is reached on the next failure: dead-lettered, offset moves on.
        clock.now += 30.0
        assert consumer._handle_message(FakeMessage(offset=4)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert stand_in.commits == [4]
        assert len(dlq.records) == 1
        key, value, headers = dlq.records[0]
        assert (key, value) == (b"key-1", b"opaque-avro-bytes")
        assert headers["dlq.error_type"] == TRANSIENT_EXHAUSTED.encode()
        assert headers["dlq.attempts"] == b"9"
        assert SECRET_TEXT not in _header_text(headers)  # error TYPE only (T-3-64)
        assert dlq_events_total.labels(group, TRANSIENT_EXHAUSTED)._value.get() == before + 1  # noqa: SLF001
        assert consumer._transient == {}  # noqa: SLF001 - the per-offset state is cleared
    finally:
        consumer.close()


def test_the_attempt_floor_is_required_even_when_the_time_cap_has_passed(clock: FakeClock) -> None:
    consumer, stand_in, dlq = make_consumer(
        {EVENT_TYPE: _always_transient}, max_transient_attempts=3, max_transient_seconds=100.0
    )
    _arm(consumer)
    try:
        for _ in range(2):
            assert consumer._handle_message(FakeMessage(offset=1)) is Outcome.RETRY_LATER  # noqa: SLF001
            clock.now += 500.0  # long past the time cap, but only 1 and 2 attempts so far
        assert dlq.records == []
        assert consumer._handle_message(FakeMessage(offset=1)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert dlq.records[0][2]["dlq.attempts"] == b"3"
        assert stand_in.commits == [1]
    finally:
        consumer.close()


def _rpc(status: RPCStatusCode) -> Callable[[], BaseException]:
    return lambda: RPCError("temporal", status, b"")


@pytest.mark.parametrize(
    "make_error",
    [
        pytest.param(_rpc(RPCStatusCode.INTERNAL), id="INTERNAL"),
        pytest.param(_rpc(RPCStatusCode.UNKNOWN), id="UNKNOWN"),
        pytest.param(lambda: RuntimeError("Client is closed"), id="closed-client"),
    ],
)
def test_a_temporal_outage_ends_in_transient_exhausted_not_in_a_quick_dead_letter(
    make_error: Callable[[], BaseException], clock: FakeClock
) -> None:
    """W3: the statuses stay transient; with the DEFAULT cap (10 attempts and 900 s) a
    handler that maps them like the visit and notification handlers do is held far past
    the base's 3 handler-error attempts, then dead-lettered as TransientExhausted."""

    async def handler(_envelope: EventEnvelope) -> None:
        try:
            raise make_error()
        except Exception as exc:  # noqa: BLE001
            if classify.is_transient(exc, temporal=True):
                raise TransientEventError(type(exc).__name__) from exc
            raise

    consumer, stand_in, dlq = make_consumer({EVENT_TYPE: handler})
    _arm(consumer)
    try:
        for _ in range(14):
            assert consumer._handle_message(FakeMessage(offset=6)) is Outcome.RETRY_LATER  # noqa: SLF001
            clock.now += 20.0  # 280 s: past 10 attempts, under 900 s
        assert dlq.records == [] and stand_in.commits == []
        clock.now += 700.0
        assert consumer._handle_message(FakeMessage(offset=6)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert dlq.records[0][2]["dlq.error_type"] == TRANSIENT_EXHAUSTED.encode()
        assert dlq.records[0][2]["dlq.attempts"] == b"15"
        assert stand_in.commits == [6]
    finally:
        consumer.close()


def test_the_transient_state_is_cleared_after_a_success(clock: FakeClock) -> None:
    del clock
    calls: list[int] = []

    async def handler(_envelope: EventEnvelope) -> None:
        calls.append(1)
        if len(calls) <= 2:
            raise TransientEventError("down")

    consumer, stand_in, _dlq = make_consumer({EVENT_TYPE: handler})
    _arm(consumer)
    try:
        assert consumer._handle_message(FakeMessage(offset=8)) is Outcome.RETRY_LATER  # noqa: SLF001
        assert consumer._handle_message(FakeMessage(offset=8)) is Outcome.RETRY_LATER  # noqa: SLF001
        assert len(consumer._transient) == 1  # noqa: SLF001
        assert consumer._handle_message(FakeMessage(offset=8)) is Outcome.PROCESSED  # noqa: SLF001
        assert consumer._transient == {}  # noqa: SLF001
        assert stand_in.commits == [8]
    finally:
        consumer.close()


def test_a_registry_outage_while_decoding_is_capped_too(clock: FakeClock) -> None:
    consumer, stand_in, dlq = make_consumer(
        max_transient_attempts=2, max_transient_seconds=50.0
    )
    consumer._deserializer = _failing_decoder(SchemaRegistryError(503, 1, "registry down"))  # type: ignore[assignment]  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage(offset=2)) is Outcome.RETRY_LATER  # noqa: SLF001
        clock.now += 60.0
        assert consumer._handle_message(FakeMessage(offset=2)) is Outcome.DEAD_LETTERED  # noqa: SLF001
        assert dlq.records[0][2]["dlq.error_type"] == TRANSIENT_EXHAUSTED.encode()
        assert stand_in.commits == [2]
    finally:
        consumer.close()


# -------------------------------------------------------------- commit failures (WR-07)


def _kafka_error(*, fatal: bool = False) -> KafkaException:
    code = KafkaError._FATAL if fatal else KafkaError.REQUEST_TIMED_OUT
    return KafkaException(KafkaError(code, "reason carrying no payload", fatal=fatal, retriable=not fatal))


def _processing_consumer(stand_in: StandInKafkaConsumer, **kwargs: Any) -> EventConsumer:
    async def handler(_envelope: EventEnvelope) -> None:
        return None

    consumer, _stand, _dlq = make_consumer(
        {EVENT_TYPE: handler}, stand_in=stand_in, group_id=f"p3test-commit-{uuid.uuid4().hex}", **kwargs
    )
    _arm(consumer)
    return consumer


def test_a_non_fatal_commit_failure_is_retried_and_counted() -> None:
    stand_in = StandInKafkaConsumer([_kafka_error(), _kafka_error(), None])
    consumer = _processing_consumer(stand_in)
    counter = event_commit_failures_total.labels(consumer.group_id)
    before = counter._value.get()  # noqa: SLF001
    try:
        assert consumer._handle_message(FakeMessage(offset=3)) is Outcome.PROCESSED  # noqa: SLF001
        assert stand_in.commit_calls == 3 and stand_in.commits == [3]
        assert counter._value.get() == before + 2  # noqa: SLF001
    finally:
        consumer.close()


def test_a_commit_that_always_fails_is_abandoned_without_ending_the_consumer() -> None:
    stand_in = StandInKafkaConsumer(always_fail=_kafka_error())
    consumer = _processing_consumer(stand_in, commit_attempts=3)
    counter = event_commit_failures_total.labels(consumer.group_id)
    before = counter._value.get()  # noqa: SLF001
    try:
        with capture_logs() as logs:
            assert consumer._handle_message(FakeMessage(offset=3)) is Outcome.PROCESSED  # noqa: SLF001
        assert stand_in.commit_calls == 3 and stand_in.commits == []
        assert counter._value.get() == before + 3  # noqa: SLF001
        failed = [entry for entry in logs if entry["event"] == "event.commit_failed"]
        assert len(failed) == 3
        assert {entry["error_type"] for entry in failed} == {"REQUEST_TIMED_OUT"}  # the name only
        assert any(entry["event"] == "event.commit_abandoned" for entry in logs)
        assert all("reason carrying no payload" not in str(entry) for entry in logs)
    finally:
        consumer.close()


def test_a_fatal_commit_error_still_raises() -> None:
    stand_in = StandInKafkaConsumer(always_fail=_kafka_error(fatal=True))
    consumer = _processing_consumer(stand_in)
    try:
        with pytest.raises(KafkaException):
            consumer._handle_message(FakeMessage(offset=3))  # noqa: SLF001
        assert stand_in.commit_calls == 1
    finally:
        consumer.close()


# ------------------------------------------------------ the cap on a live broker (T-3-63)


def test_live_broker_a_capped_message_is_dead_lettered_and_the_next_one_is_processed(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    from tests.events.test_consumer_dlq import _committed, _headers, _read_topic  # noqa: PLC0415

    seen: list[str] = []

    async def handler(envelope: EventEnvelope) -> None:
        seen.append(str(envelope.event_id))
        if len(seen) <= 2 and str(envelope.event_id) == seen[0]:
            raise TransientEventError(SECRET_TEXT)

    stuck = produce_event(unique_topic)
    healthy = produce_event(unique_topic)
    consumer = EventConsumer(
        group_id=unique_group,
        topics=[unique_topic],
        handlers={EVENT_TYPE: handler},
        dlq_topic=dlq_topic,
        poll_timeout=0.5,
        backoff_seconds=0.02,
        max_transient_attempts=2,
        max_transient_seconds=0.0,
    )
    try:
        outcomes = poll_until(consumer, want=3)
    finally:
        consumer.close()
    assert outcomes == [Outcome.RETRY_LATER, Outcome.DEAD_LETTERED, Outcome.PROCESSED]
    assert seen == [stuck["event_id"], stuck["event_id"], healthy["event_id"]]
    records = _read_topic(dlq_topic, 1)
    assert len(records) == 1
    headers = _headers(records[0])
    assert headers["dlq.error_type"] == TRANSIENT_EXHAUSTED.encode()
    assert headers["dlq.attempts"] == b"2"
    assert headers["dlq.consumer_group"] == unique_group.encode()
    assert records[0].key() == stuck["appointment_id"].encode()
    assert SECRET_TEXT not in _header_text(headers)
    assert _committed(unique_group, unique_topic) == 2  # moved past both messages
