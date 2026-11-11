"""Consumer base reliability: registry back-pressure, the transient cap, commit failures
(plan 03-21; WR-02, WR-03, WR-07, CR-01, EVENT-05, T-3-63..T-3-65).

Most tests drive the REAL `_handle_message` / `_decode` / `_commit` code with the stand-ins
of `tests/events/standins.py` (no broker, no clock waiting); the cap test also runs on the
live broker with a unique topic, DLQ topic and group, like `test_consumer_dlq.py`.
"""

from __future__ import annotations

from typing import Any

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError

from libs.events.consumer import DESERIALIZATION_ERROR, EventConsumer, Outcome
from tests.events.standins import FakeMessage, StandInKafkaConsumer, install

GROUP = "p3test-reliability-group"
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
    consumer = EventConsumer(
        group_id=GROUP,
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
