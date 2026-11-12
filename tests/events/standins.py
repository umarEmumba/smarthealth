"""Small stand-ins for the Kafka side of `EventConsumer` (plan 03-21).

They let a test drive the REAL `_handle_message` / `_decode` / `_commit` code of the
consumer base without a broker: a fake message object, and a recording replacement for
the confluent `Consumer` (seek, commit, close, assignment). A consumer built by a
service's `build_consumer(...)` still creates its real confluent `Consumer` (which does
not connect until it is polled); `install` closes that one and swaps the stand-in in.
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from libs.events.consumer import EventConsumer


class FakeMessage:
    """The subset of `confluent_kafka.Message` the consumer base reads."""

    def __init__(
        self,
        *,
        topic: str = "smarthealth.test.events",
        partition: int = 0,
        offset: int = 7,
        key: bytes | None = b"key-1",
        value: bytes | None = b"opaque-avro-bytes",
        headers: Sequence[tuple[str, bytes | None]] | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._headers = list(headers or [])

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes | None]]:
        return list(self._headers)

    def error(self) -> None:
        return None


class StandInKafkaConsumer:
    """Records seeks and commits. `commit_script` is consumed one entry per commit call:
    an exception is raised, None succeeds; once it is empty every commit succeeds,
    unless `always_fail` is an exception, which is then raised for every call."""

    def __init__(
        self,
        commit_script: Sequence[BaseException | None] | None = None,
        *,
        always_fail: BaseException | None = None,
    ) -> None:
        self._script = list(commit_script or [])
        self._always_fail = always_fail
        self.seeks: list[tuple[str, int, int]] = []
        self.commits: list[int] = []
        self.commit_calls = 0
        self.closed = False

    def seek(self, partition: Any) -> None:
        self.seeks.append((partition.topic, partition.partition, partition.offset))

    def commit(self, message: Any = None, asynchronous: bool = False) -> None:
        del asynchronous
        self.commit_calls += 1
        if self._always_fail is not None:
            raise self._always_fail
        if self._script:
            outcome = self._script.pop(0)
            if outcome is not None:
                raise outcome
        self.commits.append(message.offset())

    def assignment(self) -> list[Any]:
        return []

    def close(self) -> None:
        self.closed = True


def install(consumer: EventConsumer, stand_in: StandInKafkaConsumer) -> None:
    """Replace the consumer's real confluent `Consumer` with `stand_in`."""
    consumer._consumer.close()  # noqa: SLF001
    consumer._consumer = stand_in  # type: ignore[assignment]  # noqa: SLF001


def stub_decoder(record: dict[str, Any]) -> Callable[[Any, Any], dict[str, Any]]:
    """A `_deserializer` replacement that returns `record` whatever the bytes are."""

    def decode(_value: Any, _context: Any) -> dict[str, Any]:
        return dict(record)

    return decode


def event_record(event_type: str, **payload: Any) -> dict[str, Any]:
    """A decoded event record (envelope fields plus `payload`)."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "aggregate_id": str(uuid.uuid4()),
        "occurred_at": datetime.datetime.now(datetime.UTC),
        "schema_version": 1,
        **payload,
    }
