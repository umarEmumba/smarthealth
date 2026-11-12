"""Fixtures for the analytics projection tests (plan 03-10).

The live Kafka fixtures (`unique_topic`, `dlq_topic`, `unique_group`, `produce_event`,
`mock_registry`) come from `tests/events/conftest.py`; they are imported here so the
analytics tests see them without a `pytest_plugins` entry. Every test asserts on a
UNIQUE clinic id (research Pitfall 12), so rows other tests or the live stack leave in
analytics_db can never change a number, and nothing a test writes needs deleting for
another test to be correct (each test still removes its own rows at teardown).
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from services.analytics.config import settings
from services.analytics.consumer import build_consumer
from tests.events.conftest import (  # noqa: F401 - re-exported fixtures
    dlq_topic,
    kafka_admin,
    mock_registry,
    produce_event,
    topic_factory,
    unique_group,
    unique_topic,
)


async def _run_sql(sql: str, params: dict[str, Any], *, write: bool = False) -> Any:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params)
            return None if write else result.all()
    finally:
        await engine.dispose()


def query(sql: str, **params: Any) -> list[Any]:
    """Run a read-only statement on analytics_db from a SYNC test (own loop and engine)."""
    return asyncio.run(_run_sql(sql, params))


@pytest.fixture
def clinic_id() -> Iterator[uuid.UUID]:
    """A unique clinic id; the projection rows written for it are removed afterwards
    (test-data hygiene only: no other test's numbers depend on them)."""
    clinic = uuid.uuid4()
    yield clinic
    for table in ("patients", "appointments", "visits_completed"):
        asyncio.run(_run_sql(f"DELETE FROM {table} WHERE clinic_id = :c", {"c": clinic}, write=True))


@pytest.fixture
def claimed_event_ids() -> Iterator[list[str]]:
    """Event ids a Kafka-driven test produced; their `processed_events` claims are
    removed afterwards (test-data hygiene)."""
    ids: list[str] = []
    yield ids
    if ids:
        asyncio.run(
            _run_sql(
                "DELETE FROM processed_events WHERE event_id = ANY(CAST(:ids AS uuid[]))",
                {"ids": ids},
                write=True,
            )
        )


@contextmanager
def running_consumer(topic: str, group: str, dlq: str) -> Iterator[threading.Thread]:
    """Run the REAL analytics consumer (real `handle_event`) on one test topic in a thread."""
    stop = threading.Event()
    errors: list[BaseException] = []

    def target() -> None:
        try:
            consumer = build_consumer(
                group_id=group,
                topics=[topic],
                dlq_topic=dlq,
                poll_timeout=0.5,
                backoff_seconds=0.05,
                heartbeat_path=None,
                metrics_port=None,
            )
            consumer.run(stop)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the test
            errors.append(exc)

    thread = threading.Thread(target=target, name="analytics-test-consumer", daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        stop.set()
        thread.join(timeout=20)
    assert not thread.is_alive(), "analytics consumer did not stop"
    assert not errors, errors


def wait_for(read: Callable[[], Any], expected: Any, *, timeout: float = 30.0) -> Any:
    """Poll `read()` until it equals `expected` (or the timeout); returns the last value."""
    import time

    deadline = time.monotonic() + timeout
    value = read()
    while value != expected and time.monotonic() < deadline:
        time.sleep(0.3)
        value = read()
    return value


# --------------------------------------------------------------------------
# Envelope factory and projector harness for the metric-math tests (Task 2).
# --------------------------------------------------------------------------

import datetime  # noqa: E402
import json  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402

from libs.events.catalog import EVENT_CATALOG  # noqa: E402
from libs.events.consumer import EventEnvelope  # noqa: E402
from libs.events.registry import load_schema_str  # noqa: E402
from services.analytics.consumer import handle_event  # noqa: E402

T0 = datetime.datetime(2026, 11, 10, 9, 0, tzinfo=datetime.UTC)
ENVELOPE_FIELDS = {"event_id", "event_type", "schema_version", "occurred_at", "aggregate_id"}
_AGGREGATE_FIELD = {
    "appointment": "appointment_id",
    "visit": "visit_id",
    "patient": "patient_id",
}


def _avro_fields(event_type: str) -> list[dict[str, Any]]:
    return json.loads(load_schema_str(event_type))["fields"]


@dataclass
class Projector:
    """Builds schema-checked envelopes for one clinic and applies them through the real
    `handle_event` (claim + upsert in one transaction) on the live analytics_db."""

    clinic_id: uuid.UUID
    event_ids: list[uuid.UUID] = field(default_factory=list)
    _offset: int = 0

    def make(
        self,
        event_type: str,
        aggregate_id: uuid.UUID,
        *,
        occurred_at: datetime.datetime = T0,
        event_id: uuid.UUID | None = None,
        **fields: Any,
    ) -> EventEnvelope:
        """An envelope shaped like a decoded record: UUID and datetime values, every
        Avro field of the event's schema present (a field the schema lacks is an error)."""
        spec = EVENT_CATALOG[event_type]
        event_id = event_id or uuid.uuid4()
        payload: dict[str, Any] = {
            "event_id": event_id,
            "event_type": event_type,
            "schema_version": 1,
            "occurred_at": occurred_at,
            "aggregate_id": str(aggregate_id),
        }
        values = {"clinic_id": self.clinic_id, **fields}
        values[_AGGREGATE_FIELD[spec.aggregate_type]] = aggregate_id
        schema_fields = {f["name"]: f for f in _avro_fields(event_type)}
        unknown = set(values) - set(schema_fields)
        assert not unknown, f"{event_type} has no field(s) {sorted(unknown)}"
        for name, avro in schema_fields.items():
            if name in ENVELOPE_FIELDS:
                continue
            if name in values:
                payload[name] = values[name]
            elif isinstance(avro["type"], list):
                payload[name] = None
            elif isinstance(avro["type"], dict) and avro["type"].get("logicalType") == "uuid":
                payload[name] = uuid.uuid4()
            else:
                payload[name] = occurred_at
        self._offset += 1
        return EventEnvelope(
            event_id=event_id,
            event_type=event_type,
            aggregate_id=str(aggregate_id),
            occurred_at=occurred_at,
            schema_version=1,
            payload=payload,
            topic="p3test.analytics",
            partition=0,
            offset=self._offset,
        )

    async def apply(self, *envelopes: EventEnvelope) -> None:
        for envelope in envelopes:
            self.event_ids.append(envelope.event_id)
            await handle_event(envelope)


async def read(sql: str, **params: Any) -> list[Any]:
    """Run a read-only statement on analytics_db from an async test."""
    return await _run_sql(sql, params)


@pytest.fixture
async def projector(clinic_id: uuid.UUID) -> Any:
    harness = Projector(clinic_id)
    yield harness
    if harness.event_ids:
        await _run_sql(
            "DELETE FROM processed_events WHERE event_id = ANY(:ids)",
            {"ids": harness.event_ids},
            write=True,
        )
