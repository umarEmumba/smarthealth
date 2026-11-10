"""Transactional outbox write helper (plan 03-01, D-06, EVENT-02).

`enqueue_event` is the only thing a service calls to publish an event. It
serializes the payload to registry-framed Avro and inserts ONE row into the
service's own `outbox` table through the CALLER'S `AsyncSession`, so the row
commits or rolls back with the state change (no dual write). It never commits
and never flushes a transaction boundary: the repository contract
("every function takes the caller's AsyncSession and never commits") holds.
Debezium reads the WAL and relays rows to Kafka; no service-side relay exists.

Event ids are deterministic (uuid5 of `aggregate_id:event_type:discriminator`):
the primary key then makes a duplicate emission impossible at the data layer,
and the id is the idempotency key every consumer dedupes on. Payloads carry
ids, types and timestamps only (PHI-free, T-3-01); nothing here logs a payload.

Costly to change once consumers exist: the column set and the id scheme are
read by Debezium and by every consumer (D-06).
"""

from __future__ import annotations

import asyncio
import datetime
import time
import uuid
from typing import Any

from confluent_kafka.serialization import MessageField, SerializationContext
from opentelemetry import propagate
from prometheus_client import Histogram
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.catalog import EVENT_CATALOG, topic_for
from libs.events.registry import is_warm, mark_warm, serializer_for

# Fixed namespace for event ids. Never change: every existing event id and every
# consumer's idempotency record is derived from it.
EVENT_ID_NAMESPACE = uuid.UUID("3f0c6c0e-7d0b-5a62-9b0e-5b6d4a1d9a10")

SCHEMA_VERSION = 1

_ENVELOPE_FIELDS = ("event_id", "event_type", "schema_version", "occurred_at", "aggregate_id")

outbox_insert_seconds = Histogram(
    "outbox_insert_seconds",
    "Time to serialize an event and insert its outbox row (NFR write-path cost, D-04).",
    ["event_type"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

_INSERT_SQL = text(
    """
    INSERT INTO outbox
        (id, aggregatetype, aggregateid, type, payload, traceparent, schema_version, occurred_at)
    VALUES
        (:id, :aggregatetype, :aggregateid, :type, :payload, :traceparent, :schema_version,
         :occurred_at)
    ON CONFLICT (id) DO NOTHING
    """
)


def event_id_for(aggregate_id: str | uuid.UUID, event_type: str, discriminator: str) -> uuid.UUID:
    """Deterministic event id: same aggregate, type and discriminator, same id."""
    return uuid.uuid5(EVENT_ID_NAMESPACE, f"{aggregate_id}:{event_type}:{discriminator}")


def current_traceparent() -> str | None:
    """W3C traceparent of the active span, or None when there is no valid span."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def outbox_table_ddl(temporary: bool = False) -> str:
    """DDL for the per-service outbox table (a TEMP table in tests, so nothing
    reaches the real table or CDC)."""
    create = "CREATE TEMP TABLE" if temporary else "CREATE TABLE"
    return f"""
    {create} outbox (
        id             uuid PRIMARY KEY,
        aggregatetype  text NOT NULL,
        aggregateid    text NOT NULL,
        type           text NOT NULL,
        payload        bytea NOT NULL,
        traceparent    text,
        schema_version integer NOT NULL DEFAULT 1,
        occurred_at    timestamptz NOT NULL DEFAULT now()
    )
    """


def _serialize(event_type: str, record: dict[str, Any]) -> bytes:
    serialized = serializer_for(event_type)(
        record, SerializationContext(topic_for(event_type), MessageField.VALUE)
    )
    if serialized is None:  # AvroSerializer returns None only for a None record
        raise ValueError(f"serializer returned no bytes for {event_type}")
    mark_warm(event_type)
    return serialized


async def enqueue_event(
    db: AsyncSession,
    *,
    event_type: str,
    aggregate_id: str | uuid.UUID,
    payload: dict[str, Any],
    discriminator: str,
    occurred_at: datetime.datetime | None = None,
) -> uuid.UUID:
    """Write one outbox row in the caller's transaction; return the event id.

    Serialization happens BEFORE the INSERT, so a payload that violates the
    schema raises with nothing written. A second call with the same aggregate,
    type and discriminator writes nothing and returns the same id. The caller
    owns commit and rollback.
    """
    started = time.perf_counter()
    spec = EVENT_CATALOG[event_type]
    overlap = sorted(set(payload) & set(_ENVELOPE_FIELDS))
    if overlap:
        raise ValueError(f"payload must not set envelope fields: {overlap}")

    event_id = event_id_for(aggregate_id, event_type, discriminator)
    when = occurred_at or datetime.datetime.now(datetime.UTC)
    record: dict[str, Any] = {
        "event_id": str(event_id),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": when,
        "aggregate_id": str(aggregate_id),
        **payload,
    }

    if is_warm(event_type):
        body = _serialize(event_type, record)  # cached schema id: no network call
    else:
        # First use in this process: the serializer may call the registry, which
        # blocks, so keep it off the event loop (T-3-04).
        body = await asyncio.to_thread(_serialize, event_type, record)

    await db.execute(
        _INSERT_SQL,
        {
            "id": event_id,
            "aggregatetype": spec.aggregate_type,
            "aggregateid": str(aggregate_id),
            "type": event_type,
            "payload": body,
            "traceparent": current_traceparent(),
            "schema_version": SCHEMA_VERSION,
            "occurred_at": when,
        },
    )
    outbox_insert_seconds.labels(event_type=event_type).observe(time.perf_counter() - started)
    return event_id
