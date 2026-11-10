"""One appointment.booked event round-trips through an outbox row (plan 03-01).

Runs against the live scheduling_db with the mock registry. The outbox is a
TEMP table (`outbox_table_ddl(temporary=True)`), so nothing reaches the real
table or the CDC connector. A TEMP table is private to one connection, so the
whole test runs on one `AsyncConnection`; "the helper never commits" is proven
by a rollback removing the row (a committed row would survive it).
"""

from __future__ import annotations

import datetime
import re
import uuid
from collections.abc import AsyncIterator

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from libs.config import shared_settings
from libs.db.postgres import get_engine
from libs.events import EVENT_CATALOG, enqueue_event
from libs.events.catalog import topic_for
from libs.events.outbox import event_id_for, outbox_table_ddl
from libs.events.registry import deserializer, is_warm, reset_registry_for_tests, warm_up
from services.scheduling.config import settings

pytestmark = pytest.mark.integration

EVENT_TYPE = "appointment.booked"
TOPIC = "smarthealth.appointment.events"
TRACEPARENT_RE = r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$"

# Local provider: the test only needs a valid span context to be current; it
# does not depend on whether the process-wide SDK provider has been configured.
_tracer = TracerProvider().get_tracer("tests.events")


def _payload() -> dict[str, object]:
    now = datetime.datetime(2026, 11, 10, 9, 0, tzinfo=datetime.UTC)
    return {
        "appointment_id": str(uuid.uuid4()),
        "clinic_id": str(uuid.uuid4()),
        "patient_id": str(uuid.uuid4()),
        "provider_id": str(uuid.uuid4()),
        "slot_id": str(uuid.uuid4()),
        "slot_start": now + datetime.timedelta(days=1),
        "slot_end": now + datetime.timedelta(days=1, minutes=30),
        "booked_at": now,
    }


@pytest.fixture
async def conn(mock_registry: SchemaRegistryClient) -> AsyncIterator[AsyncConnection]:
    del mock_registry  # fixture ordering only
    async with get_engine(settings.database_url).connect() as connection:
        await connection.execute(text(outbox_table_ddl(temporary=True)))
        await connection.commit()  # the TEMP table survives; the session below starts clean
        yield connection


async def _rows(session: AsyncSession) -> list:
    return list((await session.execute(text("SELECT * FROM outbox ORDER BY occurred_at"))).all())


async def test_row_columns_and_avro_payload_round_trip(conn: AsyncConnection) -> None:
    payload = _payload()
    aggregate_id = payload["appointment_id"]
    async with AsyncSession(bind=conn) as session:
        event_id = await enqueue_event(
            session,
            event_type=EVENT_TYPE,
            aggregate_id=str(aggregate_id),
            payload=payload,
            discriminator="1",
        )
        rows = await _rows(session)
        assert len(rows) == 1
        row = rows[0]
        assert row.id == event_id == event_id_for(aggregate_id, EVENT_TYPE, "1")
        assert row.aggregatetype == "appointment"
        assert row.aggregateid == aggregate_id
        assert row.type == EVENT_TYPE
        assert row.schema_version == 1

        decoded = deserializer()(
            bytes(row.payload), SerializationContext(TOPIC, MessageField.VALUE)
        )
        assert decoded["event_id"] == event_id
        assert decoded["event_type"] == EVENT_TYPE
        assert decoded["schema_version"] == 1
        assert decoded["aggregate_id"] == aggregate_id
        for key, value in payload.items():
            expected = value if isinstance(value, datetime.datetime) else uuid.UUID(str(value))
            assert decoded[key] == expected, key
        await session.rollback()


async def test_traceparent_is_the_active_span(conn: AsyncConnection) -> None:
    payload = _payload()
    async with AsyncSession(bind=conn) as session:
        with _tracer.start_as_current_span("producer") as span:
            await enqueue_event(
                session,
                event_type=EVENT_TYPE,
                aggregate_id=str(payload["appointment_id"]),
                payload=payload,
                discriminator="1",
            )
            expected_trace_id = format(span.get_span_context().trace_id, "032x")
        (row,) = await _rows(session)
        assert row.traceparent is not None
        assert row.traceparent.split("-")[1] == expected_trace_id
        assert re.match(TRACEPARENT_RE, row.traceparent)
        await session.rollback()


async def test_no_active_span_stores_null_traceparent(conn: AsyncConnection) -> None:
    payload = _payload()
    async with AsyncSession(bind=conn) as session:
        await enqueue_event(
            session,
            event_type=EVENT_TYPE,
            aggregate_id=str(payload["appointment_id"]),
            payload=payload,
            discriminator="1",
        )
        (row,) = await _rows(session)
        assert row.traceparent is None
        await session.rollback()


async def test_duplicate_call_writes_one_row_and_returns_same_id(conn: AsyncConnection) -> None:
    payload = _payload()
    async with AsyncSession(bind=conn) as session:
        kwargs = {
            "event_type": EVENT_TYPE,
            "aggregate_id": str(payload["appointment_id"]),
            "payload": payload,
            "discriminator": "1",
        }
        first = await enqueue_event(session, **kwargs)
        second = await enqueue_event(session, **kwargs)
        assert first == second
        assert len(await _rows(session)) == 1
        # A different discriminator (the next version) is a different event.
        third = await enqueue_event(session, **{**kwargs, "discriminator": "2"})
        assert third != first
        assert len(await _rows(session)) == 2
        await session.rollback()


async def test_helper_never_commits(conn: AsyncConnection) -> None:
    payload = _payload()
    async with AsyncSession(bind=conn) as session:
        await enqueue_event(
            session,
            event_type=EVENT_TYPE,
            aggregate_id=str(payload["appointment_id"]),
            payload=payload,
            discriminator="1",
        )
        assert session.in_transaction()
        assert len(await _rows(session)) == 1
        await session.rollback()
        # A committed row would survive the rollback.
        assert await _rows(session) == []

    # And the caller's commit is what makes it durable.
    async with AsyncSession(bind=conn) as session:
        await enqueue_event(
            session,
            event_type=EVENT_TYPE,
            aggregate_id=str(payload["appointment_id"]),
            payload=payload,
            discriminator="1",
        )
        await session.commit()
    async with AsyncSession(bind=conn) as session:
        assert len(await _rows(session)) == 1
        await session.rollback()


async def test_missing_required_field_raises_before_any_insert(conn: AsyncConnection) -> None:
    payload = _payload()
    del payload["slot_id"]
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(Exception, match="slot_id"):
            await enqueue_event(
                session,
                event_type=EVENT_TYPE,
                aggregate_id=str(payload["appointment_id"]),
                payload=payload,
                discriminator="1",
            )
        assert await _rows(session) == []
        await session.rollback()


async def test_payload_may_not_override_envelope_fields(conn: AsyncConnection) -> None:
    payload = {**_payload(), "event_id": str(uuid.uuid4())}
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(ValueError, match="envelope"):
            await enqueue_event(
                session,
                event_type=EVENT_TYPE,
                aggregate_id=str(payload["appointment_id"]),
                payload=payload,
                discriminator="1",
            )
        assert await _rows(session) == []
        await session.rollback()


async def test_strict_serializer_does_not_register_unknown_schemas() -> None:
    """A registry without the subject makes serialization fail, not register it."""
    from libs.config import shared_settings  # noqa: PLC0415
    from libs.events.registry import (  # noqa: PLC0415
        get_registry_client,
        reset_registry_for_tests,
        serializer_for,
    )

    original = shared_settings.schema_registry_url
    shared_settings.schema_registry_url = "mock://p3-empty-registry"
    reset_registry_for_tests()
    try:
        client = get_registry_client()
        serializer = serializer_for(EVENT_TYPE)
        with pytest.raises(Exception):  # noqa: B017,PT011 - registry error type varies by version
            serializer(
                {**_payload(), "event_id": str(uuid.uuid4()), "event_type": EVENT_TYPE,
                 "schema_version": 1, "occurred_at": datetime.datetime.now(datetime.UTC),
                 "aggregate_id": "x"},
                SerializationContext(topic_for(EVENT_TYPE), MessageField.VALUE),
            )
        assert client.get_subjects() == []
    finally:
        shared_settings.schema_registry_url = original
        reset_registry_for_tests()


def test_catalogue_names_only_existing_schema_files(mock_registry: SchemaRegistryClient) -> None:
    from libs.events.registry import load_schema_str  # noqa: PLC0415

    assert EVENT_TYPE in EVENT_CATALOG
    for event_type, spec in EVENT_CATALOG.items():
        assert load_schema_str(event_type)
        assert spec.subject in mock_registry.get_subjects()
        assert topic_for(event_type).startswith("smarthealth.")


async def test_warm_up_resolves_schema_ids_and_marks_types_warm(
    mock_registry: SchemaRegistryClient,
) -> None:
    del mock_registry
    assert not is_warm(EVENT_TYPE)
    await warm_up([EVENT_TYPE], attempts=1, delay_seconds=0)
    assert is_warm(EVENT_TYPE)


async def test_warm_up_raises_after_the_last_attempt_when_unregistered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shared_settings, "schema_registry_url", "mock://p3-warm-empty")
    reset_registry_for_tests()
    try:
        with pytest.raises(Exception):  # noqa: B017,PT011 - registry error type varies by version
            await warm_up([EVENT_TYPE], attempts=2, delay_seconds=0)
        assert not is_warm(EVENT_TYPE)
    finally:
        reset_registry_for_tests()
