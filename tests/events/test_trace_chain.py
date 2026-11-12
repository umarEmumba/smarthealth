"""One trace across the outbox, Kafka and Celery boundaries, in memory (plan 03-17, Task 1).

The chain the stack runs for a booking (D-11, research Pattern 8):

    producer span -> `enqueue_event` (outbox row stores the traceparent)
        -> Debezium turns the row into a Kafka record whose `traceparent` is a HEADER
        -> `EventConsumer` processing span (CHILD of the producer span)
        -> Celery publish span (`apply_async/notifications.send`, child of the consumer span)
        -> Celery task span (`run/notifications.send`, child of the publish span)

The test reproduces the Debezium step by reading the row it just wrote on a TEMP outbox
(scheduling_db, nothing reaches the real table or connector) and producing the row's own
payload and columns as a record with the same headers to a unique live topic. The Celery
leg is the real thing: the notification app on a throwaway RabbitMQ vhost with an in-process
threads worker (the fixtures of `tests/notification/conftest.py`).
"""

from __future__ import annotations

import datetime
import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanContext, SpanKind
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from libs.events import enqueue_event
from libs.events.consumer import EventConsumer, EventEnvelope, Outcome
from libs.events.outbox import outbox_table_ddl
from services.notification.celery_app import instrument_publisher
from services.notification.tasks import enqueue_send
from services.scheduling.config import settings
from tests.events.conftest import booked_payload, poll_until
from tests.notification.conftest import (  # noqa: F401  (fixtures reused by name)
    app,
    broker,
    delivery,
    make_worker,
    mongo_db,
    wait_for,
)

pytestmark = pytest.mark.integration

_producer_tracer = TracerProvider().get_tracer("tests.events.trace_chain")


async def _write_outbox_row(payload: dict[str, Any]) -> tuple[Any, SpanContext]:
    """Enqueue one booked event inside a producer span on a TEMP outbox; return the row."""
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(text(outbox_table_ddl(temporary=True)))
            await connection.commit()
            with _producer_tracer.start_as_current_span("producer") as producer:
                async with AsyncSession(bind=connection) as session:
                    await enqueue_event(
                        session,
                        event_type="appointment.booked",
                        aggregate_id=payload["appointment_id"],
                        payload=payload,
                        discriminator="1",
                    )
                    row = (await session.execute(text("SELECT * FROM outbox"))).one()
                    await session.rollback()
                return row, producer.get_span_context()
    finally:
        await engine.dispose()


def _by_name(finished: list[ReadableSpan], name: str) -> ReadableSpan:
    matches = [s for s in finished if s.name == name]
    assert matches, f"no span named {name!r}; got {sorted({s.name for s in finished})}"
    return matches[-1]


def test_producer_outbox_consumer_celery_form_one_parent_chain(
    mock_registry: SchemaRegistryClient,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    in_memory_spans: InMemorySpanExporter,
    app: Any,  # noqa: F811
    make_worker: Callable[..., Any],  # noqa: F811
    mongo_db: Any,  # noqa: F811
) -> None:
    instrument_publisher()  # the consumer process does this at start-up (D-11)
    make_worker(app)

    del mock_registry  # fixture ordering only
    payload = booked_payload()
    appointment_id = payload["appointment_id"]
    row, producer_ctx = asyncio.run(_write_outbox_row(payload))
    assert row.traceparent and row.traceparent.split("-")[1] == format(producer_ctx.trace_id, "032x")

    # What Debezium does: the outbox payload becomes the record value, `aggregateid` the key and
    # `type` / `traceparent` / `schema_version` become headers.
    produce_event(
        unique_topic,
        raw_value=bytes(row.payload),
        key=row.aggregateid,
        headers=[
            ("eventType", row.type.encode()),
            ("traceparent", row.traceparent.encode()),
            ("schemaVersion", str(row.schema_version).encode()),
        ],
    )

    dedupe_key = f"appointment.booked:{appointment_id}:{row.id}"

    async def handler(envelope: EventEnvelope) -> None:
        enqueue_send(
            app,
            notification_type="appointment.booked",
            dedupe_key=dedupe_key,
            patient_id=str(envelope.payload["patient_id"]),
            appointment_id=str(envelope.payload["appointment_id"]),
            event_id=str(envelope.event_id),
            requested_at=datetime.datetime.now(datetime.UTC).isoformat(),
        )

    consumer = EventConsumer(
        group_id=unique_group,
        topics=[unique_topic],
        handlers={"appointment.booked": handler},
        dlq_topic=dlq_topic,
        poll_timeout=0.5,
        backoff_seconds=0.05,
    )
    try:
        assert poll_until(consumer) == [Outcome.PROCESSED]
    finally:
        consumer.close()

    sent = wait_for(lambda: (d := delivery(mongo_db, dedupe_key)) and d["status"] == "sent" and d)
    assert sent, "the Celery task never recorded the delivery as sent"
    run = wait_for(
        lambda: [s for s in in_memory_spans.get_finished_spans() if s.name == "run/notifications.send"]
    )
    assert run, [s.name for s in in_memory_spans.get_finished_spans()]

    finished = list(in_memory_spans.get_finished_spans())
    consumer_span = next(s for s in finished if s.kind == SpanKind.CONSUMER)
    publish = _by_name(finished, "apply_async/notifications.send")
    task = _by_name(finished, "run/notifications.send")

    trace_id = producer_ctx.trace_id
    assert consumer_span.context.trace_id == publish.context.trace_id == task.context.trace_id == trace_id

    # CHILD_OF at every hop (a link alone would leave parent None or a foreign parent).
    assert consumer_span.parent is not None
    assert consumer_span.parent.span_id == producer_ctx.span_id
    assert not consumer_span.links, "the consumer span must be a child, not only linked"
    assert publish.parent is not None
    assert publish.parent.span_id == consumer_span.context.span_id
    assert task.parent is not None
    assert task.parent.span_id == publish.context.span_id
    assert consumer_span.attributes is not None
    assert consumer_span.attributes["smarthealth.event_id"] == str(row.id)
