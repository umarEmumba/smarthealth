"""Same-transaction idempotency for database effects (plan 03-04, Task 3).

The live scheduling_db is a scratch database. The single-connection tests use
TEMP tables. Two-session behaviour (concurrency, a real consumer on its own
loop) cannot use TEMP tables, which are private to one connection, so those
tests use a throw-away schema `p3test_<hex>` selected with `SET LOCAL
search_path`, dropped afterwards; no real table is touched.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from libs.db.postgres import get_engine, get_session_factory
from libs.events.consumer import EventConsumer, EventEnvelope, Outcome
from libs.events.idempotency import (
    claim_event,
    create_processed_events_table,
    processed_events_table_ddl,
)
from services.scheduling.config import settings
from tests.events.conftest import KAFKA_BOOTSTRAP, poll_until

pytestmark = pytest.mark.integration

_EFFECT_DDL = "CREATE TABLE effects (event_id uuid NOT NULL)"


@pytest.fixture
async def schema() -> AsyncIterator[str]:
    name = f"p3test_{uuid.uuid4().hex}"
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"CREATE SCHEMA {name}"))
            await conn.execute(text(f"SET LOCAL search_path TO {name}"))
            await conn.execute(text(processed_events_table_ddl()))
            await conn.execute(text(_EFFECT_DDL))
        yield name
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {name} CASCADE"))
        await engine.dispose()


async def _scoped(db: AsyncSession, schema: str) -> AsyncSession:
    await db.execute(text(f"SET LOCAL search_path TO {schema}"))
    return db


async def _apply(db: AsyncSession, schema: str, event_id: uuid.UUID, consumer: str = "c") -> bool:
    """The handler shape every database-effect consumer uses: claim + effect in one tx."""
    await _scoped(db, schema)
    if not await claim_event(db, event_id=event_id, consumer=consumer):
        return False
    await db.execute(text("INSERT INTO effects (event_id) VALUES (:e)"), {"e": event_id})
    return True


async def _effects(schema: str, event_id: uuid.UUID) -> int:
    async with get_engine(settings.database_url).connect() as conn:
        await conn.execute(text(f"SET LOCAL search_path TO {schema}"))
        return int(
            (
                await conn.execute(
                    text("SELECT count(*) FROM effects WHERE event_id = :e"), {"e": event_id}
                )
            ).scalar_one()
        )


async def test_claim_is_true_once_per_consumer_across_transactions(schema: str) -> None:
    event_id = uuid.uuid4()
    factory = get_session_factory(settings.database_url)
    async with factory() as db:
        await _scoped(db, schema)
        assert await claim_event(db, event_id=event_id, consumer="c") is True
        await db.commit()
    async with factory() as db:
        await _scoped(db, schema)
        assert await claim_event(db, event_id=event_id, consumer="c") is False
        assert await claim_event(db, event_id=event_id, consumer="other") is True
        assert await claim_event(db, event_id=str(event_id), consumer="other") is False
        await db.commit()


async def test_temporary_ddl_and_alembic_helper_build_the_same_table() -> None:
    ddl = processed_events_table_ddl(temporary=True)
    assert "CREATE TEMP TABLE processed_events" in ddl
    assert "PRIMARY KEY (event_id, consumer)" in ddl
    emitted: list[str] = []

    class _Op:
        def execute(self, sql: str) -> None:
            emitted.append(sql)

    create_processed_events_table(_Op())
    assert emitted == [processed_events_table_ddl()]
    async with get_engine(settings.database_url).connect() as conn:
        await conn.execute(text(ddl))
        async with AsyncSession(bind=conn) as db:
            assert await claim_event(db, event_id=uuid.uuid4(), consumer="c") is True


async def test_rolled_back_effect_rolls_back_the_claim(schema: str) -> None:
    event_id = uuid.uuid4()
    factory = get_session_factory(settings.database_url)
    async with factory() as db:
        assert await _apply(db, schema, event_id) is True
        await db.rollback()
    assert await _effects(schema, event_id) == 0
    async with factory() as db:
        assert await _apply(db, schema, event_id) is True
        await db.commit()
    assert await _effects(schema, event_id) == 1


async def test_concurrent_handlers_for_one_event_apply_it_once(schema: str) -> None:
    event_id = uuid.uuid4()
    factory = get_session_factory(settings.database_url)

    async def handler() -> bool:
        async with factory() as db:
            applied = await _apply(db, schema, event_id)
            await asyncio.sleep(0.2)  # hold the transaction open so the race is real
            await db.commit()
            return applied

    results = await asyncio.gather(handler(), handler())
    assert sorted(results) == [False, True]
    assert await _effects(schema, event_id) == 1


async def test_concurrent_loser_applies_when_the_winner_rolls_back(schema: str) -> None:
    event_id = uuid.uuid4()
    factory = get_session_factory(settings.database_url)
    winner = factory()
    loser = factory()
    try:
        assert await _apply(winner, schema, event_id) is True
        task = asyncio.create_task(_apply(loser, schema, event_id))
        await asyncio.sleep(0.3)
        assert not task.done()  # blocked on the winner's uncommitted claim
        await winner.rollback()
        assert await task is True
        await loser.commit()
    finally:
        await winner.close()
        await loser.close()
    assert await _effects(schema, event_id) == 1


def _consumer(group: str, topic: str, dlq: str, handler: Any) -> EventConsumer:
    return EventConsumer(
        group_id=group,
        topics=[topic],
        handlers={"appointment.booked": handler},
        dlq_topic=dlq,
        poll_timeout=0.5,
        backoff_seconds=0.02,
    )


def _rewind_group(group: str, topic: str) -> None:
    """Move the (empty) group back to offset 0, as after a crash before the commit."""
    probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group})
    try:
        probe.commit(offsets=[TopicPartition(topic, 0, 0)], asynchronous=False)
    finally:
        probe.close()


def test_redelivered_event_through_the_real_consumer_leaves_one_effect_row(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    schema_name = f"p3test_{uuid.uuid4().hex}"
    calls: list[bool] = []
    admin = _sync_admin_run

    async def handler(envelope: EventEnvelope) -> None:
        # Runs on the consumer's persistent loop, so the cached engine is loop-safe.
        async with get_session_factory(settings.database_url)() as db:
            calls.append(await _apply(db, schema_name, envelope.event_id, "p3-consumer"))
            await db.commit()

    admin(f"CREATE SCHEMA {schema_name}")
    admin(processed_events_table_ddl(), schema_name)
    admin(_EFFECT_DDL, schema_name)
    try:
        record = produce_event(unique_topic)
        event_id = uuid.UUID(record["event_id"])

        first = _consumer(unique_group, unique_topic, dlq_topic, handler)
        try:
            assert poll_until(first) == [Outcome.PROCESSED]
        finally:
            first.close()

        _rewind_group(unique_group, unique_topic)
        second = _consumer(unique_group, unique_topic, dlq_topic, handler)
        try:
            assert poll_until(second) == [Outcome.PROCESSED]  # redelivered and handled
        finally:
            second.close()

        assert calls == [True, False]  # the second delivery claimed nothing
        assert admin(f"SELECT count(*) FROM {schema_name}.effects WHERE event_id = '{event_id}'") == 1
    finally:
        admin(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def test_effect_failing_after_the_claim_is_applied_on_the_retry(
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    schema_name = f"p3test_{uuid.uuid4().hex}"
    attempts: list[int] = []
    admin = _sync_admin_run

    async def handler(envelope: EventEnvelope) -> None:
        async with get_session_factory(settings.database_url)() as db:
            await _apply(db, schema_name, envelope.event_id, "p3-consumer")
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("effect failed after the claim")  # tx rolls back on close
            await db.commit()

    admin(f"CREATE SCHEMA {schema_name}")
    admin(processed_events_table_ddl(), schema_name)
    admin(_EFFECT_DDL, schema_name)
    try:
        record = produce_event(unique_topic)
        consumer = _consumer(unique_group, unique_topic, dlq_topic, handler)
        try:
            assert poll_until(consumer) == [Outcome.PROCESSED]
        finally:
            consumer.close()
        assert len(attempts) == 2
        assert admin(f"SELECT count(*) FROM {schema_name}.effects") == 1
        assert admin(f"SELECT count(*) FROM {schema_name}.processed_events") == 1
        del record
    finally:
        admin(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def _sync_admin_run(sql: str, schema: str | None = None) -> Any:
    """Run one statement on its own short-lived loop and engine (test scaffolding only)."""

    async def run() -> Any:
        engine = create_async_engine(settings.database_url)
        try:
            async with engine.begin() as conn:
                if schema:
                    await conn.execute(text(f"SET LOCAL search_path TO {schema}"))
                result = await conn.execute(text(sql))
                return result.scalar_one() if result.returns_rows else None
        finally:
            await engine.dispose()

    return asyncio.run(run())
