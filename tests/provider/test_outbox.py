"""Provider's outbox emission on live Postgres (plan 03-08; EVENT-01, EVENT-02; D-06,
D-07, T-3-25, T-3-26).

Every schedule change that already writes a `schedule_change_events` row (a
schedule-affecting template edit, a template deactivation, a provider deactivation) now
also writes one `provider.schedule_changed` outbox row in the SAME transaction, carrying
the feed row's id as `change_event_id`. A change that alters nothing, a repeat
deactivation and a failure before the commit write neither row. The Phase 2.1 pull feed
itself (`tests/provider/test_schedule_change_feed.py`) is not changed.

These tests COMMIT on purpose (the repository functions commit, and atomicity must be
shown from a second connection), so each test uses its own throwaway provider; its
provider, template and feed rows are removed at the end of the test (test-data hygiene
only; application code never deletes). The outbox is insert-only by design (D-09), so its
rows stay and are relayed by Debezium as ordinary events for unknown providers. Serialization
uses the LIVE Schema Registry (`localhost:8081`, subjects registered by `schema-init`).
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from confluent_kafka.serialization import MessageField, SerializationContext
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from libs.events.catalog import EVENT_CATALOG, topic_for
from libs.events.outbox import event_id_for
from libs.events.registry import deserializer
from services.provider.config import settings
from services.provider.events import PROVIDER_EVENT_TYPES, emit_schedule_changed
from services.provider.repository import provider_repo
from services.provider.repository.models import ScheduleChangeType

pytestmark = pytest.mark.integration

PROVIDER_NAME = "Dr. Zzyzx Outboxprovider Sentinel"


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(settings.database_url, pool_size=5, max_overflow=0)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def provider_id(engine: AsyncEngine) -> AsyncIterator[uuid.UUID]:
    """A committed throwaway provider with one weekly template. Its provider, template
    and feed rows are removed at teardown; its outbox rows stay (insert-only)."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        provider = await provider_repo.create_provider(
            session,
            department_id=uuid.uuid4(),
            full_name=PROVIDER_NAME,
            specialty_names=["General"],
        )
    pid = provider.id
    try:
        yield pid
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM schedule_change_events WHERE provider_id = :p"), {"p": pid}
            )
            await conn.execute(
                text("DELETE FROM availability_templates WHERE provider_id = :p"), {"p": pid}
            )
            await conn.execute(
                text("DELETE FROM provider_specialties WHERE provider_id = :p"), {"p": pid}
            )
            await conn.execute(text("DELETE FROM providers WHERE id = :p"), {"p": pid})


async def _template(engine: AsyncEngine, provider_id: uuid.UUID) -> uuid.UUID:
    """A committed template. Creating one is not a schedule change (no feed row, no event)."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        template = await provider_repo.create_availability_template(
            session,
            provider_id=provider_id,
            department_id=uuid.uuid4(),
            day_of_week=1,
            start_time=datetime.time(9, 0),
            end_time=datetime.time(12, 0),
            slot_duration_minutes=30,
            effective_from=datetime.date(2026, 1, 5),
        )
        return template.id


async def _feed_rows(engine: AsyncEngine, provider_id: uuid.UUID) -> list[Any]:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT id, provider_id, change_type, template_id, created_at, delivered_at "
                "FROM schedule_change_events WHERE provider_id = :p ORDER BY created_at, id"
            ),
            {"p": provider_id},
        )
        return list(result.all())


async def _outbox_rows(engine: AsyncEngine, provider_id: uuid.UUID) -> list[Any]:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT id, aggregatetype, aggregateid, type, payload, schema_version, occurred_at "
                "FROM outbox WHERE aggregateid = :a ORDER BY occurred_at, id"
            ),
            {"a": str(provider_id)},
        )
        return list(result.all())


def _decode(row: Any) -> dict[str, Any]:
    decoded = deserializer()(
        bytes(row.payload), SerializationContext(topic_for(row.type), MessageField.VALUE)
    )
    assert decoded is not None
    return decoded


def _ms(moment: datetime.datetime) -> datetime.datetime:
    """Avro `timestamp-millis` keeps milliseconds; Postgres timestamps have microseconds."""
    return moment.replace(microsecond=moment.microsecond // 1000 * 1000)


async def _update_template(engine: AsyncEngine, provider_id: uuid.UUID, template_id: uuid.UUID, **kw):
    async with AsyncSession(engine, expire_on_commit=False) as session:
        return await provider_repo.update_availability_template(
            session, template_id, provider_id, **kw
        )


def _assert_pairs_one_to_one(feed: list[Any], outbox: list[Any]) -> None:
    """Every feed row has exactly one outbox row whose change_event_id is the feed row's id."""
    assert len(feed) == len(outbox)
    assert {str(f.id) for f in feed} == {str(_decode(o)["change_event_id"]) for o in outbox}


# --- the three schedule-change callers --------------------------------------------------


async def test_template_update_writes_feed_row_and_event_together(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)
    assert await _feed_rows(engine, provider_id) == []
    assert await _outbox_rows(engine, provider_id) == []  # creating a template is not a change

    await _update_template(engine, provider_id, template_id, end_time=datetime.time(13, 0))

    feed = await _feed_rows(engine, provider_id)
    outbox = await _outbox_rows(engine, provider_id)
    assert [f.change_type for f in feed] == [ScheduleChangeType.TEMPLATE_UPDATED]
    assert [o.type for o in outbox] == ["provider.schedule_changed"]
    _assert_pairs_one_to_one(feed, outbox)

    row, change = outbox[0], feed[0]
    assert row.aggregatetype == "provider"
    assert row.aggregateid == str(provider_id)
    assert row.id == event_id_for(provider_id, "provider.schedule_changed", str(change.id))
    assert row.schema_version == 1
    assert row.occurred_at == change.created_at

    value = _decode(row)
    assert str(value["provider_id"]) == str(provider_id)
    assert str(value["aggregate_id"]) == str(provider_id)
    assert str(value["change_event_id"]) == str(change.id)
    assert value["change_type"] == "template_updated"
    assert str(value["template_id"]) == str(template_id)
    assert value["changed_at"] == _ms(change.created_at)
    assert value["event_type"] == "provider.schedule_changed"


async def test_template_deactivation_writes_feed_row_and_event_together(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        result = await provider_repo.deactivate_availability_template(
            session, template_id, provider_id
        )
    assert result is not None and result.is_active is False

    feed = await _feed_rows(engine, provider_id)
    outbox = await _outbox_rows(engine, provider_id)
    assert [f.change_type for f in feed] == [ScheduleChangeType.TEMPLATE_DEACTIVATED]
    _assert_pairs_one_to_one(feed, outbox)
    value = _decode(outbox[0])
    assert value["change_type"] == "template_deactivated"
    assert str(value["template_id"]) == str(template_id)


async def test_provider_deactivation_writes_feed_row_and_event_without_template(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        result = await provider_repo.deactivate_provider(session, provider_id)
    assert result is not None and result.is_active is False

    feed = await _feed_rows(engine, provider_id)
    outbox = await _outbox_rows(engine, provider_id)
    assert [f.change_type for f in feed] == [ScheduleChangeType.PROVIDER_DEACTIVATED]
    _assert_pairs_one_to_one(feed, outbox)
    value = _decode(outbox[0])
    assert value["change_type"] == "provider_deactivated"
    assert value["template_id"] is None


# --- changes that alter nothing, and failures --------------------------------------------


async def test_no_op_changes_write_neither_feed_row_nor_event(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)

    # A PATCH that supplies the stored values changes nothing (M2).
    await _update_template(engine, provider_id, template_id, end_time=datetime.time(12, 0))
    assert await _feed_rows(engine, provider_id) == []
    assert await _outbox_rows(engine, provider_id) == []

    # Deactivating twice: the second call is a no-op success and records nothing more.
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await provider_repo.deactivate_availability_template(session, template_id, provider_id)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await provider_repo.deactivate_availability_template(session, template_id, provider_id)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await provider_repo.deactivate_provider(session, provider_id)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await provider_repo.deactivate_provider(session, provider_id)

    feed = await _feed_rows(engine, provider_id)
    outbox = await _outbox_rows(engine, provider_id)
    assert sorted(f.change_type for f in feed) == sorted(
        [ScheduleChangeType.TEMPLATE_DEACTIVATED, ScheduleChangeType.PROVIDER_DEACTIVATED]
    )
    _assert_pairs_one_to_one(feed, outbox)


async def test_rollback_leaves_neither_feed_row_nor_event(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        change = await provider_repo.record_schedule_change(
            session,
            provider_id=provider_id,
            change_type=ScheduleChangeType.PROVIDER_DEACTIVATED,
        )
        # Inside the open transaction both rows exist ...
        assert change.id is not None
        await session.rollback()
    # ... and a rollback takes both away.
    assert await _feed_rows(engine, provider_id) == []
    assert await _outbox_rows(engine, provider_id) == []


async def test_failure_after_the_event_before_commit_leaves_neither_row(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)

    async def failing_emit(db: AsyncSession, event: Any) -> None:
        await emit_schedule_changed(db, event)
        raise RuntimeError("simulated failure before commit")

    async with AsyncSession(engine, expire_on_commit=False) as session:
        with patch.object(provider_repo, "emit_schedule_changed", failing_emit):
            with pytest.raises(RuntimeError, match="simulated failure"):
                await provider_repo.update_availability_template(
                    session, template_id, provider_id, end_time=datetime.time(13, 0)
                )
        await session.rollback()

    assert await _feed_rows(engine, provider_id) == []
    assert await _outbox_rows(engine, provider_id) == []
    async with AsyncSession(engine, expire_on_commit=False) as session:
        reloaded = await provider_repo.get_availability_template(session, template_id, provider_id)
    assert reloaded is not None and reloaded.end_time == datetime.time(12, 0)


async def test_emitting_the_same_change_twice_still_writes_one_row(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)
    await _update_template(engine, provider_id, template_id, end_time=datetime.time(13, 0))
    async with AsyncSession(engine, expire_on_commit=False) as session:
        change = await session.get(provider_repo.ScheduleChangeEvent, (await _feed_rows(engine, provider_id))[0].id)
        assert change is not None
        again = await emit_schedule_changed(session, change)
        await session.commit()
    assert again == event_id_for(provider_id, "provider.schedule_changed", str(change.id))
    assert len(await _outbox_rows(engine, provider_id)) == 1


# --- the pull feed is unchanged, the payload is PHI-free ----------------------------------


async def test_pull_feed_still_returns_and_acknowledges_the_same_rows(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)
    await _update_template(engine, provider_id, template_id, end_time=datetime.time(13, 0))
    (change,) = await _feed_rows(engine, provider_id)
    assert change.delivered_at is None

    async with AsyncSession(engine, expire_on_commit=False) as session:
        pending = await provider_repo.list_pending_changes(session, limit=1000)
        assert change.id in {event.id for event in pending}
        assert await provider_repo.mark_change_delivered(session, change.id) == "delivered"
        await session.commit()
    async with AsyncSession(engine, expire_on_commit=False) as session:
        assert await provider_repo.mark_change_delivered(session, change.id) == "already"

    (acked,) = await _feed_rows(engine, provider_id)
    assert acked.delivered_at is not None
    # Acknowledging touches the feed only: still exactly one event for the change.
    assert len(await _outbox_rows(engine, provider_id)) == 1


async def test_payload_carries_ids_types_and_timestamps_only(
    engine: AsyncEngine, provider_id: uuid.UUID
) -> None:
    template_id = await _template(engine, provider_id)
    await _update_template(engine, provider_id, template_id, end_time=datetime.time(13, 0))
    (row,) = await _outbox_rows(engine, provider_id)

    assert set(_decode(row)) == {
        "event_id",
        "event_type",
        "schema_version",
        "occurred_at",
        "aggregate_id",
        "provider_id",
        "change_event_id",
        "change_type",
        "template_id",
        "changed_at",
    }
    raw = bytes(row.payload)
    for fragment in (PROVIDER_NAME, "Outboxprovider", "Sentinel", "General"):
        assert fragment.encode() not in raw


def test_provider_event_types_are_the_one_catalogued_provider_event() -> None:
    assert PROVIDER_EVENT_TYPES == ("provider.schedule_changed",)
    assert EVENT_CATALOG["provider.schedule_changed"].aggregate_type == "provider"
