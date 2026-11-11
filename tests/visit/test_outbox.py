"""Visit's outbox emission on live Postgres (plan 03-09; EVENT-01, EVENT-02, ANLY-03,
ANLY-05; D-03, D-06, D-07, T-3-27, T-3-29).

`transition_visit(..., target=COMPLETED)` writes exactly one `visit.completed` outbox row
in the SAME transaction as the status change and its history row. A repeat completion,
a skipped state, every other target and a rollback write none. The payload carries the
visit's ids and its three timestamps (check-in, start, completion) and nothing else.

These tests COMMIT on purpose (atomicity is shown from a second connection), so every
visit is created through the harness (cleaned up with its world); the outbox rows of the
visits a test created are deleted at teardown (hygiene only: application code never
deletes). Serialization uses the LIVE Schema Registry (`localhost:8081`).
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from confluent_kafka.serialization import MessageField, SerializationContext
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from libs.events.catalog import EVENT_CATALOG, topic_for
from libs.events.outbox import event_id_for
from libs.events.registry import deserializer
from services.visit.config import settings
from services.visit.domain.states import TransitionOutcome, VisitStatus
from services.visit.events import VISIT_EVENT_TYPES
from services.visit.repository import visit_repo
from tests.scheduling.seed import auth
from tests.visit.conftest import VisitWorld, checked_in_visit, history_rows, outbox_rows

pytestmark = pytest.mark.integration

PAYLOAD_FIELDS = {
    "event_id",
    "event_type",
    "schema_version",
    "occurred_at",
    "aggregate_id",
    "visit_id",
    "appointment_id",
    "clinic_id",
    "patient_id",
    "provider_id",
    "checked_in_at",
    "started_at",
    "completed_at",
}


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(settings.database_url, pool_size=5, max_overflow=0)
    try:
        yield eng
    finally:
        await eng.dispose()


async def _move(engine: AsyncEngine, visit_id: str, target: str, *, commit: bool = True) -> str:
    """One `transition_visit` in its own transaction; commit (default) or roll back."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        outcome = await visit_repo.transition_visit(
            session, visit_id=uuid.UUID(visit_id), target=target, actor="test"
        )
        if commit:
            await session.commit()
        else:
            await session.rollback()
        return outcome


async def _visit_row(engine: AsyncEngine, visit_id: str) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, checked_in_at, started_at, completed_at, appointment_id, "
                    "clinic_id, patient_id, provider_id FROM visits WHERE id = :v"
                ),
                {"v": visit_id},
            )
        ).one()


def _decode(row: Any) -> dict[str, Any]:
    return deserializer()(
        bytes(row.payload), SerializationContext(topic_for(row.type), MessageField.VALUE)
    )


def _ms(value: datetime.datetime) -> datetime.datetime:
    """The Avro `timestamp-millis` truncation of a database timestamp."""
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


async def test_completion_writes_exactly_one_outbox_row_with_the_transition(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    assert await _move(engine, visit_id, VisitStatus.IN_PROGRESS) == TransitionOutcome.APPLIED
    assert await outbox_rows(visit_id) == []

    assert await _move(engine, visit_id, VisitStatus.COMPLETED) == TransitionOutcome.APPLIED

    # Seen from a second connection: status, history row and the event are all there.
    assert (await _visit_row(engine, visit_id)).status == VisitStatus.COMPLETED
    assert [to for _, to in await history_rows(visit_id)] == [
        "checked_in",
        "in_progress",
        "completed",
    ]
    rows = await outbox_rows(visit_id)
    assert len(rows) == 1
    row = rows[0]
    assert row.type == "visit.completed"
    assert row.aggregatetype == EVENT_CATALOG["visit.completed"].aggregate_type == "visit"
    assert row.aggregateid == visit_id
    assert row.id == event_id_for(visit_id, "visit.completed", "completed")
    assert row.schema_version == 1
    assert "visit.completed" in VISIT_EVENT_TYPES


async def test_payload_has_the_ids_and_the_three_timestamps_and_nothing_else(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    await _move(engine, visit_id, VisitStatus.IN_PROGRESS)
    await _move(engine, visit_id, VisitStatus.COMPLETED)

    stored = await _visit_row(engine, visit_id)
    row = (await outbox_rows(visit_id))[0]
    value = _decode(row)

    assert set(value) == PAYLOAD_FIELDS  # no notes, no free text, no PHI field exists
    assert str(value["visit_id"]) == str(value["aggregate_id"]) == visit_id
    assert str(value["appointment_id"]) == str(stored.appointment_id)
    assert str(value["clinic_id"]) == str(stored.clinic_id) == str(visit_world.clinic_id)
    assert str(value["patient_id"]) == str(stored.patient_id)
    assert str(value["provider_id"]) == str(stored.provider_id)
    assert value["checked_in_at"] == _ms(stored.checked_in_at)
    assert value["started_at"] == _ms(stored.started_at)
    assert value["completed_at"] == _ms(stored.completed_at)
    assert value["checked_in_at"] <= value["started_at"] <= value["completed_at"]
    assert value["event_type"] == "visit.completed"
    # The outbox keeps the full-precision completion time; the Avro field is millis.
    assert row.occurred_at == stored.completed_at


async def test_repeat_completion_writes_no_second_event(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    await _move(engine, visit_id, VisitStatus.IN_PROGRESS)
    await _move(engine, visit_id, VisitStatus.COMPLETED)

    assert await _move(engine, visit_id, VisitStatus.COMPLETED) == TransitionOutcome.NOOP

    assert len(await outbox_rows(visit_id)) == 1
    assert [to for _, to in await history_rows(visit_id)].count("completed") == 1


async def test_other_targets_and_a_skipped_state_write_no_event(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]

    # Completing straight from checked_in is a skipped state: nothing changes, no event.
    assert await _move(engine, visit_id, VisitStatus.COMPLETED) == TransitionOutcome.SKIPPED
    assert await outbox_rows(visit_id) == []
    # Starting the visit is evented nowhere (D-07: visit in-progress is not an event).
    assert await _move(engine, visit_id, VisitStatus.IN_PROGRESS) == TransitionOutcome.APPLIED
    assert await outbox_rows(visit_id) == []
    assert (await _visit_row(engine, visit_id)).status == VisitStatus.IN_PROGRESS


async def test_a_rollback_writes_neither_the_transition_nor_the_event(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    await _move(engine, visit_id, VisitStatus.IN_PROGRESS)

    assert (
        await _move(engine, visit_id, VisitStatus.COMPLETED, commit=False)
        == TransitionOutcome.APPLIED
    )

    assert await outbox_rows(visit_id) == []
    stored = await _visit_row(engine, visit_id)
    assert stored.status == VisitStatus.IN_PROGRESS
    assert stored.completed_at is None
    assert [to for _, to in await history_rows(visit_id)] == ["checked_in", "in_progress"]


async def test_the_event_is_invisible_until_the_transaction_commits(
    client: AsyncClient, visit_world: VisitWorld, engine: AsyncEngine
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    await _move(engine, visit_id, VisitStatus.IN_PROGRESS)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        outcome = await visit_repo.transition_visit(
            session, visit_id=uuid.UUID(visit_id), target=VisitStatus.COMPLETED, actor="test"
        )
        assert outcome == TransitionOutcome.APPLIED
        # Flushed in this transaction, but a second connection sees neither the new
        # status nor the event: they commit together or not at all.
        assert await outbox_rows(visit_id) == []
        assert (await _visit_row(engine, visit_id)).status == VisitStatus.IN_PROGRESS
        await session.commit()

    assert len(await outbox_rows(visit_id)) == 1
    assert (await _visit_row(engine, visit_id)).status == VisitStatus.COMPLETED


async def test_the_completion_route_commits_the_event_with_the_response(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    visit = await checked_in_visit(client, visit_world)
    visit_id = visit["id"]
    provider = visit_world.world.provider
    started = await client.post(f"/api/visits/{visit_id}/start", headers=auth(provider))
    assert started.status_code == 200, started.text
    assert await outbox_rows(visit_id) == []

    completed = await client.post(f"/api/visits/{visit_id}/complete", headers=auth(provider))
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert len(await outbox_rows(visit_id)) == 1

    # A repeated request is a no-op: still one event.
    repeat = await client.post(f"/api/visits/{visit_id}/complete", headers=auth(provider))
    assert repeat.status_code == 200, repeat.text
    assert len(await outbox_rows(visit_id)) == 1
