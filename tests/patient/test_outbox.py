"""Patient's outbox emission on live Postgres (plan 03-08; EVENT-01, EVENT-02, ANLY-01;
D-06, D-07, D-10, T-3-24, T-3-25).

`create_patient` writes a `patient.registered` outbox row and `deactivate_patient` (only
when it actually deactivates) a `patient.deactivated` row, each in the SAME transaction
as the patient change. A repeat deactivation, an out-of-clinic deactivation and a
failure before the commit write no event. Both events carry ids and timestamps only.

These tests COMMIT on purpose (the repository functions commit, and atomicity must be
shown from a second connection), so every test uses its own throwaway clinic id and the
patient rows are removed at the end of the test (test-data hygiene only; application code
never deletes). The outbox is insert-only by design (D-09), so the rows stay and are
relayed by Debezium as ordinary events for unknown clinics. Serialization uses the LIVE
Schema Registry (`localhost:8081`, subjects registered by `schema-init`).
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
from services.patient.config import settings
from services.patient.events import (
    PATIENT_EVENT_TYPES,
    emit_patient_deactivated,
    emit_patient_registered,
)
from services.patient.repository import patient_repo

pytestmark = pytest.mark.integration

PHI_FULL_NAME = "Zzyzx Outboxpatient Sentinelname"
PHI_DATE_OF_BIRTH = datetime.date(1984, 6, 29)
PHI_PHONE = "+1-555-010-7777"
PHI_ADDRESS = "7777 Sentinel Street Unit 12"
PHI_STRINGS = (PHI_FULL_NAME, PHI_DATE_OF_BIRTH.isoformat(), PHI_PHONE, PHI_ADDRESS)
PHI_FRAGMENTS = ("Outboxpatient", "Sentinelname", "1984", "555-010", "Sentinel Street")


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(settings.database_url, pool_size=5, max_overflow=0)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def clinic(engine: AsyncEngine) -> AsyncIterator[uuid.UUID]:
    """A throwaway clinic id; its patient rows are removed at the end of the test."""
    clinic_id = uuid.uuid4()
    try:
        yield clinic_id
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM patients WHERE clinic_id = :c"), {"c": clinic_id})


async def _register(engine: AsyncEngine, clinic_id: uuid.UUID):
    async with AsyncSession(engine, expire_on_commit=False) as session:
        return await patient_repo.create_patient(
            session,
            clinic_id=clinic_id,
            full_name=PHI_FULL_NAME,
            date_of_birth=PHI_DATE_OF_BIRTH,
            phone=PHI_PHONE,
            address=PHI_ADDRESS,
        )


async def _outbox(engine: AsyncEngine, aggregate_id: uuid.UUID) -> list[Any]:
    """The aggregate's outbox rows, read on a separate connection (committed rows only)."""
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT id, aggregatetype, aggregateid, type, payload, schema_version, occurred_at "
                "FROM outbox WHERE aggregateid = :a ORDER BY occurred_at, type"
            ),
            {"a": str(aggregate_id)},
        )
        return list(result.all())


async def _patient_count(engine: AsyncEngine, clinic_id: uuid.UUID) -> int:
    async with engine.connect() as conn:
        return int(
            await conn.scalar(
                text("SELECT count(*) FROM patients WHERE clinic_id = :c"), {"c": clinic_id}
            )
            or 0
        )


def _decode(row: Any) -> dict[str, Any]:
    decoded = deserializer()(
        bytes(row.payload), SerializationContext(topic_for(row.type), MessageField.VALUE)
    )
    assert decoded is not None
    return decoded


def _ms(moment: datetime.datetime) -> datetime.datetime:
    """Avro `timestamp-millis` keeps milliseconds; Postgres timestamps have microseconds."""
    return moment.replace(microsecond=moment.microsecond // 1000 * 1000)


def _assert_phi_free(row: Any) -> None:
    raw = bytes(row.payload)
    for phi in PHI_STRINGS:
        assert phi.encode() not in raw, f"PHI string {phi!r} found in {row.type} payload"
    for fragment in PHI_FRAGMENTS:
        assert fragment.encode() not in raw, f"PHI fragment {fragment!r} found in {row.type} payload"


# --- patient.registered ---------------------------------------------------------------


async def test_create_patient_writes_exactly_one_registered_event_with_the_patient(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)

    rows = await _outbox(engine, patient.id)
    assert [row.type for row in rows] == ["patient.registered"]
    row = rows[0]
    assert row.aggregatetype == "patient"
    assert row.aggregateid == str(patient.id)
    assert row.id == event_id_for(patient.id, "patient.registered", "registered")
    assert row.schema_version == 1
    # The patient row it describes is committed too (one transaction).
    assert await _patient_count(engine, clinic) == 1

    value = _decode(row)
    assert str(value["patient_id"]) == str(patient.id)
    assert str(value["clinic_id"]) == str(clinic)
    assert str(value["aggregate_id"]) == str(patient.id)
    assert value["event_type"] == "patient.registered"
    assert value["registered_at"] == _ms(patient.created_at)
    # The outbox column keeps the full-precision commit-side timestamp.
    assert row.occurred_at == patient.created_at


async def test_registered_event_payload_has_no_phi_bytes(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)
    (row,) = await _outbox(engine, patient.id)

    _assert_phi_free(row)
    # Only ids and timestamps: the decoded record is exactly the PHI-free field set.
    assert set(_decode(row)) == {
        "event_id",
        "event_type",
        "schema_version",
        "occurred_at",
        "aggregate_id",
        "patient_id",
        "clinic_id",
        "registered_at",
    }


async def test_failure_before_commit_leaves_neither_patient_nor_event(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    """A failure after the flush (patient row and outbox row not yet committed) rolls
    both back: nothing is visible from another connection."""
    seen: list[uuid.UUID] = []

    async def failing_emit(db: AsyncSession, patient: Any) -> None:
        seen.append(patient.id)
        # The real emit runs first, so the outbox row IS in the open transaction ...
        await emit_patient_registered(db, patient)
        # ... and then the work fails before the commit.
        raise RuntimeError("simulated failure before commit")

    async with AsyncSession(engine, expire_on_commit=False) as session:
        with patch.object(patient_repo, "emit_patient_registered", failing_emit):
            with pytest.raises(RuntimeError, match="simulated failure"):
                await patient_repo.create_patient(
                    session,
                    clinic_id=clinic,
                    full_name=PHI_FULL_NAME,
                    date_of_birth=PHI_DATE_OF_BIRTH,
                )
        await session.rollback()

    assert len(seen) == 1
    assert await _patient_count(engine, clinic) == 0
    assert await _outbox(engine, seen[0]) == []


async def test_emit_registered_twice_still_writes_one_row(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    """A second emission of the same registration collides on the outbox primary key."""
    patient = await _register(engine, clinic)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        again = await emit_patient_registered(session, patient)
        await session.commit()
    assert again == event_id_for(patient.id, "patient.registered", "registered")
    assert len(await _outbox(engine, patient.id)) == 1


# --- patient.deactivated --------------------------------------------------------------


async def test_deactivate_active_patient_writes_one_deactivated_event(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        deactivated = await patient_repo.deactivate_patient(session, patient.id, clinic)
    assert deactivated is not None and deactivated.is_active is False
    assert deactivated.deactivated_at is not None

    rows = await _outbox(engine, patient.id)
    assert sorted(row.type for row in rows) == ["patient.deactivated", "patient.registered"]
    row = next(r for r in rows if r.type == "patient.deactivated")
    assert row.aggregatetype == "patient"
    assert row.aggregateid == str(patient.id)
    assert row.id == event_id_for(
        patient.id, "patient.deactivated", deactivated.deactivated_at.isoformat()
    )

    value = _decode(row)
    assert str(value["patient_id"]) == str(patient.id)
    assert str(value["clinic_id"]) == str(clinic)
    assert value["deactivated_at"] == _ms(deactivated.deactivated_at)
    assert row.occurred_at == deactivated.deactivated_at


async def test_repeat_deactivation_writes_no_second_event(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await patient_repo.deactivate_patient(session, patient.id, clinic)
    before = await _outbox(engine, patient.id)
    assert len(before) == 2

    async with AsyncSession(engine, expire_on_commit=False) as session:
        repeat = await patient_repo.deactivate_patient(session, patient.id, clinic)
    assert repeat is not None and repeat.is_active is False

    after = await _outbox(engine, patient.id)
    assert [r.id for r in after] == [r.id for r in before]


async def test_deactivation_out_of_clinic_scope_writes_nothing(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        result = await patient_repo.deactivate_patient(session, patient.id, uuid.uuid4())
    assert result is None

    rows = await _outbox(engine, patient.id)
    assert [row.type for row in rows] == ["patient.registered"]
    async with AsyncSession(engine, expire_on_commit=False) as session:
        still_active = await patient_repo.get_patient(session, patient.id, clinic)
    assert still_active is not None and still_active.is_active is True


async def test_failure_before_deactivation_commit_leaves_patient_active_and_no_event(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)

    async def failing_emit(db: AsyncSession, patient_obj: Any) -> None:
        await emit_patient_deactivated(db, patient_obj)
        raise RuntimeError("simulated failure before commit")

    async with AsyncSession(engine, expire_on_commit=False) as session:
        with patch.object(patient_repo, "emit_patient_deactivated", failing_emit):
            with pytest.raises(RuntimeError, match="simulated failure"):
                await patient_repo.deactivate_patient(session, patient.id, clinic)
        await session.rollback()

    async with AsyncSession(engine, expire_on_commit=False) as session:
        reloaded = await patient_repo.get_patient(session, patient.id, clinic)
    assert reloaded is not None and reloaded.is_active is True
    assert [row.type for row in await _outbox(engine, patient.id)] == ["patient.registered"]


async def test_deactivated_event_payload_has_no_phi_bytes(
    engine: AsyncEngine, clinic: uuid.UUID
) -> None:
    patient = await _register(engine, clinic)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        await patient_repo.deactivate_patient(session, patient.id, clinic)

    rows = await _outbox(engine, patient.id)
    assert len(rows) == 2
    for row in rows:
        _assert_phi_free(row)
    deactivated = next(r for r in rows if r.type == "patient.deactivated")
    assert set(_decode(deactivated)) == {
        "event_id",
        "event_type",
        "schema_version",
        "occurred_at",
        "aggregate_id",
        "patient_id",
        "clinic_id",
        "deactivated_at",
    }


# --- plain updates and the catalogue ---------------------------------------------------


async def test_update_patient_emits_no_event(engine: AsyncEngine, clinic: uuid.UUID) -> None:
    """Profile edits are not domain events (D-07): only registration and deactivation."""
    patient = await _register(engine, clinic)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        updated = await patient_repo.update_patient(
            session, patient.id, clinic, phone="+1-555-010-0000"
        )
    assert updated is not None
    assert [row.type for row in await _outbox(engine, patient.id)] == ["patient.registered"]


def test_patient_event_types_are_the_two_catalogued_patient_events() -> None:
    assert PATIENT_EVENT_TYPES == ("patient.registered", "patient.deactivated")
    for event_type in PATIENT_EVENT_TYPES:
        assert EVENT_CATALOG[event_type].aggregate_type == "patient"
