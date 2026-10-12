"""Integration + unit tests for the Phase-1 data model (Plan 01-02).

Integration tests (`@pytest.mark.integration`) require the live Postgres and
MongoDB containers from infra/docker-compose.yml — run via
`pytest tests/test_models.py -x` with the stack up, or
`pytest tests/ -x -k "not integration"` to skip them.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from pymongo import AsyncMongoClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.mongo_indexes import AUDIT_COLLECTION_NAME, ensure_audit_indexes
from services.identity.repository.models import (
    AvailabilityTemplate,
    Clinic,
    Department,
    Provider,
    ProviderSlot,
)
from smarthealth.config import settings


@pytest.mark.integration
async def test_exclusion_constraint_live(db_session: AsyncSession) -> None:
    """Overlapping non-cancelled provider_slots for the same provider must
    raise an IntegrityError — proving the GiST exclusion constraint (the
    Phase 2 double-booking backstop) is enforced by Postgres itself, not
    merely declared in the migration file.
    """
    clinic = Clinic(name="Test Clinic", timezone="America/New_York")
    db_session.add(clinic)
    await db_session.flush()

    department = Department(clinic_id=clinic.id, name="Cardiology")
    db_session.add(department)
    await db_session.flush()

    provider = Provider(department_id=department.id, full_name="Dr. Test Provider")
    db_session.add(provider)
    await db_session.flush()

    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    first_slot = ProviderSlot(
        provider_id=provider.id,
        start_time=base,
        end_time=base + timedelta(minutes=30),
        status="available",
    )
    db_session.add(first_slot)
    await db_session.flush()

    # Overlaps the first slot by 15 minutes for the SAME provider. Flushed
    # inside its own SAVEPOINT (begin_nested) so the expected IntegrityError
    # only rolls back this one statement, leaving the outer transaction (and
    # the db_session fixture's own teardown rollback) in a clean state.
    overlapping_slot = ProviderSlot(
        provider_id=provider.id,
        start_time=base + timedelta(minutes=15),
        end_time=base + timedelta(minutes=45),
        status="available",
    )
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(overlapping_slot)
            await db_session.flush()


@pytest.mark.integration
async def test_audit_indexes_created(mongo_client: AsyncMongoClient) -> None:
    """ensure_audit_indexes creates the three query indexes idempotently.

    ensure_audit_indexes writes to settings.mongo_db_name (the app's real
    configured database, not a hardcoded test override) — so this test reads
    the indexes back from that same database and cleans up the collection
    itself afterwards (the mongo_client fixture's teardown only drops
    `{mongo_db_name}_test`, a different database).
    """
    collection = mongo_client[settings.mongo_db_name][AUDIT_COLLECTION_NAME]

    try:
        # First call creates the indexes.
        names_first = await ensure_audit_indexes(mongo_client)
        assert set(names_first) == {
            "ix_audit_entity_type_entity_id_timestamp",
            "ix_audit_actor_id_timestamp",
            "ix_audit_clinic_id_timestamp",
        }

        # Second call must not raise (idempotent by index name).
        names_second = await ensure_audit_indexes(mongo_client)
        assert set(names_second) == set(names_first)

        index_cursor = await collection.list_indexes()
        existing = {doc["name"] async for doc in index_cursor}
        assert "ix_audit_entity_type_entity_id_timestamp" in existing
        assert "ix_audit_actor_id_timestamp" in existing
        assert "ix_audit_clinic_id_timestamp" in existing
    finally:
        await collection.drop()


def test_availability_template_day_of_week_check_declared() -> None:
    """The day_of_week CHECK (0-6) is declared on the ORM model.

    Pure unit test — no live DB needed. Asserts the CheckConstraint exists
    in AvailabilityTemplate.__table_args__ so PPL-03's day-of-week domain
    (Monday=0..Sunday=6) is enforced at the schema level, not left to
    application-layer trust alone.
    """
    check_constraints = [
        c for c in AvailabilityTemplate.__table__.constraints if hasattr(c, "sqltext")
    ]
    assert any(
        "day_of_week" in str(c.sqltext) and "0" in str(c.sqltext) and "6" in str(c.sqltext)
        for c in check_constraints
    ), "AvailabilityTemplate must declare a day_of_week BETWEEN 0 AND 6 CHECK constraint"
