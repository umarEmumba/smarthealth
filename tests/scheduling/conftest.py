"""Fixtures for the Scheduling service tests (scheduling_db only).

Two kinds of database fixture live here:

* `scheduling_db_session` - one session inside an outer transaction that is
  always rolled back. Right for single-session constraint proofs.
* `committed_scheduling_engine` + `seed_slot` + `cleanup_scheduling_rows` - for
  concurrency and idempotency proofs, which need many sessions whose
  transactions really COMMIT and race each other. A rolled-back outer
  transaction cannot show that. Every row such a test creates is scoped by a
  random `provider_id`, and `cleanup_scheduling_rows` removes exactly those
  rows at the end of the test (test-data hygiene only; application code never
  deletes).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterable
from datetime import datetime, timedelta

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine


@pytest_asyncio.fixture
async def scheduling_db_session() -> AsyncIterator[AsyncSession]:
    """A session on scheduling_db whose work is always rolled back.

    The session joins an OUTER transaction that is rolled back at teardown, so
    inserted slots never persist. Tests that expect an IntegrityError flush it
    inside `begin_nested()` (a SAVEPOINT) so only that statement is undone and
    the outer transaction stays usable.

    Requires the live Postgres container with scheduling migrations applied
    (`alembic -c services/scheduling/migrations/alembic.ini upgrade head`).
    """
    from services.scheduling.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            session = AsyncSession(bind=connection, expire_on_commit=False)
            try:
                yield session
            finally:
                await session.close()
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def committed_scheduling_engine() -> AsyncIterator[AsyncEngine]:
    """An engine on scheduling_db for tests whose transactions COMMIT.

    The pool is bounded to 20 connections (10 + 10 overflow): the concurrency
    tests prove the database constraints, not pool exhaustion, and Postgres'
    `max_connections` is shared with Temporal and every other service
    (research Pitfall 11).
    """
    from services.scheduling.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url, pool_size=10, max_overflow=10)
    try:
        yield engine
    finally:
        await engine.dispose()


async def seed_slot(
    engine: AsyncEngine,
    *,
    provider_id: uuid.UUID,
    start: datetime,
    minutes: int = 30,
    clinic_id: uuid.UUID,
    department_id: uuid.UUID,
    status: str = "available",
) -> uuid.UUID:
    """Insert and COMMIT one slot; return its id."""
    async with engine.begin() as conn:
        return await conn.scalar(
            text(
                "INSERT INTO provider_slots "
                "(provider_id, clinic_id, department_id, start_time, end_time, status) "
                "VALUES (:p, :c, :d, :s, :e, :st) RETURNING id"
            ),
            {
                "p": provider_id,
                "c": clinic_id,
                "d": department_id,
                "s": start,
                "e": start + timedelta(minutes=minutes),
                "st": status,
            },
        )


async def cleanup_scheduling_rows(engine: AsyncEngine, provider_ids: Iterable[uuid.UUID]) -> None:
    """Remove ONLY the rows a test created, scoped by its random provider ids.

    Test-data cleanup, not application behaviour: the scheduling code paths
    never delete. Order follows the foreign keys (history -> appointments ->
    slots); idempotency keys are matched through the test's appointments.
    """
    ids = list(provider_ids)
    if not ids:
        return
    async with engine.begin() as conn:
        params = {"ids": ids}
        await conn.execute(
            text(
                "DELETE FROM appointment_status_history WHERE appointment_id IN "
                "(SELECT id FROM appointments WHERE provider_id = ANY(:ids))"
            ),
            params,
        )
        await conn.execute(
            text(
                "DELETE FROM idempotency_keys WHERE appointment_id IN "
                "(SELECT id FROM appointments WHERE provider_id = ANY(:ids))"
            ),
            params,
        )
        await conn.execute(text("DELETE FROM appointments WHERE provider_id = ANY(:ids)"), params)
        # Plan 02-09: waitlist entries are scoped by provider too.
        await conn.execute(
            text("DELETE FROM waitlist_entries WHERE provider_id = ANY(:ids)"), params
        )
        await conn.execute(text("DELETE FROM provider_slots WHERE provider_id = ANY(:ids)"), params)
