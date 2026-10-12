"""Fixtures for the Scheduling service tests (scheduling_db only)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


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
