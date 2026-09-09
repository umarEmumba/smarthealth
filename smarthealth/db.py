"""Shared async SQLAlchemy 2.0 engine, session factory, declarative Base, and
FastAPI DB dependency.

Async-only per CLAUDE.md's "What NOT to Use" mandate — `create_async_engine`
on the `postgresql+asyncpg://` URL from `smarthealth.config.settings`. Never
import the forbidden sync Postgres driver or a sync `Session` here; a sync
call inside an `async def` route blocks the single event-loop thread for
every other concurrent request, which defeats the whole point of an async
framework under this project's peak-load requirements.

Every entity model in `services/identity/repository/models.py` (and every
later service's models) shares this one `Base`, so Alembic's
`target_metadata = Base.metadata` sees the full schema regardless of which
service module defines a given table.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from smarthealth.config import settings

engine = create_async_engine(settings.database_url, pool_pre_ping=True)

async_session = async_sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    """Shared declarative base for every ORM model in the project."""


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped AsyncSession.

    Usage: `db: AsyncSession = Depends(get_db)` in a route handler. The
    session is closed at the end of the request regardless of outcome; the
    caller (route/repository code) is responsible for commit/rollback.
    """
    async with async_session() as session:
        yield session
