"""Loop-aware async SQLAlchemy engine/session helpers, keyed by database URL.

Async-only per CLAUDE.md's "What NOT to Use" mandate — `create_async_engine`
on a `postgresql+asyncpg://` URL. A sync driver or `Session` inside an
`async def` route blocks the event-loop thread for every concurrent request.

Why a registry keyed by URL (not one global engine): database-per-service
means six apps can share ONE process (the in-process test harness), each with
its own URL. A single global engine would hand every service the first
service's database.

Why loop-aware: asyncpg binds pooled connections to the event loop that
opened them ("attached to a different loop" otherwise). A cached engine is
only safe under one long-lived loop (a real uvicorn/gunicorn worker), NOT
under pytest-asyncio's per-test loops. Each entry records its owning loop
and is rebuilt when the running loop differs, so the same dependency is
correct in both production and the test suite.

No declarative `Base` lives here: each service declares its own so Alembic
autogenerate never sees another service's tables (RESEARCH Pitfall 2).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


@dataclass(slots=True)
class _Entry:
    engine: AsyncEngine
    loop: asyncio.AbstractEventLoop
    session_factory: async_sessionmaker[AsyncSession]


_registry: dict[str, _Entry] = {}


def _entry_for(database_url: str) -> _Entry:
    # get_running_loop() (not the deprecated get_event_loop()): every call
    # site is awaited from inside a running loop, and a missing loop here is
    # a bug that implicit loop creation would mask.
    current_loop = asyncio.get_running_loop()
    entry = _registry.get(database_url)
    if entry is None or entry.loop is not current_loop:
        engine = create_async_engine(database_url, pool_pre_ping=True)
        entry = _Entry(
            engine=engine,
            loop=current_loop,
            session_factory=async_sessionmaker(bind=engine, expire_on_commit=False),
        )
        _registry[database_url] = entry
    return entry


def get_engine(database_url: str) -> AsyncEngine:
    """AsyncEngine for `database_url`, bound to the current running loop."""
    return _entry_for(database_url).engine


def get_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    """Session factory for `database_url`, bound to the current running loop."""
    return _entry_for(database_url).session_factory


def make_session_dependency(
    database_url_getter: Callable[[], str],
) -> Callable[[], AsyncIterator[AsyncSession]]:
    """Build a FastAPI dependency yielding one request-scoped AsyncSession.

    The URL is resolved per request via `database_url_getter` (not captured at
    import) so a service's settings object stays the single source of truth.
    The session is closed at the end of the request regardless of outcome; the
    route/repository code owns commit/rollback.
    """

    async def get_db() -> AsyncIterator[AsyncSession]:
        session_factory = get_session_factory(database_url_getter())
        async with session_factory() as session:
            yield session

    return get_db


async def ping(database_url: str) -> None:
    """Execute `SELECT 1` against `database_url`; raises on any failure.

    Used by the shared health router so a service reports unhealthy when its
    own database is unreachable.
    """
    async with get_session_factory(database_url)() as session:
        await session.execute(text("SELECT 1"))
