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
under pytest-asyncio's per-test loops. The registry is therefore keyed by
(database URL, loop): each entry records its owning loop, a new loop gets its
own engine, and the same dependency is correct in both production and the
test suite. A consumer thread's persistent loop next to the test's loop uses
one engine each, with no rebuild on every switch.

Why closed loops are retired (plan 03-23, WR-05): replacing an entry without
disposing its engine leaves the pool's server connections alive until garbage
collection, so every per-test loop leaked backends and a long test run crept
toward the Postgres connection limit. At every lookup, entries whose loop is
closed are retired: their idle pooled connections are closed and the entry is
dropped. An entry whose loop is still running is never touched from another
loop (its connections are in use there).

No declarative `Base` lives here: each service declares its own so Alembic
autogenerate never sees another service's tables (RESEARCH Pitfall 2).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from libs.config import shared_settings


@dataclass(slots=True)
class _Entry:
    engine: AsyncEngine
    loop: asyncio.AbstractEventLoop
    session_factory: async_sessionmaker[AsyncSession]


# Keyed by (database URL, id(loop)); the entry's loop is compared by identity on
# lookup so a recycled id() can never hand one loop another loop's engine.
_registry: dict[tuple[str, int], _Entry] = {}


def _close_server_side(record: object) -> None:
    """Shut down the socket under one pooled connection record.

    A closed loop cannot run asyncpg's own close/terminate (`terminate()` raises
    RuntimeError there), but the server notices a shut-down socket and ends the
    backend. The socket is shut down through a dup of its descriptor so no private
    socket attribute is needed. Attribute paths (`dbapi_connection._connection`,
    asyncpg `Connection._transport`) are private; verified against SQLAlchemy 2.0.52
    and asyncpg 0.31.0, and every step is best-effort (a miss costs an idle
    connection until garbage collection, the old behaviour, never an error).
    """
    raw = getattr(record, "dbapi_connection", None)
    asyncpg_connection = getattr(raw, "_connection", None)
    transport = getattr(asyncpg_connection, "_transport", None)
    sock = transport.get_extra_info("socket") if transport is not None else None
    if sock is None:
        return
    duplicate = socket.socket(fileno=os.dup(sock.fileno()))
    try:
        duplicate.shutdown(socket.SHUT_RDWR)
    finally:
        duplicate.close()


def _retire(entry: _Entry) -> None:
    """Release a closed loop's engine: drain the idle pool, close each server side."""
    sync_engine = entry.engine.sync_engine
    idle = getattr(sync_engine.pool, "_pool", None)  # QueuePool's checked-in records
    while idle is not None:
        try:
            record = idle.get_nowait()
        except Exception:  # queue.Empty (or an unexpected pool shape): nothing left to drain
            break
        with contextlib.suppress(Exception):
            _close_server_side(record)
    with contextlib.suppress(Exception):
        sync_engine.dispose(close=False)


def _entry_for(database_url: str) -> _Entry:
    # get_running_loop() (not the deprecated get_event_loop()): every call
    # site is awaited from inside a running loop, and a missing loop here is
    # a bug that implicit loop creation would mask.
    current_loop = asyncio.get_running_loop()
    for key, stale in list(_registry.items()):
        if stale.loop.is_closed():
            del _registry[key]
            _retire(stale)
    key = (database_url, id(current_loop))
    entry = _registry.get(key)
    if entry is None or entry.loop is not current_loop:
        engine = create_async_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=shared_settings.db_pool_size,
            max_overflow=shared_settings.db_max_overflow,
        )
        entry = _Entry(
            engine=engine,
            loop=current_loop,
            session_factory=async_sessionmaker(bind=engine, expire_on_commit=False),
        )
        _registry[key] = entry
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
