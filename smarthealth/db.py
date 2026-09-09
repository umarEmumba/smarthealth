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

import asyncio
from collections.abc import AsyncIterator

from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from smarthealth.config import settings


class Base(DeclarativeBase):
    """Shared declarative base for every ORM model in the project."""


# `create_async_engine`'s underlying asyncpg connection pool binds its
# connections to the event loop active when they were first opened
# (asyncpg/protocol raises "attached to a different loop" otherwise). A
# module-level singleton engine is only safe under ONE long-lived event loop
# (a real uvicorn/gunicorn worker) — it is NOT safe across pytest-asyncio's
# per-test-function event loops. This module tracks which loop the cached
# engine belongs to and transparently builds a fresh engine (and session
# factory) whenever the running loop has changed, so the same `get_db()`
# dependency is correct in both the single-loop production process and the
# multi-loop test suite.
_engine: AsyncEngine | None = None
_engine_loop: asyncio.AbstractEventLoop | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the AsyncEngine bound to the current running event loop,
    creating a new one if none exists yet or if the previously-cached
    engine belongs to a different (e.g. prior test's) event loop.
    """
    global _engine, _engine_loop, _session_factory  # noqa: PLW0603

    # WR-06: `asyncio.get_event_loop()` is deprecated since Python 3.10 when
    # called with no running loop, and its old fallback behavior of
    # implicitly CREATING a new loop (rather than raising) can mask a bug
    # where this is called from a context with no running event loop.
    # `get_running_loop()` is correct here — every call site (`get_db`,
    # every FastAPI dependency) is always awaited from within a running loop.
    current_loop = asyncio.get_running_loop()
    if _engine is None or _engine_loop is not current_loop:
        _engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        _engine_loop = current_loop
        _session_factory = async_sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    get_engine()  # ensures _session_factory is built for the current loop
    assert _session_factory is not None  # noqa: S101 (invariant, not user input)
    return _session_factory


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped AsyncSession.

    Usage: `db: AsyncSession = Depends(get_db)` in a route handler. The
    session is closed at the end of the request regardless of outcome; the
    caller (route/repository code) is responsible for commit/rollback.
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


def __getattr__(name: str) -> object:
    """Module-level lazy attribute access preserving the pre-existing
    `smarthealth.db.engine` / `smarthealth.db.async_session` import surface
    (infra/migrations/env.py, Plan 01-02) without reintroducing the
    cross-event-loop singleton bug — each access resolves to the engine/
    session-factory bound to the CURRENT running event loop.
    """
    if name == "engine":
        return get_engine()
    if name == "async_session":
        return get_session_factory()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# --- MongoDB (audit trail) ---------------------------------------------------
#
# PyMongo's native async client (never Motor, per CLAUDE.md's "What NOT to
# Use") binds itself to the asyncio event loop it was created on and raises
# `RuntimeError` if reused from a different loop
# (pymongo.asynchronous.mongo_client._get_topology). A single process-wide
# singleton is therefore only safe under ONE long-lived event loop (a real
# uvicorn/gunicorn worker process) — it is NOT safe across pytest-asyncio's
# per-test-function event loops, where each test gets a fresh loop. This
# module tracks which loop the cached client belongs to and transparently
# creates a fresh client whenever the running loop has changed, so the same
# `get_mongo_client()` call is correct in both the single-loop production
# process and the multi-loop test suite.
import asyncio  # noqa: E402

_mongo_client: AsyncMongoClient | None = None
_mongo_client_loop: asyncio.AbstractEventLoop | None = None


def get_mongo_client() -> AsyncMongoClient:
    """Return the AsyncMongoClient bound to the current running event loop,
    creating a new one if none exists yet or if the previously-cached
    client belongs to a different (e.g. prior test's) event loop.
    """
    global _mongo_client, _mongo_client_loop  # noqa: PLW0603

    # WR-06: see the matching note in get_engine() above — get_running_loop()
    # is correct here since every call site is awaited from within a
    # running loop (get_mongo() FastAPI dependency).
    current_loop = asyncio.get_running_loop()
    if _mongo_client is None or _mongo_client_loop is not current_loop:
        _mongo_client = AsyncMongoClient(settings.mongo_url)
        _mongo_client_loop = current_loop
    return _mongo_client


async def get_mongo() -> AsyncIterator[AsyncMongoClient]:
    """FastAPI dependency yielding the loop-bound AsyncMongoClient.

    Usage: `mongo: AsyncMongoClient = Depends(get_mongo)` — used by every
    mutating route to call `libs.audit.writer.write_audit`.
    """
    yield get_mongo_client()
