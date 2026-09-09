"""Alembic environment — async engine via the run_sync bridge.

Per CLAUDE.md (Alembic 1.19.2 async via run_sync bridging): Alembic's core
migration-running machinery is synchronous, so we open an async engine
and bridge into it with `connection.run_sync(...)`. This never imports a
sync driver; the bridge runs Alembic's sync migration context on top of
an async connection borrowed from the async engine, not a second sync
connection pool.

NOTE (WR-06 follow-up): this module deliberately does NOT import
`smarthealth.db.engine` (the lazy, loop-bound singleton `__getattr__`
property) — `smarthealth.db.get_engine()` calls `asyncio.get_running_loop()`
(correct inside the app's async request-handling code, see the WR-06 fix
in smarthealth/db.py), which raises `RuntimeError` if resolved at plain
module-import time, before `asyncio.run(...)` below has started a loop.
Alembic's own `run_env()` imports this file synchronously at zero
running-loop context, so this module builds its OWN engine directly from
`settings.database_url` instead of reusing the app's request-scoped
lazy singleton.

target_metadata = Base.metadata is set by importing
services.identity.repository.models, which populates Base.metadata as a side
effect of module import (every declarative model registers itself on import).
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# Import models FIRST so Base.metadata is fully populated before Alembic
# (autogenerate or otherwise) inspects target_metadata.
from services.identity.repository import models  # noqa: F401
from smarthealth.config import settings
from smarthealth.db import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emits SQL, no live DB connection)."""
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a dedicated async engine.

    Deliberately its own `create_async_engine(...)` call, not
    `smarthealth.db.get_engine()` — this coroutine IS running inside
    `asyncio.run(...)` below, so `get_running_loop()` would actually
    succeed here, but sharing the app's request-scoped singleton across
    this short-lived, one-shot migration process is unnecessary; a
    dedicated engine disposed at the end of this function is simpler and
    avoids any accidental cross-process engine-reuse assumption.
    """
    connectable = create_async_engine(settings.database_url)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
