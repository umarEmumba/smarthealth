"""Alembic environment — async engine via the run_sync bridge.

Per CLAUDE.md (Alembic 1.19.2 async via run_sync bridging): Alembic's core
migration-running machinery is synchronous, so we open an async engine
(reusing smarthealth.db.engine — the same engine the app itself uses,
built from settings.database_url) and bridge into it with
`connection.run_sync(...)`. This never imports a sync driver; the bridge
runs Alembic's sync migration context on top of an async connection borrowed
from the async engine, not a second sync connection pool.

target_metadata = Base.metadata is set by importing
services.identity.repository.models, which populates Base.metadata as a side
effect of module import (every declarative model registers itself on import).
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection

# Import models FIRST so Base.metadata is fully populated before Alembic
# (autogenerate or otherwise) inspects target_metadata.
from services.identity.repository import models  # noqa: F401
from smarthealth.db import Base, engine

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emits SQL, no live DB connection)."""
    url = str(engine.url)
    context.configure(
        url=url,
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
    """Run migrations in 'online' mode against the live async engine."""
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
