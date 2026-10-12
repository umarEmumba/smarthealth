"""Alembic environment for patient_db — async engine via the run_sync bridge.

Per CLAUDE.md (Alembic 1.19.2 async via run_sync bridging): Alembic's
migration-running machinery is synchronous, so we open an async engine and
bridge into it with `connection.run_sync(...)`. No sync driver is imported.

`target_metadata = PatientBase.metadata` — Patient's OWN metadata,
never a shared one, so autogenerate only ever sees `patients`
(RESEARCH Pitfall 2). The engine is built here from `settings.database_url`
(PATIENT_DATABASE_URL), deliberately not the app's request-scoped,
loop-bound engine registry: this module is imported synchronously by Alembic
before `asyncio.run(...)` starts a loop, and a one-shot dedicated engine
disposed at the end is simpler and avoids any cross-process engine-reuse
assumption.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# Import models FIRST so PatientBase.metadata is fully populated before
# Alembic (autogenerate or otherwise) inspects target_metadata.
from services.patient.config import settings
from services.patient.repository import models

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = models.PatientBase.metadata


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
    """Run migrations in 'online' mode against a dedicated async engine."""
    connectable = create_async_engine(settings.database_url)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
