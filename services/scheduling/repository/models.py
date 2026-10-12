"""Scheduling's ORM model: `provider_slots` and its double-booking constraint (D-03).

Scheduling owns the concrete bookable slots, so the table and its exclusion
constraint live here from the start and Phase 2's booking saga never has to move
a table between services or databases.

`provider_id` is a plain UUID with NO foreign key: the provider row lives in
Provider's database. Scheduling validates it at write time against Provider's
`/internal/providers/{id}` (D-05), never through a cross-database constraint.

The `EXCLUDE USING gist (provider_id WITH =, tstzrange(start_time, end_time)
WITH &&) WHERE (status <> 'cancelled')` constraint is the load-bearing
guarantee — NOT an application-level check-then-insert and NOT a lock-only
scheme: both have failure windows under concurrent load that only a
database-level constraint closes. It needs the `btree_gist` extension to combine
a scalar-equality column with a range-overlap operator; the db-init job creates
it as the superuser and the migration's `CREATE EXTENSION IF NOT EXISTS` is then
a no-op.

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables. Async-only per CLAUDE.md; no sync driver or engine is imported.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class SchedulingBase(DeclarativeBase):
    """Declarative base for Scheduling's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call (a Column instance can belong
    to only one Table, so it cannot be shared across declarative classes)."""
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


class ProviderSlot(SchedulingBase):
    """A concrete bookable slot for a provider — empty until Phase 2."""

    __tablename__ = "provider_slots"

    id: Mapped[uuid.UUID] = _uuid_pk()
    provider_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'available'")
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('available','held','booked','cancelled')",
            name="ck_provider_slots_status_valid",
        ),
        ExcludeConstraint(
            (text("provider_id"), "="),
            (text("tstzrange(start_time, end_time)"), "&&"),
            where=text("status <> 'cancelled'"),
            using="gist",
            name="ex_provider_slots_no_overlap",
        ),
    )
