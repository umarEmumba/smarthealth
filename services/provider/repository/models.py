"""Provider's ORM models: providers, specialties, provider_specialties,
availability_templates and schedule_change_events (D-01, D-04, M2).

The provider directory is its own bounded context, deliberately separate from
Organization (the user's finer-bounded-context choice). `providers.department_id`
(owned by Organization), `providers.user_id` (owned by Identity) and
`availability_templates.department_id` (Organization) are plain UUID columns with
NO foreign key: the rows they point at live in other services' databases, so they
are validated at write time through the shared reference client (D-05) and never
cascade (D-06). The links INSIDE this service keep their foreign keys:
`provider_specialties` -> providers/specialties and
`availability_templates.provider_id` -> providers.

There is NO concrete-slot table here (Phase 1 Decision 2, preserved): availability
is configured as weekly templates only; bookable slots belong to Scheduling.

`AvailabilityTemplate.start_time` / `end_time` are naive TIME values that MUST be
resolved against the owning clinic's IANA timezone (Phase 1 Pitfall 3), never
assumed UTC or server-local. Provider holds no clinic data: the clinic (and its
timezone) is reachable through the department's Organization reference, which is
the concern of whoever turns a template into a concrete instant (Scheduling).

`schedule_change_events` is Provider's outbox for schedule-affecting changes (M2): a
row is written in the SAME transaction as the template edit, template deactivation
or provider deactivation that can make already-generated slots wrong, and stays
pending (`delivered_at IS NULL`) until a consumer acknowledges it through the
internal feed. Consumers are other services reading `/internal/schedule-changes`;
there is no cross-service foreign key and `provider_id` / `template_id` are plain
UUIDs (no FK even to Provider's own tables, so an event outlives any later cleanup).

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables (RESEARCH Pitfall 2). Async-only per CLAUDE.md; no sync driver
or engine is imported here.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Table,
    Time,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class ProviderBase(DeclarativeBase):
    """Declarative base for Provider's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call (a Column instance can belong
    to only one Table, so it cannot be shared across declarative classes)."""
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


# Intra-service many-to-many: both sides are owned here, so both keep their FKs.
provider_specialties = Table(
    "provider_specialties",
    ProviderBase.metadata,
    Column(
        "provider_id",
        PG_UUID(as_uuid=True),
        ForeignKey("providers.id"),
        primary_key=True,
    ),
    Column(
        "specialty_id",
        PG_UUID(as_uuid=True),
        ForeignKey("specialties.id"),
        primary_key=True,
    ),
)


class Specialty(ProviderBase):
    """A medical specialty a provider may carry (PPL-02).

    `name` is UNIQUE: that constraint, not application code, is what stops two
    concurrent provider creations that introduce the same new specialty from
    producing two rows.
    """

    __tablename__ = "specialties"

    id: Mapped[uuid.UUID] = _uuid_pk()
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    providers: Mapped[list["Provider"]] = relationship(
        secondary=provider_specialties, back_populates="specialties"
    )


class Provider(ProviderBase):
    """A care provider, tied to a department by a bare id (ORG-03).

    `department_id` (Organization's) and `user_id` (Identity's) are plain UUIDs
    with no foreign key; `user_id` is nullable and unique (one provider profile
    per login account).
    """

    __tablename__ = "providers"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, unique=True
    )
    department_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    specialties: Mapped[list[Specialty]] = relationship(
        secondary=provider_specialties, back_populates="providers"
    )


class AvailabilityTemplate(ProviderBase):
    """A provider's recurring weekly availability rule (PPL-03).

    Config/rules only — NOT a materialized slot row. `day_of_week` follows the
    0=Monday..6=Sunday convention (CHECK-enforced at the database level);
    `start_time`/`end_time` are naive TIME values interpreted in the owning
    clinic's timezone (Phase 1 Pitfall 3) — whoever generates concrete slots
    resolves them against a calendar date and DST.

    `department_id` is a plain UUID copied from the provider at creation time
    (validated with Organization then); it has no foreign key.
    """

    __tablename__ = "availability_templates"

    id: Mapped[uuid.UUID] = _uuid_pk()
    provider_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("providers.id"), nullable=False, index=True
    )
    department_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    day_of_week: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    start_time: Mapped[time] = mapped_column(Time, nullable=False)
    end_time: Mapped[time] = mapped_column(Time, nullable=False)
    slot_duration_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("30")
    )
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "day_of_week BETWEEN 0 AND 6", name="ck_availability_templates_day_of_week"
        ),
    )


class ScheduleChangeType:
    """Allowed `schedule_change_events.change_type` values (plain str constants,
    mirrored by the `ck_schedule_change_events_type_valid` CHECK)."""

    TEMPLATE_UPDATED = "template_updated"
    TEMPLATE_DEACTIVATED = "template_deactivated"
    PROVIDER_DEACTIVATED = "provider_deactivated"

    ALL = (TEMPLATE_UPDATED, TEMPLATE_DEACTIVATED, PROVIDER_DEACTIVATED)


class ScheduleChangeEvent(ProviderBase):
    """One committed change that can make already-generated slots wrong (M2).

    Carries ids and a change type only (no names, no PHI). `delivered_at` is set by
    the consumer's acknowledgement; until then the event is pending and is returned
    by the feed, oldest first by `(created_at, id)`.
    """

    __tablename__ = "schedule_change_events"

    id: Mapped[uuid.UUID] = _uuid_pk()
    provider_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    change_type: Mapped[str] = mapped_column(String(32), nullable=False)
    template_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "change_type IN ('template_updated','template_deactivated','provider_deactivated')",
            name="ck_schedule_change_events_type_valid",
        ),
        Index(
            "ix_schedule_change_events_pending",
            "created_at",
            "id",
            postgresql_where=text("delivered_at IS NULL"),
        ),
    )
