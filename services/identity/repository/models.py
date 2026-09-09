"""SQLAlchemy 2.0 async declarative models for the Phase-1 data foundation.

Covers identity (users), operational patient/provider/clinic/department
management, provider availability, and the (empty-in-Phase-1) provider_slots
table whose GiST exclusion constraint is Phase 2's data-layer double-booking
guarantee. See .planning/phases/01-foundation-core-services/01-RESEARCH.md
(Decision 2 + Pattern 3) and the 01-02 plan's resolved decision checkpoint
for the full rationale behind every choice below.

Resolved decision checkpoint (recorded here, not just in STATE.md, because
it is load-bearing for every model in this file):
    - `users` (auth/login accounts) and `patients` (operational profiles
      that may have no login) are SEPARATE tables. `patients.user_id` is
      nullable so a front-desk-created profile satisfies PPL-01 without
      requiring IAM-01 self-registration to have happened first.
    - Provider availability is modeled as WEEKLY AVAILABILITY TEMPLATES
      (AvailabilityTemplate) — config/rules, not materialized slot rows.
    - `provider_slots` is created now but stays EMPTY this phase (no 90-day
      horizon / regeneration logic — that is Phase 2's job). Its GiST
      exclusion constraint is created and proven live now (see
      tests/test_models.py::test_exclusion_constraint_live) so Phase 2
      inherits an already-tested double-booking invariant.

No sync driver/engine is imported anywhere in this module — SQLAlchemy 2.0
async only, per CLAUDE.md.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smarthealth.db import Base

# --- Reusable column helpers -------------------------------------------------


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call.

    A shared `mapped_column(...)` instance cannot be reused across multiple
    declarative classes — SQLAlchemy raises "Column object already assigned
    to Table" because each Column belongs to exactly one Table. Each entity
    below calls this factory to get its own Column instance.
    """
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


class Clinic(Base):
    """A physical/operational clinic location (ORG-01).

    `timezone` is a required IANA tz-database string (e.g.
    "America/New_York") — RESEARCH Pitfall 3: without a clinic-level
    timezone, `availability_templates.start_time`/`end_time` (naive TIME
    values) are ambiguous and silently corrupt multi-region slot generation
    in Phase 2. This column is what resolves that ambiguity.
    """

    __tablename__ = "clinics"

    id: Mapped[uuid.UUID] = _uuid_pk()
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    departments: Mapped[list["Department"]] = relationship(back_populates="clinic")


class Department(Base):
    """A department within a clinic (ORG-02); part of the ORG-03 scope chain."""

    __tablename__ = "departments"

    id: Mapped[uuid.UUID] = _uuid_pk()
    clinic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("clinics.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    clinic: Mapped[Clinic] = relationship(back_populates="departments")
    providers: Mapped[list["Provider"]] = relationship(back_populates="department")


class User(Base):
    """Auth/login identity (IAM-01/IAM-02/IAM-03) — distinct from `Patient`.

    A `User` is created whenever someone can log in: patients who
    self-register, providers, front-desk staff, and admins. `Patient` rows
    created by front-desk/admin staff on a walk-in's behalf (PPL-01) do NOT
    require a `User` row to exist first — see `Patient.user_id`.
    """

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    # Nullable: admins may be clinic-unscoped; patients/front-desk/provider
    # users are scoped to the clinic they operate at or registered through.
    clinic_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("clinics.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('patient','provider','front_desk','admin')",
            name="ck_users_role_valid",
        ),
    )


class Patient(Base):
    """Operational patient profile (PPL-01/PPL-04).

    `user_id` is NULLABLE — a front-desk/admin-created profile (PPL-01) need
    not have a login account. When a patient self-registers (IAM-01), a
    `User` row is created and linked here via `user_id`. This is the
    resolved users-vs-patients separation from the plan's opening decision
    checkpoint.

    PHI note: `full_name`, `date_of_birth`, `phone`, and `address` are
    PHI-bearing columns. They live in Postgres (system of record) but MUST
    NEVER be written to logs, traces, or the Mongo audit `changes` field —
    see .planning/phases/01-foundation-core-services/01-RESEARCH.md's
    PHI-Safe Structured Logging section; enforced in Plan 03's audit writer.
    """

    __tablename__ = "patients"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, unique=True
    )
    clinic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("clinics.id"), nullable=False
    )
    # --- PHI-bearing operational profile fields ---
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    date_of_birth: Mapped[date] = mapped_column(Date, nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


# --- Providers, specialties, and their many-to-many association -------------

provider_specialties = Table(
    "provider_specialties",
    Base.metadata,
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


class Specialty(Base):
    """A medical specialty a provider may carry (PPL-02)."""

    __tablename__ = "specialties"

    id: Mapped[uuid.UUID] = _uuid_pk()
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    providers: Mapped[list["Provider"]] = relationship(
        secondary=provider_specialties, back_populates="specialties"
    )


class Provider(Base):
    """A care provider, scoped to a department (ORG-03 scope chain root).

    `provider_id → department_id → clinic_id` is the structural FK chain
    that makes clinic/department scoping non-optional for anything hanging
    off a provider — including `provider_slots` below.
    """

    __tablename__ = "providers"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, unique=True
    )
    department_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("departments.id"), nullable=False
    )
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    department: Mapped[Department] = relationship(back_populates="providers")
    specialties: Mapped[list[Specialty]] = relationship(
        secondary=provider_specialties, back_populates="providers"
    )


class AvailabilityTemplate(Base):
    """A provider's recurring weekly availability rule (PPL-03).

    Config/rules only — NOT a materialized slot row. `day_of_week` follows
    ISO-ish 0=Monday..6=Sunday convention (CHECK-enforced at the DB level);
    `start_time`/`end_time` are naive TIME values interpreted in the owning
    clinic's `Clinic.timezone` (RESEARCH Pitfall 3) — Phase 2's
    slot-generation job is what resolves them against a concrete calendar
    date and DST.
    """

    __tablename__ = "availability_templates"

    id: Mapped[uuid.UUID] = _uuid_pk()
    provider_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("providers.id"), nullable=False
    )
    department_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("departments.id"), nullable=False
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

    __table_args__ = (
        CheckConstraint(
            "day_of_week BETWEEN 0 AND 6", name="ck_availability_templates_day_of_week"
        ),
    )


class ProviderSlot(Base):
    """A concrete bookable slot for a provider — EMPTY in Phase 1.

    Created now, with its exclusion constraint live, so Phase 2's booking
    saga inherits an already-tested double-booking invariant instead of
    designing this schema under saga-implementation time pressure
    (RESEARCH Pattern 3 + Decision 2; plan's resolved checkpoint).

    The `EXCLUDE USING gist (provider_id WITH =, tstzrange(start_time,
    end_time) WITH &&) WHERE (status <> 'cancelled')` constraint is the
    load-bearing guarantee — NOT an application-level check-then-insert and
    NOT a Redis-lock-only scheme (RESEARCH "Don't Hand-Roll": both have real
    failure windows under concurrent load that only a DB-level constraint
    closes). Requires the `btree_gist` extension (created by the Alembic
    migration in Task 2) to combine a scalar-equality column with a
    range-overlap operator in one exclusion constraint.
    """

    __tablename__ = "provider_slots"

    id: Mapped[uuid.UUID] = _uuid_pk()
    provider_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("providers.id"), nullable=False
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
