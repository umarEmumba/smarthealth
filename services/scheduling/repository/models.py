"""Scheduling's ORM models: `provider_slots`, `appointments`, `idempotency_keys`
and `appointment_status_history` (D-02, D-03, D-08).

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

Phase 2 adds the booking data layer on top. The constraints that make
double-booking and duplicate requests impossible (`uq_appointments_active_slot`,
`ex_appointments_patient_no_overlap`, the `idempotency_keys` primary key) are
mirrored here from migration `scheduling_0002`, which is their source of truth;
see that migration's docstring for what each one guarantees.

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables. Async-only per CLAUDE.md; no sync driver or engine is imported.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CHAR,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from services.scheduling.domain.enums import AppointmentStatus


def _status_in(statuses: tuple[str, ...]) -> str:
    return "status IN (" + ",".join(f"'{s}'" for s in statuses) + ")"


# Predicate shared by the active-slot unique index and the patient-overlap
# exclusion constraint: the appointment statuses that occupy a slot. Built from
# `AppointmentStatus.ACTIVE` (single source) so the model equals migration
# `scheduling_0004` (M1: checked_in, completed and no_show keep their slot).
_ACTIVE_PREDICATE = _status_in(AppointmentStatus.ACTIVE)


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
    """A concrete bookable slot for a provider.

    `clinic_id`/`department_id` are denormalized at generation (ORG-03);
    `held_until`/`held_by_appointment_id` carry the short booking hold (D-03).
    `held_by_appointment_id` stays set once the slot is `booked`, so a retried
    hold or confirm by the same appointment is recognised as already done.
    """

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
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    department_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    held_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    held_by_appointment_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('available','held','booked','cancelled')",
            name="ck_provider_slots_status_valid",
        ),
        CheckConstraint("end_time > start_time", name="ck_provider_slots_end_after_start"),
        UniqueConstraint("provider_id", "start_time", name="uq_provider_slots_provider_start"),
        Index(
            "ix_provider_slots_held_until",
            "held_until",
            postgresql_where=text("status = 'held'"),
        ),
        ExcludeConstraint(
            (text("provider_id"), "="),
            (text("tstzrange(start_time, end_time)"), "&&"),
            where=text("status <> 'cancelled'"),
            using="gist",
            name="ex_provider_slots_no_overlap",
        ),
    )


class Appointment(SchedulingBase):
    """A booking of one slot by one patient.

    `id` is generated by the application (the idempotency row written first in
    the same transaction references it). `slot_start`/`slot_end` are copied
    from the slot so the patient-overlap exclusion constraint can see them.
    `coverage_ref` is an opaque billing reference: stored here for the billing
    activity, never logged and never placed in workflow I/O.
    """

    __tablename__ = "appointments"

    id: Mapped[uuid.UUID] = _uuid_pk()
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    patient_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    provider_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    slot_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("provider_slots.id", name="fk_appointments_slot_id"),
        nullable=False,
    )
    slot_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    slot_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'pending'"))
    failure_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    coverage_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    workflow_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_by: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            _status_in(AppointmentStatus.ALL),
            name="ck_appointments_status_valid",
        ),
        # D-02: at most one active appointment per slot.
        Index(
            "uq_appointments_active_slot",
            "slot_id",
            unique=True,
            postgresql_where=text(_ACTIVE_PREDICATE),
        ),
        # One patient cannot hold two overlapping active appointments.
        ExcludeConstraint(
            (text("patient_id"), "="),
            (text("tstzrange(slot_start, slot_end)"), "&&"),
            where=text(_ACTIVE_PREDICATE),
            using="gist",
            name="ex_appointments_patient_no_overlap",
        ),
        Index(
            "ix_appointments_pending_unstarted",
            "created_at",
            postgresql_where=text("status = 'pending' AND workflow_started_at IS NULL"),
        ),
        Index("ix_appointments_clinic_status", "clinic_id", "status", "slot_start"),
    )


class IdempotencyKey(SchedulingBase):
    """One `(caller, Idempotency-Key)` claim (D-08).

    The composite primary key is the duplicate-request arbiter. Expiry is lazy:
    an expired row is overwritten by the next claim, never deleted.
    """

    __tablename__ = "idempotency_keys"

    caller_user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    appointment_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (PrimaryKeyConstraint("caller_user_id", "key", name="pk_idempotency_keys"),)


class AppointmentStatusHistory(SchedulingBase):
    """Append-only record of every appointment transition (D-09)."""

    __tablename__ = "appointment_status_history"

    id: Mapped[uuid.UUID] = _uuid_pk()
    appointment_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("appointments.id", name="fk_appointment_status_history_appointment_id"),
        nullable=False,
        index=True,
    )
    from_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_status: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class WaitlistEntry(SchedulingBase):
    """A patient waiting for a provider's slot (plan 02-09; SCHED-07, D-12).

    Mirrors migration `scheduling_0003`, its source of truth: the
    one-active-entry partial unique index, the FIFO partial index and the
    status/window CHECKs. `coverage_ref` is an opaque billing reference copied
    onto the auto-booked appointment; it is never returned, logged or put in
    workflow I/O.
    """

    __tablename__ = "waitlist_entries"

    id: Mapped[uuid.UUID] = _uuid_pk()
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    provider_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    patient_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    coverage_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'waiting'"))
    failure_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    matched_appointment_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    booked_appointment_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_by: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('waiting','matched','booked','failed','cancelled')",
            name="ck_waitlist_entries_status_valid",
        ),
        CheckConstraint(
            "window_start IS NULL OR window_end IS NULL OR window_end > window_start",
            name="ck_waitlist_entries_window_order",
        ),
        Index(
            "uq_waitlist_active_patient_provider",
            "patient_id",
            "provider_id",
            unique=True,
            postgresql_where=text("status IN ('waiting','matched')"),
        ),
        Index(
            "ix_waitlist_fifo",
            "provider_id",
            "created_at",
            "id",
            postgresql_where=text("status = 'waiting'"),
        ),
        Index("ix_waitlist_clinic_status", "clinic_id", "status", "created_at"),
    )
