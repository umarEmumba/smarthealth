"""Analytics' ORM mirror of the three projection tables (D-10, D-04).

These tables are DERIVED data: only the analytics-projection consumer writes them,
from Kafka events, and every row can be rebuilt by replaying the topics (plan
03-13). They hold ids and timestamps only (no PHI arrives in events, T-3-34), no
foreign keys (the referenced aggregates live in other services' databases), and
every id is a plain UUID.

`processed_events` (the consumer's idempotency claims, created by
`libs.events.idempotency.create_processed_events_table`) and the five metric views
are created by raw SQL in `analytics_0001` and are intentionally NOT ORM tables.

Analytics declares its OWN `DeclarativeBase` (never a shared one; RESEARCH Pitfall
2). Async-only per CLAUDE.md.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class AnalyticsBase(DeclarativeBase):
    """Declarative base for Analytics' tables only."""


class Patient(AnalyticsBase):
    """One row per patient seen in `patient.registered` / `patient.deactivated`.

    Either event may create the row (arrival order is not guaranteed across
    topics' replays), so `registered_at` is nullable until the registration lands.
    """

    __tablename__ = "patients"

    patient_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    registered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_patients_clinic", "clinic_id"),)


class Appointment(AnalyticsBase):
    """One row per appointment, folded from the six appointment events.

    Every fact is first-write-wins (`COALESCE(existing, new)`); `slot_start` is
    versioned by `last_event_at`, the latest `occurred_at` of an event that carried
    a slot start, so a late `appointment.rescheduled` cannot overwrite a newer slot.
    `booked_at` stays NULL for an appointment booked before the Phase 3 cutover
    (D-13): such a row is excluded from the booked and cancellation views.
    """

    __tablename__ = "appointments"

    appointment_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    patient_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    booked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    slot_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checked_in_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    no_show_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rescheduled_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_appointments_clinic_booked", "clinic_id", "booked_at"),
        Index("ix_appointments_clinic_completed", "clinic_id", "completed_at"),
        Index("ix_appointments_clinic", "clinic_id"),
    )


class VisitCompleted(AnalyticsBase):
    """One row per completed visit (`visit.completed`); `started_at` is NULL for a
    visit completed without a recorded start, which is excluded from the wait only."""

    __tablename__ = "visits_completed"

    visit_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    appointment_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    checked_in_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_visits_completed_clinic_completed", "clinic_id", "completed_at"),
        Index("ix_visits_completed_clinic", "clinic_id"),
    )
