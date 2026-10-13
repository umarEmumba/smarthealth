"""Visit's ORM models: `visits` and `visit_state_history` (D-04, D-15).

`appointment_id`, `clinic_id`, `patient_id` and `provider_id` are plain UUID
columns with NO foreign key: the appointment lives in Scheduling's database,
the clinic in Organization's, the patient in Patient's and the provider in
Provider's. Visit verifies the appointment with Scheduling at check-in and
copies its ids; it stores no PHI (ids, statuses and timestamps only).

Two data-layer guards make transitions duplicate-safe (research Pattern 9):
  * every move is a conditional `UPDATE ... WHERE status = <expected>`, so of
    N concurrent identical requests exactly one changes the row;
  * `uq_visit_history_visit_to UNIQUE (visit_id, to_status)` — valid because
    the lifecycle never re-enters a state — so a second history row for the
    same move cannot exist even if the first guard were bypassed.

`uq_visits_appointment` makes check-in one visit per appointment.
`downstream_started_at` stays NULL at completion; plan 02-11's sweeper finds
completed visits whose downstream workflow was never started through the
partial index `ix_visits_downstream_pending`.

Visit declares its OWN `DeclarativeBase` (never a shared one; RESEARCH
Pitfall 2). Async-only per CLAUDE.md.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class VisitBase(DeclarativeBase):
    """Declarative base for Visit's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


STATUS_CHECK = "status IN ('checked_in','in_progress','completed')"


class Visit(VisitBase):
    """One visit per confirmed appointment (VISIT-01)."""

    __tablename__ = "visits"

    id: Mapped[uuid.UUID] = _uuid_pk()
    appointment_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    patient_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    provider_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    checked_in_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    downstream_started_at: Mapped[datetime | None] = mapped_column(
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
        UniqueConstraint("appointment_id", name="uq_visits_appointment"),
        CheckConstraint(STATUS_CHECK, name="ck_visits_status_valid"),
        Index("ix_visits_clinic_status_created", "clinic_id", "status", "created_at"),
        Index(
            "ix_visits_downstream_pending",
            "completed_at",
            postgresql_where=text("status = 'completed' AND downstream_started_at IS NULL"),
        ),
    )


class VisitStateHistory(VisitBase):
    """Append-only record of every visit state change (VISIT-03)."""

    __tablename__ = "visit_state_history"

    id: Mapped[uuid.UUID] = _uuid_pk()
    visit_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("visits.id", name="fk_visit_state_history_visit_id"),
        nullable=False,
    )
    from_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_status: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )

    __table_args__ = (
        UniqueConstraint("visit_id", "to_status", name="uq_visit_history_visit_to"),
    )
