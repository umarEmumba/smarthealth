"""Billing's ORM models: `billing_records` and `billing_status_history` (D-04).

`appointment_id` and `clinic_id` are plain UUID columns with NO foreign key:
the appointment lives in Scheduling's database and the clinic in
Organization's. Billing trusts its authenticated internal callers for them.

Only a coarse `coverage_kind` (`self_pay` / `insured`) and a machine-readable
`reason_code` are stored; the raw reference the pre-check evaluated is never
persisted (T-02-24).

History is append-only. `UNIQUE(record_id, idempotency_key)` lets a retried
activity (same `Idempotency-Key`) be recognised as already applied. History
timestamps use `clock_timestamp()` so the two rows a pre-check writes in one
transaction still order correctly.

Billing declares its OWN `DeclarativeBase` (never a shared one; RESEARCH
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


class BillingBase(DeclarativeBase):
    """Declarative base for Billing's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


_STATUS_CHECK = (
    "status IN ('pending','verified','failed','voided',"
    "'refund_pending','refunded','no_refund','finalized')"
)


class BillingRecord(BillingBase):
    """One billing record per appointment (BILL-01)."""

    __tablename__ = "billing_records"

    id: Mapped[uuid.UUID] = _uuid_pk()
    appointment_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    clinic_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    coverage_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        UniqueConstraint("appointment_id", name="uq_billing_records_appointment_id"),
        CheckConstraint(_STATUS_CHECK, name="ck_billing_records_status_valid"),
        Index("ix_billing_records_clinic_created", "clinic_id", "created_at"),
    )


class BillingStatusHistory(BillingBase):
    """Append-only record of every billing change, including re-links (BILL-02)."""

    __tablename__ = "billing_status_history"

    id: Mapped[uuid.UUID] = _uuid_pk()
    record_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("billing_records.id", name="fk_billing_status_history_record_id"),
        nullable=False,
        index=True,
    )
    from_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_status: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )

    __table_args__ = (
        UniqueConstraint("record_id", "idempotency_key", name="uq_billing_history_record_key"),
    )
