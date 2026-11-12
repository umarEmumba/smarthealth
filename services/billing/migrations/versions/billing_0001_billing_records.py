"""Billing baseline: billing_records and the append-only billing_status_history.

Revision ID: billing_0001
Revises:
Create Date: 2026-10-13

A FRESH single-baseline history for Billing's own database (database-per-service,
D-04). `appointment_id` and `clinic_id` are plain UUIDs with NO foreign key:
those rows live in Scheduling's and Organization's databases.

Constraints carry the state machine's invariants:
  * `uq_billing_records_appointment_id` — one record per appointment, the arbiter
    for a concurrent duplicate pre-check;
  * `ck_billing_records_status_valid` — the eight billing states only;
  * `uq_billing_history_record_key` — one history row per (record, Idempotency-Key),
    so a retried activity is recognised as already applied.

No raw coverage reference column exists (only `coverage_kind`), by design.
Downgrade drops everything this revision created, in reverse order.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "billing_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "billing_records",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("appointment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("coverage_kind", sa.String(16), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.UniqueConstraint("appointment_id", name="uq_billing_records_appointment_id"),
        sa.CheckConstraint(
            "status IN ('pending','verified','failed','voided',"
            "'refund_pending','refunded','no_refund','finalized')",
            name="ck_billing_records_status_valid",
        ),
    )
    op.create_index(
        "ix_billing_records_clinic_created", "billing_records", ["clinic_id", "created_at"]
    )

    op.create_table(
        "billing_status_history",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "record_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("billing_records.id", name="fk_billing_status_history_record_id"),
            nullable=False,
        ),
        sa.Column("from_status", sa.String(16), nullable=True),
        sa.Column("to_status", sa.String(16), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column("idempotency_key", sa.String(200), nullable=True),
        sa.Column("actor", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.UniqueConstraint("record_id", "idempotency_key", name="uq_billing_history_record_key"),
    )
    op.create_index(
        "ix_billing_status_history_record_id", "billing_status_history", ["record_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_billing_status_history_record_id", table_name="billing_status_history")
    op.drop_table("billing_status_history")
    op.drop_index("ix_billing_records_clinic_created", table_name="billing_records")
    op.drop_table("billing_records")
