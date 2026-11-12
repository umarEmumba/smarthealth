"""Waitlist entries (plan 02-09; SCHED-07, D-12).

Revision ID: scheduling_0003
Revises: scheduling_0002
Create Date: 2026-10-13

waitlist_entries (new)
  * One row per "book me with this provider when a slot frees" request:
    `provider_id`, `patient_id`, an optional time window
    (`window_start`/`window_end`, NULL = open-ended) and an optional opaque
    `coverage_ref` (used only by the auto-booking, never returned or logged).
  * `ck_waitlist_entries_status_valid`: waiting / matched / booked / failed /
    cancelled. `matched` is the claim state: the waitlist-advance activity
    moves ONE entry `waiting -> matched` under `FOR UPDATE SKIP LOCKED` and
    creates the appointment in the same transaction, so two concurrent slot
    frees can never pick the same entry.
  * `ck_waitlist_entries_window_order`: a window, when both ends are given,
    must not be empty or inverted.
  * `uq_waitlist_active_patient_provider` partial UNIQUE (patient_id,
    provider_id) WHERE status IN ('waiting','matched'): one patient holds at
    most one ACTIVE entry per provider. Booked, failed and cancelled entries
    leave the predicate, so the patient may join again without deleting
    anything.
  * `ix_waitlist_fifo` partial index (provider_id, created_at, id) WHERE
    status = 'waiting': the FIFO claim scan (`ORDER BY created_at, id` for one
    provider, waiting rows only), so the claim reads the oldest candidates
    first and stays cheap as served/failed rows accumulate.
  * `ix_waitlist_clinic_status` (clinic_id, status, created_at): clinic-scoped
    listings for front desk and admin.
  * Ids of other services' rows (clinic, provider, patient, creator) are plain
    UUIDs, validated over REST at write time, never by cross-database FK.

Downgrade drops the indexes, then the table.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "scheduling_0003"
down_revision: Union[str, None] = "scheduling_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "waitlist_entries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("patient_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("coverage_ref", sa.String(64), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'waiting'")),
        sa.Column("failure_reason", sa.String(64), nullable=True),
        sa.Column("matched_appointment_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("booked_appointment_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.CheckConstraint(
            "status IN ('waiting','matched','booked','failed','cancelled')",
            name="ck_waitlist_entries_status_valid",
        ),
        sa.CheckConstraint(
            "window_start IS NULL OR window_end IS NULL OR window_end > window_start",
            name="ck_waitlist_entries_window_order",
        ),
    )
    op.create_index(
        "uq_waitlist_active_patient_provider",
        "waitlist_entries",
        ["patient_id", "provider_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('waiting','matched')"),
    )
    op.create_index(
        "ix_waitlist_fifo",
        "waitlist_entries",
        ["provider_id", "created_at", "id"],
        postgresql_where=sa.text("status = 'waiting'"),
    )
    op.create_index(
        "ix_waitlist_clinic_status", "waitlist_entries", ["clinic_id", "status", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_waitlist_clinic_status", table_name="waitlist_entries")
    op.drop_index("ix_waitlist_fifo", table_name="waitlist_entries")
    op.drop_index("uq_waitlist_active_patient_provider", table_name="waitlist_entries")
    op.drop_table("waitlist_entries")
