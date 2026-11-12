"""Visit baseline: visits and the append-only visit_state_history.

Revision ID: visit_0001
Revises:
Create Date: 2026-10-13

A FRESH single-baseline history for Visit's own database (database-per-service,
D-04). `appointment_id`, `clinic_id`, `patient_id` and `provider_id` are plain
UUIDs with NO foreign key: those rows live in other services' databases.

Two guards make every state transition duplicate-safe (research Pattern 9):
  * the application moves a visit only with a conditional
    `UPDATE visits ... WHERE id = :v AND status = :expected`, so of N concurrent
    identical requests exactly one changes the row;
  * `uq_visit_history_visit_to UNIQUE (visit_id, to_status)` is the second,
    data-layer guard: the lifecycle (checked_in -> in_progress -> completed)
    never re-enters a state, so one history row per (visit, target) is exact.

Other invariants:
  * `uq_visits_appointment` — one visit per appointment (concurrent check-ins
    arbitrate on it with `INSERT ... ON CONFLICT DO NOTHING`);
  * `ck_visits_status_valid` — the three visit states only;
  * `ix_visits_clinic_status_created` — the clinic-scoped `GET /api/visits`;
  * `ix_visits_downstream_pending` — partial index over completed visits whose
    downstream workflow has not been started yet, for plan 02-11's sweeper.

No PHI column exists, by design. Downgrade drops everything this revision
created, in reverse order.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "visit_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "visits",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("appointment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("patient_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "checked_in_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("downstream_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.UniqueConstraint("appointment_id", name="uq_visits_appointment"),
        sa.CheckConstraint(
            "status IN ('checked_in','in_progress','completed')", name="ck_visits_status_valid"
        ),
    )
    op.create_index(
        "ix_visits_clinic_status_created", "visits", ["clinic_id", "status", "created_at"]
    )
    op.create_index(
        "ix_visits_downstream_pending",
        "visits",
        ["completed_at"],
        postgresql_where=sa.text("status = 'completed' AND downstream_started_at IS NULL"),
    )

    op.create_table(
        "visit_state_history",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "visit_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("visits.id", name="fk_visit_state_history_visit_id"),
            nullable=False,
        ),
        sa.Column("from_status", sa.String(16), nullable=True),
        sa.Column("to_status", sa.String(16), nullable=False),
        sa.Column("actor", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.UniqueConstraint("visit_id", "to_status", name="uq_visit_history_visit_to"),
    )


def downgrade() -> None:
    op.drop_table("visit_state_history")
    op.drop_index("ix_visits_downstream_pending", table_name="visits")
    op.drop_index("ix_visits_clinic_status_created", table_name="visits")
    op.drop_table("visits")
