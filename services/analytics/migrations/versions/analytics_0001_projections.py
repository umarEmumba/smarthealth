"""Analytics baseline: projection tables, the idempotency table and the metric views.

Revision ID: analytics_0001
Revises:
Create Date: 2026-11-11

A FRESH single-baseline history for Analytics' own database (analytics_db,
database-per-service D-04). Everything here is DERIVED from Kafka events by the
analytics-projection consumer (D-10 revised) and can be rebuilt by replay.

  * `processed_events`: the consumer's same-transaction idempotency claims
    (`libs.events.idempotency`); a redelivered event changes no number (EVENT-03).
  * `patients`, `appointments`, `visits_completed`: order-tolerant projections; ids
    and timestamps only, no foreign keys (T-3-34).
  * the metric views, each with a `COMMENT ON VIEW` that states the definition.

`grafana_ro` reads every table and view created here through the default privileges
the db-init job set up for the `analytics_svc` role.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from libs.events.idempotency import create_processed_events_table

# revision identifiers, used by Alembic.
revision: str = "analytics_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_VIEWS = ("v_active_patients",)


def _ts(name: str, *, nullable: bool = True) -> sa.Column:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def upgrade() -> None:
    create_processed_events_table(op)

    op.create_table(
        "patients",
        sa.Column("patient_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        _ts("registered_at"),
        _ts("deactivated_at"),
    )
    op.create_index("ix_patients_clinic", "patients", ["clinic_id"])

    op.create_table(
        "appointments",
        sa.Column("appointment_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("patient_id", postgresql.UUID(as_uuid=True), nullable=True),
        _ts("booked_at"),
        _ts("slot_start"),
        _ts("cancelled_at"),
        _ts("checked_in_at"),
        _ts("completed_at"),
        _ts("no_show_at"),
        sa.Column("rescheduled_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        _ts("last_event_at"),
    )
    op.create_index("ix_appointments_clinic_booked", "appointments", ["clinic_id", "booked_at"])
    op.create_index(
        "ix_appointments_clinic_completed", "appointments", ["clinic_id", "completed_at"]
    )
    op.create_index("ix_appointments_clinic", "appointments", ["clinic_id"])

    op.create_table(
        "visits_completed",
        sa.Column("visit_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("appointment_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        _ts("checked_in_at"),
        _ts("started_at"),
        _ts("completed_at", nullable=False),
    )
    op.create_index(
        "ix_visits_completed_clinic_completed", "visits_completed", ["clinic_id", "completed_at"]
    )
    op.create_index("ix_visits_completed_clinic", "visits_completed", ["clinic_id"])

    op.execute(
        """
        CREATE VIEW v_active_patients AS
        SELECT clinic_id, count(*) AS active_patients
        FROM patients
        WHERE registered_at IS NOT NULL AND deactivated_at IS NULL
        GROUP BY clinic_id
        """
    )
    op.execute(
        "COMMENT ON VIEW v_active_patients IS "
        "'Total patients (ANLY-01): patients registered (patient.registered) and not "
        "deactivated (patient.deactivated), per clinic. A deactivation lowers the count. "
        "Counts start at the Phase 3 cutover (D-13): earlier patients are not backfilled.'"
    )


def downgrade() -> None:
    for view in reversed(_VIEWS):
        op.execute(f"DROP VIEW IF EXISTS {view}")
    op.drop_table("visits_completed")
    op.drop_table("appointments")
    op.drop_table("patients")
    op.execute("DROP TABLE IF EXISTS processed_events")
