"""Scheduling baseline: provider_slots with its double-booking exclusion constraint.

Revision ID: scheduling_0001
Revises:
Create Date: 2026-10-12

A FRESH single-baseline history for Scheduling's own database (database-per-service,
D-01/D-03) — not a replay of the monolith's migrations. The table is created
empty; Phase 2's booking saga is what fills it.

`provider_id` is a plain UUID with NO foreign key: the provider row lives in
Provider's database and is validated at write time (D-05).

The GiST exclusion constraint is the load-bearing double-booking guarantee:
overlapping NON-cancelled slots for the same provider are rejected by Postgres
itself. It needs `btree_gist` to combine scalar equality (provider_id) with
range overlap (tstzrange &&). The db-init job creates the extension as the
superuser; `btree_gist` is a trusted extension, so `CREATE EXTENSION IF NOT
EXISTS` here is a no-op in Compose and still works for the owner role elsewhere.

Downgrade drops the constraint, index and table, and deliberately leaves the
extension in place: it is provisioned by db-init and not owned by scheduling_svc.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "scheduling_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    op.create_table(
        "provider_slots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("provider_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status",
            sa.String(16),
            nullable=False,
            server_default=sa.text("'available'"),
        ),
        sa.CheckConstraint(
            "status IN ('available','held','booked','cancelled')",
            name="ck_provider_slots_status_valid",
        ),
    )
    op.create_index("ix_provider_slots_provider_id", "provider_slots", ["provider_id"])

    op.execute(
        """
        ALTER TABLE provider_slots
        ADD CONSTRAINT ex_provider_slots_no_overlap
        EXCLUDE USING gist (
            provider_id WITH =,
            tstzrange(start_time, end_time) WITH &&
        ) WHERE (status <> 'cancelled')
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE provider_slots DROP CONSTRAINT IF EXISTS ex_provider_slots_no_overlap")
    op.drop_index("ix_provider_slots_provider_id", table_name="provider_slots")
    op.drop_table("provider_slots")
