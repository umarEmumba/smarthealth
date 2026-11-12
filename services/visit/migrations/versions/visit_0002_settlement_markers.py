"""Visit settlement markers: billing and appointment completion outcomes.

Revision ID: visit_0002
Revises: visit_0001
Create Date: 2026-10-25

Plan 02.1-09 (M1, S4 groundwork). After a visit completes, two downstream
steps must land in other services: Billing's finalize and Scheduling's
appointment completion. Each leaves a durable marker in `visit_db`:

  * `billing_settled_at` / `billing_outcome` (`finalized`, `no_record`,
    `not_finalizable`);
  * `appointment_settled_at` / `appointment_outcome` (`completed`,
    `no_appointment`, `not_checked_in`).

A NULL timestamp means "not settled yet"; a business refusal is a terminal
outcome and sets its marker too, so nothing loops forever. A later reconciler
(plan 02.1-10) finds the unsettled visits through the partial index
`ix_visits_unsettled`. Visits that completed before this revision have NULL
markers and are not backfilled (synthetic dev data).

Downgrade drops the index, then the columns.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "visit_0002"
down_revision: Union[str, None] = "visit_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("visits", sa.Column("billing_settled_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("visits", sa.Column("billing_outcome", sa.String(32), nullable=True))
    op.add_column(
        "visits", sa.Column("appointment_settled_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("visits", sa.Column("appointment_outcome", sa.String(32), nullable=True))
    op.create_index(
        "ix_visits_unsettled",
        "visits",
        ["completed_at"],
        postgresql_where=sa.text(
            "status = 'completed' AND "
            "(billing_settled_at IS NULL OR appointment_settled_at IS NULL)"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_visits_unsettled", table_name="visits")
    op.drop_column("visits", "appointment_outcome")
    op.drop_column("visits", "appointment_settled_at")
    op.drop_column("visits", "billing_outcome")
    op.drop_column("visits", "billing_settled_at")
