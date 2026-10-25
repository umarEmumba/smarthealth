"""Appointment schedule-change flags (plan 02.1-08; M2).

Revision ID: scheduling_0005
Revises: scheduling_0004
Create Date: 2026-10-25

When a provider's schedule changes (a template is edited or deactivated, or the
provider is deactivated) the slots that no longer exist are withdrawn, and the
bookings that sit on them are FLAGGED instead of cancelled: staff decide whether
to cancel or reschedule. The flag is two nullable columns on `appointments`:

  * `schedule_change_flagged_at timestamptz NULL`  - when the flag was set; NULL
    means "not flagged". The flag is set at most once (a conditional UPDATE
    `WHERE schedule_change_flagged_at IS NULL`), so a repeated or concurrent
    apply flags and notifies each appointment once.
  * `schedule_change_reason varchar(32) NULL`      - why: `template_updated`,
    `template_deactivated` or `provider_deactivated` (Provider's `change_type`
    vocabulary). A fixed code, never staff-entered text.

`ix_appointments_schedule_flagged (clinic_id, slot_start) WHERE
schedule_change_flagged_at IS NOT NULL` serves the staff "flagged" list; it holds
only the (few) flagged rows.

Both columns are nullable with no default, so the upgrade is a metadata-only
change on PostgreSQL and no existing row needs a rewrite.

Downgrade drops the index and the columns. It is LOSSY only in the sense that the
flags are forgotten: the `schedule_changed:<reason>` rows in
`appointment_status_history` are never touched and keep the record.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "scheduling_0005"
down_revision: Union[str, None] = "scheduling_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "appointments",
        sa.Column("schedule_change_flagged_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "appointments",
        sa.Column("schedule_change_reason", sa.String(length=32), nullable=True),
    )
    op.create_index(
        "ix_appointments_schedule_flagged",
        "appointments",
        ["clinic_id", "slot_start"],
        postgresql_where=sa.text("schedule_change_flagged_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_appointments_schedule_flagged", table_name="appointments")
    op.drop_column("appointments", "schedule_change_reason")
    op.drop_column("appointments", "schedule_change_flagged_at")
