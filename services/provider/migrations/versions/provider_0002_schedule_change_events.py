"""schedule_change_events: Provider's outbox for schedule-affecting changes (M2).

Revision ID: provider_0002
Revises: provider_0001
Create Date: 2026-10-25

One row per committed change that can make already-generated slots wrong (a
schedule-affecting template edit, a template deactivation, a provider deactivation),
written in the same transaction as the change. Rows stay pending
(`delivered_at IS NULL`) until a consumer acknowledges them through Provider's
internal feed. `provider_id` and `template_id` are plain UUIDs with no foreign key.

`ix_schedule_change_events_pending` is a partial index on `(created_at, id)` for
undelivered rows only, so draining the feed stays cheap however many delivered rows
accumulate. Downgrade drops the index then the table, so
`downgrade provider_0001; upgrade head` returns to the identical schema.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "provider_0002"
down_revision: Union[str, None] = "provider_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "schedule_change_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("provider_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("change_type", sa.String(32), nullable=False),
        sa.Column("template_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "change_type IN ('template_updated','template_deactivated','provider_deactivated')",
            name="ck_schedule_change_events_type_valid",
        ),
    )
    op.create_index(
        "ix_schedule_change_events_pending",
        "schedule_change_events",
        ["created_at", "id"],
        postgresql_where=sa.text("delivered_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_schedule_change_events_pending",
        table_name="schedule_change_events",
        postgresql_where=sa.text("delivered_at IS NULL"),
    )
    op.drop_table("schedule_change_events")
