"""Add soft-delete columns (is_active/deactivated_at) to clinics,
departments, patients, providers, and availability_templates.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-10

Gap-closure plan 01-07: healthcare records must remain auditable and
retained — "delete" is a status flip (is_active=false + deactivated_at set
by application code), never a row removal. No hard deletes are introduced
anywhere in this migration or the routes it backs.

`is_active BOOLEAN NOT NULL DEFAULT true` keeps every pre-existing row (and
every future insert that doesn't explicitly set it) active by default —
the server_default means this migration needs no data backfill step.
`deactivated_at TIMESTAMPTZ NULL` stays NULL until a resource is
deactivated. Downgrade drops both columns from all five tables, in the
reverse order they were added — upgrade/downgrade is symmetric, verified
via `alembic upgrade head && alembic downgrade -1 && alembic upgrade head`
against the live Postgres container.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLES = ["clinics", "departments", "patients", "providers", "availability_templates"]


def upgrade() -> None:
    for table_name in _TABLES:
        op.add_column(
            table_name,
            sa.Column(
                "is_active",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("true"),
            ),
        )
        op.add_column(
            table_name,
            sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    for table_name in reversed(_TABLES):
        op.drop_column(table_name, "deactivated_at")
        op.drop_column(table_name, "is_active")
