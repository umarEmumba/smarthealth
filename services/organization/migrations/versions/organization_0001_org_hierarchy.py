"""Organization baseline: `clinics` and `departments` for organization_db.

Revision ID: organization_0001
Revises:
Create Date: 2026-10-12

A FRESH single-baseline history for Organization's own database (database-per-
service, D-01/D-04) — not a replay of the monolith's 0001/0002, which touched
every table now owned by other services.

`departments.clinic_id` keeps its foreign key to `clinics.id`: both tables
belong to this service, so the constraint is intra-service and remains the
real guard against orphan departments. Soft-delete columns (`is_active`
default true, `deactivated_at`) are part of the baseline.

`gen_random_uuid()` is built into Postgres 13+, so no pgcrypto extension is
needed. Downgrade drops the index, then `departments`, then `clinics` (FK
order), so `downgrade base; upgrade head` returns to the identical schema.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "organization_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "clinics",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("timezone", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "departments",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "clinic_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("clinics.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_departments_clinic_id", "departments", ["clinic_id"])


def downgrade() -> None:
    op.drop_index("ix_departments_clinic_id", table_name="departments")
    op.drop_table("departments")
    op.drop_table("clinics")
