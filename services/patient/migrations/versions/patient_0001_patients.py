"""Patient baseline: `patients` for patient_db.

Revision ID: patient_0001
Revises:
Create Date: 2026-10-12

A FRESH single-baseline history for Patient's own database (database-per-service,
D-01/D-04) — not a replay of the monolith's 0001/0002, which touched every table
now owned by other services.

`patients.clinic_id` (Organization's) and `patients.user_id` (Identity's) are
plain UUID columns with NO foreign keys: the rows they reference live in other
services' databases and are validated at write time (D-05). `user_id` is nullable
and unique (one profile per account); the PHI columns (`full_name`,
`date_of_birth`, `phone`, `address`) exist in this schema and in no other
service's. Soft-delete columns (`is_active` default true, `deactivated_at`) are
part of the baseline.

`gen_random_uuid()` is built into Postgres 13+, so no pgcrypto extension is
needed. Downgrade drops the index and then the table, so `downgrade base;
upgrade head` returns to the identical schema.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "patient_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "patients",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Plain UUIDs, no foreign keys (D-01, D-04).
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("date_of_birth", sa.Date(), nullable=False),
        sa.Column("phone", sa.String(32), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("user_id", name="uq_patients_user_id"),
    )
    op.create_index("ix_patients_clinic_id", "patients", ["clinic_id"])


def downgrade() -> None:
    op.drop_index("ix_patients_clinic_id", table_name="patients")
    op.drop_table("patients")
