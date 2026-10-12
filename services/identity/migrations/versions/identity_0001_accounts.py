"""Identity baseline: the `users` (accounts) table for identity_db.

Revision ID: identity_0001
Revises:
Create Date: 2026-10-12

A FRESH single-baseline history for Identity's own database (database-per-
service, D-01/D-04) — not a replay of the monolith's 0001/0002, which touched
every table now owned by other services. Identity owns accounts only:
credentials, role, and `clinic_id`.

`users.clinic_id` is a plain nullable UUID with NO foreign key: the clinic
lives in the Organization service's database (D-04); validating it at write
time is a cross-service call delivered by plan 01.1-05 (D-05).

`gen_random_uuid()` is built into Postgres 13+, so no pgcrypto extension is
needed. Downgrade drops the index then the table, so
`downgrade base; upgrade head` returns to the identical schema.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "identity_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("email", sa.String(320), nullable=False, unique=True),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("clinic_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "role IN ('patient','provider','front_desk','admin')",
            name="ck_users_role_valid",
        ),
    )
    op.create_index("ix_users_clinic_id", "users", ["clinic_id"])


def downgrade() -> None:
    op.drop_index("ix_users_clinic_id", table_name="users")
    op.drop_table("users")
