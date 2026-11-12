"""Provider baseline: specialties, providers, provider_specialties and
availability_templates for provider_db.

Revision ID: provider_0001
Revises:
Create Date: 2026-10-12

A FRESH single-baseline history for Provider's own database (database-per-service,
D-01/D-04) — not a replay of the monolith's 0001/0002, which touched every table
now owned by other services.

`providers.department_id`, `providers.user_id` and
`availability_templates.department_id` are plain UUID columns with NO foreign
keys: the rows they reference live in Organization's and Identity's databases and
are validated at write time (D-05). The links INSIDE this service keep their
foreign keys (`provider_specialties` -> providers/specialties,
`availability_templates.provider_id` -> providers).

No concrete-slot table is created here: availability is stored as weekly
templates only (Phase 1 Decision 2). `specialties.name` is UNIQUE — the backstop
that keeps two concurrent provider creations from making the same specialty twice.
Soft-delete columns (`is_active` default true, `deactivated_at`) are part of the
baseline.

`gen_random_uuid()` is built into Postgres 13+, so no pgcrypto extension is
needed. Downgrade drops indexes and tables in reverse dependency order, so
`downgrade base; upgrade head` returns to the identical schema.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "provider_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _uuid_pk() -> sa.Column:
    return sa.Column(
        "id",
        postgresql.UUID(as_uuid=True),
        primary_key=True,
        server_default=sa.text("gen_random_uuid()"),
    )


def upgrade() -> None:
    op.create_table(
        "specialties",
        _uuid_pk(),
        sa.Column("name", sa.String(255), nullable=False),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "providers",
        _uuid_pk(),
        # Plain UUIDs, no foreign keys (D-01, D-04): the account is Identity's,
        # the department is Organization's.
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("department_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("user_id"),
    )
    op.create_index("ix_providers_department_id", "providers", ["department_id"])

    op.create_table(
        "provider_specialties",
        sa.Column(
            "provider_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("providers.id"),
            primary_key=True,
        ),
        sa.Column(
            "specialty_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("specialties.id"),
            primary_key=True,
        ),
    )

    op.create_table(
        "availability_templates",
        _uuid_pk(),
        sa.Column(
            "provider_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("providers.id"),
            nullable=False,
        ),
        # Plain UUID, no foreign key: the department is Organization's.
        sa.Column("department_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("day_of_week", sa.SmallInteger(), nullable=False),
        sa.Column("start_time", sa.Time(), nullable=False),
        sa.Column("end_time", sa.Time(), nullable=False),
        sa.Column(
            "slot_duration_minutes",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("30"),
        ),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "day_of_week BETWEEN 0 AND 6", name="ck_availability_templates_day_of_week"
        ),
    )
    op.create_index(
        "ix_availability_templates_provider_id", "availability_templates", ["provider_id"]
    )
    op.create_index(
        "ix_availability_templates_department_id", "availability_templates", ["department_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_availability_templates_department_id", table_name="availability_templates")
    op.drop_index("ix_availability_templates_provider_id", table_name="availability_templates")
    op.drop_table("availability_templates")
    op.drop_table("provider_specialties")
    op.drop_index("ix_providers_department_id", table_name="providers")
    op.drop_table("providers")
    op.drop_table("specialties")
