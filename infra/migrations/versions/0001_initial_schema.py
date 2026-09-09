"""Initial Phase-1 schema: identity, patients, providers, clinics,
departments, availability templates, and the empty provider_slots table with
its live GiST double-booking exclusion constraint.

Revision ID: 0001
Revises:
Create Date: 2026-09-09

Creates the btree_gist and pgcrypto extensions (btree_gist for the
ProviderSlot exclusion constraint per RESEARCH Pattern 3; pgcrypto for
gen_random_uuid() used as every table's UUID primary-key server default),
then every Phase-1 table with its FK chain, CHECK constraints, and indexes.
Downgrade drops everything in reverse dependency order plus both extensions
— upgrade/downgrade is symmetric (alembic downgrade base; upgrade head
returns to the identical schema).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Extensions -----------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    # --- clinics ----------------------------------------------------------
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
    )

    # --- departments (ORG-03 scope chain: departments -> clinics) --------
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
    )
    op.create_index("ix_departments_clinic_id", "departments", ["clinic_id"])

    # --- users (auth identity — separate from patients) ------------------
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
        sa.Column(
            "clinic_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("clinics.id"),
            nullable=True,
        ),
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

    # --- patients (operational profile; user_id nullable — PPL-01) -------
    op.create_table(
        "patients",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "clinic_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("clinics.id"),
            nullable=False,
        ),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("date_of_birth", sa.Date, nullable=False),
        sa.Column("phone", sa.String(32), nullable=True),
        sa.Column("address", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_patients_clinic_id", "patients", ["clinic_id"])

    # --- providers (ORG-03 scope chain root: providers -> departments) ---
    op.create_table(
        "providers",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "department_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("departments.id"),
            nullable=False,
        ),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_providers_department_id", "providers", ["department_id"])

    # --- specialties + provider_specialties (many-to-many, PPL-02) -------
    op.create_table(
        "specialties",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
    )
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

    # --- availability_templates (PPL-03; config only, no slot rows) ------
    op.create_table(
        "availability_templates",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "provider_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("providers.id"),
            nullable=False,
        ),
        sa.Column(
            "department_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("departments.id"),
            nullable=False,
        ),
        sa.Column("day_of_week", sa.SmallInteger, nullable=False),
        sa.Column("start_time", sa.Time, nullable=False),
        sa.Column("end_time", sa.Time, nullable=False),
        sa.Column(
            "slot_duration_minutes",
            sa.Integer,
            nullable=False,
            server_default=sa.text("30"),
        ),
        sa.Column("effective_from", sa.Date, nullable=False),
        sa.Column("effective_to", sa.Date, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "day_of_week BETWEEN 0 AND 6",
            name="ck_availability_templates_day_of_week",
        ),
    )
    op.create_index(
        "ix_availability_templates_provider_id", "availability_templates", ["provider_id"]
    )
    op.create_index(
        "ix_availability_templates_department_id", "availability_templates", ["department_id"]
    )

    # --- provider_slots — created EMPTY this phase; the double-booking ---
    # --- backstop (GiST exclusion constraint) is live immediately. -------
    op.create_table(
        "provider_slots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "provider_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("providers.id"),
            nullable=False,
        ),
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

    # The GiST exclusion constraint requires btree_gist (created above) to
    # combine a scalar-equality column (provider_id) with a range-overlap
    # operator (tstzrange &&) in one constraint — see RESEARCH Pattern 3.
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

    op.drop_index(
        "ix_availability_templates_department_id", table_name="availability_templates"
    )
    op.drop_index("ix_availability_templates_provider_id", table_name="availability_templates")
    op.drop_table("availability_templates")

    op.drop_table("provider_specialties")
    op.drop_table("specialties")

    op.drop_index("ix_providers_department_id", table_name="providers")
    op.drop_table("providers")

    op.drop_index("ix_patients_clinic_id", table_name="patients")
    op.drop_table("patients")

    op.drop_index("ix_users_clinic_id", table_name="users")
    op.drop_table("users")

    op.drop_index("ix_departments_clinic_id", table_name="departments")
    op.drop_table("departments")

    op.drop_table("clinics")

    op.execute("DROP EXTENSION IF EXISTS btree_gist")
    op.execute("DROP EXTENSION IF EXISTS pgcrypto")
