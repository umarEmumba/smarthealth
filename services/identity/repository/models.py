"""Identity's ORM models — accounts ONLY (D-01, D-04).

Identity owns exactly one table, `users` (credentials, role, `clinic_id`). It
holds no patient or provider profile data: PHI never enters Identity, and
credentials never enter the Patient/Provider services (they link to an
account via a plain `user_id` UUID).

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables (RESEARCH Pitfall 2).

`users.clinic_id` is a plain nullable UUID with NO foreign key — the clinic
lives in the Organization service's database. Validating it against
Organization at write time is delivered by plan 01.1-05 (D-05).

Async-only per CLAUDE.md; no sync driver or engine is imported here.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class IdentityBase(DeclarativeBase):
    """Declarative base for Identity's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call (a Column instance can belong
    to only one Table, so it cannot be shared across declarative classes)."""
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


class User(IdentityBase):
    """Auth/login identity (IAM-01/IAM-02/IAM-03).

    A `User` exists for everyone who can log in: patients who self-register,
    providers, front-desk staff, and admins. Patient/provider PROFILES are
    separate records in their own services and need not have a login.
    """

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    # Nullable: admins may be clinic-unscoped; patients/front-desk/provider
    # users are scoped to the clinic they operate at or registered through.
    # Plain UUID, no FK — the clinic belongs to the Organization service.
    clinic_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('patient','provider','front_desk','admin')",
            name="ck_users_role_valid",
        ),
    )
