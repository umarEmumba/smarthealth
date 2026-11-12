"""Patient's ORM models: `patients` ONLY (D-01, D-04).

Patient is the ONLY service whose schema holds patient PHI (`full_name`,
`date_of_birth`, `phone`, `address`). `clinic_id` (owned by Organization) and
`user_id` (owned by Identity) are plain UUID columns with NO foreign key: the
rows they point at live in other services' databases, so both are validated at
write time through the shared reference client (D-05) and never cascade (D-06).

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables (RESEARCH Pitfall 2). Async-only per CLAUDE.md; no sync driver
or engine is imported here.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class PatientBase(DeclarativeBase):
    """Declarative base for Patient's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call (a Column instance can belong
    to only one Table, so it cannot be shared across declarative classes)."""
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


class Patient(PatientBase):
    """Operational patient profile (PPL-01/PPL-04).

    `user_id` is NULLABLE — a front-desk/admin-created profile need not have a
    login account, and creating a profile never creates one (D-04). When set it
    is a plain UUID pointing at an Identity account, validated at write time.

    PHI note: `full_name`, `date_of_birth`, `phone`, and `address` are
    PHI-bearing columns. They live in this service's Postgres database (system
    of record) but MUST NEVER be written to logs, traces, internal-call URLs,
    headers or bodies, another service's database, or the Mongo audit `changes`
    field — only field NAMES and identifiers ever leave this service.
    """

    __tablename__ = "patients"

    id: Mapped[uuid.UUID] = _uuid_pk()
    # Plain UUIDs, no foreign keys: the account is Identity's, the clinic is Organization's.
    user_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    clinic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), nullable=False, index=True
    )
    # --- PHI-bearing operational profile fields ---
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    date_of_birth: Mapped[date] = mapped_column(Date, nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # One profile per login account (names match migration patient_0001).
    __table_args__ = (UniqueConstraint("user_id", name="uq_patients_user_id"),)
