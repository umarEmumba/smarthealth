"""Organization's ORM models: `clinics` and `departments` ONLY (D-01).

`departments.clinic_id` keeps its foreign key to `clinics.id` because BOTH
tables belong to this service and live in the same database — the foreign key
is intra-service and still the real integrity guard (a department can never be
orphaned, even by a create racing its clinic's deactivation).

Every reference from OTHER services to these rows (users.clinic_id,
patients.clinic_id, providers.department_id, ...) is a plain UUID with no
foreign key in their own databases; those are validated at write time through
this service's `/internal` lookups (D-05) and never cascade (D-06).

Each service declares its OWN `DeclarativeBase` (never a shared one): a shared
`MetaData` would make every service's Alembic autogenerate see every other
service's tables (RESEARCH Pitfall 2). Async-only per CLAUDE.md; no sync
driver or engine is imported here.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class OrganizationBase(DeclarativeBase):
    """Declarative base for Organization's tables only."""


def _uuid_pk() -> Mapped[uuid.UUID]:
    """A fresh UUID primary-key column per call (a Column instance can belong
    to only one Table, so it cannot be shared across declarative classes)."""
    return mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )


class Clinic(OrganizationBase):
    """A physical/operational clinic location (ORG-01).

    `timezone` is a required IANA tz-database string (e.g. "America/New_York"):
    without a clinic-level timezone, naive availability TIME values are
    ambiguous and silently corrupt multi-region slot generation (RESEARCH
    Pitfall 3 of Phase 1).
    """

    __tablename__ = "clinics"

    id: Mapped[uuid.UUID] = _uuid_pk()
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    departments: Mapped[list["Department"]] = relationship(back_populates="clinic")


class Department(OrganizationBase):
    """A department within a clinic (ORG-02); part of the ORG-03 scope chain."""

    __tablename__ = "departments"

    id: Mapped[uuid.UUID] = _uuid_pk()
    clinic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("clinics.id"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    clinic: Mapped[Clinic] = relationship(back_populates="departments")
