"""Unit tests for Provider's data model (relocated from the Phase 1 monolith).

Pure metadata assertions — no live database needed.
"""

from __future__ import annotations

from services.provider.repository.models import AvailabilityTemplate, ProviderBase


def test_availability_template_day_of_week_check_declared() -> None:
    """The day_of_week CHECK (0-6) is declared on the ORM model.

    Asserts the CheckConstraint exists in AvailabilityTemplate.__table_args__ so
    PPL-03's day-of-week domain (Monday=0..Sunday=6) is enforced at the schema
    level, not left to application-layer trust alone.
    """
    check_constraints = [
        c for c in AvailabilityTemplate.__table__.constraints if hasattr(c, "sqltext")
    ]
    assert any(
        "day_of_week" in str(c.sqltext) and "0" in str(c.sqltext) and "6" in str(c.sqltext)
        for c in check_constraints
    ), "AvailabilityTemplate must declare a day_of_week BETWEEN 0 AND 6 CHECK constraint"


def test_provider_schema_has_no_cross_service_foreign_keys_and_no_slots_table() -> None:
    """D-01: references to other services' rows are plain UUIDs; only the
    intra-service links keep foreign keys. No concrete-slot table lives here."""
    tables = ProviderBase.metadata.tables
    assert set(tables) == {
        "providers",
        "specialties",
        "provider_specialties",
        "availability_templates",
        "schedule_change_events",  # 02.1-05: Provider's own outbox, plain-UUID columns only
    }

    targets = {fk.target_fullname.split(".")[0] for t in tables.values() for fk in t.foreign_keys}
    assert targets <= {"providers", "specialties"}

    for table, column in (
        ("providers", "department_id"),
        ("providers", "user_id"),
        ("availability_templates", "department_id"),
        ("schedule_change_events", "provider_id"),
        ("schedule_change_events", "template_id"),
    ):
        assert not tables[table].c[column].foreign_keys, f"{table}.{column} must be a plain UUID"
    assert tables["specialties"].c["name"].unique, "specialties.name UNIQUE is the creation-race backstop"
