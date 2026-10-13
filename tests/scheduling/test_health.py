"""Scheduling skeleton: schema ownership, /health, and 'templates never
materialize slots' (D-03).

Scheduling exposes no `/api` routes this phase; it exists so the slot table and
its double-booking constraint already live in their final service and database.
"""

from __future__ import annotations

import uuid

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import bootstrap_admin_and_login, service_base_url


def test_scheduling_owns_its_tables_without_a_cross_service_foreign_key() -> None:
    from services.scheduling.repository.models import ProviderSlot, SchedulingBase  # noqa: PLC0415

    # Phase 2 (plan 02-02) adds the booking data layer alongside the slots.
    assert set(SchedulingBase.metadata.tables) == {
        "provider_slots",
        "appointments",
        "idempotency_keys",
        "appointment_status_history",
        "waitlist_entries",  # plan 02-09
    }
    assert not ProviderSlot.__table__.c.provider_id.foreign_keys
    # Only intra-service foreign keys: provider/patient/clinic ids are plain UUIDs.
    for table in SchedulingBase.metadata.tables.values():
        for fk in table.foreign_keys:
            assert fk.column.table.name in SchedulingBase.metadata.tables, fk.target_fullname
    names = {c.name for c in ProviderSlot.__table__.constraints}
    assert "ex_provider_slots_no_overlap" in names
    assert "ck_provider_slots_status_valid" in names


@pytest.mark.integration
async def test_scheduling_health_through_the_mesh(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(f"{service_base_url('scheduling')}/health")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "ok", "service": "scheduling"}


@pytest.mark.integration
async def test_scheduling_exposes_no_api_routes_this_phase() -> None:
    from services.scheduling.app import app  # noqa: PLC0415

    # /health is answered from an included router; nothing may be mounted under /api.
    paths = {getattr(r, "path", "") for r in app.routes}
    assert not any(p.startswith("/api") for p in paths), paths


@pytest.mark.integration
async def test_availability_templates_never_materialize_slots(client: AsyncClient) -> None:
    """Creating availability in the Provider service writes templates only; the
    scheduling-side slot table stays empty for that provider this phase."""
    admin = await bootstrap_admin_and_login(client)
    headers = {"Authorization": f"Bearer {admin['access_token']}"}

    clinic = await client.post(
        "/api/clinics", json={"name": "Scheduling Empty-Slots Clinic", "timezone": "UTC"}, headers=headers
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments", json={"name": "General"}, headers=headers
    )
    assert department.status_code == 201, department.text
    provider = await client.post(
        "/api/providers",
        json={
            "department_id": department.json()["id"],
            "full_name": "Dr. Scheduling Emptycheck",
            "specialties": ["General Medicine"],
        },
        headers=headers,
    )
    assert provider.status_code == 201, provider.text
    provider_id = provider.json()["id"]

    availability = await client.post(
        f"/api/providers/{provider_id}/availability",
        json={
            "day_of_week": 1,
            "start_time": "09:00:00",
            "end_time": "12:00:00",
            "slot_duration_minutes": 30,
            "effective_from": "2026-01-05",
            "effective_to": "2026-12-31",
        },
        headers=headers,
    )
    assert availability.status_code == 201, availability.text

    from services.scheduling.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            slots = await conn.scalar(
                text("SELECT count(*) FROM provider_slots WHERE provider_id = :p"),
                {"p": uuid.UUID(provider_id)},
            )
    finally:
        await engine.dispose()
    assert slots == 0
