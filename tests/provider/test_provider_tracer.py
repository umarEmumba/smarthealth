"""Tracer for plan 01.1-07: client -> gateway -> Provider (-> Organization).

One thin vertical slice proving the Provider service works before any expansion:
an admin registers a provider under a department through the gateway, Provider
validates the department with Organization (D-05, forwarding the caller's own
token), writes only to its OWN database (provider_db) and answers with id-only
references (D-07). A department Organization does not know is a 422 (the Phase 1
404 is superseded by D-05) and leaves no row behind.

Service-module imports are done inside the tests so a missing module fails the
target test, not collection of the whole file. All names are synthetic.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _clinic_and_department(client: AsyncClient, admin: dict) -> tuple[str, str]:
    clinic = await client.post(
        "/api/clinics",
        json={"name": "Provider Tracer Clinic", "timezone": "UTC"},
        headers=_auth(admin),
    )
    assert clinic.status_code == 201, clinic.text
    clinic_id = clinic.json()["id"]
    department = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "Tracer Cardiology"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    return clinic_id, department.json()["id"]


async def _provider_rows(where: str, params: dict) -> int:
    """Committed-state count straight against provider_db (own short-lived engine)."""
    from services.provider.config import settings as provider_settings  # noqa: PLC0415

    engine = create_async_engine(provider_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(text(f"SELECT count(*) FROM providers WHERE {where}"), params)
    finally:
        await engine.dispose()


async def test_admin_registers_provider_through_gateway(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin)

    created = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. Tracer Provider",
            "specialties": ["Tracer Cardiology", "Tracer Internal Medicine"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    provider = created.json()
    assert provider["department_id"] == department_id
    assert {s["name"] for s in provider["specialties"]} == {
        "Tracer Cardiology",
        "Tracer Internal Medicine",
    }

    fetched = await client.get(f"/api/providers/{provider['id']}", headers=_auth(admin))
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == provider

    # The row lives in provider_db (checked with an independent engine).
    assert await _provider_rows("id = :id", {"id": provider["id"]}) == 1


async def test_provider_under_unknown_department_rejected_422(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    full_name = f"Dr. Orphan {uuid.uuid4().hex[:8]}"

    resp = await client.post(
        "/api/providers",
        json={
            "department_id": str(uuid.uuid4()),
            "full_name": full_name,
            "specialties": ["Tracer General"],
        },
        headers=_auth(admin),
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "department not found"

    # No half-created provider: nothing was written under that name.
    assert await _provider_rows("full_name = :n", {"n": full_name}) == 0

    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    await audit.delete_many({"action": "provider.create", "entity_id": "unknown", "outcome": "failure"})


async def test_provider_response_is_id_only(client: AsyncClient) -> None:
    """D-07: a provider body carries the department as a bare id and nothing
    fetched from another service (no department name, no clinic data)."""
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin)

    created = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. Id Only",
            "specialties": ["Tracer General"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    assert set(created.json()) == {
        "id",
        "department_id",
        "full_name",
        "specialties",
        "user_id",
        "is_active",
        "deactivated_at",
    }
