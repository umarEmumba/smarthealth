"""Tracer for plan 01.1-03: client -> gateway -> {Identity, Organization}.

One thin vertical slice proving the whole new edge works before any
expansion: an admin logs in (gateway routes /api/auth to Identity), creates a
clinic and reads it back (gateway routes /api/clinics to Organization),
Organization verifies the RS256 token LOCALLY from a cached JWKS, persists to
its OWN database (organization_db) and writes an audit document.

Service-module imports are deliberately done inside the tests so a missing
module fails the target test (not collection of the whole file).
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login, harness_apps, unique_email

PASSWORD = "correct-horse-battery"


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _staff_tokens(client: AsyncClient, *, role: str) -> dict:
    """Create a non-admin staff account through the gateway and log it in."""
    admin_tokens = await bootstrap_admin_and_login(client)
    email = unique_email(role)
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": role},
        headers=_auth(admin_tokens),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


@pytest.mark.integration
async def test_admin_creates_and_reads_clinic_through_gateway(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    from services.organization.config import settings as org_settings  # noqa: PLC0415

    tokens = await bootstrap_admin_and_login(client)

    created = await client.post(
        "/api/clinics",
        json={"name": "Tracer Clinic", "timezone": "Europe/London"},
        headers=_auth(tokens),
    )
    assert created.status_code == 201, created.text
    clinic = created.json()

    fetched = await client.get(f"/api/clinics/{clinic['id']}", headers=_auth(tokens))
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == clinic

    # The row lives in organization_db (checked with an independent engine).
    engine = create_async_engine(org_settings.database_url)
    try:
        async with engine.connect() as conn:
            found = await conn.scalar(
                text("SELECT count(*) FROM clinics WHERE id = :id"), {"id": clinic["id"]}
            )
    finally:
        await engine.dispose()
    assert found == 1

    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    doc = await audit.find_one({"action": "clinic.create", "entity_id": clinic["id"]})
    assert doc is not None
    assert doc["outcome"] == "success"


@pytest.mark.integration
async def test_organization_verifies_locally_from_cached_jwks(client: AsyncClient) -> None:
    resolver = harness_apps()["organization"].state.key_resolver
    tokens = await bootstrap_admin_and_login(client)

    before = resolver.fetch_count

    first = await client.get("/api/clinics", headers=_auth(tokens))
    assert first.status_code == 200, first.text
    fetches_after_first = resolver.fetch_count

    second = await client.get("/api/clinics", headers=_auth(tokens))
    assert second.status_code == 200, second.text

    # Verification is local: at most ONE JWKS fetch across both requests (the
    # cold-cache one), and the second request never refetches.
    assert resolver.fetch_count - before <= 1
    assert resolver.fetch_count == fetches_after_first


@pytest.mark.integration
async def test_front_desk_forbidden_on_clinics_through_gateway(client: AsyncClient) -> None:
    tokens = await _staff_tokens(client, role="front_desk")
    resp = await client.post(
        "/api/clinics",
        json={"name": "Should Not Exist", "timezone": "America/Denver"},
        headers=_auth(tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_identity_routes_still_work_through_gateway(client: AsyncClient) -> None:
    resp = await client.get("/.well-known/jwks.json")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json()["keys"], list)
    assert resp.json()["keys"]
