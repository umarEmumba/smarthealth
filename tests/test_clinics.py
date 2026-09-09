"""Clinic CRUD integration tests (ORG-01) — admin-only, audited,
read-after-write consistency.

Exercises the real app end-to-end (`client` fixture), the same pattern
`tests/test_rbac.py` established in Plan 03.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient

from smarthealth.config import settings


def _unique_email(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}@example.com"


async def _register_and_login(client: AsyncClient, *, role: str) -> dict:
    email = _unique_email(role)
    register_resp = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "correct-horse-battery", "role": role},
    )
    assert register_resp.status_code == 201, register_resp.text

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    return login_resp.json()


def _auth_header(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


@pytest.mark.integration
async def test_create_clinic(client: AsyncClient) -> None:
    """An admin can create a clinic with a required timezone (ORG-01)."""
    tokens = await _register_and_login(client, role="admin")
    resp = await client.post(
        "/api/clinics",
        json={"name": "Downtown Clinic", "timezone": "America/New_York"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "Downtown Clinic"
    assert body["timezone"] == "America/New_York"
    assert "id" in body


@pytest.mark.integration
async def test_read_after_write(client: AsyncClient) -> None:
    """Creating a clinic and immediately reading it back returns the
    just-committed row — single Postgres system of record, no stale cache.
    """
    tokens = await _register_and_login(client, role="admin")
    create_resp = await client.post(
        "/api/clinics",
        json={"name": "Riverside Clinic", "timezone": "America/Chicago"},
        headers=_auth_header(tokens),
    )
    assert create_resp.status_code == 201, create_resp.text
    clinic_id = create_resp.json()["id"]

    get_resp = await client.get(f"/api/clinics/{clinic_id}", headers=_auth_header(tokens))
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json() == {
        "id": clinic_id,
        "name": "Riverside Clinic",
        "timezone": "America/Chicago",
    }

    list_resp = await client.get("/api/clinics", headers=_auth_header(tokens))
    assert list_resp.status_code == 200
    assert any(c["id"] == clinic_id for c in list_resp.json())


@pytest.mark.integration
async def test_non_admin_forbidden(client: AsyncClient) -> None:
    """A front-desk token creating a clinic returns 403 (admin-only, ORG-01)."""
    tokens = await _register_and_login(client, role="front_desk")
    resp = await client.post(
        "/api/clinics",
        json={"name": "Should Not Exist", "timezone": "America/Denver"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_create_clinic_audited(client: AsyncClient, mongo_client: AsyncMongoClient) -> None:
    """A clinic.create audit entry exists after a successful creation."""
    tokens = await _register_and_login(client, role="admin")
    resp = await client.post(
        "/api/clinics",
        json={"name": "Audited Clinic", "timezone": "America/Los_Angeles"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    clinic_id = resp.json()["id"]

    collection = mongo_client[settings.mongo_db_name]["audit_log"]
    doc = await collection.find_one({"action": "clinic.create", "entity_id": clinic_id})
    assert doc is not None
    assert doc["outcome"] == "success"
    assert doc["entity_type"] == "clinic"
