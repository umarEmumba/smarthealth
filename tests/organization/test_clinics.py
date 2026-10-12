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

from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login, unique_email


async def _register_and_login(client: AsyncClient, *, role: str) -> dict:
    """Create a user with `role` and return its login token pair.

    CR-01: public `/api/auth/register` always creates a PATIENT account
    now, so a `role="patient"` request goes straight through it; any
    other role is created via the authenticated-admin-only
    `/api/auth/admin/users` route, using a throwaway bootstrap admin.
    """
    email = unique_email(role)
    if role == "patient":
        register_resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert register_resp.status_code == 201, register_resp.text
    else:
        admin_tokens = await bootstrap_admin_and_login(client)
        create_resp = await client.post(
            "/api/auth/admin/users",
            json={"email": email, "password": "correct-horse-battery", "role": role},
            headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
        )
        assert create_resp.status_code == 201, create_resp.text

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
        "is_active": True,
        "deactivated_at": None,
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

    collection = mongo_client[shared_settings.mongo_db_name]["audit_log"]
    doc = await collection.find_one({"action": "clinic.create", "entity_id": clinic_id})
    assert doc is not None
    assert doc["outcome"] == "success"
    assert doc["entity_type"] == "clinic"


async def _create_clinic(client: AsyncClient, tokens: dict, *, name: str = "PATCH Test Clinic") -> str:
    resp = await client.post(
        "/api/clinics",
        json={"name": name, "timezone": "America/New_York"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_patch_clinic_full_and_partial(client: AsyncClient) -> None:
    """PATCH with both fields updates both; PATCH with one field leaves the
    other untouched (partial update — gap-closure Plan 01-07)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens)

    full_resp = await client.patch(
        f"/api/clinics/{clinic_id}",
        json={"name": "Renamed Clinic", "timezone": "America/Denver"},
        headers=_auth_header(tokens),
    )
    assert full_resp.status_code == 200, full_resp.text
    assert full_resp.json()["name"] == "Renamed Clinic"
    assert full_resp.json()["timezone"] == "America/Denver"

    partial_resp = await client.patch(
        f"/api/clinics/{clinic_id}",
        json={"name": "Renamed Again"},
        headers=_auth_header(tokens),
    )
    assert partial_resp.status_code == 200, partial_resp.text
    assert partial_resp.json()["name"] == "Renamed Again"
    assert partial_resp.json()["timezone"] == "America/Denver"  # untouched


@pytest.mark.integration
async def test_patch_clinic_empty_body_422(client: AsyncClient) -> None:
    """An empty PATCH body is rejected 422 (must_haves backstop)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Empty Body Clinic")

    resp = await client.patch(f"/api/clinics/{clinic_id}", json={}, headers=_auth_header(tokens))
    assert resp.status_code == 422


@pytest.mark.integration
async def test_patch_clinic_unknown_id_404(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    resp = await client.patch(
        f"/api/clinics/{uuid.uuid4()}",
        json={"name": "Ghost Clinic"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_clinic_non_admin_forbidden(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens, name="RBAC Clinic")

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.patch(
        f"/api/clinics/{clinic_id}",
        json={"name": "Should Not Change"},
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_deactivate_clinic_hides_from_default_list(client: AsyncClient) -> None:
    """Deactivating a clinic sets is_active=false, hides it from the default
    list, keeps it visible with ?include_inactive=true, and get-by-id still
    returns it (with its status visible)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Deactivate Me Clinic")

    deactivate_resp = await client.post(
        f"/api/clinics/{clinic_id}/deactivate", headers=_auth_header(tokens)
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text
    body = deactivate_resp.json()
    assert body["is_active"] is False
    assert body["deactivated_at"] is not None

    default_list = await client.get("/api/clinics", headers=_auth_header(tokens))
    assert all(c["id"] != clinic_id for c in default_list.json())

    inactive_list = await client.get(
        "/api/clinics", params={"include_inactive": "true"}, headers=_auth_header(tokens)
    )
    assert any(c["id"] == clinic_id for c in inactive_list.json())

    get_resp = await client.get(f"/api/clinics/{clinic_id}", headers=_auth_header(tokens))
    assert get_resp.status_code == 200
    assert get_resp.json()["is_active"] is False


@pytest.mark.integration
async def test_deactivate_clinic_idempotent(client: AsyncClient) -> None:
    """Deactivating an already-inactive clinic is a no-op success."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Idempotent Deactivate Clinic")

    first = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth_header(tokens))
    assert first.status_code == 200

    second = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth_header(tokens))
    assert second.status_code == 200
    assert second.json()["is_active"] is False
    # No-op success: the row comes back unchanged (deactivated_at not re-stamped).
    assert second.json() == first.json()


@pytest.mark.integration
async def test_deactivate_clinic_unknown_id_404(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    resp = await client.post(
        f"/api/clinics/{uuid.uuid4()}/deactivate", headers=_auth_header(tokens)
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_clinic_update_and_deactivate_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    """Both PATCH and deactivate write audit_log entries (clinic.update,
    clinic.deactivate)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Audited Update Clinic")

    patch_resp = await client.patch(
        f"/api/clinics/{clinic_id}", json={"name": "Audited Renamed"}, headers=_auth_header(tokens)
    )
    assert patch_resp.status_code == 200, patch_resp.text

    deactivate_resp = await client.post(
        f"/api/clinics/{clinic_id}/deactivate", headers=_auth_header(tokens)
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text

    collection = mongo_client[shared_settings.mongo_db_name]["audit_log"]
    update_doc = await collection.find_one({"action": "clinic.update", "entity_id": clinic_id})
    assert update_doc is not None
    assert update_doc["outcome"] == "success"

    deactivate_doc = await collection.find_one(
        {"action": "clinic.deactivate", "entity_id": clinic_id}
    )
    assert deactivate_doc is not None
    assert deactivate_doc["outcome"] == "success"
