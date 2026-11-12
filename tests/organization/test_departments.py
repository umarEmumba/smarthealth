"""Department CRUD integration tests (ORG-02/03) — admin-only, audited,
clinic-scoped, referential integrity against a non-existent clinic.
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


async def _create_clinic(client: AsyncClient, tokens: dict, *, name: str = "Test Clinic") -> str:
    resp = await client.post(
        "/api/clinics",
        json={"name": name, "timezone": "America/New_York"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_create_department(client: AsyncClient) -> None:
    """An admin can create a department under an existing clinic (ORG-02)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens)

    resp = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "Cardiology"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "Cardiology"
    assert body["clinic_id"] == clinic_id
    assert "id" in body


@pytest.mark.integration
async def test_department_under_missing_clinic_404(client: AsyncClient) -> None:
    """Creating a department under a non-existent clinic id returns 404 and
    writes no orphan row (T-04-04).
    """
    tokens = await _register_and_login(client, role="admin")
    missing_clinic_id = str(uuid.uuid4())

    resp = await client.post(
        f"/api/clinics/{missing_clinic_id}/departments",
        json={"name": "Orphan Department"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 404

    # The missing clinic's department list also 404s — no orphan row exists
    # to be returned by a differently-scoped read either.
    list_resp = await client.get(
        f"/api/clinics/{missing_clinic_id}/departments", headers=_auth_header(tokens)
    )
    assert list_resp.status_code == 404


@pytest.mark.integration
async def test_list_departments_empty(client: AsyncClient) -> None:
    """A clinic with no departments returns an empty list, not an error."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Empty Clinic")

    resp = await client.get(f"/api/clinics/{clinic_id}/departments", headers=_auth_header(tokens))
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.integration
async def test_department_scoped_to_clinic(client: AsyncClient) -> None:
    """A department lists only under its own clinic — ORG-03 scoping."""
    tokens = await _register_and_login(client, role="admin")
    clinic_a = await _create_clinic(client, tokens, name="Clinic A")
    clinic_b = await _create_clinic(client, tokens, name="Clinic B")

    create_resp = await client.post(
        f"/api/clinics/{clinic_a}/departments",
        json={"name": "Radiology"},
        headers=_auth_header(tokens),
    )
    assert create_resp.status_code == 201, create_resp.text
    department_id = create_resp.json()["id"]

    a_list = await client.get(f"/api/clinics/{clinic_a}/departments", headers=_auth_header(tokens))
    assert a_list.status_code == 200
    assert any(d["id"] == department_id for d in a_list.json())

    b_list = await client.get(f"/api/clinics/{clinic_b}/departments", headers=_auth_header(tokens))
    assert b_list.status_code == 200
    assert all(d["id"] != department_id for d in b_list.json())


@pytest.mark.integration
async def test_duplicate_department_name_defined_outcome(client: AsyncClient) -> None:
    """Two departments with the same name under the same clinic have a
    deterministic outcome: both are allowed to exist with distinct ids
    (no uniqueness constraint on department name is declared in the
    Phase-1 schema — see models.py — so this test locks in that the
    absence of a constraint is an intentional, asserted choice, not an
    untested gap).
    """
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Duplicate-Name Clinic")

    first = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "Pediatrics"},
        headers=_auth_header(tokens),
    )
    assert first.status_code == 201, first.text

    second = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "Pediatrics"},
        headers=_auth_header(tokens),
    )
    assert second.status_code == 201, second.text
    assert first.json()["id"] != second.json()["id"]

    departments = await client.get(
        f"/api/clinics/{clinic_id}/departments", headers=_auth_header(tokens)
    )
    names = [d["name"] for d in departments.json()]
    assert names.count("Pediatrics") == 2


async def _create_department(
    client: AsyncClient, tokens: dict, clinic_id: str, *, name: str = "PATCH Test Department"
) -> str:
    resp = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": name},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_patch_department_full_and_partial(client: AsyncClient) -> None:
    """PATCH updates only supplied fields (gap-closure Plan 01-07)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department PATCH Clinic")
    department_id = await _create_department(client, tokens, clinic_id)

    resp = await client.patch(
        f"/api/clinics/{clinic_id}/departments/{department_id}",
        json={"name": "Renamed Department"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["name"] == "Renamed Department"


@pytest.mark.integration
async def test_patch_department_empty_body_422(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department Empty Body Clinic")
    department_id = await _create_department(client, tokens, clinic_id)

    resp = await client.patch(
        f"/api/clinics/{clinic_id}/departments/{department_id}",
        json={},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 422


@pytest.mark.integration
async def test_patch_department_unknown_id_404(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department Unknown Clinic")

    resp = await client.patch(
        f"/api/clinics/{clinic_id}/departments/{uuid.uuid4()}",
        json={"name": "Ghost Department"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_department_wrong_clinic_scope_404(client: AsyncClient) -> None:
    """A department id from a different clinic is 404, not editable
    (clinic-scoped PATCH, mirrors create's scoping discipline)."""
    tokens = await _register_and_login(client, role="admin")
    clinic_a = await _create_clinic(client, tokens, name="Scope Clinic A PATCH")
    clinic_b = await _create_clinic(client, tokens, name="Scope Clinic B PATCH")
    department_id = await _create_department(client, tokens, clinic_a)

    resp = await client.patch(
        f"/api/clinics/{clinic_b}/departments/{department_id}",
        json={"name": "Cross-Clinic Rename"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_department_non_admin_forbidden(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens, name="Department RBAC Clinic")
    department_id = await _create_department(client, admin_tokens, clinic_id)

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.patch(
        f"/api/clinics/{clinic_id}/departments/{department_id}",
        json={"name": "Should Not Change"},
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_deactivate_department_hides_from_default_list(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department Deactivate Clinic")
    department_id = await _create_department(client, tokens, clinic_id)

    deactivate_resp = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate",
        headers=_auth_header(tokens),
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text
    assert deactivate_resp.json()["is_active"] is False
    assert deactivate_resp.json()["deactivated_at"] is not None

    default_list = await client.get(
        f"/api/clinics/{clinic_id}/departments", headers=_auth_header(tokens)
    )
    assert all(d["id"] != department_id for d in default_list.json())

    inactive_list = await client.get(
        f"/api/clinics/{clinic_id}/departments",
        params={"include_inactive": "true"},
        headers=_auth_header(tokens),
    )
    assert any(d["id"] == department_id for d in inactive_list.json())


@pytest.mark.integration
async def test_deactivate_department_idempotent(client: AsyncClient) -> None:
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department Idempotent Clinic")
    department_id = await _create_department(client, tokens, clinic_id)

    first = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate",
        headers=_auth_header(tokens),
    )
    assert first.status_code == 200

    second = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate",
        headers=_auth_header(tokens),
    )
    assert second.status_code == 200
    assert second.json()["is_active"] is False
    # No-op success: the row comes back unchanged (deactivated_at not re-stamped).
    assert second.json() == first.json()


@pytest.mark.integration
async def test_department_update_and_deactivate_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, tokens, name="Department Audited Clinic")
    department_id = await _create_department(client, tokens, clinic_id)

    patch_resp = await client.patch(
        f"/api/clinics/{clinic_id}/departments/{department_id}",
        json={"name": "Audited Renamed Department"},
        headers=_auth_header(tokens),
    )
    assert patch_resp.status_code == 200, patch_resp.text

    deactivate_resp = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate",
        headers=_auth_header(tokens),
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text

    collection = mongo_client[shared_settings.mongo_db_name]["audit_log"]
    update_doc = await collection.find_one(
        {"action": "department.update", "entity_id": department_id}
    )
    assert update_doc is not None
    assert update_doc["outcome"] == "success"

    deactivate_doc = await collection.find_one(
        {"action": "department.deactivate", "entity_id": department_id}
    )
    assert deactivate_doc is not None
    assert deactivate_doc["outcome"] == "success"
