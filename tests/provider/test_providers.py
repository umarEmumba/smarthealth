"""Provider CRUD integration tests (PPL-02, ORG-03) — admin-only,
audited, department-scoped, specialties.
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


async def _create_clinic_and_department(
    client: AsyncClient, tokens: dict, *, clinic_name: str, department_name: str = "Cardiology"
) -> tuple[str, str]:
    clinic_resp = await client.post(
        "/api/clinics",
        json={"name": clinic_name, "timezone": "America/New_York"},
        headers=_auth_header(tokens),
    )
    assert clinic_resp.status_code == 201, clinic_resp.text
    clinic_id = clinic_resp.json()["id"]

    dept_resp = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": department_name},
        headers=_auth_header(tokens),
    )
    assert dept_resp.status_code == 201, dept_resp.text
    return clinic_id, dept_resp.json()["id"]


@pytest.mark.integration
async def test_create_provider_with_specialties(client: AsyncClient) -> None:
    """Admin creates a provider with >=1 specialty (PPL-02)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider Test Clinic"
    )

    resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. Synthetic Testperson",
            "specialties": ["Cardiology", "Internal Medicine"],
        },
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["department_id"] == department_id
    specialty_names = {s["name"] for s in body["specialties"]}
    assert specialty_names == {"Cardiology", "Internal Medicine"}


@pytest.mark.integration
async def test_provider_zero_specialties_defined_outcome(client: AsyncClient) -> None:
    """Creating a provider with zero specialties has a defined, deterministic
    outcome: rejected 422 by the Pydantic min_length=1 constraint on
    `specialties` — the must_haves backstop for the empty-list edge case.
    """
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Zero-Specialty Clinic"
    )

    resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. No Specialty",
            "specialties": [],
        },
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 422


@pytest.mark.integration
async def test_provider_scoped_to_department(client: AsyncClient) -> None:
    """A provider lists only under its own department — ORG-03 scoping."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_a, department_a = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Scope Clinic A", department_name="Neurology"
    )
    _clinic_b, department_b = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Scope Clinic B", department_name="Oncology"
    )

    create_resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_a,
            "full_name": "Dr. Scoped Provider",
            "specialties": ["Neurology"],
        },
        headers=_auth_header(admin_tokens),
    )
    assert create_resp.status_code == 201, create_resp.text
    provider_id = create_resp.json()["id"]

    a_list = await client.get(
        "/api/providers", params={"department_id": department_a}, headers=_auth_header(admin_tokens)
    )
    assert a_list.status_code == 200
    assert any(p["id"] == provider_id for p in a_list.json())

    b_list = await client.get(
        "/api/providers", params={"department_id": department_b}, headers=_auth_header(admin_tokens)
    )
    assert b_list.status_code == 200
    assert all(p["id"] != provider_id for p in b_list.json())


@pytest.mark.integration
async def test_provider_under_missing_department_422(client: AsyncClient) -> None:
    """Creating a provider under a department Organization does not know is a
    422 "department not found" with no orphan row.

    Phase 1 answered 404 here because the department lived in the same database;
    per locked decision D-05 the department is now owned by another service and is
    validated synchronously at write time, so a missing reference is a 422 (a
    reference problem in the request body), not a 404 on this resource.
    """
    admin_tokens = await _register_and_login(client, role="admin")
    missing_department_id = str(uuid.uuid4())

    resp = await client.post(
        "/api/providers",
        json={
            "department_id": missing_department_id,
            "full_name": "Dr. Orphan",
            "specialties": ["General"],
        },
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "department not found"


@pytest.mark.integration
async def test_non_admin_forbidden(client: AsyncClient) -> None:
    """A front-desk token registering a provider returns 403 (admin-only)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Non-Admin Forbidden Clinic"
    )

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Should Not Be Created",
            "specialties": ["General"],
        },
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403


async def _create_provider(
    client: AsyncClient, admin_tokens: dict, department_id: str, *, full_name: str = "Dr. PATCH Testperson"
) -> str:
    resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": full_name,
            "specialties": ["General Medicine"],
        },
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_patch_provider_full_and_partial(client: AsyncClient) -> None:
    """PATCH updates only supplied fields; specialties REPLACES the full
    set when supplied (gap-closure Plan 01-07)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider PATCH Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    name_resp = await client.patch(
        f"/api/providers/{provider_id}",
        json={"full_name": "Dr. Renamed"},
        headers=_auth_header(admin_tokens),
    )
    assert name_resp.status_code == 200, name_resp.text
    assert name_resp.json()["full_name"] == "Dr. Renamed"
    specialty_names = {s["name"] for s in name_resp.json()["specialties"]}
    assert specialty_names == {"General Medicine"}  # untouched

    specialty_resp = await client.patch(
        f"/api/providers/{provider_id}",
        json={"specialties": ["Cardiology"]},
        headers=_auth_header(admin_tokens),
    )
    assert specialty_resp.status_code == 200, specialty_resp.text
    new_specialty_names = {s["name"] for s in specialty_resp.json()["specialties"]}
    assert new_specialty_names == {"Cardiology"}  # replaced, not appended
    assert specialty_resp.json()["full_name"] == "Dr. Renamed"  # untouched


@pytest.mark.integration
async def test_patch_provider_empty_body_422(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider Empty Body Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    resp = await client.patch(
        f"/api/providers/{provider_id}", json={}, headers=_auth_header(admin_tokens)
    )
    assert resp.status_code == 422


@pytest.mark.integration
async def test_patch_provider_unknown_id_404(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    resp = await client.patch(
        f"/api/providers/{uuid.uuid4()}",
        json={"full_name": "Dr. Ghost"},
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_provider_non_admin_forbidden(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider RBAC PATCH Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.patch(
        f"/api/providers/{provider_id}",
        json={"full_name": "Should Not Change"},
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_deactivate_provider_hides_from_default_list(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider Deactivate Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    deactivate_resp = await client.post(
        f"/api/providers/{provider_id}/deactivate", headers=_auth_header(admin_tokens)
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text
    assert deactivate_resp.json()["is_active"] is False
    assert deactivate_resp.json()["deactivated_at"] is not None

    default_list = await client.get(
        "/api/providers", params={"department_id": department_id}, headers=_auth_header(admin_tokens)
    )
    assert all(p["id"] != provider_id for p in default_list.json())

    inactive_list = await client.get(
        "/api/providers",
        params={"department_id": department_id, "include_inactive": "true"},
        headers=_auth_header(admin_tokens),
    )
    assert any(p["id"] == provider_id for p in inactive_list.json())

    get_resp = await client.get(f"/api/providers/{provider_id}", headers=_auth_header(admin_tokens))
    assert get_resp.status_code == 200
    assert get_resp.json()["is_active"] is False


@pytest.mark.integration
async def test_deactivate_provider_idempotent(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider Idempotent Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    first = await client.post(
        f"/api/providers/{provider_id}/deactivate", headers=_auth_header(admin_tokens)
    )
    assert first.status_code == 200

    second = await client.post(
        f"/api/providers/{provider_id}/deactivate", headers=_auth_header(admin_tokens)
    )
    assert second.status_code == 200
    assert second.json()["is_active"] is False


@pytest.mark.integration
async def test_provider_update_and_deactivate_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id = await _create_clinic_and_department(
        client, admin_tokens, clinic_name="Provider Audited Clinic"
    )
    provider_id = await _create_provider(client, admin_tokens, department_id)

    patch_resp = await client.patch(
        f"/api/providers/{provider_id}",
        json={"full_name": "Dr. Audited Renamed"},
        headers=_auth_header(admin_tokens),
    )
    assert patch_resp.status_code == 200, patch_resp.text

    deactivate_resp = await client.post(
        f"/api/providers/{provider_id}/deactivate", headers=_auth_header(admin_tokens)
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text

    collection = mongo_client[shared_settings.mongo_db_name]["audit_log"]
    update_doc = await collection.find_one({"action": "provider.update", "entity_id": provider_id})
    assert update_doc is not None
    assert update_doc["outcome"] == "success"

    deactivate_doc = await collection.find_one(
        {"action": "provider.deactivate", "entity_id": provider_id}
    )
    assert deactivate_doc is not None
    assert deactivate_doc["outcome"] == "success"
