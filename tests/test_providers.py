"""Provider CRUD integration tests (PPL-02, ORG-03) — admin-only,
audited, department-scoped, specialties.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient

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
async def test_provider_under_missing_department_404(client: AsyncClient) -> None:
    """Creating a provider under a non-existent department id returns 404
    with no orphan row, mirroring `org_repo.create_department`'s pattern.
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
    assert resp.status_code == 404


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
