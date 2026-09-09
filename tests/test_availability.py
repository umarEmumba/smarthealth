"""Availability-template integration tests (PPL-03) — provider/admin,
validated windows, RESEARCH Decision 2 (templates only, no concrete
slots generated this phase).
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.identity.repository.models import ProviderSlot
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


async def _create_clinic_department_provider(
    client: AsyncClient, admin_tokens: dict, *, clinic_name: str
) -> tuple[str, str, str]:
    clinic_resp = await client.post(
        "/api/clinics",
        json={"name": clinic_name, "timezone": "America/New_York"},
        headers=_auth_header(admin_tokens),
    )
    assert clinic_resp.status_code == 201, clinic_resp.text
    clinic_id = clinic_resp.json()["id"]

    dept_resp = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "General"},
        headers=_auth_header(admin_tokens),
    )
    assert dept_resp.status_code == 201, dept_resp.text
    department_id = dept_resp.json()["id"]

    provider_resp = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. Availability Testperson",
            "specialties": ["General Medicine"],
        },
        headers=_auth_header(admin_tokens),
    )
    assert provider_resp.status_code == 201, provider_resp.text
    provider_id = provider_resp.json()["id"]

    return clinic_id, department_id, provider_id


def _valid_template_payload() -> dict:
    return {
        "day_of_week": 0,
        "start_time": "09:00:00",
        "end_time": "17:00:00",
        "slot_duration_minutes": 30,
        "effective_from": "2026-01-05",
        "effective_to": "2026-12-31",
    }


@pytest.mark.integration
async def test_create_availability_template(client: AsyncClient) -> None:
    """Provider/admin creates a valid weekly availability template (PPL-03)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Clinic"
    )

    resp = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_valid_template_payload(),
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["provider_id"] == provider_id
    assert body["department_id"] == department_id
    assert body["day_of_week"] == 0

    list_resp = await client.get(
        f"/api/providers/{provider_id}/availability", headers=_auth_header(admin_tokens)
    )
    assert list_resp.status_code == 200
    assert any(t["id"] == body["id"] for t in list_resp.json())


@pytest.mark.integration
async def test_invalid_window_rejected(client: AsyncClient) -> None:
    """end_time<=start_time and day_of_week=7 are both rejected 422."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Invalid Window Clinic"
    )

    end_before_start = _valid_template_payload()
    end_before_start["start_time"] = "17:00:00"
    end_before_start["end_time"] = "09:00:00"
    resp1 = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=end_before_start,
        headers=_auth_header(admin_tokens),
    )
    assert resp1.status_code == 422

    bad_day = _valid_template_payload()
    bad_day["day_of_week"] = 7
    resp2 = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=bad_day,
        headers=_auth_header(admin_tokens),
    )
    assert resp2.status_code == 422


@pytest.mark.integration
async def test_no_concrete_slots_generated(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """After creating an availability template, provider_slots remains
    empty — RESEARCH Decision 2 (templates only, no slot generation this
    phase).
    """
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="No-Slots Clinic"
    )

    resp = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_valid_template_payload(),
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 201, resp.text

    result = await db_session.execute(
        select(ProviderSlot).where(ProviderSlot.provider_id == uuid.UUID(provider_id))
    )
    assert result.scalars().all() == []


@pytest.mark.integration
async def test_availability_forbidden_for_front_desk(client: AsyncClient) -> None:
    """A front-desk token cannot configure availability (provider/admin only)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Forbidden Availability Clinic"
    )

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_valid_template_payload(),
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403
