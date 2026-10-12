"""Availability-template integration tests (PPL-03) — provider/admin,
validated windows, RESEARCH Decision 2 (templates only, no concrete
slots generated this phase).
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login, unique_email, verify_identity_token


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
async def test_no_concrete_slots_generated(client: AsyncClient) -> None:
    """Availability configuration never writes concrete bookable slots —
    RESEARCH Decision 2 (templates only). Provider's schema and database contain
    no `provider_slots` table at all: concrete slots belong to Scheduling
    (emptiness on the scheduling side is checked by plan 01.1-08).
    """
    from services.provider.config import settings as provider_settings  # noqa: PLC0415
    from services.provider.repository.models import ProviderBase  # noqa: PLC0415

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

    assert "provider_slots" not in ProviderBase.metadata.tables

    engine = create_async_engine(provider_settings.database_url)
    try:
        async with engine.connect() as conn:
            regclass = await conn.scalar(text("select to_regclass('public.provider_slots')::text"))
    finally:
        await engine.dispose()
    assert regclass is None


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


def _sub_from_tokens(tokens: dict) -> str:
    return verify_identity_token(tokens["access_token"])["sub"]


async def _create_clinic_department_provider_for_user(
    client: AsyncClient,
    admin_tokens: dict,
    *,
    clinic_name: str,
    provider_user_id: str,
) -> str:
    """Like `_create_clinic_department_provider`, but links the created
    Provider row's `user_id` to `provider_user_id` (CR-02 test support —
    lets a test authenticate AS that provider and prove ownership-scoped
    access).
    """
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
            "full_name": "Dr. Owned Provider",
            "specialties": ["General Medicine"],
            "user_id": provider_user_id,
        },
        headers=_auth_header(admin_tokens),
    )
    assert provider_resp.status_code == 201, provider_resp.text
    return provider_resp.json()["id"]


@pytest.mark.integration
async def test_provider_can_manage_own_availability(client: AsyncClient) -> None:
    """A PROVIDER token whose linked Provider.user_id matches the caller
    CAN create/list their own availability templates (CR-02: the ownership
    check must not block the legitimate owner).
    """
    admin_tokens = await _register_and_login(client, role="admin")
    provider_tokens = await _register_and_login(client, role="provider")
    provider_user_id = _sub_from_tokens(provider_tokens)

    provider_id = await _create_clinic_department_provider_for_user(
        client,
        admin_tokens,
        clinic_name="Own Availability Clinic",
        provider_user_id=provider_user_id,
    )

    create_resp = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_valid_template_payload(),
        headers=_auth_header(provider_tokens),
    )
    assert create_resp.status_code == 201, create_resp.text

    list_resp = await client.get(
        f"/api/providers/{provider_id}/availability", headers=_auth_header(provider_tokens)
    )
    assert list_resp.status_code == 200


@pytest.mark.integration
async def test_provider_cannot_manage_another_providers_availability(
    client: AsyncClient,
) -> None:
    """CR-02 regression: a PROVIDER token whose linked Provider.user_id
    does NOT match the `provider_id` in the path must be rejected 403 on
    both create and list — closes the IDOR that let any authenticated
    provider read/write every other provider's availability templates.
    """
    admin_tokens = await _register_and_login(client, role="admin")

    victim_provider_tokens = await _register_and_login(client, role="provider")
    victim_provider_user_id = _sub_from_tokens(victim_provider_tokens)
    victim_provider_id = await _create_clinic_department_provider_for_user(
        client,
        admin_tokens,
        clinic_name="Victim Provider Clinic",
        provider_user_id=victim_provider_user_id,
    )

    attacker_provider_tokens = await _register_and_login(client, role="provider")

    create_resp = await client.post(
        f"/api/providers/{victim_provider_id}/availability",
        json=_valid_template_payload(),
        headers=_auth_header(attacker_provider_tokens),
    )
    assert create_resp.status_code == 403, create_resp.text

    list_resp = await client.get(
        f"/api/providers/{victim_provider_id}/availability",
        headers=_auth_header(attacker_provider_tokens),
    )
    assert list_resp.status_code == 403, list_resp.text


async def _create_template(
    client: AsyncClient, tokens: dict, provider_id: str, *, payload: dict | None = None
) -> str:
    resp = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=payload or _valid_template_payload(),
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_patch_availability_template_full_and_partial(client: AsyncClient) -> None:
    """PATCH updates only supplied fields (gap-closure Plan 01-07)."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability PATCH Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}",
        json={"slot_duration_minutes": 45},
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["slot_duration_minutes"] == 45
    assert resp.json()["day_of_week"] == 0  # untouched


@pytest.mark.integration
async def test_patch_availability_template_empty_body_422(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Empty Body Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}",
        json={},
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 422


@pytest.mark.integration
async def test_patch_availability_template_unknown_id_404(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Unknown Clinic"
    )

    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{uuid.uuid4()}",
        json={"slot_duration_minutes": 45},
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_availability_template_invalid_resulting_window_422(client: AsyncClient) -> None:
    """A PATCH that supplies only start_time, pushing it past the existing
    end_time, is rejected 422 — validated against the RESULTING window,
    not just the supplied fields in isolation."""
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Invalid Window Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)  # 09:00-17:00

    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}",
        json={"start_time": "18:00:00"},  # after the existing end_time (17:00)
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 422


@pytest.mark.integration
async def test_patch_availability_template_non_provider_admin_forbidden(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability RBAC PATCH Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    front_desk_tokens = await _register_and_login(client, role="front_desk")
    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}",
        json={"slot_duration_minutes": 45},
        headers=_auth_header(front_desk_tokens),
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_patch_availability_template_non_owner_provider_forbidden(client: AsyncClient) -> None:
    """CR-02 on the PATCH path: a PROVIDER token whose linked Provider.user_id
    does NOT match the path's provider_id is rejected 403."""
    admin_tokens = await _register_and_login(client, role="admin")
    victim_provider_tokens = await _register_and_login(client, role="provider")
    victim_provider_user_id = _sub_from_tokens(victim_provider_tokens)
    victim_provider_id = await _create_clinic_department_provider_for_user(
        client,
        admin_tokens,
        clinic_name="Victim Provider PATCH Clinic",
        provider_user_id=victim_provider_user_id,
    )
    template_id = await _create_template(client, admin_tokens, victim_provider_id)

    attacker_provider_tokens = await _register_and_login(client, role="provider")
    resp = await client.patch(
        f"/api/providers/{victim_provider_id}/availability/{template_id}",
        json={"slot_duration_minutes": 45},
        headers=_auth_header(attacker_provider_tokens),
    )
    assert resp.status_code == 403, resp.text


@pytest.mark.integration
async def test_deactivate_availability_template_hides_from_default_list(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Deactivate Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    deactivate_resp = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate",
        headers=_auth_header(admin_tokens),
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text
    assert deactivate_resp.json()["is_active"] is False
    assert deactivate_resp.json()["deactivated_at"] is not None

    default_list = await client.get(
        f"/api/providers/{provider_id}/availability", headers=_auth_header(admin_tokens)
    )
    assert all(t["id"] != template_id for t in default_list.json())

    inactive_list = await client.get(
        f"/api/providers/{provider_id}/availability",
        params={"include_inactive": "true"},
        headers=_auth_header(admin_tokens),
    )
    assert any(t["id"] == template_id for t in inactive_list.json())


@pytest.mark.integration
async def test_deactivate_availability_template_idempotent(client: AsyncClient) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Idempotent Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    first = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate",
        headers=_auth_header(admin_tokens),
    )
    assert first.status_code == 200

    second = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate",
        headers=_auth_header(admin_tokens),
    )
    assert second.status_code == 200
    assert second.json()["is_active"] is False


@pytest.mark.integration
async def test_deactivate_availability_template_non_owner_provider_forbidden(client: AsyncClient) -> None:
    """CR-02 on the deactivate path."""
    admin_tokens = await _register_and_login(client, role="admin")
    victim_provider_tokens = await _register_and_login(client, role="provider")
    victim_provider_user_id = _sub_from_tokens(victim_provider_tokens)
    victim_provider_id = await _create_clinic_department_provider_for_user(
        client,
        admin_tokens,
        clinic_name="Victim Provider Deactivate Clinic",
        provider_user_id=victim_provider_user_id,
    )
    template_id = await _create_template(client, admin_tokens, victim_provider_id)

    attacker_provider_tokens = await _register_and_login(client, role="provider")
    resp = await client.post(
        f"/api/providers/{victim_provider_id}/availability/{template_id}/deactivate",
        headers=_auth_header(attacker_provider_tokens),
    )
    assert resp.status_code == 403, resp.text


@pytest.mark.integration
async def test_availability_template_update_and_deactivate_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin_tokens = await _register_and_login(client, role="admin")
    _clinic_id, _department_id, provider_id = await _create_clinic_department_provider(
        client, admin_tokens, clinic_name="Availability Audited Clinic"
    )
    template_id = await _create_template(client, admin_tokens, provider_id)

    patch_resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}",
        json={"slot_duration_minutes": 60},
        headers=_auth_header(admin_tokens),
    )
    assert patch_resp.status_code == 200, patch_resp.text

    deactivate_resp = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate",
        headers=_auth_header(admin_tokens),
    )
    assert deactivate_resp.status_code == 200, deactivate_resp.text

    collection = mongo_client[shared_settings.mongo_db_name]["audit_log"]
    update_doc = await collection.find_one(
        {"action": "availability.update", "entity_id": template_id}
    )
    assert update_doc is not None
    assert update_doc["outcome"] == "success"

    deactivate_doc = await collection.find_one(
        {"action": "availability.deactivate", "entity_id": template_id}
    )
    assert deactivate_doc is not None
    assert deactivate_doc["outcome"] == "success"
