"""Provider's cross-service references and the no-cascade rule (plan 01.1-07).

`providers.department_id` is Organization's and `providers.user_id` is Identity's.
Both are plain UUID columns, so Provider validates them at write time through the
shared reference client (D-05): a missing or inactive target is a 422, an owner
that cannot answer cleanly is a 503, and in EVERY refusal no `providers` row
exists. Deactivating a department never cascades into Provider (D-06): an existing
provider keeps its `department_id`, stays readable and listable, and can still be
PATCHed; only NEW providers (and new availability templates) pointing at the
inactive department are refused.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import bootstrap_admin_and_login, degraded_mesh, unique_email

PASSWORD = "correct-horse-battery"

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _clinic_and_department(client: AsyncClient, admin: dict, name: str) -> tuple[str, str]:
    clinic = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    clinic_id = clinic.json()["id"]
    department = await client.post(
        f"/api/clinics/{clinic_id}/departments", json={"name": "General"}, headers=_auth(admin)
    )
    assert department.status_code == 201, department.text
    return clinic_id, department.json()["id"]


async def _deactivate_department(
    client: AsyncClient, admin: dict, clinic_id: str, department_id: str
) -> None:
    resp = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate", headers=_auth(admin)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["is_active"] is False


async def _create_provider_account(client: AsyncClient, admin: dict) -> str:
    """A provider-role login account created by the admin; returns its user id."""
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": unique_email("provider"), "password": PASSWORD, "role": "provider"},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


def _provider_body(department_id: str, full_name: str, user_id: str | None = None) -> dict:
    body = {"department_id": department_id, "full_name": full_name, "specialties": ["General"]}
    if user_id is not None:
        body["user_id"] = user_id
    return body


def _unique_name(prefix: str) -> str:
    return f"{prefix} {uuid.uuid4().hex[:10]}"


async def _provider_rows(where: str, params: dict) -> int:
    """Committed-state count straight against provider_db (own short-lived engine)."""
    from services.provider.config import settings as provider_settings  # noqa: PLC0415

    engine = create_async_engine(provider_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(text(f"SELECT count(*) FROM providers WHERE {where}"), params)
    finally:
        await engine.dispose()


async def test_inactive_department_rejected_and_not_written(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id, department_id = await _clinic_and_department(client, admin, "Inactive Dept Clinic")
    await _deactivate_department(client, admin, clinic_id, department_id)
    name = _unique_name("Dr. Inactive Dept")

    resp = await client.post(
        "/api/providers", json=_provider_body(department_id, name), headers=_auth(admin)
    )

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "department is inactive"
    assert await _provider_rows("full_name = :n", {"n": name}) == 0


async def test_existing_provider_survives_department_deactivation(client: AsyncClient) -> None:
    """D-06: deactivating the department does not cascade into Provider."""
    admin = await bootstrap_admin_and_login(client)
    clinic_id, department_id = await _clinic_and_department(client, admin, "No Cascade Clinic")
    created = await client.post(
        "/api/providers",
        json=_provider_body(department_id, _unique_name("Dr. Survivor")),
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    provider = created.json()

    await _deactivate_department(client, admin, clinic_id, department_id)

    fetched = await client.get(f"/api/providers/{provider['id']}", headers=_auth(admin))
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == provider  # same department_id, still active, nothing rewritten
    assert fetched.json()["is_active"] is True

    listed = await client.get(
        "/api/providers", params={"department_id": department_id}, headers=_auth(admin)
    )
    assert listed.status_code == 200, listed.text
    assert [p["id"] for p in listed.json()] == [provider["id"]]
    assert listed.json()[0]["department_id"] == department_id

    patched = await client.patch(
        f"/api/providers/{provider['id']}",
        json={"full_name": "Dr. Survivor Renamed"},
        headers=_auth(admin),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["full_name"] == "Dr. Survivor Renamed"
    assert patched.json()["department_id"] == department_id


async def test_unknown_user_id_rejected_and_not_written(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Unknown User Clinic")
    name = _unique_name("Dr. Unknown User")

    resp = await client.post(
        "/api/providers",
        json=_provider_body(department_id, name, user_id=str(uuid.uuid4())),
        headers=_auth(admin),
    )

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "user not found"
    assert await _provider_rows("full_name = :n", {"n": name}) == 0


async def test_valid_user_link_is_accepted_and_echoed(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Linked User Clinic")
    account = await _create_provider_account(client, admin)

    resp = await client.post(
        "/api/providers",
        json=_provider_body(department_id, _unique_name("Dr. Linked"), user_id=account),
        headers=_auth(admin),
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["user_id"] == account


async def test_second_provider_for_same_account_is_409_and_not_written(
    client: AsyncClient,
) -> None:
    """providers.user_id is unique (one profile per account): the database
    constraint is the arbiter, and a refused duplicate leaves no second row."""
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Duplicate Link Clinic")
    account = await _create_provider_account(client, admin)
    first = await client.post(
        "/api/providers",
        json=_provider_body(department_id, _unique_name("Dr. First Link"), user_id=account),
        headers=_auth(admin),
    )
    assert first.status_code == 201, first.text
    second_name = _unique_name("Dr. Second Link")

    second = await client.post(
        "/api/providers",
        json=_provider_body(department_id, second_name, user_id=account),
        headers=_auth(admin),
    )

    assert second.status_code == 409, second.text
    assert second.json()["detail"] == "account is already linked to a provider profile"
    assert await _provider_rows("user_id = :u", {"u": account}) == 1
    assert await _provider_rows("full_name = :n", {"n": second_name}) == 0


async def test_create_when_organization_unreachable_is_503_and_not_written(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Org Down Clinic")
    name = _unique_name("Dr. Org Down")

    async with degraded_mesh("organization"):
        resp = await client.post(
            "/api/providers", json=_provider_body(department_id, name), headers=_auth(admin)
        )

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "organization service unavailable"
    assert await _provider_rows("full_name = :n", {"n": name}) == 0


async def test_user_link_when_identity_unreachable_is_503_and_not_written(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Identity Down Clinic")
    account = await _create_provider_account(client, admin)
    name = _unique_name("Dr. Identity Down")

    # Warm Provider's JWKS cache so token verification does not need Identity
    # while it is down; only the account-link check should be affected.
    warm = await client.get(
        "/api/providers", params={"department_id": department_id}, headers=_auth(admin)
    )
    assert warm.status_code == 200, warm.text

    async with degraded_mesh("identity"):
        resp = await client.post(
            "/api/providers",
            json=_provider_body(department_id, name, user_id=account),
            headers=_auth(admin),
        )

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "identity service unavailable"
    assert await _provider_rows("full_name = :n", {"n": name}) == 0


# --- availability templates (the same references, the same no-cascade rule) ----


def _template_payload() -> dict:
    return {
        "day_of_week": 1,
        "start_time": "09:00:00",
        "end_time": "12:00:00",
        "slot_duration_minutes": 30,
        "effective_from": "2026-01-05",
        "effective_to": "2026-12-31",
    }


async def _provider_in_department(client: AsyncClient, admin: dict, department_id: str) -> str:
    created = await client.post(
        "/api/providers",
        json=_provider_body(department_id, _unique_name("Dr. Availability")),
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


async def _template_rows(provider_id: str) -> int:
    from services.provider.config import settings as provider_settings  # noqa: PLC0415

    engine = create_async_engine(provider_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT count(*) FROM availability_templates WHERE provider_id = :p"),
                {"p": provider_id},
            )
    finally:
        await engine.dispose()


async def test_new_template_for_inactive_department_rejected_while_existing_ones_work(
    client: AsyncClient,
) -> None:
    """D-06: after the provider's department is deactivated, a NEW template is
    refused 422 "department is inactive", while listing and PATCHing the
    templates that already exist still succeed (no cascade, nothing hidden)."""
    admin = await bootstrap_admin_and_login(client)
    clinic_id, department_id = await _clinic_and_department(client, admin, "Template Inactive Clinic")
    provider_id = await _provider_in_department(client, admin, department_id)
    existing = await client.post(
        f"/api/providers/{provider_id}/availability", json=_template_payload(), headers=_auth(admin)
    )
    assert existing.status_code == 201, existing.text
    template = existing.json()

    await _deactivate_department(client, admin, clinic_id, department_id)

    refused = await client.post(
        f"/api/providers/{provider_id}/availability", json=_template_payload(), headers=_auth(admin)
    )
    assert refused.status_code == 422, refused.text
    assert refused.json()["detail"] == "department is inactive"
    assert await _template_rows(provider_id) == 1

    listed = await client.get(f"/api/providers/{provider_id}/availability", headers=_auth(admin))
    assert listed.status_code == 200, listed.text
    assert [t["id"] for t in listed.json()] == [template["id"]]
    assert listed.json()[0]["department_id"] == department_id

    patched = await client.patch(
        f"/api/providers/{provider_id}/availability/{template['id']}",
        json={"slot_duration_minutes": 45},
        headers=_auth(admin),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["slot_duration_minutes"] == 45


async def test_template_create_when_organization_unreachable_is_503_and_not_written(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    _clinic_id, department_id = await _clinic_and_department(client, admin, "Template Org Down Clinic")
    provider_id = await _provider_in_department(client, admin, department_id)

    async with degraded_mesh("organization"):
        resp = await client.post(
            f"/api/providers/{provider_id}/availability",
            json=_template_payload(),
            headers=_auth(admin),
        )

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "organization service unavailable"
    assert await _template_rows(provider_id) == 0
