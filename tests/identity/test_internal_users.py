"""Identity's `GET /internal/users/{user_id}` lookup (plan 01.1-05, Task 2).

Patient and Provider call it (plans 01.1-06/07) to validate a profile -> account
link. Contract under test (STRIDE T-01.1-24): returns exactly `{id, role,
clinic_id}` (never an email or any credential material); accepts admin/front_desk
user tokens and service tokens; 401 without a token, 403 for other roles; an
unknown user and a user outside the caller's clinic scope get the IDENTICAL 404,
so existence cannot be probed across clinics; the route is excluded from OpenAPI
and the gateway has no route to it.
"""

from __future__ import annotations

import uuid

import httpx
import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"
NOT_FOUND = {"detail": "not found"}

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _internal(user_id: str) -> str:
    return f"{service_base_url('identity')}/internal/users/{user_id}"


async def _create_clinic(client: AsyncClient, admin: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _create_account(
    client: AsyncClient, admin: dict, *, role: str, clinic_id: str | None = None
) -> dict:
    """Create an account through the public API and return its UserOut body plus
    the credentials needed to log in."""
    email = unique_email(role)
    if role == "patient":
        payload: dict = {"email": email, "password": PASSWORD}
        if clinic_id is not None:
            payload["clinic_id"] = clinic_id
        created = await client.post("/api/auth/register", json=payload)
    else:
        payload = {"email": email, "password": PASSWORD, "role": role}
        if clinic_id is not None:
            payload["clinic_id"] = clinic_id
        created = await client.post("/api/auth/admin/users", json=payload, headers=_auth(admin))
    assert created.status_code == 201, created.text
    return {**created.json(), "password": PASSWORD}


async def _login(client: AsyncClient, account: dict) -> dict:
    resp = await client.post(
        "/api/auth/login", json={"email": account["email"], "password": account["password"]}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_admin_gets_exactly_id_role_and_clinic_id(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Internal User Clinic")
    account = await _create_account(client, admin, role="provider", clinic_id=clinic_id)

    resp = await mesh.get(_internal(account["id"]), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
    assert set(resp.json()) == {"id", "role", "clinic_id"}
    assert resp.json() == {"id": account["id"], "role": "provider", "clinic_id": clinic_id}
    # No credential or contact data, anywhere in the raw body.
    assert account["email"] not in resp.text
    assert "password" not in resp.text.lower()


async def test_unknown_user_is_404(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await mesh.get(_internal(str(uuid.uuid4())), headers=_auth(admin))

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


async def test_front_desk_cannot_see_a_user_of_another_clinic_and_it_looks_like_missing(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_a = await _create_clinic(client, admin, "Scope Clinic A")
    clinic_b = await _create_clinic(client, admin, "Scope Clinic B")
    front_desk = await _create_account(client, admin, role="front_desk", clinic_id=clinic_a)
    front_desk_tokens = await _login(client, front_desk)
    in_scope = await _create_account(client, admin, role="provider", clinic_id=clinic_a)
    other_clinic = await _create_account(client, admin, role="provider", clinic_id=clinic_b)

    same_clinic = await mesh.get(_internal(in_scope["id"]), headers=_auth(front_desk_tokens))
    cross_clinic = await mesh.get(_internal(other_clinic["id"]), headers=_auth(front_desk_tokens))
    missing = await mesh.get(_internal(str(uuid.uuid4())), headers=_auth(front_desk_tokens))

    assert same_clinic.status_code == 200
    assert cross_clinic.status_code == 404
    assert cross_clinic.json() == NOT_FOUND
    assert cross_clinic.json() == missing.json()


async def test_front_desk_can_look_up_a_clinic_less_patient_account(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_a = await _create_clinic(client, admin, "Walk-in Clinic")
    front_desk = await _create_account(client, admin, role="front_desk", clinic_id=clinic_a)
    front_desk_tokens = await _login(client, front_desk)
    patient = await _create_account(client, admin, role="patient")

    resp = await mesh.get(_internal(patient["id"]), headers=_auth(front_desk_tokens))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": patient["id"], "role": "patient", "clinic_id": None}


@pytest.mark.parametrize("role", ["provider", "patient"])
async def test_other_roles_are_forbidden(
    client: AsyncClient, mesh: httpx.AsyncClient, role: str
) -> None:
    admin = await bootstrap_admin_and_login(client)
    account = await _create_account(client, admin, role=role)
    tokens = await _login(client, account)

    resp = await mesh.get(_internal(account["id"]), headers=_auth(tokens))

    assert resp.status_code == 403


async def test_missing_token_is_401(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(_internal(str(uuid.uuid4())))

    assert resp.status_code == 401


async def test_service_token_may_look_up_users(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    admin = await bootstrap_admin_and_login(client)
    patient = await _create_account(client, admin, role="patient")

    resp = await mesh.get(
        _internal(patient["id"]),
        headers={"Authorization": f"Bearer {create_service_token('identity')}"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == patient["id"]


async def test_route_is_absent_from_identity_openapi(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(f"{service_base_url('identity')}/openapi.json")

    assert resp.status_code == 200
    assert not [path for path in resp.json()["paths"] if path.startswith("/internal")]


async def test_route_is_not_reachable_through_the_gateway(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await client.get(f"/internal/users/{uuid.uuid4()}", headers=_auth(admin))

    assert resp.status_code == 404
