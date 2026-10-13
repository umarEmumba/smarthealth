"""Patient's internal reference lookups (plan 02-03, research G3, D-06, D-18).

Scheduling's booking validation (D-09 step 1) confirms a patient exists, is
active and belongs to the clinic being booked. It calls Patient DIRECTLY over
the in-process mesh with a service token whose `aud` is `patient`:

    GET /internal/patients/{patient_id}?clinic_id=        -> {id, clinic_id, user_id, is_active}
    GET /internal/patients/by-user/{user_id}?clinic_id=   -> same shape

Contract under test: `clinic_id` is REQUIRED (422 without it) and re-applies
clinic scope for service callers; a patient in another clinic is the same 404
as a missing one; the body carries ids and the active flag only (no PHI);
billing can neither mint nor use an `aud=patient` token; the routes are absent
from OpenAPI and unreachable through the gateway.
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest
from httpx import AsyncClient

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"
NOT_FOUND = {"detail": "not found"}
REF_KEYS = {"id", "clinic_id", "user_id", "is_active"}

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _internal(path: str) -> str:
    return f"{service_base_url('patient')}{path}"


def _secret(client: str) -> str:
    return load_client_secret(os.environ[f"{client.upper()}_CLIENT_SECRET_PATH"])


async def _scheduling_token(mesh: httpx.AsyncClient, audience: str = "patient") -> str:
    tokens = ServiceTokenProvider(
        "scheduling", _secret("scheduling"), service_base_url("identity"), http=mesh
    )
    return await tokens.get(audience)


async def _clinic(client: AsyncClient, admin: dict, name: str = "Internal Patient Clinic") -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _front_desk(client: AsyncClient, admin: dict, clinic_id: str) -> dict:
    email = unique_email("front-desk")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk", "clinic_id": clinic_id},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def _patient_account(client: AsyncClient) -> str:
    resp = await client.post(
        "/api/auth/register", json={"email": unique_email("patient"), "password": PASSWORD}
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _patient(
    client: AsyncClient, tokens: dict, clinic_id: str, user_id: str | None = None
) -> str:
    body: dict = {
        "clinic_id": clinic_id,
        "full_name": "Zzyzx Internal Synthetic",
        "date_of_birth": "1990-01-01",
    }
    if user_id is not None:
        body["user_id"] = user_id
    resp = await client.post("/api/patients", json=body, headers=_auth(tokens))
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def test_tracer_scheduling_service_token_resolves_patient_in_clinic(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    front_desk = await _front_desk(client, admin, clinic_id)
    patient_id = await _patient(client, front_desk, clinic_id)
    token = await _scheduling_token(mesh)

    resp = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )

    assert resp.status_code == 200, resp.text
    assert set(resp.json()) == REF_KEYS
    assert resp.json() == {
        "id": patient_id,
        "clinic_id": clinic_id,
        "user_id": None,
        "is_active": True,
    }


async def test_patient_in_another_clinic_is_the_same_404_as_missing(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    other_clinic = await _clinic(client, admin, name="Other Internal Clinic")
    patient_id = await _patient(client, admin, clinic_id)
    token = await _scheduling_token(mesh)

    wrong_clinic = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": other_clinic},
        headers=_bearer(token),
    )
    missing = await mesh.get(
        _internal(f"/internal/patients/{uuid.uuid4()}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )

    assert wrong_clinic.status_code == 404
    assert wrong_clinic.json() == NOT_FOUND
    assert missing.status_code == 404
    assert missing.json() == NOT_FOUND


async def test_clinic_id_is_required(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    patient_id = await _patient(client, admin, clinic_id)
    token = await _scheduling_token(mesh)

    by_id = await mesh.get(_internal(f"/internal/patients/{patient_id}"), headers=_bearer(token))
    by_user = await mesh.get(
        _internal(f"/internal/patients/by-user/{uuid.uuid4()}"), headers=_bearer(token)
    )

    assert by_id.status_code == 422
    assert by_user.status_code == 422


async def test_by_user_resolves_linked_patient_account(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    other_clinic = await _clinic(client, admin, name="By-User Other Clinic")
    account = await _patient_account(client)
    patient_id = await _patient(client, admin, clinic_id, user_id=account)
    token = await _scheduling_token(mesh)

    resp = await mesh.get(
        _internal(f"/internal/patients/by-user/{account}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )
    wrong_clinic = await mesh.get(
        _internal(f"/internal/patients/by-user/{account}"),
        params={"clinic_id": other_clinic},
        headers=_bearer(token),
    )
    unknown = await mesh.get(
        _internal(f"/internal/patients/by-user/{uuid.uuid4()}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "id": patient_id,
        "clinic_id": clinic_id,
        "user_id": account,
        "is_active": True,
    }
    assert wrong_clinic.status_code == 404
    assert wrong_clinic.json() == NOT_FOUND
    assert unknown.status_code == 404
    assert unknown.json() == NOT_FOUND


async def test_inactive_patient_is_reported_not_hidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    patient_id = await _patient(client, admin, clinic_id)
    deactivated = await client.post(
        f"/api/patients/{patient_id}/deactivate",
        params={"clinic_id": clinic_id},
        headers=_auth(admin),
    )
    assert deactivated.status_code == 200, deactivated.text
    token = await _scheduling_token(mesh)

    resp = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["is_active"] is False


async def test_front_desk_user_outside_clinic_scope_gets_404(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    other_clinic = await _clinic(client, admin, name="Front Desk Other Clinic")
    patient_id = await _patient(client, admin, clinic_id)
    in_scope = await _front_desk(client, admin, clinic_id)
    out_of_scope = await _front_desk(client, admin, other_clinic)

    ok = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_auth(in_scope),
    )
    refused = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_auth(out_of_scope),
    )

    assert ok.status_code == 200, ok.text
    assert refused.status_code == 404
    assert refused.json() == NOT_FOUND


async def test_service_token_for_another_audience_is_401(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    patient_id = await _patient(client, admin, clinic_id)
    token = await _scheduling_token(mesh, audience="provider")

    resp = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(token),
    )

    assert resp.status_code == 401


async def test_billing_cannot_mint_or_use_a_patient_token(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    patient_id = await _patient(client, admin, clinic_id)

    mint = await mesh.post(
        f"{service_base_url('identity')}/internal/service-tokens",
        json={"audience": "patient"},
        auth=("billing", _secret("billing")),
    )
    assert mint.status_code == 403

    hand_signed = create_service_token("billing", audience="patient")
    resp = await mesh.get(
        _internal(f"/internal/patients/{patient_id}"),
        params={"clinic_id": clinic_id},
        headers=_bearer(hand_signed),
    )
    assert resp.status_code == 403


async def test_internal_route_requires_a_token(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(
        _internal(f"/internal/patients/{uuid.uuid4()}"), params={"clinic_id": str(uuid.uuid4())}
    )

    assert resp.status_code == 401


async def test_internal_route_is_not_reachable_through_the_gateway(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin)
    patient_id = await _patient(client, admin, clinic_id)

    by_id = await client.get(
        f"/internal/patients/{patient_id}", params={"clinic_id": clinic_id}, headers=_auth(admin)
    )
    by_user = await client.get(
        f"/internal/patients/by-user/{uuid.uuid4()}",
        params={"clinic_id": clinic_id},
        headers=_auth(admin),
    )

    assert by_id.status_code == 404
    assert by_user.status_code == 404


async def test_internal_routes_are_excluded_from_openapi() -> None:
    from services.patient.app import app  # noqa: PLC0415

    paths = app.openapi()["paths"]

    assert not [path for path in paths if path.startswith("/internal")]
    assert "/api/patients" in paths
