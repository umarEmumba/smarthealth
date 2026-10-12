"""Organization's internal reference lookups (D-05 consumer side, D-06).

`GET /internal/clinics/{id}` and `GET /internal/departments/{id}` are what
Identity/Patient/Provider will call (plans 01.1-05..07) before accepting a
reference to a clinic or department. They are called here DIRECTLY over the
in-process mesh at Organization's internal base URL — the gateway has no route
to `/internal` at all (see tests/gateway/test_routing.py).

Contract under test: authenticated internal caller with an allowed role (401
without a token, 403 for a disallowed role); clinic-scoped (a caller bound to
another clinic gets the same 404 as a missing id, so existence cannot be
probed); inactive rows are REPORTED (`is_active: false`), never hidden, because
the caller turns "inactive" into a 422; minimal fields; excluded from OpenAPI.
"""

from __future__ import annotations

import uuid

import httpx
import pytest
from httpx import AsyncClient

from libs.auth.roles import Role
from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"
NOT_FOUND = {"detail": "not found"}


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _internal(path: str) -> str:
    return f"{service_base_url('organization')}{path}"


async def _account_tokens(
    client: AsyncClient, admin_tokens: dict, *, role: str, clinic_id: str | None = None
) -> dict:
    email = unique_email(role)
    if role == "patient":
        created = await client.post(
            "/api/auth/register", json={"email": email, "password": PASSWORD}
        )
    else:
        payload: dict = {"email": email, "password": PASSWORD, "role": role}
        if clinic_id is not None:
            payload["clinic_id"] = clinic_id
        created = await client.post("/api/auth/admin/users", json=payload, headers=_auth(admin_tokens))
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def _create_clinic(client: AsyncClient, admin_tokens: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics",
        json={"name": name, "timezone": "UTC"},
        headers=_auth(admin_tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _create_department(client: AsyncClient, admin_tokens: dict, clinic_id: str) -> str:
    resp = await client.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "Lookup Department"},
        headers=_auth(admin_tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


# --- clinic lookup -----------------------------------------------------------


@pytest.mark.integration
async def test_admin_reads_active_clinic_ref(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Lookup Clinic")

    resp = await mesh.get(_internal(f"/internal/clinics/{clinic_id}"), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": clinic_id, "is_active": True}


@pytest.mark.integration
async def test_deactivated_clinic_is_reported_inactive_not_hidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Soon Inactive Clinic")
    deactivated = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200

    resp = await mesh.get(_internal(f"/internal/clinics/{clinic_id}"), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": clinic_id, "is_active": False}


@pytest.mark.integration
async def test_unknown_clinic_is_404(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await mesh.get(_internal(f"/internal/clinics/{uuid.uuid4()}"), headers=_auth(admin))

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


@pytest.mark.integration
async def test_front_desk_is_clinic_scoped_and_out_of_scope_looks_like_missing(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_a = await _create_clinic(client, admin, "Scope A")
    clinic_b = await _create_clinic(client, admin, "Scope B")
    front_desk = await _account_tokens(client, admin, role="front_desk", clinic_id=clinic_a)

    own = await mesh.get(_internal(f"/internal/clinics/{clinic_a}"), headers=_auth(front_desk))
    other = await mesh.get(_internal(f"/internal/clinics/{clinic_b}"), headers=_auth(front_desk))
    missing = await mesh.get(
        _internal(f"/internal/clinics/{uuid.uuid4()}"), headers=_auth(front_desk)
    )

    assert own.status_code == 200, own.text
    assert own.json() == {"id": clinic_a, "is_active": True}
    assert other.status_code == 404
    # Identical to a genuinely missing id, so scope cannot be probed.
    assert other.status_code == missing.status_code
    assert other.json() == missing.json() == NOT_FOUND


@pytest.mark.integration
async def test_provider_token_is_forbidden_on_clinic_lookup(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Provider Forbidden Clinic")
    provider = await _account_tokens(client, admin, role="provider")

    resp = await mesh.get(_internal(f"/internal/clinics/{clinic_id}"), headers=_auth(provider))

    assert resp.status_code == 403


# --- department lookup -------------------------------------------------------


@pytest.mark.integration
async def test_admin_reads_department_ref(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Department Lookup Clinic")
    department_id = await _create_department(client, admin, clinic_id)

    resp = await mesh.get(
        _internal(f"/internal/departments/{department_id}"), headers=_auth(admin)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": department_id, "clinic_id": clinic_id, "is_active": True}


@pytest.mark.integration
async def test_deactivated_department_is_reported_inactive_not_hidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Department Inactive Clinic")
    department_id = await _create_department(client, admin, clinic_id)
    deactivated = await client.post(
        f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate", headers=_auth(admin)
    )
    assert deactivated.status_code == 200

    resp = await mesh.get(
        _internal(f"/internal/departments/{department_id}"), headers=_auth(admin)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": department_id, "clinic_id": clinic_id, "is_active": False}


@pytest.mark.integration
async def test_unknown_department_is_404(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await mesh.get(
        _internal(f"/internal/departments/{uuid.uuid4()}"), headers=_auth(admin)
    )

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


@pytest.mark.integration
async def test_provider_without_a_clinic_claim_may_read_a_department(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    """A provider caller with no clinic claim is permitted: the calling service
    has already applied its own ownership rule before asking (CR-02)."""
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Provider Department Clinic")
    department_id = await _create_department(client, admin, clinic_id)
    provider = await _account_tokens(client, admin, role="provider")

    resp = await mesh.get(
        _internal(f"/internal/departments/{department_id}"), headers=_auth(provider)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == department_id


@pytest.mark.integration
async def test_patient_and_front_desk_tokens_are_forbidden_on_department_lookup(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Department Forbidden Clinic")
    department_id = await _create_department(client, admin, clinic_id)
    patient = await _account_tokens(client, admin, role="patient")
    front_desk = await _account_tokens(client, admin, role="front_desk", clinic_id=clinic_id)

    for tokens in (patient, front_desk):
        resp = await mesh.get(
            _internal(f"/internal/departments/{department_id}"), headers=_auth(tokens)
        )
        assert resp.status_code == 403


# --- authentication, exposure ------------------------------------------------


@pytest.mark.integration
async def test_internal_routes_require_a_token(mesh: httpx.AsyncClient) -> None:
    clinic = await mesh.get(_internal(f"/internal/clinics/{uuid.uuid4()}"))
    department = await mesh.get(_internal(f"/internal/departments/{uuid.uuid4()}"))

    assert clinic.status_code == 401
    assert department.status_code == 401


@pytest.mark.integration
async def test_internal_routes_are_not_reachable_through_the_gateway(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Gateway Isolation Clinic")

    resp = await client.get(f"/internal/clinics/{clinic_id}", headers=_auth(admin))

    assert resp.status_code == 404


def test_internal_routes_are_excluded_from_openapi() -> None:
    from services.organization.app import app  # noqa: PLC0415

    paths = app.openapi()["paths"]

    assert not [path for path in paths if path.startswith("/internal")]
    # ...while the public routes are documented as before.
    assert "/api/clinics" in paths


# --- clinic_scope_permits (pure) ---------------------------------------------


def _caller(*, kind: str = "user", role: Role | None, clinic_id: str | None):  # noqa: ANN202
    from libs.auth.internal import InternalCaller  # noqa: PLC0415

    return InternalCaller(
        kind=kind,  # type: ignore[arg-type]
        sub=str(uuid.uuid4()),
        role=role,
        clinic_id=clinic_id,
        raw_token="synthetic",
    )


def test_clinic_scope_permits_matching_clinic() -> None:
    from libs.auth.internal import clinic_scope_permits  # noqa: PLC0415

    clinic = uuid.uuid4()
    assert clinic_scope_permits(_caller(role=Role.FRONT_DESK, clinic_id=str(clinic)), clinic)


def test_clinic_scope_denies_a_different_clinic() -> None:
    from libs.auth.internal import clinic_scope_permits  # noqa: PLC0415

    assert not clinic_scope_permits(
        _caller(role=Role.FRONT_DESK, clinic_id=str(uuid.uuid4())), uuid.uuid4()
    )


def test_clinic_scope_permits_admin_service_unscoped_callers_and_clinicless_targets() -> None:
    from libs.auth.internal import clinic_scope_permits  # noqa: PLC0415

    target = uuid.uuid4()
    other = str(uuid.uuid4())
    assert clinic_scope_permits(_caller(role=Role.ADMIN, clinic_id=other), target)
    assert clinic_scope_permits(_caller(kind="service", role=None, clinic_id=other), target)
    assert clinic_scope_permits(_caller(role=Role.PROVIDER, clinic_id=None), target)
    assert clinic_scope_permits(_caller(role=Role.FRONT_DESK, clinic_id=other), None)


def test_clinic_scope_denies_a_malformed_clinic_claim() -> None:
    from libs.auth.internal import clinic_scope_permits  # noqa: PLC0415

    assert not clinic_scope_permits(
        _caller(role=Role.FRONT_DESK, clinic_id="not-a-uuid"), uuid.uuid4()
    )
