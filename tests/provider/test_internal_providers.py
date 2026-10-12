"""Provider's internal integrity lookup (plan 01.1-07, T-01.1-31).

`GET /internal/providers/{provider_id}` is what Scheduling's
`provider_slots.provider_id` will use (Phase 2) to confirm a provider exists and is
active before accepting a reference to it. It is called here DIRECTLY over the
in-process mesh at Provider's internal base URL — the gateway has no route to
`/internal` at all.

Contract under test: ADMIN user tokens and service tokens only (401 without a
token, 403 for any other role), identical 404 for an unknown id, inactive rows
REPORTED (`is_active: false`) rather than hidden, minimal fields (id,
department_id, is_active), and excluded from OpenAPI.
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


def _internal(path: str) -> str:
    return f"{service_base_url('provider')}{path}"


async def _provider(client: AsyncClient, admin: dict) -> tuple[str, str]:
    """(provider id, department id) of a fresh provider created through the gateway."""
    clinic = await client.post(
        "/api/clinics", json={"name": "Internal Provider Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments",
        json={"name": "Internal"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    department_id = department.json()["id"]
    created = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. Internal Lookup",
            "specialties": ["General"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"], department_id


async def _front_desk_tokens(client: AsyncClient, admin: dict) -> dict:
    email = unique_email("front-desk")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk"},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def test_admin_reads_provider_ref_with_exactly_three_fields(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id = await _provider(client, admin)

    resp = await mesh.get(_internal(f"/internal/providers/{provider_id}"), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": True}


async def test_deactivated_provider_is_reported_inactive_not_hidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id = await _provider(client, admin)
    deactivated = await client.post(f"/api/providers/{provider_id}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200, deactivated.text

    resp = await mesh.get(_internal(f"/internal/providers/{provider_id}"), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": False}


async def test_unknown_provider_is_404(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await mesh.get(_internal(f"/internal/providers/{uuid.uuid4()}"), headers=_auth(admin))

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


async def test_front_desk_token_is_forbidden(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _department_id = await _provider(client, admin)
    front_desk = await _front_desk_tokens(client, admin)

    resp = await mesh.get(_internal(f"/internal/providers/{provider_id}"), headers=_auth(front_desk))

    assert resp.status_code == 403


async def test_service_token_is_accepted(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id = await _provider(client, admin)

    resp = await mesh.get(
        _internal(f"/internal/providers/{provider_id}"),
        headers={"Authorization": f"Bearer {create_service_token('scheduling')}"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": True}


async def test_internal_route_requires_a_token(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(_internal(f"/internal/providers/{uuid.uuid4()}"))

    assert resp.status_code == 401


async def test_internal_route_is_excluded_from_openapi() -> None:
    from services.provider.app import app  # noqa: PLC0415

    paths = app.openapi()["paths"]

    assert not [path for path in paths if path.startswith("/internal")]
    # ...while the public routes are documented as before.
    assert "/api/providers" in paths


async def test_internal_route_is_not_reachable_through_the_gateway(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _department_id = await _provider(client, admin)

    resp = await client.get(f"/internal/providers/{provider_id}", headers=_auth(admin))

    assert resp.status_code == 404
