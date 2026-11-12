"""Provider's internal integrity lookup (plan 01.1-07, T-01.1-31).

`GET /internal/providers/{provider_id}` is what Scheduling's
`provider_slots.provider_id` will use (Phase 2) to confirm a provider exists and is
active before accepting a reference to it. It is called here DIRECTLY over the
in-process mesh at Provider's internal base URL — the gateway has no route to
`/internal` at all.

Contract under test: ADMIN user tokens and Scheduling/Visit service tokens with
`aud=provider` only (401 without a token, 403 for any other role), identical 404 for an unknown id, inactive rows
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
        headers={"Authorization": f"Bearer {create_service_token('scheduling', audience='provider')}"},
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


# --- plan 02-03: availability and by-user lookups (research G3, D-01, D-15) ---

AVAILABILITY_KEYS = {
    "id",
    "department_id",
    "day_of_week",
    "start_time",
    "end_time",
    "slot_duration_minutes",
    "effective_from",
    "effective_to",
    "is_active",
}


def _service(name: str) -> dict:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    return {"Authorization": f"Bearer {create_service_token(name, audience='provider')}"}


async def _scheduling_minted(mesh: httpx.AsyncClient, audience: str = "provider") -> dict:
    import os  # noqa: PLC0415

    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415

    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(os.environ["SCHEDULING_CLIENT_SECRET_PATH"]),
        service_base_url("identity"),
        http=mesh,
    )
    return {"Authorization": f"Bearer {await tokens.get(audience)}"}


async def _templates(client: AsyncClient, admin: dict, provider_id: str) -> tuple[str, str]:
    """(active template id, deactivated template id) for `provider_id`."""
    ids = []
    for day in (0, 2):
        created = await client.post(
            f"/api/providers/{provider_id}/availability",
            json={
                "day_of_week": day,
                "start_time": "09:00:00",
                "end_time": "12:00:00",
                "slot_duration_minutes": 20,
                "effective_from": "2026-01-01",
            },
            headers=_auth(admin),
        )
        assert created.status_code == 201, created.text
        ids.append(created.json()["id"])
    deactivated = await client.post(
        f"/api/providers/{provider_id}/availability/{ids[1]}/deactivate", headers=_auth(admin)
    )
    assert deactivated.status_code == 200, deactivated.text
    return ids[0], ids[1]


async def _linked_provider(client: AsyncClient, admin: dict) -> tuple[str, str, str]:
    """(provider id, department id, user id) of a provider linked to a login account."""
    clinic = await client.post(
        "/api/clinics", json={"name": "By-User Provider Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    clinic_id = clinic.json()["id"]
    department = await client.post(
        f"/api/clinics/{clinic_id}/departments", json={"name": "By-User"}, headers=_auth(admin)
    )
    assert department.status_code == 201, department.text
    department_id = department.json()["id"]
    account = await client.post(
        "/api/auth/admin/users",
        json={
            "email": unique_email("provider"),
            "password": PASSWORD,
            "role": "provider",
            "clinic_id": clinic_id,
        },
        headers=_auth(admin),
    )
    assert account.status_code == 201, account.text
    user_id = account.json()["id"]
    created = await client.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. By User",
            "specialties": ["General"],
            "user_id": user_id,
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"], department_id, user_id


async def test_scheduling_reads_availability_including_inactive_templates(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id = await _provider(client, admin)
    active_id, inactive_id = await _templates(client, admin, provider_id)

    resp = await mesh.get(
        _internal(f"/internal/providers/{provider_id}/availability"),
        headers=await _scheduling_minted(mesh),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert all(set(item) == AVAILABILITY_KEYS for item in body)
    by_id = {item["id"]: item for item in body}
    assert set(by_id) == {active_id, inactive_id}
    assert by_id[active_id]["is_active"] is True
    assert by_id[inactive_id]["is_active"] is False
    assert by_id[active_id] == {
        "id": active_id,
        "department_id": department_id,
        "day_of_week": 0,
        "start_time": "09:00:00",
        "end_time": "12:00:00",
        "slot_duration_minutes": 20,
        "effective_from": "2026-01-01",
        "effective_to": None,
        "is_active": True,
    }


async def test_availability_for_unknown_provider_is_404(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(
        _internal(f"/internal/providers/{uuid.uuid4()}/availability"), headers=_service("scheduling")
    )

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


async def test_visit_token_is_refused_on_availability(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(
        _internal(f"/internal/providers/{uuid.uuid4()}/availability"), headers=_service("visit")
    )

    assert resp.status_code == 403


async def test_availability_admin_allowed_front_desk_forbidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _department_id = await _provider(client, admin)
    front_desk = await _front_desk_tokens(client, admin)

    ok = await mesh.get(_internal(f"/internal/providers/{provider_id}/availability"), headers=_auth(admin))
    refused = await mesh.get(
        _internal(f"/internal/providers/{provider_id}/availability"), headers=_auth(front_desk)
    )

    assert ok.status_code == 200, ok.text
    assert ok.json() == []
    assert refused.status_code == 403


async def test_visit_token_resolves_provider_by_user(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id, user_id = await _linked_provider(client, admin)

    resp = await mesh.get(_internal(f"/internal/providers/by-user/{user_id}"), headers=_service("visit"))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": True}


async def test_scheduling_token_resolves_provider_by_user(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id, user_id = await _linked_provider(client, admin)

    resp = await mesh.get(
        _internal(f"/internal/providers/by-user/{user_id}"), headers=await _scheduling_minted(mesh)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": True}


async def test_by_user_unknown_user_is_404_not_422(mesh: httpx.AsyncClient) -> None:
    # A 422 here would mean `/providers/{provider_id}` captured the literal
    # `by-user` segment; the by-user route must be matched first.
    resp = await mesh.get(
        _internal(f"/internal/providers/by-user/{uuid.uuid4()}"), headers=_service("visit")
    )

    assert resp.status_code == 404
    assert resp.json() == NOT_FOUND


async def test_by_user_admin_allowed_front_desk_forbidden(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _department_id, user_id = await _linked_provider(client, admin)
    front_desk = await _front_desk_tokens(client, admin)

    ok = await mesh.get(_internal(f"/internal/providers/by-user/{user_id}"), headers=_auth(admin))
    refused = await mesh.get(
        _internal(f"/internal/providers/by-user/{user_id}"), headers=_auth(front_desk)
    )

    assert ok.status_code == 200, ok.text
    assert ok.json()["id"] == provider_id
    assert refused.status_code == 403


async def test_billing_token_is_refused_on_new_routes(mesh: httpx.AsyncClient) -> None:
    availability = await mesh.get(
        _internal(f"/internal/providers/{uuid.uuid4()}/availability"), headers=_service("billing")
    )
    by_user = await mesh.get(
        _internal(f"/internal/providers/by-user/{uuid.uuid4()}"), headers=_service("billing")
    )

    assert availability.status_code == 403
    assert by_user.status_code == 403


async def test_new_routes_are_not_reachable_through_the_gateway(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)

    availability = await client.get(
        f"/internal/providers/{uuid.uuid4()}/availability", headers=_auth(admin)
    )
    by_user = await client.get(f"/internal/providers/by-user/{uuid.uuid4()}", headers=_auth(admin))

    assert availability.status_code == 404
    assert by_user.status_code == 404
