"""Provider's schedule-change feed (plan 02.1-05, mentor item M2).

Every committed change that can make already-generated slots wrong writes ONE event
in the same transaction (template edit that changes a schedule-affecting field,
template deactivation, provider deactivation); Scheduling drains the events through
`GET /internal/schedule-changes` and acknowledges them idempotently. The feed is read
here over the in-process mesh at Provider's internal base URL (the gateway has no
`/internal` route). Other tests leave events in the shared database, so every
assertion is scoped to the provider id the test created.
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _feed(path: str = "/internal/schedule-changes") -> str:
    return f"{service_base_url('provider')}{path}"


async def _service_headers(mesh: httpx.AsyncClient, service: str, audience: str = "provider") -> dict:
    """A REAL service token minted through Identity's client-credentials endpoint."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415

    secret_path = os.environ[f"{service.upper()}_CLIENT_SECRET_PATH"]
    provider = ServiceTokenProvider(
        service,
        load_client_secret(secret_path),
        service_base_url("identity"),
        http=mesh,
    )
    return {"Authorization": f"Bearer {await provider.get(audience)}"}


async def _front_desk(client: AsyncClient, admin: dict) -> dict:
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


async def _provider(client: AsyncClient, admin: dict) -> str:
    """Provider id of a fresh provider created through the gateway."""
    clinic = await client.post(
        "/api/clinics", json={"name": "Schedule Feed Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments",
        json={"name": "Feed"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    created = await client.post(
        "/api/providers",
        json={
            "department_id": department.json()["id"],
            "full_name": "Dr. Schedule Feed",
            "specialties": ["General"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


def _template_payload(**overrides) -> dict:
    payload = {
        "day_of_week": 0,
        "start_time": "09:00:00",
        "end_time": "17:00:00",
        "slot_duration_minutes": 30,
        "effective_from": "2026-01-05",
        "effective_to": "2026-12-31",
    }
    payload.update(overrides)
    return payload


async def _template(client: AsyncClient, admin: dict, provider_id: str, **overrides) -> str:
    created = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_template_payload(**overrides),
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


async def _patch(client: AsyncClient, admin: dict, provider_id: str, template_id: str, body: dict):
    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}", json=body, headers=_auth(admin)
    )
    assert resp.status_code == 200, resp.text
    return resp


async def _deactivate_template(client: AsyncClient, admin: dict, provider_id: str, template_id: str):
    resp = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate", headers=_auth(admin)
    )
    assert resp.status_code == 200, resp.text
    return resp


async def _events(mesh: httpx.AsyncClient, headers: dict, provider_id: str) -> list[dict]:
    """The pending events of `provider_id` (the feed is global; scope by provider)."""
    resp = await mesh.get(_feed(), params={"limit": 200}, headers=headers)
    assert resp.status_code == 200, resp.text
    return [e for e in resp.json() if e["provider_id"] == provider_id]


async def test_tracer_deactivation_event_is_readable_and_ackable(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id = await _provider(client, admin)
    template_id = await _template(client, admin, provider_id)
    await _deactivate_template(client, admin, provider_id, template_id)
    scheduling = await _service_headers(mesh, "scheduling")

    events = await _events(mesh, scheduling, provider_id)

    assert len(events) == 1, events
    event = events[0]
    assert event["change_type"] == "template_deactivated"
    assert event["template_id"] == template_id
    assert set(event) == {"id", "provider_id", "change_type", "template_id", "created_at"}

    ack = await mesh.post(_feed(f"/internal/schedule-changes/{event['id']}/ack"), headers=scheduling)
    assert ack.status_code == 200, ack.text
    assert ack.json() == {"id": event["id"], "delivered": True}
    assert await _events(mesh, scheduling, provider_id) == []

    # Only a Scheduling service token or an ADMIN user may use the feed.
    front_desk = _auth(await _front_desk(client, admin))
    visit = await _service_headers(mesh, "visit")
    ack_path = f"/internal/schedule-changes/{uuid.uuid4()}/ack"
    for headers in (front_desk, visit):
        assert (await mesh.get(_feed(), headers=headers)).status_code == 403
        assert (await mesh.post(_feed(ack_path), headers=headers)).status_code == 403
    assert (await mesh.get(_feed())).status_code == 401
    assert (await mesh.post(_feed(ack_path))).status_code == 401
