"""Cross-service behaviour of Organization: revocation, no cascade, no reverse
calls (IAM-03, D-06).

* A token revoked by logging out through Identity is rejected by Organization
  on its very next request: both services read the SAME Redis denylist and
  keep no per-service session state.
* Deactivating a department (or a clinic) makes no call to any other service
  and cascades nothing — references held elsewhere keep their ids (D-06).
* A department create racing its clinic's deactivation stays inside
  Organization's own transaction and keeps the intra-service foreign key.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from tests.conftest import bootstrap_admin_and_login, harness_apps, service_base_url


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _create_clinic(client: AsyncClient, tokens: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(tokens)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


@pytest.mark.integration
async def test_logout_through_identity_revokes_the_token_for_organization(
    client: AsyncClient,
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    before = await client.get("/api/clinics", headers=_auth(tokens))
    assert before.status_code == 200, before.text

    logout = await client.post("/api/auth/logout", headers=_auth(tokens))
    assert logout.status_code == 204, logout.text

    after = await client.get("/api/clinics", headers=_auth(tokens))
    assert after.status_code == 401
    # ...and every other Organization route, not just the list.
    write = await client.post(
        "/api/clinics", json={"name": "Revoked", "timezone": "UTC"}, headers=_auth(tokens)
    )
    assert write.status_code == 401


@pytest.mark.integration
async def test_deactivating_a_department_calls_no_other_service(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, tokens, "No Reverse Call Clinic")
    created = await client.post(
        f"/api/clinics/{clinic_id}/departments", json={"name": "Quiet"}, headers=_auth(tokens)
    )
    assert created.status_code == 201, created.text
    department_id = created.json()["id"]

    # Warm Organization's JWKS cache first: the one legitimate outbound call it
    # ever makes is that cached key fetch, which is not a business call.
    warm = await client.get("/api/clinics", headers=_auth(tokens))
    assert warm.status_code == 200

    recorded: list[httpx.URL] = []

    async def record(request: httpx.Request) -> None:
        recorded.append(request.url)

    organization = harness_apps()["organization"]
    mounts = {
        service_base_url(name): ASGITransport(app=app) for name, app in harness_apps().items()
    }
    async with httpx.AsyncClient(mounts=mounts, event_hooks={"request": [record]}) as recorder:
        organization.state.internal_http_client = recorder
        try:
            deactivated = await client.post(
                f"/api/clinics/{clinic_id}/departments/{department_id}/deactivate",
                headers=_auth(tokens),
            )
        finally:
            organization.state.internal_http_client = mesh

    assert deactivated.status_code == 200, deactivated.text
    assert deactivated.json()["is_active"] is False
    # Organization issued no outbound request at all during the deactivation
    # (in particular none to a host other than its own).
    assert [url for url in recorded if url.host != "organization"] == []
    assert recorded == []


@pytest.mark.integration
async def test_clinic_deactivation_does_not_cascade_to_its_departments(
    client: AsyncClient,
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, tokens, "No Cascade Clinic")
    created = await client.post(
        f"/api/clinics/{clinic_id}/departments", json={"name": "Stays Active"}, headers=_auth(tokens)
    )
    assert created.status_code == 201, created.text

    deactivated = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(tokens))
    assert deactivated.status_code == 200

    listed = await client.get(f"/api/clinics/{clinic_id}/departments", headers=_auth(tokens))
    assert listed.status_code == 200
    assert [(d["id"], d["is_active"]) for d in listed.json()] == [(created.json()["id"], True)]


@pytest.mark.integration
async def test_department_creates_racing_clinic_deactivation_leave_no_orphans(
    client: AsyncClient,
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, tokens, "Race Clinic")

    creates = [
        client.post(
            f"/api/clinics/{clinic_id}/departments",
            json={"name": f"Racer {index}"},
            headers=_auth(tokens),
        )
        for index in range(5)
    ]
    deactivate = client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(tokens))

    *create_responses, deactivate_response = await asyncio.gather(*creates, deactivate)

    # Each interleaving is acceptable (the clinic row exists either way and
    # soft-delete never removes it); a 5xx or a dangling row is not.
    assert deactivate_response.status_code == 200
    assert [r.status_code for r in create_responses] == [201] * 5

    listed = await client.get(
        f"/api/clinics/{clinic_id}/departments",
        params={"include_inactive": "true"},
        headers=_auth(tokens),
    )
    assert listed.status_code == 200
    assert len(listed.json()) == 5
    assert {d["clinic_id"] for d in listed.json()} == {clinic_id}
