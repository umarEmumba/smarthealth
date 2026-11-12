"""Clinic list pagination (C5), through the gateway against live Postgres."""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, unique_email
from tests.pagination_helpers import list_all


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _create_clinics(client: AsyncClient, tokens: dict, count: int) -> list[str]:
    tag = uuid.uuid4().hex[:8]
    ids = []
    for n in range(count):
        response = await client.post(
            "/api/clinics",
            json={"name": f"Pagination Clinic {tag}-{n}", "timezone": "UTC"},
            headers=_auth(tokens),
        )
        assert response.status_code == 201, response.text
        ids.append(response.json()["id"])
    return ids


@pytest.mark.integration
async def test_pages_of_two_never_overlap_and_cover_every_created_clinic(
    client: AsyncClient,
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    created = await _create_clinics(client, tokens, 5)

    first = await client.get("/api/clinics", params={"limit": 2}, headers=_auth(tokens))
    assert first.status_code == 200, first.text
    assert len(first.json()) == 2
    total = int(first.headers["X-Total-Count"])
    assert total >= 5

    seen: list[str] = []
    offset = 0
    while offset < total:
        page = await client.get(
            "/api/clinics", params={"limit": 2, "offset": offset}, headers=_auth(tokens)
        )
        assert page.status_code == 200, page.text
        assert len(page.json()) <= 2
        seen.extend(c["id"] for c in page.json())
        offset += 2

    assert len(seen) == len(set(seen)), "pages overlapped"
    assert set(created) <= set(seen), "a created clinic was never returned"
    assert [i for i in seen if i in set(created)] == created  # stable order


@pytest.mark.integration
async def test_include_inactive_works_with_paging_and_the_total_follows_it(
    client: AsyncClient,
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    created = await _create_clinics(client, tokens, 2)
    deactivated = await client.post(f"/api/clinics/{created[0]}/deactivate", headers=_auth(tokens))
    assert deactivated.status_code == 200

    active = await client.get("/api/clinics", params={"limit": 1}, headers=_auth(tokens))
    everyone = await client.get(
        "/api/clinics", params={"limit": 1, "include_inactive": "true"}, headers=_auth(tokens)
    )
    assert int(everyone.headers["X-Total-Count"]) > int(active.headers["X-Total-Count"])

    default_ids = {c["id"] for c in await list_all(client, "/api/clinics", _auth(tokens))}
    all_ids = {
        c["id"]
        for c in await list_all(client, "/api/clinics", _auth(tokens), {"include_inactive": "true"})
    }
    assert created[0] not in default_ids
    assert created[0] in all_ids
    assert created[1] in default_ids


@pytest.mark.integration
@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 201}, {"limit": -1}, {"offset": -1}, {"limit": "x"}, {"offset": "x"}],
)
async def test_invalid_paging_values_are_422(client: AsyncClient, params: dict) -> None:
    tokens = await bootstrap_admin_and_login(client)
    response = await client.get("/api/clinics", params=params, headers=_auth(tokens))
    assert response.status_code == 422


@pytest.mark.integration
async def test_non_admin_is_still_forbidden_on_the_paged_list(client: AsyncClient) -> None:
    email = unique_email("patient-page")
    registered = await client.post(
        "/api/auth/register", json={"email": email, "password": "correct-horse-battery"}
    )
    assert registered.status_code == 201, registered.text
    login = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    response = await client.get("/api/clinics", params={"limit": 2}, headers=_auth(login.json()))
    assert response.status_code == 403
    assert "X-Total-Count" not in response.headers
