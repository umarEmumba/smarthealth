"""Provider list pagination (C5), through the gateway against live Postgres."""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, unique_email
from tests.pagination_helpers import list_all


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _department(client: AsyncClient, admin: dict) -> str:
    clinic = await client.post(
        "/api/clinics",
        json={"name": f"Provider Pagination {uuid.uuid4().hex[:8]}", "timezone": "UTC"},
        headers=_auth(admin),
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments",
        json={"name": "Pagination Dept"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    return department.json()["id"]


async def _providers(client: AsyncClient, admin: dict, department_id: str, count: int) -> list[str]:
    ids = []
    for n in range(count):
        response = await client.post(
            "/api/providers",
            json={
                "department_id": department_id,
                "full_name": f"Dr. Paged {n}",
                "specialties": [f"Specialty {n}"],
            },
            headers=_auth(admin),
        )
        assert response.status_code == 201, response.text
        ids.append(response.json()["id"])
    return ids


@pytest.mark.integration
async def test_pages_of_two_cover_the_department_once_with_specialties_on_every_row(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    department_id = await _department(client, admin)
    created = await _providers(client, admin, department_id, 5)

    pages = []
    for offset in (0, 2, 4):
        response = await client.get(
            "/api/providers",
            params={"department_id": department_id, "limit": 2, "offset": offset},
            headers=_auth(admin),
        )
        assert response.status_code == 200, response.text
        assert response.headers["X-Total-Count"] == "5"
        pages.append(response.json())

    assert [len(p) for p in pages] == [2, 2, 1]
    rows = [p for page in pages for p in page]
    assert [r["id"] for r in rows] == created
    for row in rows:
        assert len(row["specialties"]) == 1, "specialties must be eagerly loaded on every paged row"
        assert row["specialties"][0]["name"].startswith("Specialty ")


@pytest.mark.integration
async def test_department_filter_and_include_inactive_keep_working_with_paging(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    department_a = await _department(client, admin)
    department_b = await _department(client, admin)
    in_a = await _providers(client, admin, department_a, 3)
    in_b = await _providers(client, admin, department_b, 1)
    deactivated = await client.post(f"/api/providers/{in_a[0]}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200

    active = await client.get(
        "/api/providers", params={"department_id": department_a, "limit": 1}, headers=_auth(admin)
    )
    everyone = await client.get(
        "/api/providers",
        params={"department_id": department_a, "limit": 1, "include_inactive": "true"},
        headers=_auth(admin),
    )
    assert active.headers["X-Total-Count"] == "2"
    assert everyone.headers["X-Total-Count"] == "3"

    ids_a = {
        r["id"]
        for r in await list_all(
            client, "/api/providers", _auth(admin), {"department_id": department_a}
        )
    }
    assert ids_a == set(in_a[1:])
    assert in_b[0] not in ids_a


@pytest.mark.integration
async def test_offset_beyond_the_end_is_empty_with_the_same_total(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    department_id = await _department(client, admin)
    await _providers(client, admin, department_id, 2)

    response = await client.get(
        "/api/providers",
        params={"department_id": department_id, "limit": 5, "offset": 100},
        headers=_auth(admin),
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "2"


@pytest.mark.integration
@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 201}, {"limit": -1}, {"offset": -1}, {"limit": "x"}, {"offset": "x"}],
)
async def test_invalid_paging_values_are_422(client: AsyncClient, params: dict) -> None:
    admin = await bootstrap_admin_and_login(client)
    response = await client.get(
        "/api/providers", params={"department_id": str(uuid.uuid4()), **params}, headers=_auth(admin)
    )
    assert response.status_code == 422


@pytest.mark.integration
async def test_non_admin_is_still_forbidden_on_the_paged_list(client: AsyncClient) -> None:
    email = unique_email("provider-page")
    registered = await client.post(
        "/api/auth/register", json={"email": email, "password": "correct-horse-battery"}
    )
    assert registered.status_code == 201, registered.text
    login = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    response = await client.get(
        "/api/providers",
        params={"department_id": str(uuid.uuid4()), "limit": 2},
        headers=_auth(login.json()),
    )
    assert response.status_code == 403
    assert "X-Total-Count" not in response.headers
