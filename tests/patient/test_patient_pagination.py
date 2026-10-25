"""Patient list pagination (C5), through the gateway against live Postgres."""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, unique_email
from tests.pagination_helpers import list_all

PASSWORD = "correct-horse-battery"


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _clinic(client: AsyncClient, admin: dict, name: str) -> str:
    response = await client.post(
        "/api/clinics",
        json={"name": f"{name} {uuid.uuid4().hex[:8]}", "timezone": "UTC"},
        headers=_auth(admin),
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _patient(client: AsyncClient, admin: dict, clinic_id: str, n: int) -> str:
    response = await client.post(
        "/api/patients",
        json={
            "clinic_id": clinic_id,
            "full_name": f"Zzyzx Synthetic {n}",
            "date_of_birth": "1990-01-01",
        },
        headers=_auth(admin),
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _front_desk(client: AsyncClient, admin: dict, clinic_id: str) -> dict:
    email = unique_email("fd-page")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk", "clinic_id": clinic_id},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def _page(client, tokens, clinic_id, **params):
    return await client.get(
        "/api/patients",
        params={"clinic_id": clinic_id, **params},
        headers=_auth(tokens),
    )


@pytest.mark.integration
async def test_patient_list_pages_cover_every_row_once_with_total_header(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Patients")
    created = [await _patient(client, admin, clinic_id, n) for n in range(7)]

    first = await _page(client, admin, clinic_id, limit=3, offset=0)
    second = await _page(client, admin, clinic_id, limit=3, offset=3)
    third = await _page(client, admin, clinic_id, limit=3, offset=6)
    for response in (first, second, third):
        assert response.status_code == 200, response.text
        assert response.headers["X-Total-Count"] == "7"

    sizes = [len(first.json()), len(second.json()), len(third.json())]
    assert sizes == [3, 3, 1]
    ids = [p["id"] for r in (first, second, third) for p in r.json()]
    assert len(set(ids)) == 7
    assert ids == created  # (created_at, id) order == creation order


@pytest.mark.integration
async def test_default_page_returns_all_small_lists_and_reports_total(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Default")
    for n in range(2):
        await _patient(client, admin, clinic_id, n)

    response = await _page(client, admin, clinic_id)
    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.headers["X-Total-Count"] == "2"


@pytest.mark.integration
async def test_row_added_between_pages_does_not_duplicate_or_skip(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Concurrent")
    original = [await _patient(client, admin, clinic_id, n) for n in range(6)]

    first = await _page(client, admin, clinic_id, limit=3, offset=0)
    late = await _patient(client, admin, clinic_id, 99)
    second = await _page(client, admin, clinic_id, limit=3, offset=3)
    third = await _page(client, admin, clinic_id, limit=3, offset=6)

    seen = [p["id"] for r in (first, second, third) for p in r.json()]
    assert len(seen) == len(set(seen)), "a row was returned twice"
    assert seen[:6] == original, "an earlier row was skipped or reordered"
    assert seen[6:] == [late]
    assert second.headers["X-Total-Count"] == "7"


@pytest.mark.integration
async def test_offset_beyond_the_end_is_empty_with_the_same_total(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Beyond")
    for n in range(3):
        await _patient(client, admin, clinic_id, n)

    response = await _page(client, admin, clinic_id, limit=10, offset=500)
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "3"


@pytest.mark.integration
async def test_total_follows_the_include_inactive_filter(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Inactive")
    ids = [await _patient(client, admin, clinic_id, n) for n in range(3)]
    deactivated = await client.post(
        f"/api/patients/{ids[0]}/deactivate", params={"clinic_id": clinic_id}, headers=_auth(admin)
    )
    assert deactivated.status_code == 200

    active = await _page(client, admin, clinic_id)
    everyone = await _page(client, admin, clinic_id, include_inactive="true")
    assert active.headers["X-Total-Count"] == "2"
    assert len(active.json()) == 2
    assert everyone.headers["X-Total-Count"] == "3"
    assert len(everyone.json()) == 3


@pytest.mark.integration
@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 201}, {"limit": -1}, {"offset": -1}, {"limit": "x"}, {"offset": "x"}],
)
async def test_invalid_paging_values_are_422(client: AsyncClient, params: dict) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Invalid")
    response = await _page(client, admin, clinic_id, **params)
    assert response.status_code == 422


@pytest.mark.integration
async def test_scope_rules_run_before_paging(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_a = await _clinic(client, admin, "Pagination Scope A")
    clinic_b = await _clinic(client, admin, "Pagination Scope B")
    await _patient(client, admin, clinic_b, 1)
    front_desk_a = await _front_desk(client, admin, clinic_a)

    other = await _page(client, front_desk_a, clinic_b, limit=5, offset=0)
    assert other.status_code == 403
    assert "X-Total-Count" not in other.headers

    own = await _page(client, front_desk_a, clinic_a, limit=5, offset=0)
    assert own.status_code == 200
    assert own.json() == []
    assert own.headers["X-Total-Count"] == "0"

    missing = await _page(client, admin, str(uuid.uuid4()), limit=5)
    assert missing.status_code == 404


@pytest.mark.integration
async def test_list_all_helper_pages_past_one_page(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Pagination Helper")
    created = [await _patient(client, admin, clinic_id, n) for n in range(4)]

    rows = await list_all(client, "/api/patients", _auth(admin), {"clinic_id": clinic_id})
    assert [r["id"] for r in rows] == created
