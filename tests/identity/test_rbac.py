"""RBAC enforcement tests at the HTTP boundary (Plan 01-03, Task 2, IAM-02).

Exercises `require_role` through the real app (`client` fixture), not the
dependency in isolation — proving the 403 originates from the server-side
guard on an actual request/response cycle, not merely a unit-level check.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, unique_email


async def _register_and_login(client: AsyncClient, *, role: str) -> dict:
    """Create a user with `role` and return its login token pair.

    CR-01: public `/api/auth/register` always creates a PATIENT account
    now, so a `role="patient"` request goes straight through it; any
    other role is created via the authenticated-admin-only
    `/api/auth/admin/users` route, using a throwaway bootstrap admin.
    """
    email = unique_email(role)
    if role == "patient":
        register_resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert register_resp.status_code == 201, register_resp.text
    else:
        admin_tokens = await bootstrap_admin_and_login(client)
        create_resp = await client.post(
            "/api/auth/admin/users",
            json={"email": email, "password": "correct-horse-battery", "role": role},
            headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
        )
        assert create_resp.status_code == 201, create_resp.text

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    return login_resp.json()


@pytest.mark.integration
async def test_forbidden_action_403(client: AsyncClient) -> None:
    """A front-desk-role token calling the admin-only route is rejected 403."""
    tokens = await _register_and_login(client, role="front_desk")
    resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_allowed_action_200(client: AsyncClient) -> None:
    """An admin-role token calling the admin-only route is allowed 200."""
    tokens = await _register_and_login(client, role="admin")
    resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.integration
async def test_missing_token_401(client: AsyncClient) -> None:
    """No Authorization header at all returns 401, not 403 — 401 means "who
    are you", 403 means "I know who you are and you can't do this".
    """
    resp = await client.get("/api/admin/ping")
    assert resp.status_code == 401


@pytest.mark.integration
async def test_patient_token_forbidden(client: AsyncClient) -> None:
    """A patient-role token is also rejected 403 on the admin-only route
    (not just front-desk) — proves the guard checks role membership, not a
    single hardcoded "wrong role" special case.
    """
    tokens = await _register_and_login(client, role="patient")
    resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert resp.status_code == 403
