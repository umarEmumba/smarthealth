"""TRACER (plan 01.1-02, Task 1): the full Phase 1 account lifecycle on the
extracted Identity service, against identity_db.

Covers IAM-01 (register + the duplicate/race edges), IAM-03 (login, refresh,
logout, revocation) and the locked "refresh re-derives role from the CURRENT
users row" contract. Every claim assertion verifies the RS256 signature against
Identity's public key (`verify_identity_token`) — no shared secret exists.

All tests need live Postgres/Mongo/Redis (`integration`).
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.identity.config import settings as identity_settings
from tests.conftest import bootstrap_admin_and_login, unique_email, verify_identity_token

pytestmark = pytest.mark.integration


async def test_register_login_refresh_logout_lifecycle(client: AsyncClient) -> None:
    email = unique_email("lifecycle")
    password = "lifecycle-password-1"

    register_resp = await client.post(
        "/api/auth/register", json={"email": email, "password": password}
    )
    assert register_resp.status_code == 201, register_resp.text
    assert register_resp.json()["role"] == "patient"

    login_resp = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert login_resp.status_code == 200, login_resp.text
    login_tokens = login_resp.json()
    login_claims = verify_identity_token(login_tokens["access_token"])
    assert login_claims["role"] == "patient"
    assert login_claims["type"] == "access"

    refresh_resp = await client.post(
        "/api/auth/refresh", json={"refresh_token": login_tokens["refresh_token"]}
    )
    assert refresh_resp.status_code == 200, refresh_resp.text
    refreshed = refresh_resp.json()
    assert refreshed["access_token"] != login_tokens["access_token"]
    assert verify_identity_token(refreshed["access_token"])["role"] == "patient"

    headers = {"Authorization": f"Bearer {refreshed['access_token']}"}
    assert (await client.post("/api/auth/logout", headers=headers)).status_code == 204
    # The revoked token must now be rejected: repeating the logout is a 401.
    assert (await client.post("/api/auth/logout", headers=headers)).status_code == 401


async def test_duplicate_registration_creates_exactly_one_account(client: AsyncClient) -> None:
    # IAM-01 idempotency edge: the second request is a 409 and the original
    # account still authenticates.
    email = unique_email("dup")
    password = "original-password-1"

    first = await client.post("/api/auth/register", json={"email": email, "password": password})
    assert first.status_code == 201, first.text
    second = await client.post(
        "/api/auth/register", json={"email": email, "password": "different-password-2"}
    )
    assert second.status_code == 409

    original = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert original.status_code == 200
    impostor = await client.post(
        "/api/auth/login", json={"email": email, "password": "different-password-2"}
    )
    assert impostor.status_code == 401


async def test_concurrent_duplicate_registration_yields_one_account(client: AsyncClient) -> None:
    # IAM-01 concurrency edge: both requests pass the check-then-insert guard,
    # so the users.email unique constraint is what stops the second one. It must
    # surface as a 409, never as a 500 or a second account.
    email = unique_email("race")
    password = "race-password-1"
    body = {"email": email, "password": password}

    responses = await asyncio.gather(
        client.post("/api/auth/register", json=body),
        client.post("/api/auth/register", json=body),
    )

    statuses = sorted(resp.status_code for resp in responses)
    assert 500 not in statuses
    assert statuses == [201, 409]

    login = await client.post("/api/auth/login", json=body)
    assert login.status_code == 200


async def test_concurrent_duplicate_staff_creation_yields_one_account(client: AsyncClient) -> None:
    # Same race on the admin-only staff route (T-01.1-09).
    admin_tokens = await bootstrap_admin_and_login(client)
    headers = {"Authorization": f"Bearer {admin_tokens['access_token']}"}
    body = {"email": unique_email("staff-race"), "password": "staff-password-1", "role": "provider"}

    responses = await asyncio.gather(
        client.post("/api/auth/admin/users", json=body, headers=headers),
        client.post("/api/auth/admin/users", json=body, headers=headers),
    )

    statuses = sorted(resp.status_code for resp in responses)
    assert 500 not in statuses
    assert statuses == [201, 409]


async def test_refresh_rederives_role_from_current_db(client: AsyncClient) -> None:
    admin_tokens = await bootstrap_admin_and_login(client)
    admin_headers = {"Authorization": f"Bearer {admin_tokens['access_token']}"}

    email = unique_email("rederive")
    password = "rederive-password-1"
    create_resp = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": password, "role": "provider"},
        headers=admin_headers,
    )
    assert create_resp.status_code == 201, create_resp.text
    user_id = create_resp.json()["id"]

    login_resp = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert login_resp.status_code == 200, login_resp.text
    tokens = login_resp.json()
    assert verify_identity_token(tokens["access_token"])["role"] == "provider"

    engine = create_async_engine(identity_settings.database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            await session.execute(
                text("UPDATE users SET role = 'front_desk' WHERE id = :id"), {"id": user_id}
            )
            await session.commit()
    finally:
        await engine.dispose()

    refresh_resp = await client.post(
        "/api/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    assert refresh_resp.status_code == 200, refresh_resp.text
    assert verify_identity_token(refresh_resp.json()["access_token"])["role"] == "front_desk"
