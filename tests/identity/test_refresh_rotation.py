"""Refresh-token rotation and logout revocation (plan 02.1-02, carry-over C1).

A refresh token works exactly once: the exchange returns a NEW refresh token, the
old one is consumed atomically in the Redis denylist (`SET NX EX`), and logout
revokes the refresh token paired with the logged-out access token through the
access token's `rjti` claim. Redis being unreachable fails closed (503) and does
not consume the token.

Live Postgres/Mongo/Redis; every email is synthetic and uuid-suffixed.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import unique_email, verify_identity_token

PASSWORD = "correct-horse-battery"


async def _patient_login(client: AsyncClient) -> dict:
    email = unique_email("rotation")
    resp = await client.post("/api/auth/register", json={"email": email, "password": PASSWORD})
    assert resp.status_code == 201, resp.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def _refresh(client: AsyncClient, refresh_token: str):
    return await client.post("/api/auth/refresh", json={"refresh_token": refresh_token})


@pytest.mark.integration
async def test_tracer_refresh_rotates_and_logout_kills_the_paired_refresh_token(
    client: AsyncClient,
) -> None:
    tokens = await _patient_login(client)
    r1 = tokens["refresh_token"]

    first = await _refresh(client, r1)
    assert first.status_code == 200, first.text
    r2 = first.json()["refresh_token"]
    assert r2 != r1

    replay = await _refresh(client, r1)
    assert replay.status_code == 401

    second = await _refresh(client, r2)
    assert second.status_code == 200, second.text

    # Logout must kill the refresh token that was issued with the access token.
    fresh = await _patient_login(client)
    headers = {"Authorization": f"Bearer {fresh['access_token']}"}
    logout = await client.post("/api/auth/logout", headers=headers)
    assert logout.status_code == 204, logout.text
    after_logout = await _refresh(client, fresh["refresh_token"])
    assert after_logout.status_code == 401


@pytest.mark.integration
async def test_access_token_carries_the_paired_refresh_jti(client: AsyncClient) -> None:
    tokens = await _patient_login(client)
    access = verify_identity_token(tokens["access_token"])
    refresh = verify_identity_token(tokens["refresh_token"])
    assert access["rjti"] == refresh["jti"]
    assert "rjti" not in refresh

    rotated = (await _refresh(client, tokens["refresh_token"])).json()
    new_access = verify_identity_token(rotated["access_token"])
    new_refresh = verify_identity_token(rotated["refresh_token"])
    assert new_access["rjti"] == new_refresh["jti"]
    assert new_refresh["jti"] != refresh["jti"]


@pytest.mark.integration
async def test_logout_with_a_rotated_access_token_revokes_the_current_refresh_token(
    client: AsyncClient,
) -> None:
    tokens = await _patient_login(client)
    rotated = (await _refresh(client, tokens["refresh_token"])).json()
    headers = {"Authorization": f"Bearer {rotated['access_token']}"}
    assert (await client.post("/api/auth/logout", headers=headers)).status_code == 204
    assert (await _refresh(client, rotated["refresh_token"])).status_code == 401
