"""Refresh-token rotation and logout revocation (plan 02.1-02, carry-over C1).

A refresh token works exactly once: the exchange returns a NEW refresh token, the
old one is consumed atomically in the Redis denylist (`SET NX EX`), and logout
revokes the refresh token paired with the logged-out access token through the
access token's `rjti` claim. Redis being unreachable fails closed (503) and does
not consume the token.

Live Postgres/Mongo/Redis; every email is synthetic and uuid-suffixed.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from libs.auth.denylist import is_revoked
from libs.config import shared_settings
from services.identity.security.signing import _encode, create_refresh_token
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


@pytest.mark.integration
async def test_eight_concurrent_refreshes_of_one_token_make_exactly_one_success(
    client: AsyncClient,
) -> None:
    tokens = await _patient_login(client)
    responses = await asyncio.gather(*[_refresh(client, tokens["refresh_token"]) for _ in range(8)])
    codes = sorted(r.status_code for r in responses)
    assert codes == [200] + [401] * 7, codes


@pytest.mark.integration
async def test_access_token_garbage_and_expired_tokens_are_still_401(client: AsyncClient) -> None:
    tokens = await _patient_login(client)
    assert (await _refresh(client, tokens["access_token"])).status_code == 401
    assert (await _refresh(client, "not-a-jwt")).status_code == 401
    assert (await _refresh(client, "")).status_code == 401

    # The refusals above must not have consumed the real refresh token.
    assert (await _refresh(client, tokens["refresh_token"])).status_code == 200


@pytest.mark.integration
async def test_refresh_for_a_vanished_user_is_401_and_does_not_burn_the_token(
    client: AsyncClient,
) -> None:
    token, jti = create_refresh_token(str(uuid.uuid4()))
    resp = await _refresh(client, token)
    assert resp.status_code == 401

    redis = Redis.from_url(shared_settings.redis_url, decode_responses=True)
    try:
        assert await is_revoked(jti, redis) is False
    finally:
        await redis.aclose()


@pytest.mark.integration
async def test_redis_outage_during_refresh_is_503_and_the_token_is_not_consumed(
    client: AsyncClient,
) -> None:
    tokens = await _patient_login(client)

    with patch(
        "services.identity.api.auth.consume_token",
        side_effect=RedisConnectionError("redis down"),
    ):
        down = await _refresh(client, tokens["refresh_token"])
    assert down.status_code == 503

    after_recovery = await _refresh(client, tokens["refresh_token"])
    assert after_recovery.status_code == 200, after_recovery.text


@pytest.mark.integration
async def test_expired_refresh_token_is_401(client: AsyncClient) -> None:
    tokens = await _patient_login(client)
    sub = verify_identity_token(tokens["refresh_token"])["sub"]
    now = datetime.now(timezone.utc)
    expired = _encode(
        {
            "sub": sub,
            "jti": str(uuid.uuid4()),
            "type": "refresh",
            "iat": now - timedelta(days=8),
            "exp": now - timedelta(days=1),
        }
    )
    assert (await _refresh(client, expired)).status_code == 401
