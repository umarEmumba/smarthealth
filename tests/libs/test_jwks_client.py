"""Unit tests for `libs.auth.jwks_client.JwksClient` (STRIDE T-01.1-05).

Driven through `httpx.MockTransport` with an injected fake clock — no network,
no sleeping. They lock the cache contract: verification NEVER calls Identity
per request; the only network call is a JWKS fetch on a cache miss or TTL
expiry, with miss-backoff and stale-key serving on failure.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from jose import jwt

from libs.auth.jwks_client import JwksClient
from libs.auth.verification import ACCESS_ONLY, KeysUnavailable, TokenError, decode_token
from services.identity.security.keys import SigningKey, generate_private_key_pem, load_signing_key

JWKS_URL = "http://identity.test/.well-known/jwks.json"


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeIdentity:
    """A controllable JWKS endpoint that counts the requests it receives."""

    def __init__(self, keys: list[dict[str, Any]]) -> None:
        self.keys = keys
        self.requests = 0
        self.fail = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        if self.fail:
            raise httpx.ConnectError("identity down", request=request)
        return httpx.Response(200, json={"keys": self.keys})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


def _make_key(tmp_path: Path, kid: str) -> SigningKey:
    path = tmp_path / f"{kid}.pem"
    path.write_text(generate_private_key_pem())
    return load_signing_key(path, kid)


@pytest.fixture
def key_a(tmp_path: Path) -> SigningKey:
    return _make_key(tmp_path, "key-a")


@pytest.fixture
def key_b(tmp_path: Path) -> SigningKey:
    return _make_key(tmp_path, "key-b")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def _client(clock: FakeClock, *, ttl: int = 300, backoff: int = 30) -> JwksClient:
    return JwksClient(JWKS_URL, cache_ttl_seconds=ttl, miss_backoff_seconds=backoff, clock=clock)


async def test_cached_kid_is_fetched_once(key_a: SigningKey, clock: FakeClock) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock)
    async with identity.client() as http:
        first = await jwks.get_key("key-a", http)
        second = await jwks.get_key("key-a", http)

    assert first == second == key_a.public_jwk
    assert identity.requests == 1
    assert jwks.fetch_count == 1


async def test_two_key_jwks_selects_second_key_by_kid(
    key_a: SigningKey, key_b: SigningKey, clock: FakeClock
) -> None:
    identity = FakeIdentity([key_a.public_jwk, key_b.public_jwk])
    jwks = _client(clock)
    now = int(time.time())
    claims = {"sub": "u", "role": "admin", "jti": "j", "type": "access", "iat": now, "exp": now + 900}
    token = jwt.encode(claims, key_b.private_pem, algorithm="RS256", headers={"kid": "key-b"})

    async with identity.client() as http:
        decoded = await decode_token(token, expected_types=ACCESS_ONLY, resolver=jwks, http=http)

    assert decoded["jti"] == "j"
    assert identity.requests == 1


async def test_unknown_kid_refetches_once_then_backs_off(
    key_a: SigningKey, clock: FakeClock
) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock)
    async with identity.client() as http:
        await jwks.get_key("key-a", http)  # warm the cache (request 1)
        clock.advance(31)  # past the miss backoff, still inside the TTL

        with pytest.raises(TokenError):
            await jwks.get_key("bogus", http)  # one refetch (request 2)
        assert identity.requests == 2

        clock.advance(5)  # inside the backoff window
        with pytest.raises(TokenError):
            await jwks.get_key("bogus", http)
        assert identity.requests == 2  # no request: a bogus-kid flood cannot hammer Identity


async def test_ttl_expiry_triggers_refetch(key_a: SigningKey, clock: FakeClock) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock, ttl=300)
    async with identity.client() as http:
        await jwks.get_key("key-a", http)
        clock.advance(299)
        await jwks.get_key("key-a", http)
        assert identity.requests == 1  # still fresh

        clock.advance(2)  # now past the TTL
        await jwks.get_key("key-a", http)
        assert identity.requests == 2


async def test_refetch_failure_serves_stale_cached_key(
    key_a: SigningKey, clock: FakeClock
) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock, ttl=300)
    async with identity.client() as http:
        await jwks.get_key("key-a", http)
        clock.advance(400)  # stale
        identity.fail = True

        stale = await jwks.get_key("key-a", http)  # Identity down: must not log everyone out

    assert stale == key_a.public_jwk
    assert identity.requests == 2


async def test_failure_with_empty_cache_raises_keys_unavailable(clock: FakeClock) -> None:
    identity = FakeIdentity([])
    identity.fail = True
    jwks = _client(clock)
    async with identity.client() as http:
        with pytest.raises(KeysUnavailable):
            await jwks.get_key("key-a", http)


async def test_missing_http_client_raises_keys_unavailable(clock: FakeClock) -> None:
    with pytest.raises(KeysUnavailable):
        await _client(clock).get_key("key-a", None)


async def test_non_rsa_and_non_rs256_entries_are_ignored(
    key_a: SigningKey, clock: FakeClock
) -> None:
    no_kid = {k: v for k, v in key_a.public_jwk.items() if k != "kid"}
    identity = FakeIdentity(
        [
            {"kty": "EC", "kid": "ec-key", "crv": "P-256", "x": "x", "y": "y"},
            dict(key_a.public_jwk, kid="hs-key", alg="HS256"),
            no_kid,
            key_a.public_jwk,
        ]
    )
    jwks = _client(clock)
    async with identity.client() as http:
        assert await jwks.get_key("key-a", http) == key_a.public_jwk
        for ignored in ("ec-key", "hs-key"):
            clock.advance(31)
            with pytest.raises(TokenError):
                await jwks.get_key(ignored, http)


async def test_empty_jwks_keeps_previously_cached_keys(
    key_a: SigningKey, clock: FakeClock
) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock, ttl=300)
    async with identity.client() as http:
        await jwks.get_key("key-a", http)
        clock.advance(400)
        identity.keys = []  # a malfunctioning endpoint, not "every key revoked"

        assert await jwks.get_key("key-a", http) == key_a.public_jwk


async def test_rotated_out_kid_is_rejected_after_refresh(
    key_a: SigningKey, key_b: SigningKey, clock: FakeClock
) -> None:
    identity = FakeIdentity([key_a.public_jwk])
    jwks = _client(clock, ttl=300)
    async with identity.client() as http:
        await jwks.get_key("key-a", http)
        clock.advance(400)
        identity.keys = [key_b.public_jwk]  # key-a retired, key-b published

        with pytest.raises(TokenError):
            await jwks.get_key("key-a", http)
        assert await jwks.get_key("key-b", http) == key_b.public_jwk
