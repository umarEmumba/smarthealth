"""Gateway rate-limit tests (STRIDE T-01.1-17 / T-01.1-18 / T-01.1-20).

The gateway is driven in-process through an `httpx.ASGITransport` with a fixed
ASGI client address, and its upstream is an `httpx.MockTransport` that records
every call — "never forwarded" is asserted by the recorder staying at the
expected count. Counters live in the real Redis container (integration), so the
atomic Lua window is exercised against the real server, not a fake.

The fail-open tests point the gateway at an unused port: the limiter must still
forward the request, and the degradation must be VISIBLE (warning log + metric).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
import pytest
import pytest_asyncio
import structlog
from redis.asyncio import Redis

from libs.observability.logging import configure_logging
from services.gateway import ratelimit
from services.gateway.app import create_app
from services.gateway.config import GatewaySettings
from services.gateway.ratelimit import (
    AUTH_RATE_LIMITED_PATHS,
    FIXED_WINDOW_LUA,
    RATE_LIMIT_WINDOW_SECONDS,
    RATELIMIT_FAIL_OPEN_TOTAL,
    check_rate_limit,
    rate_limit_group,
)

pytestmark = pytest.mark.integration

AUTH_LIMIT = 3
DEFAULT_LIMIT = 5
IP_A = "203.0.113.7"
IP_B = "203.0.113.8"
UNREACHABLE_REDIS_URL = "redis://127.0.0.1:1/0"


class Upstream:
    """MockTransport handler that records every request it is asked to serve."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, json={"ok": True})


@pytest_asyncio.fixture
async def upstream() -> Upstream:
    return Upstream()


@pytest_asyncio.fixture
async def make_client(
    upstream: Upstream, redis_client: Redis
) -> AsyncIterator[Callable[..., object]]:
    """Factory for gateway clients that share ONE app (hence one limiter) but
    present different client IPs. `redis_client` flushes counters per test."""
    app_holder: dict[str, object] = {}

    def _build_app(**overrides: object) -> object:
        settings = GatewaySettings(
            _env_file=None,
            gateway_rate_limit_auth_per_minute=AUTH_LIMIT,
            gateway_rate_limit_default_per_minute=DEFAULT_LIMIT,
            **overrides,
        )
        app = create_app(settings)
        app.state.internal_http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app_holder["app"] = app
        return app

    @asynccontextmanager
    async def _client(ip: str = IP_A, **overrides: object) -> AsyncIterator[httpx.AsyncClient]:
        app = app_holder.get("app") if not overrides else None
        if app is None:
            app = _build_app(**overrides)
        transport = httpx.ASGITransport(app=app, client=(ip, 5000))
        async with httpx.AsyncClient(transport=transport, base_url="http://gateway") as client:
            yield client

    try:
        yield _client
    finally:
        app = app_holder.get("app")
        if app is not None:
            await app.state.internal_http_client.aclose()  # type: ignore[attr-defined]


# --- pure helpers -----------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "group"),
    [
        ("/api/auth/login", "auth"),
        ("/api/auth/register", "auth"),
        ("/api/auth/refresh", "auth"),
        ("/api/auth/logout", "default"),
        ("/api/clinics", "default"),
        ("/.well-known/jwks.json", "default"),
    ],
)
def test_rate_limit_group(path: str, group: str) -> None:
    assert rate_limit_group(path) == group


def test_auth_paths_and_window_constants() -> None:
    assert AUTH_RATE_LIMITED_PATHS == {"/api/auth/login", "/api/auth/register", "/api/auth/refresh"}
    assert RATE_LIMIT_WINDOW_SECONDS == 60
    assert "INCR" in FIXED_WINDOW_LUA and "PEXPIRE" in FIXED_WINDOW_LUA


# --- limiter against real Redis ---------------------------------------------


async def test_counter_always_carries_a_ttl(redis_client: Redis) -> None:
    """T-01.1-20: INCR + conditional PEXPIRE is one atomic script, so a counter
    key can never exist without an expiry."""
    decision = await check_rate_limit(redis_client, group="auth", client_ip=IP_A, limit=2)
    assert decision.allowed is True
    ttl_ms = await redis_client.pttl(f"ratelimit:auth:{IP_A}")
    assert 0 < ttl_ms <= RATE_LIMIT_WINDOW_SECONDS * 1000


async def test_check_rate_limit_rejects_over_budget_with_bounded_retry_after(
    redis_client: Redis,
) -> None:
    for _ in range(2):
        assert (
            await check_rate_limit(redis_client, group="default", client_ip=IP_A, limit=2)
        ).allowed
    decision = await check_rate_limit(redis_client, group="default", client_ip=IP_A, limit=2)
    assert decision.allowed is False
    assert 1 <= decision.retry_after_seconds <= RATE_LIMIT_WINDOW_SECONDS


# --- gateway behaviour ------------------------------------------------------


async def test_auth_budget_is_enforced_and_over_limit_is_never_forwarded(
    make_client: Callable[..., object], upstream: Upstream
) -> None:
    async with make_client() as client:  # type: ignore[attr-defined]
        for _ in range(AUTH_LIMIT):
            assert (await client.post("/api/auth/login", json={})).status_code == 200
        blocked = await client.post("/api/auth/login", json={})

    assert blocked.status_code == 429
    assert blocked.json() == {"detail": "rate limit exceeded"}
    assert 1 <= int(blocked.headers["retry-after"]) <= RATE_LIMIT_WINDOW_SECONDS
    assert len(upstream.requests) == AUTH_LIMIT


async def test_login_register_refresh_share_one_auth_budget(
    make_client: Callable[..., object], upstream: Upstream
) -> None:
    async with make_client() as client:  # type: ignore[attr-defined]
        assert (await client.post("/api/auth/login", json={})).status_code == 200
        assert (await client.post("/api/auth/register", json={})).status_code == 200
        assert (await client.post("/api/auth/refresh", json={})).status_code == 200
        assert (await client.post("/api/auth/login", json={})).status_code == 429
    assert len(upstream.requests) == 3


async def test_default_group_has_its_own_budget(
    make_client: Callable[..., object], upstream: Upstream
) -> None:
    async with make_client() as client:  # type: ignore[attr-defined]
        for _ in range(AUTH_LIMIT):
            await client.post("/api/auth/login", json={})
        assert (await client.post("/api/auth/login", json={})).status_code == 429

        # The exhausted auth budget does not touch the default budget.
        for _ in range(DEFAULT_LIMIT):
            assert (await client.get("/api/clinics")).status_code == 200
        over = await client.get("/api/clinics")

    assert over.status_code == 429
    assert "retry-after" in over.headers
    assert len(upstream.requests) == AUTH_LIMIT + DEFAULT_LIMIT


async def test_second_client_ip_has_an_independent_budget(
    make_client: Callable[..., object], upstream: Upstream
) -> None:
    async with make_client(IP_A) as first, make_client(IP_B) as second:  # type: ignore[attr-defined]
        for _ in range(AUTH_LIMIT):
            await first.post("/api/auth/login", json={})
        assert (await first.post("/api/auth/login", json={})).status_code == 429
        assert (await second.post("/api/auth/login", json={})).status_code == 200


async def test_spoofed_forwarded_for_does_not_buy_a_fresh_budget(
    make_client: Callable[..., object], upstream: Upstream
) -> None:
    """Only the socket peer address counts; a client cannot rotate X-Forwarded-For."""
    async with make_client() as client:  # type: ignore[attr-defined]
        for index in range(AUTH_LIMIT):
            await client.post(
                "/api/auth/login", json={}, headers={"x-forwarded-for": f"198.51.100.{index}"}
            )
        blocked = await client.post(
            "/api/auth/login", json={}, headers={"x-forwarded-for": "198.51.100.99"}
        )
    assert blocked.status_code == 429


async def test_health_and_metrics_are_not_rate_limited(
    make_client: Callable[..., object],
) -> None:
    async with make_client() as client:  # type: ignore[attr-defined]
        for _ in range(DEFAULT_LIMIT + 3):
            assert (await client.get("/health")).status_code == 200
            # /metrics only answers on the internal metrics listener port.
            assert (await client.get("http://gateway:9100/metrics")).status_code == 200


# --- fail-open --------------------------------------------------------------


async def test_redis_outage_fails_open_visibly(
    make_client: Callable[..., object],
    upstream: Upstream,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """T-01.1-18: a limiter outage must forward the request AND be visible —
    a warning log line and a counter increment, never silence."""
    configure_logging()
    # A logger first used inside this test binds to THIS test's captured stdout.
    monkeypatch.setattr(ratelimit, "log", structlog.get_logger("test-ratelimit-failopen"))
    before = RATELIMIT_FAIL_OPEN_TOTAL._value.get()

    async with make_client(redis_url=UNREACHABLE_REDIS_URL) as client:  # type: ignore[attr-defined]
        response = await client.post("/api/auth/login", json={})

    assert response.status_code == 200
    assert len(upstream.requests) == 1
    assert RATELIMIT_FAIL_OPEN_TOTAL._value.get() == before + 1

    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    outage = [e for e in events if e.get("event") == "gateway.ratelimit.unavailable_fail_open"]
    assert len(outage) == 1
    assert outage[0]["level"] == "warning"
    assert outage[0]["group"] == "auth"
    # Group only: no client address, headers, path or body in the log record.
    assert IP_A not in json.dumps(outage[0])
