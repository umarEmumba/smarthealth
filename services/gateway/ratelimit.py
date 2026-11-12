"""Per-client-IP fixed-window rate limiting at the gateway edge (roadmap SC3).

Design (RESEARCH "Don't Hand-Roll" and Pitfall 4):
  * ONE atomic Lua script per request: INCR the window counter and set its
    expiry only when the counter was just created, then return the count and the
    remaining TTL. INCR and PEXPIRE are never separate round trips, so a crash or
    a lost connection between them cannot leave a counter without an expiry
    (STRIDE T-01.1-20).
  * Two budgets per client address: a stricter `auth` group shared by
    login/register/refresh, and a `default` group for everything else.
  * The limiter FAILS OPEN when Redis is unreachable: an outage of the counter
    store must not take the whole platform down (RESEARCH assumption A1). The
    degradation is never silent: it logs `gateway.ratelimit.unavailable_fail_open`
    (group only) and increments `gateway_ratelimit_fail_open_total`
    (STRIDE T-01.1-18).
  * The key uses the socket peer address (`request.client.host`) only. A
    client-supplied X-Forwarded-For is never trusted at the edge, so rotating it
    cannot buy a fresh budget.

Logs carry the rate-limit group only: never the client address, headers, tokens
or bodies (PHI / credential safety).
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass

from prometheus_client import Counter
from redis.asyncio import Redis
from redis.exceptions import RedisError

from libs.observability.logging import get_logger

log = get_logger(__name__)

RATE_LIMIT_WINDOW_SECONDS = 60

# Credential-bearing endpoints share one stricter budget (brute force, credential
# stuffing, registration spam). Everything else uses the default budget.
AUTH_RATE_LIMITED_PATHS: frozenset[str] = frozenset(
    {"/api/auth/login", "/api/auth/register", "/api/auth/refresh"}
)

# KEYS[1] = counter key, ARGV[1] = window in milliseconds.
# Returns {count, remaining_ttl_ms}. PEXPIRE only when the key was just created,
# in the same script as the INCR, so a counter can never exist without a TTL.
FIXED_WINDOW_LUA = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {count, ttl}
"""

RATELIMIT_FAIL_OPEN_TOTAL = Counter(
    "gateway_ratelimit_fail_open_total",
    "Requests allowed because the rate-limit store was unreachable",
)

# Short timeouts: a slow Redis must not add seconds of latency to every request.
_REDIS_TIMEOUT_SECONDS = 0.5

_clients: dict[str, tuple[Redis, asyncio.AbstractEventLoop]] = {}


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int


def rate_limit_group(path: str) -> str:
    """Budget group for a request path: "auth" or "default"."""
    return "auth" if path in AUTH_RATE_LIMITED_PATHS else "default"


def get_rate_limit_redis(redis_url: str) -> Redis:
    """Loop-aware cached client per URL.

    A `redis.asyncio` client's connection pool binds to the event loop it was
    first used on (same reason as the internal HTTP client), so a client created
    on a different loop is replaced rather than reused.
    """
    loop = asyncio.get_running_loop()
    cached = _clients.get(redis_url)
    if cached is not None and cached[1] is loop:
        return cached[0]
    client = Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=_REDIS_TIMEOUT_SECONDS,
        socket_timeout=_REDIS_TIMEOUT_SECONDS,
    )
    _clients[redis_url] = (client, loop)
    return client


async def check_rate_limit(
    redis: Redis, *, group: str, client_ip: str, limit: int
) -> RateLimitDecision:
    """Count this request against `client_ip`'s `group` window.

    Allowed while the in-window count is <= `limit`. When refused,
    `retry_after_seconds` is the whole seconds until the window resets, clamped
    to 1..RATE_LIMIT_WINDOW_SECONDS. Fails OPEN (visibly) if Redis is unreachable.
    """
    key = f"ratelimit:{group}:{client_ip}"
    window_ms = RATE_LIMIT_WINDOW_SECONDS * 1000
    try:
        count, ttl_ms = await redis.eval(FIXED_WINDOW_LUA, 1, key, window_ms)  # type: ignore[misc]
    except (RedisError, OSError):
        log.warning("gateway.ratelimit.unavailable_fail_open", group=group)
        RATELIMIT_FAIL_OPEN_TOTAL.inc()
        return RateLimitDecision(allowed=True, retry_after_seconds=0)

    if int(count) <= limit:
        return RateLimitDecision(allowed=True, retry_after_seconds=0)

    ttl = int(ttl_ms)
    remaining = RATE_LIMIT_WINDOW_SECONDS if ttl <= 0 else math.ceil(ttl / 1000)
    return RateLimitDecision(
        allowed=False,
        retry_after_seconds=max(1, min(RATE_LIMIT_WINDOW_SECONDS, remaining)),
    )
