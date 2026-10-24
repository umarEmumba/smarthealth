"""Redis jti-denylist for JWT revocation (IAM-03).

Locked contract (checkpoint-resolved): logout/revocation adds the token's
`jti` to Redis with TTL = the token's remaining lifetime, so denylist entries
self-expire and Redis usage stays bounded (never accumulates stale
revocation records). Every authenticated request checks the denylist —
signature+expiry valid AND jti not denylisted — via
`services/identity/security/current_user.py::get_current_user`.

Keyed `revoked:{jti}` per RESEARCH Pattern 2.

Refresh-token rotation (C1) reuses the same keyspace through `consume_token`:
a refresh jti is "consumed" by being written to the denylist on first use, so a
replayed, concurrent or logged-out refresh token is refused by the one check.
"""

from __future__ import annotations

from redis.asyncio import Redis

_DENYLIST_KEY_PREFIX = "revoked:"


def _denylist_key(jti: str) -> str:
    return f"{_DENYLIST_KEY_PREFIX}{jti}"


async def revoke_token(jti: str, ttl_seconds: int, redis: Redis) -> None:
    """Add a token's jti to the denylist with TTL = its remaining life.

    A non-positive TTL (already-expired token) is a no-op — Redis rejects
    `ex<=0`, and an already-expired token needs no denylist entry since
    signature/expiry validation alone will already reject it.
    """
    if ttl_seconds <= 0:
        return
    await redis.set(_denylist_key(jti), "1", ex=ttl_seconds)


async def is_revoked(jti: str, redis: Redis) -> bool:
    """Check whether a jti has been revoked (logged out)."""
    return await redis.exists(_denylist_key(jti)) == 1


async def consume_token(jti: str, ttl_seconds: int, redis: Redis) -> bool:
    """Atomically claim a jti for single use. True only for the FIRST claimer.

    One `SET revoked:{jti} 1 NX EX ttl` both checks the denylist and consumes the
    jti: if the key already exists (the token was revoked at logout, or already
    exchanged) nothing is written and the result is False; otherwise the key is
    created and the result is True. Because it is a single Redis command, two
    concurrent callers presenting the same token cannot both win.

    A revoked token and an already-used one are deliberately indistinguishable
    to the caller. The TTL is floored at 1 second (Redis rejects `ex<=0`); an
    entry only needs to outlive the token itself, after which signature/expiry
    validation refuses it anyway.
    """
    claimed = await redis.set(_denylist_key(jti), "1", nx=True, ex=max(ttl_seconds, 1))
    return bool(claimed)
