"""Redis jti-denylist for JWT revocation (IAM-03).

Locked contract (checkpoint-resolved): logout/revocation adds the token's
`jti` to Redis with TTL = the token's remaining lifetime, so denylist entries
self-expire and Redis usage stays bounded (never accumulates stale
revocation records). Every authenticated request checks the denylist —
signature+expiry valid AND jti not denylisted — via
`services/identity/security/current_user.py::get_current_user`.

Keyed `revoked:{jti}` per RESEARCH Pattern 2.
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
