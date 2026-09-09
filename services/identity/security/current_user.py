"""`get_current_user` — the single dependency every protected route uses.

Validates the bearer access token: (1) signature + expiry + type via
`libs.auth.jwt.decode_token` (algorithms allowlist enforced there), then (2)
a Redis jti-denylist check via `libs.auth.denylist.is_revoked` (IAM-03
revocation). Returns `AuthenticatedUser`. Raises 401 on any failure —
invalid signature, expired, wrong token type, or revoked jti.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis

from libs.auth.denylist import is_revoked
from libs.auth.jwt import TokenError, decode_token
from services.identity.domain.enums import Role

_bearer_scheme = HTTPBearer(auto_error=True)


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    sub: str
    role: Role
    clinic_id: str | None
    jti: str
    raw_token: str


async def get_redis() -> Redis:
    """FastAPI dependency yielding a Redis client for denylist checks.

    A fresh client per request is intentionally simple for Phase 1's
    tracer scope — connection pooling/reuse is a later optimization, not a
    correctness requirement this plan needs to solve.
    """
    from smarthealth.config import settings  # noqa: PLC0415

    client = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
    redis: Redis = Depends(get_redis),
) -> AuthenticatedUser:
    token = credentials.credentials

    try:
        claims = decode_token(token, expected_type="access")
    except TokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or expired token",
        ) from exc

    jti = claims["jti"]
    if await is_revoked(jti, redis):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token has been revoked",
        )

    return AuthenticatedUser(
        sub=claims["sub"],
        role=Role(claims["role"]),
        clinic_id=claims.get("clinic_id"),
        jti=jti,
        raw_token=token,
    )


def user_id_as_uuid(user: AuthenticatedUser) -> uuid.UUID:
    return uuid.UUID(user.sub)
