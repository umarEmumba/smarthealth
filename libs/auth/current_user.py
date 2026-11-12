"""`get_current_user` — the single dependency every protected route uses, in
EVERY service.

Order of checks (IAM-02, IAM-03):
  1. Bearer credentials present, else 401 (never 403 — "who are you?" comes
     before "are you allowed?").
  2. `decode_token(..., expected_types=ACCESS_ONLY)` against the app's key
     resolver: RS256 pinned, `kid` required, signature + expiry + type checked
     locally. `TokenError` or an unknown `role` value -> 401; `KeysUnavailable`
     -> 503.
  3. The shared Redis jti denylist (`libs.auth.denylist.is_revoked`) -> 401 if
     revoked. Every service checks it, not only Identity, so a logged-out
     token stops working everywhere (STRIDE T-01.1-04).

The key resolver comes from `request.app.state.key_resolver`: Identity sets a
`StaticKeyResolver` over its own public key; every other service sets a
`JwksClient`. Verification never calls Identity per request.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from redis.exceptions import RedisError

from libs.auth.denylist import is_revoked
from libs.auth.roles import Role
from libs.auth.verification import ACCESS_ONLY, KeyResolver, KeysUnavailable, TokenError, decode_token
from libs.db.redis import get_redis
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger

log = get_logger(__name__)

# auto_error=False: HTTPBearer's own missing-credentials response is not
# guaranteed to be 401 across FastAPI versions; we raise it ourselves.
_bearer_scheme = HTTPBearer(auto_error=False)

_UNAUTHORIZED = {"WWW-Authenticate": "Bearer"}


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    sub: str
    role: Role
    clinic_id: str | None
    jti: str
    raw_token: str


def get_key_resolver(request: Request) -> KeyResolver:
    resolver = getattr(request.app.state, "key_resolver", None)
    if resolver is None:
        raise RuntimeError(
            "app.state.key_resolver is not set — create_app() must install a "
            "StaticKeyResolver (Identity) or a JwksClient (every other service)"
        )
    return resolver


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    redis: Redis = Depends(get_redis),
    resolver: KeyResolver = Depends(get_key_resolver),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
) -> AuthenticatedUser:
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="not authenticated",
            headers=_UNAUTHORIZED,
        )
    token = credentials.credentials

    try:
        claims = await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver, http=http)
    except TokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or expired token",
            headers=_UNAUTHORIZED,
        ) from exc
    except KeysUnavailable as exc:
        log.warning("auth.keys_unavailable")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication keys unavailable",
        ) from exc

    try:
        jti = claims["jti"]
        sub = claims["sub"]
        role = Role(claims["role"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or expired token",
            headers=_UNAUTHORIZED,
        ) from exc

    try:
        revoked = await is_revoked(jti, redis)
    except RedisError as exc:
        # Fail closed: if revocation cannot be checked, the request is refused.
        log.warning("auth.denylist_unavailable", error=type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="revocation check unavailable",
        ) from exc
    if revoked:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token has been revoked",
            headers=_UNAUTHORIZED,
        )

    return AuthenticatedUser(
        sub=sub,
        role=role,
        clinic_id=claims.get("clinic_id"),
        jti=jti,
        raw_token=token,
    )


def user_id_as_uuid(user: AuthenticatedUser) -> uuid.UUID:
    return uuid.UUID(user.sub)
