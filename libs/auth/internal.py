"""Authentication and clinic scoping for `/internal/*` endpoints.

Internal endpoints (e.g. Organization's clinic/department reference lookups,
D-05) are called by OTHER services at write time, forwarding the original
caller's access token (the callee re-applies RBAC and clinic scope itself).
They are not routed by the gateway at all; this module is the in-service half
of that isolation.

`require_internal_caller(*allowed_roles, audience=..., allowed_services=...)`
verifies the bearer token locally (RS256 via the app's key resolver, never a
call to Identity), checks the shared Redis denylist, and enforces a per-endpoint
allowlist. It accepts a forwarded user ACCESS token or a SERVICE token
(`INTERNAL_CALLER_TYPES`); the latter is used when a call has no user context
(anonymous registration validating a clinic, Temporal activities, D-06) and is
signed by Identity with `type=service`, `sub=service:<name>` and `aud=<target>`.
`InternalCaller.kind` distinguishes the two:

  * user callers: the role allowlist applies (`allowed_roles`); user tokens carry
    no `aud`, so they behave exactly as before.
  * service callers (D-06, research G2, review WR-02): the token's `aud` MUST be
    present and name `audience` (this service) — 401 otherwise — and the calling
    service MUST be in the route's `allowed_services` — 403 otherwise. An empty
    `allowed_services` means no service may call the route.

Public routes keep using `get_current_user`, which accepts access tokens only,
so a service token can never authenticate a public request.

`clinic_scope_permits` is the clinic-scope rule: an out-of-scope target is
treated by endpoints exactly like a missing one (same 404), so existence cannot
be probed across clinics.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from redis.exceptions import RedisError

from libs.auth.current_user import get_key_resolver
from libs.auth.denylist import is_revoked
from libs.auth.roles import Role
from libs.auth.verification import (
    INTERNAL_CALLER_TYPES,
    SERVICE_TOKEN_TYPE,
    KeyResolver,
    KeysUnavailable,
    TokenError,
    decode_token,
)
from libs.db.redis import get_redis
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger

log = get_logger(__name__)

# auto_error=False: we raise 401 ourselves (HTTPBearer's own status for missing
# credentials is not guaranteed to be 401 across FastAPI versions).
_bearer_scheme = HTTPBearer(auto_error=False)

_UNAUTHORIZED = {"WWW-Authenticate": "Bearer"}

SERVICE_SUBJECT_PREFIX = "service:"


@dataclass(frozen=True, slots=True)
class InternalCaller:
    kind: Literal["user", "service"]
    sub: str
    role: Role | None
    clinic_id: str | None
    raw_token: str
    # Name after the `service:` prefix for service callers; None for user callers.
    service_name: str | None = None


def _audience_matches(aud: object, audience: str) -> bool:
    if isinstance(aud, str):
        return aud == audience
    if isinstance(aud, list):
        return audience in aud
    return False


def require_internal_caller(
    *allowed_roles: Role,
    audience: str,
    allowed_services: frozenset[str] = frozenset(),
) -> Callable[..., InternalCaller]:
    """Dependency factory: an authenticated internal caller — a user whose role is
    in `allowed_roles`, or a service named in `allowed_services` presenting a
    token whose `aud` is `audience` (401 unauthenticated/revoked/wrong or missing
    aud, 403 wrong role or unlisted service, 503 when keys or the denylist are
    unavailable — fail closed)."""
    allowed_services = frozenset(allowed_services)

    async def _dependency(
        credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
        redis: Redis = Depends(get_redis),
        resolver: KeyResolver = Depends(get_key_resolver),
        http: httpx.AsyncClient = Depends(get_internal_http_client),
    ) -> InternalCaller:
        if credentials is None or not credentials.credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="not authenticated",
                headers=_UNAUTHORIZED,
            )
        token = credentials.credentials

        try:
            claims = await decode_token(
                token,
                expected_types=INTERNAL_CALLER_TYPES,
                resolver=resolver,
                http=http,
                audience=audience,
            )
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

        is_service = claims.get("type") == SERVICE_TOKEN_TYPE
        try:
            jti = claims["jti"]
            sub = claims["sub"]
            # A service token carries no role; a user token must carry a valid one.
            role = None if is_service else Role(claims["role"])
            if is_service and not (isinstance(sub, str) and sub.startswith(SERVICE_SUBJECT_PREFIX)):
                raise ValueError("service token subject must start with 'service:'")
            # python-jose skips the audience check when `aud` is absent, so a
            # service token must be proven to carry OUR audience here.
            if is_service and not _audience_matches(claims.get("aud"), audience):
                raise ValueError("service token audience does not name this service")
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

        if is_service:
            service_name = sub.removeprefix(SERVICE_SUBJECT_PREFIX)
            if service_name not in allowed_services:
                log.info("auth.internal_service_refused", service=service_name, audience=audience)
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="service not permitted for this operation",
                )
            return InternalCaller(
                kind="service",
                sub=sub,
                role=None,
                clinic_id=None,
                raw_token=token,
                service_name=service_name,
            )

        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="insufficient role for this operation",
            )

        return InternalCaller(
            kind="user",
            sub=sub,
            role=role,
            clinic_id=claims.get("clinic_id"),
            raw_token=token,
        )

    return _dependency


def clinic_scope_permits(caller: InternalCaller, target_clinic_id: uuid.UUID | None) -> bool:
    """May `caller` see a record that belongs to `target_clinic_id`?

    True for service callers, ADMIN callers, callers carrying no clinic claim
    (the calling service has already applied its own ownership/scope rule, e.g.
    CR-02 provider ownership), and targets that have no clinic. Otherwise the
    caller's clinic claim must equal the target clinic; a malformed claim
    never matches.
    """
    if caller.kind == "service" or caller.role is Role.ADMIN:
        return True
    if caller.clinic_id is None or target_clinic_id is None:
        return True
    try:
        return uuid.UUID(caller.clinic_id) == target_clinic_id
    except ValueError:
        return False
