"""JWT issuance — Identity-only (it alone holds the private key).

Locked claim contract (unchanged from Phase 1): the short-lived ACCESS token
(~15 min) carries `sub`, `role`, `clinic_id`, `jti`, `type=access`, `iat`,
`exp`; when issued together with a refresh token it also carries `rjti`, the
paired refresh token's jti, so logout can revoke both without client help (C1;
tokens minted before that change lack it and simply revoke only themselves).
The long-lived REFRESH token (~7 days) carries ONLY `sub`, `jti`,
`type=refresh`, `iat`, `exp` — no role/clinic claims, because a refresh
exchange re-derives role/clinic from current database state (a role change
takes effect on next refresh, not just next login).

What changed in the split: tokens are signed RS256 with a `kid` header (so
verifiers can select the right public key from the JWKS) instead of HS256
with a shared secret. Full token contents are never logged — only `jti`/`sub`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import jwt

from libs.auth.verification import (
    ACCESS_TOKEN_TYPE,
    JWT_ALGORITHM,
    REFRESH_TOKEN_TYPE,
    SERVICE_TOKEN_TYPE,
    StaticKeyResolver,
)
from services.identity.config import settings
from services.identity.security.keys import get_signing_key


def _encode(payload: dict[str, Any]) -> str:
    key = get_signing_key()
    return jwt.encode(payload, key.private_pem, algorithm=JWT_ALGORITHM, headers={"kid": key.kid})


def create_access_token(
    user_id: str, role: str, clinic_id: str | None, refresh_jti: str | None = None
) -> tuple[str, str]:
    """Issue a short-lived access token. Returns (token, jti) — the jti is also
    returned so callers can log/audit it without re-decoding the token.

    `refresh_jti` (the paired refresh token's jti) is carried as the `rjti` claim
    so logout can also revoke the refresh token (C1)."""
    jti = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": user_id,
        "role": role,
        "clinic_id": clinic_id,
        "jti": jti,
        "type": ACCESS_TOKEN_TYPE,
        "iat": now,
        "exp": now + timedelta(minutes=settings.access_token_expire_minutes),
    }
    if refresh_jti is not None:
        payload["rjti"] = refresh_jti
    return _encode(payload), jti


def create_refresh_token(user_id: str) -> tuple[str, str]:
    """Issue a long-lived refresh token carrying ONLY sub/jti/type/iat/exp."""
    jti = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": user_id,
        "jti": jti,
        "type": REFRESH_TOKEN_TYPE,
        "iat": now,
        "exp": now + timedelta(days=settings.refresh_token_expire_days),
    }
    return _encode(payload), jti


def create_service_token(service_name: str, *, audience: str) -> str:
    """Issue a short-lived service token for an internal call with NO user context.

    Claims: `sub=service:<name>`, `aud=<audience>` (the ONE service the token may be
    presented to, D-06), `type=service`, `jti`, `iat`, `exp` (default 60 s,
    `service_token_expire_seconds`) — no role, no clinic. Only `require_internal_caller`
    accepts this type, and only when `aud` names the receiving service;
    `get_current_user` accepts access tokens only, so every public route rejects it.
    Identity attaches one to its own outgoing reference checks and hands others out
    only through the client-credentials mint (`POST /internal/service-tokens`).
    """
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": f"service:{service_name}",
        "aud": audience,
        "type": SERVICE_TOKEN_TYPE,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + timedelta(seconds=settings.service_token_expire_seconds),
    }
    return _encode(payload)


def local_key_resolver() -> StaticKeyResolver:
    """Resolver over Identity's OWN public key — Identity verifies its own
    tokens with no self-HTTP call to its JWKS endpoint."""
    key = get_signing_key()
    return StaticKeyResolver({key.kid: key.public_jwk})
