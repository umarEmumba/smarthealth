"""JWT issuance and validation via python-jose (CLAUDE.md-mandated).

Locked contract (checkpoint-resolved, RESEARCH Decision 1 recommended
option): short-lived ACCESS token (~15 min, `settings.access_token_expire_minutes`)
carries `sub`, `role`, `clinic_id`, `jti`, `type=access`, `exp`, `iat`.
Long-lived REFRESH token (~7 days, `settings.refresh_token_expire_days`)
carries ONLY `sub`, `jti`, `type=refresh`, `exp` — no role/clinic claims,
since a refresh exchange re-derives role/clinic from the current DB state
(a role change takes effect on next refresh, not just next login).

V6 crypto (STRIDE T-03-01): `decode_token` pins `algorithms=[settings.jwt_algorithm]`
explicitly on every call — python-jose (like every JWT library) will happily
honor whatever algorithm the token HEADER claims unless the caller pins an
explicit allowlist, which is exactly the "alg:none" / algorithm-confusion
footgun. Never call `jwt.decode(...)` without an explicit `algorithms=` list
anywhere in this codebase.

Full token contents are never logged anywhere in this codebase — only `jti`
and `sub` are safe to log for correlation (RESEARCH anti-patterns section).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from jose import JWTError, jwt

from smarthealth.config import settings

ACCESS_TOKEN_TYPE = "access"
REFRESH_TOKEN_TYPE = "refresh"


class TokenError(Exception):
    """Raised when a token fails to decode, is expired, has the wrong type,
    or was forged with a disallowed algorithm. Callers map this to a 401.
    """


def create_access_token(user_id: str, role: str, clinic_id: str | None) -> tuple[str, str]:
    """Issue a short-lived access token. Returns (token, jti) — the jti is
    also returned so callers can log/audit it without re-decoding the token.
    """
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
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, jti


def create_refresh_token(user_id: str) -> tuple[str, str]:
    """Issue a long-lived refresh token carrying ONLY sub/jti/type/exp —
    deliberately no role/clinic claims (locked contract, see module docstring).
    """
    jti = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": user_id,
        "jti": jti,
        "type": REFRESH_TOKEN_TYPE,
        "iat": now,
        "exp": now + timedelta(days=settings.refresh_token_expire_days),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, jti


def decode_token(
    token: str, expected_type: Literal["access", "refresh"]
) -> dict[str, Any]:
    """Decode and validate a JWT: signature + expiry via an explicit
    algorithms allowlist (never trusts the token header's own `alg`), then
    asserts `type` matches `expected_type`.

    Raises `TokenError` on any failure (invalid signature, expired, wrong
    type, malformed, or an alg:none / disallowed-algorithm forgery attempt)
    — python-jose raises `JWTError` for algorithm mismatches when
    `algorithms=` is passed explicitly, which is what closes the alg:none
    footgun (T-03-01).
    """
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise TokenError(f"invalid token: {exc}") from exc

    if claims.get("type") != expected_type:
        raise TokenError(f"expected token type {expected_type!r}, got {claims.get('type')!r}")

    return claims
