"""Token verification — the half of auth EVERY service runs (no private key).

Identity signs RS256 tokens with a private key only it holds; every other
service verifies them locally against Identity's published public JWKS. This
module is the verification half: it never signs, and never needs a secret.

Security invariants (STRIDE T-01.1-02) — locked, not style choices:
  * `algorithms=["RS256"]` is pinned on every decode, AND the token header's
    own `alg` must equal RS256. python-jose (like every JWT library) will
    honour whatever algorithm the header claims unless the caller pins an
    explicit allowlist — that is the alg:none / algorithm-confusion footgun
    (e.g. an HS256 token "signed" with the public key as the HMAC secret).
  * A non-empty `kid` header is REQUIRED. Key selection is by `kid`, never by
    position in the JWKS and never by falling back to a default key.
  * The `type` claim must be in the caller's `expected_types`, so a refresh
    token can never authenticate an API call.
  * `exp` is REQUIRED on every token (`require_exp`), so a token minted without
    an expiry can never live forever (review WR-03).

Full token contents are never logged — only `jti`/`sub` are safe to log.
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx
from jose import JWTError, jwt
from jose.exceptions import JWKError

JWT_ALGORITHM = "RS256"

ACCESS_TOKEN_TYPE = "access"
REFRESH_TOKEN_TYPE = "refresh"
SERVICE_TOKEN_TYPE = "service"

ACCESS_ONLY: frozenset[str] = frozenset({ACCESS_TOKEN_TYPE})
REFRESH_ONLY: frozenset[str] = frozenset({REFRESH_TOKEN_TYPE})
# Internal endpoints accept a forwarded user access token or a service token.
INTERNAL_CALLER_TYPES: frozenset[str] = frozenset({ACCESS_TOKEN_TYPE, SERVICE_TOKEN_TYPE})


class TokenError(Exception):
    """The token is invalid for any reason (malformed, bad signature, expired,
    unknown key, wrong algorithm or type). Callers map this to a 401."""


class KeysUnavailable(Exception):
    """Verification keys could not be obtained (e.g. Identity's JWKS endpoint
    is unreachable and nothing is cached). Callers map this to a 503 — the
    token itself is not known to be bad."""


class KeyResolver(Protocol):
    """Resolves a `kid` to the public JWK (dict) that verifies its tokens."""

    async def get_key(self, kid: str, http: httpx.AsyncClient | None) -> dict[str, Any]: ...


class StaticKeyResolver:
    """In-memory `kid -> JWK` resolver. Identity uses it to verify its own
    tokens (no self-HTTP call); unit tests use it to avoid any network."""

    def __init__(self, keys: dict[str, dict[str, Any]]) -> None:
        self._keys = dict(keys)

    async def get_key(self, kid: str, http: httpx.AsyncClient | None = None) -> dict[str, Any]:
        try:
            return self._keys[kid]
        except KeyError:
            raise TokenError("unknown signing key") from None


async def decode_token(
    token: str,
    *,
    expected_types: frozenset[str],
    resolver: KeyResolver,
    http: httpx.AsyncClient | None = None,
    audience: str | None = None,
) -> dict[str, Any]:
    """Verify signature + expiry + type (+ audience) and return the claims.

    Raises `TokenError` on any invalid token and lets `KeysUnavailable`
    propagate from the resolver. Never trusts the header's `alg`, never
    selects a key by position, never accepts a token without a `kid` or `exp`.

    `audience`: when the token carries `aud`, it must contain this value; a
    token WITH `aud` decoded with `audience=None` is rejected. python-jose
    SKIPS the audience check entirely when the token has no `aud`, so passing
    `audience=` can never REQUIRE `aud` — callers that must (service tokens on
    internal routes, `libs.auth.internal`) check its presence themselves.
    """
    try:
        header = jwt.get_unverified_header(token)
    except (JWTError, ValueError, TypeError) as exc:
        raise TokenError("malformed token") from exc

    if header.get("alg") != JWT_ALGORITHM:
        raise TokenError("unsupported token algorithm")

    kid = header.get("kid")
    if not isinstance(kid, str) or not kid:
        raise TokenError("token has no key id")

    key = await resolver.get_key(kid, http)

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[JWT_ALGORITHM],
            audience=audience,
            options={"require_exp": True},
        )
    except (JWTError, JWKError) as exc:
        raise TokenError("invalid token") from exc

    if claims.get("type") not in expected_types:
        raise TokenError("unexpected token type")

    return claims
