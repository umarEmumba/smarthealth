"""Cached JWKS resolver — how every non-Identity service learns Identity's keys.

The ONLY network call this performs is a GET of Identity's
`/.well-known/jwks.json`, and only on a cache miss or TTL expiry. Token
verification itself is always local against the cached key material;
verifying a token never calls Identity (RESEARCH Pitfall 1 — doing so would
silently make Identity a hard dependency of every protected request on every
service).

Behaviour (STRIDE T-01.1-05):
  * fresh cached kid            -> returned, zero network.
  * cached but stale kid        -> one refresh; if the refresh FAILS the stale
    key is still served (logged `jwks.refresh.failed_serving_stale`) — an
    Identity outage must not log everyone out.
  * unknown kid                 -> at most ONE refresh per
    `miss_backoff_seconds`, then `TokenError("unknown signing key")` — a
    flood of bogus-kid tokens cannot hammer Identity.
  * nothing cached + fetch fails -> `KeysUnavailable` (callers answer 503).

No `asyncio.Lock`: locks bind to a loop and the test harness uses per-test
loops; concurrent refreshes are idempotent GETs, so the worst case is a
duplicated fetch.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

from libs.auth.verification import JWT_ALGORITHM, KeysUnavailable, TokenError
from libs.config import SharedSettings
from libs.http import outgoing_headers
from libs.observability.logging import get_logger

log = get_logger(__name__)


class JwksClient:
    def __init__(
        self,
        jwks_url: str,
        *,
        cache_ttl_seconds: int = 300,
        miss_backoff_seconds: int = 30,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._jwks_url = jwks_url
        self._cache_ttl = cache_ttl_seconds
        self._miss_backoff = miss_backoff_seconds
        self._clock = clock
        self._keys: dict[str, dict[str, Any]] = {}
        self._fetched_at: float | None = None
        self._last_attempt: float | None = None
        # Number of JWKS HTTP requests issued (successful or not).
        self.fetch_count = 0

    @classmethod
    def from_settings(cls, settings: SharedSettings) -> "JwksClient":
        return cls(
            settings.jwks_url,
            cache_ttl_seconds=settings.jwks_cache_ttl_seconds,
            miss_backoff_seconds=settings.jwks_miss_backoff_seconds,
        )

    async def get_key(self, kid: str, http: httpx.AsyncClient | None) -> dict[str, Any]:
        now = self._clock()
        cached = self._keys.get(kid)
        fresh = self._fetched_at is not None and (now - self._fetched_at) < self._cache_ttl
        if cached is not None and fresh:
            return cached

        recently_attempted = (
            self._last_attempt is not None and (now - self._last_attempt) < self._miss_backoff
        )
        if recently_attempted:
            # Back off: do not re-fetch. A known-but-stale key is still served;
            # an unknown kid is rejected without touching Identity.
            if cached is not None:
                return cached
            if not self._keys:
                raise KeysUnavailable("signing keys are not available yet")
            raise TokenError("unknown signing key")

        try:
            await self.refresh(http)
        except KeysUnavailable:
            if cached is not None:
                log.warning("jwks.refresh.failed_serving_stale")
                return cached
            raise

        key = self._keys.get(kid)
        if key is None:
            raise TokenError("unknown signing key")
        return key

    async def refresh(self, http: httpx.AsyncClient) -> None:
        """Fetch the JWKS and replace the cache. Raises `KeysUnavailable` on
        any failure (the existing cache is left untouched)."""
        if http is None:
            raise KeysUnavailable("no HTTP client available to fetch the JWKS")

        self._last_attempt = self._clock()
        self.fetch_count += 1
        try:
            response = await http.get(self._jwks_url, headers=outgoing_headers())
            response.raise_for_status()
            document = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            # Error class only — never the response body or URL internals.
            log.warning("jwks.refresh.failed", error=type(exc).__name__)
            raise KeysUnavailable("could not fetch the JWKS") from exc

        entries = document.get("keys") if isinstance(document, dict) else None
        keys: dict[str, dict[str, Any]] = {}
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            kid = entry.get("kid")
            if entry.get("kty") != "RSA" or not isinstance(kid, str) or not kid:
                continue
            if entry.get("alg") not in (None, JWT_ALGORITHM):
                continue
            keys[kid] = entry

        if not keys:
            # An empty/garbled JWKS is a malfunction, not "all keys revoked":
            # keep serving the previous keys rather than wiping the cache.
            log.warning("jwks.refresh.failed", error="no_usable_keys")
            raise KeysUnavailable("the JWKS contained no usable keys")

        self._keys = keys
        self._fetched_at = self._clock()
        log.info("jwks.refresh.success", key_count=len(keys))
