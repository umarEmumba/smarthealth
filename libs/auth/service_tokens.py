"""Service tokens for processes with no user context (D-06, research G1).

Temporal activities outlive the HTTP request that started them and carry no
user token, so a Scheduling / Visit / Billing worker authenticates to another
service's `/internal/*` route with a SERVICE token. Only Identity holds the
signing key, so the worker obtains one from Identity's client-credentials mint:

    POST {identity_url}/internal/service-tokens
    Authorization: Basic <service>:<secret>
    {"audience": "<target service>"}
    -> {"access_token", "token_type": "Bearer", "expires_in": 60}

`ServiceTokenProvider.get(audience)` caches one token PER AUDIENCE and refreshes
it `refresh_margin_seconds` before it expires; concurrent callers for the same
audience share one refresh (`asyncio.Lock`). Any failure to obtain a token —
Identity unreachable, timeout, non-200, unreadable body — raises
`ServiceTokenUnavailable` (fail closed). There are no retries here: an activity
treats the exception as retryable and Temporal owns the retry budget.

Neither the secret nor any token is ever logged.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from libs.http import _get_default_client, outgoing_headers
from libs.observability.logging import get_logger

log = get_logger(__name__)

MINT_PATH = "/internal/service-tokens"


class ServiceTokenUnavailable(Exception):
    """A service token for `audience` could not be obtained from Identity.
    Retryable: the caller's request must not proceed without a token."""

    def __init__(self, audience: str) -> None:
        super().__init__(f"service token unavailable for audience {audience!r}")
        self.audience = audience


def load_client_secret(path: str) -> str:
    """Read a per-service client secret file (whitespace stripped).

    Raises `ServiceTokenUnavailable` when the file is missing, unreadable or
    empty — a worker without its secret cannot call anyone."""
    try:
        with open(path, encoding="utf-8") as handle:
            secret = handle.read().strip()
    except OSError as exc:
        raise ServiceTokenUnavailable("*") from exc
    if not secret:
        raise ServiceTokenUnavailable("*")
    return secret


@dataclass(slots=True)
class _CachedToken:
    token: str
    expires_at: float


class ServiceTokenProvider:
    """Mints and caches per-audience service tokens for one calling service."""

    def __init__(
        self,
        service_name: str,
        secret: str,
        identity_url: str,
        http: httpx.AsyncClient | None = None,
        refresh_margin_seconds: int = 15,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._service_name = service_name
        self._secret = secret
        self._identity_url = identity_url.rstrip("/")
        self._http = http
        self._refresh_margin = refresh_margin_seconds
        self._clock = clock
        self._cache: dict[str, _CachedToken] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _fresh(self, audience: str) -> str | None:
        cached = self._cache.get(audience)
        if cached is not None and self._clock() < cached.expires_at - self._refresh_margin:
            return cached.token
        return None

    async def get(self, audience: str) -> str:
        """A valid bearer token whose `aud` is `audience`."""
        token = self._fresh(audience)
        if token is not None:
            return token
        lock = self._locks.setdefault(audience, asyncio.Lock())
        async with lock:
            token = self._fresh(audience)
            if token is not None:
                return token
            return await self._mint(audience)

    async def _mint(self, audience: str) -> str:
        http = self._http if self._http is not None else _get_default_client()
        requested_at = self._clock()
        try:
            response = await http.post(
                f"{self._identity_url}{MINT_PATH}",
                json={"audience": audience},
                auth=httpx.BasicAuth(self._service_name, self._secret),
                headers=outgoing_headers(),
            )
        except httpx.HTTPError as exc:
            log.warning(
                "service_token.mint_unreachable",
                service=self._service_name,
                audience=audience,
                error=type(exc).__name__,
            )
            raise ServiceTokenUnavailable(audience) from exc

        if response.status_code != 200:
            log.warning(
                "service_token.mint_refused",
                service=self._service_name,
                audience=audience,
                status=response.status_code,
            )
            raise ServiceTokenUnavailable(audience)

        try:
            body = response.json()
            token = body["access_token"]
            expires_in = int(body["expires_in"])
            if not isinstance(token, str) or not token or expires_in <= 0:
                raise ValueError("malformed token response")
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("service_token.mint_malformed", service=self._service_name, audience=audience)
            raise ServiceTokenUnavailable(audience) from exc

        # Measured from when the request was SENT, so the cached lifetime never
        # outlasts the token's real `exp`.
        self._cache[audience] = _CachedToken(token=token, expires_at=requested_at + expires_in)
        log.info("service_token.minted", service=self._service_name, audience=audience)
        return token
