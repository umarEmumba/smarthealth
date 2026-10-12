"""Internal HTTP client seam (service-to-service calls and JWKS fetches).

Production: every call uses a process-default `httpx.AsyncClient` with an
explicit timeout (`shared_settings.internal_http_timeout_seconds`) — never an
unbounded call to another service (httpx only, never `requests`, per
CLAUDE.md).

Tests: the harness injects its mounted in-process client through
`app.state.internal_http_client`, so the same code path runs against mounted
ASGI apps with zero real sockets. A value of `None` means "use the default".

The default client is loop-aware for the same reason the DB helpers are: an
`AsyncClient`'s connection pool binds to the loop it was first used on.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import httpx
from fastapi import Request
from opentelemetry.propagate import inject

from libs.config import shared_settings

_default_client: httpx.AsyncClient | None = None
_default_client_loop: asyncio.AbstractEventLoop | None = None


def _get_default_client() -> httpx.AsyncClient:
    global _default_client, _default_client_loop  # noqa: PLW0603

    current_loop = asyncio.get_running_loop()
    if _default_client is None or _default_client_loop is not current_loop:
        _default_client = httpx.AsyncClient(
            timeout=httpx.Timeout(shared_settings.internal_http_timeout_seconds)
        )
        _default_client_loop = current_loop
    return _default_client


def get_internal_http_client(request: Request) -> httpx.AsyncClient:
    """FastAPI dependency: the injected client if the app carries one,
    otherwise the process-default timeout-bounded client."""
    injected = getattr(request.app.state, "internal_http_client", None)
    if injected is not None:
        return injected
    return _get_default_client()


def outgoing_headers(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Headers for an outgoing internal call: a COPY of `extra` plus the active
    span's W3C trace context.

    Trace context crosses every service boundary through the OpenTelemetry
    propagator API (`inject` writes `traceparent`, and `tracestate` when
    present) — never a hand-built header string — so the callee's server span
    joins the caller's trace (roadmap SC4). With no active span nothing is added.

    Only trace/span identifiers travel. No OpenTelemetry baggage is ever set
    anywhere in this codebase, so no PHI, token content or other data can ride
    along (STRIDE T-01.1-19).
    """
    headers: dict[str, str] = dict(extra) if extra else {}
    inject(headers)
    return headers
