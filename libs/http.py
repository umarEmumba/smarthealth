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

import httpx
from fastapi import Request

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
