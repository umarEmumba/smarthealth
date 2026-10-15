"""Scheduling API dependencies (plans 02-04, 02-06).

`get_service_tokens` hands a route the process's one `ServiceTokenProvider`
for Scheduling's outgoing `/internal/*` reads (D-06). It is built lazily on
first use, not at import: the client secret is read from
`settings.client_secret_path` then, and a missing or unreadable secret makes the
dependency fail closed with 503 "service credentials unavailable" instead of
crashing the process at start-up (T-02-21). The secret is never logged.

The provider mints through `app.state.internal_http_client` (the in-process
mesh in tests, None = the process-default timeout-bounded client). If that
client is swapped (the test harness does, per test and for degraded meshes), the
provider is rebuilt so it never holds a closed or stale client.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request, status
from temporalio.client import Client

from libs.auth.service_tokens import ServiceTokenProvider, ServiceTokenUnavailable, load_client_secret
from libs.temporal import connect_client
from services.scheduling.config import settings

SERVICE_NAME = "scheduling"


def build_service_tokens(app: FastAPI) -> ServiceTokenProvider:
    """The app's cached provider, (re)built when missing or when the injected
    HTTP client changed. Raises `ServiceTokenUnavailable` if the secret cannot
    be read."""
    http = getattr(app.state, "internal_http_client", None)
    provider: ServiceTokenProvider | None = getattr(app.state, "service_tokens", None)
    if provider is not None and getattr(app.state, "service_tokens_http", None) is http:
        return provider
    secret = load_client_secret(settings.client_secret_path)
    provider = ServiceTokenProvider(SERVICE_NAME, secret, settings.identity_service_url, http=http)
    app.state.service_tokens = provider
    app.state.service_tokens_http = http
    return provider


async def get_service_tokens(request: Request) -> ServiceTokenProvider:
    """FastAPI dependency: the provider, or 503 when credentials are unreadable."""
    try:
        return build_service_tokens(request.app)
    except ServiceTokenUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="service credentials unavailable",
        ) from exc


async def get_temporal_client(request: Request) -> Client:
    """FastAPI dependency (plan 02-06): the app's Temporal client.

    Connected LAZILY on first use and cached on `app.state.temporal_client`:
    the API boots and serves reads while Temporal is down, and a failed start
    is deferred to the starter sweeper (research Pitfall 4). Tests inject the
    test environment's client (or a failing stand-in) on the same attribute.
    """
    client = getattr(request.app.state, "temporal_client", None)
    if client is None:
        client = await connect_client(lazy=True)
        request.app.state.temporal_client = client
    return client
