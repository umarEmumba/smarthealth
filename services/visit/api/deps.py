"""Visit API dependencies (plan 02-10).

`get_service_tokens` hands a route the process's one `ServiceTokenProvider`
for Visit's outgoing `/internal/*` reads (Scheduling's appointment lookup,
Provider's by-user lookup; D-06). Same shape as Scheduling's dependency, but
Visit's own code (no cross-service import). It is built lazily on first use,
not at import: the client secret is read from `settings.client_secret_path`
then, and a missing or unreadable secret makes the dependency fail closed with
503 "service credentials unavailable" instead of crashing the process at
start-up. The secret is never logged.

The provider mints through `app.state.internal_http_client` (the in-process
mesh in tests, None = the process-default timeout-bounded client). If that
client is swapped (the test harness does, per test and for degraded meshes),
the provider is rebuilt so it never holds a closed or stale client.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request, status

from libs.auth.service_tokens import ServiceTokenProvider, ServiceTokenUnavailable, load_client_secret
from services.visit.config import settings

SERVICE_NAME = "visit"


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
