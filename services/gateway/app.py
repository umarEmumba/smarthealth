"""API gateway: the single ingress for all external traffic (D-01, D-07).

A thin FastAPI reverse proxy. It routes by path prefix to the owning service
(`routing.ROUTE_TABLE`), refuses unsafe paths and anything outside the table
with 404, and returns each upstream response unchanged. It does NOT verify
JWTs: every service verifies tokens and applies RBAC itself (defence in
depth, roadmap SC3), so a gateway compromise or misroute never widens access.

`/health` and `/metrics` are registered BEFORE the catch-all so they are
answered by the gateway itself and never proxied.

`/metrics` is served ONLY on the gateway's internal metrics listener
(`gateway_metrics_port`, default 9100), which Compose never publishes. On the
public listener (port 8000, the only published one) the path is a plain 404.
The decision uses the ASGI `server` scope (the local socket the request arrived
on), which a client cannot influence — unlike a Host or forwarding header.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from libs.health import build_health_router
from libs.http import get_internal_http_client
from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import setup_tracing
from services.gateway.config import GatewaySettings
from services.gateway.config import settings as default_settings
from services.gateway.proxy import forward
from services.gateway.ratelimit import check_rate_limit, get_rate_limit_redis, rate_limit_group
from services.gateway.routing import is_safe_path, resolve_upstream

log = get_logger(__name__)

_NOT_FOUND = {"detail": "not found"}
_PROXIED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    yield


async def metrics_endpoint(request: Request) -> Response:
    """Prometheus exposition, answered only on the internal metrics listener."""
    settings: GatewaySettings = request.app.state.settings
    server = request.scope.get("server")
    # Fail closed: no server scope (e.g. a unix socket) or any other port -> 404.
    if not server or server[1] != settings.gateway_metrics_port:
        return JSONResponse(status_code=404, content=_NOT_FOUND)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def gateway_entry(request: Request) -> Response:
    settings: GatewaySettings = request.app.state.settings

    raw_path: bytes = request.scope.get("raw_path") or request.url.path.encode("ascii", "ignore")
    if not is_safe_path(request.url.path, raw_path):
        return JSONResponse(status_code=404, content=_NOT_FOUND)

    resolved = resolve_upstream(request.url.path, settings)
    if resolved is None:
        return JSONResponse(status_code=404, content=_NOT_FOUND)
    upstream_name, base_url = resolved

    # Edge rate limit (per client IP, atomic Redis window). Runs after the path
    # checks so junk paths are not counted, and before forward so a limited
    # request never reaches an upstream. /health and /metrics are answered by
    # their own routes above the catch-all and are never limited. The key uses
    # the socket peer only; X-Forwarded-For from a client is never trusted here.
    group = rate_limit_group(request.url.path)
    limit = (
        settings.gateway_rate_limit_auth_per_minute
        if group == "auth"
        else settings.gateway_rate_limit_default_per_minute
    )
    client_ip = request.client.host if request.client is not None else "unknown"
    decision = await check_rate_limit(
        get_rate_limit_redis(settings.redis_url),
        group=group,
        client_ip=client_ip,
        limit=limit,
    )
    if not decision.allowed:
        log.warning("gateway.ratelimit.rejected", group=group, path=request.url.path)
        return JSONResponse(
            status_code=429,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": str(decision.retry_after_seconds)},
        )

    return await forward(
        request,
        upstream_name=upstream_name,
        base_url=base_url,
        http=await get_internal_http_client(request),
        timeout=settings.gateway_upstream_timeout_seconds,
    )


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    # The gateway's only surface is /health, /metrics and the proxy catch-all:
    # no OpenAPI/docs endpoints of its own, so every path outside the route
    # table (including /openapi.json and /docs) is a plain 404.
    app = FastAPI(
        title="SmartHealth API Gateway",
        lifespan=lifespan,
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )

    app.state.settings = settings or default_settings
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here.
    app.state.internal_http_client = None

    app.include_router(build_health_router("gateway"))

    setup_tracing(app, service_name="smarthealth-gateway")

    # /metrics must exist BEFORE the catch-all, or the catch-all would proxy it.
    # It is our own route (not Instrumentator.expose) so it can refuse requests
    # that arrive on the public listener.
    Instrumentator().instrument(app)
    app.add_api_route("/metrics", metrics_endpoint, methods=["GET"], include_in_schema=False)

    app.add_api_route(
        "/{full_path:path}",
        gateway_entry,
        methods=_PROXIED_METHODS,
        include_in_schema=False,
    )
    return app


app = create_app()
