"""API gateway: the single ingress for all external traffic (D-01, D-07).

A thin FastAPI reverse proxy. It routes by path prefix to the owning service
(`routing.ROUTE_TABLE`), refuses unsafe paths and anything outside the table
with 404, and returns each upstream response unchanged. It does NOT verify
JWTs: every service verifies tokens and applies RBAC itself (defence in
depth, roadmap SC3), so a gateway compromise or misroute never widens access.

`/health` and `/metrics` are registered BEFORE the catch-all so they are
answered by the gateway itself and never proxied.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from libs.health import build_health_router
from libs.http import get_internal_http_client
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.gateway.config import GatewaySettings
from services.gateway.config import settings as default_settings
from services.gateway.proxy import forward
from services.gateway.routing import is_safe_path, resolve_upstream

_NOT_FOUND = {"detail": "not found"}
_PROXIED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    yield


async def gateway_entry(request: Request) -> Response:
    settings: GatewaySettings = request.app.state.settings

    raw_path: bytes = request.scope.get("raw_path") or request.url.path.encode("ascii", "ignore")
    if not is_safe_path(request.url.path, raw_path):
        return JSONResponse(status_code=404, content=_NOT_FOUND)

    resolved = resolve_upstream(request.url.path, settings)
    if resolved is None:
        return JSONResponse(status_code=404, content=_NOT_FOUND)
    upstream_name, base_url = resolved

    return await forward(
        request,
        upstream_name=upstream_name,
        base_url=base_url,
        http=get_internal_http_client(request),
        timeout=settings.gateway_upstream_timeout_seconds,
    )


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    app = FastAPI(title="SmartHealth API Gateway", lifespan=lifespan)

    app.state.settings = settings or default_settings
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here.
    app.state.internal_http_client = None

    app.include_router(build_health_router("gateway"))

    setup_tracing(app, service_name="smarthealth-gateway")

    # /metrics must exist BEFORE the catch-all, or the catch-all would proxy it.
    Instrumentator().instrument(app).expose(app, include_in_schema=False)

    app.add_api_route(
        "/{full_path:path}",
        gateway_entry,
        methods=_PROXIED_METHODS,
        include_in_schema=False,
    )
    return app


app = create_app()
