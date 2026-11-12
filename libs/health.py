"""Shared `/health` router every service mounts.

Unauthenticated and excluded from the OpenAPI schema: it reports only that
the service is up and — when the service owns a database — that the database
answers `SELECT 1`. No version strings, hostnames, or connection details are
exposed. An unreachable database yields 503 so Compose/orchestrator health
checks and the gateway can see the service is unhealthy.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from libs.db.postgres import ping
from libs.observability.logging import get_logger

log = get_logger(__name__)


def build_health_router(
    service_name: str,
    database_url_getter: Callable[[], str] | None = None,
) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/health", include_in_schema=False)
    async def health() -> JSONResponse:
        if database_url_getter is not None:
            try:
                await ping(database_url_getter())
            except Exception as exc:  # noqa: BLE001 - any failure means unhealthy
                # Exception class only — never the message (it can embed the DSN).
                log.warning("health.db_unreachable", service=service_name, error=type(exc).__name__)
                return JSONResponse(
                    status_code=503, content={"status": "unhealthy", "service": service_name}
                )
        return JSONResponse(content={"status": "ok", "service": service_name})

    return router
