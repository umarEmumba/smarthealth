"""FastAPI app factory: wires routers, OTel tracing, structlog logging,
Mongo audit-index setup, and the Prometheus /metrics endpoint.

This is the ASGI app `tests/conftest.py`'s `client` fixture targets
(`smarthealth.main:app` re-exports `app` from here — see smarthealth/main.py).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.identity.api.admin_ping import router as admin_ping_router
from services.identity.api.auth import router as auth_router
from smarthealth.db import get_mongo_client


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Identity Service", lifespan=lifespan)

    app.include_router(auth_router)
    app.include_router(admin_ping_router)

    # OTel FastAPI auto-instrumentation — request/response body capture is
    # NOT enabled anywhere in this codebase (PHI safety, see libs/observability/otel.py).
    setup_tracing(app)

    # Prometheus /metrics endpoint.
    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
