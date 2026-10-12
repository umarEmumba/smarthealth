"""Organization service app factory: clinics and departments on organization_db.

Organization verifies every bearer token LOCALLY with a `JwksClient` over
Identity's public JWKS (cached; it never calls Identity per request) and
re-applies `require_role` itself — the gateway does not authenticate on its
behalf. Deactivating a clinic or department never calls another service and
never cascades (D-06); this service makes no outbound business calls at all.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.auth.jwks_client import JwksClient
from libs.db.mongo import get_mongo_client
from libs.health import build_health_router
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.organization.api.clinics import router as clinics_router
from services.organization.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Organization Service", lifespan=lifespan)

    app.include_router(build_health_router("organization", lambda: settings.database_url))
    app.include_router(clinics_router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere (PHI safety, see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-organization")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
