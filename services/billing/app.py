"""Billing service app factory: billing pre-checks and refund state on billing_db (D-04).

Billing owns `billing_records` and its append-only `billing_status_history`.
It is called by Scheduling's booking/cancel/reschedule activities and by
Visit's completion workflow over aud-scoped internal routes, and it exposes a
read-only, clinic-scoped `/api/billing` for front-desk/admin through the
gateway. It calls no other service and runs no workflow of its own (D-05).

Every bearer token is verified LOCALLY with a `JwksClient` over Identity's
public JWKS (cached; never a call to Identity per request).
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
from services.billing.api import internal
from services.billing.api.records import router as records_router
from services.billing.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Billing Service", lifespan=lifespan)

    app.include_router(build_health_router("billing", lambda: settings.database_url))
    app.include_router(records_router)
    app.include_router(internal.router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-billing")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
