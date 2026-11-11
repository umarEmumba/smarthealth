"""Analytics service app factory: the read-only metrics API over analytics_db (D-10).

The projections are maintained by the separate `analytics-consumer` process from Kafka
events (plan 03-10); this API only reads the metric views. It owns analytics_db and has
no URL, role or import for any OLTP database (ANLY-06, database-per-service D-04).

Every bearer token is verified LOCALLY with a `JwksClient` over Identity's public JWKS.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from libs.auth.jwks_client import JwksClient
from libs.health import build_health_router
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.analytics.api.analytics import router as analytics_router
from services.analytics.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Analytics Service", lifespan=lifespan)

    app.include_router(build_health_router("analytics", lambda: settings.database_url))
    app.include_router(analytics_router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-analytics")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
