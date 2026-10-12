"""Scheduling service app factory: the slot table's home, health only this phase (D-03).

Scheduling owns `provider_slots` and its double-booking exclusion constraint on
its own database (scheduling_db). It exposes NO `/api` routes yet — the booking
saga and slot generation are Phase 2 — but it is a real, independently deployed
service now, so Phase 2 never has to move a table between services.

The token-verification and internal-client state is wired up already (the same
two attributes every other service sets) so Phase 2 routes inherit local RS256
verification through Identity's cached public JWKS and the shared, timeout-bounded
internal HTTP client without further setup.
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
from services.scheduling.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Scheduling Service", lifespan=lifespan)

    app.include_router(build_health_router("scheduling", lambda: settings.database_url))

    # Every token will be verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-scheduling")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
