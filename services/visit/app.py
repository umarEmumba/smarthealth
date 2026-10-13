"""Visit service app factory: the visit lifecycle on visit_db (D-04, D-15).

Visit owns `visits` and its append-only `visit_state_history`. Front-desk
checks a CONFIRMED appointment in (verified with Scheduling over an
aud=scheduling service token, D-06); the visit's provider, or front-desk/admin
within the clinic, advance it checked_in -> in_progress -> completed with
duplicate-safe conditional updates (research Pattern 9); ops staff poll a
clinic-scoped, filtered list (D-17). All through the gateway at `/api/visits`.

Completion leaves `downstream_started_at` NULL: plan 02-11 adds the
non-blocking downstream workflow and its sweeper on top (VISIT-04, D-16).

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
from services.visit.api.visits import router as visits_router
from services.visit.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Visit Service", lifespan=lifespan)

    app.include_router(build_health_router("visit", lambda: settings.database_url))
    app.include_router(visits_router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None
    # Visit's ServiceTokenProvider, built lazily by api.deps (D-06).
    app.state.service_tokens = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-visit")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
