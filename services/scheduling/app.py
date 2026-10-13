"""Scheduling service app factory: the slot table's home (D-03).

Scheduling owns `provider_slots` and its double-booking exclusion constraint on
its own database (scheduling_db). Plan 02-04 adds its first public routes,
`/api/slots` (generate / search / withdraw, D-01), reached through the gateway;
plan 02-06 adds `/api/appointments` (202 accept + Temporal booking saga, whose
worker runs separately: `python -m services.scheduling.worker`); plan 02-09
adds `/api/waitlist` (join / list / read / cancel a provider waitlist entry).

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

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.auth.jwks_client import JwksClient
from libs.db.mongo import get_mongo_client
from libs.health import build_health_router
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.scheduling.api import appointments, slots, waitlist
from services.scheduling.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Scheduling Service", lifespan=lifespan)

    app.include_router(build_health_router("scheduling", lambda: settings.database_url))
    app.include_router(slots.router)
    app.include_router(appointments.router)
    app.include_router(waitlist.router)

    # Every token will be verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None
    # Scheduling's ServiceTokenProvider, built lazily by api.deps (D-06).
    app.state.service_tokens = None
    # Temporal client for starting sagas, connected lazily on first use by
    # api.deps.get_temporal_client (plan 02-06); tests inject their own.
    app.state.temporal_client = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-scheduling")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
