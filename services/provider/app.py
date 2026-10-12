"""Provider service app factory: the provider directory on provider_db.

Provider owns providers, specialties and weekly availability templates, kept
deliberately separate from Organization (the user's finer-bounded-context choice,
D-01). It verifies every bearer token LOCALLY with a `JwksClient` over Identity's
public JWKS (cached; never a call to Identity per request) and re-applies
`require_role` and the CR-02 ownership rule itself — the gateway does not
authenticate on its behalf.

Its cross-service references are validated at write time through the shared
reference client: `department_id` against Organization and, when supplied,
`user_id` against Identity (D-05). Deactivating a department never cascades into
this service, and reads return bare ids with no enrichment (D-06, D-07).
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
from services.provider.api.providers import router as providers_router
from services.provider.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Provider Service", lifespan=lifespan)

    app.include_router(build_health_router("provider", lambda: settings.database_url))
    app.include_router(providers_router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-provider")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
