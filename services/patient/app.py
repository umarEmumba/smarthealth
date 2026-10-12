"""Patient service app factory: patient profiles on patient_db.

Patient is the ONLY service holding patient PHI. It verifies every bearer token
LOCALLY with a `JwksClient` over Identity's public JWKS (cached; never a call to
Identity per request) and re-applies `require_role` and clinic scoping itself —
the gateway does not authenticate on its behalf.

Its two cross-service references are validated at write time through the shared
reference client: `clinic_id` against Organization and, when supplied, `user_id`
against Identity (D-05). Those calls carry ids only; PHI never leaves this
service. Deactivating or reading never cascades or enriches (D-06, D-07).
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
from services.patient.api.patients import router as patients_router
from services.patient.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="SmartHealth Patient Service", lifespan=lifespan)

    app.include_router(build_health_router("patient", lambda: settings.database_url))
    app.include_router(patients_router)

    # Every token is verified locally against Identity's cached public JWKS.
    app.state.key_resolver = JwksClient.from_settings(settings)
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here (also what the JWKS fetch goes through).
    app.state.internal_http_client = None

    # No request/response body capture anywhere: patient payloads are PHI
    # (see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-patient")

    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
