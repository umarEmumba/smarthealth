"""Identity service app factory: accounts, token issuance, JWKS, health.

Identity owns `identity_db` and the RSA private key. `create_app()` loads the
signing key FIRST and fails closed (`SigningKeyUnavailable`) when it is
missing — there is no insecure fallback key, so a misconfigured Identity never
starts (STRIDE T-01.1-03).

Identity verifies its own tokens through a `StaticKeyResolver` over its own
public key (no self-HTTP call); every OTHER service verifies through a
`JwksClient` pointed at this service's `/.well-known/jwks.json`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.db.mongo import get_mongo_client
from libs.health import build_health_router
from libs.observability.logging import configure_logging
from libs.observability.otel import setup_tracing
from services.identity.api.admin_ping import router as admin_ping_router
from services.identity.api.auth import router as auth_router
from services.identity.api.jwks import router as jwks_router
from services.identity.config import settings
from services.identity.security.keys import get_signing_key
from services.identity.security.signing import local_key_resolver


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await ensure_audit_indexes(get_mongo_client())
    yield


def create_app() -> FastAPI:
    # Fail closed before anything else: no signing key, no Identity.
    get_signing_key()

    app = FastAPI(title="SmartHealth Identity Service", lifespan=lifespan)

    app.include_router(build_health_router("identity", lambda: settings.database_url))
    app.include_router(auth_router)
    app.include_router(admin_ping_router)
    app.include_router(jwks_router)

    app.state.key_resolver = local_key_resolver()
    # None = use the process-default timeout-bounded client; the test harness
    # injects its in-process mesh here.
    app.state.internal_http_client = None

    # OTel FastAPI auto-instrumentation — request/response body capture is NOT
    # enabled anywhere in this codebase (PHI safety, see libs/observability/otel.py).
    setup_tracing(app, service_name="smarthealth-identity")

    # Prometheus /metrics endpoint.
    Instrumentator().instrument(app).expose(app)

    return app


app = create_app()
