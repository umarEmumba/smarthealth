"""Visit service settings: the shared infra settings plus Visit's OWN database
URL and its service-client secret (plan 02-10, D-04).

`database_url` is bound to `VISIT_DATABASE_URL` only — there is deliberately no
bare `DATABASE_URL` (it would silently point a service at someone else's
database, defeating database-per-service, D-04). The default is the local-dev
placeholder credential for the `visit_svc` role that the db-init job creates;
real deployments override it from the environment.

Visit calls Scheduling (is the appointment confirmed?) and Provider (is this
login the visit's provider?) over aud-scoped service tokens (D-06), so it has a
client secret of its own.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class VisitSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://visit_svc:changeme-visit-local-dev-only@localhost:5433/visit_db",
        validation_alias="VISIT_DATABASE_URL",
    )
    # Path to Visit's client-credentials secret (plan 02-01). Read lazily on
    # first use by `api.deps.get_service_tokens`, never at import, and never
    # logged; an unreadable file makes the dependency fail closed (503).
    client_secret_path: str = Field(
        default="infra/secrets/service-clients/visit.secret",
        validation_alias="VISIT_CLIENT_SECRET_PATH",
    )


settings = VisitSettings()
