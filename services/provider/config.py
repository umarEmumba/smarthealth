"""Provider service settings: the shared infra settings plus Provider's OWN
database URL.

`database_url` is bound to `PROVIDER_DATABASE_URL` only — there is deliberately
no bare `DATABASE_URL` (it would silently point a service at someone else's
database, defeating database-per-service, D-01). The default is the local-dev
placeholder credential for the `provider_svc` role that the db-init job creates;
real deployments override it from the environment.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class ProviderSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://provider_svc:changeme-provider-local-dev-only@localhost:5433/provider_db",
        validation_alias="PROVIDER_DATABASE_URL",
    )


settings = ProviderSettings()
