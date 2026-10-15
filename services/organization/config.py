"""Organization service settings: the shared infra settings plus Organization's
OWN database URL.

`database_url` is bound to `ORGANIZATION_DATABASE_URL` only — there is
deliberately no bare `DATABASE_URL` (it would silently point a service at
someone else's database, defeating database-per-service, D-01). The default
is the local-dev placeholder credential for the `organization_svc` role that
the db-init job creates; real deployments override it from the environment.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class OrganizationSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://organization_svc:changeme-organization-local-dev-only@localhost:5433/organization_db",
        validation_alias="ORGANIZATION_DATABASE_URL",
    )


settings = OrganizationSettings()
