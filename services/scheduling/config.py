"""Scheduling service settings: the shared infra settings plus Scheduling's OWN
database URL.

`database_url` is bound to `SCHEDULING_DATABASE_URL` only — there is
deliberately no bare `DATABASE_URL` (it would silently point a service at
someone else's database, defeating database-per-service, D-01). The default is
the local-dev placeholder credential for the `scheduling_svc` role that the
db-init job creates; real deployments override it from the environment.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class SchedulingSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://scheduling_svc:changeme-scheduling-local-dev-only@localhost:5433/scheduling_db",
        validation_alias="SCHEDULING_DATABASE_URL",
    )


settings = SchedulingSettings()
