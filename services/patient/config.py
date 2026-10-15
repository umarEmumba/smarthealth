"""Patient service settings: the shared infra settings plus Patient's OWN
database URL.

`database_url` is bound to `PATIENT_DATABASE_URL` only — there is deliberately
no bare `DATABASE_URL` (it would silently point a service at someone else's
database, defeating database-per-service, D-01). The default is the local-dev
placeholder credential for the `patient_svc` role that the db-init job creates;
real deployments override it from the environment.

Patient is the ONLY service whose database holds patient PHI.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class PatientSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://patient_svc:changeme-patient-local-dev-only@localhost:5433/patient_db",
        validation_alias="PATIENT_DATABASE_URL",
    )


settings = PatientSettings()
