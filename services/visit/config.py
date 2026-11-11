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
    # Reconciler (plan 02.1-10, S4): the Schedule's interval, and how long after
    # a visit completed its billing/appointment markers may stay empty before the
    # reconciler steps in (the workflow normally settles them within seconds and,
    # with an unlimited billing retry, may legitimately still be retrying; the
    # reconciler only covers what slipped through).
    reconcile_interval_seconds: int = Field(
        default=60, ge=1, validation_alias="VISIT_RECONCILE_INTERVAL_SECONDS"
    )
    reconcile_grace_minutes: int = Field(
        default=10, ge=0, validation_alias="VISIT_RECONCILE_GRACE_MINUTES"
    )
    # visit-downstream consumer (plan 03-09): the Prometheus metrics port it serves
    # on (internal to the Compose network, never published to the host) and the
    # heartbeat file the Compose healthcheck reads.
    consumer_metrics_port: int = Field(
        default=9100, ge=1, le=65535, validation_alias="VISIT_CONSUMER_METRICS_PORT"
    )
    consumer_heartbeat_path: str = Field(
        default="/tmp/visit-consumer.alive", validation_alias="VISIT_CONSUMER_HEARTBEAT_PATH"
    )


settings = VisitSettings()
