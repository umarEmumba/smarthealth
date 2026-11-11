"""Analytics service settings: the shared infra settings plus Analytics' OWN
database URL (analytics_db) and the projection consumer's runtime knobs (D-10).

`database_url` is bound to `ANALYTICS_DATABASE_URL` only. It is the ONE database
this package holds: the analytics process computes every metric from its own
projections (fed from Kafka) and never reads Scheduling, Visit, Patient or
Provider data (ANLY-06, database-per-service D-04). There is deliberately no bare
`DATABASE_URL` and no second URL field. The default is the local-dev placeholder
credential for the `analytics_svc` role the db-init job creates; real deployments
override it from the environment.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class AnalyticsSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://analytics_svc:changeme-analytics-local-dev-only@localhost:5433/analytics_db",
        validation_alias="ANALYTICS_DATABASE_URL",
    )
    # analytics-projection consumer: the Prometheus metrics port it serves on
    # (internal to the Compose network, never published to the host) and the
    # heartbeat file the Compose healthcheck reads (plan 03-13 adds the container).
    consumer_metrics_port: int = Field(
        default=9100, ge=1, le=65535, validation_alias="ANALYTICS_CONSUMER_METRICS_PORT"
    )
    consumer_heartbeat_path: str = Field(
        default="/tmp/analytics-consumer.alive",
        validation_alias="ANALYTICS_CONSUMER_HEARTBEAT_PATH",
    )
    # Default reporting window of the metrics read API (plan 03-13).
    default_window_days: int = Field(
        default=30, ge=1, le=366, validation_alias="ANALYTICS_DEFAULT_WINDOW_DAYS"
    )


settings = AnalyticsSettings()
