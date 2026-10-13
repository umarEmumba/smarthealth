"""Scheduling service settings: the shared infra settings plus Scheduling's OWN
database URL, its service-client secret and the slot-generation bounds
(plan 02-04, D-01).

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
    # Path to Scheduling's client-credentials secret (plan 02-01). Read lazily
    # on first use by `api.deps.get_service_tokens`, never at import, and never
    # logged; an unreadable file makes the dependency fail closed (503).
    client_secret_path: str = Field(
        default="infra/secrets/service-clients/scheduling.secret",
        validation_alias="SCHEDULING_CLIENT_SECRET_PATH",
    )
    # D-01: slots are materialized on demand over a rolling horizon. A search
    # extends generation only up to now + horizon, and no generate/search range
    # may be wider than `slot_max_range_days` (a GET can trigger a bounded write,
    # research Pattern 6 / T-02-17).
    slot_horizon_days: int = Field(default=28, gt=0, validation_alias="SCHEDULING_SLOT_HORIZON_DAYS")
    slot_max_range_days: int = Field(
        default=31, gt=0, validation_alias="SCHEDULING_SLOT_MAX_RANGE_DAYS"
    )

    # D-03: how long the booking saga's reserve step holds a slot. Must
    # comfortably exceed the saga's bounded worst case (the sum of the forward
    # activities' schedule_to_close timeouts, about 4.5 minutes), so the reaper
    # never frees a slot under a live saga in normal operation.
    hold_ttl_minutes: int = Field(default=10, gt=0, validation_alias="SCHEDULING_HOLD_TTL_MINUTES")

    # Plan 02-07 (D-03, research G7 / Pattern 8): Temporal Schedules registered
    # by the worker. The hold reaper runs every `reaper_interval_seconds`; the
    # pending-booking starter runs every `starter_interval_seconds` and only
    # touches appointments older than `starter_grace_seconds`, so it never
    # races the API's own start right after the accept commit.
    reaper_interval_seconds: int = Field(
        default=60, gt=0, validation_alias="SCHEDULING_REAPER_INTERVAL_SECONDS"
    )
    starter_interval_seconds: int = Field(
        default=30, gt=0, validation_alias="SCHEDULING_STARTER_INTERVAL_SECONDS"
    )
    starter_grace_seconds: int = Field(
        default=30, gt=0, validation_alias="SCHEDULING_STARTER_GRACE_SECONDS"
    )

    # D-13: a cancel at least this many hours before the slot starts is
    # refunded (refund_pending -> refunded); inside it, no_refund. Staff may
    # waive the cutoff. Decided in the API request, never in the workflow.
    cancel_cutoff_hours: int = Field(
        default=24, ge=0, validation_alias="SCHEDULING_CANCEL_CUTOFF_HOURS"
    )


settings = SchedulingSettings()
