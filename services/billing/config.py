"""Billing service settings: the shared infra settings plus Billing's OWN
database URL and the deterministic pre-check rule's failing prefix (D-10).

`database_url` is bound to `BILLING_DATABASE_URL` only — there is deliberately
no bare `DATABASE_URL` (it would silently point a service at someone else's
database, defeating database-per-service, D-04). The default is the local-dev
placeholder credential for the `billing_svc` role that the db-init job creates;
real deployments override it from the environment.

Billing calls no other service (it only answers Scheduling and Visit), so it
has no client secret of its own here.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class BillingSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://billing_svc:changeme-billing-local-dev-only@localhost:5433/billing_db",
        validation_alias="BILLING_DATABASE_URL",
    )
    # D-10: a coverage reference starting with this prefix (case-sensitive) is
    # deterministically DENIED by the simulated pre-check. Configurable so a
    # demo or test can force a failed booking without randomness.
    precheck_fail_prefix: str = Field(
        default="DENY-", min_length=1, validation_alias="BILLING_PRECHECK_FAIL_PREFIX"
    )


settings = BillingSettings()
