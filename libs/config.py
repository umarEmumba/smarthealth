"""Shared infrastructure settings, common to every service process.

`SharedSettings` holds ONLY what every service needs and none of them owns:
the shared Mongo audit sink, the shared Redis (JWT denylist, rate limits),
the OTLP endpoint, the internal base URLs of the other services, and the
JWKS-cache / internal-HTTP knobs. Each service subclasses it (e.g.
`IdentitySettings`) to add its OWN prefixed database URL and secrets.

Deliberately absent: any database URL and any JWT secret. A bare
`DATABASE_URL` shared across services would silently point a new service at
someone else's database (defeating database-per-service, D-01), and a shared
signing secret is exactly what RS256/JWKS removes — Identity alone holds a
private key, everyone else only ever sees public keys.

Alias binding (`IDENTITY_DATABASE_URL`) is by ALIAS ONLY: `validate_by_name`
is False. pydantic-settings treats `populate_by_name=True` as "ALSO read the
env var named after the field", which would make `database_url` silently pick
up a stale, unprefixed `DATABASE_URL` and point a service at the wrong
database (verified against pydantic-settings 2.15.0). Construct alias-bound
fields by alias (`IdentitySettings(IDENTITY_DATABASE_URL=...)`) or via the
environment. No secrets are hardcoded here; every value is read from the environment (or
a single `.env` during local dev, per `.env.example`).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class SharedSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        validate_by_name=False,
        validate_by_alias=True,
    )

    environment: str = "development"

    # --- MongoDB (shared audit sink; D-02: audit stays a library) ---
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db_name: str = "smarthealth"

    # --- Redis (shared JWT denylist, rate-limit counters) ---
    redis_url: str = "redis://localhost:6379/0"

    # --- OpenTelemetry / Jaeger (OTLP ingestion) ---
    otlp_endpoint: str = "http://localhost:4317"

    # --- Internal service base URLs (docker-compose network names) ---
    identity_service_url: str = "http://identity:8000"
    organization_service_url: str = "http://organization:8000"
    provider_service_url: str = "http://provider:8000"
    patient_service_url: str = "http://patient:8000"
    scheduling_service_url: str = "http://scheduling:8000"
    visit_service_url: str = "http://visit:8000"
    billing_service_url: str = "http://billing:8000"

    # --- JWKS cache + internal HTTP behaviour ---
    jwks_cache_ttl_seconds: int = 300
    jwks_miss_backoff_seconds: int = 30
    internal_http_timeout_seconds: float = 2.0

    @property
    def jwks_url(self) -> str:
        return f"{self.identity_service_url}/.well-known/jwks.json"


shared_settings = SharedSettings()
