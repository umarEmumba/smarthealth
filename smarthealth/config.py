"""Shared application configuration.

A single pydantic-settings `Settings` object, imported by the API process and
(in later phases) every Celery/Temporal worker process, so database, broker,
and OTLP URLs stay identical between how the app connects and how Docker
Compose wires the network (see infra/docker-compose.yml `env_file:`).

No secrets are hardcoded here — every field is read from the environment
(or a single `.env` file during local dev, per `.env.example`).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


_INSECURE_DEFAULT_JWT_SECRET = "changeme-local-dev-only"


class Settings(BaseSettings):
    """Application-wide settings, loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Deployment environment ("development" | "test" | "staging" | "production") ---
    # Defaults to "development" so local dev / CI never breaks; anything
    # else is treated as a real deployment for the JWT-secret fail-closed
    # check below (CR-03).
    environment: str = "development"

    # --- PostgreSQL (system of record) ---
    database_url: str = "postgresql+asyncpg://smarthealth:changeme@localhost:5432/smarthealth"

    # --- MongoDB (audit trail / operational logs) ---
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db_name: str = "smarthealth"

    # --- Redis (cache / locks / denylist / rate limiting) ---
    redis_url: str = "redis://localhost:6379/0"

    # --- JWT / Auth ---
    jwt_secret: str = _INSECURE_DEFAULT_JWT_SECRET
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # --- OpenTelemetry / Jaeger (OTLP ingestion) ---
    otlp_endpoint: str = "http://localhost:4317"

    def model_post_init(self, __context: object) -> None:
        """CR-03: fail closed rather than silently booting with a
        source-visible, well-known JWT secret in any real deployment.

        `environment="development"` (the default, used for local dev and
        CI) is exempt — everywhere else (staging/production/anything not
        explicitly "development") MUST have JWT_SECRET set via the
        environment/secret-manager, or the process refuses to start.
        """
        if self.environment != "development" and self.jwt_secret == _INSECURE_DEFAULT_JWT_SECRET:
            raise RuntimeError(
                "JWT_SECRET must be set via environment/secret-manager in any "
                "deployment where ENVIRONMENT != 'development' — refusing to "
                "start with the source-visible default secret."
            )


settings = Settings()
