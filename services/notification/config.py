"""Notification service settings (plan 03-02).

The broker password never lives in an environment variable or in this object's
repr: `NOTIFICATION_BROKER_PASSWORD_FILE` names a file (a Compose secret in
deployments, `infra/secrets/infra/notification_broker.secret` locally) that is
read at the moment a URL is needed, held as a `SecretStr` only for the length of
the URL construction, and never logged. Importing this module reads no file.

There is deliberately no database setting: the delivery log lives in the shared
Mongo (`shared_settings.mongo_db_name`, D-02), not in a per-service Postgres.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from pydantic import Field, SecretStr, field_validator

from libs.config import SharedSettings


class BrokerPasswordError(RuntimeError):
    """The broker password file could not be read (the message names the setting only)."""


class NotificationSettings(SharedSettings):
    broker_host: str = Field(default="localhost", validation_alias="NOTIFICATION_BROKER_HOST")
    broker_port: int = Field(default=5672, validation_alias="NOTIFICATION_BROKER_PORT")
    broker_vhost: str = Field(default="smarthealth", validation_alias="NOTIFICATION_BROKER_VHOST")
    broker_user: str = Field(
        default="smarthealth_notification", validation_alias="NOTIFICATION_BROKER_USER"
    )
    broker_password_file: str = Field(
        default="infra/secrets/infra/notification_broker.secret",
        validation_alias="NOTIFICATION_BROKER_PASSWORD_FILE",
    )
    # Failure injection by dedupe-key prefix, mirroring BILLING_PRECHECK_FAIL_PREFIX.
    fake_fail_prefix: str = Field(default="FAIL-", validation_alias="NOTIFICATION_FAKE_FAIL_PREFIX")
    # Test-only switch: lets a `<fail_prefix>CRASH-AFTER-PROVIDER-` send end the
    # worker process right after the provider call. Nothing in Compose sets it.
    fake_crash_enabled: bool = Field(default=False, validation_alias="NOTIFICATION_FAKE_CRASH_ENABLED")
    retry_backoff_base_seconds: float = Field(
        default=2.0, gt=0, validation_alias="NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS"
    )
    max_retries: int = Field(default=5, ge=0, validation_alias="NOTIFICATION_MAX_RETRIES")
    metrics_port: int = Field(default=9100, ge=0, validation_alias="NOTIFICATION_METRICS_PORT")
    # Read by plan 03-14's broker provisioning job, not by the worker itself.
    rabbitmq_management_url: str = Field(
        default="http://localhost:15672", validation_alias="NOTIFICATION_RABBITMQ_MANAGEMENT_URL"
    )

    # Reminder scheduling (plan 03-06, assumption A7). The leads and the delay are
    # passed to the workflows as inputs; a workflow never reads these settings.
    reminder_leads_minutes: str = Field(
        default="1440,120", validation_alias="NOTIFICATION_REMINDER_LEADS_MINUTES"
    )
    followup_delay_minutes: int = Field(
        default=4320, gt=0, validation_alias="NOTIFICATION_FOLLOWUP_DELAY_MINUTES"
    )
    temporal_heartbeat_path: str = Field(
        default="/tmp/notification-temporal-worker.alive",  # noqa: S108 - liveness file, no secret
        validation_alias="NOTIFICATION_TEMPORAL_HEARTBEAT_PATH",
    )

    @field_validator("reminder_leads_minutes")
    @classmethod
    def _leads_are_positive_integers(cls, value: str) -> str:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if not parts or not all(part.isdigit() and int(part) > 0 for part in parts):
            raise ValueError("NOTIFICATION_REMINDER_LEADS_MINUTES must be a comma list of positive integers")
        return value

    @property
    def reminder_leads(self) -> list[int]:
        """The reminder leads in minutes, largest first, without duplicates."""
        parts = {int(part) for part in self.reminder_leads_minutes.split(",") if part.strip()}
        return sorted(parts, reverse=True)

    def read_broker_password(self) -> SecretStr:
        try:
            raw = Path(self.broker_password_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise BrokerPasswordError(
                "NOTIFICATION_BROKER_PASSWORD_FILE is not readable (set it to the broker password file)"
            ) from exc
        if not raw:
            raise BrokerPasswordError("NOTIFICATION_BROKER_PASSWORD_FILE is empty")
        return SecretStr(raw)

    @property
    def broker_url(self) -> str:
        """The AMQP URL, built on demand so the password file is read lazily."""
        password = self.read_broker_password().get_secret_value()
        return (
            f"amqp://{quote(self.broker_user, safe='')}:{quote(password, safe='')}"
            f"@{self.broker_host}:{self.broker_port}/{quote(self.broker_vhost, safe='')}"
        )


settings = NotificationSettings()
