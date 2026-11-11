"""The `notifications.send` Celery task (plan 03-02).

Task kwargs are plain strings (ids, a type, a dedupe key, an ISO timestamp):
never a Pydantic object and never PHI. The task re-validates them with
`SendRequest` inside the worker, so a malformed message is rejected there
instead of being trusted because it came off the broker.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from celery import Celery, shared_task
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, ConfigDict, Field, field_validator

from libs.observability.logging import get_logger
from services.notification.celery_app import ensure_broker_url
from services.notification.config import settings
from services.notification.deliveries import (
    Claim,
    claim_delivery,
    get_db,
    mark_sent,
)
from services.notification.provider import FakeProvider

logger = get_logger()

NotificationType = Literal[
    "appointment.booked",
    "appointment.cancelled",
    "appointment.rescheduled",
    "appointment.reminder",
    "visit.followup",
]
NOTIFICATION_TYPES: tuple[str, ...] = (
    "appointment.booked",
    "appointment.cancelled",
    "appointment.rescheduled",
    "appointment.reminder",
    "visit.followup",
)
QUEUE_FOR_TYPE: dict[str, str] = {
    "appointment.booked": "notifications.booking",
    "appointment.cancelled": "notifications.booking",
    "appointment.rescheduled": "notifications.booking",
    "appointment.reminder": "notifications.reminder",
    "visit.followup": "notifications.followup",
}

SENT = Counter("notifications_sent_total", "Notifications delivered to the provider", ["notification_type"])
DELIVERY_SECONDS = Histogram(
    "notification_delivery_seconds",
    "Seconds from the send request to provider success",
    ["notification_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600),
)


def _uuid_str(value: str) -> str:
    UUID(value)
    return value


class SendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    notification_type: NotificationType
    dedupe_key: str = Field(min_length=1, max_length=200)
    patient_id: str
    appointment_id: str
    visit_id: str | None = None
    event_id: str | None = None
    requested_at: str

    @field_validator("patient_id", "appointment_id", "visit_id", "event_id")
    @classmethod
    def _uuid_shaped(cls, value: str | None) -> str | None:
        return None if value is None else _uuid_str(value)

    @field_validator("requested_at")
    @classmethod
    def _iso_timestamp(cls, value: str) -> str:
        datetime.fromisoformat(value)
        return value


def enqueue_send(app: Celery, **kwargs: Any) -> Any:
    """Validate, then publish one `notifications.send` to the queue for its type."""
    request = SendRequest(**kwargs)
    ensure_broker_url(app)
    return app.send_task(
        "notifications.send",
        kwargs=request.model_dump(exclude_none=True),
        queue=QUEUE_FOR_TYPE[request.notification_type],
    )


def _seconds_since(requested_at: str) -> float:
    started = datetime.fromisoformat(requested_at)
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return max(0.0, (datetime.now(UTC) - started).total_seconds())


@shared_task(bind=True, name="notifications.send", max_retries=settings.max_retries)
def send(self: Any, *args: Any, **kwargs: Any) -> str:
    request = SendRequest(**kwargs)
    db = get_db()
    claim = claim_delivery(db, request)
    if claim.state is Claim.DONE:
        return "duplicate"
    result = FakeProvider(db).deliver(request, claim.attempts)
    mark_sent(db, request.dedupe_key, result.message_id, replayed=result.replayed)
    SENT.labels(request.notification_type).inc()
    DELIVERY_SECONDS.labels(request.notification_type).observe(_seconds_since(request.requested_at))
    logger.info(
        "notification.sent",
        notification_type=request.notification_type,
        dedupe_key=request.dedupe_key,
        patient_id=request.patient_id,
        appointment_id=request.appointment_id,
        attempts=claim.attempts,
    )
    return "sent"
