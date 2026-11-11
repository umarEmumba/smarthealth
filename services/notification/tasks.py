"""The `notifications.send` Celery task (plan 03-02).

One send per dedupe key holds at two layers: the delivery log (no new attempt
once `sent`) and the provider's idempotency store (unique `dedupe_key`, checked
and written atomically at the call). A worker killed between the provider call
and `mark_sent` therefore replays the recorded result instead of sending again.
A real external provider needs to accept an idempotency key for this to hold;
without one, each crash at exactly that point could cause one duplicate (stated
in the docs, plan 03-18).

Task kwargs are plain strings (ids, a type, a dedupe key, an ISO timestamp):
never a Pydantic object and never PHI. The task re-validates them with
`SendRequest` inside the worker, so a malformed message is rejected there
instead of being trusted because it came off the broker.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from celery import Celery, shared_task
from celery.exceptions import Reject, Retry
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pymongo.errors import PyMongoError

from libs.observability.logging import get_logger
from services.notification.celery_app import ensure_broker_url
from services.notification.config import settings
from services.notification.deliveries import (
    Claim,
    claim_delivery,
    get_db,
    is_appointment_closed,
    mark_failed,
    mark_sent,
    mark_skipped,
)
from services.notification.provider import (
    FakeProvider,
    PermanentDeliveryError,
    TransientDeliveryError,
)

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
SKIPPED = Counter(
    "notifications_skipped_total",
    "Notifications not sent because they were no longer wanted",
    ["notification_type", "reason"],
)
RETRIED = Counter(
    "notifications_retried_total", "Transient provider failures retried with backoff", ["notification_type"]
)
DEAD_LETTERED = Counter(
    "notifications_dead_lettered_total",
    "Notifications rejected into the dead-letter queue",
    ["notification_type", "error_type"],
)

CRASH_MARKER = "CRASH-AFTER-PROVIDER-"
CRASH_EXIT_CODE = 70


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


def _label_type(kwargs: dict[str, Any]) -> str:
    value = kwargs.get("notification_type")
    return value if isinstance(value, str) and value in NOTIFICATION_TYPES else "unknown"


def _dead_letter(
    notification_type: str, exc: BaseException, request: SendRequest | None, db: Any = None
) -> Reject:
    """Count and log, then return the `Reject(requeue=False)` that dead-letters the message."""
    error_type = type(exc).__name__
    if request is not None and db is not None:
        try:
            mark_failed(db, request.dedupe_key, error_type)
        except PyMongoError:
            pass
    DEAD_LETTERED.labels(notification_type, error_type).inc()
    logger.warning(
        "notification.dead_lettered",
        notification_type=notification_type,
        dedupe_key=request.dedupe_key if request is not None else None,
        error_type=error_type,
    )
    return Reject(exc, requeue=False)


def _transient(
    task: Any, request: SendRequest, db: Any, exc: BaseException
) -> BaseException:
    """Retry with exponential backoff, or return the dead-letter Reject when retries are exhausted."""
    retries = task.request.retries
    if retries >= settings.max_retries:
        return _dead_letter(request.notification_type, exc, request, db)
    try:
        mark_failed(db, request.dedupe_key, type(exc).__name__)
    except PyMongoError:
        pass
    RETRIED.labels(request.notification_type).inc()
    return task.retry(exc=exc, countdown=settings.retry_backoff_base_seconds * 2**retries, throw=False)


@shared_task(bind=True, name="notifications.send", max_retries=settings.max_retries)
def send(self: Any, *args: Any, **kwargs: Any) -> str:
    notification_type = _label_type(kwargs)
    if args:
        raise _dead_letter(notification_type, TypeError("positional arguments"), None)
    try:
        request = SendRequest(**kwargs)
    except ValidationError as exc:
        raise _dead_letter(notification_type, exc, None) from exc

    db = get_db()
    try:
        return _deliver(self, request, db)
    except (Retry, Reject):
        raise
    except TransientDeliveryError as exc:
        raise _transient(self, request, db, exc) from exc
    except PyMongoError as exc:
        # The delivery log is unreachable: retry rather than lose the message.
        raise _transient(self, request, db, exc) from exc
    except PermanentDeliveryError as exc:
        raise _dead_letter(request.notification_type, exc, request, db) from exc
    except Exception as exc:
        raise _dead_letter(request.notification_type, exc, request, db) from exc


def _deliver(task: Any, request: SendRequest, db: Any) -> str:
    if request.notification_type == "appointment.reminder" and is_appointment_closed(
        db, request.appointment_id
    ):
        mark_skipped(db, request, "closed")
        SKIPPED.labels(request.notification_type, "closed").inc()
        logger.info(
            "notification.skipped",
            notification_type=request.notification_type,
            dedupe_key=request.dedupe_key,
            reason="closed",
        )
        return "skipped_closed"

    claim = claim_delivery(db, request)
    if claim.state is Claim.DONE:
        return "duplicate"

    result = FakeProvider(db).deliver(request, claim.attempts)
    if result.replayed:
        logger.info(
            "notification.provider_replayed",
            notification_type=request.notification_type,
            dedupe_key=request.dedupe_key,
        )

    if (
        settings.fake_crash_enabled
        and claim.attempts == 1
        and request.dedupe_key.startswith(f"{settings.fake_fail_prefix}{CRASH_MARKER}")
    ):
        logger.warning("notification.fake_crash", dedupe_key=request.dedupe_key)
        os._exit(CRASH_EXIT_CODE)  # no cleanup, no ack: models a killed worker

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
