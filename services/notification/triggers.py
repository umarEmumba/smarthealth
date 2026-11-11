"""Event -> notification and reminder triggers (plan 03-14; NOTIF-01..04, EVENT-03,
D-02, D-09 revised, research Pattern 6/7).

`NotificationTriggers` is the handler set of the `notification-trigger` consumer group.
Each handler turns one decoded event into at most:

* one Celery send (`enqueue_send`, a dedupe key of type + appointment id + event id),
* one Temporal reminder operation (deterministic workflow ids, signal-with-start), and
* one write to the closed-appointment guard (Mongo).

Idempotency (D-08 refined: no processed-event table here). A redelivered event is
handled again, and every effect carries its own guard: the send task's delivery log and
the provider's idempotency store dedupe the key, the reminder helpers are idempotent
(`USE_EXISTING` + deterministic ids) and the closed-appointment write is an upsert. The
Phase 2 workflows' notification stubs stay intent records, so nothing sends twice
(D-09 revised): this consumer is the only sender for booking, cancellation and
reschedule.

Failure classification (the consumer base in `libs.events.consumer` acts on it):

* a payload that cannot be turned into a send or a workflow input (a missing field, a
  non-UUID id) can never succeed: `NonRetryableEventError`, dead-lettered after one try;
* a broker, Temporal or Mongo outage: `TransientEventError`, the offset is not committed
  and the event is retried, so an outage delays a trigger and never drops it;
* anything else goes through the base's bounded retries.

Booking-type events (booked, cancelled, rescheduled) older than
`NOTIFICATION_MAX_EVENT_AGE_MINUTES` are recorded `skipped_stale` and not published, so a
replay of history never sends day-old confirmations; the reminder side effects still run
(a workflow skips the leads that are already in the past itself).

Everything handled here is ids, types and timestamps: no payload text is logged and the
Celery kwargs hold ids and timestamps only (PHI rule, T-3-44).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from amqp.exceptions import AMQPError
from kombu.exceptions import KombuError
from pydantic import ValidationError
from pymongo.database import Database
from pymongo.errors import PyMongoError
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from libs.events.catalog import EventType
from libs.events.consumer import EventEnvelope, Handler, NonRetryableEventError, TransientEventError
from libs.observability.logging import get_logger
from services.notification import deliveries, reminders
from services.notification.config import NotificationSettings
from services.notification.config import settings as default_settings
from services.notification.tasks import SKIPPED, SendRequest

__all__ = ["NotificationTriggers", "dedupe_key_for"]

log = get_logger(__name__)

TYPE_BOOKED = "appointment.booked"
TYPE_CANCELLED = "appointment.cancelled"
TYPE_RESCHEDULED = "appointment.rescheduled"

# Temporal statuses that mean "the server is not serving right now", not "this request is wrong".
_TRANSIENT_RPC_STATUSES = frozenset(
    {
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.ABORTED,
        RPCStatusCode.CANCELLED,
        RPCStatusCode.UNKNOWN,
        RPCStatusCode.INTERNAL,
    }
)


class Publisher(Protocol):
    """Publishes one `notifications.send` (blocking; the triggers call it in a thread)."""

    def __call__(self, **kwargs: str) -> Any: ...


TemporalSource = Client | Callable[[], Awaitable[Client]]
DbSource = Database | Callable[[], Database]


def dedupe_key_for(notification_type: str, appointment_id: str, event_id: str) -> str:
    """`{type}:{appointment_id}:{event_id}`; never starts with the `FAIL-` injection prefix."""
    return f"{notification_type}:{appointment_id}:{event_id}"


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, RPCError):
        return exc.status in _TRANSIENT_RPC_STATUSES
    # KombuError / AMQPError: the Celery broker; PyMongoError: the delivery log;
    # RuntimeError: the Temporal SDK's lost-connection / closed-client signal.
    return isinstance(
        exc, (KombuError, AMQPError, PyMongoError, RuntimeError, TimeoutError, ConnectionError, OSError)
    )


def _uuid(value: str) -> str:
    """The id unchanged when it is UUID-shaped, else ValueError (a poison id must not reach Mongo)."""
    UUID(value)
    return value


def _iso(value: Any) -> str:
    """An Avro timestamp (aware datetime, epoch millis or ISO text) as an ISO-8601 string."""
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        moment = datetime.fromtimestamp(value / 1000, tz=UTC)
    elif isinstance(value, str):
        moment = datetime.fromisoformat(value)
        moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    else:
        raise TypeError("not a timestamp")
    return moment.astimezone(UTC).isoformat()


class NotificationTriggers:
    """The notification-trigger handlers. Ports are injected so unit tests need no
    broker, Temporal server or Mongo for the routing logic.

    `publisher`   sync callable taking `enqueue_send` kwargs (production: `enqueue_send`
                  bound to the module Celery app), run through `asyncio.to_thread`;
    `temporal`    a Temporal client or an async factory (connected lazily on the
                  consumer's persistent loop, where the client must live);
    `deliveries_db` a sync Mongo database or a factory for one;
    `helpers`     the reminder helper module (injectable so tests can fake Temporal).
    """

    def __init__(
        self,
        *,
        publisher: Publisher,
        temporal: TemporalSource,
        deliveries_db: DbSource,
        settings: NotificationSettings | None = None,
        helpers: Any = reminders,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._publisher = publisher
        self._temporal = temporal
        self._db = deliveries_db
        self._settings = settings if settings is not None else default_settings
        self._helpers = helpers
        self._now = now or (lambda: datetime.now(UTC))
        self._client: Client | None = None

    # ------------------------------------------------------------- plumbing

    def handlers(self) -> Mapping[str, Handler]:
        return {
            EventType.APPOINTMENT_BOOKED: self.on_booked,
            EventType.APPOINTMENT_CANCELLED: self.on_cancelled,
            EventType.APPOINTMENT_RESCHEDULED: self.on_rescheduled,
            EventType.APPOINTMENT_NO_SHOW: self.on_no_show,
            EventType.APPOINTMENT_CHECKED_IN: self.on_checked_in,
            EventType.VISIT_COMPLETED: self.on_visit_completed,
        }

    def _database(self) -> Database:
        if isinstance(self._db, Database) or not callable(self._db):
            return self._db  # type: ignore[return-value]
        return self._db()

    async def _client_for_call(self) -> Client:
        if isinstance(self._temporal, Client):
            return self._temporal
        if self._client is None:
            self._client = await self._temporal()
        return self._client

    async def _guarded(self, envelope: EventEnvelope, step: Awaitable[Any]) -> Any:
        """Run one effect, mapping failures to the consumer base's classification."""
        try:
            return await step
        except (NonRetryableEventError, TransientEventError):
            raise
        except (KeyError, ValueError, TypeError, ValidationError) as exc:
            log.warning(
                "notification.invalid_event",
                event_id=str(envelope.event_id),
                event_type=envelope.event_type,
                error=type(exc).__name__,
            )
            raise NonRetryableEventError("InvalidEvent") from None
        except Exception as exc:  # noqa: BLE001 - classified here, re-raised if not transient
            if _is_transient(exc):
                log.warning(
                    "notification.trigger_transient_failure",
                    event_id=str(envelope.event_id),
                    event_type=envelope.event_type,
                    error=type(exc).__name__,
                )
                raise TransientEventError(type(exc).__name__) from exc
            raise

    def _is_stale(self, envelope: EventEnvelope) -> bool:
        age = self._now() - envelope.occurred_at
        return age > timedelta(minutes=self._settings.max_event_age_minutes)

    def _send_kwargs(
        self, envelope: EventEnvelope, notification_type: str, patient_id: str, appointment_id: str
    ) -> dict[str, str]:
        event_id = str(envelope.event_id)
        return {
            "notification_type": notification_type,
            "dedupe_key": dedupe_key_for(notification_type, appointment_id, event_id),
            "patient_id": patient_id,
            "appointment_id": appointment_id,
            "event_id": event_id,
            "requested_at": self._now().isoformat(),
        }

    async def _send(
        self, envelope: EventEnvelope, notification_type: str, patient_id: str, appointment_id: str
    ) -> None:
        """Publish one send, or record it `skipped_stale` when the event is too old.

        The kwargs are validated first (a malformed id raises before anything is written
        or published); the publish runs in a worker thread so the consumer loop never
        blocks on the broker."""
        kwargs = self._send_kwargs(envelope, notification_type, patient_id, appointment_id)
        request = SendRequest(**kwargs)
        if self._is_stale(envelope):
            await asyncio.to_thread(deliveries.record_skipped, self._database(), request, "stale")
            SKIPPED.labels(notification_type, "stale").inc()
            log.info(
                "notification.skipped",
                notification_type=notification_type,
                dedupe_key=request.dedupe_key,
                reason="stale",
            )
            return
        await asyncio.to_thread(self._publisher, **kwargs)

    # -------------------------------------------------------------- handlers

    async def on_booked(self, envelope: EventEnvelope) -> None:
        async def effect() -> None:
            payload = envelope.payload
            patient_id = str(payload["patient_id"])
            appointment_id = str(payload["appointment_id"])
            slot_start = _iso(payload["slot_start"])
            await self._send(envelope, TYPE_BOOKED, patient_id, appointment_id)
            await self._helpers.start_reminder(
                await self._client_for_call(),
                appointment_id=appointment_id,
                patient_id=patient_id,
                slot_start_iso=slot_start,
                lead_minutes=self._settings.reminder_leads,
            )

        await self._guarded(envelope, effect())

    async def on_cancelled(self, envelope: EventEnvelope) -> None:
        """Cancellation: stop the reminder, record the appointment closed (so a reminder
        that already fired is skipped at send time) and send one cancellation notice."""

        async def effect() -> None:
            payload = envelope.payload
            patient_id = str(payload["patient_id"])
            appointment_id = str(payload["appointment_id"])
            await self._close(appointment_id, "cancelled")
            await self._helpers.cancel_reminder(await self._client_for_call(), appointment_id)
            await self._send(envelope, TYPE_CANCELLED, patient_id, appointment_id)

        await self._guarded(envelope, effect())

    async def on_rescheduled(self, envelope: EventEnvelope) -> None:
        """Reschedule: move the reminder to the new slot and send one reschedule notice."""

        async def effect() -> None:
            payload = envelope.payload
            patient_id = str(payload["patient_id"])
            appointment_id = str(payload["appointment_id"])
            new_slot = _iso(payload["new_slot_start"])
            await self._send(envelope, TYPE_RESCHEDULED, patient_id, appointment_id)
            await self._helpers.reschedule_reminder(
                await self._client_for_call(),
                appointment_id=appointment_id,
                patient_id=patient_id,
                new_slot_iso=new_slot,
                lead_minutes=self._settings.reminder_leads,
            )

        await self._guarded(envelope, effect())

    async def on_no_show(self, envelope: EventEnvelope) -> None:
        """No-show: no notification, only the reminder stops and the appointment is closed."""

        async def effect() -> None:
            appointment_id = str(envelope.payload["appointment_id"])
            await self._close(appointment_id, "no_show")
            await self._helpers.cancel_reminder(await self._client_for_call(), appointment_id)

        await self._guarded(envelope, effect())

    async def on_checked_in(self, envelope: EventEnvelope) -> None:
        """Check-in: the patient is here, so the reminder is no longer wanted."""

        async def effect() -> None:
            appointment_id = str(envelope.payload["appointment_id"])
            await self._close(appointment_id, "checked_in")
            await self._helpers.cancel_reminder(await self._client_for_call(), appointment_id)

        await self._guarded(envelope, effect())

    async def on_visit_completed(self, envelope: EventEnvelope) -> None:
        """A completed visit starts the follow-up workflow (`followup-{visit_id}`)."""

        async def effect() -> None:
            payload = envelope.payload
            visit_id = _uuid(str(payload["visit_id"]))
            appointment_id = _uuid(str(payload["appointment_id"]))
            patient_id = _uuid(str(payload["patient_id"]))
            completed_at = _iso(payload["completed_at"])
            await self._helpers.start_followup(
                await self._client_for_call(),
                visit_id=visit_id,
                appointment_id=appointment_id,
                patient_id=patient_id,
                completed_at_iso=completed_at,
                delay_minutes=self._settings.followup_delay_minutes,
            )

        await self._guarded(envelope, effect())

    async def _close(self, appointment_id: str, reason: str) -> None:
        await asyncio.to_thread(
            deliveries.mark_appointment_closed, self._database(), _uuid(appointment_id), reason
        )
