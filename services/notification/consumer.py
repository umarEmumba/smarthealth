"""notification-trigger consumer: `python -m services.notification.consumer` (plan 03-14;
NOTIF-01..04, EVENT-03, EVENT-05, D-02, D-09 revised, D-13).

Reads the appointment and visit topics in the `notification-trigger` group and, through
`NotificationTriggers`, turns booking, cancellation and reschedule events into Celery
sends and every appointment/visit lifecycle event into Temporal reminder control (see
`triggers.py`). Poison messages go to `smarthealth.dlq.notification-trigger` through the
consumer base.

Start-up order matters:

1. logging, tracer provider (`smarthealth-notification-consumer`) and
   `instrument_publisher()`, so the Celery publish joins the consumer span and the send
   task's span is its child (one booking stays one trace, D-11);
2. the delivery-log indexes (the unique dedupe key must exist before the first write)
   and the broker topology (`declare_topology`, idempotent): a message published to a
   queue that does not exist yet would be dropped as unroutable, and the Celery worker
   that normally declares it may not have started;
3. the Schema Registry warm-up (retried while it starts);
4. the go-live offset (D-13): with `NOTIFICATION_CONSUMER_START=latest` (Compose) the
   group's position is pinned to the current end of the topics before it subscribes, so
   the throw-away events already retained are not turned into reminders and sends, while
   an event produced right after the container is healthy is never skipped. A restarted
   container keeps its committed position.

The Temporal client connects lazily on the consumer's persistent event loop (a client
must live on the loop that uses it), so the process boots while Temporal is down and an
outage is retried per event as a transient failure. The metrics port is internal to the
Compose network and is never published to the host.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

from libs.events.catalog import DLQ_TOPICS, EventType, topic_for
from libs.events.consumer import EventConsumer
from libs.events.registry import warm_up
from libs.events.start_offsets import pin_start_offsets
from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from libs.temporal import connect_client
from services.notification import deliveries
from services.notification.celery_app import app as celery_app
from services.notification.celery_app import declare_topology, instrument_publisher
from services.notification.config import settings
from services.notification.tasks import enqueue_send
from services.notification.triggers import NotificationTriggers

__all__ = ["GROUP_ID", "SUBSCRIBED_EVENT_TYPES", "TOPICS", "build_consumer", "build_triggers", "main"]

log = get_logger(__name__)

GROUP_ID = "notification-trigger"
SERVICE_NAME = "smarthealth-notification-consumer"

# The consumer reads every event type of the two topics it subscribes to; the base
# ignores (and commits past) the ones without a handler.
TOPICS = [topic_for(EventType.APPOINTMENT_BOOKED), topic_for(EventType.VISIT_COMPLETED)]
SUBSCRIBED_EVENT_TYPES = (
    EventType.APPOINTMENT_BOOKED,
    EventType.APPOINTMENT_CANCELLED,
    EventType.APPOINTMENT_RESCHEDULED,
    EventType.APPOINTMENT_CHECKED_IN,
    EventType.APPOINTMENT_NO_SHOW,
    EventType.VISIT_COMPLETED,
)


def _publish(**kwargs: str) -> Any:
    return enqueue_send(celery_app, **kwargs)


async def _temporal() -> Any:
    return await connect_client(lazy=True)


def build_triggers(**overrides: Any) -> NotificationTriggers:
    kwargs: dict[str, Any] = {
        "publisher": _publish,
        "temporal": _temporal,
        "deliveries_db": deliveries.get_db,
    }
    kwargs.update(overrides)
    return NotificationTriggers(**kwargs)


def build_consumer(triggers: NotificationTriggers | None = None, **overrides: Any) -> EventConsumer:
    """The notification-trigger consumer: group `notification-trigger`, the appointment and
    visit topics, DLQ `smarthealth.dlq.notification-trigger`."""
    triggers = triggers if triggers is not None else build_triggers()
    kwargs: dict[str, Any] = {
        "group_id": GROUP_ID,
        "topics": TOPICS,
        "handlers": triggers.handlers(),
        "dlq_topic": DLQ_TOPICS[GROUP_ID],
        "heartbeat_path": settings.consumer_heartbeat_path,
        "metrics_port": settings.metrics_port,
        # Where a NEW group starts (D-13); a committed offset always wins.
        "consumer_overrides": {"auto.offset.reset": settings.consumer_start},
    }
    kwargs.update(overrides)
    return EventConsumer(**kwargs)


def main() -> int:
    configure_logging()
    configure_tracer_provider(SERVICE_NAME)
    instrument_publisher()
    deliveries.ensure_notification_indexes(deliveries.get_db())
    declare_topology(celery_app)
    # Resolve the event schema ids once, retried while the registry starts; a registry
    # that never comes up exits non-zero and the orchestrator restarts the container.
    asyncio.run(warm_up(SUBSCRIBED_EVENT_TYPES))
    pinned = pin_start_offsets(GROUP_ID, TOPICS, settings.consumer_start)
    consumer = build_consumer()
    log.info("consumer.started", group=GROUP_ID, start=settings.consumer_start, pinned_partitions=len(pinned))
    consumer.run()  # returns on SIGTERM/SIGINT after closing cleanly
    log.info("consumer.stopped", group=GROUP_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
