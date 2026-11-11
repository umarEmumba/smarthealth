"""analytics-projection consumer: `python -m services.analytics.consumer` (plan 03-10;
ANLY-06, EVENT-03, D-10 revised, D-08 refined).

Maintains the analytics_db projections from the three aggregate topics. This is a
Kafka consumer, not a Celery task: the EVENT-04 deviation the user accepted at plan
03-01, because a projection needs the idempotency claim in the SAME database
transaction as its upsert, which a Celery task cannot give.

`handle_event` opens one transaction: `claim_event` (consumer `analytics-projection`)
then `apply_event`, then commit. A redelivered event finds the claim and changes
nothing; an effect that fails rolls the claim back with it, so the base's bounded
retry re-applies it. Nothing outside analytics_db is read or written.

Failure classification (the consumer base in `libs.events.consumer` acts on it):

* a malformed payload (a missing field, a bad id) can never succeed:
  `NonRetryableEventError`, dead-lettered after one attempt;
* a database outage (`OperationalError`, `InterfaceError`, an invalidated connection,
  `OSError`) is `TransientEventError`: the offset is NOT committed and the event is
  retried, so an outage delays a projection and never drops it;
* anything else goes through the base's bounded retries.

Events for unknown or already-deleted ids are NOT errors: the upserts create a row
that the views exclude, so the live topics' throw-away test events are absorbed
without reaching the DLQ. Logs carry event ids and types only (no PHI exists in
events, T-3-34). The metrics port is internal to the Compose network and is never
published to the host. Plan 03-13 added the `analytics-consumer` container.

Where a new group starts (D-13): `ANALYTICS_CONSUMER_START` (`latest` in Compose,
`earliest` as the code default) maps to `auto.offset.reset` for a group without
committed offsets. `main()` additionally PINS the start: with `latest` it commits the
current high watermark for every partition of the group that has no committed offset
before the consumer subscribes, so an event produced right after the container
starts is never skipped by the usual "latest is decided at first assignment" race.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from typing import Any

import sqlalchemy.exc
from sqlalchemy.ext.asyncio import AsyncSession

from libs.db.postgres import get_session_factory
from libs.events.catalog import DLQ_TOPICS, topic_for
from libs.events.consumer import (
    EventConsumer,
    EventEnvelope,
    NonRetryableEventError,
    TransientEventError,
)
from libs.events.idempotency import claim_event
from libs.events.registry import warm_up
from libs.events.start_offsets import pin_start_offsets
from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from services.analytics.config import settings
from services.analytics.projections import PROJECTED_EVENT_TYPES, apply_event

__all__ = [
    "CONSUMER_NAME",
    "GROUP_ID",
    "build_consumer",
    "handle_event",
    "main",
    "pin_start_offsets",
]

log = get_logger(__name__)

GROUP_ID = "analytics-projection"
CONSUMER_NAME = GROUP_ID  # the `processed_events.consumer` value
SERVICE_NAME = "smarthealth-analytics-consumer"

SessionFactory = Callable[[], AsyncSession]


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, sqlalchemy.exc.DBAPIError):
        return exc.connection_invalidated or isinstance(
            exc, (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)
        )
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))


async def handle_event(
    envelope: EventEnvelope, *, session_factory: SessionFactory | None = None
) -> None:
    """Project one event: claim its id and apply it in one transaction.

    `session_factory` is an injection point for tests; production leaves it None and
    uses the process's own analytics_db factory, created lazily on the calling loop
    (the consumer's persistent loop).
    """
    try:
        factory = session_factory or get_session_factory(settings.database_url)
        async with factory() as db, db.begin():
            if not await claim_event(db, event_id=envelope.event_id, consumer=CONSUMER_NAME):
                return  # already projected: nothing to commit
            await apply_event(db, envelope)
    except (KeyError, ValueError, TypeError) as exc:
        log.warning(
            "analytics.invalid_event",
            event_id=str(envelope.event_id),
            event_type=envelope.event_type,
            error=type(exc).__name__,
        )
        raise NonRetryableEventError("InvalidEvent") from exc
    except Exception as exc:  # noqa: BLE001 - classified below, re-raised if not transient
        if _is_transient(exc):
            log.warning(
                "analytics.consumer_transient_failure",
                event_id=str(envelope.event_id),
                error=type(exc).__name__,
            )
            raise TransientEventError(type(exc).__name__) from exc
        raise


def build_consumer(**overrides: Any) -> EventConsumer:
    """The analytics-projection consumer: group `analytics-projection`, the patient,
    appointment and visit topics, DLQ `smarthealth.dlq.analytics-projection`."""
    topics = sorted({topic_for(event_type) for event_type in PROJECTED_EVENT_TYPES})
    kwargs: dict[str, Any] = {
        "group_id": GROUP_ID,
        "topics": topics,
        "handlers": {event_type: handle_event for event_type in PROJECTED_EVENT_TYPES},
        "dlq_topic": DLQ_TOPICS[GROUP_ID],
        "heartbeat_path": settings.consumer_heartbeat_path,
        "metrics_port": settings.consumer_metrics_port,
        # Where a NEW group starts (D-13); a committed offset always wins.
        "consumer_overrides": {"auto.offset.reset": settings.consumer_start},
    }
    kwargs.update(overrides)
    return EventConsumer(**kwargs)


def main() -> int:
    configure_logging()
    configure_tracer_provider(SERVICE_NAME)
    # Resolve the event schema ids once, retried while the registry starts; a registry
    # that never comes up exits non-zero and the orchestrator restarts the container.
    asyncio.run(warm_up(PROJECTED_EVENT_TYPES))
    topics = sorted({topic_for(event_type) for event_type in PROJECTED_EVENT_TYPES})
    pinned = pin_start_offsets(GROUP_ID, topics, settings.consumer_start)
    consumer = build_consumer()
    log.info(
        "consumer.started",
        group=GROUP_ID,
        start=settings.consumer_start,
        pinned_partitions=len(pinned),
    )
    consumer.run()  # returns on SIGTERM/SIGINT after closing cleanly
    log.info("consumer.stopped", group=GROUP_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
