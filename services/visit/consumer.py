"""visit-downstream consumer: `python -m services.visit.consumer` (plan 03-09;
EVENT-03, D-03, D-08 refined, research Pattern 5).

Turns `visit.completed` (committed with the completion by `transition_visit`, relayed
to `smarthealth.visit.events` by the `visit-outbox` connector) into the existing
`VisitCompletedWorkflow` start and the conditional `downstream_started_at` marker, by
calling `ensure_downstream_started` unchanged. The Visit API makes no Temporal call
any more; this is the trigger.

Idempotency (D-08 refined: Temporal effects need no processed-event table): the
workflow id is `visit-completed-{visit_id}` and `start_once` starts it with USE_EXISTING
+ REJECT_DUPLICATE, so a redelivered event attaches to the running execution or is
refused once it closed, and the marker update is conditional. Handling the same event
twice therefore starts one execution and sets the marker once.

Failure classification (the consumer base in `libs.events.consumer` acts on it):

* an event for an unknown visit id, or for a visit that is not completed, can never
  succeed (forged, replayed from a wiped database, or a throw-away test event):
  `NonRetryableEventError`, dead-lettered after one attempt (T-3-27);
* a Temporal or database outage is `TransientEventError`, decided by the one shared
  classifier (`libs.events.classify`, plan 03-21): SQLSTATE classes 08, 53, 57 and 40
  (so a Postgres `max_connections` ceiling, 53300, or a restart, 57P03, never
  dead-letters a healthy event), SQLAlchemy connection errors and pool timeouts, the
  Temporal outage statuses (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED,
  ABORTED, CANCELLED, UNKNOWN, INTERNAL), the SDK's closed-client `RuntimeError`, and
  network errors. The offset is NOT committed and the event is retried with capped
  backoff; a transient failure is bounded by the consumer base's cap, after which the
  event is dead-lettered as `TransientExhausted` (visible and replayable, never a
  silent loss); the recovery routes are in docs/architecture.md section 10.2;
* anything else is a handler bug and goes through the base's bounded retries.

The Temporal client connects lazily on the consumer's persistent event loop (a client
must live on the loop that uses it), so the process boots while Temporal is down and
the first handled event connects. The workflow start happens inside the consumer span,
so the downstream workflow joins the visit's trace through `TracingInterceptor`.

Logs carry ids and types only (PHI rule, T-3-29). The metrics port is internal to the
Compose network and is never published to the host.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client

from libs.db.postgres import get_session_factory
from libs.events import classify
from libs.events.catalog import DLQ_TOPICS, EventType, topic_for
from libs.events.consumer import (
    EventConsumer,
    EventEnvelope,
    NonRetryableEventError,
    TransientEventError,
)
from libs.events.registry import warm_up
from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from libs.temporal import connect_client
from services.visit.config import settings
from services.visit.domain.states import VisitStatus
from services.visit.downstream import ensure_downstream_started
from services.visit.events import VISIT_EVENT_TYPES
from services.visit.repository import visit_repo

__all__ = ["GROUP_ID", "build_consumer", "handle_visit_completed", "main"]

log = get_logger(__name__)

GROUP_ID = "visit-downstream"
SERVICE_NAME = "smarthealth-visit-consumer"

SessionFactory = Callable[[], AsyncSession]

_client: Client | None = None


async def _temporal_client() -> Client:
    """The process's Temporal client, connected lazily on the calling (consumer) loop."""
    global _client  # noqa: PLW0603
    if _client is None:
        _client = await connect_client(lazy=True)
    return _client


async def handle_visit_completed(
    envelope: EventEnvelope,
    *,
    client: Client | None = None,
    session_factory: SessionFactory | None = None,
) -> None:
    """Start the visit's downstream workflow for one `visit.completed` event.

    `client` and `session_factory` are injection points for tests (the harness has no
    running consumer); production leaves them None and uses the process's own.
    """
    try:
        visit_id = uuid.UUID(str(envelope.payload["visit_id"]))
    except (KeyError, ValueError) as exc:
        raise NonRetryableEventError("InvalidVisitId") from exc

    try:
        factory = session_factory or get_session_factory(settings.database_url)
        async with factory() as db:
            visit = await visit_repo.get_visit(db, visit_id)
            if visit is None:
                log.warning("visit.event_unknown_visit", event_id=str(envelope.event_id))
                raise NonRetryableEventError("VisitNotFound")
            if visit.status != VisitStatus.COMPLETED:
                log.warning("visit.event_not_completed", event_id=str(envelope.event_id))
                raise NonRetryableEventError("VisitNotCompleted")
            temporal = client if client is not None else await _temporal_client()
            await ensure_downstream_started(temporal, db, visit)
    except NonRetryableEventError:
        raise
    except Exception as exc:  # noqa: BLE001 - classified below, re-raised if not transient
        if classify.is_transient(exc, temporal=True):
            log.warning(
                "visit.consumer_transient_failure",
                event_id=str(envelope.event_id),
                error=type(exc).__name__,
            )
            raise TransientEventError(type(exc).__name__) from exc
        raise


def build_consumer(**overrides: Any) -> EventConsumer:
    """The visit-downstream consumer: group `visit-downstream`, topic
    `smarthealth.visit.events`, DLQ `smarthealth.dlq.visit-downstream`."""
    kwargs: dict[str, Any] = {
        "group_id": GROUP_ID,
        "topics": [topic_for(EventType.VISIT_COMPLETED)],
        "handlers": {EventType.VISIT_COMPLETED: handle_visit_completed},
        "dlq_topic": DLQ_TOPICS[GROUP_ID],
        "heartbeat_path": settings.consumer_heartbeat_path,
        "metrics_port": settings.consumer_metrics_port,
    }
    kwargs.update(overrides)
    return EventConsumer(**kwargs)


def main() -> int:
    configure_logging()
    configure_tracer_provider(SERVICE_NAME)
    # Resolve the event schema ids once, retried while the registry starts; a registry
    # that never comes up exits non-zero and the orchestrator restarts the container.
    asyncio.run(warm_up(VISIT_EVENT_TYPES))
    consumer = build_consumer()
    log.info("consumer.started", group=GROUP_ID)
    consumer.run()  # returns on SIGTERM/SIGINT after closing cleanly
    log.info("consumer.stopped", group=GROUP_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
