"""Provider's domain event: the schedule-change payload builder and outbox emit helper
(plan 03-08; EVENT-01, EVENT-02; D-06, D-07).

`provider.schedule_changed` is written to provider_db's outbox from
`record_schedule_change`, right after the `schedule_change_events` row is flushed and
inside the caller's transaction. The Phase 2.1 pull feed (`/internal/schedule-changes`
and its acknowledgement) is unchanged; the event is the same change published for
consumers. Both rows commit or roll back together, and the event carries the feed row's
id (`change_event_id`), so every event maps to exactly one feed row (T-3-26).

Scheduling does NOT consume this event in Phase 3: the pull feed keeps serving it and the
`scheduling-schedule-change-poller` Schedule stays paused (decision of 2026-11-10).

The builder reads ids, the change type and a timestamp only. The event id is uuid5 of
`provider id : provider.schedule_changed : feed row id`, so one change maps to one id
and a second emission of the same change collides on the outbox primary key (T-3-25).
"""

from __future__ import annotations

import datetime
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.catalog import EventType
from libs.events.outbox import enqueue_event
from services.provider.repository.models import ScheduleChangeEvent

PROVIDER_EVENT_TYPES: tuple[str, ...] = (EventType.PROVIDER_SCHEDULE_CHANGED,)


async def emit_schedule_changed(db: AsyncSession, event: ScheduleChangeEvent) -> uuid.UUID:
    """Write the `provider.schedule_changed` outbox row for one feed row.

    `event.id` and `event.created_at` must already be loaded (after the flush).
    """
    changed_at: datetime.datetime = event.created_at
    return await enqueue_event(
        db,
        event_type=EventType.PROVIDER_SCHEDULE_CHANGED,
        aggregate_id=str(event.provider_id),
        payload={
            "provider_id": str(event.provider_id),
            "change_event_id": str(event.id),
            "change_type": str(event.change_type),
            "template_id": None if event.template_id is None else str(event.template_id),
            "changed_at": changed_at,
        },
        discriminator=str(event.id),
        occurred_at=changed_at,
    )
