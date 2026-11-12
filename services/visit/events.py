"""Visit's domain event: the `visit.completed` payload builder and outbox emit helper
(plan 03-09; EVENT-01, EVENT-02, ANLY-03, ANLY-05; D-03, D-06, D-07, D-10 revised).

`visit.completed` is written to visit_db's outbox from `transition_visit`, in the same
transaction as the COMPLETED status change and its history row, and only when that
UPDATE actually applied (a repeat completion, a skipped state or a rollback writes
nothing). Debezium relays the committed row to `smarthealth.visit.events` keyed by the
visit id (plan 03-09 registers the `visit-outbox` connector); nothing here talks to
Kafka. The visit-downstream consumer turns the event into the existing
`VisitCompletedWorkflow` start, and the analytics projection reads the three timestamps
for completed visits and wait time (check-in to visit start, D-10 revised).

The builder reads ids and timestamps only: no notes, diagnoses or free text exist on the
visit row, and the Avro schema (plan 03-03) has no such fields either, so a payload
carrying one is rejected at serialization (T-3-29).

The event id is uuid5 of `visit id : visit.completed : completed`: one completion, one id.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.catalog import EventType
from libs.events.outbox import enqueue_event

VISIT_EVENT_TYPES: tuple[str, ...] = (EventType.VISIT_COMPLETED,)

COMPLETED_DISCRIMINATOR = "completed"


async def emit_visit_completed(db: AsyncSession, row: Any) -> uuid.UUID:
    """Write the `visit.completed` outbox row for a just-completed visit.

    `row` is the `RETURNING` row of the COMPLETED update: `id`, `appointment_id`,
    `clinic_id`, `patient_id`, `provider_id`, `checked_in_at`, `started_at` and
    `completed_at`. All three timestamps are set by the transition path.
    """
    completed_at: datetime.datetime = row.completed_at
    started_at: datetime.datetime | None = row.started_at
    return await enqueue_event(
        db,
        event_type=EventType.VISIT_COMPLETED,
        aggregate_id=str(row.id),
        payload={
            "visit_id": str(row.id),
            "appointment_id": str(row.appointment_id),
            "clinic_id": str(row.clinic_id),
            "patient_id": str(row.patient_id),
            "provider_id": str(row.provider_id),
            "checked_in_at": row.checked_in_at,
            "started_at": started_at,
            "completed_at": completed_at,
        },
        discriminator=COMPLETED_DISCRIMINATOR,
        occurred_at=completed_at,
    )
