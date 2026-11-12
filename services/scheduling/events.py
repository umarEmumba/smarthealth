"""Scheduling's domain events: payload builders and the outbox emit helper
(plan 03-07; EVENT-01, EVENT-02; D-06, D-07, D-10).

Each appointment transition that is a domain event calls `emit_appointment_event`
right after its `append_status_history`, inside the caller's transaction, so the
outbox row commits (or rolls back) with the state change and its history row.
The relay is Debezium reading the WAL (plan 03-05); nothing here talks to Kafka.

Payloads are PHI-free by construction (T-3-20): each builder copies ONLY ids and
timestamps from the row it is handed. `coverage_ref`, failure reasons, free text
and every patient attribute are never read here, so they cannot reach an event.

The event id is uuid5 of `appointment id : event type : post-transition version`
(`enqueue_event(discriminator=str(row.version))`). Every applied transition bumps
`appointments.version`, so a retried activity that finds the transition already
committed emits nothing, and a second emission of the same transition collides on
the outbox primary key (`ON CONFLICT (id) DO NOTHING`).

The accept path (`accept_booking`, the 202) emits nothing (D-04): `appointment.booked`
is emitted at the saga's confirm step, `appointment.failed` is not an event (D-07),
and history notes are not domain events.
"""

from __future__ import annotations

import datetime
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.outbox import enqueue_event

APPOINTMENT_BOOKED = "appointment.booked"
APPOINTMENT_CANCELLED = "appointment.cancelled"
APPOINTMENT_RESCHEDULED = "appointment.rescheduled"
APPOINTMENT_CHECKED_IN = "appointment.checked_in"
APPOINTMENT_COMPLETED = "appointment.completed"
APPOINTMENT_NO_SHOW = "appointment.no_show"

SCHEDULING_EVENT_TYPES: tuple[str, ...] = (
    APPOINTMENT_BOOKED,
    APPOINTMENT_CANCELLED,
    APPOINTMENT_RESCHEDULED,
    APPOINTMENT_CHECKED_IN,
    APPOINTMENT_COMPLETED,
    APPOINTMENT_NO_SHOW,
)

# Every transition's UPDATE ... RETURNING supplies: id, clinic_id, patient_id,
# provider_id, slot_id, slot_start, slot_end, version, updated_at (the builders'
# only inputs; no other appointment column is ever read into an event).


def _s(value: Any) -> str:
    return str(value)


def _base(row: Mapping[str, Any] | Any) -> dict[str, Any]:
    return {
        "appointment_id": _s(row.id),
        "clinic_id": _s(row.clinic_id),
        "patient_id": _s(row.patient_id),
        "provider_id": _s(row.provider_id),
    }


def booked_payload(row: Any) -> dict[str, Any]:
    return {
        **_base(row),
        "slot_id": _s(row.slot_id),
        "slot_start": row.slot_start,
        "slot_end": row.slot_end,
        "booked_at": row.updated_at,
    }


def cancelled_payload(row: Any) -> dict[str, Any]:
    return {
        **_base(row),
        "slot_id": _s(row.slot_id),
        "slot_start": row.slot_start,
        "cancelled_at": row.updated_at,
    }


def rescheduled_payload(
    row: Any, *, old_slot_id: Any, old_slot_start: datetime.datetime
) -> dict[str, Any]:
    """`row` is the appointment AFTER the swap (it points at the new slot)."""
    return {
        **_base(row),
        "old_slot_id": _s(old_slot_id),
        "new_slot_id": _s(row.slot_id),
        "old_slot_start": old_slot_start,
        "new_slot_start": row.slot_start,
        "new_slot_end": row.slot_end,
        "rescheduled_at": row.updated_at,
    }


def checked_in_payload(row: Any) -> dict[str, Any]:
    return {**_base(row), "slot_start": row.slot_start, "checked_in_at": row.updated_at}


def completed_payload(row: Any) -> dict[str, Any]:
    return {**_base(row), "slot_start": row.slot_start, "completed_at": row.updated_at}


def no_show_payload(row: Any) -> dict[str, Any]:
    return {**_base(row), "slot_start": row.slot_start, "marked_at": row.updated_at}


_BUILDERS: dict[str, Callable[..., dict[str, Any]]] = {
    APPOINTMENT_BOOKED: booked_payload,
    APPOINTMENT_CANCELLED: cancelled_payload,
    APPOINTMENT_RESCHEDULED: rescheduled_payload,
    APPOINTMENT_CHECKED_IN: checked_in_payload,
    APPOINTMENT_COMPLETED: completed_payload,
    APPOINTMENT_NO_SHOW: no_show_payload,
}


async def emit_appointment_event(
    db: AsyncSession, event_type: str, row: Any, **extra: Any
) -> None:
    """Write the outbox row for one applied transition, in the caller's transaction.

    `row` is the appointment's RETURNING row AFTER the update (its `version` is the
    new one). `extra` carries what only the transition knows (the reschedule's old
    slot). Never commits.
    """
    builder = _BUILDERS[event_type]
    await enqueue_event(
        db,
        event_type=event_type,
        aggregate_id=_s(row.id),
        payload=builder(row, **extra),
        discriminator=_s(row.version),
        occurred_at=row.updated_at,
    )
