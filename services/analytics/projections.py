"""Event projections: fold Kafka events into the analytics_db projection tables
(plan 03-10; ANLY-01..05, EVENT-03, D-10 revised, D-08 refined).

`apply_event` is called by `services.analytics.consumer.handle_event` inside ONE
transaction that has already claimed the event id, so a redelivered event never gets
here. Every handler is an upsert that CREATES the row if it is absent, and every
fact is first-write-wins (`COALESCE(existing, new)`), so the same set of events
produces the same numbers in any arrival order, including an event whose aggregate
the projection has never seen (a throw-away or pre-cutover id): it just creates a
row that the views exclude (no `booked_at`, no `registered_at`).

`appointments.slot_start` is the one value that can legitimately change
(`appointment.rescheduled`), so it is versioned: an event may replace it only with
a strictly newer `(occurred_at, slot_start)`, whatever order events arrive in.
`rescheduled_count` increments once per fresh claim, which is why it is safe here.

No decoded timestamp is used to dedupe (the Avro decode truncates to milliseconds):
dedupe is the claim on the event id.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.catalog import EventType
from libs.events.consumer import EventEnvelope
from services.analytics.repository.models import Appointment, Patient, VisitCompleted

__all__ = ["PROJECTED_EVENT_TYPES", "apply_event"]


def _uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _dt(value: Any) -> datetime:
    """A tz-aware datetime from a decoded Avro timestamp (datetime, ms epoch or ISO)."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _opt_dt(value: Any) -> datetime | None:
    return None if value is None else _dt(value)


# ---------------------------------------------------------------- patients


async def _patient_registered(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    stmt = pg_insert(Patient).values(
        patient_id=_uuid(p["patient_id"]),
        clinic_id=_uuid(p["clinic_id"]),
        registered_at=_dt(p["registered_at"]),
    )
    # Earliest registration wins (LEAST ignores NULL, COALESCE covers a NULL row).
    stmt = stmt.on_conflict_do_update(
        index_elements=[Patient.patient_id],
        set_={
            "registered_at": sa.func.coalesce(
                sa.func.least(Patient.registered_at, stmt.excluded.registered_at),
                stmt.excluded.registered_at,
            )
        },
    )
    await db.execute(stmt)


async def _patient_deactivated(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    stmt = pg_insert(Patient).values(
        patient_id=_uuid(p["patient_id"]),
        clinic_id=_uuid(p["clinic_id"]),
        deactivated_at=_dt(p["deactivated_at"]),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Patient.patient_id],
        set_={
            "deactivated_at": sa.func.coalesce(
                sa.func.least(Patient.deactivated_at, stmt.excluded.deactivated_at),
                stmt.excluded.deactivated_at,
            )
        },
    )
    await db.execute(stmt)


# ------------------------------------------------------------ appointments


async def _upsert_appointment(
    db: AsyncSession,
    envelope: EventEnvelope,
    *,
    slot_start: datetime,
    fact: str | None = None,
    fact_value: datetime | None = None,
    reschedule: bool = False,
) -> None:
    """Upsert one appointment row for an appointment event.

    `fact` names the first-write-wins timestamp column this event sets (booked_at,
    cancelled_at, ...). `slot_start` is the slot the event reports; it replaces the
    stored one only when this event's `(occurred_at, slot_start)` is strictly newer.
    """
    p = envelope.payload
    occurred_at = _dt(envelope.occurred_at)
    values: dict[str, Any] = {
        "appointment_id": _uuid(p["appointment_id"]),
        "clinic_id": _uuid(p["clinic_id"]),
        "patient_id": _uuid(p["patient_id"]) if p.get("patient_id") else None,
        "slot_start": slot_start,
        "last_event_at": occurred_at,
        "rescheduled_count": 1 if reschedule else 0,
    }
    if fact is not None:
        values[fact] = fact_value
    stmt = pg_insert(Appointment).values(**values)
    table = Appointment.__table__
    ex = stmt.excluded
    newer = sa.tuple_(ex.last_event_at, ex.slot_start) > sa.tuple_(
        table.c.last_event_at, table.c.slot_start
    )
    set_: dict[str, Any] = {
        "patient_id": sa.func.coalesce(table.c.patient_id, ex.patient_id),
        "slot_start": sa.case(
            (table.c.last_event_at.is_(None), ex.slot_start),
            (newer, ex.slot_start),
            else_=table.c.slot_start,
        ),
        "last_event_at": sa.func.greatest(table.c.last_event_at, ex.last_event_at),
        "rescheduled_count": table.c.rescheduled_count + ex.rescheduled_count,
    }
    if fact is not None:
        set_[fact] = sa.func.coalesce(table.c[fact], ex[fact])
    await db.execute(stmt.on_conflict_do_update(index_elements=[table.c.appointment_id], set_=set_))


async def _appointment_booked(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(
        db,
        envelope,
        slot_start=_dt(p["slot_start"]),
        fact="booked_at",
        fact_value=_dt(p["booked_at"]),
    )


async def _appointment_cancelled(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(
        db,
        envelope,
        slot_start=_dt(p["slot_start"]),
        fact="cancelled_at",
        fact_value=_dt(p["cancelled_at"]),
    )


async def _appointment_rescheduled(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(db, envelope, slot_start=_dt(p["new_slot_start"]), reschedule=True)


async def _appointment_checked_in(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(
        db,
        envelope,
        slot_start=_dt(p["slot_start"]),
        fact="checked_in_at",
        fact_value=_dt(p["checked_in_at"]),
    )


async def _appointment_completed(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(
        db,
        envelope,
        slot_start=_dt(p["slot_start"]),
        fact="completed_at",
        fact_value=_dt(p["completed_at"]),
    )


async def _appointment_no_show(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    await _upsert_appointment(
        db,
        envelope,
        slot_start=_dt(p["slot_start"]),
        fact="no_show_at",
        fact_value=_dt(p["marked_at"]),
    )


# ------------------------------------------------------------------ visits


async def _visit_completed(db: AsyncSession, envelope: EventEnvelope) -> None:
    p = envelope.payload
    stmt = pg_insert(VisitCompleted).values(
        visit_id=_uuid(p["visit_id"]),
        appointment_id=_uuid(p["appointment_id"]) if p.get("appointment_id") else None,
        clinic_id=_uuid(p["clinic_id"]),
        checked_in_at=_opt_dt(p.get("checked_in_at")),
        started_at=_opt_dt(p.get("started_at")),
        completed_at=_dt(p["completed_at"]),
    )
    ex = stmt.excluded
    table = VisitCompleted.__table__
    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c.visit_id],
        set_={
            "appointment_id": sa.func.coalesce(table.c.appointment_id, ex.appointment_id),
            "checked_in_at": sa.func.coalesce(table.c.checked_in_at, ex.checked_in_at),
            "started_at": sa.func.coalesce(table.c.started_at, ex.started_at),
            "completed_at": sa.func.coalesce(table.c.completed_at, ex.completed_at),
        },
    )
    await db.execute(stmt)


_HANDLERS = {
    EventType.PATIENT_REGISTERED: _patient_registered,
    EventType.PATIENT_DEACTIVATED: _patient_deactivated,
    EventType.APPOINTMENT_BOOKED: _appointment_booked,
    EventType.APPOINTMENT_CANCELLED: _appointment_cancelled,
    EventType.APPOINTMENT_RESCHEDULED: _appointment_rescheduled,
    EventType.APPOINTMENT_CHECKED_IN: _appointment_checked_in,
    EventType.APPOINTMENT_COMPLETED: _appointment_completed,
    EventType.APPOINTMENT_NO_SHOW: _appointment_no_show,
    EventType.VISIT_COMPLETED: _visit_completed,
}

# The event types this projection consumes (the consumer registers a handler for each).
PROJECTED_EVENT_TYPES: tuple[str, ...] = tuple(_HANDLERS)


async def apply_event(db: AsyncSession, envelope: EventEnvelope) -> None:
    """Fold one event into the projections on the caller's session (never commits).

    Unknown event types change nothing.
    """
    handler = _HANDLERS.get(envelope.event_type)
    if handler is not None:
        await handler(db, envelope)
