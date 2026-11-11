"""Projection tests for the analytics-projection consumer (plan 03-10).

`test_tracer_*`: a patient event produced to live Kafka (a unique topic and group per
test, so nothing reaches the real topics) is consumed by the real `EventConsumer`
running the real `handle_event`, lands in the live analytics_db and shows in
`v_active_patients` for a unique clinic.
"""

from __future__ import annotations

import dataclasses
import datetime
import random
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from libs.events.consumer import EventEnvelope, NonRetryableEventError
from services.analytics.config import AnalyticsSettings
from services.analytics.consumer import handle_event
from services.analytics.repository import AnalyticsBase
from tests.analytics.conftest import Projector, _run_sql, query, read, running_consumer, wait_for

pytestmark = pytest.mark.integration

T0 = datetime.datetime(2026, 11, 11, 9, 0, tzinfo=datetime.UTC)


def _active(clinic: uuid.UUID) -> int:
    rows = query(
        "SELECT active_patients FROM v_active_patients WHERE clinic_id = :c", c=clinic
    )
    return int(rows[0][0]) if rows else 0


def _registered(clinic: uuid.UUID, patient: uuid.UUID) -> dict[str, Any]:
    return {
        "aggregate_id": str(patient),
        "patient_id": str(patient),
        "clinic_id": str(clinic),
        "registered_at": T0,
    }


def _deactivated(clinic: uuid.UUID, patient: uuid.UUID) -> dict[str, Any]:
    return {
        "aggregate_id": str(patient),
        "patient_id": str(patient),
        "clinic_id": str(clinic),
        "deactivated_at": T0 + datetime.timedelta(hours=1),
    }


def test_tracer_registered_events_reach_v_active_patients(
    clinic_id: uuid.UUID,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    claimed_event_ids: list[str],
) -> None:
    patients = [uuid.uuid4(), uuid.uuid4()]
    with running_consumer(unique_topic, unique_group, dlq_topic):
        for patient in patients:
            record = produce_event(
                unique_topic,
                "patient.registered",
                _registered(clinic_id, patient),
                occurred_at=T0,
            )
            claimed_event_ids.append(record["event_id"])
        assert wait_for(lambda: _active(clinic_id), 2) == 2


def test_tracer_deactivation_applied_before_registration_still_lowers_the_count(
    clinic_id: uuid.UUID,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    claimed_event_ids: list[str],
) -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    with running_consumer(unique_topic, unique_group, dlq_topic):
        # The deactivation of `first` is on the topic BEFORE its registration.
        for event_type, payload in (
            ("patient.deactivated", _deactivated(clinic_id, first)),
            ("patient.registered", _registered(clinic_id, first)),
            ("patient.registered", _registered(clinic_id, second)),
        ):
            record = produce_event(unique_topic, event_type, payload, occurred_at=T0)
            claimed_event_ids.append(record["event_id"])
        assert wait_for(lambda: _active(clinic_id), 1) == 1
        # The registration of `first` did not resurrect it: still one after a pause.
        assert wait_for(lambda: _active(clinic_id), 1, timeout=2) == 1


def test_tracer_settings_hold_exactly_one_database_url() -> None:
    fields = {
        name: field
        for name, field in AnalyticsSettings.model_fields.items()
        if "database" in name.lower() or "database" in str(field.validation_alias).lower()
    }
    assert set(fields) == {"database_url"}
    assert fields["database_url"].validation_alias == "ANALYTICS_DATABASE_URL"


# ---------------------------------------------------------------------------
# Metric math, order tolerance, idempotency and isolation (handler driven directly:
# the tracer above already proved the Kafka path).
# ---------------------------------------------------------------------------

DAY1 = datetime.datetime(2026, 11, 10, 23, 30, tzinfo=datetime.UTC)  # UTC day 10
DAY2 = datetime.datetime(2026, 11, 11, 0, 30, tzinfo=datetime.UTC)  # UTC day 11 (an hour later)
MIN = datetime.timedelta(minutes=1)


def fixed_sequence(p: Projector) -> list[EventEnvelope]:
    """3 patients (1 deactivated); 4 appointments booked on two UTC days (1 cancelled,
    1 rescheduled, 2 checked in, 2 completed); 3 visit.completed events (waits of 300 s
    and 900 s, and one with no start). Event timestamps follow causality."""
    pats = [uuid.uuid4() for _ in range(3)]
    apps = [uuid.uuid4() for _ in range(4)]
    visits = [uuid.uuid4() for _ in range(3)]
    slot = datetime.datetime(2026, 11, 20, 10, 0, tzinfo=datetime.UTC)
    booked = [DAY1, DAY1 + MIN, DAY2, DAY2 + MIN]
    ev: list[EventEnvelope] = []
    for pat in pats:
        ev.append(p.make("patient.registered", pat, occurred_at=T0, registered_at=T0))
    ev.append(
        p.make(
            "patient.deactivated",
            pats[2],
            occurred_at=T0 + 5 * MIN,
            deactivated_at=T0 + 5 * MIN,
        )
    )
    for app, when in zip(apps, booked, strict=True):
        ev.append(
            p.make(
                "appointment.booked",
                app,
                occurred_at=when,
                booked_at=when,
                slot_start=slot,
            )
        )
    done = DAY2 + 10 * MIN
    ev.append(
        p.make(
            "appointment.cancelled",
            apps[3],
            occurred_at=done,
            cancelled_at=done,
            slot_start=slot,
        )
    )
    ev.append(
        p.make(
            "appointment.rescheduled",
            apps[1],
            occurred_at=done,
            new_slot_start=slot + datetime.timedelta(days=1),
            rescheduled_at=done,
        )
    )
    for app in apps[:2]:
        ev.append(
            p.make(
                "appointment.checked_in",
                app,
                occurred_at=done + MIN,
                checked_in_at=done + MIN,
                slot_start=slot,
            )
        )
    for app in (apps[0], apps[2]):
        ev.append(
            p.make(
                "appointment.completed",
                app,
                occurred_at=done + 2 * MIN,
                completed_at=done + 2 * MIN,
                slot_start=slot,
            )
        )
    waits = (300, 900, None)
    for visit, app, wait in zip(visits, apps, waits, strict=False):
        checked_in = done + MIN
        ev.append(
            p.make(
                "visit.completed",
                visit,
                appointment_id=app,
                occurred_at=done + 3 * MIN,
                checked_in_at=checked_in,
                started_at=None if wait is None else checked_in + datetime.timedelta(seconds=wait),
                completed_at=done + 3 * MIN,
            )
        )
    return ev


async def metrics(clinic: uuid.UUID) -> dict[str, Any]:
    """Every metric for one clinic, read through the views only."""
    c = {"c": clinic}
    active = await read("SELECT active_patients FROM v_active_patients WHERE clinic_id = :c", **c)
    booked = await read(
        "SELECT day, appointments_booked FROM v_appointments_booked_daily "
        "WHERE clinic_id = :c ORDER BY day",
        **c,
    )
    visits = await read(
        "SELECT day, completed_visits FROM v_completed_visits_daily "
        "WHERE clinic_id = :c ORDER BY day",
        **c,
    )
    cancel = await read(
        "SELECT coalesce(sum(booked), 0), coalesce(sum(cancelled), 0) FROM v_cancellation "
        "WHERE clinic_id = :c",
        **c,
    )
    wait = await read(
        "SELECT avg(wait_seconds), count(*) FROM v_visit_wait WHERE clinic_id = :c", **c
    )
    n_booked, n_cancelled = cancel[0]
    return {
        "active_patients": active[0][0] if active else 0,
        "booked_per_day": [(d.isoformat(), n) for d, n in booked],
        "completed_visits": sum(n for _, n in visits),
        "cancellation_rate": (n_cancelled / n_booked) if n_booked else None,
        "avg_wait_seconds": wait[0][0],
        "wait_samples": wait[0][1],
    }


EXPECTED = {
    "active_patients": 2,
    "booked_per_day": [("2026-11-10", 2), ("2026-11-11", 2)],
    "completed_visits": 3,
    "cancellation_rate": 0.25,
    "avg_wait_seconds": 600.0,
    "wait_samples": 2,
}


async def test_fixed_sequence_gives_the_five_metrics(projector: Projector) -> None:
    await projector.apply(*fixed_sequence(projector))
    assert await metrics(projector.clinic_id) == EXPECTED
    # Total booked is 4 and the day buckets are UTC days (23:30 and 00:30 split).
    assert sum(n for _, n in EXPECTED["booked_per_day"]) == 4


async def test_event_order_across_aggregates_does_not_change_the_numbers(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    # Reversed puts every cancel/complete/visit event before its booking.
    await projector.apply(*reversed(fixed_sequence(projector)))
    assert await metrics(clinic_id) == EXPECTED

    # A seeded shuffle for a second clinic (fresh aggregate ids: ids are global keys).
    other = Projector(uuid.uuid4())
    shuffled = fixed_sequence(other)
    random.Random(11).shuffle(shuffled)
    try:
        await other.apply(*shuffled)
        assert await metrics(other.clinic_id) == EXPECTED
    finally:
        await _drop_clinic(other)


async def _drop_clinic(p: Projector) -> None:
    for table in ("patients", "appointments", "visits_completed"):
        await _run_sql(f"DELETE FROM {table} WHERE clinic_id = :c", {"c": p.clinic_id}, write=True)
    await _run_sql(
        "DELETE FROM processed_events WHERE event_id = ANY(:ids)", {"ids": p.event_ids}, write=True
    )


async def test_applying_every_event_twice_changes_nothing(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    sequence = fixed_sequence(projector)
    await projector.apply(*sequence)
    await projector.apply(*sequence)  # the same event ids again (redelivery)
    assert await metrics(clinic_id) == EXPECTED
    rows = await read(
        "SELECT rescheduled_count FROM appointments WHERE clinic_id = :c AND rescheduled_count > 0",
        c=clinic_id,
    )
    assert rows == [(1,)]  # a redelivered reschedule did not count twice
    claims = await read(
        "SELECT count(*) FROM processed_events WHERE event_id = ANY(:ids)",
        ids=[e.event_id for e in sequence],
    )
    assert claims[0][0] == len(sequence)


async def test_cancel_before_booked_is_counted_once_booked_arrives(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    app = uuid.uuid4()
    slot = datetime.datetime(2026, 11, 20, 10, 0, tzinfo=datetime.UTC)
    cancel = projector.make(
        "appointment.cancelled", app, occurred_at=T0 + 5 * MIN, cancelled_at=T0 + 5 * MIN, slot_start=slot
    )
    book = projector.make("appointment.booked", app, occurred_at=T0, booked_at=T0, slot_start=slot)
    await projector.apply(cancel)
    # Cancelled but never seen booked (pre-cutover or not yet delivered): not in the window.
    assert (await metrics(clinic_id))["cancellation_rate"] is None
    await projector.apply(book)
    after = await metrics(clinic_id)
    assert after["cancellation_rate"] == 1.0
    assert after["booked_per_day"] == [("2026-11-11", 1)]


async def test_a_late_reschedule_never_replaces_a_newer_slot(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    app = uuid.uuid4()
    s0, s1, s2 = (
        datetime.datetime(2026, 11, 20, 10, 0, tzinfo=datetime.UTC) + datetime.timedelta(days=n)
        for n in range(3)
    )
    book = projector.make("appointment.booked", app, occurred_at=T0, booked_at=T0, slot_start=s0)
    first = projector.make(
        "appointment.rescheduled", app, occurred_at=T0 + MIN, new_slot_start=s1, rescheduled_at=T0 + MIN
    )
    second = projector.make(
        "appointment.rescheduled", app, occurred_at=T0 + 2 * MIN, new_slot_start=s2, rescheduled_at=T0 + 2 * MIN
    )
    await projector.apply(second, first, book)  # newest first, booking last
    rows = await read(
        "SELECT slot_start, rescheduled_count, booked_at FROM appointments WHERE clinic_id = :c",
        c=clinic_id,
    )
    assert rows == [(s2, 2, T0)]


async def test_another_clinics_events_never_change_this_clinics_numbers(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    await projector.apply(*fixed_sequence(projector))
    before = await metrics(clinic_id)
    other = Projector(uuid.uuid4())
    try:
        await other.apply(*fixed_sequence(other))
        await other.apply(
            other.make("patient.registered", uuid.uuid4(), registered_at=T0),
            other.make(
                "visit.completed",
                uuid.uuid4(),
                checked_in_at=T0,
                started_at=T0 + 100 * MIN,
                completed_at=T0 + 101 * MIN,
            ),
        )
        assert await metrics(clinic_id) == before == EXPECTED
        assert (await metrics(other.clinic_id))["active_patients"] == 3
    finally:
        await _drop_clinic(other)


async def test_unknown_ids_and_event_types_are_absorbed_without_error(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    ghost_app, ghost_pat = uuid.uuid4(), uuid.uuid4()
    slot = datetime.datetime(2026, 11, 20, 10, 0, tzinfo=datetime.UTC)
    unknown_type = dataclasses.replace(
        projector.make("patient.registered", uuid.uuid4(), registered_at=T0),
        event_type="provider.schedule_changed",
    )
    await projector.apply(
        projector.make("appointment.completed", ghost_app, completed_at=T0, slot_start=slot),
        projector.make("patient.deactivated", ghost_pat, deactivated_at=T0),
        projector.make("visit.completed", uuid.uuid4(), checked_in_at=T0, started_at=None, completed_at=T0),
        unknown_type,  # an event type this projection does not consume: no effect, no error
    )
    m = await metrics(clinic_id)
    # A completion for an appointment never seen booked is not a booking; a deactivation
    # for a never-registered patient is not an active patient; the visit still counts.
    assert m["booked_per_day"] == [] and m["active_patients"] == 0
    assert m["completed_visits"] == 1 and m["avg_wait_seconds"] is None


async def test_negative_or_missing_wait_is_excluded_from_the_wait_only(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    await projector.apply(
        projector.make(
            "visit.completed", uuid.uuid4(), checked_in_at=T0, started_at=T0 + 2 * MIN, completed_at=T0 + 9 * MIN
        ),
        projector.make(
            "visit.completed", uuid.uuid4(), checked_in_at=T0, started_at=T0 - MIN, completed_at=T0 + 9 * MIN
        ),
        projector.make(
            "visit.completed", uuid.uuid4(), checked_in_at=T0, started_at=None, completed_at=T0 + 9 * MIN
        ),
    )
    m = await metrics(clinic_id)
    assert m["completed_visits"] == 3
    assert m["wait_samples"] == 1 and m["avg_wait_seconds"] == 120.0


async def test_a_failing_effect_rolls_the_claim_back_so_the_retry_applies_it(
    projector: Projector, clinic_id: uuid.UUID
) -> None:
    pat = uuid.uuid4()
    good = projector.make("patient.registered", pat, registered_at=T0)
    bad_payload = {k: v for k, v in good.payload.items() if k != "registered_at"}
    bad = dataclasses.replace(good, payload=bad_payload)
    with pytest.raises(NonRetryableEventError):
        await handle_event(bad)
    claims = await read("SELECT count(*) FROM processed_events WHERE event_id = :e", e=good.event_id)
    assert claims[0][0] == 0  # no claim survived the failed effect
    await projector.apply(good)  # the same event id, now well-formed
    assert (await metrics(clinic_id))["active_patients"] == 1


async def test_orm_metadata_mirrors_exactly_the_three_projection_tables() -> None:
    assert set(AnalyticsBase.metadata.tables) == {"patients", "appointments", "visits_completed"}
    for name, table in AnalyticsBase.metadata.tables.items():
        rows = await read(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t",
            t=name,
        )
        assert {r[0] for r in rows} == {c.name for c in table.columns}, name


async def test_every_metric_view_carries_its_definition_as_a_comment() -> None:
    rows = await read(
        "SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind = 'v'"
    )
    comments = dict(rows)
    assert set(comments) == {
        "v_active_patients",
        "v_appointments_booked_daily",
        "v_completed_visits_daily",
        "v_cancellation",
        "v_visit_wait",
    }
    assert all(text and len(text) > 40 for text in comments.values())
    assert "check-in" in comments["v_visit_wait"] and "booked in the window" in comments["v_cancellation"]
