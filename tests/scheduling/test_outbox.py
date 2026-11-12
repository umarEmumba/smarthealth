"""Scheduling's outbox emission on live Postgres (plan 03-07; EVENT-01, EVENT-02;
D-04, D-06, D-07).

Every appointment transition that is a domain event (confirm, cancel,
reschedule, check-in, complete, no-show) writes its outbox row in the SAME
transaction as the state change and its history row; repeats, refusals,
rollbacks, the accept path and history notes write none; payloads carry ids and
timestamps only.

Most tests run inside the `scheduling_db_session` outer transaction, which is
always rolled back, so nothing they write is committed (and nothing is relayed to
Kafka by Debezium). Two tests commit on purpose to prove the cross-connection
behaviour (the rollback leaves nothing, the commit leaves the row and the history
together); the one committed outbox row is for a throwaway appointment in a
throwaway clinic, which is harmless downstream. Serialization uses the LIVE Schema
Registry (`localhost:8081`, subjects registered by `schema-init`).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from confluent_kafka.serialization import MessageField, SerializationContext
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from libs.events.catalog import EVENT_CATALOG, topic_for
from libs.events.outbox import event_id_for
from libs.events.registry import deserializer
from services.scheduling.domain.enums import AppointmentStatus
from services.scheduling.events import (
    APPOINTMENT_BOOKED,
    APPOINTMENT_CANCELLED,
    APPOINTMENT_CHECKED_IN,
    APPOINTMENT_COMPLETED,
    APPOINTMENT_NO_SHOW,
    APPOINTMENT_RESCHEDULED,
    SCHEDULING_EVENT_TYPES,
)
from services.scheduling.repository import appointment_repo
from tests.scheduling.conftest import cleanup_scheduling_rows

pytestmark = pytest.mark.integration

ACTOR = "test:outbox"
COVERAGE_REF = "COVERAGE-REF-SENTINEL-0042"
FLAG_REASON = "SENTINEL-schedule-change-reason"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@dataclass
class Seeded:
    appointment_id: uuid.UUID
    clinic_id: uuid.UUID
    provider_id: uuid.UUID
    patient_id: uuid.UUID
    slot_id: uuid.UUID
    start: datetime
    new_slot_id: uuid.UUID | None = None
    new_start: datetime | None = None


async def _insert_slot(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    clinic_id: uuid.UUID,
    start: datetime,
    status: str,
    held_by: uuid.UUID | None = None,
    minutes: int = 30,
) -> uuid.UUID:
    return await db.scalar(
        text(
            "INSERT INTO provider_slots (provider_id, clinic_id, department_id, start_time, "
            "end_time, status, held_by_appointment_id, held_until) "
            "VALUES (:p, :c, :d, :s, :e, :st, :h, :hu) RETURNING id"
        ),
        {
            "p": provider_id,
            "c": clinic_id,
            "d": uuid.uuid4(),
            "s": start,
            "e": start + timedelta(minutes=minutes),
            "st": status,
            "h": held_by,
            "hu": _now() + timedelta(minutes=10) if status == "held" else None,
        },
    )


async def seed_appointment(
    db: AsyncSession,
    *,
    status: str,
    version: int = 1,
    start: datetime | None = None,
    with_new_slot: bool = False,
    flagged: bool = False,
) -> Seeded:
    """One appointment in `status` on its own slot (a fresh clinic, provider and
    patient, so nothing collides), written through `db`."""
    clinic_id, provider_id, patient_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    appointment_id = uuid.uuid4()
    start = start or (_now() + timedelta(hours=2))
    # A pending appointment holds its slot (the saga confirms it later); every
    # other active state has the slot booked.
    slot_status = "held" if status == AppointmentStatus.PENDING else "booked"
    slot_id = await _insert_slot(
        db,
        provider_id=provider_id,
        clinic_id=clinic_id,
        start=start,
        status=slot_status,
        held_by=appointment_id,
    )
    await db.execute(
        text(
            "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
            "slot_start, slot_end, status, version, created_by, coverage_ref, "
            "schedule_change_flagged_at, schedule_change_reason) "
            "VALUES (:id, :c, :pt, :pr, :s, :st, :en, :status, :v, :by, :cov, :fl, :flr)"
        ),
        {
            "id": appointment_id,
            "c": clinic_id,
            "pt": patient_id,
            "pr": provider_id,
            "s": slot_id,
            "st": start,
            "en": start + timedelta(minutes=30),
            "status": status,
            "v": version,
            "by": uuid.uuid4(),
            "cov": COVERAGE_REF,
            "fl": _now() if flagged else None,
            "flr": FLAG_REASON if flagged else None,
        },
    )
    seeded = Seeded(appointment_id, clinic_id, provider_id, patient_id, slot_id, start)
    if with_new_slot:
        seeded.new_start = start + timedelta(hours=1)
        seeded.new_slot_id = await _insert_slot(
            db,
            provider_id=provider_id,
            clinic_id=clinic_id,
            start=seeded.new_start,
            status="held",
            held_by=appointment_id,
        )
    await db.flush()
    return seeded


async def outbox_rows(db: AsyncSession, appointment_id: uuid.UUID) -> list[Any]:
    result = await db.execute(
        text(
            "SELECT id, aggregatetype, aggregateid, type, payload, traceparent, schema_version "
            "FROM outbox WHERE aggregateid = :a ORDER BY occurred_at, type"
        ),
        {"a": str(appointment_id)},
    )
    return list(result.all())


async def history_count(db: AsyncSession, appointment_id: uuid.UUID) -> int:
    return await db.scalar(
        text("SELECT count(*) FROM appointment_status_history WHERE appointment_id = :a"),
        {"a": appointment_id},
    )


def decode(row: Any) -> dict[str, Any]:
    decoded = deserializer()(
        bytes(row.payload), SerializationContext(topic_for(row.type), MessageField.VALUE)
    )
    assert decoded is not None
    return decoded


# --- one scenario per transition --------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    event_type: str
    seed: Callable[[AsyncSession], Awaitable[Seeded]]
    run: Callable[[AsyncSession, Seeded], Awaitable[Any]]
    ok: Any  # the outcome that means "applied"
    seeded_version: int


async def _seed_pending(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="pending", version=0)


async def _seed_confirmed(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="confirmed")


async def _seed_confirmed_with_new_slot(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="confirmed", with_new_slot=True)


async def _seed_confirmed_near(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="confirmed", start=_now() + timedelta(minutes=10))


async def _seed_checked_in(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="checked_in", start=_now() + timedelta(minutes=10))


async def _seed_confirmed_past(db: AsyncSession) -> Seeded:
    return await seed_appointment(db, status="confirmed", start=_now() - timedelta(hours=3))


async def _run_confirm(db: AsyncSession, s: Seeded) -> bool:
    return await appointment_repo.confirm_booking(
        db, appointment_id=s.appointment_id, slot_id=s.slot_id, actor=ACTOR
    )


async def _run_cancel(db: AsyncSession, s: Seeded) -> str:
    return await appointment_repo.cancel_appointment(db, appointment_id=s.appointment_id, actor=ACTOR)


async def _run_swap(db: AsyncSession, s: Seeded) -> str:
    assert s.new_slot_id is not None
    return await appointment_repo.swap_slot(
        db,
        appointment_id=s.appointment_id,
        old_slot_id=s.slot_id,
        new_slot_id=s.new_slot_id,
        actor=ACTOR,
    )


async def _run_check_in(db: AsyncSession, s: Seeded) -> str:
    return await appointment_repo.check_in_appointment(
        db,
        appointment_id=s.appointment_id,
        actor=ACTOR,
        opens_before_minutes=60,
        closes_after_end_minutes=15,
    )


async def _run_complete(db: AsyncSession, s: Seeded) -> str:
    return await appointment_repo.complete_appointment(
        db, appointment_id=s.appointment_id, actor=ACTOR
    )


async def _run_no_show(db: AsyncSession, s: Seeded) -> str:
    return await appointment_repo.mark_no_show(
        db, appointment_id=s.appointment_id, actor=ACTOR, grace_minutes=15
    )


SCENARIOS = {
    APPOINTMENT_BOOKED: Scenario(APPOINTMENT_BOOKED, _seed_pending, _run_confirm, True, 0),
    APPOINTMENT_CANCELLED: Scenario(
        APPOINTMENT_CANCELLED, _seed_confirmed, _run_cancel, "cancelled", 1
    ),
    APPOINTMENT_RESCHEDULED: Scenario(
        APPOINTMENT_RESCHEDULED, _seed_confirmed_with_new_slot, _run_swap, "swapped", 1
    ),
    APPOINTMENT_CHECKED_IN: Scenario(
        APPOINTMENT_CHECKED_IN, _seed_confirmed_near, _run_check_in, "checked_in", 1
    ),
    APPOINTMENT_COMPLETED: Scenario(
        APPOINTMENT_COMPLETED, _seed_checked_in, _run_complete, "completed", 1
    ),
    APPOINTMENT_NO_SHOW: Scenario(
        APPOINTMENT_NO_SHOW, _seed_confirmed_past, _run_no_show, "no_show", 1
    ),
}
ALL_TYPES = list(SCENARIOS)


@pytest.fixture
def db(scheduling_db_session: AsyncSession) -> AsyncSession:
    return scheduling_db_session


def test_scenarios_cover_every_scheduling_event_type() -> None:
    assert set(SCENARIOS) == set(SCHEDULING_EVENT_TYPES)
    assert set(SCHEDULING_EVENT_TYPES) <= set(EVENT_CATALOG)


# --- one row per applied transition ------------------------------------------------


@pytest.mark.parametrize("event_type", ALL_TYPES)
async def test_transition_writes_exactly_one_outbox_row_with_the_new_version(
    db: AsyncSession, event_type: str
) -> None:
    scenario = SCENARIOS[event_type]
    seeded = await scenario.seed(db)
    history_before = await history_count(db, seeded.appointment_id)

    assert await scenario.run(db, seeded) == scenario.ok

    rows = await outbox_rows(db, seeded.appointment_id)
    assert [r.type for r in rows] == [event_type]
    row = rows[0]
    new_version = scenario.seeded_version + 1
    assert row.id == event_id_for(seeded.appointment_id, event_type, str(new_version))
    assert row.aggregatetype == "appointment"
    assert row.aggregateid == str(seeded.appointment_id)
    assert row.schema_version == 1
    assert await history_count(db, seeded.appointment_id) == history_before + 1
    version = await db.scalar(
        text("SELECT version FROM appointments WHERE id = :a"), {"a": seeded.appointment_id}
    )
    assert version == new_version

    value = decode(row)
    assert value["event_type"] == event_type
    assert str(value["event_id"]) == str(row.id)
    assert str(value["aggregate_id"]) == str(seeded.appointment_id)
    assert str(value["appointment_id"]) == str(seeded.appointment_id)
    assert str(value["clinic_id"]) == str(seeded.clinic_id)
    assert str(value["patient_id"]) == str(seeded.patient_id)
    assert str(value["provider_id"]) == str(seeded.provider_id)


async def test_booked_payload_has_the_slot_and_booking_time(db: AsyncSession) -> None:
    seeded = await _seed_pending(db)
    await _run_confirm(db, seeded)
    value = decode((await outbox_rows(db, seeded.appointment_id))[0])
    assert str(value["slot_id"]) == str(seeded.slot_id)
    assert value["slot_start"] == seeded.start
    assert value["slot_end"] == seeded.start + timedelta(minutes=30)
    assert value["booked_at"] is not None


async def test_cancelled_payload_has_slot_start_and_cancellation_time(db: AsyncSession) -> None:
    seeded = await _seed_confirmed(db)
    await _run_cancel(db, seeded)
    value = decode((await outbox_rows(db, seeded.appointment_id))[0])
    assert str(value["slot_id"]) == str(seeded.slot_id)
    assert value["slot_start"] == seeded.start
    assert value["cancelled_at"] is not None


async def test_rescheduled_payload_has_old_and_new_slot_ids_and_starts(db: AsyncSession) -> None:
    seeded = await _seed_confirmed_with_new_slot(db)
    await _run_swap(db, seeded)
    value = decode((await outbox_rows(db, seeded.appointment_id))[0])
    assert str(value["old_slot_id"]) == str(seeded.slot_id)
    assert str(value["new_slot_id"]) == str(seeded.new_slot_id)
    assert value["old_slot_start"] == seeded.start
    assert value["new_slot_start"] == seeded.new_start
    assert value["new_slot_end"] == seeded.new_start + timedelta(minutes=30)
    assert value["rescheduled_at"] is not None


async def test_checked_in_payload_has_slot_start_and_check_in_time(db: AsyncSession) -> None:
    seeded = await _seed_confirmed_near(db)
    await _run_check_in(db, seeded)
    value = decode((await outbox_rows(db, seeded.appointment_id))[0])
    assert value["slot_start"] == seeded.start
    assert value["checked_in_at"] is not None


async def test_completed_and_no_show_payloads_have_slot_start_and_their_timestamps(
    db: AsyncSession,
) -> None:
    done = await _seed_checked_in(db)
    await _run_complete(db, done)
    value = decode((await outbox_rows(db, done.appointment_id))[0])
    assert value["slot_start"] == done.start and value["completed_at"] is not None

    missed = await _seed_confirmed_past(db)
    await _run_no_show(db, missed)
    value = decode((await outbox_rows(db, missed.appointment_id))[0])
    assert value["slot_start"] == missed.start and value["marked_at"] is not None


# --- repeats, refusals and non-events write nothing ---------------------------------


@pytest.mark.parametrize("event_type", ALL_TYPES)
async def test_repeated_transition_adds_no_second_row(db: AsyncSession, event_type: str) -> None:
    """A retried activity or a duplicate call finds the transition already applied:
    it returns its repeat outcome and writes neither history nor outbox rows."""
    scenario = SCENARIOS[event_type]
    seeded = await scenario.seed(db)
    assert await scenario.run(db, seeded) == scenario.ok
    ids_before = [r.id for r in await outbox_rows(db, seeded.appointment_id)]
    history_before = await history_count(db, seeded.appointment_id)

    second = await scenario.run(db, seeded)

    assert second != "rejected"  # the repeat is "already" (confirm: True again), not a refusal
    assert [r.id for r in await outbox_rows(db, seeded.appointment_id)] == ids_before
    assert len(ids_before) == 1
    assert await history_count(db, seeded.appointment_id) == history_before


async def test_refused_transitions_emit_nothing(db: AsyncSession) -> None:
    pending = await seed_appointment(db, status="pending", version=0)
    assert await _run_cancel(db, pending) == "not_cancellable"  # pending cannot be cancelled
    assert await _run_complete(db, pending) == "not_checked_in"
    assert await _run_no_show(db, pending) == "not_confirmed"

    early = await seed_appointment(db, status="confirmed", start=_now() + timedelta(hours=5))
    assert await _run_check_in(db, early) == "too_early"
    assert await _run_no_show(db, early) == "too_early"

    stale = await seed_appointment(db, status="confirmed")  # slot not held by anyone new
    assert await appointment_repo.swap_slot(
        db,
        appointment_id=stale.appointment_id,
        old_slot_id=stale.slot_id,
        new_slot_id=uuid.uuid4(),  # a slot it does not hold
        actor=ACTOR,
    ) == "rejected"

    missing = uuid.uuid4()
    assert await appointment_repo.cancel_appointment(db, appointment_id=missing, actor=ACTOR) == (
        "not_cancellable"
    )
    for seeded in (pending, early, stale):
        assert await outbox_rows(db, seeded.appointment_id) == []
    assert await outbox_rows(db, missing) == []


async def test_confirm_of_a_lost_hold_emits_nothing(db: AsyncSession) -> None:
    seeded = await seed_appointment(db, status="pending", version=0)
    await db.execute(
        text("UPDATE provider_slots SET status = 'available', held_by_appointment_id = NULL "
             "WHERE id = :s"),
        {"s": seeded.slot_id},
    )
    assert await _run_confirm(db, seeded) is False
    assert await outbox_rows(db, seeded.appointment_id) == []


async def test_accept_booking_writes_no_outbox_row(db: AsyncSession) -> None:
    """D-04: the 202 accept path gains nothing; appointment.booked comes at confirm."""
    clinic_id, provider_id, patient_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    appointment_id = uuid.uuid4()
    start = _now() + timedelta(hours=3)
    slot_id = await _insert_slot(
        db, provider_id=provider_id, clinic_id=clinic_id, start=start, status="held",
        held_by=appointment_id,
    )

    result = await appointment_repo.accept_booking(
        db,
        appointment_id=appointment_id,
        caller_user_id=uuid.uuid4(),
        key=f"outbox-test-{uuid.uuid4().hex}",
        request_hash="h" * 64,
        clinic_id=clinic_id,
        patient_id=patient_id,
        provider_id=provider_id,
        slot_id=slot_id,
        slot_start=start,
        slot_end=start + timedelta(minutes=30),
        coverage_ref=COVERAGE_REF,
        created_by=uuid.uuid4(),
    )
    assert result.appointment_id == appointment_id and not result.replayed
    assert await outbox_rows(db, appointment_id) == []
    # And the confirm that follows is what emits it.
    assert await appointment_repo.confirm_booking(
        db, appointment_id=appointment_id, slot_id=slot_id, actor=ACTOR
    )
    assert [r.type for r in await outbox_rows(db, appointment_id)] == [APPOINTMENT_BOOKED]


async def test_history_notes_flag_and_failure_are_not_domain_events(db: AsyncSession) -> None:
    confirmed = await seed_appointment(db, status="confirmed")
    assert await appointment_repo.record_history_note(
        db, appointment_id=confirmed.appointment_id, reason="reschedule_failed:SlotUnavailable",
        actor="wf-1", bump_version=True,
    )
    assert await appointment_repo.flag_schedule_change(
        db, appointment_id=confirmed.appointment_id, reason="removed"
    )
    assert await outbox_rows(db, confirmed.appointment_id) == []

    pending = await seed_appointment(db, status="pending", version=0)
    assert await appointment_repo.mark_appointment_failed(
        db, appointment_id=pending.appointment_id, reason="BillingDenied", actor=ACTOR
    )
    assert await outbox_rows(db, pending.appointment_id) == []


# --- PHI-free payloads (T-3-20) -------------------------------------------------------


@pytest.mark.parametrize("event_type", ALL_TYPES)
async def test_payload_bytes_never_carry_coverage_ref_or_free_text(
    db: AsyncSession, event_type: str
) -> None:
    scenario = SCENARIOS[event_type]
    seeded = await scenario.seed(db)
    await scenario.run(db, seeded)
    (row,) = await outbox_rows(db, seeded.appointment_id)

    raw = bytes(row.payload)
    for forbidden in (COVERAGE_REF, FLAG_REASON, "coverage_ref", "failure_reason", "full_name",
                      "date_of_birth", "phone", "email"):
        assert forbidden.encode() not in raw, forbidden

    value = decode(row)
    assert "coverage_ref" not in value
    # Only ids, the event type name and timestamps: every non-timestamp value is a UUID,
    # a small int (schema version) or the event type.
    for name, item in value.items():
        if isinstance(item, datetime):
            continue
        if name in {"event_type"}:
            assert item == event_type
        elif name == "schema_version":
            assert item == 1
        else:
            uuid.UUID(str(item))  # raises for anything that is not an id


async def test_a_flagged_appointment_reschedule_still_omits_the_reason(db: AsyncSession) -> None:
    seeded = await seed_appointment(db, status="confirmed", with_new_slot=True, flagged=True)
    assert await _run_swap(db, seeded) == "swapped"
    (row,) = await outbox_rows(db, seeded.appointment_id)
    assert FLAG_REASON.encode() not in bytes(row.payload)


# --- trace context (OBS-02) -----------------------------------------------------------


async def test_outbox_row_carries_the_active_spans_traceparent(db: AsyncSession) -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-outbox")
    seeded = await _seed_confirmed(db)
    with tracer.start_as_current_span("cancel-request") as span:
        await _run_cancel(db, seeded)
        trace_id = format(span.get_span_context().trace_id, "032x")
    (row,) = await outbox_rows(db, seeded.appointment_id)
    assert row.traceparent is not None
    assert row.traceparent.split("-")[1] == trace_id


async def test_outbox_row_has_no_traceparent_without_an_active_span(db: AsyncSession) -> None:
    seeded = await _seed_confirmed(db)
    await _run_cancel(db, seeded)
    (row,) = await outbox_rows(db, seeded.appointment_id)
    assert row.traceparent is None


# --- atomicity across connections ------------------------------------------------------


async def _commit_seed(engine: AsyncEngine, **kwargs: Any) -> Seeded:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        seeded = await seed_appointment(session, **kwargs)
        await session.commit()
    return seeded


async def _count_from_other_connection(
    engine: AsyncEngine, appointment_id: uuid.UUID
) -> tuple[int, int, str]:
    async with engine.connect() as conn:
        outbox = await conn.scalar(
            text("SELECT count(*) FROM outbox WHERE aggregateid = :a"), {"a": str(appointment_id)}
        )
        history = await conn.scalar(
            text("SELECT count(*) FROM appointment_status_history WHERE appointment_id = :a"),
            {"a": appointment_id},
        )
        status = await conn.scalar(
            text("SELECT status FROM appointments WHERE id = :a"), {"a": appointment_id}
        )
    return outbox, history, status


@pytest_asyncio.fixture
async def committed_cleanup(committed_scheduling_engine: AsyncEngine) -> AsyncIterator[list[uuid.UUID]]:
    providers: list[uuid.UUID] = []
    yield providers
    await cleanup_scheduling_rows(committed_scheduling_engine, providers)


async def test_rolled_back_transition_leaves_no_outbox_row_and_no_history_row(
    committed_scheduling_engine: AsyncEngine, committed_cleanup: list[uuid.UUID]
) -> None:
    engine = committed_scheduling_engine
    seeded = await _commit_seed(engine, status="confirmed")
    committed_cleanup.append(seeded.provider_id)
    history_before = (await _count_from_other_connection(engine, seeded.appointment_id))[1]

    async with AsyncSession(engine, expire_on_commit=False) as session:
        assert await _run_cancel(session, seeded) == "cancelled"
        # Visible inside the transaction ...
        assert len(await outbox_rows(session, seeded.appointment_id)) == 1
        # ... and invisible to another connection until it commits.
        assert (await _count_from_other_connection(engine, seeded.appointment_id))[0] == 0
        await session.rollback()

    outbox, history, status = await _count_from_other_connection(engine, seeded.appointment_id)
    assert (outbox, history, status) == (0, history_before, "confirmed")


async def test_committed_transition_publishes_row_history_and_state_together(
    committed_scheduling_engine: AsyncEngine, committed_cleanup: list[uuid.UUID]
) -> None:
    engine = committed_scheduling_engine
    seeded = await _commit_seed(engine, status="confirmed")
    committed_cleanup.append(seeded.provider_id)
    history_before = (await _count_from_other_connection(engine, seeded.appointment_id))[1]

    async with AsyncSession(engine, expire_on_commit=False) as session:
        assert await _run_cancel(session, seeded) == "cancelled"
        await session.commit()

    outbox, history, status = await _count_from_other_connection(engine, seeded.appointment_id)
    assert (outbox, history, status) == (1, history_before + 1, "cancelled")
