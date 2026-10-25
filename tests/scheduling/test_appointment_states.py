"""The appointment state machine on live Postgres (plan 02.1-01; mentor item M1).

`checked_in`, `completed` and `no_show` exist in Scheduling, are reached only
through Scheduling's own conditional transitions (Visit drives them over
`/internal/appointments/{id}/check-in` and `/complete` with a service token),
and keep their slot at the database level. Once an appointment is checked in
or finished, cancel and reschedule are refused at the API and at the
repository, so the slot is never rebooked and no refund is issued.

Appointments are seeded with plain SQL on slots of throwaway provider ids (so
slot times near "now" never collide with the world's generated slots); rows
are removed by `cleanup_scheduling_rows` scoped to those provider ids.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from services.scheduling.domain.enums import AppointmentStatus
from services.scheduling.repository import appointment_repo
from tests.conftest import service_base_url
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import auth
from tests.visit.conftest import VisitWorld, _visit_world, audit_count, confirmed_appointment

pytestmark = pytest.mark.integration

ACTOR = "service:visit"


# --- helpers -----------------------------------------------------------------------


@dataclass(frozen=True)
class Seeded:
    appointment_id: uuid.UUID
    slot_id: uuid.UUID
    clinic_id: uuid.UUID
    provider_id: uuid.UUID
    patient_id: uuid.UUID
    start: datetime


Seeder = Callable[..., Awaitable[Seeded]]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@pytest_asyncio.fixture
async def seed(committed_scheduling_engine: AsyncEngine) -> AsyncIterator[Seeder]:
    """Factory: insert and COMMIT an appointment (and its slot) in any status.

    Each appointment gets its own throwaway provider and patient unless given,
    so seeded rows never collide with each other or with generated slots.
    Everything created is removed at teardown (test-data hygiene only).
    """
    engine = committed_scheduling_engine
    providers: list[uuid.UUID] = []

    async def _seed(
        *,
        status: str = "confirmed",
        start: datetime | None = None,
        minutes: int = 30,
        clinic_id: uuid.UUID | None = None,
        provider_id: uuid.UUID | None = None,
        patient_id: uuid.UUID | None = None,
        slot_status: str | None = None,
    ) -> Seeded:
        start = start or (_now() + timedelta(minutes=20))
        clinic_id = clinic_id or uuid.uuid4()
        provider_id = provider_id or uuid.uuid4()
        patient_id = patient_id or uuid.uuid4()
        providers.append(provider_id)
        holds = status in AppointmentStatus.ACTIVE
        slot_id = await seed_slot(
            engine,
            provider_id=provider_id,
            start=start,
            minutes=minutes,
            clinic_id=clinic_id,
            department_id=uuid.uuid4(),
            status=slot_status or ("booked" if holds else "available"),
        )
        appointment_id = uuid.uuid4()
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
                    "slot_start, slot_end, status, version, created_by) "
                    "VALUES (:id, :c, :pt, :pr, :s, :st, :en, :status, 1, :by)"
                ),
                {
                    "id": appointment_id,
                    "c": clinic_id,
                    "pt": patient_id,
                    "pr": provider_id,
                    "s": slot_id,
                    "st": start,
                    "en": start + timedelta(minutes=minutes),
                    "status": status,
                    "by": uuid.uuid4(),
                },
            )
            if holds:
                await conn.execute(
                    text("UPDATE provider_slots SET held_by_appointment_id = :a WHERE id = :s"),
                    {"a": appointment_id, "s": slot_id},
                )
        return Seeded(appointment_id, slot_id, clinic_id, provider_id, patient_id, start)

    try:
        yield _seed
    finally:
        await cleanup_scheduling_rows(engine, providers)


async def row(engine: AsyncEngine, appointment_id: uuid.UUID) -> tuple[str, int]:
    async with engine.connect() as conn:
        r = (
            await conn.execute(
                text("SELECT status, version FROM appointments WHERE id = :a"), {"a": appointment_id}
            )
        ).one()
    return r.status, r.version


async def slot_state(engine: AsyncEngine, slot_id: uuid.UUID) -> str:
    async with engine.connect() as conn:
        return await conn.scalar(text("SELECT status FROM provider_slots WHERE id = :s"), {"s": slot_id})


async def history(engine: AsyncEngine, appointment_id: uuid.UUID) -> list[tuple[str | None, str, str]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT from_status, to_status, actor FROM appointment_status_history "
                "WHERE appointment_id = :a ORDER BY created_at, id"
            ),
            {"a": appointment_id},
        )
        return [(r.from_status, r.to_status, r.actor) for r in rows]


async def in_session(engine: AsyncEngine, fn):
    """Run `fn(db)` in its own session and COMMIT; return its result."""
    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        result = await fn(db)
        await db.commit()
        return result


def check_in(engine: AsyncEngine, appointment_id: uuid.UUID, *, before: int = 60, after: int = 0):
    return in_session(
        engine,
        lambda db: appointment_repo.check_in_appointment(
            db,
            appointment_id=appointment_id,
            actor=ACTOR,
            opens_before_minutes=before,
            closes_after_end_minutes=after,
        ),
    )


def _url(appointment_id: uuid.UUID | str, action: str, clinic_id: uuid.UUID | str | None) -> str:
    base = f"{service_base_url('scheduling')}/internal/appointments/{appointment_id}/{action}"
    return base if clinic_id is None else f"{base}?clinic_id={clinic_id}"


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _visit_token(mesh: httpx.AsyncClient, audience: str = "scheduling") -> str:
    secret = load_client_secret(os.environ["VISIT_CLIENT_SECRET_PATH"])
    tokens = ServiceTokenProvider("visit", secret, service_base_url("identity"), http=mesh)
    return await tokens.get(audience)


@pytest_asyncio.fixture
async def state_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    async for vw in _visit_world(client, mesh):
        yield vw


# --- Task 1 tracer ---------------------------------------------------------------------


async def test_tracer_check_in_then_cancel_refused(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """A Visit service token checks a confirmed appointment in through the real
    internal route; a front-desk cancel of it is then refused and the slot stays
    booked."""
    engine = committed_scheduling_engine
    world = state_world.world
    appt = await seed(status="confirmed", clinic_id=world.clinic_id)

    resp = await mesh.post(
        _url(appt.appointment_id, "check-in", world.clinic_id),
        headers=_bearer(await _visit_token(mesh)),
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "checked_in"
    assert await row(engine, appt.appointment_id) == ("checked_in", 2)
    assert await history(engine, appt.appointment_id) == [("confirmed", "checked_in", ACTOR)]
    assert await slot_state(engine, appt.slot_id) == "booked"

    cancel = await client.post(
        f"/api/appointments/{appt.appointment_id}/cancel", json={}, headers=auth(world.front_desk)
    )

    assert cancel.status_code == 409, cancel.text
    assert await row(engine, appt.appointment_id) == ("checked_in", 2)
    assert await slot_state(engine, appt.slot_id) == "booked"


@pytest.mark.parametrize("who", ["admin", "front_desk"])
async def test_user_tokens_are_refused_on_check_in(
    mesh: httpx.AsyncClient, state_world: VisitWorld, seed: Seeder, who: str
) -> None:
    """The route allows no user role: even ADMIN and FRONT_DESK get 403 (T-02.1-01)."""
    world = state_world.world
    appt = await seed(clinic_id=world.clinic_id)

    resp = await mesh.post(
        _url(appt.appointment_id, "check-in", world.clinic_id), headers=auth(getattr(world, who))
    )

    assert resp.status_code == 403, resp.text


# --- complete -----------------------------------------------------------------------------


async def test_complete_moves_checked_in_to_completed_once(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    appt = await seed(status="checked_in")

    def complete(db: AsyncSession):
        return appointment_repo.complete_appointment(db, appointment_id=appt.appointment_id, actor=ACTOR)

    assert await in_session(engine, complete) == "completed"
    assert await in_session(engine, complete) == "already"

    assert await row(engine, appt.appointment_id) == ("completed", 2)
    assert await history(engine, appt.appointment_id) == [("checked_in", "completed", ACTOR)]
    assert await slot_state(engine, appt.slot_id) == "booked"


async def test_complete_refuses_unchecked_in_and_missing(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    appt = await seed(status="confirmed")

    outcomes = [
        await in_session(
            engine,
            lambda db, a=a: appointment_repo.complete_appointment(db, appointment_id=a, actor=ACTOR),
        )
        for a in (appt.appointment_id, uuid.uuid4())
    ]

    assert outcomes == ["not_checked_in", "not_found"]
    assert await row(engine, appt.appointment_id) == ("confirmed", 1)
    assert await history(engine, appt.appointment_id) == []


async def test_a_failed_audit_does_not_fail_an_applied_complete(
    mesh: httpx.AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    committed_scheduling_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3 (plan 02.1-07): the `appointment.complete` audit is written after the
    commit; if the audit sink is down the route still answers 200 and the
    transition (with its history row) stands."""
    from services.scheduling.api import internal  # noqa: PLC0415

    engine = committed_scheduling_engine
    world = state_world.world
    appt = await seed(status="checked_in", clinic_id=world.clinic_id)
    real = internal.write_audit

    async def flaky(mongo, **kwargs):
        if kwargs.get("action") == "appointment.complete":
            raise RuntimeError("audit sink down")
        await real(mongo, **kwargs)

    monkeypatch.setattr(internal, "write_audit", flaky)

    resp = await mesh.post(
        _url(appt.appointment_id, "complete", world.clinic_id),
        headers=_bearer(await _visit_token(mesh)),
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "completed"
    assert await row(engine, appt.appointment_id) == ("completed", 2)
    assert [(h[0], h[1]) for h in await history(engine, appt.appointment_id)] == [
        ("checked_in", "completed")
    ]
    assert await audit_count(str(appt.appointment_id), "appointment.complete") == 0


# --- no-show --------------------------------------------------------------------------------


def _no_show(engine: AsyncEngine, appointment_id: uuid.UUID, grace: int = 30):
    return in_session(
        engine,
        lambda db: appointment_repo.mark_no_show(
            db, appointment_id=appointment_id, actor="system:sweeper", grace_minutes=grace
        ),
    )


async def test_mark_no_show_only_after_slot_end_plus_grace(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    # Ended 10 minutes ago: inside the 30-minute grace.
    recent = await seed(start=_now() - timedelta(minutes=40))
    # Ended 45 minutes ago: past the grace.
    old = await seed(start=_now() - timedelta(minutes=75))

    assert await _no_show(engine, recent.appointment_id) == "too_early"
    assert await row(engine, recent.appointment_id) == ("confirmed", 1)
    assert await _no_show(engine, recent.appointment_id, grace=5) == "no_show"

    assert await _no_show(engine, old.appointment_id) == "no_show"
    assert await _no_show(engine, old.appointment_id) == "already"
    assert await row(engine, old.appointment_id) == ("no_show", 2)
    assert await history(engine, old.appointment_id) == [("confirmed", "no_show", "system:sweeper")]
    assert await slot_state(engine, old.slot_id) == "booked"


async def test_mark_no_show_never_applies_to_a_checked_in_or_missing_appointment(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    checked_in = await seed(status="checked_in", start=_now() - timedelta(hours=3))

    assert await _no_show(engine, checked_in.appointment_id) == "not_confirmed"
    assert await _no_show(engine, uuid.uuid4()) == "not_found"
    assert await row(engine, checked_in.appointment_id) == ("checked_in", 1)


async def test_list_no_show_candidates_returns_only_overdue_confirmed_oldest_first(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    base = datetime(2019, 1, 1, 9, 0, tzinfo=timezone.utc)
    newer = await seed(start=base + timedelta(days=1))
    older = await seed(start=base)
    await seed(status="checked_in", start=base + timedelta(days=2))
    await seed(status="cancelled", start=base + timedelta(days=3))
    await seed(start=_now() - timedelta(minutes=40))  # inside the grace
    await seed(start=_now() + timedelta(days=1))  # future
    mine = {older.appointment_id, newer.appointment_id}

    rows = await in_session(
        engine, lambda db: appointment_repo.list_no_show_candidates(db, grace_minutes=30, limit=500)
    )
    limited = await in_session(
        engine, lambda db: appointment_repo.list_no_show_candidates(db, grace_minutes=30, limit=1)
    )

    ids = [a.id for a in rows if a.id in mine]
    assert ids == [older.appointment_id, newer.appointment_id]
    assert all(a.status == "confirmed" for a in rows)
    assert len(limited) == 1


# --- check-in window --------------------------------------------------------------------------


async def test_check_in_window_is_judged_by_the_database_clock(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    early = await seed(start=_now() + timedelta(hours=3))
    late = await seed(start=_now() - timedelta(hours=1, minutes=30))  # ended an hour ago
    inside = await seed(start=_now() + timedelta(minutes=20))
    in_progress = await seed(start=_now() - timedelta(minutes=10))

    assert await check_in(engine, early.appointment_id) == "too_early"
    assert await check_in(engine, late.appointment_id) == "too_late"
    assert await check_in(engine, inside.appointment_id) == "checked_in"
    assert await check_in(engine, in_progress.appointment_id) == "checked_in"

    # Refusals wrote nothing.
    for refused in (early, late):
        assert await row(engine, refused.appointment_id) == ("confirmed", 1)
        assert await history(engine, refused.appointment_id) == []
    assert await row(engine, inside.appointment_id) == ("checked_in", 2)


async def test_check_in_window_follows_the_given_minutes(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    early = await seed(start=_now() + timedelta(hours=3))
    ended = await seed(start=_now() - timedelta(minutes=40))  # ended 10 minutes ago

    assert await check_in(engine, early.appointment_id, before=240) == "checked_in"
    assert await check_in(engine, ended.appointment_id, after=5) == "too_late"
    assert await check_in(engine, ended.appointment_id, after=15) == "checked_in"


async def test_check_in_refuses_other_states_and_missing(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    outcomes = {}
    for status in ("pending", "cancelled", "failed", "completed", "no_show"):
        appt = await seed(status=status)
        outcomes[status] = await check_in(engine, appt.appointment_id)
        assert (await row(engine, appt.appointment_id))[0] == status
    outcomes["missing"] = await check_in(engine, uuid.uuid4())

    assert outcomes == {
        "pending": "not_confirmed",
        "cancelled": "not_confirmed",
        "failed": "not_confirmed",
        "completed": "not_confirmed",
        "no_show": "not_confirmed",
        "missing": "not_found",
    }


async def test_repeat_check_in_is_already_and_writes_nothing(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    appt = await seed()

    first = await check_in(engine, appt.appointment_id)
    second = await check_in(engine, appt.appointment_id)

    assert (first, second) == ("checked_in", "already")
    assert await row(engine, appt.appointment_id) == ("checked_in", 2)
    assert await history(engine, appt.appointment_id) == [("confirmed", "checked_in", ACTOR)]


async def test_twenty_concurrent_check_ins_make_exactly_one_transition(
    seed: Seeder, committed_scheduling_engine: AsyncEngine
) -> None:
    engine = committed_scheduling_engine
    appt = await seed()

    outcomes = await asyncio.gather(*(check_in(engine, appt.appointment_id) for _ in range(20)))

    assert sorted(outcomes) == ["already"] * 19 + ["checked_in"]
    async with engine.connect() as conn:
        count = await conn.scalar(
            text(
                "SELECT count(*) FROM appointment_status_history "
                "WHERE appointment_id = :a AND to_status = 'checked_in'"
            ),
            {"a": appt.appointment_id},
        )
    assert count == 1
    assert await row(engine, appt.appointment_id) == ("checked_in", 2)


# --- cancel / reschedule are refused at the repository and the activity layer ----------------


NEW_STATES = ("checked_in", "completed", "no_show")


@pytest.mark.parametrize("status", NEW_STATES)
async def test_repo_cancel_and_swap_refuse_the_new_states(
    seed: Seeder, committed_scheduling_engine: AsyncEngine, status: str
) -> None:
    """The race where the state changes after the API check: the repository's
    own conditional logic still refuses and changes nothing."""
    engine = committed_scheduling_engine
    appt = await seed(status=status)
    new_slot = await seed_slot(
        engine,
        provider_id=appt.provider_id,
        start=appt.start + timedelta(days=1),
        clinic_id=appt.clinic_id,
        department_id=uuid.uuid4(),
        status="held",
    )
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE provider_slots SET held_by_appointment_id = :a WHERE id = :s"),
            {"a": appt.appointment_id, "s": new_slot},
        )

    cancelled = await in_session(
        engine,
        lambda db: appointment_repo.cancel_appointment(
            db, appointment_id=appt.appointment_id, actor="user:test"
        ),
    )
    swapped = await in_session(
        engine,
        lambda db: appointment_repo.swap_slot(
            db,
            appointment_id=appt.appointment_id,
            old_slot_id=appt.slot_id,
            new_slot_id=new_slot,
            actor="user:test",
        ),
    )

    assert (cancelled, swapped) == ("not_cancellable", "rejected")
    assert await row(engine, appt.appointment_id) == (status, 1)
    assert await slot_state(engine, appt.slot_id) == "booked"
    assert await slot_state(engine, new_slot) == "held"
    assert await history(engine, appt.appointment_id) == []


@pytest.mark.parametrize("status", NEW_STATES)
async def test_workflow_activities_refuse_the_new_states(
    seed: Seeder, committed_scheduling_engine: AsyncEngine, status: str
) -> None:
    """The cancel and reschedule workflows' activities raise the non-retryable
    business errors, so the saga records a refusal instead of freeing the slot
    or issuing a refund."""
    from temporalio.exceptions import ApplicationError  # noqa: PLC0415

    from services.scheduling.activities.changes import AppointmentChangeActivities  # noqa: PLC0415
    from services.scheduling.domain.contracts import CancelInput, RescheduleInput  # noqa: PLC0415

    engine = committed_scheduling_engine
    appt = await seed(status=status)
    acts = AppointmentChangeActivities(
        async_sessionmaker(engine, expire_on_commit=False), http=None, tokens=None, mongo=None
    )

    with pytest.raises(ApplicationError) as cancel_error:
        await acts.cancel_appointment(
            CancelInput(
                appointment_id=str(appt.appointment_id),
                clinic_id=str(appt.clinic_id),
                slot_id=str(appt.slot_id),
                refund_policy="refund",
                actor="user:test",
            )
        )
    with pytest.raises(ApplicationError) as swap_error:
        await acts.swap_slot(
            RescheduleInput(
                appointment_id=str(appt.appointment_id),
                clinic_id=str(appt.clinic_id),
                old_slot_id=str(appt.slot_id),
                new_slot_id=str(uuid.uuid4()),
                actor="user:test",
            )
        )

    assert cancel_error.value.type == "NotCancellable" and cancel_error.value.non_retryable
    assert swap_error.value.type == "SwapRejected" and swap_error.value.non_retryable
    assert await row(engine, appt.appointment_id) == (status, 1)
    assert await slot_state(engine, appt.slot_id) == "booked"


@pytest.mark.parametrize(
    ("status", "detail"),
    [
        ("checked_in", "appointment already checked in"),
        ("completed", "appointment already completed"),
        ("no_show", "appointment was a no-show"),
    ],
)
async def test_gateway_cancel_and_reschedule_name_the_state(
    client: AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    committed_scheduling_engine: AsyncEngine,
    status: str,
    detail: str,
) -> None:
    engine = committed_scheduling_engine
    world = state_world.world
    appt = await seed(status=status, clinic_id=world.clinic_id)
    headers = auth(world.front_desk)

    cancel = await client.post(f"/api/appointments/{appt.appointment_id}/cancel", json={}, headers=headers)
    reschedule = await client.post(
        f"/api/appointments/{appt.appointment_id}/reschedule",
        json={"new_slot_id": str(uuid.uuid4())},
        headers=headers,
    )

    assert cancel.status_code == 409 and cancel.json()["detail"] == detail
    assert reschedule.status_code == 409 and reschedule.json()["detail"] == detail
    assert await row(engine, appt.appointment_id) == (status, 1)
    assert await slot_state(engine, appt.slot_id) == "booked"


async def test_pending_and_failed_keep_their_existing_refusals(
    client: AsyncClient, state_world: VisitWorld, seed: Seeder
) -> None:
    world = state_world.world
    pending = await seed(status="pending", clinic_id=world.clinic_id)
    failed = await seed(status="failed", clinic_id=world.clinic_id)
    headers = auth(world.front_desk)

    p = await client.post(f"/api/appointments/{pending.appointment_id}/cancel", json={}, headers=headers)
    f = await client.post(f"/api/appointments/{failed.appointment_id}/cancel", json={}, headers=headers)

    assert (p.status_code, p.json()["detail"]) == (409, "booking in progress")
    assert (f.status_code, f.json()["detail"]) == (409, "appointment not cancellable")


# --- database-level proofs: the new states keep their slot ------------------------------------

BASE = datetime(2027, 6, 7, 9, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("status", NEW_STATES)
async def test_second_active_appointment_is_rejected_when_the_first_is_in_a_new_state(
    scheduling_db_session: AsyncSession, status: str
) -> None:
    from tests.scheduling.test_appointment_constraints import _appointment, _slot  # noqa: PLC0415

    db = scheduling_db_session
    slot_id = await _slot(db)
    await _appointment(db, slot_id=slot_id, status=status)

    for second in ("pending", "confirmed", "checked_in"):
        with pytest.raises(IntegrityError) as excinfo:
            async with db.begin_nested():
                await _appointment(db, slot_id=slot_id, status=second)
        assert "uq_appointments_active_slot" in str(excinfo.value)


async def test_raw_update_into_a_new_state_cannot_free_or_double_book_a_slot(
    scheduling_db_session: AsyncSession,
) -> None:
    """A cancelled twin may share the slot; flipping it back to an active state
    while a checked-in appointment holds the slot is rejected by the index."""
    from tests.scheduling.test_appointment_constraints import _appointment, _slot  # noqa: PLC0415

    db = scheduling_db_session
    slot_id = await _slot(db)
    await _appointment(db, slot_id=slot_id, status="checked_in")
    twin = await _appointment(db, slot_id=slot_id, status="cancelled")

    with pytest.raises(IntegrityError) as excinfo:
        async with db.begin_nested():
            await db.execute(
                text("UPDATE appointments SET status = 'completed' WHERE id = :a"), {"a": twin}
            )
    assert "uq_appointments_active_slot" in str(excinfo.value)


async def test_patient_with_a_checked_in_appointment_cannot_hold_an_overlapping_one(
    scheduling_db_session: AsyncSession,
) -> None:
    from tests.scheduling.test_appointment_constraints import _appointment, _slot  # noqa: PLC0415

    db = scheduling_db_session
    patient_id = uuid.uuid4()
    first_slot = await _slot(db, start=BASE)
    other_slot = await _slot(db, start=BASE + timedelta(minutes=15))
    await _appointment(db, slot_id=first_slot, patient_id=patient_id, start=BASE, status="checked_in")

    with pytest.raises(IntegrityError) as excinfo:
        async with db.begin_nested():
            await _appointment(
                db,
                slot_id=other_slot,
                patient_id=patient_id,
                start=BASE + timedelta(minutes=15),
                status="confirmed",
            )
    assert "ex_appointments_patient_no_overlap" in str(excinfo.value)


# --- the internal routes ---------------------------------------------------------------------


@pytest.mark.parametrize("action", ["check-in", "complete"])
@pytest.mark.parametrize("who", ["admin", "front_desk", "provider", "patient"])
async def test_user_tokens_are_refused_on_both_routes(
    mesh: httpx.AsyncClient, state_world: VisitWorld, seed: Seeder, action: str, who: str
) -> None:
    world = state_world.world
    appt = await seed(status="checked_in" if action == "complete" else "confirmed", clinic_id=world.clinic_id)

    resp = await mesh.post(
        _url(appt.appointment_id, action, world.clinic_id), headers=auth(getattr(world, who))
    )

    assert resp.status_code == 403, resp.text


@pytest.mark.parametrize("action", ["check-in", "complete"])
async def test_other_service_audience_and_anonymous_callers(
    mesh: httpx.AsyncClient, state_world: VisitWorld, seed: Seeder, action: str
) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    world = state_world.world
    appt = await seed(clinic_id=world.clinic_id)
    url = _url(appt.appointment_id, action, world.clinic_id)

    billing = await mesh.post(url, headers=_bearer(create_service_token("billing", audience="scheduling")))
    wrong_aud = await mesh.post(url, headers=_bearer(await _visit_token(mesh, audience="provider")))
    anonymous = await mesh.post(url)

    assert billing.status_code == 403, billing.text
    assert wrong_aud.status_code == 401, wrong_aud.text
    assert anonymous.status_code == 401


@pytest.mark.parametrize("action", ["check-in", "complete"])
async def test_clinic_id_is_required_and_foreign_clinics_get_the_identical_404(
    mesh: httpx.AsyncClient, state_world: VisitWorld, seed: Seeder, action: str
) -> None:
    world = state_world.world
    appt = await seed(status="checked_in", clinic_id=world.clinic_id)
    headers = _bearer(await _visit_token(mesh))

    missing_param = await mesh.post(_url(appt.appointment_id, action, None), headers=headers)
    other_clinic = await mesh.post(_url(appt.appointment_id, action, uuid.uuid4()), headers=headers)
    missing = await mesh.post(_url(uuid.uuid4(), action, world.clinic_id), headers=headers)

    assert missing_param.status_code == 422
    assert other_clinic.status_code == missing.status_code == 404
    assert other_clinic.json() == missing.json() == {"detail": "not found"}


async def test_check_in_route_window_and_state_refusals(
    mesh: httpx.AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    engine = committed_scheduling_engine
    world = state_world.world
    headers = _bearer(await _visit_token(mesh))
    early = await seed(start=_now() + timedelta(hours=3), clinic_id=world.clinic_id)
    late = await seed(start=_now() - timedelta(hours=2), clinic_id=world.clinic_id)
    pending = await seed(status="pending", clinic_id=world.clinic_id)

    got = {
        name: await mesh.post(_url(a.appointment_id, "check-in", world.clinic_id), headers=headers)
        for name, a in (("early", early), ("late", late), ("pending", pending))
    }

    assert {k: (v.status_code, v.json()["detail"]) for k, v in got.items()} == {
        "early": (409, "check-in window not open"),
        "late": (409, "check-in window closed"),
        "pending": (409, "appointment not confirmed"),
    }
    assert await row(engine, early.appointment_id) == ("confirmed", 1)
    assert await audit_count(str(early.appointment_id), "appointment.check_in") == 0


async def test_check_in_window_minutes_are_read_at_request_time(
    mesh: httpx.AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.scheduling.config import settings  # noqa: PLC0415

    world = state_world.world
    headers = _bearer(await _visit_token(mesh))
    appt = await seed(start=_now() + timedelta(hours=3), clinic_id=world.clinic_id)
    url = _url(appt.appointment_id, "check-in", world.clinic_id)

    assert (await mesh.post(url, headers=headers)).status_code == 409
    monkeypatch.setattr(settings, "checkin_opens_before_minutes", 240)
    resp = await mesh.post(url, headers=headers)

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "checked_in"


async def test_check_in_then_complete_routes_are_idempotent_and_audited_once(
    mesh: httpx.AsyncClient,
    state_world: VisitWorld,
    seed: Seeder,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    engine = committed_scheduling_engine
    world = state_world.world
    headers = _bearer(await _visit_token(mesh))
    appt = await seed(clinic_id=world.clinic_id)
    appointment_id = str(appt.appointment_id)

    early_complete = await mesh.post(_url(appointment_id, "complete", world.clinic_id), headers=headers)
    first = await mesh.post(_url(appointment_id, "check-in", world.clinic_id), headers=headers)
    repeat = await mesh.post(_url(appointment_id, "check-in", world.clinic_id), headers=headers)
    done = await mesh.post(_url(appointment_id, "complete", world.clinic_id), headers=headers)
    done_again = await mesh.post(_url(appointment_id, "complete", world.clinic_id), headers=headers)
    late_check_in = await mesh.post(_url(appointment_id, "check-in", world.clinic_id), headers=headers)

    assert (early_complete.status_code, early_complete.json()["detail"]) == (409, "appointment not checked in")
    assert (first.status_code, first.json()["status"]) == (200, "checked_in")
    assert (repeat.status_code, repeat.json()["status"]) == (200, "checked_in")
    assert (done.status_code, done.json()["status"]) == (200, "completed")
    assert (done_again.status_code, done_again.json()["status"]) == (200, "completed")
    assert (late_check_in.status_code, late_check_in.json()["detail"]) == (409, "appointment not confirmed")

    assert await history(engine, appt.appointment_id) == [
        ("confirmed", "checked_in", "service:visit"),
        ("checked_in", "completed", "service:visit"),
    ]
    assert await audit_count(appointment_id, "appointment.check_in") == 1
    assert await audit_count(appointment_id, "appointment.complete") == 1
    assert await slot_state(engine, appt.slot_id) == "booked"


# --- the shared Visit-suite helper seeds any state at any time --------------------------------


@pytest.mark.parametrize("status", ["confirmed", *NEW_STATES])
async def test_confirmed_appointment_helper_accepts_start_and_books_the_slot_for_every_active_state(
    state_world: VisitWorld, committed_scheduling_engine: AsyncEngine, status: str
) -> None:
    engine = committed_scheduling_engine
    provider_id = uuid.uuid4()  # its own provider: a start near now must not overlap generated slots
    state_world.provider_ids.append(provider_id)
    start = _now() + timedelta(minutes=20)

    appointment_id = await confirmed_appointment(
        state_world, status=status, provider_id=provider_id, start=start
    )

    async with engine.connect() as conn:
        r = (
            await conn.execute(
                text(
                    "SELECT a.status, a.slot_start, s.status AS slot_status "
                    "FROM appointments a JOIN provider_slots s ON s.id = a.slot_id WHERE a.id = :a"
                ),
                {"a": appointment_id},
            )
        ).one()
    assert (r.status, r.slot_start, r.slot_status) == (status, start, "booked")
