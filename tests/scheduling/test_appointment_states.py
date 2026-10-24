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
