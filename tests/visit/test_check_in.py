"""Visit check-in (plan 02-10, VISIT-01, D-15).

Front-desk (own clinic) or admin checks a CONFIRMED appointment in through the
gateway. Visit confirms the appointment with Scheduling's internal lookup over
an aud=scheduling service token and creates exactly one visit per appointment.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from httpx import AsyncClient

from tests.conftest import degraded_mesh
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    get_visit,
    history_rows,
    visit_rows,
)

pytestmark = pytest.mark.integration


async def test_front_desk_checks_in_confirmed_appointment(
    client: AsyncClient, mesh: httpx.AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)

    resp = await check_in(client, world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit = resp.json()
    assert visit["status"] == "checked_in"
    assert visit["appointment_id"] == appointment_id
    assert visit["clinic_id"] == str(world.clinic_id)
    assert visit["patient_id"] == str(world.patient_id)
    assert visit["provider_id"] == str(world.provider_id)
    assert visit["started_at"] is None and visit["completed_at"] is None

    detail = await get_visit(client, world.front_desk, visit["id"])
    assert detail.status_code == 200, detail.text
    history = detail.json()["history"]
    assert [(h["from_status"], h["to_status"]) for h in history] == [(None, "checked_in")]

    assert await audit_count(visit["id"], "visit.check_in") == 1


# --- Task 2: refusals, fail-closed, idempotency, concurrency -----------------


@pytest.mark.parametrize("appointment_status", ["pending", "cancelled", "failed"])
async def test_unconfirmed_appointment_is_refused(
    client: AsyncClient, visit_world: VisitWorld, appointment_status: str
) -> None:
    appointment_id = await confirmed_appointment(visit_world, status=appointment_status)

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json() == {"detail": "appointment not confirmed"}
    assert await visit_rows(appointment_id) == 0


async def test_other_clinics_appointment_is_identical_404(
    client: AsyncClient, visit_world: VisitWorld, other_world: VisitWorld
) -> None:
    foreign = await confirmed_appointment(other_world)
    front_desk = visit_world.world.front_desk

    other_clinic = await check_in(client, front_desk, foreign)
    named_other_clinic = await check_in(client, front_desk, foreign, other_world.clinic_id)
    missing = await check_in(client, front_desk, str(uuid.uuid4()))

    assert other_clinic.status_code == named_other_clinic.status_code == missing.status_code == 404
    assert other_clinic.json() == named_other_clinic.json() == missing.json() == {"detail": "not found"}
    assert await visit_rows(foreign) == 0


async def test_admin_must_name_the_clinic(client: AsyncClient, visit_world: VisitWorld) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)

    without = await check_in(client, world.admin, appointment_id)
    assert without.status_code == 422

    wrong = await check_in(client, world.admin, appointment_id, uuid.uuid4())
    assert wrong.status_code == 404

    created = await check_in(client, world.admin, appointment_id, world.clinic_id)
    assert created.status_code == 201, created.text
    assert created.json()["status"] == "checked_in"


async def test_provider_and_patient_cannot_check_in(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)

    for tokens in (world.provider, world.patient):
        resp = await check_in(client, tokens, appointment_id)
        assert resp.status_code == 403
    assert await visit_rows(appointment_id) == 0


async def test_scheduling_unreachable_is_503_and_writes_nothing(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(visit_world)

    async with degraded_mesh("scheduling"):
        resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 503, resp.text
    assert resp.json() == {"detail": "scheduling service unavailable"}
    assert await visit_rows(appointment_id) == 0


async def test_repeat_check_in_returns_the_existing_visit(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    first = await check_in(client, world.front_desk, appointment_id)
    assert first.status_code == 201
    visit_id = first.json()["id"]
    started = await client.post(f"/api/visits/{visit_id}/start", headers=auth(world.front_desk))
    assert started.status_code == 200, started.text

    again = await check_in(client, world.front_desk, appointment_id)

    assert again.status_code == 200, again.text
    assert again.json()["id"] == visit_id
    assert again.json()["status"] == "in_progress"
    assert await visit_rows(appointment_id) == 1
    assert await audit_count(visit_id, "visit.check_in") == 1


async def test_concurrent_check_ins_create_one_visit(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(visit_world)
    front_desk = visit_world.world.front_desk

    responses = await asyncio.gather(
        *(check_in(client, front_desk, appointment_id) for _ in range(10))
    )

    codes = sorted(r.status_code for r in responses)
    assert codes == [200] * 9 + [201], [r.text for r in responses]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await visit_rows(appointment_id) == 1
    visit_id = responses[0].json()["id"]
    assert await history_rows(visit_id) == [(None, "checked_in")]
    assert await audit_count(visit_id, "visit.check_in") == 1
