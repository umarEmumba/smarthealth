"""Visit check-in (plan 02-10, VISIT-01, D-15).

Front-desk (own clinic) or admin checks a CONFIRMED appointment in through the
gateway. Visit confirms the appointment with Scheduling's internal lookup over
an aud=scheduling service token and creates exactly one visit per appointment.
"""

from __future__ import annotations

import httpx
import pytest
from httpx import AsyncClient

from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    get_visit,
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
