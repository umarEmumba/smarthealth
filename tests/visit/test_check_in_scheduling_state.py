"""Visit check-in changes the appointment in Scheduling (plan 02.1-06; mentor item M1).

Visit's public check-in used to read the appointment and write only its own
visit row, so the appointment stayed `confirmed` and cancel/reschedule kept
working for a patient already in the building. Now Visit asks Scheduling's
internal check-in FIRST (which enforces the window and arbitrates the cancel
race) and only then inserts the visit. These tests drive the public route
through the gateway and read Scheduling's state back through its public API and
its database.

Slots are seeded with SQL by `confirmed_appointment`; every test under
tests/visit runs with the opening window widened (autouse) unless it asks for
`real_checkin_window`.
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text

from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    visit_rows,
)

pytestmark = pytest.mark.integration


async def get_appointment(client: AsyncClient, tokens: dict, appointment_id: str) -> dict[str, Any]:
    resp = await client.get(f"/api/appointments/{appointment_id}", headers=auth(tokens))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def slot_status(vw: VisitWorld, appointment_id: str) -> str:
    async with vw.engine.connect() as conn:
        return await conn.scalar(
            text(
                "SELECT s.status FROM provider_slots s JOIN appointments a ON a.slot_id = s.id "
                "WHERE a.id = :a"
            ),
            {"a": appointment_id},
        )


async def cancel(client: AsyncClient, tokens: dict, appointment_id: str) -> httpx.Response:
    return await client.post(
        f"/api/appointments/{appointment_id}/cancel", json={}, headers=auth(tokens)
    )


# --- Task 1 tracer -------------------------------------------------------------


async def test_tracer_check_in_makes_the_appointment_checked_in_and_cancel_is_refused(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)

    resp = await check_in(client, world.front_desk, appointment_id)

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "checked_in"

    appointment = await get_appointment(client, world.front_desk, appointment_id)
    assert appointment["status"] == "checked_in"
    assert [(h["from_status"], h["to_status"]) for h in appointment["history"]] == [
        ("confirmed", "checked_in")
    ]

    refused = await cancel(client, world.front_desk, appointment_id)

    assert refused.status_code == 409, refused.text
    assert (await get_appointment(client, world.front_desk, appointment_id))["status"] == "checked_in"
    assert await slot_status(visit_world, appointment_id) == "booked"
    assert await visit_rows(appointment_id) == 1
    assert await audit_count(appointment_id, "appointment.check_in") == 1
    assert uuid.UUID(resp.json()["appointment_id"])  # the visit is bound to this appointment
