"""Cancel through the gateway, carried out by the Temporal cancel workflow
(plan 02-08; SCHED-06, BILL-02; D-13, Planning-Time Decision "409 while pending").

`POST /api/appointments/{id}/cancel` decides the refund policy in the request
(24-hour cutoff, staff waiver) and starts `cancel-{id}-{version}`; the
`scheduling_worker` fixture runs the real `CancelWorkflow` and
`AppointmentChangeActivities` on a time-skipping Temporal test server against
the real scheduling_db, Billing and the in-process mesh.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.testing import WorkflowEnvironment

from services.scheduling.domain.contracts import (
    BookingResult,
    ChangeResult,
    booking_workflow_id,
    cancel_workflow_id,
)
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, auth

pytestmark = pytest.mark.integration


# --- helpers (shared with test_reschedule.py) -----------------------------------


async def day_slots(
    client: AsyncClient, world: World, *, day=None, status: str = "available"
) -> list[dict]:
    start, end = world.local_day_bounds(day or world.last_day)
    resp = await client.get(
        "/api/slots",
        params={
            "provider_id": str(world.provider_id),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "status": status,
        },
        headers=auth(world.admin),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def slot_status(client: AsyncClient, world: World, slot_id: str, *, day=None) -> str:
    return {s["id"]: s["status"] for s in await day_slots(client, world, day=day, status="any")}[slot_id]


async def book_confirmed(
    client: AsyncClient, env: WorkflowEnvironment, world: World, slot_id: str
) -> dict[str, Any]:
    """Book `slot_id` for the world's patient (front desk) and await the saga."""
    resp = await client.post(
        "/api/appointments",
        json={"slot_id": slot_id, "patient_id": str(world.patient_id)},
        headers=auth(world.front_desk) | {"Idempotency-Key": f"test-{uuid.uuid4().hex}"},
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    handle = env.client.get_workflow_handle(
        booking_workflow_id(appointment_id), result_type=BookingResult
    )
    assert (await handle.result()).outcome == "confirmed"
    return await get_appointment(client, world, appointment_id)


async def get_appointment(client: AsyncClient, world: World, appointment_id: str) -> dict[str, Any]:
    got = await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.front_desk))
    assert got.status_code == 200, got.text
    return got.json()


async def change_result(env: WorkflowEnvironment, workflow_id: str) -> ChangeResult:
    handle = env.client.get_workflow_handle(workflow_id, result_type=ChangeResult)
    async with env.time_skipping_unlocked():
        return await handle.result()


async def billing_moves(client: AsyncClient, world: World, appointment_id: str) -> list[tuple]:
    resp = await client.get(
        "/api/billing/records",
        params={"appointment_id": appointment_id},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 200, resp.text
    record = resp.json()
    return [(h["from_status"], h["to_status"]) for h in record["history"]], record["status"]


async def cancel(
    client: AsyncClient, tokens: dict, appointment_id: str, body: dict | None = None
):
    return await client.post(
        f"/api/appointments/{appointment_id}/cancel", json=body or {}, headers=auth(tokens)
    )


# --- Task 1 tracer -----------------------------------------------------------------


async def test_tracer_cancel_frees_the_slot_and_refunds(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    # The last seeded day is at least 33 hours ahead: before the 24-hour cutoff.
    slot = (await day_slots(client, world))[0]
    appointment = await book_confirmed(client, temporal_env, world, slot["id"])
    appointment_id = appointment["id"]
    assert appointment["status"] == "confirmed"

    resp = await cancel(client, world.patient, appointment_id)

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["appointment_id"] == appointment_id
    assert body["operation"] == "cancel"
    assert body["status_url"] == f"/api/appointments/{appointment_id}"

    result = await change_result(
        temporal_env, cancel_workflow_id(appointment_id, appointment["version"])
    )
    assert result == ChangeResult(outcome="cancelled", reason=None)

    after = await get_appointment(client, world, appointment_id)
    assert after["status"] == "cancelled"
    assert [(h["from_status"], h["to_status"]) for h in after["history"]] == [
        (None, "pending"),
        ("pending", "confirmed"),
        ("confirmed", "cancelled"),
    ]
    assert await slot_status(client, world, slot["id"]) == "available"

    moves, billing_status = await billing_moves(client, world, appointment_id)
    assert billing_status == "refunded"
    assert moves[-2:] == [("verified", "refund_pending"), ("refund_pending", "refunded")]
