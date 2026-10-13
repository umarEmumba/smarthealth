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
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment

from services.scheduling.domain.contracts import (
    BookingResult,
    ChangeResult,
    booking_workflow_id,
    cancel_workflow_id,
)
from tests.scheduling.conftest import seed_slot
from tests.conftest import unique_email
from tests.scheduling.seed import PASSWORD, World, auth

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
    history = [(h["from_status"], h["to_status"], h["reason_code"]) for h in record["history"]]
    return history, record["status"]


async def insert_appointment(
    engine: AsyncEngine,
    world: World,
    *,
    status: str,
    start: datetime,
    provider_id: uuid.UUID | None = None,
    clinic_id: uuid.UUID | None = None,
    slot_status: str = "available",
) -> tuple[str, str]:
    """Seed a slot and an appointment in `status` directly (states the API
    cannot be steered into on demand: pending, failed, already started).
    Rows are scoped by `provider_id` for clean-up. Returns (appointment, slot)."""
    provider_id = provider_id or world.provider_id
    clinic_id = clinic_id or world.clinic_id
    slot_id = await seed_slot(
        engine,
        provider_id=provider_id,
        start=start,
        clinic_id=clinic_id,
        department_id=world.department_id,
        status=slot_status,
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
                "pt": world.patient_id,
                "pr": provider_id,
                "s": slot_id,
                "st": start,
                "en": start + timedelta(minutes=30),
                "status": status,
                "by": uuid.uuid4(),
            },
        )
    return str(appointment_id), str(slot_id)


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
    assert [m[:2] for m in moves[-2:]] == [("verified", "refund_pending"), ("refund_pending", "refunded")]


# --- Task 3: cutoff, waiver, no-op and refusals -------------------------------------


def inside_cutoff_start(world: World) -> datetime:
    """About two hours from now, moved past the seeded 09:00-12:00 local window
    when it would overlap a generated slot (still well inside 24 hours)."""
    start = (datetime.now(timezone.utc) + timedelta(hours=2)).replace(second=0, microsecond=0)
    local = start.astimezone(world.tz)
    window_start = local.replace(hour=9, minute=0)
    window_end = local.replace(hour=12, minute=0)
    if window_start - timedelta(minutes=30) < local < window_end:
        start = window_end.astimezone(timezone.utc)
    return start


async def confirmed_inside_cutoff(
    client: AsyncClient, env: WorkflowEnvironment, world: World, engine: AsyncEngine
) -> dict[str, Any]:
    slot_id = await seed_slot(
        engine,
        provider_id=world.provider_id,
        start=inside_cutoff_start(world),
        clinic_id=world.clinic_id,
        department_id=world.department_id,
    )
    return await book_confirmed(client, env, world, str(slot_id))


async def test_cancel_inside_the_cutoff_is_no_refund(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    appointment = await confirmed_inside_cutoff(
        client, temporal_env, world, committed_scheduling_engine
    )

    resp = await cancel(client, world.patient, appointment["id"])

    assert resp.status_code == 202, resp.text
    result = await change_result(
        temporal_env, cancel_workflow_id(appointment["id"], appointment["version"])
    )
    assert result.outcome == "cancelled"
    moves, billing_status = await billing_moves(client, world, appointment["id"])
    assert billing_status == "no_refund"
    assert [m[:2] for m in moves[-1:]] == [("verified", "no_refund")]


async def test_staff_waiver_inside_the_cutoff_refunds(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    appointment = await confirmed_inside_cutoff(
        client, temporal_env, world, committed_scheduling_engine
    )

    resp = await cancel(client, world.front_desk, appointment["id"], {"waive_cutoff": True})

    assert resp.status_code == 202, resp.text
    result = await change_result(
        temporal_env, cancel_workflow_id(appointment["id"], appointment["version"])
    )
    assert result.outcome == "cancelled"
    moves, billing_status = await billing_moves(client, world, appointment["id"])
    assert billing_status == "refunded"
    assert [m[:2] for m in moves[-2:]] == [("verified", "refund_pending"), ("refund_pending", "refunded")]


async def test_patient_cannot_waive_the_cutoff(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    appointment = await book_confirmed(client, temporal_env, world, slot["id"])

    resp = await cancel(client, world.patient, appointment["id"], {"waive_cutoff": True})

    assert resp.status_code == 403, resp.text
    assert (await get_appointment(client, world, appointment["id"]))["status"] == "confirmed"


async def test_cancel_twice_is_a_no_op(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    appointment = await book_confirmed(client, temporal_env, world, slot["id"])
    appointment_id = appointment["id"]

    first = await cancel(client, world.front_desk, appointment_id)
    assert first.status_code == 202, first.text
    await change_result(temporal_env, cancel_workflow_id(appointment_id, appointment["version"]))
    second = await cancel(client, world.front_desk, appointment_id)

    assert second.status_code == 200, second.text
    assert second.json()["status"] == "cancelled"
    assert second.json()["operation"] == "cancel"
    # No second cancel workflow (the cancelled appointment's next version).
    with pytest.raises(RPCError) as exc:
        await temporal_env.client.get_workflow_handle(
            cancel_workflow_id(appointment_id, appointment["version"] + 1)
        ).describe()
    assert exc.value.status == RPCStatusCode.NOT_FOUND
    after = await get_appointment(client, world, appointment_id)
    assert [(h["from_status"], h["to_status"]) for h in after["history"]].count(
        ("confirmed", "cancelled")
    ) == 1
    moves, billing_status = await billing_moves(client, world, appointment_id)
    assert billing_status == "refunded"
    assert [m[1] for m in moves].count("refund_pending") == 1
    assert [m[1] for m in moves].count("refunded") == 1


async def test_cancel_while_pending_is_booking_in_progress(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    appointment_id, _ = await insert_appointment(
        committed_scheduling_engine,
        booking_world,
        status="pending",
        start=datetime.now(timezone.utc) + timedelta(days=10),
    )

    resp = await cancel(client, world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "booking in progress"


async def test_cancel_of_a_failed_booking_is_refused(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    appointment_id, _ = await insert_appointment(
        committed_scheduling_engine,
        world,
        status="failed",
        start=datetime.now(timezone.utc) + timedelta(days=10),
    )

    resp = await cancel(client, world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "appointment not cancellable"


async def test_cancel_after_the_slot_started_is_refused(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    appointment_id, slot_id = await insert_appointment(
        committed_scheduling_engine,
        world,
        status="confirmed",
        start=datetime.now(timezone.utc) - timedelta(days=3),
        slot_status="booked",
    )

    resp = await cancel(client, world.front_desk, appointment_id, {"waive_cutoff": True})

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "appointment already started"
    assert (await get_appointment(client, world, appointment_id))["status"] == "confirmed"


async def test_another_patients_appointment_is_not_found(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    appointment = await book_confirmed(client, temporal_env, world, slot["id"])
    email = unique_email("other-patient")
    registered = await client.post(
        "/api/auth/register",
        json={"email": email, "password": PASSWORD, "clinic_id": str(world.clinic_id)},
    )
    assert registered.status_code == 201, registered.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text

    resp = await cancel(client, login.json(), appointment["id"])
    missing = await cancel(client, login.json(), str(uuid.uuid4()))

    assert resp.status_code == 404, resp.text
    assert resp.json() == missing.json()
    assert (await get_appointment(client, world, appointment["id"]))["status"] == "confirmed"
