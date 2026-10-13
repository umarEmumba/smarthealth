"""Waitlist through the gateway, advanced by Temporal (plan 02-09; SCHED-07,
D-12, D-13, D-14, research Pattern 7 / Pitfall 9).

A freed slot (cancel, or the old slot of a reschedule) starts
`WaitlistAdvanceWorkflow` as an ABANDONed child of the cancel/reschedule
workflow; it claims the oldest matching waiting entry and auto-books it through
the normal `BookingWorkflow` saga (child). The `scheduling_worker` fixture runs
every real workflow and activity on a time-skipping Temporal test server
against the real scheduling_db, Billing and the in-process mesh.
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

from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.domain.contracts import (
    WAITLIST_ADVANCE_WORKFLOW,
    BookingResult,
    ChangeResult,
    WaitlistAdvanceInput,
    WaitlistAdvanceResult,
    booking_workflow_id,
    cancel_workflow_id,
    reschedule_workflow_id,
    waitlist_advance_workflow_id,
)
from tests.conftest import unique_email
from tests.scheduling.seed import PASSWORD, World, auth
from tests.scheduling.test_cancel import day_slots, get_appointment, slot_status

pytestmark = pytest.mark.integration


# --- helpers (shared with test_waitlist_concurrency.py) ---------------------------


async def add_patient(client: AsyncClient, world: World) -> tuple[str, dict[str, Any]]:
    """A second synthetic patient in the world's clinic, linked to its own
    PATIENT login. Returns (patient_id, tokens)."""
    email = unique_email("patient")
    account = await client.post(
        "/api/auth/register",
        json={"email": email, "password": PASSWORD, "clinic_id": str(world.clinic_id)},
    )
    assert account.status_code == 201, account.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    patient = await client.post(
        "/api/patients",
        json={
            "clinic_id": str(world.clinic_id),
            "full_name": "Qwxv Waitlist Synthetic",
            "date_of_birth": "1991-02-03",
            "user_id": account.json()["id"],
        },
        headers=auth(world.front_desk),
    )
    assert patient.status_code == 201, patient.text
    return patient.json()["id"], login.json()


async def book_for(
    client: AsyncClient, env: WorkflowEnvironment, world: World, slot_id: str, patient_id: str
) -> dict[str, Any]:
    """Front desk books `slot_id` for `patient_id`; await the confirmed saga."""
    resp = await client.post(
        "/api/appointments",
        json={"slot_id": slot_id, "patient_id": patient_id},
        headers=auth(world.front_desk) | {"Idempotency-Key": f"wl-{uuid.uuid4().hex}"},
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    handle = env.client.get_workflow_handle(
        booking_workflow_id(appointment_id), result_type=BookingResult
    )
    async with env.time_skipping_unlocked():
        assert (await handle.result()).outcome == "confirmed"
    return await get_appointment(client, world, appointment_id)


async def join(client: AsyncClient, tokens: dict, body: dict) -> Any:
    return await client.post("/api/waitlist", json=body, headers=auth(tokens))


async def joined(client: AsyncClient, tokens: dict, body: dict) -> dict[str, Any]:
    resp = await join(client, tokens, body)
    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "waiting"
    return resp.json()


async def get_entry(client: AsyncClient, world: World, entry_id: str) -> dict[str, Any]:
    resp = await client.get(f"/api/waitlist/{entry_id}", headers=auth(world.front_desk))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def result_of(env: WorkflowEnvironment, workflow_id: str, result_type: type) -> Any:
    handle = env.client.get_workflow_handle(workflow_id, result_type=result_type)
    async with env.time_skipping_unlocked():
        return await handle.result()


async def cancel_and_advance(
    client: AsyncClient, env: WorkflowEnvironment, world: World, appointment: dict[str, Any]
) -> WaitlistAdvanceResult:
    """Front desk cancels `appointment`; await the cancel and its advance."""
    resp = await client.post(
        f"/api/appointments/{appointment['id']}/cancel", json={}, headers=auth(world.front_desk)
    )
    assert resp.status_code == 202, resp.text
    cancel_id = cancel_workflow_id(appointment["id"], appointment["version"])
    assert (await result_of(env, cancel_id, ChangeResult)).outcome == "cancelled"
    return await result_of(env, waitlist_advance_workflow_id(cancel_id), WaitlistAdvanceResult)


async def run_advance(
    env: WorkflowEnvironment, world: World, slot_id: str
) -> WaitlistAdvanceResult:
    """Run an advance directly for `slot_id` (no cancel needed)."""
    source = f"test-{uuid.uuid4().hex}"
    handle = await env.client.start_workflow(
        WAITLIST_ADVANCE_WORKFLOW,
        WaitlistAdvanceInput(slot_id=slot_id, clinic_id=str(world.clinic_id), source_workflow_id=source),
        id=waitlist_advance_workflow_id(source),
        task_queue=SCHEDULING_TASK_QUEUE,
        result_type=WaitlistAdvanceResult,
    )
    async with env.time_skipping_unlocked():
        return await handle.result()


async def appointments_on_slot(engine: AsyncEngine, slot_id: str) -> list[tuple]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT id, patient_id, status, failure_reason FROM appointments "
                "WHERE slot_id = :s ORDER BY created_at"
            ),
            {"s": uuid.UUID(slot_id)},
        )
        return [tuple(r) for r in rows]


# --- Task 1 tracer -------------------------------------------------------------------


async def test_patient_joins_waitlist_and_is_auto_booked_on_cancel(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    patient_b, _ = await add_patient(client, world)
    booked_b = await book_for(client, temporal_env, world, slot["id"], patient_b)

    # Patient A (the world's patient) joins with their own token, no window.
    entry = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    assert entry["patient_id"] == str(world.patient_id)
    assert "coverage_ref" not in entry

    result = await cancel_and_advance(client, temporal_env, world, booked_b)

    assert result.outcome == "booked"
    assert result.booked_entry_id == entry["id"]
    assert result.attempts == 1
    after = await get_entry(client, world, entry["id"])
    assert after["status"] == "booked"
    assert after["booked_appointment_id"] is not None
    appointment = await get_appointment(client, world, after["booked_appointment_id"])
    assert appointment["status"] == "confirmed"
    assert appointment["patient_id"] == str(world.patient_id)
    assert appointment["slot_id"] == slot["id"]
    assert await slot_status(client, world, slot["id"]) == "booked"
