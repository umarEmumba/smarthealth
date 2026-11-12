"""Reschedule as atomic reserve-then-swap (plan 02-08; SCHED-05, BILL-02; D-14,
research Pattern 5 and Pitfall 9).

`POST /api/appointments/{id}/reschedule` checks scope and the new slot, then
starts `reschedule-{id}-{version}`; the real `RescheduleWorkflow` holds the new
slot first and swaps in ONE local transaction. Every failure leaves the original
booking untouched and is written to the appointment's history.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.testing import WorkflowEnvironment

from libs.db.postgres import get_session_factory
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    RESCHEDULE_WORKFLOW,
    ChangeResult,
    RescheduleInput,
    reschedule_workflow_id,
)
from services.scheduling.repository import appointment_repo
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, auth
from tests.scheduling.test_cancel import (
    billing_moves,
    book_confirmed,
    change_result,
    day_slots,
    get_appointment,
    insert_appointment,
    slot_status,
)

pytestmark = pytest.mark.integration


async def reschedule(client: AsyncClient, tokens: dict, appointment_id: str, new_slot_id: str):
    return await client.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": new_slot_id},
        headers=auth(tokens),
    )


def history_pairs(appointment: dict) -> list[tuple]:
    return [(h["from_status"], h["to_status"], h["reason"]) for h in appointment["history"]]


async def test_reschedule_swaps_slots_atomically_and_relinks_billing(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    old, new = (await day_slots(client, world))[:2]
    appointment = await book_confirmed(client, temporal_env, world, old["id"])
    appointment_id = appointment["id"]

    resp = await reschedule(client, world.patient, appointment_id, new["id"])

    assert resp.status_code == 202, resp.text
    assert resp.json()["operation"] == "reschedule"
    result = await change_result(
        temporal_env, reschedule_workflow_id(appointment_id, appointment["version"])
    )
    assert result == ChangeResult(outcome="rescheduled", reason=None)

    after = await get_appointment(client, world, appointment_id)
    assert after["status"] == "confirmed"
    assert after["slot_id"] == new["id"]
    assert datetime.fromisoformat(after["slot_start"]) == datetime.fromisoformat(new["start_time"])
    assert datetime.fromisoformat(after["slot_end"]) == datetime.fromisoformat(new["end_time"])
    assert after["version"] == appointment["version"] + 1
    assert history_pairs(after)[-1] == ("confirmed", "confirmed", "rescheduled")
    assert await slot_status(client, world, old["id"]) == "available"
    assert await slot_status(client, world, new["id"]) == "booked"

    moves, billing_status = await billing_moves(client, world, appointment_id)
    assert billing_status == "verified"
    assert moves[-1] == ("verified", "verified", "rescheduled")
    assert not [m for m in moves if m[1] in ("refund_pending", "refunded", "no_refund")]


async def test_new_slot_taken_before_the_hold_leaves_the_booking_untouched(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    old, taken, other = (await day_slots(client, world))[:3]
    appointment = await book_confirmed(client, temporal_env, world, old["id"])
    appointment_id = appointment["id"]
    # Another booking grabs the new slot between the request's check and the hold.
    async with committed_scheduling_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = 'held', held_by_appointment_id = :x, "
                "held_until = now() + interval '10 minutes' WHERE id = :s"
            ),
            {"x": uuid.uuid4(), "s": taken["id"]},
        )
    workflow_id = reschedule_workflow_id(appointment_id, appointment["version"])
    await temporal_env.client.start_workflow(
        RESCHEDULE_WORKFLOW,
        RescheduleInput(
            appointment_id=appointment_id,
            clinic_id=str(world.clinic_id),
            old_slot_id=old["id"],
            new_slot_id=taken["id"],
            actor="user:test",
        ),
        id=workflow_id,
        task_queue=SCHEDULING_TASK_QUEUE,
    )

    result = await change_result(temporal_env, workflow_id)

    assert result == ChangeResult(outcome="failed", reason="SlotUnavailable")
    after = await get_appointment(client, world, appointment_id)
    assert after["status"] == "confirmed"
    assert after["slot_id"] == old["id"]
    assert history_pairs(after)[-1] == ("confirmed", "confirmed", "reschedule_failed:SlotUnavailable")
    assert await slot_status(client, world, old["id"]) == "booked"
    assert await slot_status(client, world, taken["id"]) == "held"  # the other holder's, untouched

    # The recorded failure bumped the version, so a new attempt gets a fresh workflow id.
    assert after["version"] == appointment["version"] + 1
    retry = await reschedule(client, world.front_desk, appointment_id, other["id"])
    assert retry.status_code == 202, retry.text
    retried = await change_result(
        temporal_env, reschedule_workflow_id(appointment_id, after["version"])
    )
    assert retried.outcome == "rescheduled"
    assert (await get_appointment(client, world, appointment_id))["slot_id"] == other["id"]


async def test_patient_overlap_rejects_the_swap_and_releases_the_new_hold(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    old, new = (await day_slots(client, world))[:2]
    appointment = await book_confirmed(client, temporal_env, world, old["id"])
    appointment_id = appointment["id"]
    # The patient already has an appointment with ANOTHER provider at the new time.
    other_provider = uuid.uuid4()
    try:
        await insert_appointment(
            committed_scheduling_engine,
            world,
            status="confirmed",
            start=datetime.fromisoformat(new["start_time"]),
            provider_id=other_provider,
            slot_status="booked",
        )

        resp = await reschedule(client, world.front_desk, appointment_id, new["id"])
        assert resp.status_code == 202, resp.text
        result = await change_result(
            temporal_env, reschedule_workflow_id(appointment_id, appointment["version"])
        )

        assert result == ChangeResult(outcome="failed", reason="SwapRejected")
        after = await get_appointment(client, world, appointment_id)
        assert after["status"] == "confirmed"
        assert after["slot_id"] == old["id"]
        assert history_pairs(after)[-1] == ("confirmed", "confirmed", "reschedule_failed:SwapRejected")
        assert await slot_status(client, world, old["id"]) == "booked"
        assert await slot_status(client, world, new["id"]) == "available"
        moves, billing_status = await billing_moves(client, world, appointment_id)
        assert billing_status == "verified"
        assert not [m for m in moves if m[2] == "rescheduled"]
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [other_provider])


async def test_swap_repeat_is_already_with_no_extra_history(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    old, new = (await day_slots(client, world))[:2]
    appointment = await book_confirmed(client, temporal_env, world, old["id"])
    appointment_id = appointment["id"]
    await reschedule(client, world.front_desk, appointment_id, new["id"])
    assert (
        await change_result(temporal_env, reschedule_workflow_id(appointment_id, appointment["version"]))
    ).outcome == "rescheduled"
    rows_before = len((await get_appointment(client, world, appointment_id))["history"])

    session_factory = get_session_factory(settings.database_url)
    async with session_factory() as db:
        outcome = await appointment_repo.swap_slot(
            db,
            appointment_id=uuid.UUID(appointment_id),
            old_slot_id=uuid.UUID(old["id"]),
            new_slot_id=uuid.UUID(new["id"]),
            actor="user:test",
        )
        await db.commit()

    assert outcome == "already"
    after = await get_appointment(client, world, appointment_id)
    assert len(after["history"]) == rows_before
    assert after["slot_id"] == new["id"]


async def test_reschedule_refuses_pending_cancelled_and_failed(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    target = (await day_slots(client, world))[-1]
    base = datetime.now(timezone.utc) + timedelta(days=10)
    for offset, (state, detail) in enumerate(
        [
            ("pending", "booking in progress"),
            ("cancelled", "appointment not reschedulable"),
            ("failed", "appointment not reschedulable"),
        ]
    ):
        appointment_id, _ = await insert_appointment(
            committed_scheduling_engine, world, status=state, start=base + timedelta(hours=offset)
        )
        resp = await reschedule(client, world.front_desk, appointment_id, target["id"])
        assert resp.status_code == 409, (state, resp.text)
        assert resp.json()["detail"] == detail


async def test_reschedule_checks_the_new_slot(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    slots = await day_slots(client, world)
    appointment = await book_confirmed(client, temporal_env, world, slots[0]["id"])
    appointment_id = appointment["id"]
    other_provider = uuid.uuid4()
    try:
        # A different provider's slot in the same clinic -> 422.
        foreign = await seed_slot(
            committed_scheduling_engine,
            provider_id=other_provider,
            start=datetime.now(timezone.utc) + timedelta(days=9),
            clinic_id=world.clinic_id,
            department_id=world.department_id,
        )
        resp = await reschedule(client, world.front_desk, appointment_id, str(foreign))
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"] == "reschedule must keep the same provider"

        # A slot in another clinic, or no slot at all -> 404.
        elsewhere = await seed_slot(
            committed_scheduling_engine,
            provider_id=other_provider,
            start=datetime.now(timezone.utc) + timedelta(days=11),
            clinic_id=uuid.uuid4(),
            department_id=world.department_id,
        )
        for missing in (str(elsewhere), str(uuid.uuid4())):
            resp = await reschedule(client, world.front_desk, appointment_id, missing)
            assert resp.status_code == 404, resp.text

        # A past slot of the same provider -> 409.
        past = await seed_slot(
            committed_scheduling_engine,
            provider_id=world.provider_id,
            start=datetime.now(timezone.utc) - timedelta(days=3),
            clinic_id=world.clinic_id,
            department_id=world.department_id,
        )
        resp = await reschedule(client, world.front_desk, appointment_id, str(past))
        assert resp.status_code == 409, resp.text

        # A withdrawn (not available) slot -> 409.
        withdrawn = slots[1]["id"]
        cancelled = await client.post(f"/api/slots/{withdrawn}/cancel", headers=auth(world.admin))
        assert cancelled.status_code == 200, cancelled.text
        resp = await reschedule(client, world.front_desk, appointment_id, withdrawn)
        assert resp.status_code == 409, resp.text

        # Out of scope appointment (unknown id) -> 404; nothing changed.
        resp = await reschedule(client, world.front_desk, str(uuid.uuid4()), slots[2]["id"])
        assert resp.status_code == 404, resp.text
        assert (await get_appointment(client, world, appointment_id))["slot_id"] == slots[0]["id"]
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [other_provider])
