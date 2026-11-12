"""Failures AFTER a committed step cannot undo or misreport it (plan 02.1-07;
S3, T-02.1-25, T-02.1-26).

The real `BookingWorkflow`, `CancelWorkflow`, `RescheduleWorkflow` and their
activities run on a time-skipping Temporal test server against the real
scheduling_db, Billing (through the in-process mesh) and the shared Mongo audit
sink, exactly like `test_booking_compensation.py`.

* An audit write that fails after `confirm` (or after a committed cancel or
  swap) is logged and non-fatal: the saga/workflow outcome follows the committed
  state, and the transition stays durable in `appointment_status_history`.
* `void_billing` and `release_slot` read the appointment's status and do
  nothing when it is `confirmed`, `checked_in`, `completed` or `no_show`.

The audit sink is made to fail by replacing the `write_audit` name in the
activity module under test with a wrapper that raises for ONE action; every
other audit still writes.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from services.scheduling.activities import booking as booking_activities
from services.scheduling.activities import changes as change_activities
from services.scheduling.domain.contracts import (
    BookingInput,
    BookingResult,
    booking_workflow_id,
    cancel_workflow_id,
    reschedule_workflow_id,
)
from tests.scheduling.seed import World, auth
from tests.scheduling.test_cancel import (
    billing_moves,
    book_confirmed,
    change_result,
    day_slots,
    get_appointment,
)

pytestmark = pytest.mark.integration

COMMITTED_STATUSES = ["confirmed", "checked_in", "completed", "no_show"]


class RecordingLog:
    """Stands in for a module's structlog logger and records every event."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def _record(self, level: str) -> Callable[..., None]:
        def emit(event: str, **kwargs: Any) -> None:
            self.events.append((level, event, kwargs))

        return emit

    def __getattr__(self, level: str) -> Callable[..., None]:
        return self._record(level)

    def named(self, event: str) -> list[dict[str, Any]]:
        return [kwargs for _, name, kwargs in self.events if name == event]


def fail_audit(monkeypatch: pytest.MonkeyPatch, module: Any, action: str) -> RecordingLog:
    """`write_audit` in `module` raises for `action` only; returns the log that
    now receives the module's log events."""
    real = module.write_audit

    async def flaky(mongo: Any, **kwargs: Any) -> None:
        if kwargs.get("action") == action:
            raise RuntimeError("audit sink down")
        await real(mongo, **kwargs)

    log = RecordingLog()
    monkeypatch.setattr(module, "write_audit", flaky)
    monkeypatch.setattr(module, "log", log)
    return log


async def audit_count(mongo: AsyncMongoClient, dedupe_key: str) -> int:
    audit = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    return await audit.count_documents({"dedupe_key": dedupe_key})


async def history(engine: AsyncEngine, appointment_id: str) -> list[tuple[str | None, str]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT from_status, to_status FROM appointment_status_history "
                    "WHERE appointment_id = :a ORDER BY created_at, id"
                ),
                {"a": uuid.UUID(appointment_id)},
            )
        ).all()
    return [tuple(r) for r in rows]


async def slot_row(engine: AsyncEngine, slot_id: str) -> tuple[str, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, held_by_appointment_id FROM provider_slots WHERE id = :s"),
                {"s": uuid.UUID(slot_id)},
            )
        ).one()
    return row.status, row.held_by_appointment_id


async def set_status(engine: AsyncEngine, appointment_id: str, status: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE appointments SET status = :st WHERE id = :a"),
            {"st": status, "a": uuid.UUID(appointment_id)},
        )


async def book_and_wait(
    client: AsyncClient, env: WorkflowEnvironment, world: World, slot_id: str
) -> tuple[str, BookingResult]:
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
    async with env.time_skipping_unlocked():
        return appointment_id, await handle.result()


def booking_input(world: World, appointment_id: str, slot_id: str) -> BookingInput:
    return BookingInput(
        appointment_id=appointment_id,
        clinic_id=str(world.clinic_id),
        patient_id=str(world.patient_id),
        provider_id=str(world.provider_id),
        slot_id=slot_id,
    )


def activity_env(workflow_id: str) -> ActivityEnvironment:
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, workflow_id=workflow_id)
    return env


# --- S3: an audit failure after confirm leaves a confirmed booking -----------------------


async def test_audit_failure_after_confirm_leaves_a_confirmed_booking_with_billing_intact(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = booking_world
    fail_audit(monkeypatch, booking_activities, "appointment.confirm")
    slot_id = (await day_slots(client, world))[0]["id"]

    appointment_id, result = await book_and_wait(client, temporal_env, world, slot_id)

    # The saga follows the committed state.
    assert result == BookingResult(outcome="confirmed", reason=None)
    assert (await get_appointment(client, world, appointment_id))["status"] == "confirmed"
    assert (await slot_row(committed_scheduling_engine, slot_id))[0] == "booked"
    # The transition is durable without its audit entry.
    assert await history(committed_scheduling_engine, appointment_id) == [
        (None, "pending"),
        ("pending", "confirmed"),
    ]
    # Billing was NOT voided: the record is still verified and no void happened.
    moves, status = await billing_moves(client, world, appointment_id)
    assert status == "verified"
    assert "voided" not in [to for _, to, _ in moves]
    wf = booking_workflow_id(appointment_id)
    assert await audit_count(mongo_client, f"{wf}:void_billing") == 0
    assert await audit_count(mongo_client, f"{wf}:release_slot") == 0
    assert await audit_count(mongo_client, f"{wf}:mark_failed") == 0
    # The lost entry is the only gap; the later notification intent still wrote.
    assert await audit_count(mongo_client, f"{wf}:confirm") == 0
    assert await audit_count(mongo_client, f"{wf}:schedule_notification") == 1


async def test_the_failed_audit_is_logged_with_action_and_ids_only(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = booking_world
    log = fail_audit(monkeypatch, booking_activities, "appointment.confirm")
    slot_id = (await day_slots(client, world))[0]["id"]

    appointment_id, result = await book_and_wait(client, temporal_env, world, slot_id)

    assert result.outcome == "confirmed"
    entries = log.named("audit.write_failed")
    assert len(entries) == 1, "one confirm attempt, one logged gap (the activity does not retry)"
    assert entries[0] == {
        "action": "appointment.confirm",
        "entity_type": "appointment",
        "entity_id": appointment_id,
        "error": "RuntimeError",
    }
    # No exception message (it could carry data) reached the log.
    assert "audit sink down" not in repr(log.events)


# --- S3: compensations never touch a committed appointment ---------------------------------


@pytest.mark.parametrize("status", COMMITTED_STATUSES)
async def test_void_billing_skips_a_committed_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    status: str,
) -> None:
    world = booking_world
    slot_id = (await day_slots(client, world))[0]["id"]
    confirmed = await book_confirmed(client, temporal_env, world, slot_id)
    appointment_id = confirmed["id"]
    if status != "confirmed":
        await set_status(committed_scheduling_engine, appointment_id, status)
    before = await billing_moves(client, world, appointment_id)
    assert before[1] == "verified"

    wf = f"guard-{uuid.uuid4().hex}"
    await activity_env(wf).run(
        scheduling_worker.void_billing, booking_input(world, appointment_id, slot_id)
    )

    assert await billing_moves(client, world, appointment_id) == before
    assert await audit_count(mongo_client, f"{wf}:void_billing") == 0


async def test_void_billing_still_voids_a_pending_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    """Positive control: the guard is about COMMITTED states, not every call."""
    world = booking_world
    slot_id = (await day_slots(client, world))[0]["id"]
    confirmed = await book_confirmed(client, temporal_env, world, slot_id)
    appointment_id = confirmed["id"]
    await set_status(committed_scheduling_engine, appointment_id, "pending")

    wf = f"guard-{uuid.uuid4().hex}"
    await activity_env(wf).run(
        scheduling_worker.void_billing, booking_input(world, appointment_id, slot_id)
    )

    moves, status = await billing_moves(client, world, appointment_id)
    assert status == "voided"
    assert [to for _, to, _ in moves][-1] == "voided"
    assert await audit_count(mongo_client, f"{wf}:void_billing") == 1


@pytest.mark.parametrize("status", COMMITTED_STATUSES)
async def test_release_slot_leaves_a_committed_appointments_slot_untouched(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    status: str,
) -> None:
    world = booking_world
    engine = committed_scheduling_engine
    slot_id = (await day_slots(client, world))[0]["id"]
    appointment_id = (await book_confirmed(client, temporal_env, world, slot_id))["id"]
    if status != "confirmed":
        await set_status(engine, appointment_id, status)
    assert await slot_row(engine, slot_id) == ("booked", uuid.UUID(appointment_id))

    wf = f"guard-{uuid.uuid4().hex}"
    await activity_env(wf).run(
        scheduling_worker.release_slot, booking_input(world, appointment_id, slot_id)
    )

    assert await slot_row(engine, slot_id) == ("booked", uuid.UUID(appointment_id))
    assert await audit_count(mongo_client, f"{wf}:release_slot") == 0


async def test_the_release_guard_is_what_protects_a_slot_still_held_by_a_committed_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """`release_hold` alone would free a HELD slot of this appointment; the
    status guard is what stops a compensation from doing so once the
    appointment is committed (an inconsistent row the guard must still respect)."""
    world = booking_world
    engine = committed_scheduling_engine
    slot_id = (await day_slots(client, world))[0]["id"]
    appointment_id = (await book_confirmed(client, temporal_env, world, slot_id))["id"]
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = 'held', held_until = now() + interval '10 minutes' "
                "WHERE id = :s"
            ),
            {"s": uuid.UUID(slot_id)},
        )

    await activity_env(f"guard-{uuid.uuid4().hex}").run(
        scheduling_worker.release_slot, booking_input(world, appointment_id, slot_id)
    )

    assert await slot_row(engine, slot_id) == ("held", uuid.UUID(appointment_id))


async def test_release_slot_still_frees_the_hold_of_a_pending_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    """Positive control for the release guard."""
    world = booking_world
    engine = committed_scheduling_engine
    slot_id = (await day_slots(client, world))[0]["id"]
    appointment_id = (await book_confirmed(client, temporal_env, world, slot_id))["id"]
    await set_status(engine, appointment_id, "pending")
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = 'held', held_until = now() + interval '10 minutes' "
                "WHERE id = :s"
            ),
            {"s": uuid.UUID(slot_id)},
        )

    await activity_env(f"guard-{uuid.uuid4().hex}").run(
        scheduling_worker.release_slot, booking_input(world, appointment_id, slot_id)
    )

    assert await slot_row(engine, slot_id) == ("available", None)


# --- S3: a failed audit after cancel / swap commits is not a refusal ------------------------


async def test_audit_failure_after_cancel_commits_does_not_turn_the_cancel_into_a_refusal(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = booking_world
    log = fail_audit(monkeypatch, change_activities, "appointment.cancel")
    slot_id = (await day_slots(client, world))[0]["id"]
    appointment = await book_confirmed(client, temporal_env, world, slot_id)

    resp = await client.post(
        f"/api/appointments/{appointment['id']}/cancel", json={}, headers=auth(world.front_desk)
    )
    assert resp.status_code == 202, resp.text
    workflow_id = cancel_workflow_id(appointment["id"], appointment["version"])
    result = await change_result(temporal_env, workflow_id)

    assert (result.outcome, result.reason) == ("cancelled", None)
    assert (await get_appointment(client, world, appointment["id"]))["status"] == "cancelled"
    assert (await slot_row(committed_scheduling_engine, slot_id))[0] == "available"
    # billing_cancel still ran: the refund was issued.
    assert (await billing_moves(client, world, appointment["id"]))[1] == "refunded"
    assert await audit_count(mongo_client, f"{workflow_id}:cancel_appointment") == 0
    assert [e["action"] for e in log.named("audit.write_failed")] == ["appointment.cancel"]


async def test_audit_failure_after_swap_commits_does_not_make_the_reschedule_fail(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = booking_world
    log = fail_audit(monkeypatch, change_activities, "appointment.reschedule")
    slots = await day_slots(client, world)
    appointment = await book_confirmed(client, temporal_env, world, slots[0]["id"])

    resp = await client.post(
        f"/api/appointments/{appointment['id']}/reschedule",
        json={"new_slot_id": slots[1]["id"]},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 202, resp.text
    workflow_id = reschedule_workflow_id(appointment["id"], appointment["version"])
    result = await change_result(temporal_env, workflow_id)

    assert (result.outcome, result.reason) == ("rescheduled", None)
    moved = await get_appointment(client, world, appointment["id"])
    assert (moved["status"], moved["slot_id"]) == ("confirmed", slots[1]["id"])
    engine = committed_scheduling_engine
    assert (await slot_row(engine, slots[1]["id"]))[0] == "booked"
    assert (await slot_row(engine, slots[0]["id"]))[0] == "available"
    assert await audit_count(mongo_client, f"{workflow_id}:swap_slot") == 0
    assert [e["action"] for e in log.named("audit.write_failed")] == ["appointment.reschedule"]
