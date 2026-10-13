"""Booking-saga failure paths end to end (plan 02-07 Task 1; SCHED-08, D-09,
D-10, research Pitfall 6).

The real `BookingWorkflow` and `SchedulingActivities` run on a time-skipping
Temporal test server against the real scheduling_db, the in-process mesh and
the shared Mongo audit sink, so retry backoff costs no wall-clock time.

* A deliberately rejected pre-check (coverage reference with Billing's
  configured failing prefix, D-10) must leave a clean, audited state: the
  appointment `failed` with reason `BillingRejected`, the slot `available`
  again, the billing record `voided`, exactly one audit entry per saga step,
  a `pending -> failed` history trail, and no deleted rows.
* Billing unreachable for every attempt: the saga stops retrying within its
  bounded budget and compensates instead of hanging.
"""

from __future__ import annotations

import dataclasses
import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from services.scheduling.domain.contracts import (
    BookingInput,
    BookingResult,
    MarkFailedInput,
    booking_workflow_id,
)
from tests.scheduling.seed import World, auth

pytestmark = pytest.mark.integration

# Billing's default failing prefix (services/billing/domain/rules.py, D-10).
DENIED_COVERAGE = "DENY-0001"

SAGA_STEPS_ON_BILLING_REJECTION = (
    "reserve_slot",
    "billing_precheck",
    "void_billing",
    "release_slot",
    "mark_failed",
)


def _key() -> str:
    return f"test-{uuid.uuid4().hex}"


async def _slots(client: AsyncClient, world: World, *, status: str = "available") -> list[dict]:
    start, end = world.local_day_bounds(world.last_day)
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


async def _book(
    client: AsyncClient, world: World, slot_id: str, *, coverage_ref: str | None = None
) -> str:
    body: dict[str, Any] = {"slot_id": slot_id, "patient_id": str(world.patient_id)}
    if coverage_ref is not None:
        body["coverage_ref"] = coverage_ref
    headers = auth(world.front_desk) | {"Idempotency-Key": _key()}
    resp = await client.post("/api/appointments", json=body, headers=headers)
    assert resp.status_code == 202, resp.text
    return resp.json()["appointment_id"]


async def _result(env: WorkflowEnvironment, appointment_id: str) -> BookingResult:
    """Await the saga's result with time skipping UNLOCKED: a handle from
    `get_workflow_handle` (unlike one from `start_workflow`) does not unlock it
    by itself, so retry backoff would otherwise run in real time."""
    handle = env.client.get_workflow_handle(
        booking_workflow_id(appointment_id), result_type=BookingResult
    )
    async with env.time_skipping_unlocked():
        return await handle.result()


async def _audit_count(mongo: AsyncMongoClient, dedupe_key: str) -> int:
    audit = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    return await audit.count_documents({"dedupe_key": dedupe_key})


async def _row_counts(engine: AsyncEngine, world: World) -> tuple[int, int]:
    async with engine.connect() as conn:
        slots = await conn.scalar(
            text("SELECT count(*) FROM provider_slots WHERE provider_id = :p"),
            {"p": world.provider_id},
        )
        appointments = await conn.scalar(
            text("SELECT count(*) FROM appointments WHERE provider_id = :p"),
            {"p": world.provider_id},
        )
    return slots, appointments


async def _history(engine: AsyncEngine, appointment_id: str) -> list[tuple[str | None, str, str | None]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT from_status, to_status, reason FROM appointment_status_history "
                    "WHERE appointment_id = :a ORDER BY created_at, id"
                ),
                {"a": uuid.UUID(appointment_id)},
            )
        ).all()
    return [tuple(r) for r in rows]


async def _slot_row(engine: AsyncEngine, slot_id: str) -> tuple[str, Any, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT status, held_by_appointment_id, held_until "
                    "FROM provider_slots WHERE id = :s"
                ),
                {"s": uuid.UUID(slot_id)},
            )
        ).one()
    return row.status, row.held_by_appointment_id, row.held_until


async def _rejected_booking(
    client: AsyncClient, world: World, env: WorkflowEnvironment
) -> tuple[str, str]:
    slot_id = (await _slots(client, world))[0]["id"]
    appointment_id = await _book(client, world, slot_id, coverage_ref=DENIED_COVERAGE)
    result = await _result(env, appointment_id)
    assert result == BookingResult(outcome="failed", reason="BillingRejected")
    return appointment_id, slot_id


async def test_deliberately_rejected_booking_compensates_to_a_clean_audited_state(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    counts_before = await _row_counts(committed_scheduling_engine, world)

    appointment_id, slot_id = await _rejected_booking(client, world, temporal_env)

    # The appointment: failed, machine-readable reason, pending -> failed trail.
    got = await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.front_desk))
    assert got.status_code == 200, got.text
    assert got.json()["status"] == "failed"
    assert got.json()["failure_reason"] == "BillingRejected"
    assert await _history(committed_scheduling_engine, appointment_id) == [
        (None, "pending", None),
        ("pending", "failed", "BillingRejected"),
    ]

    # The slot: available again, no hold left behind.
    assert await _slot_row(committed_scheduling_engine, slot_id) == ("available", None, None)

    # The billing record: voided, through pending -> failed -> voided.
    billing = await client.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=auth(world.front_desk)
    )
    assert billing.status_code == 200, billing.text
    record = billing.json()
    assert record["status"] == "voided"
    assert [h["to_status"] for h in record["history"]] == ["pending", "failed", "voided"]

    # Exactly one audit entry per saga step that ran; none for confirm.
    wf = booking_workflow_id(appointment_id)
    for step in SAGA_STEPS_ON_BILLING_REJECTION:
        assert await _audit_count(mongo_client, f"{wf}:{step}") == 1, step
    assert await _audit_count(mongo_client, f"{wf}:confirm") == 0
    assert await _audit_count(mongo_client, f"{wf}:schedule_notification") == 0

    # Nothing was deleted: the slot set is unchanged and the failed appointment
    # is still there (the booking added exactly that one row).
    slots_before, appointments_before = counts_before
    assert await _row_counts(committed_scheduling_engine, world) == (
        slots_before,
        appointments_before + 1,
    )


async def test_compensated_slot_is_re_offered_and_books(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    failed_id, slot_id = await _rejected_booking(client, world, temporal_env)

    assert slot_id in {s["id"] for s in await _slots(client, world)}
    retry_id = await _book(client, world, slot_id)

    assert retry_id != failed_id
    assert (await _result(temporal_env, retry_id)).outcome == "confirmed"
    statuses = {s["id"]: s["status"] for s in await _slots(client, world, status="any")}
    assert statuses[slot_id] == "booked"


async def test_compensations_rerun_after_the_saga_change_nothing(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: Any,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    """A duplicate delivery of every compensation (as after a worker crash)
    must be a no-op: no second audit entry, history row or billing change."""
    world = booking_world
    appointment_id, slot_id = await _rejected_booking(client, world, temporal_env)
    wf = booking_workflow_id(appointment_id)
    inp = BookingInput(
        appointment_id=appointment_id,
        clinic_id=str(world.clinic_id),
        patient_id=str(world.patient_id),
        provider_id=str(world.provider_id),
        slot_id=slot_id,
    )

    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, workflow_id=wf)
    acts = scheduling_worker
    await env.run(acts.void_billing, inp)
    await env.run(acts.release_slot, inp)
    await env.run(acts.mark_failed, MarkFailedInput(appointment_id=appointment_id, reason="BillingRejected"))

    for step in SAGA_STEPS_ON_BILLING_REJECTION:
        assert await _audit_count(mongo_client, f"{wf}:{step}") == 1, step
    assert len(await _history(committed_scheduling_engine, appointment_id)) == 2
    assert await _slot_row(committed_scheduling_engine, slot_id) == ("available", None, None)
    billing = await client.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=auth(world.front_desk)
    )
    assert [h["to_status"] for h in billing.json()["history"]] == ["pending", "failed", "voided"]


async def test_unreachable_billing_exhausts_bounded_retries_then_compensates(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker_degraded: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
    mongo_client: AsyncMongoClient,
) -> None:
    world = booking_world
    slot_id = (await _slots(client, world))[0]["id"]
    appointment_id = await _book(client, world, slot_id)
    wf = booking_workflow_id(appointment_id)

    # Bounded: the result arrives (no hang) and names the infrastructure
    # failure, never a business outcome.
    result = await _result(temporal_env, appointment_id)
    assert result.outcome == "failed"
    assert result.reason == "UpstreamServiceUnavailable"

    handle = temporal_env.client.get_workflow_handle(wf)
    attempts: dict[str, int] = {}
    scheduled_names: dict[int, str] = {}
    last_attempt: dict[str, int] = {}
    async for event in handle.fetch_history_events():
        if event.HasField("activity_task_scheduled_event_attributes"):
            name = event.activity_task_scheduled_event_attributes.activity_type.name
            scheduled_names[event.event_id] = name
            attempts[name] = attempts.get(name, 0) + 1
        if event.HasField("activity_task_started_event_attributes"):
            started = event.activity_task_started_event_attributes
            last_attempt[scheduled_names[started.scheduled_event_id]] = started.attempt
    # Each activity is scheduled once; retries are attempts of that one task,
    # and the forward policy stops at its maximum_attempts (5).
    assert attempts["booking.billing_precheck"] == 1
    assert last_attempt["booking.billing_precheck"] == 5
    assert "booking.confirm" not in attempts
    assert attempts["booking.release_slot"] == 1
    assert attempts["booking.mark_failed"] == 1

    got = await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.front_desk))
    assert got.json()["status"] == "failed"
    assert got.json()["failure_reason"] == "UpstreamServiceUnavailable"
    assert await _slot_row(committed_scheduling_engine, slot_id) == ("available", None, None)
    assert await _history(committed_scheduling_engine, appointment_id) == [
        (None, "pending", None),
        ("pending", "failed", "UpstreamServiceUnavailable"),
    ]

    # Billing never saw the booking: no record, no billing audit entries.
    billing = await client.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=auth(world.front_desk)
    )
    assert billing.status_code == 404, billing.text
    assert await _audit_count(mongo_client, f"{wf}:billing_precheck") == 0
    assert await _audit_count(mongo_client, f"{wf}:void_billing") == 0
    assert await _audit_count(mongo_client, f"{wf}:release_slot") == 1
    assert await _audit_count(mongo_client, f"{wf}:mark_failed") == 1
