"""Appointment events on the deployed stack (plan 03-07; EVENT-01, EVENT-02, OBS-02).

A booking, a cancel and a reschedule sent through the gateway each commit their
outbox row with the state change in scheduling_db; Debezium relays it to
`smarthealth.appointment.events` keyed by the appointment id. This file reads the
topic back with a fresh consumer group per call (nothing here disturbs a real
consumer) and counts messages by their `id` header.

The tests up to the "Plan 03-11" marker never stop, kill or restart a container. The
kill-after-commit and Connect-down proofs below that marker (plan 03-11) do, and need a
quiet stack: each restores what it killed or stopped in `finally`, waits for health and,
for Connect, for every registered connector task to be RUNNING. Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_booking_events.py -m e2e
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import secrets
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
import pytest_asyncio

from libs.events.outbox import event_id_for
from libs.events.catalog import topic_for
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
    day_slots,
    gateway_url,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

TOPIC = topic_for("appointment.booked")
BOOKING_COVERAGE_REF = "COVERAGE-REF-E2E-DO-NOT-LEAK"


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_connectors_running(["scheduling-outbox"])


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway)


async def _slots(http: httpx.AsyncClient, ew: E2EWorld) -> list[dict[str, Any]]:
    slots = await day_slots(http, ew, ew.world.last_day)
    assert len(slots) >= 2, slots
    return slots


async def _events(appointment_id: str, event_type: str, *, timeout: float = 60) -> list[dict[str, Any]]:
    """Every message on the appointment topic for this key and type (blocking read,
    run off the event loop)."""
    return await asyncio.to_thread(
        stack.consume_matching,
        TOPIC,
        lambda m: m["key"] == appointment_id and m["headers"].get("eventType") == event_type,
        timeout,
        1,
    )


async def _settled(appointment_id: str, event_type: str, *, extra_wait: float = 8) -> list[dict[str, Any]]:
    """Wait for the first message, then read once more after a short settle so a
    spurious second message would be seen (counting by event id)."""
    found = await _events(appointment_id, event_type)
    assert found, f"no {event_type} event for {appointment_id} within 60 s"
    await asyncio.sleep(extra_wait)
    return await asyncio.to_thread(
        stack.consume_matching,
        TOPIC,
        lambda m: m["key"] == appointment_id and m["headers"].get("eventType") == event_type,
        10,
        1,
        done=lambda matches: False,  # read the whole topic for this key until the timeout
    )


def _distinct_ids(messages: list[dict[str, Any]]) -> set[str]:
    return {m["headers"]["id"] for m in messages}


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)


def _same_instant(avro_value: datetime.datetime, api_iso: str) -> bool:
    return _aware(avro_value) == datetime.datetime.fromisoformat(api_iso.replace("Z", "+00:00"))


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_booking_publishes_appointment_booked_in_the_requests_trace(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    """A booking through the gateway, sent with a client traceparent (trace id T),
    reaches confirmed and then appears on Kafka as appointment.booked: key = the
    appointment id, id header = uuid5(appointment:type:version), traceparent in T."""
    slot = (await _slots(gateway, ew))[0]
    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)

    resp = await gateway.post(
        "/api/appointments",
        json={
            "slot_id": slot["id"],
            "patient_id": ew.patient_ids[0],
            "coverage_ref": BOOKING_COVERAGE_REF,
        },
        headers={
            **ew.front_desk,
            "Idempotency-Key": f"e2e-{uuid.uuid4().hex}",
            "traceparent": f"00-{trace_id}-{span_id}-01",
        },
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]

    confirmed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
    assert confirmed["status"] == "confirmed", confirmed

    messages = await _events(appointment_id, "appointment.booked")
    assert messages, "no appointment.booked message within 60 s"
    assert _distinct_ids(messages) == {messages[0]["headers"]["id"]}
    message = messages[0]

    assert message["key"] == appointment_id
    assert message["headers"]["eventType"] == "appointment.booked"
    # uuid5 of appointment id, type and the post-transition version: pending is
    # version 0 (the model default) and the confirm bumps it to 1.
    assert message["headers"]["id"] == str(event_id_for(appointment_id, "appointment.booked", "1"))
    traceparent = message["headers"]["traceparent"]
    assert traceparent is not None and traceparent.split("-")[1] == trace_id, traceparent

    value = message["value"]
    assert str(value["appointment_id"]) == appointment_id
    assert str(value["aggregate_id"]) == appointment_id
    assert str(value["clinic_id"]) == ew.clinic_id
    assert str(value["patient_id"]) == ew.patient_ids[0]
    assert str(value["provider_id"]) == ew.provider_id
    assert str(value["slot_id"]) == slot["id"]
    assert _same_instant(value["slot_start"], slot["start_time"])
    assert _same_instant(value["slot_end"], slot["end_time"])
    assert value["booked_at"] is not None
    assert "coverage_ref" not in value
    assert BOOKING_COVERAGE_REF.encode() not in message["raw_value"]


# --- Task 3: cancel and reschedule publish exactly one event each --------------------


async def test_cancel_publishes_exactly_one_cancelled_event_after_booked(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    slot = (await _slots(gateway, ew))[0]
    appointment_id = await book(gateway, ew, slot_id=slot["id"], patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    cancel = await gateway.post(
        f"/api/appointments/{appointment_id}/cancel", json={}, headers=ew.front_desk
    )
    assert cancel.status_code == 202, cancel.text
    assert (await appointment_in(gateway, ew, appointment_id, {"cancelled"}))["status"] == "cancelled"

    cancelled = await _settled(appointment_id, "appointment.cancelled")
    assert len(_distinct_ids(cancelled)) == 1, [m["headers"]["id"] for m in cancelled]
    booked = await _events(appointment_id, "appointment.booked")
    assert len(_distinct_ids(booked)) == 1

    assert cancelled[0]["partition"] == booked[0]["partition"]
    assert booked[0]["offset"] < cancelled[0]["offset"], "booked must precede cancelled"
    assert cancelled[0]["headers"]["id"] == str(
        event_id_for(appointment_id, "appointment.cancelled", "2")
    )
    value = cancelled[0]["value"]
    assert value["cancelled_at"] is not None
    assert str(value["appointment_id"]) == appointment_id
    assert str(value["slot_id"]) == slot["id"]
    assert "coverage_ref" not in value


async def test_reschedule_publishes_exactly_one_rescheduled_event_after_booked(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    slots = await _slots(gateway, ew)
    old_slot, new_slot = slots[0], slots[1]
    appointment_id = await book(gateway, ew, slot_id=old_slot["id"], patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    resp = await gateway.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": new_slot["id"]},
        headers=ew.front_desk,
    )
    assert resp.status_code == 202, resp.text

    async def moved() -> bool:
        got = await gateway.get(f"/api/appointments/{appointment_id}", headers=ew.front_desk)
        assert got.status_code == 200, got.text
        return got.json()["slot_id"] == new_slot["id"]

    deadline = asyncio.get_running_loop().time() + 90
    while not await moved():
        assert asyncio.get_running_loop().time() < deadline, "reschedule did not finish in 90 s"
        await asyncio.sleep(0.5)

    rescheduled = await _settled(appointment_id, "appointment.rescheduled")
    assert len(_distinct_ids(rescheduled)) == 1, [m["headers"]["id"] for m in rescheduled]
    booked = await _events(appointment_id, "appointment.booked")
    assert len(_distinct_ids(booked)) == 1

    assert rescheduled[0]["partition"] == booked[0]["partition"]
    assert booked[0]["offset"] < rescheduled[0]["offset"], "booked must precede rescheduled"
    value = rescheduled[0]["value"]
    assert str(value["old_slot_id"]) == old_slot["id"]
    assert str(value["new_slot_id"]) == new_slot["id"]
    assert _same_instant(value["old_slot_start"], old_slot["start_time"])
    assert _same_instant(value["new_slot_start"], new_slot["start_time"])
    assert _same_instant(value["new_slot_end"], new_slot["end_time"])
    assert "coverage_ref" not in value


# --- Plan 03-11: kill after commit, Connect outage -----------------------------------
#
# These use a fresh clinic (the `ew` fixture) and identify messages by the appointment key
# and the `id` header only; the topic holds many unrelated events from earlier runs.

log = logging.getLogger(__name__)

CONNECT_DOWN_QUIET_SECONDS = 10


@contextmanager
def _restored(service: str, *, connect: bool = False) -> Iterator[None]:
    """Whatever the body does to `service` (kill, stop), start it again afterwards and
    wait until it is healthy; for Connect also until every connector task is RUNNING."""
    try:
        yield
    finally:
        stack.start_service(service)
        stack.wait_service_healthy(service)
        if connect:
            stack.wait_connectors_running()


async def _confirmed_booking(gateway: httpx.AsyncClient, ew: E2EWorld, slot: dict[str, Any]) -> str:
    appointment_id = await book(gateway, ew, slot_id=slot["id"], patient_id=ew.patient_ids[0])
    got = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
    assert got["status"] == "confirmed", got
    return appointment_id


async def _copies(appointment_id: str, event_type: str, window: float) -> list[dict[str, Any]]:
    """Every copy of this key's event of this type on the topic, reading for the whole
    `window` (never stops early), so wire duplicates are seen and counted."""
    return await asyncio.to_thread(
        stack.consume_matching,
        TOPIC,
        lambda m: m["key"] == appointment_id and m["headers"].get("eventType") == event_type,
        window,
        1,
        done=lambda matches: False,
        decode=False,
    )


async def test_cancel_event_survives_worker_kill_after_commit(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    """Kill the process that committed the cancel (scheduling-worker, SIGKILL) as soon as
    the appointment reads `cancelled`: the outbox row was committed with the transition,
    so the cancelled event id (uuid5 of appointment, type, version 2) still reaches Kafka
    and the worker comes back healthy (roadmap success criterion 1, delivery half)."""
    slot = (await _slots(gateway, ew))[0]
    appointment_id = await _confirmed_booking(gateway, ew, slot)
    expected_id = str(event_id_for(appointment_id, "appointment.cancelled", "2"))

    with _restored("scheduling-worker"):
        cancel = await gateway.post(
            f"/api/appointments/{appointment_id}/cancel", json={}, headers=ew.front_desk
        )
        assert cancel.status_code == 202, cancel.text
        assert (await appointment_in(gateway, ew, appointment_id, {"cancelled"}))["status"] == "cancelled"
        await asyncio.to_thread(stack.kill_service, "scheduling-worker")

        found = await _events(appointment_id, "appointment.cancelled", timeout=60)
        assert found, "the appointment.cancelled event was lost after the worker was killed"
        assert {m["headers"]["id"] for m in found} == {expected_id}

    assert stack.service_health("scheduling-worker") == "healthy"
    stack.wait_connectors_running(["scheduling-outbox"])


async def test_booked_event_delivered_after_connect_restart(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    """With Connect stopped a booking still confirms (the outbox row commits with it) but
    no booked event reaches Kafka for 10 s; once Connect is started and every connector
    task is RUNNING the event id arrives. Debezium is at-least-once, so the number of
    copies on the wire is logged, not asserted to be one."""
    slot = (await _slots(gateway, ew))[0]
    with _restored(stack.CONNECT_SERVICE, connect=True):
        await asyncio.to_thread(stack.stop_service, stack.CONNECT_SERVICE)
        appointment_id = await _confirmed_booking(gateway, ew, slot)
        expected_id = str(event_id_for(appointment_id, "appointment.booked", "1"))

        quiet = await _copies(appointment_id, "appointment.booked", CONNECT_DOWN_QUIET_SECONDS)
        assert quiet == [], f"a booked event arrived while Connect was stopped: {len(quiet)} copies"

        await asyncio.to_thread(stack.start_service, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_service_healthy, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_connectors_running)

        arrived = await _events(appointment_id, "appointment.booked", timeout=90)
        assert arrived, "the booked event never arrived after Connect restarted"
        copies = await _copies(appointment_id, "appointment.booked", 15)

    ids = {m["headers"]["id"] for m in copies}
    assert ids == {expected_id}, ids
    log.warning(
        "connect-restart wire copies for booked event %s: %d (distinct ids: %d)",
        expected_id,
        len(copies),
        len(ids),
    )
    print(f"CONNECT_RESTART_COPIES appointment={appointment_id} event_id={expected_id} copies={len(copies)}")
