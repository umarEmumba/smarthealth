"""Phase 2 flows on the deployed stack (plan 02-12; roadmap SC1, SC4, SC5, SC6; D-19; SCHED-05 reschedule: quick 261110-gvd).

Nothing runs in-process: the scheduling, billing and visit APIs, both Temporal
workers and the Compose Temporal are separate containers, reached only through
the published gateway port. Each test seeds its own synthetic world (fresh
clinic and provider) and books on the LAST generated day, more than 24 h
ahead, so cancels are refundable and no slot is already in the past.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase2_flows.py -m e2e
Each seed logs in / creates four accounts, so running this file together with
the other e2e suites exceeds the gateway's auth budget (20/min per client);
start the gateway with GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 (and
GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 for the demo) for e2e runs.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    billing_in,
    billing_record,
    book,
    book_slot_inside_checkin_window,
    day_slots,
    gateway_url,
    get_appointment,
    poll,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway)


async def _first_slot(http: httpx.AsyncClient, ew: E2EWorld, index: int = 0) -> str:
    slots = await day_slots(http, ew, ew.world.last_day)
    assert len(slots) > index, slots
    return slots[index]["id"]


async def test_booking_confirmed_via_gateway(gateway: httpx.AsyncClient, ew: E2EWorld) -> None:
    slot_id = await _first_slot(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])

    confirmed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
    assert confirmed["status"] == "confirmed", confirmed
    record = await billing_record(gateway, ew, appointment_id)
    assert record is not None and record["status"] == "verified", record

    booked = {s["id"]: s["status"] for s in await day_slots(gateway, ew, ew.world.last_day, status="any")}
    assert booked[slot_id] == "booked"


async def test_forced_billing_failure_compensates(gateway: httpx.AsyncClient, ew: E2EWorld) -> None:
    slot_id = await _first_slot(gateway, ew)
    appointment_id = await book(
        gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0], coverage_ref="DENY-e2e-0001"
    )

    failed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
    assert failed["status"] == "failed", failed
    assert failed["failure_reason"] == "BillingRejected"

    # The slot is offered again, and billing was voided through failed.
    available = {s["id"] for s in await day_slots(gateway, ew, ew.world.last_day)}
    assert slot_id in available
    record = await billing_in(gateway, ew, appointment_id, "voided")
    assert [h["to_status"] for h in record["history"]] == ["pending", "failed", "voided"]


async def test_cancel_frees_slot_and_waitlist_advances(gateway: httpx.AsyncClient, ew: E2EWorld) -> None:
    patient_b, patient_a = ew.patient_ids[0], ew.patient_ids[1]
    slot_id = await _first_slot(gateway, ew)
    b_appointment = await book(gateway, ew, slot_id=slot_id, patient_id=patient_b)
    assert (await appointment_in(gateway, ew, b_appointment, {"confirmed", "failed"}))["status"] == "confirmed"

    joined = await gateway.post(
        "/api/waitlist",
        json={"provider_id": ew.provider_id, "patient_id": patient_a},
        headers=ew.front_desk,
    )
    assert joined.status_code == 201, joined.text
    entry_id = joined.json()["id"]

    cancel = await gateway.post(
        f"/api/appointments/{b_appointment}/cancel", json={}, headers=ew.front_desk
    )
    assert cancel.status_code == 202, cancel.text

    cancelled = await appointment_in(gateway, ew, b_appointment, {"cancelled"})
    assert cancelled["status"] == "cancelled"
    refunded = await billing_in(gateway, ew, b_appointment, "refunded")
    assert [h["to_status"] for h in refunded["history"]][-2:] == ["refund_pending", "refunded"]

    # The waitlist advance books patient A on the freed slot.
    async def booked_entry() -> dict | None:
        got = await gateway.get(f"/api/waitlist/{entry_id}", headers=ew.front_desk)
        assert got.status_code == 200, got.text
        body = got.json()
        return body if body["status"] in {"booked", "failed"} else None

    entry = await poll(booked_entry)
    assert entry["status"] == "booked", entry
    a_appointment = await appointment_in(
        gateway, ew, entry["booked_appointment_id"], {"confirmed", "failed"}
    )
    assert a_appointment["status"] == "confirmed", a_appointment
    assert a_appointment["slot_id"] == slot_id
    assert a_appointment["patient_id"] == patient_a


async def test_reschedule_moves_the_booking_and_keeps_billing_verified(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    slots = await day_slots(gateway, ew, ew.world.last_day)
    assert len(slots) >= 2, slots
    old_slot, new_slot = slots[0]["id"], slots[1]["id"]

    appointment_id = await book(gateway, ew, slot_id=old_slot, patient_id=ew.patient_ids[0])
    before = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
    assert before["status"] == "confirmed", before

    resp = await gateway.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": new_slot},
        headers=ew.front_desk,
    )
    assert resp.status_code == 202, resp.text
    assert resp.json()["operation"] == "reschedule"
    assert resp.json()["appointment_id"] == appointment_id

    async def swapped() -> dict | None:
        got = await get_appointment(gateway, ew, appointment_id)
        if got["slot_id"] == new_slot:
            return got
        last = got["history"][-1] if got["history"] else {}
        return got if str(last.get("reason") or "").startswith("reschedule_failed") else None

    after = await poll(swapped)
    assert after["slot_id"] == new_slot, after["history"][-1:]
    assert after["status"] == "confirmed"
    assert after["version"] == before["version"] + 1
    last_move = after["history"][-1]
    assert (last_move["from_status"], last_move["to_status"], last_move["reason"]) == (
        "confirmed",
        "confirmed",
        "rescheduled",
    )

    # The old slot is freed and the new one is booked (no waitlist in a fresh world).
    statuses = {s["id"]: s["status"] for s in await day_slots(gateway, ew, ew.world.last_day, status="any")}
    assert statuses[old_slot] == "available"
    assert statuses[new_slot] == "booked"

    # Billing is relinked to the new slot, still verified, with no refund moves.
    async def relinked() -> dict | None:
        record = await billing_record(gateway, ew, appointment_id)
        if record and record["history"] and record["history"][-1]["reason_code"] == "rescheduled":
            return record
        return None

    record = await poll(relinked)
    assert record["status"] == "verified", record
    moves = [(h["from_status"], h["to_status"], h["reason_code"]) for h in record["history"]]
    assert moves[-1] == ("verified", "verified", "rescheduled")
    assert not [m for m in moves if m[1] in {"refund_pending", "refunded", "no_refund"}]


async def test_visit_lifecycle_finalizes_billing(gateway: httpx.AsyncClient, ew: E2EWorld) -> None:
    # Check-in now moves the appointment in Scheduling, which only accepts it
    # inside the window around the slot (plan 02.1-06), so this flow books a slot
    # that starts within minutes instead of the day-ahead slot. The slot belongs
    # to a second provider, so the front desk (clinic-scoped) drives the visit.
    slot_id, _window_provider_id = await book_slot_inside_checkin_window(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))["status"] == "confirmed"

    checked_in = await gateway.post(
        "/api/visits", json={"appointment_id": appointment_id}, headers=ew.front_desk
    )
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    assert checked_in.json()["status"] == "checked_in"
    # M1: the appointment itself is now checked_in, so it can no longer be cancelled.
    assert (await get_appointment(gateway, ew, appointment_id))["status"] == "checked_in"
    refused = await gateway.post(
        f"/api/appointments/{appointment_id}/cancel", json={}, headers=ew.front_desk
    )
    assert refused.status_code == 409, refused.text

    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)
    assert started.status_code == 200, started.text
    assert started.json()["status"] == "in_progress"

    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"

    # Downstream runs on the visit worker after the response: billing finalized.
    await billing_in(gateway, ew, appointment_id, "finalized")
    # M1 (plan 02.1-09): the same fan-out completes the appointment in Scheduling.
    assert (await appointment_in(gateway, ew, appointment_id, {"completed"}))["status"] == "completed"

    listed = await gateway.get(
        "/api/visits", params={"status": "completed"}, headers=ew.front_desk
    )
    assert listed.status_code == 200, listed.text
    assert visit_id in {v["id"] for v in listed.json()}
