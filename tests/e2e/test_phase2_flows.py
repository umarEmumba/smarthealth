"""Phase 2 flows on the deployed stack (plan 02-12; roadmap SC1, SC4, SC5, SC6; D-19).

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
    day_slots,
    gateway_url,
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


async def test_visit_lifecycle_finalizes_billing(gateway: httpx.AsyncClient, ew: E2EWorld) -> None:
    slot_id = await _first_slot(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))["status"] == "confirmed"

    checked_in = await gateway.post(
        "/api/visits", json={"appointment_id": appointment_id}, headers=ew.front_desk
    )
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    assert checked_in.json()["status"] == "checked_in"

    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.provider)
    assert started.status_code == 200, started.text
    assert started.json()["status"] == "in_progress"

    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.provider)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"

    # Downstream runs on the visit worker after the response: billing finalized.
    await billing_in(gateway, ew, appointment_id, "finalized")

    listed = await gateway.get(
        "/api/visits", params={"status": "completed"}, headers=ew.front_desk
    )
    assert listed.status_code == 200, listed.text
    assert visit_id in {v["id"] for v in listed.json()}
