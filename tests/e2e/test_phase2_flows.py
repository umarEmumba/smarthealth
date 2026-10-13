"""Phase 2 flows on the deployed stack (plan 02-12; roadmap SC1, SC4, SC5, SC6; D-19).

Nothing runs in-process: the scheduling, billing and visit APIs, both Temporal
workers and the Compose Temporal are separate containers, reached only through
the published gateway port. Each test seeds its own synthetic world (fresh
clinic and provider) and books on the LAST generated day, more than 24 h
ahead, so cancels are refundable and no slot is already in the past.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase2_flows.py -m e2e
The gateway's default rate limit (300/min) is enough for this file.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    billing_record,
    book,
    day_slots,
    gateway_url,
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
