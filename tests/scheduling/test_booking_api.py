"""Booking through the gateway, confirmed by the Temporal saga (plan 02-06;
SCHED-01, SCHED-02, SCHED-04, BILL-01; D-07, D-08, D-09).

`POST /api/appointments` commits a `pending` appointment and starts
`booking-{appointment_id}`; the `scheduling_worker` fixture runs the real
`BookingWorkflow` and `SchedulingActivities` on a time-skipping Temporal test
server, against the real scheduling_db and the in-process mesh (Patient,
Provider, Organization, Billing, Identity's service-token mint).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from temporalio.testing import WorkflowEnvironment

from services.scheduling.domain.contracts import BookingResult, booking_workflow_id
from tests.scheduling.seed import World, auth

pytestmark = pytest.mark.integration


async def _slots(client: AsyncClient, world: World, *, day=None, status: str = "available") -> list[dict]:
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


def new_key() -> str:
    return f"test-{uuid.uuid4().hex}"


async def book(
    client: AsyncClient,
    tokens: dict[str, Any],
    *,
    slot_id: str,
    patient_id: str | None,
    key: str | None = None,
    coverage_ref: str | None = None,
):
    body: dict[str, Any] = {"slot_id": slot_id}
    if patient_id is not None:
        body["patient_id"] = patient_id
    if coverage_ref is not None:
        body["coverage_ref"] = coverage_ref
    headers = auth(tokens)
    if key is not None:
        headers["Idempotency-Key"] = key
    return await client.post("/api/appointments", json=body, headers=headers)


async def saga_result(env: WorkflowEnvironment, appointment_id: str) -> BookingResult:
    handle = env.client.get_workflow_handle(
        booking_workflow_id(appointment_id), result_type=BookingResult
    )
    return await handle.result()


async def test_tracer_front_desk_booking_is_confirmed_by_the_saga(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]

    resp = await book(
        client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key()
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    appointment_id = body["appointment_id"]
    assert body["status"] == "pending"
    assert body["status_url"] == f"/api/appointments/{appointment_id}"
    assert resp.headers["location"] == body["status_url"]

    result = await saga_result(temporal_env, appointment_id)
    assert result.outcome == "confirmed"
    assert result.reason is None

    got = await client.get(body["status_url"], headers=auth(world.front_desk))
    assert got.status_code == 200, got.text
    appointment = got.json()
    assert appointment["status"] == "confirmed"
    assert appointment["slot_id"] == slot["id"]
    assert appointment["patient_id"] == str(world.patient_id)
    assert "coverage_ref" not in appointment
    assert [(h["from_status"], h["to_status"]) for h in appointment["history"]] == [
        (None, "pending"),
        ("pending", "confirmed"),
    ]

    booked = {s["id"]: s["status"] for s in await _slots(client, world, status="any")}
    assert booked[slot["id"]] == "booked"

    billing = await client.get(
        "/api/billing/records",
        params={"appointment_id": appointment_id},
        headers=auth(world.front_desk),
    )
    assert billing.status_code == 200, billing.text
    assert billing.json()["status"] == "verified"
