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
from datetime import datetime
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment

from services.scheduling.domain.contracts import BookingResult, booking_workflow_id
from tests.conftest import harness_apps
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, _staff, auth

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


# --- API edge cases (Task 3) ---------------------------------------------------


async def test_patient_books_for_themselves(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[1]

    resp = await book(client, world.patient, slot_id=slot["id"], patient_id=None, key=new_key())

    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    assert (await saga_result(temporal_env, appointment_id)).outcome == "confirmed"
    own = await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.patient))
    assert own.status_code == 200, own.text
    assert own.json()["patient_id"] == str(world.patient_id)
    listed = await client.get("/api/appointments", headers=auth(world.patient))
    assert listed.status_code == 200, listed.text
    assert [a["id"] for a in listed.json()] == [appointment_id]


async def test_same_key_same_body_replays_the_same_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    key = new_key()

    first = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=key)
    assert first.status_code == 202, first.text
    appointment_id = first.json()["appointment_id"]
    assert (await saga_result(temporal_env, appointment_id)).outcome == "confirmed"

    # Even though the slot is now booked, the replay answers with the same
    # appointment and its CURRENT status (D-08): no slot check, no new saga.
    again = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=key)
    assert again.status_code == 202, again.text
    assert again.json()["appointment_id"] == appointment_id
    assert again.json()["status"] == "confirmed"

    listed = await client.get(
        "/api/appointments",
        params={"provider_id": str(world.provider_id)},
        headers=auth(world.front_desk),
    )
    assert [a["id"] for a in listed.json()] == [appointment_id]


async def test_same_key_with_a_different_body_is_422(
    client: AsyncClient, booking_world: World, scheduling_worker: object
) -> None:
    world = booking_world
    first_slot, other_slot = (await _slots(client, world))[:2]
    key = new_key()

    first = await book(client, world.front_desk, slot_id=first_slot["id"], patient_id=str(world.patient_id), key=key)
    assert first.status_code == 202, first.text

    changed = await book(client, world.front_desk, slot_id=other_slot["id"], patient_id=str(world.patient_id), key=key)
    assert changed.status_code == 422, changed.text

    coverage_changed = await book(
        client,
        world.front_desk,
        slot_id=first_slot["id"],
        patient_id=str(world.patient_id),
        key=key,
        coverage_ref="SELF-PAY",
    )
    assert coverage_changed.status_code == 422, coverage_changed.text


async def test_second_key_on_a_taken_slot_is_409(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    first = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    assert first.status_code == 202, first.text
    assert (await saga_result(temporal_env, first.json()["appointment_id"])).outcome == "confirmed"

    second = await book(client, world.admin, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())

    assert second.status_code == 409, second.text
    assert second.json()["detail"] == "slot not available"


async def test_pending_slot_with_another_key_is_409_from_the_database_arbiter(
    client: AsyncClient, booking_world: World, committed_scheduling_engine
) -> None:
    """No worker runs, so the first booking stays `pending` and its slot is still
    `available`: the early check passes and `uq_appointments_active_slot`
    refuses the second request."""

    world = booking_world
    slot = (await _slots(client, world))[0]
    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()
    try:
        first = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
        assert first.status_code == 202, first.text
        second = await book(client, world.admin, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    finally:
        app.state.temporal_client = previous

    assert second.status_code == 409, second.text
    assert second.json()["detail"] == "slot not available"


async def test_missing_or_malformed_idempotency_key_is_400(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]

    missing = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id))
    assert missing.status_code == 400, missing.text

    for bad in ("has space", "x" * 129, "semi;colon"):
        malformed = await book(
            client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=bad
        )
        assert malformed.status_code == 400, (bad, malformed.text)

    # Nothing was accepted.
    after = {s["id"]: s["status"] for s in await _slots(client, world, status="any")}
    assert after[slot["id"]] == "available"


async def test_patient_cannot_book_for_another_patient(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]

    resp = await book(client, world.patient, slot_id=slot["id"], patient_id=str(uuid.uuid4()), key=new_key())

    assert resp.status_code == 403, resp.text


async def test_staff_must_name_the_patient(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]

    resp = await book(client, world.front_desk, slot_id=slot["id"], patient_id=None, key=new_key())

    assert resp.status_code == 422, resp.text


async def test_front_desk_of_another_clinic_gets_404(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    other_clinic = await client.post(
        "/api/clinics",
        json={"name": f"Other Clinic {uuid.uuid4().hex[:6]}", "timezone": "UTC"},
        headers=auth(world.admin),
    )
    assert other_clinic.status_code == 201, other_clinic.text
    _, other_front_desk = await _staff(client, world.admin, "front_desk", other_clinic.json()["id"])

    resp = await book(client, other_front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    assert resp.status_code == 404, resp.text

    unknown = await book(
        client, world.front_desk, slot_id=str(uuid.uuid4()), patient_id=str(world.patient_id), key=new_key()
    )
    assert unknown.status_code == 404, unknown.text
    assert unknown.json() == resp.json()


async def test_withdrawn_slot_is_409(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    withdrawn = await client.post(f"/api/slots/{slot['id']}/cancel", headers=auth(world.admin))
    assert withdrawn.status_code == 200, withdrawn.text

    resp = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "slot not available"


async def test_overlapping_appointment_for_the_same_patient_is_409(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    first = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    assert first.status_code == 202, first.text
    assert (await saga_result(temporal_env, first.json()["appointment_id"])).outcome == "confirmed"

    # Another provider's slot in the same clinic at the same time.
    other_provider = uuid.uuid4()
    other_slot = await seed_slot(
        committed_scheduling_engine,
        provider_id=other_provider,
        start=datetime.fromisoformat(slot["start_time"]),
        clinic_id=world.clinic_id,
        department_id=world.department_id,
    )
    try:
        resp = await book(
            client, world.front_desk, slot_id=str(other_slot), patient_id=str(world.patient_id), key=new_key()
        )
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == "patient has an overlapping appointment"
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [other_provider])


async def test_temporal_unreachable_still_accepts_and_defers_the_start(
    client: AsyncClient,
    booking_world: World,
    committed_scheduling_engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.scheduling.api import appointments as appointments_api  # noqa: PLC0415

    world = booking_world
    slot = (await _slots(client, world))[0]
    events = _RecordingLog()
    monkeypatch.setattr(appointments_api, "log", events)
    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()
    try:
        resp = await book(
            client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key()
        )
    finally:
        app.state.temporal_client = previous

    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    assert resp.json()["status"] == "pending"
    assert "booking.workflow_start_deferred" in events.names

    async with committed_scheduling_engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, workflow_started_at FROM appointments WHERE id = :a"),
                {"a": uuid.UUID(appointment_id)},
            )
        ).one()
    assert row.status == "pending"
    assert row.workflow_started_at is None


async def test_started_saga_sets_workflow_started_at(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    resp = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]
    await saga_result(temporal_env, appointment_id)

    async with committed_scheduling_engine.connect() as conn:
        started = await conn.scalar(
            text("SELECT workflow_started_at FROM appointments WHERE id = :a"),
            {"a": uuid.UUID(appointment_id)},
        )
    assert started is not None


async def test_billing_rejection_fails_the_booking_and_frees_the_slot(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    resp = await book(
        client,
        world.front_desk,
        slot_id=slot["id"],
        patient_id=str(world.patient_id),
        key=new_key(),
        coverage_ref="DENY-123",
    )
    assert resp.status_code == 202, resp.text
    appointment_id = resp.json()["appointment_id"]

    result = await saga_result(temporal_env, appointment_id)
    assert result.outcome == "failed"
    assert result.reason == "BillingRejected"

    got = (await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.front_desk))).json()
    assert got["status"] == "failed"
    assert got["failure_reason"] == "BillingRejected"
    after = {s["id"]: s["status"] for s in await _slots(client, world, status="any")}
    assert after[slot["id"]] == "available"
    billing = await client.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=auth(world.front_desk)
    )
    assert billing.json()["status"] == "voided"


async def test_appointment_reads_are_scoped(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    slot = (await _slots(client, world))[0]
    app_temporal = _DownTemporal()

    app = harness_apps()["scheduling"]
    previous = app.state.temporal_client
    app.state.temporal_client = app_temporal
    try:
        resp = await book(client, world.front_desk, slot_id=slot["id"], patient_id=str(world.patient_id), key=new_key())
    finally:
        app.state.temporal_client = previous
    appointment_id = resp.json()["appointment_id"]

    other_clinic = await client.post(
        "/api/clinics",
        json={"name": f"Scope Clinic {uuid.uuid4().hex[:6]}", "timezone": "UTC"},
        headers=auth(world.admin),
    )
    _, other_front_desk = await _staff(client, world.admin, "front_desk", other_clinic.json()["id"])

    assert (await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.admin))).status_code == 200
    hidden = await client.get(f"/api/appointments/{appointment_id}", headers=auth(other_front_desk))
    assert hidden.status_code == 404
    assert (await client.get(f"/api/appointments/{appointment_id}", headers=auth(world.provider))).status_code == 403
    listed = await client.get("/api/appointments", headers=auth(other_front_desk))
    assert listed.status_code == 200
    assert appointment_id not in [a["id"] for a in listed.json()]


class _DownTemporal:
    """Stands in for an unreachable Temporal server: every start fails."""

    async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
        raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")


class _RecordingLog:
    def __init__(self) -> None:
        self.names: list[str] = []

    def _record(self, event: str, **_: Any) -> None:
        self.names.append(event)

    info = warning = error = debug = _record
