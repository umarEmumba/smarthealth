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
    """Run an advance directly for `slot_id` (no cancel needed). Not wrapped in
    `time_skipping_unlocked()`: `execute_workflow` on the test client already
    skips time while it waits (HANDOFF 02-08 gotcha)."""
    source = f"test-{uuid.uuid4().hex}"
    return await env.client.execute_workflow(
        WAITLIST_ADVANCE_WORKFLOW,
        WaitlistAdvanceInput(slot_id=slot_id, clinic_id=str(world.clinic_id), source_workflow_id=source),
        id=waitlist_advance_workflow_id(source),
        task_queue=SCHEDULING_TASK_QUEUE,
        result_type=WaitlistAdvanceResult,
    )


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


# --- Task 2: matching, failures, lost race, reschedule -------------------------------


async def test_fifo_oldest_waiting_entry_is_booked_first(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    patient_b, tokens_b = await add_patient(client, world)
    patient_c, _ = await add_patient(client, world)
    older = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    newer = await joined(client, tokens_b, {"provider_id": str(world.provider_id)})
    booked_c = await book_for(client, temporal_env, world, slot["id"], patient_c)

    result = await cancel_and_advance(client, temporal_env, world, booked_c)

    assert result.outcome == "booked" and result.booked_entry_id == older["id"]
    assert (await get_entry(client, world, older["id"]))["status"] == "booked"
    still = await get_entry(client, world, newer["id"])
    assert still["status"] == "waiting" and still["attempts"] == 0
    assert still["patient_id"] == patient_b


async def test_window_that_excludes_the_slot_is_skipped(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    _patient_b, tokens_b = await add_patient(client, world)
    patient_c, _ = await add_patient(client, world)
    now = datetime.now(timezone.utc)
    narrow = await joined(
        client,
        world.patient,
        {
            "provider_id": str(world.provider_id),
            "window_start": (now + timedelta(hours=1)).isoformat(),
            "window_end": (now + timedelta(hours=2)).isoformat(),
        },
    )
    open_ended = await joined(client, tokens_b, {"provider_id": str(world.provider_id)})
    booked_c = await book_for(client, temporal_env, world, slot["id"], patient_c)

    result = await cancel_and_advance(client, temporal_env, world, booked_c)

    assert result.outcome == "booked" and result.booked_entry_id == open_ended["id"]
    assert (await get_entry(client, world, narrow["id"]))["status"] == "waiting"
    assert (await get_entry(client, world, open_ended["id"]))["status"] == "booked"


async def test_billing_rejection_fails_the_entry_and_books_the_next(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    patient_b, tokens_b = await add_patient(client, world)
    patient_c, _ = await add_patient(client, world)
    # Billing's deterministic failing prefix, built at runtime.
    denied = "-".join(["DENY", "0001"])
    rejected = await joined(
        client, world.patient, {"provider_id": str(world.provider_id), "coverage_ref": denied}
    )
    assert "coverage_ref" not in rejected
    self_pay = await joined(client, tokens_b, {"provider_id": str(world.provider_id)})
    booked_c = await book_for(client, temporal_env, world, slot["id"], patient_c)

    result = await cancel_and_advance(client, temporal_env, world, booked_c)

    assert result.outcome == "booked"
    assert result.booked_entry_id == self_pay["id"]
    assert result.attempts == 2
    failed = await get_entry(client, world, rejected["id"])
    assert failed["status"] == "failed"
    assert failed["failure_reason"] == "BillingRejected"
    failed_appointment = await get_appointment(client, world, failed["matched_appointment_id"])
    assert failed_appointment["status"] == "failed"
    assert failed_appointment["failure_reason"] == "BillingRejected"
    billing = await client.get(
        "/api/billing/records",
        params={"appointment_id": failed["matched_appointment_id"]},
        headers=auth(world.front_desk),
    )
    assert billing.status_code == 200, billing.text
    assert billing.json()["status"] == "voided"
    won = await get_entry(client, world, self_pay["id"])
    assert won["status"] == "booked"
    appointment = await get_appointment(client, world, won["booked_appointment_id"])
    assert appointment["status"] == "confirmed" and appointment["patient_id"] == patient_b


async def test_patient_overlap_fails_the_entry_without_an_appointment(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    from tests.scheduling.conftest import cleanup_scheduling_rows  # noqa: PLC0415
    from tests.scheduling.test_cancel import insert_appointment  # noqa: PLC0415

    world = booking_world
    slot = (await day_slots(client, world))[0]
    patient_b, tokens_b = await add_patient(client, world)
    patient_c, _ = await add_patient(client, world)
    # The world's patient already holds a confirmed appointment (another
    # provider) at exactly the freed slot's time.
    other_provider = uuid.uuid4()
    await insert_appointment(
        committed_scheduling_engine,
        world,
        status="confirmed",
        start=datetime.fromisoformat(slot["start_time"]),
        provider_id=other_provider,
    )
    try:
        overlapping = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
        nxt = await joined(client, tokens_b, {"provider_id": str(world.provider_id)})
        booked_c = await book_for(client, temporal_env, world, slot["id"], patient_c)

        result = await cancel_and_advance(client, temporal_env, world, booked_c)

        assert result.outcome == "booked" and result.booked_entry_id == nxt["id"]
        assert result.attempts == 2
        failed = await get_entry(client, world, overlapping["id"])
        assert failed["status"] == "failed"
        assert failed["failure_reason"] == "PatientOverlap"
        on_slot = await appointments_on_slot(committed_scheduling_engine, slot["id"])
        assert [(str(p), s) for _, p, s, _ in on_slot] == [
            (patient_c, "cancelled"),
            (patient_b, "confirmed"),
        ]
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [other_provider])


async def test_slot_lost_at_the_claim_leaves_the_entry_waiting(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    committed_scheduling_engine: AsyncEngine,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]
    patient_c, _ = await add_patient(client, world)
    entry = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    # A competing pending booking already owns the slot; the slot row itself
    # still says `available`, so the claim reaches the database arbiter.
    competitor = uuid.uuid4()
    start = datetime.fromisoformat(slot["start_time"])
    async with committed_scheduling_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
                "slot_start, slot_end, status, created_by, workflow_started_at) "
                "VALUES (:id, :c, :pt, :pr, :s, :st, :en, 'pending', :by, now())"
            ),
            {
                "id": competitor,
                "c": world.clinic_id,
                "pt": uuid.UUID(patient_c),
                "pr": world.provider_id,
                "s": uuid.UUID(slot["id"]),
                "st": start,
                "en": start + timedelta(minutes=30),
                "by": uuid.uuid4(),
            },
        )

    result = await run_advance(temporal_env, world, slot["id"])

    assert result.outcome == "slot_taken"
    after = await get_entry(client, world, entry["id"])
    assert after["status"] == "waiting"
    assert after["attempts"] == 0
    assert after["matched_appointment_id"] is None
    on_slot = await appointments_on_slot(committed_scheduling_engine, slot["id"])
    assert [a[0] for a in on_slot] == [competitor]


async def test_no_waiting_entry_is_no_match_and_the_slot_stays_free(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    slot = (await day_slots(client, world))[0]

    result = await run_advance(temporal_env, world, slot["id"])

    assert result.outcome == "no_match" and result.attempts == 0
    assert await slot_status(client, world, slot["id"]) == "available"


async def test_reschedule_gives_the_old_slot_to_the_waitlist(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    s1, s2 = (await day_slots(client, world))[:2]
    patient_b, _ = await add_patient(client, world)
    moving = await book_for(client, temporal_env, world, s1["id"], patient_b)
    entry = await joined(
        client,
        world.patient,
        {
            "provider_id": str(world.provider_id),
            "window_start": s1["start_time"],
            "window_end": s1["end_time"],
        },
    )

    resp = await client.post(
        f"/api/appointments/{moving['id']}/reschedule",
        json={"new_slot_id": s2["id"]},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 202, resp.text
    reschedule_id = reschedule_workflow_id(moving["id"], moving["version"])
    assert (await result_of(temporal_env, reschedule_id, ChangeResult)).outcome == "rescheduled"
    result = await result_of(
        temporal_env, waitlist_advance_workflow_id(reschedule_id), WaitlistAdvanceResult
    )

    assert result.outcome == "booked" and result.booked_entry_id == entry["id"]
    booked = await get_entry(client, world, entry["id"])
    appointment = await get_appointment(client, world, booked["booked_appointment_id"])
    assert appointment["slot_id"] == s1["id"] and appointment["status"] == "confirmed"
    assert (await get_appointment(client, world, moving["id"]))["slot_id"] == s2["id"]
    assert await slot_status(client, world, s1["id"]) == "booked"
    assert await slot_status(client, world, s2["id"]) == "booked"


# --- Task 2: API rules ---------------------------------------------------------------


async def other_clinic_front_desk(client: AsyncClient, world: World) -> dict[str, Any]:
    from tests.scheduling.seed import _staff  # noqa: PLC0415

    clinic = await client.post(
        "/api/clinics",
        json={"name": f"Other Clinic {uuid.uuid4().hex[:6]}", "timezone": "Asia/Karachi"},
        headers=auth(world.admin),
    )
    assert clinic.status_code == 201, clinic.text
    _user_id, tokens = await _staff(client, world.admin, "front_desk", clinic.json()["id"])
    return tokens


async def set_entry_status(engine: AsyncEngine, entry_id: str, status: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE waitlist_entries SET status = :st WHERE id = :e"),
            {"st": status, "e": uuid.UUID(entry_id)},
        )


async def test_patient_cannot_join_for_another_patient(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    other, _ = await add_patient(client, world)
    resp = await join(
        client, world.patient, {"provider_id": str(world.provider_id), "patient_id": other}
    )
    assert resp.status_code == 403, resp.text
    # Their own id is fine.
    resp = await join(
        client,
        world.patient,
        {"provider_id": str(world.provider_id), "patient_id": str(world.patient_id)},
    )
    assert resp.status_code == 201, resp.text


async def test_provider_role_is_refused(client: AsyncClient, booking_world: World) -> None:
    world = booking_world
    body = {"provider_id": str(world.provider_id), "patient_id": str(world.patient_id)}
    assert (await join(client, world.provider, body)).status_code == 403
    assert (await client.get("/api/waitlist", headers=auth(world.provider))).status_code == 403


async def test_front_desk_of_another_clinic_gets_not_found(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    stranger = await other_clinic_front_desk(client, world)
    body = {"provider_id": str(world.provider_id), "patient_id": str(world.patient_id)}
    assert (await join(client, stranger, body)).status_code == 404
    entry = await joined(client, world.front_desk, body)
    assert (
        await client.get(f"/api/waitlist/{entry['id']}", headers=auth(stranger))
    ).status_code == 404
    assert (
        await client.post(f"/api/waitlist/{entry['id']}/cancel", headers=auth(stranger))
    ).status_code == 404
    listed = await client.get(
        "/api/waitlist", params={"provider_id": str(world.provider_id)}, headers=auth(stranger)
    )
    assert listed.status_code == 200 and listed.json() == []
    # A missing entry is the same 404.
    assert (
        await client.get(f"/api/waitlist/{uuid.uuid4()}", headers=auth(world.front_desk))
    ).status_code == 404


async def test_second_active_entry_for_the_same_provider_is_conflict(
    client: AsyncClient, booking_world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    world = booking_world
    body = {"provider_id": str(world.provider_id)}
    first = await joined(client, world.patient, body)
    resp = await join(client, world.patient, body)
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "patient already on this provider's waitlist"
    # Staff hit the same arbiter for the same patient.
    staff = await join(client, world.front_desk, body | {"patient_id": str(world.patient_id)})
    assert staff.status_code == 409
    # A matched entry is still active.
    await set_entry_status(committed_scheduling_engine, first["id"], "matched")
    assert (await join(client, world.patient, body)).status_code == 409
    # Once it leaves the active states the patient may join again.
    await set_entry_status(committed_scheduling_engine, first["id"], "failed")
    assert (await join(client, world.patient, body)).status_code == 201


async def test_invalid_windows_and_missing_patient_are_rejected(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    now = datetime.now(timezone.utc)
    base = {"provider_id": str(world.provider_id)}
    inverted = base | {
        "window_start": (now + timedelta(hours=5)).isoformat(),
        "window_end": (now + timedelta(hours=3)).isoformat(),
    }
    assert (await join(client, world.patient, inverted)).status_code == 422
    past = base | {"window_end": (now - timedelta(hours=1)).isoformat()}
    assert (await join(client, world.patient, past)).status_code == 422
    naive = base | {"window_start": "2030-01-01T09:00:00"}
    assert (await join(client, world.patient, naive)).status_code == 422
    # Staff must name the patient.
    assert (await join(client, world.front_desk, base)).status_code == 422
    # An unknown provider is the identical 404.
    unknown = {"provider_id": str(uuid.uuid4()), "patient_id": str(world.patient_id)}
    assert (await join(client, world.front_desk, unknown)).status_code == 404


async def test_cancel_waiting_entry_then_repeat_is_a_no_op(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    entry = await joined(client, world.patient, {"provider_id": str(world.provider_id)})

    first = await client.post(f"/api/waitlist/{entry['id']}/cancel", headers=auth(world.patient))
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "cancelled"
    again = await client.post(
        f"/api/waitlist/{entry['id']}/cancel", headers=auth(world.front_desk)
    )
    assert again.status_code == 200 and again.json()["status"] == "cancelled"
    # Cancelled entries leave the active set: the patient may join again.
    assert (await join(client, world.patient, {"provider_id": str(world.provider_id)})).status_code == 201


async def test_cancel_matched_or_booked_entry_is_conflict(
    client: AsyncClient, booking_world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    world = booking_world
    entry = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    url = f"/api/waitlist/{entry['id']}/cancel"

    await set_entry_status(committed_scheduling_engine, entry["id"], "matched")
    matched = await client.post(url, headers=auth(world.front_desk))
    assert matched.status_code == 409 and matched.json()["detail"] == "match in progress"

    for final in ("booked", "failed"):
        await set_entry_status(committed_scheduling_engine, entry["id"], final)
        resp = await client.post(url, headers=auth(world.front_desk))
        assert resp.status_code == 409, final
        assert resp.json()["detail"] == "waitlist entry not cancellable"
    assert (await get_entry(client, world, entry["id"]))["status"] == "failed"


async def test_patient_sees_only_their_own_entries(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    _other, other_tokens = await add_patient(client, world)
    mine = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    theirs = await joined(client, other_tokens, {"provider_id": str(world.provider_id)})

    listed = await client.get("/api/waitlist", headers=auth(world.patient))
    assert listed.status_code == 200
    assert [e["id"] for e in listed.json()] == [mine["id"]]
    assert all("coverage_ref" not in e for e in listed.json())
    assert (
        await client.get(f"/api/waitlist/{theirs['id']}", headers=auth(world.patient))
    ).status_code == 404
    assert (
        await client.post(f"/api/waitlist/{theirs['id']}/cancel", headers=auth(world.patient))
    ).status_code == 404


async def test_front_desk_lists_by_provider_and_status_in_their_clinic(
    client: AsyncClient, booking_world: World
) -> None:
    world = booking_world
    _other, other_tokens = await add_patient(client, world)
    first = await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    second = await joined(client, other_tokens, {"provider_id": str(world.provider_id)})
    cancelled = await client.post(
        f"/api/waitlist/{second['id']}/cancel", headers=auth(world.front_desk)
    )
    assert cancelled.status_code == 200

    def ids(resp: Any) -> list[str]:
        assert resp.status_code == 200, resp.text
        return [e["id"] for e in resp.json()]

    params = {"provider_id": str(world.provider_id)}
    assert ids(await client.get("/api/waitlist", params=params, headers=auth(world.front_desk))) == [
        first["id"],
        second["id"],
    ]
    waiting = await client.get(
        "/api/waitlist", params=params | {"status": "waiting"}, headers=auth(world.front_desk)
    )
    assert ids(waiting) == [first["id"]]
    other_provider = await client.get(
        "/api/waitlist", params={"provider_id": str(uuid.uuid4())}, headers=auth(world.front_desk)
    )
    assert ids(other_provider) == []
    bad = await client.get(
        "/api/waitlist", params={"status": "bogus"}, headers=auth(world.front_desk)
    )
    assert bad.status_code == 422
