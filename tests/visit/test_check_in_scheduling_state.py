"""Visit check-in changes the appointment in Scheduling (plan 02.1-06; mentor item M1).

Visit's public check-in used to read the appointment and write only its own
visit row, so the appointment stayed `confirmed` and cancel/reschedule kept
working for a patient already in the building. Now Visit asks Scheduling's
internal check-in FIRST (which enforces the window and arbitrates the cancel
race) and only then inserts the visit. These tests drive the public route
through the gateway and read Scheduling's state back through its public API and
its database.

Slots are seeded with SQL by `confirmed_appointment`; every test under
tests/visit runs with the opening window widened (autouse) unless it asks for
`real_checkin_window`.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from services.visit import references
from services.visit.api import visits as visits_api
from tests.conftest import RequestRecorder, degraded_mesh, service_base_url
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    history_rows,
    visit_rows,
)

pytestmark = pytest.mark.integration


async def get_appointment(client: AsyncClient, tokens: dict, appointment_id: str) -> dict[str, Any]:
    resp = await client.get(f"/api/appointments/{appointment_id}", headers=auth(tokens))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def slot_status(vw: VisitWorld, appointment_id: str) -> str:
    async with vw.engine.connect() as conn:
        return await conn.scalar(
            text(
                "SELECT s.status FROM provider_slots s JOIN appointments a ON a.slot_id = s.id "
                "WHERE a.id = :a"
            ),
            {"a": appointment_id},
        )


async def cancel(client: AsyncClient, tokens: dict, appointment_id: str) -> httpx.Response:
    return await client.post(
        f"/api/appointments/{appointment_id}/cancel", json={}, headers=auth(tokens)
    )


# --- Task 1 tracer -------------------------------------------------------------


async def test_tracer_check_in_makes_the_appointment_checked_in_and_cancel_is_refused(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)

    resp = await check_in(client, world.front_desk, appointment_id)

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "checked_in"

    appointment = await get_appointment(client, world.front_desk, appointment_id)
    assert appointment["status"] == "checked_in"
    assert [(h["from_status"], h["to_status"]) for h in appointment["history"]] == [
        ("confirmed", "checked_in")
    ]

    refused = await cancel(client, world.front_desk, appointment_id)

    assert refused.status_code == 409, refused.text
    assert (await get_appointment(client, world.front_desk, appointment_id))["status"] == "checked_in"
    assert await slot_status(visit_world, appointment_id) == "booked"
    assert await visit_rows(appointment_id) == 1
    assert await audit_count(appointment_id, "appointment.check_in") == 1
    assert uuid.UUID(resp.json()["appointment_id"])  # the visit is bound to this appointment


# --- Task 2 helpers ----------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


async def near_appointment(vw: VisitWorld, *, start: datetime, status: str = "confirmed") -> str:
    """An appointment at `start` (near now) on a throwaway provider, so it can
    never collide with the world's generated slots; cleaned up with the world."""
    provider_id = uuid.uuid4()
    vw.provider_ids.append(provider_id)
    return await confirmed_appointment(vw, status=status, provider_id=provider_id, start=start)


async def scheduling_history(vw: VisitWorld, appointment_id: str) -> list[tuple[str | None, str]]:
    async with vw.engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT from_status, to_status FROM appointment_status_history "
                "WHERE appointment_id = :a ORDER BY created_at, id"
            ),
            {"a": appointment_id},
        )
        return [(r.from_status, r.to_status) for r in rows]


async def appointment_status(vw: VisitWorld, appointment_id: str) -> str:
    async with vw.engine.connect() as conn:
        return await conn.scalar(
            text("SELECT status FROM appointments WHERE id = :a"), {"a": appointment_id}
        )


async def scheduling_check_in_directly(mesh: httpx.AsyncClient, vw: VisitWorld, appointment_id: str):
    """What Visit's first step does, without the second: the Visit service token
    calls Scheduling's internal check-in (the crash-between-steps state)."""
    secret = load_client_secret(os.environ["VISIT_CLIENT_SECRET_PATH"])
    tokens = ServiceTokenProvider("visit", secret, service_base_url("identity"), http=mesh)
    token = await tokens.get("scheduling")
    return await mesh.post(
        f"{service_base_url('scheduling')}/internal/appointments/{appointment_id}/check-in"
        f"?clinic_id={vw.clinic_id}",
        headers={"Authorization": f"Bearer {token}"},
    )


# --- window refusals through the public route (the REAL 60-minute window) -----------


async def test_a_slot_three_hours_away_is_refused_as_window_not_open(
    client: AsyncClient, visit_world: VisitWorld, real_checkin_window: None
) -> None:
    appointment_id = await near_appointment(visit_world, start=_now() + timedelta(hours=3))

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json() == {"detail": "check-in window not open"}
    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == "confirmed"
    assert await scheduling_history(visit_world, appointment_id) == []
    assert await audit_count(appointment_id, "appointment.check_in") == 0


async def test_a_slot_that_ended_an_hour_ago_is_refused_as_window_closed(
    client: AsyncClient, visit_world: VisitWorld, real_checkin_window: None
) -> None:
    appointment_id = await near_appointment(visit_world, start=_now() - timedelta(minutes=90))

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json() == {"detail": "check-in window closed"}
    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == "confirmed"


async def test_a_slot_inside_the_real_window_checks_in(
    client: AsyncClient, visit_world: VisitWorld, real_checkin_window: None
) -> None:
    """Positive control for the two refusals above: same route, same real
    window, a slot that starts in 30 minutes is accepted."""
    appointment_id = await near_appointment(visit_world, start=_now() + timedelta(minutes=30))

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 201, resp.text
    assert await appointment_status(visit_world, appointment_id) == "checked_in"


# --- other states, upstream trouble, error mapping ----------------------------------


@pytest.mark.parametrize("appointment_status_", ["completed", "no_show", "cancelled"])
async def test_a_finished_or_cancelled_appointment_is_refused_as_not_confirmed(
    client: AsyncClient, visit_world: VisitWorld, appointment_status_: str
) -> None:
    appointment_id = await confirmed_appointment(visit_world, status=appointment_status_)

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json() == {"detail": "appointment not confirmed"}
    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == appointment_status_


async def test_scheduling_unreachable_is_503_with_no_visit_and_the_appointment_untouched(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(visit_world)

    async with degraded_mesh("scheduling"):
        resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 503, resp.text
    assert resp.json() == {"detail": "scheduling service unavailable"}
    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == "confirmed"


async def test_identity_unreachable_is_503_with_no_visit_and_the_appointment_untouched(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    # Warm Visit's token-verification (JWKS) cache so only the service-token
    # mint, which the degraded mesh forces to run afresh, needs Identity.
    warm = await client.get(
        "/api/visits", params={"limit": 1}, headers=auth(world.front_desk)
    )
    assert warm.status_code == 200, warm.text

    async with degraded_mesh("identity"):
        resp = await check_in(client, world.front_desk, appointment_id)

    assert resp.status_code == 503, resp.text
    assert resp.json() == {"detail": "scheduling service unavailable"}
    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == "confirmed"


async def test_an_unknown_scheduling_refusal_is_reported_as_not_confirmed(
    client: AsyncClient, visit_world: VisitWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    appointment_id = await confirmed_appointment(visit_world)

    async def refuse(*args: Any, **kwargs: Any) -> None:
        raise references.CheckInRefused("unexpected internal wording")

    monkeypatch.setattr(visits_api, "check_in_appointment", refuse)

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)

    assert resp.status_code == 409, resp.text
    assert resp.json() == {"detail": "appointment not confirmed"}  # upstream text never leaks
    assert await visit_rows(appointment_id) == 0


# --- cross-service retry convergence -----------------------------------------------


async def test_checked_in_in_scheduling_but_no_visit_converges_on_the_next_request(
    client: AsyncClient, mesh: httpx.AsyncClient, visit_world: VisitWorld
) -> None:
    """The crash window between the two steps: Scheduling already says
    checked_in, no visit exists. The repeat creates the visit (201), a further
    repeat returns it (200), and nothing is written twice."""
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    direct = await scheduling_check_in_directly(mesh, visit_world, appointment_id)
    assert direct.status_code == 200 and direct.json()["status"] == "checked_in"
    assert await visit_rows(appointment_id) == 0

    first = await check_in(client, world.front_desk, appointment_id)
    second = await check_in(client, world.front_desk, appointment_id)

    assert first.status_code == 201, first.text
    assert second.status_code == 200, second.text
    assert first.json()["id"] == second.json()["id"]
    assert await visit_rows(appointment_id) == 1
    assert await scheduling_history(visit_world, appointment_id) == [("confirmed", "checked_in")]
    assert await audit_count(first.json()["id"], "visit.check_in") == 1
    assert await audit_count(appointment_id, "appointment.check_in") == 1


async def test_a_failed_visit_insert_leaves_the_appointment_checked_in_and_the_retry_completes_it(
    client: AsyncClient, visit_world: VisitWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure injection in the real route: the visit insert raises after
    Scheduling transitioned. No visit exists, the appointment is checked_in (so
    cancel is already refused), and the repeat request creates the visit."""
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    real_create = visits_api.visit_repo.create_visit

    async def failing_create(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("visit database unavailable")

    monkeypatch.setattr(visits_api.visit_repo, "create_visit", failing_create)
    with pytest.raises(RuntimeError, match="visit database unavailable"):
        await check_in(client, world.front_desk, appointment_id)
    monkeypatch.setattr(visits_api.visit_repo, "create_visit", real_create)

    assert await visit_rows(appointment_id) == 0
    assert await appointment_status(visit_world, appointment_id) == "checked_in"
    assert (await cancel(client, world.front_desk, appointment_id)).status_code == 409

    retry = await check_in(client, world.front_desk, appointment_id)

    assert retry.status_code == 201, retry.text
    assert await visit_rows(appointment_id) == 1
    assert await scheduling_history(visit_world, appointment_id) == [("confirmed", "checked_in")]


async def test_a_repeat_after_the_visit_exists_never_asks_scheduling(
    client: AsyncClient, visit_world: VisitWorld, request_recorder: RequestRecorder
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    first = await check_in(client, world.front_desk, appointment_id)
    assert first.status_code == 201, first.text
    check_ins = [p for p in request_recorder.paths("/internal/appointments") if p.endswith("/check-in")]
    assert len(check_ins) == 1

    async with degraded_mesh("scheduling", event_hooks=request_recorder.event_hooks):
        again = await check_in(client, world.front_desk, appointment_id)

    assert again.status_code == 200, again.text
    assert again.json()["id"] == first.json()["id"]
    after = [p for p in request_recorder.paths("/internal/appointments") if p.endswith("/check-in")]
    assert after == check_ins  # Scheduling (down, here) was not called a second time


# --- single winner under concurrency ------------------------------------------------


async def test_twenty_concurrent_check_ins_make_one_visit_and_one_scheduling_transition(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(visit_world)
    front_desk = visit_world.world.front_desk

    responses = await asyncio.gather(
        *(check_in(client, front_desk, appointment_id) for _ in range(20))
    )

    assert sorted(r.status_code for r in responses) == [200] * 19 + [201], [r.text for r in responses]
    visit_id = responses[0].json()["id"]
    assert {r.json()["id"] for r in responses} == {visit_id}
    assert await visit_rows(appointment_id) == 1
    assert await history_rows(visit_id) == [(None, "checked_in")]
    assert await scheduling_history(visit_world, appointment_id) == [("confirmed", "checked_in")]
    assert await appointment_status(visit_world, appointment_id) == "checked_in"
    assert await audit_count(visit_id, "visit.check_in") == 1
    assert await audit_count(appointment_id, "appointment.check_in") == 1


# --- M1 completeness: reschedule is refused too -------------------------------------


async def test_reschedule_is_refused_after_check_in(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    assert (await check_in(client, world.front_desk, appointment_id)).status_code == 201

    resp = await client.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": str(uuid.uuid4())},
        headers=auth(world.front_desk),
    )

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "appointment already checked in"
    assert await appointment_status(visit_world, appointment_id) == "checked_in"


# --- S3 (plan 02.1-07): a failed audit AFTER the commit is not a failed check-in ----------


def _failing_audit(monkeypatch: pytest.MonkeyPatch, module: Any, action: str) -> list[str]:
    """`write_audit` in `module` raises for `action` only; returns the list the
    failing calls append their action to."""
    real = module.write_audit
    failed: list[str] = []

    async def flaky(mongo: Any, **kwargs: Any) -> None:
        if kwargs.get("action") == action:
            failed.append(action)
            raise RuntimeError("audit sink down")
        await real(mongo, **kwargs)

    monkeypatch.setattr(module, "write_audit", flaky)
    return failed


async def test_a_failed_scheduling_audit_does_not_fail_the_check_in(
    client: AsyncClient, visit_world: VisitWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scheduling's `appointment.check_in` audit is written after its commit; a
    Mongo failure there must not turn an applied transition into a 500 that the
    patient has to repeat."""
    from services.scheduling.api import internal  # noqa: PLC0415

    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    failed = _failing_audit(monkeypatch, internal, "appointment.check_in")

    resp = await check_in(client, world.front_desk, appointment_id)

    assert resp.status_code == 201, resp.text
    assert failed == ["appointment.check_in"]
    assert (await get_appointment(client, world.front_desk, appointment_id))["status"] == "checked_in"
    assert await visit_rows(appointment_id) == 1
    # The transition stays durable in the history table without its audit entry.
    assert await scheduling_history(visit_world, appointment_id) == [("confirmed", "checked_in")]
    assert await audit_count(appointment_id, "appointment.check_in") == 0
    assert await audit_count(resp.json()["id"], "visit.check_in") == 1


async def test_a_failed_visit_audit_does_not_fail_the_check_in(
    client: AsyncClient, visit_world: VisitWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Visit's own `visit.check_in` audit follows its commit; a failure there is
    logged and the 201 still reports the created visit."""
    world = visit_world.world
    appointment_id = await confirmed_appointment(visit_world)
    failed = _failing_audit(monkeypatch, visits_api, "visit.check_in")

    resp = await check_in(client, world.front_desk, appointment_id)

    assert resp.status_code == 201, resp.text
    assert failed == ["visit.check_in"]
    assert await visit_rows(appointment_id) == 1
    assert await audit_count(resp.json()["id"], "visit.check_in") == 0
    # Scheduling's own entry is unaffected.
    assert await audit_count(appointment_id, "appointment.check_in") == 1
