"""Non-blocking visit completion and its downstream workflow (plan 02-11;
VISIT-04, BILL-02, D-16).

Completing a visit commits first and returns 200; `visit-completed-{id}` then
runs on `visit-tq` (the `visit_worker` fixture), finalizes the billing record
and records the follow-up and analytics intents. A lost start (Temporal down)
leaves `downstream_started_at` NULL and the sweeper recovers it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import structlog
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment

from libs.db.postgres import get_session_factory
from libs.observability.logging import configure_logging
from services.visit.api import visits as visits_api
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import VisitCompletedResult, visit_completed_workflow_id
from services.visit.downstream import sweep_unstarted
from tests.conftest import harness_apps
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    history_rows,
)


async def _completed_visit(
    client: AsyncClient, mesh: httpx.AsyncClient, vw: VisitWorld
) -> tuple[dict[str, Any], str, httpx.Response]:
    """A visit with a verified billing record, checked in, started and
    completed through the gateway: (visit, appointment_id, complete response)."""
    appointment_id = await confirmed_appointment(vw, with_billing=True, mesh=mesh)
    resp = await check_in(client, vw.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit = resp.json()
    started = await client.post(f"/api/visits/{visit['id']}/start", headers=auth(vw.world.provider))
    assert started.status_code == 200, started.text
    completed = await client.post(
        f"/api/visits/{visit['id']}/complete", headers=auth(vw.world.provider)
    )
    return visit, appointment_id, completed


async def _billing_status(client: AsyncClient, vw: VisitWorld, appointment_id: str) -> str:
    resp = await client.get(
        "/api/billing/records",
        params={"appointment_id": appointment_id},
        headers=auth(vw.world.front_desk),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["status"]


async def _marker(visit_id: str) -> Any:
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()


async def _markers(visit_id: str) -> dict[str, Any]:
    """The visit's settlement markers (plan 02.1-09), read straight from visit_db."""
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT billing_settled_at, billing_outcome, appointment_settled_at, "
                        "appointment_outcome FROM visits WHERE id = :v"
                    ),
                    {"v": visit_id},
                )
            ).one()
            return dict(row._mapping)
    finally:
        await engine.dispose()


async def _appointment_status(client: AsyncClient, vw: VisitWorld, appointment_id: str) -> str:
    resp = await client.get(
        f"/api/appointments/{appointment_id}", headers=auth(vw.world.front_desk)
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["status"]


async def _finalized_rows(appointment_id: str) -> int:
    """The number of finalize moves Billing recorded for the appointment."""
    from services.billing.config import settings as billing_settings  # noqa: PLC0415

    engine = create_async_engine(billing_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text(
                    "SELECT count(*) FROM billing_status_history h "
                    "JOIN billing_records r ON r.id = h.record_id "
                    "WHERE r.appointment_id = :a AND h.to_status = 'finalized'"
                ),
                {"a": appointment_id},
            )
    finally:
        await engine.dispose()


async def _age_completion(visit_id: str, seconds: int) -> None:
    """Make the visit look `seconds` old. The database clock never skips under
    the time-skipping env (Pitfall 8), so the age is set by SQL."""
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE visits SET completed_at = now() - make_interval(secs => :s) WHERE id = :v"),
                {"s": seconds, "v": visit_id},
            )
    finally:
        await engine.dispose()


async def _result(env: WorkflowEnvironment, visit_id: str) -> VisitCompletedResult:
    handle = env.client.get_workflow_handle(
        visit_completed_workflow_id(visit_id), result_type=VisitCompletedResult
    )
    async with env.time_skipping_unlocked():
        return await handle.result()


async def test_complete_returns_200_and_downstream_finalizes_billing(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"

    result = await _result(temporal_env, visit["id"])
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"
    assert await _marker(visit["id"]) is not None
    assert await audit_count(visit["id"], "followup.requested") == 1
    assert await audit_count(visit["id"], "analytics.intent_recorded") == 1


async def test_complete_does_not_wait_for_downstream_work(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """No worker polls `visit-tq`: the completion still answers 200, the
    workflow is RUNNING (started, not awaited) and billing is untouched."""
    app = harness_apps()["visit"]
    app.state.temporal_client = temporal_env.client  # the autouse fixture restores it

    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)

    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
    assert await _billing_status(client, visit_world, appointment_id) == "verified"
    assert await _marker(visit["id"]) is not None
    await handle.terminate(reason="test clean-up")


async def test_lost_trigger_is_recovered_by_the_sweep_exactly_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Temporal down at completion -> 200 with the marker NULL; once the visit
    is past the grace window the sweep starts the workflow (billing ends
    finalized), and a second sweep finds nothing."""
    app = harness_apps()["visit"]
    worker_client = app.state.temporal_client
    from tests.visit.conftest import _DownTemporal  # noqa: PLC0415

    app.state.temporal_client = _DownTemporal()
    try:
        visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    finally:
        app.state.temporal_client = worker_client
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None

    # Not yet past the grace window: the sweep leaves it alone.
    factory = get_session_factory(visit_settings.database_url)
    await sweep_unstarted(factory, temporal_env.client, grace_seconds=30)
    assert await _marker(visit["id"]) is None

    # The database clock never skips (Pitfall 8): age the completion by SQL.
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE visits SET completed_at = completed_at - interval '60 seconds' WHERE id = :v"),
                {"v": visit["id"]},
            )
    finally:
        await engine.dispose()

    assert await sweep_unstarted(factory, temporal_env.client, grace_seconds=30) >= 1
    assert await _marker(visit["id"]) is not None
    result = await _result(temporal_env, visit["id"])
    assert result.billing == "finalized"
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"
    # Second sweep: this visit is no longer a candidate.
    await sweep_unstarted(factory, temporal_env.client, grace_seconds=30)
    assert await audit_count(visit["id"], "visit.billing_finalize_requested") == 1


async def test_repeat_complete_with_null_marker_starts_workflow(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """The first start was lost (Temporal down), so the marker is NULL; a
    repeated complete is a no-op transition but retries the start, and the
    workflow then finalizes billing exactly once."""
    from tests.visit.conftest import _DownTemporal  # noqa: PLC0415

    app = harness_apps()["visit"]
    worker_client = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()  # Temporal down for the first completion only
    try:
        visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    finally:
        app.state.temporal_client = worker_client
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None

    repeat = await client.post(
        f"/api/visits/{visit['id']}/complete", headers=auth(visit_world.world.provider)
    )
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["status"] == "completed"

    assert await _marker(visit["id"]) is not None
    assert await _result(temporal_env, visit["id"]) == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"
    assert await _finalized_rows(appointment_id) == 1
    assert [to for _, to in await history_rows(visit["id"]) if to == "completed"] == ["completed"]
    assert await audit_count(visit["id"], "visit.complete") == 1


async def test_grace_window_skips_recent_completion(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """A completion 5 s old is inside a 30 s grace window: the sweep leaves it
    alone and no workflow exists. With a 1 s grace the same visit is started,
    so the skip is attributable to the grace window alone."""
    visit, _, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert await _marker(visit["id"]) is None
    await _age_completion(visit["id"], 5)

    factory = get_session_factory(visit_settings.database_url)
    await sweep_unstarted(factory, temporal_env.client, grace_seconds=30)
    assert await _marker(visit["id"]) is None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    with pytest.raises(RPCError) as exc_info:
        await handle.describe()
    assert exc_info.value.status == RPCStatusCode.NOT_FOUND

    # Positive control: past a 1 s grace the sweep starts it (no worker polls visit-tq).
    await sweep_unstarted(factory, temporal_env.client, grace_seconds=1)
    assert await _marker(visit["id"]) is not None
    assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
    await handle.terminate(reason="test clean-up")


async def test_repeat_complete_after_success_is_noop_single_run(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Completing again after the downstream workflow finished changes nothing:
    same run, same marker, one finalization, one completed history row."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    result = await _result(temporal_env, visit["id"])
    assert result.billing == "finalized"
    marker = await _marker(visit["id"])
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    first_run = (await handle.describe()).run_id

    repeat = await client.post(
        f"/api/visits/{visit['id']}/complete", headers=auth(visit_world.world.provider)
    )
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["status"] == "completed"

    assert await _marker(visit["id"]) == marker
    assert (await handle.describe()).run_id == first_run
    assert await _finalized_rows(appointment_id) == 1
    assert [to for _, to in await history_rows(visit["id"]) if to == "completed"] == ["completed"]
    assert await audit_count(visit["id"], "visit.billing_finalize_requested") == 1


async def test_concurrent_sweeps_start_one_run_one_finalization(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Two sweeps racing over the same NULL-marker visit share the deterministic
    workflow id (USE_EXISTING), so there is one run and one finalization."""
    from tests.visit.conftest import _DownTemporal  # noqa: PLC0415

    app = harness_apps()["visit"]
    worker_client = app.state.temporal_client
    app.state.temporal_client = _DownTemporal()  # Temporal down for the completion only
    try:
        visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    finally:
        app.state.temporal_client = worker_client
    assert completed.status_code == 200, completed.text
    assert await _marker(visit["id"]) is None
    await _age_completion(visit["id"], 60)

    factory = get_session_factory(visit_settings.database_url)
    results = await asyncio.gather(
        sweep_unstarted(factory, temporal_env.client, grace_seconds=30),
        sweep_unstarted(factory, temporal_env.client, grace_seconds=30),
    )
    # Both sweeps may count this visit (the loser attaches and its conditional
    # marker UPDATE is a no-op), and other stale rows may be counted too, so the
    # count is only a lower bound; the stable run id is the one-run proof.
    assert sum(results) >= 1
    assert await _marker(visit["id"]) is not None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    first_run = (await handle.describe()).run_id

    assert (await _result(temporal_env, visit["id"])).billing == "finalized"
    assert (await handle.describe()).run_id == first_run
    assert await _finalized_rows(appointment_id) == 1
    assert await audit_count(visit["id"], "visit.billing_finalize_requested") == 1
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"


async def test_voided_billing_reports_not_finalizable_and_intents_still_recorded(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Billing voided before completion cannot be finalized (terminal): the
    billing branch reports the business failure while the follow-up and
    analytics intents are still recorded."""
    from tests.billing.conftest import service_token  # noqa: PLC0415
    from tests.billing.conftest import transition as billing_transition  # noqa: PLC0415

    appointment_id = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)
    token = await service_token(mesh, "scheduling")
    voided = await billing_transition(
        mesh, token, appointment_id, "voided", reason_code="booking_compensated"
    )
    assert voided.status_code == 200, voided.text
    assert voided.json()["status"] == "voided"

    resp = await check_in(client, visit_world.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit = resp.json()
    started = await client.post(
        f"/api/visits/{visit['id']}/start", headers=auth(visit_world.world.provider)
    )
    assert started.status_code == 200, started.text
    completed = await client.post(
        f"/api/visits/{visit['id']}/complete", headers=auth(visit_world.world.provider)
    )
    assert completed.status_code == 200, completed.text

    assert await _result(temporal_env, visit["id"]) == VisitCompletedResult(
        outcome="completed",
        billing="skipped:BillingNotFinalizable",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert await _billing_status(client, visit_world, appointment_id) == "voided"
    assert await _finalized_rows(appointment_id) == 0
    assert await audit_count(visit["id"], "visit.billing_finalize_requested") == 0
    assert await audit_count(visit["id"], "followup.requested") == 1
    assert await audit_count(visit["id"], "analytics.intent_recorded") == 1
    assert await _marker(visit["id"]) is not None


async def test_deferred_start_logs_downstream_start_deferred(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Temporal down at completion: 200 plus exactly one warning event carrying
    the visit id and the error TYPE only (T-02-58 ids-only logging)."""
    configure_logging()
    # A logger first used in an earlier test stays bound to that test's stdout.
    monkeypatch.setattr(visits_api, "log", structlog.get_logger("test-visit-deferred"))
    capsys.readouterr()

    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None

    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    deferred = [e for e in events if e.get("event") == "visit.downstream_start_deferred"]
    assert len(deferred) == 1
    assert deferred[0]["level"] == "warning"
    assert deferred[0]["visit_id"] == visit["id"]
    assert deferred[0]["error"] == "RPCError"
    serialized = json.dumps(deferred[0])
    assert appointment_id not in serialized
    assert "temporal unavailable" not in serialized


async def test_tracer_completion_completes_the_appointment(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """M1 tracer (plan 02.1-09): completing the visit completes its appointment
    in Scheduling through the fourth, patched branch, and both settlement
    markers land in visit_db."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text

    result = await _result(temporal_env, visit["id"])
    assert result.appointment == "completed"
    assert result.billing == "finalized"
    assert await _appointment_status(client, visit_world, appointment_id) == "completed"

    markers = await _markers(visit["id"])
    assert markers["billing_settled_at"] is not None
    assert markers["billing_outcome"] == "finalized"
    assert markers["appointment_settled_at"] is not None
    assert markers["appointment_outcome"] == "completed"
    assert await audit_count(visit["id"], "visit.appointment_completed") == 1
