"""Non-blocking visit completion and its downstream workflow (plan 02-11;
VISIT-04, BILL-02, D-16).

Completing a visit commits first and returns 200; `visit-completed-{id}` then
runs on `visit-tq` (the `visit_worker` fixture), finalizes the billing record
and records the follow-up and analytics intents. A lost start (Temporal down)
leaves `downstream_started_at` NULL and the sweeper recovers it.
"""

from __future__ import annotations

from typing import Any

import httpx
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment

from libs.db.postgres import get_session_factory
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import VisitCompletedResult, visit_completed_workflow_id
from services.visit.downstream import sweep_unstarted
from tests.conftest import harness_apps
from tests.scheduling.seed import auth
from tests.visit.conftest import VisitWorld, audit_count, check_in, confirmed_appointment


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
        outcome="completed", billing="finalized", followup="recorded", analytics="recorded"
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
