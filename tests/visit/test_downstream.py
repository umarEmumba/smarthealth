"""Non-blocking visit completion and its downstream workflow (plan 02-11;
VISIT-04, BILL-02, D-16; trigger moved to the outbox by plan 03-09, D-03).

Completing a visit commits first (with a `visit.completed` outbox row) and returns
200; the API makes no Temporal call. `visit-completed-{id}` is started by the
visit-downstream consumer, which these harness tests (no Kafka, no running consumer)
stand in for with `deliver_visit_events`; it then runs on `visit-tq` (the
`visit_worker` fixture), finalizes the billing record and records the follow-up and
analytics intents. A visit whose marker is still NULL is recovered by the sweeper
(those tests stay until plan 03-12 retires it).
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from pymongo import AsyncMongoClient
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from libs.db.postgres import get_session_factory
from libs.internal_client import UpstreamServiceUnavailable
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.api import visits as visits_api
from services.visit.config import settings as visit_settings
from services.visit.activities.downstream import VisitActivities
from services.visit.domain.contracts import (
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    VisitCompletedResult,
    visit_completed_workflow_id,
)
from services.visit.downstream import sweep_unstarted
from services.visit.repository import visit_repo
from tests.conftest import degraded_mesh, harness_apps
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    confirmed_appointment,
    deliver_visit_events,
    history_rows,
    outbox_rows,
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
    assert await _marker(visit["id"]) is None  # the API started nothing (plan 03-09)

    # The visit-downstream consumer's job: turn the committed event into the start.
    assert await deliver_visit_events(None, visit["id"], client=temporal_env.client) == 1

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
    """No worker polls `visit-tq`: the completion still answers 200, the workflow
    the consumer starts is RUNNING (started, not awaited) and billing is untouched."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)

    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None
    await deliver_visit_events(None, visit["id"], client=temporal_env.client)
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


async def test_repeat_complete_is_a_noop_and_the_event_starts_the_workflow_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """A repeated complete is a no-op transition: it writes no second event and
    starts nothing (the Phase 2 opportunistic retry of the API start is gone, D-03).
    The one committed event, delivered (and redelivered), starts the workflow once
    and billing is finalized exactly once."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None

    repeat = await client.post(
        f"/api/visits/{visit['id']}/complete", headers=auth(visit_world.world.provider)
    )
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None  # a repeat starts nothing
    assert len(await outbox_rows(visit["id"])) == 1

    await deliver_visit_events(None, visit["id"], client=temporal_env.client)
    assert await _marker(visit["id"]) is not None
    assert await _result(temporal_env, visit["id"]) == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    await deliver_visit_events(None, visit["id"], client=temporal_env.client)  # redelivery

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
    await deliver_visit_events(None, visit["id"], client=temporal_env.client)
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
    await deliver_visit_events(None, visit["id"], client=temporal_env.client)

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


class _NoTemporalCalls:
    """A Temporal client double that fails on ANY attribute access, so a single
    call (or even a lookup) from the Visit API makes the completion fail."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the Visit API touched Temporal: {name}")


async def test_completion_route_makes_no_temporal_call(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan 03-09 (D-03): the completion route starts no downstream work. A Temporal
    client double that fails on any call is injected into the Visit app; the
    completion still answers 200, commits the visit.completed event and leaves the
    marker NULL for the consumer. The Temporal connect factory is poisoned too, so a
    lazily connecting route would fail as well."""

    async def _no_connect(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the Visit API connected to Temporal")

    monkeypatch.setattr("libs.temporal.connect_client", _no_connect)
    harness_apps()["visit"].state.temporal_client = _NoTemporalCalls()  # autouse fixture restores

    visit, _, completed = await _completed_visit(client, mesh, visit_world)

    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert await _marker(visit["id"]) is None
    assert len(await outbox_rows(visit["id"])) == 1
    # Nothing in the API module can start a workflow any more.
    assert not hasattr(visits_api, "ensure_downstream_started")
    assert not hasattr(visits_api, "get_temporal_client")


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
    await deliver_visit_events(None, visit["id"], client=temporal_env.client)

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


# --- plan 02.1-09 Task 2: refusal and recovery paths, driven through the real activities ---


def _activities(http: httpx.AsyncClient, mongo: AsyncMongoClient) -> VisitActivities:
    """`VisitActivities` wired like the worker's, over `http` (the mesh or a degraded one)."""
    tokens = ServiceTokenProvider(
        "visit",
        load_client_secret(visit_settings.client_secret_path),
        visit_settings.identity_service_url,
        http=http,
    )
    return VisitActivities(get_session_factory(visit_settings.database_url), http, tokens, mongo, None)


async def _run_activity(method: Any, visit_id: str, appointment_id: str, vw: VisitWorld) -> Any:
    """Run one activity method through `ActivityEnvironment` under the visit's
    deterministic workflow id (its audit dedupe key reads it)."""
    environment = ActivityEnvironment()
    environment.info = replace(environment.info, workflow_id=visit_completed_workflow_id(visit_id))
    return await environment.run(
        method,
        VisitCompletedInput(
            visit_id=visit_id, appointment_id=appointment_id, clinic_id=str(vw.clinic_id)
        ),
    )


async def _legacy_completed_visit(vw: VisitWorld, appointment_id: str) -> str:
    """A completed visit row inserted by SQL for an appointment that was never
    checked in: what a visit created before plan 02.1-06 looks like."""
    world = vw.world
    visit_id = uuid.uuid4()
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO visits (id, appointment_id, clinic_id, patient_id, provider_id, "
                    "status, completed_at, created_by) VALUES (:id, :a, :c, :pt, :pr, 'completed', "
                    "now(), :by)"
                ),
                {
                    "id": visit_id,
                    "a": appointment_id,
                    "c": world.clinic_id,
                    "pt": world.patient_id,
                    "pr": world.provider_id,
                    "by": uuid.uuid4(),
                },
            )
    finally:
        await engine.dispose()
    return str(visit_id)


async def test_legacy_visit_settles_as_not_checked_in_and_never_blocks_billing(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """A visit whose appointment is still `confirmed` (created before the
    check-in integration): Scheduling refuses the completion (409), the marker
    records `not_checked_in`, the branch reports the skip, and billing and the
    intents still land in the same run."""
    appointment_id = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)
    visit_id = await _legacy_completed_visit(visit_world, appointment_id)

    handle = await temporal_env.client.start_workflow(
        VISIT_COMPLETED_WORKFLOW,
        VisitCompletedInput(
            visit_id=visit_id, appointment_id=appointment_id, clinic_id=str(visit_world.clinic_id)
        ),
        id=visit_completed_workflow_id(visit_id),
        task_queue=VISIT_TASK_QUEUE,
        result_type=VisitCompletedResult,
    )
    # Time skipping stays locked: no step here waits on a timer, and unlocking
    # right after the start would fast-forward past the cold workflow task.
    result = await handle.result()

    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="skipped:AppointmentNotCheckedIn",
    )
    markers = await _markers(visit_id)
    assert markers["appointment_outcome"] == "not_checked_in"
    assert markers["appointment_settled_at"] is not None
    assert markers["billing_outcome"] == "finalized"
    assert await _appointment_status(client, visit_world, appointment_id) == "confirmed"
    assert await audit_count(visit_id, "visit.appointment_completed") == 0
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"


async def test_legacy_visit_activity_raises_the_non_retryable_business_error(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    appointment_id = await confirmed_appointment(visit_world)
    visit_id = await _legacy_completed_visit(visit_world, appointment_id)
    activities = _activities(mesh, mongo_client)

    with pytest.raises(ApplicationError) as exc_info:
        await _run_activity(activities.complete_appointment, visit_id, appointment_id, visit_world)
    assert exc_info.value.type == "AppointmentNotCheckedIn"
    assert exc_info.value.non_retryable is True
    assert (await _markers(visit_id))["appointment_outcome"] == "not_checked_in"


async def test_missing_appointment_settles_as_no_appointment(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    appointment_id = str(uuid.uuid4())  # Scheduling has no such appointment (404)
    visit_id = await _legacy_completed_visit(visit_world, appointment_id)
    activities = _activities(mesh, mongo_client)

    outcome = await _run_activity(
        activities.complete_appointment, visit_id, appointment_id, visit_world
    )
    assert outcome == "no_appointment"
    markers = await _markers(visit_id)
    assert markers["appointment_outcome"] == "no_appointment"
    assert markers["appointment_settled_at"] is not None


async def test_another_clinics_appointment_is_never_completed(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    other_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    """T-02.1-35: the explicit clinic id is re-applied by Scheduling, so an
    appointment of another clinic answers 404 and is left untouched."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, other_world)
    assert completed.status_code == 200, completed.text
    activities = _activities(mesh, mongo_client)

    # The activity input names `visit_world`'s clinic, not the appointment's.
    outcome = await _run_activity(
        activities.complete_appointment, visit["id"], appointment_id, visit_world
    )
    assert outcome == "no_appointment"
    assert await _appointment_status(client, other_world, appointment_id) == "checked_in"


async def test_complete_appointment_twice_completes_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert await _appointment_status(client, visit_world, appointment_id) == "checked_in"
    activities = _activities(mesh, mongo_client)

    first = await _run_activity(
        activities.complete_appointment, visit["id"], appointment_id, visit_world
    )
    marker = (await _markers(visit["id"]))["appointment_settled_at"]
    second = await _run_activity(
        activities.complete_appointment, visit["id"], appointment_id, visit_world
    )

    assert (first, second) == ("completed", "completed")
    assert await _appointment_status(client, visit_world, appointment_id) == "completed"
    assert (await _markers(visit["id"]))["appointment_settled_at"] == marker
    assert await audit_count(visit["id"], "visit.appointment_completed") == 1


async def test_scheduling_outage_is_retryable_and_leaves_the_marker_open(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text

    async with degraded_mesh("scheduling") as degraded:
        down = _activities(degraded, mongo_client)
        with pytest.raises(UpstreamServiceUnavailable):
            await _run_activity(down.complete_appointment, visit["id"], appointment_id, visit_world)
    markers = await _markers(visit["id"])
    assert markers["appointment_settled_at"] is None
    assert markers["appointment_outcome"] is None
    assert await _appointment_status(client, visit_world, appointment_id) == "checked_in"

    # Scheduling is back: the retry lands.
    up = _activities(mesh, mongo_client)
    assert (
        await _run_activity(up.complete_appointment, visit["id"], appointment_id, visit_world)
        == "completed"
    )
    assert (await _markers(visit["id"]))["appointment_outcome"] == "completed"


async def test_billing_marker_outcomes_and_a_repeat_never_overwrites(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    from tests.billing.conftest import service_token  # noqa: PLC0415
    from tests.billing.conftest import transition as billing_transition  # noqa: PLC0415

    activities = _activities(mesh, mongo_client)

    # finalized
    visit, appointment_id, _ = await _completed_visit(client, mesh, visit_world)
    assert (
        await _run_activity(activities.finalize_billing, visit["id"], appointment_id, visit_world)
        == "finalized"
    )
    first = await _markers(visit["id"])
    assert first["billing_outcome"] == "finalized"
    assert first["billing_settled_at"] is not None
    assert first["appointment_outcome"] is None  # the other marker is untouched
    await _run_activity(activities.finalize_billing, visit["id"], appointment_id, visit_world)
    assert await _markers(visit["id"]) == first  # a repeat keeps the first marker

    # no_record: an appointment with no billing record at all
    bare = await confirmed_appointment(visit_world)
    resp = await check_in(client, visit_world.world.front_desk, bare)
    assert resp.status_code == 201, resp.text
    bare_visit = resp.json()["id"]
    assert (
        await _run_activity(activities.finalize_billing, bare_visit, bare, visit_world)
        == "no_record"
    )
    assert (await _markers(bare_visit))["billing_outcome"] == "no_record"

    # not_finalizable: the record was voided first (terminal, non-retryable)
    voided = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)
    token = await service_token(mesh, "scheduling")
    moved = await billing_transition(mesh, token, voided, "voided", reason_code="booking_compensated")
    assert moved.status_code == 200, moved.text
    resp = await check_in(client, visit_world.world.front_desk, voided)
    assert resp.status_code == 201, resp.text
    voided_visit = resp.json()["id"]
    with pytest.raises(ApplicationError) as exc_info:
        await _run_activity(activities.finalize_billing, voided_visit, voided, visit_world)
    assert exc_info.value.type == "BillingNotFinalizable"
    assert (await _markers(voided_visit))["billing_outcome"] == "not_finalizable"


async def test_marker_repository_functions_are_conditional(
    client: AsyncClient, mesh: httpx.AsyncClient, visit_world: VisitWorld
) -> None:
    """The second write of a marker is a no-op that keeps the first outcome."""
    visit, _, _ = await _completed_visit(client, mesh, visit_world)
    factory = get_session_factory(visit_settings.database_url)
    visit_id = uuid.UUID(visit["id"])
    async with factory() as db:
        assert await visit_repo.mark_appointment_settled(db, visit_id=visit_id, outcome="completed")
        assert not await visit_repo.mark_appointment_settled(
            db, visit_id=visit_id, outcome="not_checked_in"
        )
        assert await visit_repo.mark_billing_settled(db, visit_id=visit_id, outcome="finalized")
        assert not await visit_repo.mark_billing_settled(db, visit_id=visit_id, outcome="no_record")
        await db.commit()
    markers = await _markers(visit["id"])
    assert markers["appointment_outcome"] == "completed"
    assert markers["billing_outcome"] == "finalized"
