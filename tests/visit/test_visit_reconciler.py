"""The visit reconciler (plan 02.1-10 Task 2; S4, BILL-02, VISIT-04; lost-trigger pass
plan 03-22, CR-01).

`downstream.reconcile_unsettled` settles completed visits whose billing or
appointment marker is still empty after a grace period, by running the same
idempotent step bodies the workflow runs. These tests drive it against the real
`VisitActivities` over the in-process mesh (Billing and Scheduling are real,
over their real databases), seeding the state a stuck workflow leaves behind:
a completed visit whose `downstream_started_at` is set and whose markers are
empty.

Grace: the reconciler's own grace is 10 minutes, and the Compose visit worker
runs a reconciler with that grace against the same databases. These tests use
`grace_minutes=0` on freshly completed visits (minutes old, never aged by SQL),
so only the call under test can select them, never a live reconciler. The dev
database may hold a few older unsettled visits of other runs; a pass settles
those too, so assertions about counts are lower bounds and every other
assertion is about the visits a test created.

Lost-trigger pass (plan 03-22): `downstream.restart_lost_triggers` restarts a completed
visit whose marker is still NULL after the grace. The live reconciler would restart ANY
such visit in the shared visit_db, so these tests (W5/W6) pass their own `clinic_id` to
every lost-trigger call, call `VisitActivities.reconcile()` only with both pass functions
replaced by recorders, and never age a NULL-marker visit past 5 minutes; the visit_world
teardown removes the visits and a session guard (`tests/visit/conftest.py`) fails the run
if a NULL-marker harness visit survives. `tests/visit/test_harness_isolation.py` holds
that guard's tests and the AST check of these rules.
"""

from __future__ import annotations

import asyncio
import json
import signal
import uuid
import os
from typing import Any

import httpx
import pytest
import structlog
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.db.postgres import get_session_factory
from libs.observability.logging import configure_logging
from services.visit import downstream
from services.visit import worker as visit_worker_module
from services.visit.activities import downstream as activities_module
from services.visit.activities.downstream import VisitActivities
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import SweepResult, VisitCompletedInput, visit_completed_workflow_id
from services.visit.repository import visit_repo
from tests.conftest import degraded_mesh
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    audit_count,
    check_in,
    checked_in_visit,
    confirmed_appointment,
    deliver_visit_events,
)
from tests.visit.test_downstream import (
    _activities,
    _age_completion,
    _appointment_status,
    _billing_status,
    _completed_visit,
    _finalized_rows,
    _legacy_completed_visit,
    _marker,
    _markers,
    _run_activity,
)

pytestmark = pytest.mark.integration


def _events(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    """The JSON log lines written to stdout since the last read."""
    events: list[dict[str, Any]] = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


async def _started(visit_id: str) -> None:
    """What a workflow that started and then failed to settle leaves behind:
    `downstream_started_at` set (the outbox consumer's job is done), markers empty."""
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE visits SET downstream_started_at = now() WHERE id = :v"),
                {"v": visit_id},
            )
    finally:
        await engine.dispose()


async def _stuck_visit(
    client: AsyncClient, mesh: httpx.AsyncClient, vw: VisitWorld
) -> tuple[str, str]:
    """A completed visit (verified Billing record, checked_in appointment) whose
    workflow never settled anything: (visit id, appointment id)."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, vw)
    assert completed.status_code == 200, completed.text
    await _started(visit["id"])
    markers = await _markers(visit["id"])
    assert markers["billing_settled_at"] is None
    assert markers["appointment_settled_at"] is None
    return visit["id"], appointment_id


async def _reconcile(
    acts: VisitActivities, *, grace_minutes: int = 0, limit: int = downstream.RECONCILE_LIMIT
) -> int:
    return await downstream.reconcile_unsettled(
        get_session_factory(visit_settings.database_url),
        finalize_billing=acts._finalize_billing,
        complete_appointment=acts._complete_appointment,
        grace_minutes=grace_minutes,
        limit=limit,
    )


async def _unsettled_ids(grace_minutes: int = 0) -> list[str]:
    async with get_session_factory(visit_settings.database_url)() as db:
        rows = await visit_repo.list_unsettled(db, older_than_minutes=grace_minutes, limit=1000)
        return [str(v.id) for v in rows]


async def test_reconcile_settles_billing_and_records_the_marker(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    assert await _billing_status(client, visit_world, appointment_id) == "verified"

    settled = await _reconcile(_activities(mesh, mongo_client))

    assert settled >= 1
    assert await _billing_status(client, visit_world, appointment_id) == "finalized"
    markers = await _markers(visit_id)
    assert markers["billing_outcome"] == "finalized"
    assert markers["billing_settled_at"] is not None
    assert await audit_count(visit_id, "visit.billing_finalize_requested") == 1


async def test_reconcile_completes_a_checked_in_appointment(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    assert await _appointment_status(client, visit_world, appointment_id) == "checked_in"

    await _reconcile(_activities(mesh, mongo_client))

    assert await _appointment_status(client, visit_world, appointment_id) == "completed"
    markers = await _markers(visit_id)
    assert markers["appointment_outcome"] == "completed"
    assert markers["appointment_settled_at"] is not None
    assert await audit_count(visit_id, "visit.appointment_completed") == 1


async def test_a_legacy_confirmed_appointment_settles_as_not_checked_in_and_is_never_retried(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    appointment_id = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)
    visit_id = await _legacy_completed_visit(visit_world, appointment_id)
    await _started(visit_id)
    assert visit_id in await _unsettled_ids()

    await _reconcile(_activities(mesh, mongo_client))

    markers = await _markers(visit_id)
    assert markers["appointment_outcome"] == "not_checked_in"
    assert markers["appointment_settled_at"] is not None
    assert markers["billing_outcome"] == "finalized"
    assert await _appointment_status(client, visit_world, appointment_id) == "confirmed"
    assert await audit_count(visit_id, "visit.appointment_completed") == 0
    # Terminal: both markers are set, so no later pass selects it again.
    assert visit_id not in await _unsettled_ids()


async def test_a_voided_billing_record_is_a_terminal_marker_not_a_retry_loop(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    from tests.billing.conftest import service_token  # noqa: PLC0415
    from tests.billing.conftest import transition as billing_transition  # noqa: PLC0415

    appointment_id = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)
    token = await service_token(mesh, "scheduling")
    voided = await billing_transition(
        mesh, token, appointment_id, "voided", reason_code="booking_compensated"
    )
    assert voided.status_code == 200, voided.text
    resp = await check_in(client, visit_world.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit_id = resp.json()["id"]
    started = await client.post(
        f"/api/visits/{visit_id}/start", headers=auth(visit_world.world.provider)
    )
    assert started.status_code == 200, started.text
    done = await client.post(
        f"/api/visits/{visit_id}/complete", headers=auth(visit_world.world.provider)
    )
    assert done.status_code == 200, done.text
    await _started(visit_id)

    await _reconcile(_activities(mesh, mongo_client))  # must not raise

    markers = await _markers(visit_id)
    assert markers["billing_outcome"] == "not_finalizable"
    assert markers["appointment_outcome"] == "completed"
    assert await _billing_status(client, visit_world, appointment_id) == "voided"
    assert visit_id not in await _unsettled_ids()


async def test_a_billing_outage_leaves_the_marker_empty_and_never_stops_the_pass(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Billing unreachable: the billing marker of BOTH visits stays empty (and
    Billing untouched), the failure is logged with the visit id, step and error
    type only, and the pass still settles both visits' appointment step; once
    Billing is back, a later pass finishes the job."""
    configure_logging()
    monkeypatch.setattr(downstream, "log", structlog.get_logger("test-visit-reconcile"))
    capsys.readouterr()
    first, first_appointment = await _stuck_visit(client, mesh, visit_world)
    second, second_appointment = await _stuck_visit(client, mesh, visit_world)

    async with degraded_mesh("billing") as degraded:
        await _reconcile(_activities(degraded, mongo_client))

    for visit_id, appointment_id in ((first, first_appointment), (second, second_appointment)):
        markers = await _markers(visit_id)
        assert markers["billing_settled_at"] is None
        assert markers["billing_outcome"] is None
        assert markers["appointment_outcome"] == "completed"  # the pass went on
        assert await _billing_status(client, visit_world, appointment_id) == "verified"

    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    failed = [e for e in events if e.get("event") == "visit.reconcile_step_failed"]
    mine = {e["visit_id"]: e for e in failed if e["visit_id"] in (first, second)}
    assert set(mine) == {first, second}
    for event in mine.values():
        assert event["level"] == "warning"
        assert event["step"] == "billing"
        assert event["error"] == "UpstreamServiceUnavailable"
        assert set(event) <= {"event", "level", "timestamp", "visit_id", "step", "error", "logger"}
    assert first_appointment not in json.dumps(mine)

    # Billing is back: a later pass settles both.
    await _reconcile(_activities(mesh, mongo_client))
    for visit_id, appointment_id in ((first, first_appointment), (second, second_appointment)):
        assert (await _markers(visit_id))["billing_outcome"] == "finalized"
        assert await _billing_status(client, visit_world, appointment_id) == "finalized"


async def test_a_visit_inside_the_grace_period_is_not_touched(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    acts = _activities(mesh, mongo_client)

    await _reconcile(acts, grace_minutes=10)  # completed seconds ago
    assert visit_id not in await _unsettled_ids(grace_minutes=10)
    assert (await _markers(visit_id))["billing_settled_at"] is None
    assert await _billing_status(client, visit_world, appointment_id) == "verified"
    assert await _appointment_status(client, visit_world, appointment_id) == "checked_in"

    # Positive control: the same visit with no grace is settled, so the skip
    # above is attributable to the grace period alone.
    await _reconcile(acts, grace_minutes=0)
    assert (await _markers(visit_id))["billing_outcome"] == "finalized"


async def _lost_trigger_ids(clinic_id: uuid.UUID, grace_minutes: int = 0) -> list[str]:
    async with get_session_factory(visit_settings.database_url)() as db:
        rows = await visit_repo.list_lost_triggers(
            db, older_than_minutes=grace_minutes, limit=1000, clinic_id=clinic_id
        )
        return [str(v.id) for v in rows]


async def _restart(
    temporal_env: WorkflowEnvironment, vw: VisitWorld, *, grace_minutes: int = 0
) -> int:
    """One clinic-scoped lost-trigger pass against the test Temporal server (W5)."""
    return await downstream.restart_lost_triggers(
        get_session_factory(visit_settings.database_url),
        temporal_env.client,
        grace_minutes=grace_minutes,
        clinic_id=vw.clinic_id,
    )


async def test_a_visit_whose_trigger_was_lost_is_restarted_after_the_grace(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """A completed visit whose marker is NULL is the outbox consumer's job inside the
    grace (plan 03-09, D-03) and the reconciler's after it (plan 03-22, CR-01: the event was
    dead-lettered or dropped). Grace 0 stands for "past the grace" on a fresh visit; a
    second pass attaches to the same run and restarts nothing."""
    visit, appointment_id, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text  # no consumer ran: marker NULL
    assert await _marker(visit["id"]) is None
    assert visit["id"] not in await _unsettled_ids()  # not the settle pass's visit

    # Inside the grace it is not selected, so nothing starts (the grace is the only difference).
    assert await _restart(temporal_env, visit_world, grace_minutes=10) == 0
    assert await _marker(visit["id"]) is None

    assert await _restart(temporal_env, visit_world) == 1
    assert await _marker(visit["id"]) is not None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    described = await handle.describe()
    assert described.status == WorkflowExecutionStatus.RUNNING
    first_run = described.run_id

    assert await _restart(temporal_env, visit_world) == 0  # marker set: no candidate left
    again = await handle.describe()
    assert again.run_id == first_run
    assert await _billing_status(client, visit_world, appointment_id) == "verified"  # no worker
    await handle.terminate(reason="test clean-up")


async def test_list_lost_triggers_selects_only_completed_unmarked_visits_of_the_clinic(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    other_world: VisitWorld,
) -> None:
    lost, _, done = await _completed_visit(client, mesh, visit_world)
    assert done.status_code == 200, done.text
    marked, _ = await _stuck_visit(client, mesh, visit_world)  # completed WITH a marker
    await checked_in_visit(client, visit_world)  # checked in
    in_progress = await checked_in_visit(client, visit_world)
    started = await client.post(
        f"/api/visits/{in_progress['id']}/start", headers=auth(visit_world.world.provider)
    )
    assert started.status_code == 200, started.text
    foreign, _, foreign_done = await _completed_visit(client, mesh, other_world)
    assert foreign_done.status_code == 200, foreign_done.text

    assert await _lost_trigger_ids(visit_world.clinic_id) == [lost["id"]]
    assert marked not in await _lost_trigger_ids(visit_world.clinic_id)
    assert in_progress["id"] not in await _lost_trigger_ids(visit_world.clinic_id)
    assert await _lost_trigger_ids(visit_world.clinic_id, grace_minutes=10) == []
    # The clinic filter is real: the other clinic's lost trigger is only in its own list.
    assert foreign not in await _lost_trigger_ids(visit_world.clinic_id)
    assert await _lost_trigger_ids(other_world.clinic_id) == [foreign["id"]]


async def test_a_delivery_after_the_restart_attaches_to_the_same_execution(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    visit, _, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert await _restart(temporal_env, visit_world) == 1
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit["id"]))
    first_run = (await handle.describe()).run_id
    marker = await _marker(visit["id"])
    assert marker is not None

    # A racing consumer delivery (or a backfill restart) of the same event.
    assert await deliver_visit_events(None, visit["id"], client=temporal_env.client) == 1

    assert (await handle.describe()).run_id == first_run
    assert await _marker(visit["id"]) == marker  # the conditional marker is set once
    await handle.terminate(reason="test clean-up")


async def test_reconcile_runs_the_lost_trigger_pass_first_then_the_settle_pass(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Wiring only, no visit_db row is touched (W5): both pass functions are replaced by
    recorders. The lost-trigger pass runs first (WR-01) and gets the activity's Temporal
    client and grace and NO clinic id; the settle pass runs second; the activity returns the sum. Without a
    client the lost-trigger pass is skipped and logged."""
    calls: list[tuple[Any, ...]] = []

    async def settle_recorder(factory: Any, **kwargs: Any) -> int:
        calls.append(("settle", kwargs["grace_minutes"]))
        return 2

    async def lost_recorder(
        factory: Any, client: Any, *, grace_minutes: int, clinic_id: Any = None, **kwargs: Any
    ) -> int:
        calls.append(("lost", client, grace_minutes, clinic_id))
        return 3

    monkeypatch.setattr(downstream, "reconcile_unsettled", settle_recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", lost_recorder)
    sentinel = object()
    tokens_acts = _activities(mesh, mongo_client)

    def build(client: Any) -> VisitActivities:
        return VisitActivities(
            tokens_acts._session_factory,
            mesh,
            tokens_acts._tokens,
            mongo_client,
            client,
            reconcile_grace_minutes=7,
        )

    assert await build(sentinel).reconcile() == 5
    assert calls == [("lost", sentinel, 7, None), ("settle", 7)]

    configure_logging()
    monkeypatch.setattr(activities_module, "log", structlog.get_logger("test-visit-activities"))
    capsys.readouterr()
    calls.clear()
    assert await build(None).reconcile() == 2
    assert calls == [("settle", 7)]  # the lost-trigger recorder was never called
    assert [e for e in _events(capsys) if e.get("event") == "visit.lost_trigger_pass_skipped"]


async def test_a_settled_visit_is_not_selected_and_not_called(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, _ = await _stuck_visit(client, mesh, visit_world)
    await _reconcile(_activities(mesh, mongo_client))
    assert visit_id not in await _unsettled_ids()

    called: list[str] = []

    async def spy(inp: VisitCompletedInput, workflow_id: str) -> str:
        called.append(inp.visit_id)
        return "spy"

    await downstream.reconcile_unsettled(
        get_session_factory(visit_settings.database_url),
        finalize_billing=spy,
        complete_appointment=spy,
        grace_minutes=0,
    )
    assert visit_id not in called


async def test_reconciling_twice_finalizes_once_and_audits_once_per_step(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    acts = _activities(mesh, mongo_client)

    await _reconcile(acts)
    first = await _markers(visit_id)
    await _reconcile(acts)

    assert await _finalized_rows(appointment_id) == 1  # ONE Billing finalized transition
    assert await audit_count(visit_id, "visit.billing_finalize_requested") == 1
    assert await audit_count(visit_id, "visit.appointment_completed") == 1
    assert await _markers(visit_id) == first  # a repeat never overwrites


async def test_a_pass_racing_the_workflow_step_finalizes_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    """The reconciler and the workflow's `finalize_billing` activity (same
    deterministic workflow id) run at the same time for one visit: Billing
    records a single `finalized` move and the audit has a single entry."""
    # The audit dedupe is enforced by the partial unique index `ix_audit_dedupe_key`.
    # Services create it at start-up, but `tests/libs/test_audit_indexes.py` drops the
    # shared `audit_log` collection (and with it every index) when it finishes, so in a
    # full-suite run the index is gone by the time this test runs. State the
    # precondition instead of depending on test order.
    await ensure_audit_indexes(mongo_client)
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    acts = _activities(mesh, mongo_client)

    await asyncio.gather(
        _reconcile(acts),
        _run_activity(acts.finalize_billing, visit_id, appointment_id, visit_world),
        _run_activity(acts.finalize_billing, visit_id, appointment_id, visit_world),
    )

    assert await _billing_status(client, visit_world, appointment_id) == "finalized"
    assert await _finalized_rows(appointment_id) == 1
    assert await audit_count(visit_id, "visit.billing_finalize_requested") == 1
    markers = await _markers(visit_id)
    assert markers["billing_outcome"] == "finalized"
    assert markers["appointment_outcome"] == "completed"


async def test_a_pass_handles_at_most_limit_visits_oldest_first(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
) -> None:
    """Spy step bodies (they settle nothing, so no other visit is touched):
    with `limit=2` exactly the two oldest candidates are visited."""
    for _ in range(3):
        await _stuck_visit(client, mesh, visit_world)
    snapshot = await _unsettled_ids()
    assert len(snapshot) >= 3

    async with get_session_factory(visit_settings.database_url)() as db:
        rows = await visit_repo.list_unsettled(db, older_than_minutes=0, limit=1000)
    completed = [v.completed_at for v in rows]
    assert completed == sorted(completed)  # oldest first

    called: list[str] = []

    async def spy(inp: VisitCompletedInput, workflow_id: str) -> str:
        assert workflow_id == f"visit-completed-{inp.visit_id}"
        if inp.visit_id not in called:
            called.append(inp.visit_id)
        return "spy"

    await downstream.reconcile_unsettled(
        get_session_factory(visit_settings.database_url),
        finalize_billing=spy,
        complete_appointment=spy,
        grace_minutes=0,
        limit=2,
    )
    assert called == snapshot[:2]


async def test_the_reconcile_activity_runs_a_pass_and_returns_the_count(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    mongo_client: AsyncMongoClient,
) -> None:
    visit_id, appointment_id = await _stuck_visit(client, mesh, visit_world)
    tokens_acts = _activities(mesh, mongo_client)
    acts = VisitActivities(
        tokens_acts._session_factory,
        mesh,
        tokens_acts._tokens,
        mongo_client,
        None,
        reconcile_grace_minutes=0,
    )

    result: Any = await ActivityEnvironment().run(acts.reconcile_unsettled)

    assert isinstance(result, SweepResult)
    assert result.processed >= 1
    assert (await _markers(visit_id))["billing_outcome"] == "finalized"
    assert await _appointment_status(client, visit_world, appointment_id) == "completed"
    # The activity is registered under its contract name with the others.
    assert acts.reconcile_unsettled in acts.all()


async def test_list_unsettled_only_selects_completed_visits_with_an_open_marker(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
) -> None:
    from tests.visit.conftest import checked_in_visit  # noqa: PLC0415

    in_progress = await checked_in_visit(client, visit_world)
    await _started(in_progress["id"])  # started flag set, but the visit is not completed
    stuck, _ = await _stuck_visit(client, mesh, visit_world)

    ids = await _unsettled_ids()
    assert stuck in ids
    assert in_progress["id"] not in ids
    assert uuid.UUID(stuck)  # ids are visit uuids only


# --- WR-01 (gap review): the lost-trigger pass first, both passes bounded and isolated ----------


def _reconcile_activities(mesh: httpx.AsyncClient, mongo_client: AsyncMongoClient, client: Any) -> VisitActivities:
    """Activities with grace 7 and `client` as the Temporal client (set after construction;
    callers replace both pass functions by recorders, see the isolation gate)."""
    tokens_acts = _activities(mesh, mongo_client)
    acts = VisitActivities(
        tokens_acts._session_factory,
        mesh,
        tokens_acts._tokens,
        mongo_client,
        None,
        reconcile_grace_minutes=7,
    )
    acts._client = client
    return acts


async def test_a_slow_settle_pass_cannot_starve_the_lost_trigger_pass(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    order: list[str] = []

    async def settle_recorder(factory: Any, **kwargs: Any) -> int:
        order.append("settle-start")
        await asyncio.sleep(60)
        return 2

    async def lost_recorder(factory: Any, client: Any, *, clinic_id: Any = None, **kwargs: Any) -> int:
        order.append("lost")
        return 3

    monkeypatch.setattr(downstream, "reconcile_unsettled", settle_recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", lost_recorder)
    monkeypatch.setattr(activities_module, "SETTLE_PASS_BUDGET_SECONDS", 0.2, raising=False)
    configure_logging()
    monkeypatch.setattr(activities_module, "log", structlog.get_logger("test-visit-activities"))
    capsys.readouterr()

    result = await asyncio.wait_for(_reconcile_activities(mesh, mongo_client, object()).reconcile(), timeout=5)

    assert result == 3
    assert order == ["lost", "settle-start"]
    timed_out = [e for e in _events(capsys) if e.get("event") == "visit.reconcile_pass_timed_out"]
    assert [e["pass_name"] for e in timed_out] == ["settle"]


async def test_a_failing_or_slow_lost_trigger_pass_never_skips_the_settle_pass(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mode = {"value": "raise"}

    async def settle_recorder(factory: Any, **kwargs: Any) -> int:
        return 2

    async def lost_recorder(factory: Any, client: Any, *, clinic_id: Any = None, **kwargs: Any) -> int:
        if mode["value"] == "raise":
            raise RuntimeError("secret-detail-must-not-be-logged")
        await asyncio.sleep(60)
        return 3

    monkeypatch.setattr(downstream, "reconcile_unsettled", settle_recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", lost_recorder)
    monkeypatch.setattr(activities_module, "LOST_TRIGGER_PASS_BUDGET_SECONDS", 0.2, raising=False)
    configure_logging()
    monkeypatch.setattr(activities_module, "log", structlog.get_logger("test-visit-activities"))
    capsys.readouterr()
    acts = _reconcile_activities(mesh, mongo_client, object())

    assert await asyncio.wait_for(acts.reconcile(), timeout=5) == 2
    failed = [e for e in _events(capsys) if e.get("event") == "visit.lost_trigger_pass_failed"]
    assert [e["error"] for e in failed] == ["RuntimeError"]
    assert "secret-detail" not in json.dumps(failed)

    mode["value"] = "sleep"
    assert await asyncio.wait_for(acts.reconcile(), timeout=5) == 2
    timed_out = [e for e in _events(capsys) if e.get("event") == "visit.reconcile_pass_timed_out"]
    assert [e["pass_name"] for e in timed_out] == ["lost_trigger"]


def test_the_two_pass_budgets_fit_inside_the_reconcile_activity_timeout() -> None:
    from libs.temporal_policies import MAINTENANCE_ACTIVITY  # noqa: PLC0415

    lost = activities_module.LOST_TRIGGER_PASS_BUDGET_SECONDS
    settle = activities_module.SETTLE_PASS_BUDGET_SECONDS
    assert lost > 0 and settle > 0
    assert lost + settle + 5 <= MAINTENANCE_ACTIVITY["start_to_close_timeout"].total_seconds()


class _StubActivities:
    """Stands in for VisitActivities in worker.main(): reconcile() hangs or raises."""

    def __init__(self, behavior: str, heartbeat: Any, seen: dict[str, Any]) -> None:
        self._behavior = behavior
        self._heartbeat = heartbeat
        self._seen = seen

    def all(self) -> list[Any]:
        return []

    async def reconcile(self) -> int:
        end = asyncio.get_running_loop().time() + 2
        while not self._heartbeat.exists() and asyncio.get_running_loop().time() < end:
            await asyncio.sleep(0.02)
        self._seen["heartbeat_during_pass"] = self._heartbeat.exists()
        if self._behavior == "raise":
            raise RuntimeError("boom-detail")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self._seen["cancelled"] = True
            raise
        return 0


async def _drive_worker_main(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, behavior: str
) -> tuple[int, dict[str, Any], Any]:
    heartbeat = tmp_path / "visit-worker.alive"
    seen: dict[str, Any] = {"worker_entered": False}

    class _FakeWorker:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _FakeWorker:
            seen["worker_entered"] = True
            return self

        async def __aexit__(self, *exc: Any) -> None:
            return None

    async def _connect() -> object:
        return object()

    async def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    async def _retired(*args: Any, **kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(visit_worker_module, "HEARTBEAT_PATH", heartbeat)
    monkeypatch.setattr(visit_worker_module, "configure_tracer_provider", lambda name: None)
    monkeypatch.setattr(visit_worker_module, "start_worker_metrics_server", lambda: None)
    monkeypatch.setattr(visit_worker_module, "_connect_with_retry", _connect)
    monkeypatch.setattr(
        visit_worker_module, "build_activities", lambda client: _StubActivities(behavior, heartbeat, seen)
    )
    monkeypatch.setattr(visit_worker_module, "get_mongo_client", lambda: None)
    monkeypatch.setattr(visit_worker_module, "ensure_audit_indexes", _noop)
    monkeypatch.setattr(visit_worker_module, "ensure_visit_schedules", _noop)
    monkeypatch.setattr(visit_worker_module, "retire_sweeper_schedule", _retired)
    monkeypatch.setattr(visit_worker_module, "Worker", _FakeWorker)

    main_task = asyncio.ensure_future(visit_worker_module.main())
    deadline = asyncio.get_running_loop().time() + 5
    while not main_task.done() and asyncio.get_running_loop().time() < deadline:
        if heartbeat.exists() and seen["worker_entered"]:
            break
        await asyncio.sleep(0.05)
    if not main_task.done():
        os.kill(os.getpid(), signal.SIGTERM)  # main's loop handler turns it into stop.set()
    rc = await asyncio.wait_for(main_task, timeout=10)
    return rc, seen, heartbeat


async def test_a_hanging_start_up_reconcile_never_blocks_the_heartbeat_or_the_poll_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    rc, seen, heartbeat = await _drive_worker_main(monkeypatch, tmp_path, "hang")
    assert rc == 0
    assert heartbeat.exists()
    assert seen["heartbeat_during_pass"] is True  # the pass started after the heartbeat task existed
    assert seen["worker_entered"] is True  # the Worker polled while the pass still hung
    assert seen.get("cancelled") is True  # cancelled at shutdown


async def test_a_failing_start_up_reconcile_is_logged_and_never_stops_the_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    monkeypatch.setattr(visit_worker_module, "log", structlog.get_logger("test-visit-worker"))
    capsys.readouterr()
    rc, seen, heartbeat = await _drive_worker_main(monkeypatch, tmp_path, "raise")
    assert rc == 0
    assert heartbeat.exists()
    assert seen["worker_entered"] is True
    out = capsys.readouterr().out
    failed = [json.loads(x) for x in out.splitlines() if x.startswith("{") and "startup_reconcile_failed" in x]
    assert [e["error"] for e in failed] == ["RuntimeError"]
    assert "boom-detail" not in out


# --- WR-07 (gap review): age ceiling, kill switch, per-visit backoff, systemic RPC abort ---------


class _StartStub:
    """A Temporal client stand-in: records every start; raises `error` for the ids in `failing`."""

    def __init__(self, error: Exception | None = None, failing: set[str] | None = None) -> None:
        self.error = error
        self.failing = failing
        self.ids: list[str] = []

    async def start_workflow(self, workflow: Any, arg: Any, *, id: str, **kwargs: Any) -> None:
        self.ids.append(id)
        if self.error is not None and (self.failing is None or id in self.failing):
            raise self.error


def _rpc_unavailable() -> RPCError:
    return RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")


async def _aged_visits(
    client: AsyncClient, mesh: httpx.AsyncClient, vw: VisitWorld, ages: list[int]
) -> list[str]:
    ids = []
    for age in ages:
        visit, _, done = await _completed_visit(client, mesh, vw)
        assert done.status_code == 200, done.text
        await _age_completion(visit["id"], age)
        ids.append(visit["id"])
    return ids


async def _restart_stub(
    vw: VisitWorld, stub: Any, backoff: downstream.LostTriggerBackoff, **kwargs: Any
) -> int:
    return await downstream.restart_lost_triggers(
        get_session_factory(visit_settings.database_url),
        stub,
        grace_minutes=0,
        backoff=backoff,
        clinic_id=vw.clinic_id,
        **kwargs,
    )


async def test_a_lost_trigger_older_than_the_ceiling_is_reported_and_never_restarted(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    old, fresh = await _aged_visits(client, mesh, visit_world, [180, 5])
    configure_logging()
    monkeypatch.setattr(downstream, "log", structlog.get_logger("test-visit-downstream"))
    capsys.readouterr()
    backoff = downstream.LostTriggerBackoff()
    stub = _StartStub()

    assert await _restart_stub(visit_world, stub, backoff, max_age_minutes=2) == 1
    assert stub.ids == [f"visit-completed-{fresh}"]
    assert await _marker(old) is None
    events = _events(capsys)
    warned = [e for e in events if e.get("event") == "visit.lost_triggers_beyond_max_age"]
    assert [(e["count"], e["max_age_minutes"]) for e in warned] == [(1, 2)]

    assert await _restart_stub(visit_world, stub, backoff, max_age_minutes=2) == 0
    events = _events(capsys)
    assert not [e for e in events if e.get("event") == "visit.lost_triggers_beyond_max_age"]
    summary = [e for e in events if e.get("event") == "visit.lost_triggers"]
    assert summary and summary[-1]["beyond_max_age"] == 1

    # Positive control: a wider ceiling restarts the old one.
    assert await _restart_stub(visit_world, stub, backoff, max_age_minutes=10) == 1
    assert stub.ids[-1] == f"visit-completed-{old}"
    assert await _marker(old) is not None


@pytest.mark.parametrize("error_factory", [_rpc_unavailable, lambda: ValueError("x")], ids=["rpc", "value"])
async def test_a_persistently_failing_visit_does_not_starve_a_newer_one_through_reconcile(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    visit_world: VisitWorld,
    monkeypatch: pytest.MonkeyPatch,
    error_factory: Any,
) -> None:
    a, b = await _aged_visits(client, mesh, visit_world, [120, 60])
    stub = _StartStub(error=error_factory(), failing={f"visit-completed-{a}"})
    real = downstream.restart_lost_triggers

    async def settle_recorder(factory: Any, **kwargs: Any) -> int:
        return 0

    async def scoped(factory: Any, client: Any, **kwargs: Any) -> int:
        return await real(factory, client, limit=1, clinic_id=visit_world.clinic_id, **kwargs)

    monkeypatch.setattr(downstream, "reconcile_unsettled", settle_recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", scoped)
    acts = _reconcile_activities(mesh, mongo_client, stub)
    acts._reconcile_grace = 0

    assert await acts.reconcile() == 0  # pass 1: A failed
    assert await acts.reconcile() == 1  # pass 2: A backed off, B is not starved
    assert await _marker(b) is not None
    assert await acts.reconcile() == 0  # pass 3: A is tried again
    assert stub.ids.count(f"visit-completed-{a}") == 2
    assert await acts.reconcile() == 0  # pass 4: A is backed off again
    assert stub.ids.count(f"visit-completed-{a}") == 2


async def test_three_consecutive_rpc_errors_stop_the_pass_and_back_off_only_attempted_visits(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    v1, v2, v3 = await _aged_visits(client, mesh, visit_world, [180, 120, 60])
    monkeypatch.setattr(downstream, "LOST_TRIGGER_MAX_CONSECUTIVE_RPC_ERRORS", 2, raising=False)
    configure_logging()
    monkeypatch.setattr(downstream, "log", structlog.get_logger("test-visit-downstream"))
    capsys.readouterr()
    backoff = downstream.LostTriggerBackoff()
    down = _StartStub(error=_rpc_unavailable())

    assert await _restart_stub(visit_world, down, backoff) == 0
    assert down.ids == [f"visit-completed-{v1}", f"visit-completed-{v2}"]
    assert len(backoff) == 2
    assert [e for e in _events(capsys) if e.get("event") == "visit.lost_trigger_pass_aborted"]

    up = _StartStub()
    assert await _restart_stub(visit_world, up, backoff) == 1  # V3 was never attempted
    assert up.ids == [f"visit-completed-{v3}"]
    up2 = _StartStub()
    assert await _restart_stub(visit_world, up2, backoff) == 2  # V1 and V2 are eligible again
    assert sorted(up2.ids) == sorted([f"visit-completed-{v1}", f"visit-completed-{v2}"])


async def test_the_kill_switch_and_the_settings_reach_the_lost_trigger_pass(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list[dict[str, Any]] = []
    settled: list[int] = []

    async def settle_recorder(factory: Any, **kwargs: Any) -> int:
        settled.append(1)
        return 0

    async def lost_recorder(factory: Any, client: Any, *, clinic_id: Any = None, **kwargs: Any) -> int:
        seen.append(kwargs)
        return 0

    monkeypatch.setattr(downstream, "reconcile_unsettled", settle_recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", lost_recorder)
    configure_logging()
    monkeypatch.setattr(activities_module, "log", structlog.get_logger("test-visit-activities"))
    tokens_acts = _activities(mesh, mongo_client)

    def build(**kw: Any) -> VisitActivities:
        return VisitActivities(
            tokens_acts._session_factory, mesh, tokens_acts._tokens, mongo_client, object(),
            reconcile_grace_minutes=7, lost_trigger_max_age_hours=3, **kw,
        )

    acts = build()
    await acts.reconcile()
    await acts.reconcile()
    assert [k["max_age_minutes"] for k in seen] == [180, 180]
    assert seen[0]["backoff"] is seen[1]["backoff"]
    assert isinstance(seen[0]["backoff"], downstream.LostTriggerBackoff)

    seen.clear()
    capsys.readouterr()
    await build(lost_trigger_restart_enabled=False).reconcile()
    assert seen == [] and len(settled) == 3
    assert [e for e in _events(capsys) if e.get("event") == "visit.lost_trigger_pass_disabled"]


def test_the_lost_trigger_settings_default_and_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError  # noqa: PLC0415

    from services.visit.config import VisitSettings  # noqa: PLC0415

    for name in (
        "VISIT_LOST_TRIGGER_RESTART_ENABLED",
        "VISIT_LOST_TRIGGER_MAX_AGE_HOURS",
        "VISIT_RECONCILE_GRACE_MINUTES",
    ):
        monkeypatch.delenv(name, raising=False)
    defaults = VisitSettings(_env_file=None)
    assert defaults.lost_trigger_restart_enabled is True
    assert defaults.lost_trigger_max_age_hours == 24
    assert 24 * 60 == downstream.DEFAULT_LOST_TRIGGER_MAX_AGE_MINUTES
    monkeypatch.setenv("VISIT_LOST_TRIGGER_RESTART_ENABLED", "false")
    assert VisitSettings(_env_file=None).lost_trigger_restart_enabled is False
    monkeypatch.setenv("VISIT_LOST_TRIGGER_MAX_AGE_HOURS", "1")
    monkeypatch.setenv("VISIT_RECONCILE_GRACE_MINUTES", "60")
    with pytest.raises(ValidationError):
        VisitSettings(_env_file=None)


def test_the_lost_trigger_backoff_doubles_caps_forgets_and_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backoff = downstream.LostTriggerBackoff()
    visit = uuid.uuid4()
    backoff.start_pass()
    expected_skips = [1, 2, 4, 8, 16, 32, 32]
    for skips in expected_skips:
        backoff.record_failure(visit)
        for _ in range(skips):
            assert visit in backoff.start_pass()
        assert visit not in backoff.start_pass()
    backoff.record_failure(visit)
    backoff.record_success(visit)
    assert visit not in backoff.start_pass() and len(backoff) == 0

    monkeypatch.setattr(downstream, "LOST_TRIGGER_BACKOFF_MAX_ENTRIES", 3, raising=False)
    small = downstream.LostTriggerBackoff()
    small.start_pass()
    for _ in range(6):
        small.record_failure(uuid.uuid4())
    assert len(small) <= 3

    seen = downstream.LostTriggerBackoff()
    assert [seen.note_beyond_max_age(n) for n in (0, 1, 1, 2, 2, 0, 1)] == [
        False, True, False, True, False, False, True,
    ]
