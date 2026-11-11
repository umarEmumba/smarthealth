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
import uuid
from typing import Any

import httpx
import pytest
import structlog
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.db.postgres import get_session_factory
from libs.observability.logging import configure_logging
from services.visit import downstream
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


async def test_reconcile_runs_the_settle_pass_then_the_lost_trigger_pass(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Wiring only, no visit_db row is touched (W5): both pass functions are replaced by
    recorders. The settle pass runs first; the lost-trigger pass gets the activity's
    Temporal client and grace and NO clinic id; the activity returns the sum. Without a
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
    assert calls == [("settle", 7), ("lost", sentinel, 7, None)]

    configure_logging()
    monkeypatch.setattr(activities_module, "log", structlog.get_logger("test-visit-activities"))
    capsys.readouterr()
    calls.clear()
    assert await build(None).reconcile() == 2
    assert calls == [("settle", 7)]  # the lost-trigger recorder was never called
    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    assert [e for e in events if e.get("event") == "visit.lost_trigger_pass_skipped"]


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
