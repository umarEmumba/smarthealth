"""ReconcilerWorkflow: command order, failure isolation and replay (plan 02.1-11
Task 3; S4, S5, M1, D-19).

`ReconcilerWorkflow` is a NEW workflow type, so it needed no `workflow.patched`.
`histories/reconciler_p21.json` (the `p21` suffix: recorded in phase 02.1, no
patch marker) is one real run through the real worker and activities on the
time-skipping test server: a stuck pending appointment is failed, a stuck
matched waitlist entry resolved and a confirmed appointment past its slot marked
no-show, in one workflow run. Replaying it re-runs the workflow against the
recorded commands, so a later edit that adds, drops or reorders an activity
without `workflow.patched` raises a nondeterminism error here before it can break
a run in flight (the reorder test below shows the gate biting). Options-only
edits (retry or timeout presets) are NOT visible to replay, as plan 02.1-10
established; this file does not pretend otherwise.

The fixture is ids and plain strings only (the activities take no input and
return counts); the recorder strips stack traces and the checks below re-assert
that on every run.

Re-record (only for a deliberate, patched change; keep the older file):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_reconciler.py -k record
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.workflow import NondeterminismError

from libs.temporal_policies import MAINTENANCE_ACTIVITY, SCHEDULING_TASK_QUEUE
from services.scheduling.domain.contracts import (
    ACT_RECONCILE_NO_SHOWS,
    ACT_RECONCILE_PENDING,
    ACT_RECONCILE_WAITLIST,
    RECONCILER_WORKFLOW,
    MaintenanceResult,
    ReconcilePendingResult,
    ReconcileResult,
)
from services.scheduling.workflows.reconcile import ReconcilerWorkflow
from tests.workflows.test_reconciler import (  # noqa: F401 - `scope` is a fixture
    Scope,
    appointment_row,
    billing_status,
    entry_row,
    reconciler,
    scope,
    seed_appointment,
    seed_billing,
    seed_matched_entry,
)
from tests.workflows.test_replay import FORBIDDEN, HISTORIES, _events, _load, _patch_ids, _scrub

HISTORY = "reconciler_p21.json"
ACTIVITY_ORDER = [ACT_RECONCILE_PENDING, ACT_RECONCILE_WAITLIST, ACT_RECONCILE_NO_SHOWS]


def _activity_types(name: str) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]


# --- the workflow with stub activities ---------------------------------------------------


def _stubs(
    calls: list[str],
    *,
    fail: str | None = None,
    pending: ReconcilePendingResult = ReconcilePendingResult(failed=2, billing_voids=3),
    waitlist: int = 4,
    no_shows: int = 5,
):
    @activity.defn(name=ACT_RECONCILE_PENDING)
    async def stuck_pending() -> ReconcilePendingResult:
        calls.append(ACT_RECONCILE_PENDING)
        if fail == ACT_RECONCILE_PENDING:
            raise ApplicationError("boom", type="Boom", non_retryable=True)
        return pending

    @activity.defn(name=ACT_RECONCILE_WAITLIST)
    async def stuck_waitlist() -> MaintenanceResult:
        calls.append(ACT_RECONCILE_WAITLIST)
        if fail == ACT_RECONCILE_WAITLIST:
            raise ApplicationError("boom", type="Boom", non_retryable=True)
        return MaintenanceResult(processed=waitlist)

    @activity.defn(name=ACT_RECONCILE_NO_SHOWS)
    async def no_show_sweep() -> MaintenanceResult:
        calls.append(ACT_RECONCILE_NO_SHOWS)
        if fail == ACT_RECONCILE_NO_SHOWS:
            raise ApplicationError("boom", type="Boom", non_retryable=True)
        return MaintenanceResult(processed=no_shows)

    return [stuck_pending, stuck_waitlist, no_show_sweep]


async def _run_with_stubs(env: WorkflowEnvironment, stubs: list) -> ReconcileResult:
    async with Worker(
        env.client,
        task_queue=SCHEDULING_TASK_QUEUE,
        workflows=[ReconcilerWorkflow],
        activities=stubs,
    ):
        return await env.client.execute_workflow(
            RECONCILER_WORKFLOW,
            id=f"reconciler-stub-{uuid.uuid4().hex}",
            task_queue=SCHEDULING_TASK_QUEUE,
            result_type=ReconcileResult,
        )


async def test_the_workflow_runs_the_three_activities_in_order_and_sums_their_counts(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []

    result = await _run_with_stubs(temporal_env, _stubs(calls))

    assert calls == ACTIVITY_ORDER
    assert result == ReconcileResult(pending=2, billing_voids=3, waitlist=4, no_shows=5)


@pytest.mark.parametrize("failing", ACTIVITY_ORDER)
async def test_one_failing_activity_does_not_stop_the_others(
    failing: str, temporal_env: WorkflowEnvironment
) -> None:
    calls: list[str] = []

    result = await _run_with_stubs(temporal_env, _stubs(calls, fail=failing))

    assert calls == ACTIVITY_ORDER  # every activity was still attempted, in order
    expected = ReconcileResult(
        pending=0 if failing == ACT_RECONCILE_PENDING else 2,
        billing_voids=0 if failing == ACT_RECONCILE_PENDING else 3,
        waitlist=0 if failing == ACT_RECONCILE_WAITLIST else 4,
        no_shows=0 if failing == ACT_RECONCILE_NO_SHOWS else 5,
    )
    assert result == expected


# --- one real pass through the real worker (also the recorder's scenario) ------------------


async def _real_pass(
    env: WorkflowEnvironment,
    mesh: httpx.AsyncClient,
    mongo: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
):
    """Seed one stuck pending appointment (verified billing), one entry stuck
    matched to a failed booking and one confirmed appointment whose slot ended
    a minute ago, then run `ReconcilerWorkflow` once through the real worker
    and activities (threshold 0, so fresh rows qualify). Returns the handle, the
    result and the seeded rows."""
    stuck = await seed_appointment(scope)
    await seed_billing(mesh, stuck)
    failed = await seed_appointment(scope, status="failed", failure_reason="BillingRejected")
    entry = await seed_matched_entry(scope, failed.appointment_id, provider_id=failed.provider_id)
    missed = await seed_appointment(
        scope,
        status="confirmed",
        start=datetime.now(timezone.utc) - timedelta(minutes=31),
    )
    acts = reconciler(env.client, mesh, mongo)
    async with Worker(
        env.client,
        task_queue=SCHEDULING_TASK_QUEUE,
        workflows=[ReconcilerWorkflow],
        activities=acts.all(),
    ):
        handle = await env.client.start_workflow(
            RECONCILER_WORKFLOW,
            id=f"reconciler-real-{uuid.uuid4().hex}",
            task_queue=SCHEDULING_TASK_QUEUE,
            result_type=ReconcileResult,
        )
        result = await handle.result()
    return handle, result, stuck, entry, missed


async def test_a_real_run_fails_the_stuck_booking_resolves_the_entry_and_marks_the_no_show(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    _handle, result, stuck, entry, missed = await _real_pass(
        temporal_env, mesh, mongo_client, scope, client
    )

    assert result.pending >= 1 and result.waitlist >= 1 and result.no_shows >= 1
    row = await appointment_row(scope.engine, stuck.appointment_id)
    assert (row.status, row.failure_reason) == ("failed", "workflow_lost")
    assert await billing_status(client, scope.world, stuck.appointment_id) == "voided"
    resolved = await entry_row(scope.engine, entry)
    assert (resolved.status, resolved.failure_reason) == ("failed", "BillingRejected")
    assert (await appointment_row(scope.engine, missed.appointment_id)).status == "no_show"


# --- replay ---------------------------------------------------------------------------------


async def test_the_committed_reconciler_history_replays_against_the_current_workflow() -> None:
    result = await Replayer(workflows=[ReconcilerWorkflow]).replay_workflow(_load(HISTORY))
    assert result.replay_failure is None


def test_the_committed_history_ran_the_three_activities_in_order_without_a_patch() -> None:
    assert _activity_types(HISTORY) == ACTIVITY_ORDER
    assert _events(HISTORY)[-1]["eventType"] == "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED"
    # A new workflow type: no workflow.patched marker is expected yet.
    assert _patch_ids(HISTORY) == []


def test_the_committed_history_carries_no_coverage_token_or_stack_trace() -> None:
    raw = (HISTORIES / HISTORY).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{HISTORY} contains {forbidden!r}"
    assert "stackTrace" not in raw


# --- the gate bites: reordering or dropping an activity without a patch breaks replay -------


@workflow.defn(name=RECONCILER_WORKFLOW, sandboxed=False)
class _NoShowsBeforeTheWaitlist:
    """The workflow with two activities swapped (a command-sequence change made
    WITHOUT workflow.patched): must not replay the recorded history."""

    @workflow.run
    async def run(self) -> ReconcileResult:
        pending = await workflow.execute_activity(
            ACT_RECONCILE_PENDING, result_type=ReconcilePendingResult, **MAINTENANCE_ACTIVITY
        )
        no_shows = await workflow.execute_activity(
            ACT_RECONCILE_NO_SHOWS, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
        )
        waitlist = await workflow.execute_activity(
            ACT_RECONCILE_WAITLIST, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
        )
        return ReconcileResult(
            pending=pending.failed,
            billing_voids=pending.billing_voids,
            waitlist=waitlist.processed,
            no_shows=no_shows.processed,
        )


@workflow.defn(name=RECONCILER_WORKFLOW, sandboxed=False)
class _WithoutTheNoShowSweep:
    @workflow.run
    async def run(self) -> ReconcileResult:
        pending = await workflow.execute_activity(
            ACT_RECONCILE_PENDING, result_type=ReconcilePendingResult, **MAINTENANCE_ACTIVITY
        )
        waitlist = await workflow.execute_activity(
            ACT_RECONCILE_WAITLIST, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
        )
        return ReconcileResult(
            pending=pending.failed,
            billing_voids=pending.billing_voids,
            waitlist=waitlist.processed,
            no_shows=0,
        )


@pytest.mark.parametrize("variant", [_NoShowsBeforeTheWaitlist, _WithoutTheNoShowSweep])
async def test_an_unpatched_command_sequence_edit_fails_replay(variant: type) -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[variant]).replay_workflow(_load(HISTORY))


# --- recorder (opt-in) --------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_reconciler_history(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """One reconcile pass over seeded stuck rows through the real worker."""
    target = HISTORIES / HISTORY
    handle, result, *_ = await _real_pass(temporal_env, mesh, mongo_client, scope, client)
    assert result.pending >= 1 and result.waitlist >= 1 and result.no_shows >= 1

    history = await handle.fetch_history()
    # The fresh run must replay against the current workflow either way.
    fresh = await Replayer(workflows=[ReconcilerWorkflow]).replay_workflow(history)
    assert fresh.replay_failure is None
    if target.exists():
        # A committed history is frozen: older runs are replayed against it, so a
        # re-run never overwrites it (a deliberate change records a NEW file).
        return
    document = _scrub(history.to_json_dict())
    HISTORIES.mkdir(exist_ok=True)
    target.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
