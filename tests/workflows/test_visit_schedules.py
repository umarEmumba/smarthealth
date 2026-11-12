"""Visit's Schedules and the retired sweeper (plan 02-11 Task 2, plan 03-12 Task 1;
D-16, D-03, research Pattern 8).

* `ensure_visit_schedules` against the Compose Temporal: the first call creates only
  `{prefix}-reconciler` (overlap SKIP, configured interval, start action on `visit-tq`);
  a second call (worker restart) creates nothing and raises nothing. Each test uses its
  own `test-<uuid>` prefix and deletes its schedules afterwards, so the real
  `visit-*` Schedules are never touched.
* `retire_sweeper_schedule` deletes a `{prefix}-downstream-sweeper` Schedule created
  with the previous (Phase 2) definition, returns False when it is already gone, and
  leaves the reconciler Schedule alone.
* `VisitDownstreamSweeperWorkflow` (the tombstone) in the time-skipping env runs
  `ACT_SWEEP` exactly once and returns its `SweepResult`; the real tombstone activity is
  exercised in `tests/visit/test_downstream.py`.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import pytest_asyncio
from temporalio import activity
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.activities.downstream import VisitActivities
from services.visit.config import settings
from services.visit.domain.contracts import (
    ACT_RECONCILE,
    SWEEPER_WORKFLOW,
    VISIT_RECONCILER_WORKFLOW,
    SweepResult,
)
from services.visit.schedules import ensure_visit_schedules, retire_sweeper_schedule
from services.visit.workflows.reconciler import VisitReconcilerWorkflow
from services.visit.workflows.sweeper import VisitDownstreamSweeperWorkflow

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")


@pytest_asyncio.fixture
async def compose_temporal() -> AsyncIterator[Client]:
    yield await Client.connect(TEMPORAL_ADDRESS)


@pytest_asyncio.fixture
async def id_prefix(compose_temporal: Client) -> AsyncIterator[str]:
    prefix = f"test-{uuid.uuid4().hex[:12]}"
    try:
        yield prefix
    finally:
        for suffix in ("downstream-sweeper", "reconciler"):
            with contextlib.suppress(RPCError):
                await compose_temporal.get_schedule_handle(f"{prefix}-{suffix}").delete()


async def _create_phase2_sweeper_schedule(client: Client, prefix: str) -> None:
    """The Schedule as plan 02-11 registered it (the definition that is on the live
    server), created inline because the code no longer defines it."""
    await client.create_schedule(
        f"{prefix}-downstream-sweeper",
        Schedule(
            action=ScheduleActionStartWorkflow(
                SWEEPER_WORKFLOW, id=f"{prefix}-downstream-sweeper-run", task_queue=VISIT_TASK_QUEUE
            ),
            spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        ),
    )


async def _exists(client: Client, schedule_id: str) -> bool:
    try:
        await client.get_schedule_handle(schedule_id).describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            return False
        raise
    return True


@pytest.mark.integration
async def test_ensure_visit_schedules_creates_only_the_reconciler_with_overlap_skip_then_is_a_no_op(
    compose_temporal: Client, id_prefix: str
) -> None:
    reconciler_id = f"{id_prefix}-reconciler"

    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == [reconciler_id]
    assert not await _exists(compose_temporal, f"{id_prefix}-downstream-sweeper")

    schedule = (await compose_temporal.get_schedule_handle(reconciler_id).describe()).schedule
    assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP
    action = schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    assert action.workflow == VISIT_RECONCILER_WORKFLOW
    assert action.id == f"{id_prefix}-reconciler-run"
    assert action.task_queue == VISIT_TASK_QUEUE
    assert [i.every for i in schedule.spec.intervals] == [
        timedelta(seconds=settings.reconcile_interval_seconds)
    ]

    # Worker restart: registering again creates nothing and raises nothing.
    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == []


@pytest.mark.integration
async def test_retire_sweeper_schedule_deletes_the_old_schedule_once_and_only_that_one(
    compose_temporal: Client, id_prefix: str
) -> None:
    """T-3-38: the exact `{prefix}-downstream-sweeper` id is deleted; the reconciler
    Schedule next to it is untouched, and a second call is a harmless False."""
    sweeper_id = f"{id_prefix}-downstream-sweeper"
    reconciler_id = f"{id_prefix}-reconciler"
    await _create_phase2_sweeper_schedule(compose_temporal, id_prefix)
    # An upgraded deployment: the reconciler is registered and the old sweeper remains.
    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == [reconciler_id]
    assert await _exists(compose_temporal, sweeper_id)

    assert await retire_sweeper_schedule(compose_temporal, id_prefix=id_prefix) is True
    assert not await _exists(compose_temporal, sweeper_id)
    assert await _exists(compose_temporal, reconciler_id)
    assert (await compose_temporal.get_schedule_handle(reconciler_id).describe()).schedule.state.paused is False

    assert await retire_sweeper_schedule(compose_temporal, id_prefix=id_prefix) is False
    assert await _exists(compose_temporal, reconciler_id)


@pytest.mark.integration
async def test_retire_sweeper_schedule_on_a_server_that_never_had_it_returns_false(
    compose_temporal: Client, id_prefix: str
) -> None:
    assert await retire_sweeper_schedule(compose_temporal, id_prefix=id_prefix) is False


async def test_sweeper_tombstone_workflow_runs_the_retired_activity_once(
    temporal_env: WorkflowEnvironment,
) -> None:
    """The retired workflow still runs under a worker with the REAL tombstone activity
    (it touches no dependency, so none is wired): one activity, zero processed."""
    acts = VisitActivities(None, None, None, None, None)  # type: ignore[arg-type]

    async with Worker(
        temporal_env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitDownstreamSweeperWorkflow],
        activities=[acts.sweep_unstarted],
    ):
        result = await temporal_env.client.execute_workflow(
            SWEEPER_WORKFLOW,
            id=f"sweeper-{uuid.uuid4().hex}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=SweepResult,
        )

    assert result == SweepResult(processed=0)


async def test_reconciler_workflow_runs_the_reconcile_activity_once(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[int] = []

    @activity.defn(name=ACT_RECONCILE)
    async def reconcile_stub() -> SweepResult:
        calls.append(1)
        return SweepResult(processed=2)

    async with Worker(
        temporal_env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitReconcilerWorkflow],
        activities=[reconcile_stub],
    ):
        result = await temporal_env.client.execute_workflow(
            VISIT_RECONCILER_WORKFLOW,
            id=f"reconciler-{uuid.uuid4().hex}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=SweepResult,
        )

    assert result == SweepResult(processed=2)
    assert calls == [1]
