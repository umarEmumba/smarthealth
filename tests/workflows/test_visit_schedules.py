"""Visit's sweeper Schedule and sweeper workflow (plan 02-11 Task 2; D-16,
research Pattern 8).

* `ensure_visit_schedules` against the Compose Temporal: the first call creates
  `{prefix}-downstream-sweeper` and (plan 02.1-10) `{prefix}-reconciler` (overlap
  SKIP, configured interval, start action on `visit-tq`); a second call (worker restart) creates nothing and
  raises nothing. Each test uses its own `test-<uuid>` prefix and deletes its
  schedule afterwards, so the real `visit-downstream-sweeper` is never touched.
* `VisitDownstreamSweeperWorkflow` in the time-skipping env runs `ACT_SWEEP`
  exactly once and returns its `SweepResult`.
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
from temporalio.client import Client, ScheduleActionStartWorkflow, ScheduleOverlapPolicy
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.config import settings
from services.visit.domain.contracts import (
    ACT_RECONCILE,
    ACT_SWEEP,
    SWEEPER_WORKFLOW,
    VISIT_RECONCILER_WORKFLOW,
    SweepResult,
)
from services.visit.schedules import ensure_visit_schedules
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


@pytest.mark.integration
async def test_ensure_visit_schedules_creates_sweeper_and_reconciler_with_overlap_skip_then_is_a_no_op(
    compose_temporal: Client, id_prefix: str
) -> None:
    sweeper_id = f"{id_prefix}-downstream-sweeper"
    reconciler_id = f"{id_prefix}-reconciler"

    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == [
        sweeper_id,
        reconciler_id,
    ]

    for schedule_id, workflow_type, run_id, interval in (
        (
            sweeper_id,
            SWEEPER_WORKFLOW,
            f"{id_prefix}-downstream-sweeper-run",
            settings.sweeper_interval_seconds,
        ),
        (
            reconciler_id,
            VISIT_RECONCILER_WORKFLOW,
            f"{id_prefix}-reconciler-run",
            settings.reconcile_interval_seconds,
        ),
    ):
        schedule = (await compose_temporal.get_schedule_handle(schedule_id).describe()).schedule
        assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP
        action = schedule.action
        assert isinstance(action, ScheduleActionStartWorkflow)
        assert action.workflow == workflow_type
        assert action.id == run_id
        assert action.task_queue == VISIT_TASK_QUEUE
        assert [i.every for i in schedule.spec.intervals] == [timedelta(seconds=interval)]

    # Worker restart: registering again creates nothing and raises nothing.
    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == []


@pytest.mark.integration
async def test_reconciler_schedule_is_registered_when_only_the_sweeper_already_exists(
    compose_temporal: Client, id_prefix: str
) -> None:
    """Upgrading a deployment that predates the reconciler: the existing sweeper
    Schedule is left alone and only the missing reconciler is created."""
    from temporalio.client import (  # noqa: PLC0415
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleIntervalSpec,
        SchedulePolicy,
        ScheduleSpec,
    )

    await compose_temporal.create_schedule(
        f"{id_prefix}-downstream-sweeper",
        Schedule(
            action=ScheduleActionStartWorkflow(
                SWEEPER_WORKFLOW, id=f"{id_prefix}-downstream-sweeper-run", task_queue=VISIT_TASK_QUEUE
            ),
            spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        ),
    )
    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == [
        f"{id_prefix}-reconciler"
    ]


async def test_sweeper_workflow_runs_the_sweep_activity_once(temporal_env: WorkflowEnvironment) -> None:
    calls: list[int] = []

    @activity.defn(name=ACT_SWEEP)
    async def sweep_stub() -> SweepResult:
        calls.append(1)
        return SweepResult(processed=3)

    async with Worker(
        temporal_env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitDownstreamSweeperWorkflow],
        activities=[sweep_stub],
    ):
        result = await temporal_env.client.execute_workflow(
            SWEEPER_WORKFLOW,
            id=f"sweeper-{uuid.uuid4().hex}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=SweepResult,
        )

    assert result == SweepResult(processed=3)
    assert calls == [1]


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
