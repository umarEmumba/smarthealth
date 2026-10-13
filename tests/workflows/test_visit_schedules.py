"""Visit's sweeper Schedule and sweeper workflow (plan 02-11 Task 2; D-16,
research Pattern 8).

* `ensure_visit_schedules` against the Compose Temporal: the first call creates
  `{prefix}-downstream-sweeper` (overlap SKIP, configured interval, start
  action on `visit-tq`); a second call (worker restart) creates nothing and
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
from services.visit.domain.contracts import ACT_SWEEP, SWEEPER_WORKFLOW, SweepResult
from services.visit.schedules import ensure_visit_schedules
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
        with contextlib.suppress(RPCError):
            await compose_temporal.get_schedule_handle(f"{prefix}-downstream-sweeper").delete()


@pytest.mark.integration
async def test_ensure_visit_schedules_creates_the_sweeper_with_overlap_skip_then_is_a_no_op(
    compose_temporal: Client, id_prefix: str
) -> None:
    schedule_id = f"{id_prefix}-downstream-sweeper"

    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == [schedule_id]

    schedule = (await compose_temporal.get_schedule_handle(schedule_id).describe()).schedule
    assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP
    action = schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    assert action.workflow == SWEEPER_WORKFLOW
    assert action.id == f"{id_prefix}-downstream-sweeper-run"
    assert action.task_queue == VISIT_TASK_QUEUE
    assert [i.every for i in schedule.spec.intervals] == [
        timedelta(seconds=settings.sweeper_interval_seconds)
    ]

    # Worker restart: registering again creates nothing and raises nothing.
    assert await ensure_visit_schedules(compose_temporal, id_prefix=id_prefix) == []


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
