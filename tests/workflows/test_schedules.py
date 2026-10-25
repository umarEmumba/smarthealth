"""Idempotent Temporal Schedule registration against the Compose Temporal
(plan 02-07 Task 2; D-03, research Pattern 8 / Code Example 4, T-02-35).

`ensure_schedules` runs at every worker start, so a second call must create
nothing and raise nothing. Each test uses its own `test-<uuid>` id prefix and
deletes its schedules afterwards, so the real `scheduling-*` schedules are
never touched.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import pytest_asyncio
from temporalio.client import (
    Client,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
)
from temporalio.service import RPCError

from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.config import settings
from services.scheduling.schedules import ensure_schedules

pytestmark = pytest.mark.integration

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
        for suffix in (
            "hold-reaper",
            "pending-starter",
            "schedule-change-poller",
            "reconciler",
        ):
            with contextlib.suppress(RPCError):
                await compose_temporal.get_schedule_handle(f"{prefix}-{suffix}").delete()


async def test_ensure_schedules_creates_all_with_overlap_skip_then_is_a_no_op(
    compose_temporal: Client, id_prefix: str
) -> None:
    created = await ensure_schedules(compose_temporal, id_prefix=id_prefix)

    assert sorted(created) == sorted(
        [
            f"{id_prefix}-hold-reaper",
            f"{id_prefix}-pending-starter",
            f"{id_prefix}-schedule-change-poller",
            f"{id_prefix}-reconciler",
        ]
    )
    expected = {
        f"{id_prefix}-hold-reaper": (
            "HoldReaperWorkflow",
            f"{id_prefix}-hold-reaper-run",
            timedelta(seconds=settings.reaper_interval_seconds),
        ),
        f"{id_prefix}-pending-starter": (
            "PendingBookingStarterWorkflow",
            f"{id_prefix}-pending-starter-run",
            timedelta(seconds=settings.starter_interval_seconds),
        ),
        # Plan 02.1-08 (M2): drains Provider's schedule-change feed.
        f"{id_prefix}-schedule-change-poller": (
            "ScheduleChangePollerWorkflow",
            f"{id_prefix}-schedule-change-poller-run",
            timedelta(seconds=settings.schedule_change_poll_seconds),
        ),
        # Plan 02.1-11 (S4): stuck pending/matched rows, billing orphans, no-shows.
        f"{id_prefix}-reconciler": (
            "ReconcilerWorkflow",
            f"{id_prefix}-reconciler-run",
            timedelta(seconds=settings.reconciler_interval_seconds),
        ),
    }
    for schedule_id, (workflow_type, run_id, every) in expected.items():
        schedule = (await compose_temporal.get_schedule_handle(schedule_id).describe()).schedule
        assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP
        action = schedule.action
        assert isinstance(action, ScheduleActionStartWorkflow)
        assert action.workflow == workflow_type
        assert action.id == run_id
        assert action.task_queue == SCHEDULING_TASK_QUEUE
        assert [i.every for i in schedule.spec.intervals] == [every]

    # Worker restart: registering again creates nothing and raises nothing.
    assert await ensure_schedules(compose_temporal, id_prefix=id_prefix) == []
    listed = [
        s.id async for s in await compose_temporal.list_schedules() if s.id.startswith(id_prefix)
    ]
    assert len(listed) <= 4  # visibility may lag; never more than the four
