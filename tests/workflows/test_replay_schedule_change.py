"""ScheduleChangeWorkflow replays from a recorded history (plan 02.1-08 Task 3;
M2, D-19).

`histories/schedule_change_p21.json` is one real run (a template deactivation
through Provider's feed, the poller, and the workflow with its real activities on
the time-skipping test server): apply, a durable timer, apply. Replaying it with
`Replayer` re-runs `ScheduleChangeWorkflow` against the recorded events, so any
later edit that changes the command sequence without `workflow.patched` raises a
nondeterminism error here before it can break a run that is in flight.

The `p21` suffix marks "recorded in phase 02.1" (the workflow type is new, so the
history carries no patch marker). The fixture is ids and plain strings only:
workflow input carries no coverage reference or token, and the recorder strips
stack traces; the checks below re-assert that on every run.

Re-record (only for a deliberate, patched change; keep the older files):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_schedule_change.py -k record
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from temporalio import workflow
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from libs.temporal_policies import MAINTENANCE_ACTIVITY
from services.scheduling.domain.contracts import (
    ACT_APPLY_SCHEDULE_CHANGE,
    SCHEDULE_CHANGE_WORKFLOW,
    ScheduleChangeInput,
    ScheduleChangeResult,
    schedule_change_workflow_id,
)
from services.scheduling.workflows.schedule_change import (
    ScheduleChangePollerWorkflow,
    ScheduleChangeWorkflow,
)
from tests.scheduling.schedule_feed import drain_feed, events_for
from tests.scheduling.seed import World, auth
from tests.workflows.test_replay import FORBIDDEN, HISTORIES, _events, _load, _patch_ids, _scrub

HISTORY = "schedule_change_p21.json"


def _event_types() -> list[str]:
    return [e["eventType"] for e in _events(HISTORY)]


async def test_the_committed_schedule_change_history_replays_against_the_current_workflow() -> None:
    result = await Replayer(
        workflows=[ScheduleChangeWorkflow, ScheduleChangePollerWorkflow]
    ).replay_workflow(_load(HISTORY))
    assert result.replay_failure is None


def test_the_committed_history_ran_apply_then_a_timer_then_apply() -> None:
    types = _event_types()
    scheduled = [
        i for i, t in enumerate(types) if t == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]
    timers = [i for i, t in enumerate(types) if t == "EVENT_TYPE_TIMER_STARTED"]
    assert len(scheduled) == 2 and len(timers) == 1
    assert scheduled[0] < timers[0] < scheduled[1]
    names = [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(HISTORY)
        if e["eventType"] == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]
    assert names == [ACT_APPLY_SCHEDULE_CHANGE, ACT_APPLY_SCHEDULE_CHANGE]
    assert types[-1] == "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED"
    # A new workflow type: no workflow.patched marker is expected yet.
    assert _patch_ids(HISTORY) == []


def test_the_committed_history_carries_no_coverage_token_or_stack_trace() -> None:
    raw = (HISTORIES / HISTORY).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{HISTORY} contains {forbidden!r}"
    assert "stackTrace" not in raw


# --- the gate bites: removing the timer breaks replay ----------------------------------


@workflow.defn(name=SCHEDULE_CHANGE_WORKFLOW, sandboxed=False)
class _WithoutTheTimer:
    """The workflow with its durable timer removed (a command-sequence change
    made WITHOUT workflow.patched): must not replay the recorded history."""

    @workflow.run
    async def run(self, inp: ScheduleChangeInput) -> ScheduleChangeResult:
        for pass_number in (1, 2):
            await workflow.execute_activity(
                ACT_APPLY_SCHEDULE_CHANGE,
                ScheduleChangeInput(
                    event_id=inp.event_id,
                    provider_id=inp.provider_id,
                    change_type=inp.change_type,
                    template_id=inp.template_id,
                    pass_number=pass_number,
                    second_pass_after_minutes=inp.second_pass_after_minutes,
                ),
                result_type=ScheduleChangeResult,
                **MAINTENANCE_ACTIVITY,
            )
        return ScheduleChangeResult(withdrawn=0, flagged=0)


async def test_removing_the_timer_without_a_patch_fails_replay() -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_WithoutTheTimer]).replay_workflow(_load(HISTORY))


# --- recorder (opt-in) ----------------------------------------------------------------


@pytest_asyncio.fixture
async def _drained_feed(mesh: httpx.AsyncClient) -> None:
    await drain_feed(mesh)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_schedule_change_history(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    _drained_feed: None,
    booking_world: World,
    scheduling_worker: object,
    schedule_changes: Any,
    temporal_env: WorkflowEnvironment,
) -> None:
    """One template deactivation through the whole chain: Provider's event, the
    poller, the real workflow and activities."""
    world = booking_world
    resp = await client.post(
        f"/api/providers/{world.provider_id}/availability/"
        f"{world.template_ids[world.last_day.weekday()]}/deactivate",
        headers=auth(world.admin),
    )
    assert resp.status_code == 200, resp.text
    (event,) = await events_for(mesh, world.provider_id)

    polled = await ActivityEnvironment().run(schedule_changes.poll_schedule_changes)
    assert polled.processed >= 1
    handle = temporal_env.client.get_workflow_handle(
        schedule_change_workflow_id(event["id"]), result_type=ScheduleChangeResult
    )
    async with temporal_env.time_skipping_unlocked():
        result = await handle.result()
    assert result.withdrawn > 0

    history = await handle.fetch_history()
    document = _scrub(history.to_json_dict())
    HISTORIES.mkdir(exist_ok=True)
    (HISTORIES / HISTORY).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
