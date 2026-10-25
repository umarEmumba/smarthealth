"""VisitCompletedWorkflow's `workflow.patched` fourth branch replays safely
(plan 02.1-09; M1, VISIT-04, T-02.1-37).

`VISIT_COMPLETE_APPOINTMENT_PATCH_ID` adds a concurrent branch that completes
the appointment in Scheduling.

* `visit_completed_v1.json` (recorded in plan 02-11, before the change: three
  activities, no marker) must keep replaying against the patched workflow.
* `visit_completed_p21.json` (recorded with it: the `core_patch` marker and a
  scheduled `visit.complete_appointment`) proves the new path.

The suffix `p21` marks "recorded with the plan 02.1-0x patches" instead of a
version number. Both fixtures are ids and plain strings only; the checks below
re-assert that on every run.

Re-record (only for a deliberate, patched change; keep the older files):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_visit_completion.py -k record
"""

from __future__ import annotations

import asyncio
import json
import os

import httpx
import pytest
from httpx import AsyncClient
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from libs.temporal_policies import COMPENSATION_ACTIVITY, NOTIFY_ACTIVITY
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
    ACT_COMPLETE_APPOINTMENT,
    ACT_FINALIZE_BILLING,
    ACT_FOLLOWUP_INTENT,
    OUTCOME_COMPLETED,
    VISIT_COMPLETE_APPOINTMENT_PATCH_ID,
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    VisitCompletedResult,
    visit_completed_workflow_id,
)
from services.visit.workflows.visit_completed import VisitCompletedWorkflow, _branch
from tests.visit.conftest import widened_checkin_window
from tests.workflows.test_replay import (
    FORBIDDEN,
    HISTORIES,
    _events,
    _load,
    _patch_ids,
    _scrub,
)

PRE_PATCH = ["visit_completed_v1.json"]
PATCHED = ["visit_completed_p21.json"]


def _activity_types(name: str) -> list[str]:
    """Activity types in the order they were scheduled."""
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]


# --- the old history still replays -------------------------------------------------


@pytest.mark.parametrize("name", PRE_PATCH)
async def test_pre_patch_visit_history_replays_against_the_patched_workflow(name: str) -> None:
    assert VISIT_COMPLETE_APPOINTMENT_PATCH_ID not in _patch_ids(name)
    assert ACT_COMPLETE_APPOINTMENT not in _activity_types(name)
    result = await Replayer(workflows=[VisitCompletedWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


# --- the new history carries the marker and replays --------------------------------


@pytest.mark.parametrize("name", PATCHED)
async def test_patched_visit_history_replays_against_the_current_workflow(name: str) -> None:
    result = await Replayer(workflows=[VisitCompletedWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


def test_the_p21_history_took_the_fourth_branch() -> None:
    name = "visit_completed_p21.json"
    assert _patch_ids(name) == [VISIT_COMPLETE_APPOINTMENT_PATCH_ID]
    assert sorted(_activity_types(name)) == sorted(
        [ACT_FINALIZE_BILLING, ACT_FOLLOWUP_INTENT, ACT_ANALYTICS_INTENT, ACT_COMPLETE_APPOINTMENT]
    )


@pytest.mark.parametrize("name", [*PRE_PATCH, *PATCHED])
def test_visit_history_fixtures_carry_no_token_coverage_or_stack_trace(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


@pytest.mark.parametrize("name", PATCHED)
def test_patched_histories_have_a_core_patch_marker(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    assert raw.count('"markerName": "core_patch"') >= 1


# --- the gate bites: the same change WITHOUT the patch breaks replay ---------------


@workflow.defn(name=VISIT_COMPLETED_WORKFLOW, sandboxed=False)
class _UnpatchedFourthBranch:
    """The fourth branch added WITHOUT workflow.patched: must not replay a
    history recorded before it."""

    @workflow.run
    async def run(self, inp: VisitCompletedInput) -> VisitCompletedResult:
        billing, followup, analytics, appointment = await asyncio.gather(
            _branch(ACT_FINALIZE_BILLING, inp, COMPENSATION_ACTIVITY),
            _branch(ACT_FOLLOWUP_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_ANALYTICS_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_COMPLETE_APPOINTMENT, inp, COMPENSATION_ACTIVITY),
        )
        return VisitCompletedResult(
            outcome=OUTCOME_COMPLETED,
            billing=billing,
            followup=followup,
            analytics=analytics,
            appointment=appointment,
        )


async def test_the_unpatched_fourth_branch_fails_replay_of_the_pre_patch_history() -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_UnpatchedFourthBranch]).replay_workflow(
            _load("visit_completed_v1.json")
        )


# --- recorder (opt-in) --------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_visit_completion_patched_history(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """One visit completed through the gateway with the patched workflow: the
    appointment is checked in (widened window), the visit completes, all four
    branches run."""
    from tests.visit.conftest import _visit_world  # noqa: PLC0415
    from tests.visit.test_downstream import _completed_visit  # noqa: PLC0415

    with widened_checkin_window():
        async for vw in _visit_world(client, mesh):
            visit, _, completed = await _completed_visit(client, mesh, vw)
            assert completed.status_code == 200, completed.text
            handle = temporal_env.client.get_workflow_handle(
                visit_completed_workflow_id(visit["id"]), result_type=VisitCompletedResult
            )
            result = await handle.result()  # no timers: time skipping stays locked
            assert result.billing == "finalized"
            assert result.appointment == "completed"
            history = await handle.fetch_history()
            document = _scrub(history.to_json_dict())
            HISTORIES.mkdir(exist_ok=True)
            (HISTORIES / PATCHED[0]).write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n"
            )
