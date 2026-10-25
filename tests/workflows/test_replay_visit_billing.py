"""VisitCompletedWorkflow's `workflow.patched` billing retry change replays
safely (plan 02.1-10; S4, BILL-02, VISIT-04).

`BILLING_RETRY_PATCH_ID` switches the billing branch from the bounded
`COMPENSATION_ACTIVITY` options to the open-ended `BILLING_FINALIZE_ACTIVITY`.

* `visit_completed_v1.json` (plan 02-11: three activities, no marker) and
  `visit_completed_p21.json` (plan 02.1-09: one marker,
  `visit-complete-appointment-v1`) are frozen and must keep replaying against
  the final workflow. Neither is ever re-recorded.
* `visit_completed_billing_p21.json` (recorded with this change) carries BOTH
  markers and proves the current path.

Honest note on what the replayer checks: Temporal's replay matches the
sequence of commands (activity types, markers, timers), not the retry or timeout
options of a scheduled activity. An options-only edit would therefore replay
even without `workflow.patched`; the patch is the project convention (D-19)
and what makes the version of a run visible in its history. The tests below pin
both facts: the marker is recorded and replays, and the marker is positioned
before the branch commands (an out-of-order marker breaks replay).

Re-record (only for a deliberate, patched change; keep the older files):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_visit_billing.py -k record
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
    BILLING_RETRY_PATCH_ID,
    OUTCOME_COMPLETED,
    VISIT_COMPLETE_APPOINTMENT_PATCH_ID,
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    VisitCompletedResult,
    visit_completed_workflow_id,
)
from services.visit.domain.policies import BILLING_FINALIZE_ACTIVITY
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

PRE_PATCH = ["visit_completed_v1.json", "visit_completed_p21.json"]
PATCHED = ["visit_completed_billing_p21.json"]


def _activity_types(name: str) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]


# --- the older histories still replay ------------------------------------------------


@pytest.mark.parametrize("name", PRE_PATCH)
async def test_older_visit_histories_replay_against_the_final_workflow(name: str) -> None:
    assert BILLING_RETRY_PATCH_ID not in _patch_ids(name)
    result = await Replayer(workflows=[VisitCompletedWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


def test_the_older_fixtures_are_the_frozen_ones() -> None:
    assert _patch_ids("visit_completed_v1.json") == []
    assert _patch_ids("visit_completed_p21.json") == ["visit-complete-appointment-v1"]
    assert ACT_COMPLETE_APPOINTMENT not in _activity_types("visit_completed_v1.json")


# --- the new history carries both markers and replays --------------------------------


@pytest.mark.parametrize("name", PATCHED)
async def test_billing_patched_history_replays_against_the_current_workflow(name: str) -> None:
    result = await Replayer(workflows=[VisitCompletedWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


def test_the_billing_history_carries_both_patch_markers() -> None:
    name = "visit_completed_billing_p21.json"
    # Both ids are durable (they live in recorded histories): renaming one is a migration.
    assert BILLING_RETRY_PATCH_ID == "visit-billing-retry-v1"
    assert VISIT_COMPLETE_APPOINTMENT_PATCH_ID == "visit-complete-appointment-v1"
    assert _patch_ids(name) == ["visit-billing-retry-v1", "visit-complete-appointment-v1"]
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
def test_patched_histories_have_core_patch_markers(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    assert raw.count('"markerName": "core_patch"') >= 2


# --- what the gate guards -----------------------------------------------------------


@workflow.defn(name=VISIT_COMPLETED_WORKFLOW, sandboxed=False)
class _OptionsOnlyEditWithoutPatch:
    """The billing options changed WITHOUT workflow.patched (the appointment
    patch kept): no command changes, so the replayer cannot tell."""

    @workflow.run
    async def run(self, inp: VisitCompletedInput) -> VisitCompletedResult:
        branches = [
            _branch(ACT_FINALIZE_BILLING, inp, BILLING_FINALIZE_ACTIVITY),
            _branch(ACT_FOLLOWUP_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_ANALYTICS_INTENT, inp, NOTIFY_ACTIVITY),
        ]
        patched = workflow.patched(VISIT_COMPLETE_APPOINTMENT_PATCH_ID)
        if patched:
            branches.append(_branch(ACT_COMPLETE_APPOINTMENT, inp, COMPENSATION_ACTIVITY))
        results = await asyncio.gather(*branches)
        return VisitCompletedResult(
            outcome=OUTCOME_COMPLETED,
            billing=results[0],
            followup=results[1],
            analytics=results[2],
            appointment=results[3] if patched else "",
        )


@pytest.mark.parametrize("name", PRE_PATCH)
async def test_an_options_only_edit_is_invisible_to_replay_so_the_patch_is_convention(
    name: str,
) -> None:
    """Documents WHY this change cannot have a 'fails without the patch' proof:
    Temporal replay matches commands, not activity options. The patch marker is
    the project's versioning convention (and makes a run's version visible in
    its history); if a future SDK starts comparing options, this test fails and
    the patch becomes load-bearing."""
    result = await Replayer(workflows=[_OptionsOnlyEditWithoutPatch]).replay_workflow(_load(name))
    assert result.replay_failure is None


@workflow.defn(name=VISIT_COMPLETED_WORKFLOW, sandboxed=False)
class _BillingMarkerAfterTheBranches:
    """The billing patch call moved AFTER the branch activities were scheduled:
    the recorded marker is no longer the first command."""

    @workflow.run
    async def run(self, inp: VisitCompletedInput) -> VisitCompletedResult:
        branches = [
            _branch(ACT_FINALIZE_BILLING, inp, BILLING_FINALIZE_ACTIVITY),
            _branch(ACT_FOLLOWUP_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_ANALYTICS_INTENT, inp, NOTIFY_ACTIVITY),
        ]
        workflow.patched(VISIT_COMPLETE_APPOINTMENT_PATCH_ID)
        branches.append(_branch(ACT_COMPLETE_APPOINTMENT, inp, COMPENSATION_ACTIVITY))
        tasks = [asyncio.ensure_future(b) for b in branches]
        await asyncio.sleep(0)  # let the branch activities be scheduled first
        workflow.patched(BILLING_RETRY_PATCH_ID)
        results = await asyncio.gather(*tasks)
        return VisitCompletedResult(
            outcome=OUTCOME_COMPLETED,
            billing=results[0],
            followup=results[1],
            analytics=results[2],
            appointment=results[3],
        )


async def test_a_reordered_marker_fails_replay_of_the_billing_history() -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_BillingMarkerAfterTheBranches]).replay_workflow(
            _load("visit_completed_billing_p21.json")
        )


# --- recorder (opt-in) --------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_visit_billing_patched_history(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """One visit completed through the gateway with the final workflow: both
    patch markers are recorded and all four branches run."""
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
