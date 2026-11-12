"""BookingWorkflow's two `workflow.patched` changes replay safely (plan 02.1-07;
S7, S5, D-19, T-02.1-27).

* `NOTIFY_AFTER_CONFIRM_PATCH_ID` (S7): the notification intent moves after
  confirm. `booking_confirmed_v1.json` (recorded before the change: notify, then
  confirm, no marker) must keep replaying; `booking_confirmed_p21.json`
  (recorded with it: marker, confirm, then notify) proves the new path.
* `BOOKING_FAILURE_WAITLIST_PATCH_ID` (S5): a top-level booking that fails
  after holding the slot starts a `WaitlistAdvanceWorkflow` child.
  `booking_billing_rejected_v1.json` (no marker, no child) must keep replaying;
  `booking_rejected_p21.json` has the marker and one child start.

The fixture suffix `p21` marks "recorded with the plan 02.1-07 patches" instead
of a version number. All four fixtures are ids and plain strings only; the
checks below re-assert that on every run.

Re-record (only for a deliberate, patched change; keep the older files):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_booking_patches.py -k record
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
from httpx import AsyncClient
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from libs.temporal_policies import FORWARD_ACTIVITY, NOTIFY_ACTIVITY
from services.scheduling.domain.contracts import (
    ACT_BILLING,
    ACT_CONFIRM,
    ACT_NOTIFY,
    ACT_RESERVE,
    ACT_VALIDATE,
    BOOKING_FAILURE_WAITLIST_PATCH_ID,
    BOOKING_WORKFLOW,
    NOTIFY_AFTER_CONFIRM_PATCH_ID,
    BookingInput,
    BookingResult,
    booking_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from services.scheduling.workflows.cancel import CancelWorkflow
from services.scheduling.workflows.reschedule import RescheduleWorkflow
from services.scheduling.workflows.waitlist_advance import WaitlistAdvanceWorkflow
from tests.scheduling.seed import World, auth
from tests.workflows.test_replay import (
    FORBIDDEN,
    HISTORIES,
    _child_types,
    _events,
    _load,
    _load_as_recorded,
    _patch_ids,
    _scrub,
)

PRE_PATCH = ["booking_confirmed_v1.json", "booking_billing_rejected_v1.json"]
PATCHED = ["booking_confirmed_p21.json", "booking_rejected_p21.json"]
ALL_WORKFLOWS = [BookingWorkflow, CancelWorkflow, RescheduleWorkflow, WaitlistAdvanceWorkflow]


def _activity_types(name: str) -> list[str]:
    """Activity types in the order they were scheduled."""
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]


# --- old histories still replay ---------------------------------------------------


@pytest.mark.parametrize("name", PRE_PATCH)
async def test_pre_patch_booking_history_replays_against_the_patched_workflow(name: str) -> None:
    assert NOTIFY_AFTER_CONFIRM_PATCH_ID not in _patch_ids(name)
    assert BOOKING_FAILURE_WAITLIST_PATCH_ID not in _patch_ids(name)
    assert _child_types(name) == []
    result = await Replayer(workflows=ALL_WORKFLOWS).replay_workflow(_load(name))
    assert result.replay_failure is None


def test_the_pre_patch_confirmed_history_notified_before_confirm() -> None:
    order = _activity_types("booking_confirmed_v1.json")
    assert order.index(ACT_NOTIFY) < order.index(ACT_CONFIRM)


# --- new histories carry the markers and replay -----------------------------------


@pytest.mark.parametrize("name", PATCHED)
async def test_patched_booking_history_replays_against_the_current_workflow(name: str) -> None:
    result = await Replayer(workflows=ALL_WORKFLOWS).replay_workflow(_load_as_recorded(name))
    assert result.replay_failure is None


def test_the_confirmed_p21_history_took_the_notify_after_confirm_path() -> None:
    name = "booking_confirmed_p21.json"
    assert _patch_ids(name) == [NOTIFY_AFTER_CONFIRM_PATCH_ID]
    order = _activity_types(name)
    assert order == [ACT_VALIDATE, ACT_RESERVE, ACT_BILLING, ACT_CONFIRM, ACT_NOTIFY]
    assert order.index(ACT_CONFIRM) < order.index(ACT_NOTIFY)
    assert _child_types(name) == []


def test_the_rejected_p21_history_took_the_waitlist_reoffer_path() -> None:
    name = "booking_rejected_p21.json"
    assert BOOKING_FAILURE_WAITLIST_PATCH_ID in _patch_ids(name)
    assert _child_types(name) == ["WaitlistAdvanceWorkflow"]
    assert ACT_NOTIFY not in _activity_types(name)
    assert ACT_CONFIRM not in _activity_types(name)


@pytest.mark.parametrize("name", [*PRE_PATCH, *PATCHED])
def test_booking_history_fixtures_carry_no_coverage_token_or_failing_prefix(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


@pytest.mark.parametrize("name", PATCHED)
def test_patched_histories_have_a_core_patch_marker(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    assert raw.count('"markerName": "core_patch"') >= 1


# --- the gate bites: the same move WITHOUT the patch breaks replay ---------------


@workflow.defn(name=BOOKING_WORKFLOW, sandboxed=False)
class _UnpatchedNotifyAfterConfirm:
    """The S7 change made WITHOUT workflow.patched: must not replay a history
    recorded before it."""

    @workflow.run
    async def run(self, inp: BookingInput) -> BookingResult:
        await workflow.execute_activity(ACT_VALIDATE, inp, **FORWARD_ACTIVITY)
        await workflow.execute_activity(ACT_RESERVE, inp, **FORWARD_ACTIVITY)
        await workflow.execute_activity(ACT_BILLING, inp, **FORWARD_ACTIVITY)
        await workflow.execute_activity(ACT_CONFIRM, inp, **FORWARD_ACTIVITY)
        await workflow.execute_activity(ACT_NOTIFY, inp, **NOTIFY_ACTIVITY)
        return BookingResult(outcome="confirmed")


async def test_the_unpatched_reorder_fails_replay_of_the_pre_patch_history() -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_UnpatchedNotifyAfterConfirm]).replay_workflow(
            _load("booking_confirmed_v1.json")
        )


# --- recorder (opt-in) ------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_booking_patch_histories(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """One confirmed and one billing-rejected booking through the gateway with
    the patched workflow."""
    world = booking_world
    start, end = world.local_day_bounds(world.last_day)
    slots = (
        await client.get(
            "/api/slots",
            params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
            headers=auth(world.admin),
        )
    ).json()
    # Built at runtime so the failing prefix is never a literal in this module;
    # it travels only in the HTTP body, never in workflow input.
    denied = "-".join(["DENY", uuid.uuid4().hex[:6]])
    HISTORIES.mkdir(exist_ok=True)

    for name, outcome, slot, coverage in (
        ("booking_rejected_p21.json", "failed", slots[0], denied),
        ("booking_confirmed_p21.json", "confirmed", slots[1], None),
    ):
        body = {"slot_id": slot["id"], "patient_id": str(world.patient_id)}
        if coverage is not None:
            body["coverage_ref"] = coverage
        resp = await client.post(
            "/api/appointments",
            json=body,
            headers=auth(world.front_desk) | {"Idempotency-Key": f"rec-{uuid.uuid4().hex}"},
        )
        assert resp.status_code == 202, resp.text
        handle = temporal_env.client.get_workflow_handle(
            booking_workflow_id(resp.json()["appointment_id"]), result_type=BookingResult
        )
        async with temporal_env.time_skipping_unlocked():
            assert (await handle.result()).outcome == outcome
        history = await handle.fetch_history()
        document = _scrub(history.to_json_dict())
        (HISTORIES / name).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")

