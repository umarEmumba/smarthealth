"""Recorded histories of the reminder workflows replay safely (plan 03-06;
NOTIF-02, NOTIF-03, D-19 convention, T-3-31).

* `reminder_v1.json`: a reminder with two leads (24 h, 2 h) for a slot two days
  ahead, rescheduled by a signal to a slot five hours ahead (the 24 h lead is then
  past and skipped), and one lead (2 h) fired after the signal.
* `followup_v1.json`: a follow-up that slept its delay and enqueued once.

Both are synthetic ids and ISO timestamps only; the checks below re-assert that on
every run. The `_v1` suffix is the version of the command sequence: any later
change to either workflow's command sequence needs a `workflow.patched` id and a
NEW recorded history (keep these files), exactly like the booking patches.

Re-record (only for a deliberate, patched change; keep the older files):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay_reminder.py -k record
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from services.notification.contracts import (
    FOLLOWUP_WORKFLOW,
    FollowUpInput,
    ReminderInput,
    ReminderResult,
    followup_workflow_id,
    reminder_workflow_id,
)
from services.notification.reminders import reschedule_reminder, start_followup, start_reminder
from services.notification.workflows.reminder import FollowUpReminderWorkflow, ReminderWorkflow
from tests.workflows.test_replay import FORBIDDEN, HISTORIES, _events, _load, _scrub
from tests.workflows.test_reminder_workflow import _result, _settle, running

REMINDER_HISTORY = "reminder_v1.json"
FOLLOWUP_HISTORY = "followup_v1.json"
WORKFLOWS = [ReminderWorkflow, FollowUpReminderWorkflow]


def _event_types(name: str) -> list[str]:
    return [e.get("eventType", "") for e in _events(name)]


def _signal_names(name: str) -> list[str]:
    return [
        e["workflowExecutionSignaledEventAttributes"]["signalName"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED"
    ]


def _activity_types(name: str) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]


# --- the recorded histories replay against the current code -------------------------


@pytest.mark.parametrize("name", [REMINDER_HISTORY, FOLLOWUP_HISTORY])
async def test_committed_reminder_history_replays_against_current_code(name: str) -> None:
    result = await Replayer(workflows=WORKFLOWS).replay_workflow(_load(name))
    assert result.replay_failure is None


def test_the_reminder_history_has_one_reschedule_signal_and_one_fired_lead() -> None:
    assert _signal_names(REMINDER_HISTORY) == ["reschedule"]
    assert _activity_types(REMINDER_HISTORY) == ["notification.enqueue"]
    types = _event_types(REMINDER_HISTORY)
    assert types.index("EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED") < types.index(
        "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    )
    assert "EVENT_TYPE_TIMER_STARTED" in types or "EVENT_TYPE_TIMER_FIRED" in types


def test_the_followup_history_slept_then_enqueued_once() -> None:
    types = _event_types(FOLLOWUP_HISTORY)
    assert types.count("EVENT_TYPE_TIMER_FIRED") == 1
    assert _activity_types(FOLLOWUP_HISTORY) == ["notification.enqueue"]


@pytest.mark.parametrize("name", [REMINDER_HISTORY, FOLLOWUP_HISTORY])
def test_reminder_history_fixtures_carry_no_token_coverage_or_stack_trace(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


# --- the gate bites: a changed command sequence breaks replay -------------------------


@workflow.defn(name=FOLLOWUP_WORKFLOW, sandboxed=False)
class _FollowUpWithAnExtraTimer:
    """The follow-up with one extra timer added WITHOUT workflow.patched."""

    @workflow.run
    async def run(self, inp: FollowUpInput) -> ReminderResult:
        await workflow.sleep(timedelta(seconds=1))
        return ReminderResult("completed", [])


async def test_an_unpatched_change_fails_replay_of_the_committed_followup() -> None:
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_FollowUpWithAnExtraTimer]).replay_workflow(_load(FOLLOWUP_HISTORY))


# --- recording (opt in) -----------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_reminder_history(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = str(uuid.uuid4()), str(uuid.uuid4())
    now = await temporal_env.get_current_time()
    old_slot, new_slot = now + timedelta(days=2), now + timedelta(hours=5)
    async with running(temporal_env) as recorder:
        await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=old_slot.isoformat(),
            lead_minutes=[1440, 120],
        )
        await _settle()
        await reschedule_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            new_slot_iso=new_slot.isoformat(),
            lead_minutes=[1440, 120],
        )
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("completed", [120])
    assert len(recorder.calls) == 1
    handle = temporal_env.client.get_workflow_handle(reminder_workflow_id(appointment_id))
    history = await handle.fetch_history()
    HISTORIES.mkdir(exist_ok=True)
    (HISTORIES / REMINDER_HISTORY).write_text(
        json.dumps(_scrub(history.to_json_dict()), indent=2, sort_keys=True) + "\n"
    )


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_followup_history(temporal_env: WorkflowEnvironment) -> None:
    visit_id, appointment_id, patient_id = (str(uuid.uuid4()) for _ in range(3))
    completed_at = await temporal_env.get_current_time()
    async with running(temporal_env) as recorder:
        await start_followup(
            temporal_env.client,
            visit_id=visit_id,
            appointment_id=appointment_id,
            patient_id=patient_id,
            completed_at_iso=completed_at.isoformat(),
            delay_minutes=4320,
        )
        result = await _result(temporal_env, followup_workflow_id(visit_id))
    assert result == ReminderResult("completed", [4320])
    assert len(recorder.calls) == 1
    handle = temporal_env.client.get_workflow_handle(followup_workflow_id(visit_id))
    history = await handle.fetch_history()
    (HISTORIES / FOLLOWUP_HISTORY).write_text(
        json.dumps(_scrub(history.to_json_dict()), indent=2, sort_keys=True) + "\n"
    )
