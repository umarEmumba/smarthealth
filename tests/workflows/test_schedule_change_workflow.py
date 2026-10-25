"""ScheduleChangeWorkflow and ScheduleChangePollerWorkflow logic in isolation
(plan 02.1-08 Task 3; M2).

Stub activities are registered under the real `ACT_*` names, so these tests
exercise only the workflows: two apply passes separated by a durable timer, the
summed counts, retries inside the `MAINTENANCE_ACTIVITY` preset, a non-retryable
`ReferenceInvalid` that ends the run before pass 2, and the poller running its
one activity. They run on a time-skipping Temporal test server, so the hold-TTL
timer and the retry backoff cost no wall-clock time.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import MAINTENANCE_ACTIVITY
from services.scheduling.domain.contracts import (
    ACT_APPLY_SCHEDULE_CHANGE,
    ACT_POLL_SCHEDULE_CHANGES,
    MaintenanceResult,
    ScheduleChangeInput,
    ScheduleChangeResult,
)
from services.scheduling.workflows.schedule_change import (
    ScheduleChangePollerWorkflow,
    ScheduleChangeWorkflow,
)

TASK_QUEUE = "schedule-change-workflow-test"


def _input(**overrides: Any) -> ScheduleChangeInput:
    values: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "provider_id": str(uuid.uuid4()),
        "change_type": "template_deactivated",
        "template_id": str(uuid.uuid4()),
        "second_pass_after_minutes": 11,
    }
    values.update(overrides)
    return ScheduleChangeInput(**values)


class Recorder:
    """What the stub apply activity saw, in order: (pass_number, attempt)."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []
        self.inputs: list[ScheduleChangeInput] = []


def apply_stub(
    recorder: Recorder,
    results: dict[int, ScheduleChangeResult],
    *,
    fail: Any = None,
) -> Any:
    """A stub apply: records `(pass_number, attempt)`, raises `fail(pass, attempt)`
    when it returns an exception, else returns the pass's result."""

    @activity.defn(name=ACT_APPLY_SCHEDULE_CHANGE)
    async def _apply(inp: ScheduleChangeInput) -> ScheduleChangeResult:
        attempt = activity.info().attempt
        recorder.calls.append((inp.pass_number, attempt))
        recorder.inputs.append(inp)
        if fail is not None:
            error = fail(inp.pass_number, attempt)
            if error is not None:
                raise error
        return results[inp.pass_number]

    return _apply


def poll_stub(counter: list[int], processed: int = 7) -> Any:
    @activity.defn(name=ACT_POLL_SCHEDULE_CHANGES)
    async def _poll() -> MaintenanceResult:
        counter.append(1)
        return MaintenanceResult(processed=processed)

    return _poll


async def _run_change(
    env: WorkflowEnvironment, inp: ScheduleChangeInput, stub: Any
) -> tuple[ScheduleChangeResult, Any]:
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[ScheduleChangeWorkflow, ScheduleChangePollerWorkflow],
        activities=[stub],
    ):
        handle = await env.client.start_workflow(
            ScheduleChangeWorkflow.run,
            inp,
            id=f"schedule-change-{inp.event_id}",
            task_queue=TASK_QUEUE,
        )
        result = await handle.result()
        return result, await handle.fetch_history()


def _timers(history: Any) -> list[timedelta]:
    return [
        e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED
    ]


def _command_order(history: Any) -> list[str]:
    """`activity` / `timer` in the order the workflow scheduled them."""
    kinds = {
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED: "activity",
        EventType.EVENT_TYPE_TIMER_STARTED: "timer",
    }
    return [kinds[e.event_type] for e in history.events if e.event_type in kinds]


async def test_two_passes_run_around_a_durable_timer_and_the_counts_are_summed() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        recorder = Recorder()
        inp = _input(second_pass_after_minutes=7)
        stub = apply_stub(
            recorder,
            {1: ScheduleChangeResult(withdrawn=3, flagged=1), 2: ScheduleChangeResult(withdrawn=2, flagged=4)},
        )
        started = await env.get_current_time()

        result, history = await _run_change(env, inp, stub)

        elapsed = await env.get_current_time() - started
        assert result == ScheduleChangeResult(withdrawn=5, flagged=5)
        # The pass numbers the activity saw, once each, on their first attempt.
        assert recorder.calls == [(1, 1), (2, 1)]
        # Everything else in the input is passed through unchanged.
        for seen in recorder.inputs:
            assert (seen.event_id, seen.provider_id, seen.change_type, seen.template_id) == (
                inp.event_id,
                inp.provider_id,
                inp.change_type,
                inp.template_id,
            )
        # apply, ONE durable timer of second_pass_after_minutes, apply: in that order.
        assert _command_order(history) == ["activity", "timer", "activity"]
        assert _timers(history) == [timedelta(minutes=7)]
        # The timer elapsed in workflow time (the test server skipped it).
        assert elapsed >= timedelta(minutes=7)
    finally:
        await env.shutdown()


async def test_the_default_second_pass_delay_is_the_hold_ttl_plus_one_minute() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        recorder = Recorder()
        zero = ScheduleChangeResult(withdrawn=0, flagged=0)
        inp = ScheduleChangeInput(
            event_id=str(uuid.uuid4()), provider_id=str(uuid.uuid4()), change_type="provider_deactivated"
        )
        _result, history = await _run_change(env, inp, apply_stub(recorder, {1: zero, 2: zero}))
        assert _timers(history) == [timedelta(minutes=11)]  # 10-minute hold TTL + 1
        assert [r.template_id for r in recorder.inputs] == [None, None]
    finally:
        await env.shutdown()


async def test_a_retryable_apply_failure_is_retried_within_the_maintenance_preset() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        recorder = Recorder()

        def fail(pass_number: int, attempt: int) -> Exception | None:
            # Pass 1 fails twice with a transient error, then succeeds.
            if pass_number == 1 and attempt < 3:
                return ApplicationError("owner unavailable", type="UpstreamServiceUnavailable")
            return None

        stub = apply_stub(
            recorder,
            {1: ScheduleChangeResult(withdrawn=1, flagged=0), 2: ScheduleChangeResult(withdrawn=0, flagged=2)},
            fail=fail,
        )
        result, _history = await _run_change(env, _input(), stub)

        assert result == ScheduleChangeResult(withdrawn=1, flagged=2)
        assert recorder.calls == [(1, 1), (1, 2), (1, 3), (2, 1)]
        # The preset allows it: more than two attempts, the business error is non-retryable.
        policy = MAINTENANCE_ACTIVITY["retry_policy"]
        assert policy.maximum_attempts >= 3
        assert "ReferenceInvalid" in policy.non_retryable_error_types
    finally:
        await env.shutdown()


async def test_a_retryable_failure_that_never_clears_stops_at_the_preset_and_skips_pass_two() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        recorder = Recorder()
        stub = apply_stub(
            recorder,
            {1: ScheduleChangeResult(withdrawn=0, flagged=0), 2: ScheduleChangeResult(withdrawn=0, flagged=0)},
            fail=lambda _p, _a: ApplicationError("owner unavailable", type="UpstreamServiceUnavailable"),
        )
        with pytest.raises(WorkflowFailureError) as raised:
            await _run_change(env, _input(), stub)

        assert isinstance(raised.value.cause, ActivityError)
        assert [call[0] for call in recorder.calls] == [1] * MAINTENANCE_ACTIVITY["retry_policy"].maximum_attempts
    finally:
        await env.shutdown()


async def test_reference_invalid_ends_the_workflow_and_pass_two_never_runs() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        recorder = Recorder()
        stub = apply_stub(
            recorder,
            {1: ScheduleChangeResult(withdrawn=9, flagged=9), 2: ScheduleChangeResult(withdrawn=9, flagged=9)},
            fail=lambda _p, _a: ApplicationError("reference invalid", type="ReferenceInvalid", non_retryable=True),
        )
        with pytest.raises(WorkflowFailureError) as raised:
            await _run_change(env, _input(), stub)

        activity_error = raised.value.cause
        assert isinstance(activity_error, ActivityError)
        assert isinstance(activity_error.cause, ApplicationError)
        assert activity_error.cause.type == "ReferenceInvalid"
        assert recorder.calls == [(1, 1)]  # one attempt, and no second pass
    finally:
        await env.shutdown()


async def test_the_poller_workflow_runs_the_poll_activity_once_and_returns_its_count() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        counter: list[int] = []
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ScheduleChangeWorkflow, ScheduleChangePollerWorkflow],
            activities=[poll_stub(counter, processed=7)],
        ):
            result = await env.client.execute_workflow(
                ScheduleChangePollerWorkflow.run,
                id=f"poller-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )
        assert result == MaintenanceResult(processed=7)
        assert counter == [1]
    finally:
        await env.shutdown()
