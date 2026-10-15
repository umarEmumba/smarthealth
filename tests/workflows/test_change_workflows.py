"""CancelWorkflow and RescheduleWorkflow logic in isolation (plan 02-08; D-13, D-14).

Stub activities are registered under the real `change.*` names, so these tests
exercise only the workflows: step order, forward-only cancel (billing failure
never undoes the cancellation), reserve-then-swap with hold release on failure,
and best-effort notification. Time-skipping server: retry backoff is free.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from services.scheduling.domain.contracts import (
    ACT_BILLING_CANCEL,
    ACT_BILLING_RELINK,
    ACT_CANCEL_APPOINTMENT,
    ACT_HOLD_NEW,
    ACT_NOTIFY_CHANGE,
    ACT_RECORD_OUTCOME,
    ACT_RELEASE_NEW,
    ACT_SWAP,
    CancelInput,
    ChangeResult,
    RescheduleInput,
    cancel_workflow_id,
    reschedule_workflow_id,
)
from services.scheduling.workflows.cancel import CancelWorkflow
from services.scheduling.workflows.reschedule import RescheduleWorkflow

ALL_ACTIVITIES = (
    ACT_CANCEL_APPOINTMENT,
    ACT_BILLING_CANCEL,
    ACT_HOLD_NEW,
    ACT_SWAP,
    ACT_RELEASE_NEW,
    ACT_BILLING_RELINK,
    ACT_RECORD_OUTCOME,
    ACT_NOTIFY_CHANGE,
)
TASK_QUEUE = "change-workflow-test"


def business(kind: str) -> Callable[[], Exception]:
    return lambda: ApplicationError("business outcome", type=kind, non_retryable=True)


def retryable(kind: str) -> Callable[[], Exception]:
    return lambda: ApplicationError("transient", type=kind)


def make_stubs(calls: list[str], failures: dict[str, Callable[[], Exception]]) -> list[Any]:
    """One stub per activity: records its short name (record_outcome with its
    note, notify with its type) and raises the configured failure, if any."""

    def stub(name: str) -> Any:
        @activity.defn(name=name)
        async def _run(arg: Any) -> None:
            short = name.removeprefix("change.")
            if name == ACT_RECORD_OUTCOME:
                calls.append(f"record_outcome:{arg['note']}")
            elif name == ACT_NOTIFY_CHANGE:
                calls.append(f"notify:{arg['notification_type']}")
            else:
                calls.append(short)
            if name in failures:
                raise failures[name]()

        return _run

    return [stub(name) for name in ALL_ACTIVITIES]


async def run_workflow(
    env: WorkflowEnvironment,
    workflow: Any,
    inp: Any,
    workflow_id: str,
    failures: dict[str, Callable[[], Exception]] | None = None,
) -> tuple[ChangeResult, list[str]]:
    calls: list[str] = []
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[CancelWorkflow, RescheduleWorkflow],
        activities=make_stubs(calls, failures or {}),
    ):
        # `execute_workflow` on the test client skips time while it waits.
        result = await env.client.execute_workflow(
            workflow.run, inp, id=workflow_id, task_queue=TASK_QUEUE
        )
    return result, calls


def cancel_input(policy: str = "refund") -> CancelInput:
    return CancelInput(
        appointment_id=str(uuid.uuid4()),
        clinic_id=str(uuid.uuid4()),
        slot_id=str(uuid.uuid4()),
        refund_policy=policy,
        actor="user:test",
    )


def reschedule_input() -> RescheduleInput:
    return RescheduleInput(
        appointment_id=str(uuid.uuid4()),
        clinic_id=str(uuid.uuid4()),
        old_slot_id=str(uuid.uuid4()),
        new_slot_id=str(uuid.uuid4()),
        actor="user:test",
    )


async def run_cancel(env, failures=None):
    inp = cancel_input()
    return await run_workflow(env, CancelWorkflow, inp, cancel_workflow_id(inp.appointment_id, 1), failures)


async def run_reschedule(env, failures=None):
    inp = reschedule_input()
    return await run_workflow(
        env, RescheduleWorkflow, inp, reschedule_workflow_id(inp.appointment_id, 1), failures
    )


# --- cancel ------------------------------------------------------------------------


async def test_cancel_runs_cancel_then_billing_then_notify(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_cancel(temporal_env)

    assert calls == ["cancel_appointment", "billing_cancel", "notify:booking_cancelled"]
    assert result == ChangeResult(outcome="cancelled", reason=None)


async def test_cancel_billing_failure_is_billing_pending_and_never_undone(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_cancel(temporal_env, {ACT_BILLING_CANCEL: retryable("UpstreamServiceUnavailable")})

    assert result == ChangeResult(outcome="cancelled", reason="billing_pending")
    # The long compensation budget was spent on billing, then the gap recorded.
    assert calls.count("billing_cancel") > 5
    assert calls.count("cancel_appointment") == 1
    assert calls[-2:] == ["record_outcome:cancel_incomplete:billing_pending", "notify:booking_cancelled"]
    assert not {"release_new_hold", "hold_new_slot", "swap_slot"} & set(calls)


async def test_not_cancellable_is_recorded_and_fails(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_cancel(temporal_env, {ACT_CANCEL_APPOINTMENT: business("NotCancellable")})

    assert calls == ["cancel_appointment", "record_outcome:cancel_failed:NotCancellable"]
    assert result == ChangeResult(outcome="failed", reason="NotCancellable")


async def test_cancel_notification_failure_still_cancels(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_cancel(temporal_env, {ACT_NOTIFY_CHANGE: retryable("NotifierDown")})

    assert result == ChangeResult(outcome="cancelled", reason=None)
    assert calls.count("notify:booking_cancelled") == 3


# --- reschedule ----------------------------------------------------------------------


async def test_reschedule_holds_then_swaps_then_relinks_then_notifies(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_reschedule(temporal_env)

    assert calls == ["hold_new_slot", "swap_slot", "billing_relink", "notify:booking_rescheduled"]
    assert result == ChangeResult(outcome="rescheduled", reason=None)


async def test_swap_rejected_releases_the_new_hold_and_records(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_reschedule(temporal_env, {ACT_SWAP: business("SwapRejected")})

    assert calls == [
        "hold_new_slot",
        "swap_slot",
        "release_new_hold",
        "record_outcome:reschedule_failed:SwapRejected",
    ]
    assert result == ChangeResult(outcome="failed", reason="SwapRejected")


async def test_hold_unavailable_never_swaps(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_reschedule(temporal_env, {ACT_HOLD_NEW: business("SlotUnavailable")})

    assert calls == [
        "hold_new_slot",
        "release_new_hold",
        "record_outcome:reschedule_failed:SlotUnavailable",
    ]
    assert result == ChangeResult(outcome="failed", reason="SlotUnavailable")


async def test_relink_failure_keeps_the_swap_and_reports_billing_pending(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_reschedule(temporal_env, {ACT_BILLING_RELINK: business("BillingConflict")})

    assert result == ChangeResult(outcome="rescheduled", reason="billing_pending")
    assert calls == [
        "hold_new_slot",
        "swap_slot",
        "billing_relink",
        "record_outcome:reschedule_incomplete:billing_pending",
        "notify:booking_rescheduled",
    ]
    assert "release_new_hold" not in calls
