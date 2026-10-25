"""`VisitCompletedWorkflow` branch independence with stub activities (plan
02-11 Task 3; VISIT-04, BILL-02, D-16).

Stubs are registered under the real `ACT_*` names on `visit-tq` in the
time-skipping env, so retries and backoff cost no wall-clock time.
"""

from __future__ import annotations

import uuid

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import COMPENSATION_ACTIVITY, NOTIFY_ACTIVITY, VISIT_TASK_QUEUE
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
    ACT_COMPLETE_APPOINTMENT,
    ACT_FINALIZE_BILLING,
    ACT_FOLLOWUP_INTENT,
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    VisitCompletedResult,
)
from services.visit.workflows.visit_completed import VisitCompletedWorkflow


def _input() -> VisitCompletedInput:
    return VisitCompletedInput(
        visit_id=str(uuid.uuid4()), appointment_id=str(uuid.uuid4()), clinic_id=str(uuid.uuid4())
    )


async def _run(
    env: WorkflowEnvironment, billing, followup, analytics, appointment
) -> VisitCompletedResult:
    async with Worker(
        env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitCompletedWorkflow],
        activities=[billing, followup, analytics, appointment],
    ):
        return await env.client.execute_workflow(
            VISIT_COMPLETED_WORKFLOW,
            _input(),
            id=f"visit-completed-{uuid.uuid4()}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=VisitCompletedResult,
        )


def _stubs(
    calls: list[str],
    *,
    billing_error: Exception | None = None,
    billing_failures: int = 0,
    followup_error: Exception | None = None,
    appointment_error: Exception | None = None,
    appointment_failures: int = 0,
):
    @activity.defn(name=ACT_FINALIZE_BILLING)
    async def billing(inp: VisitCompletedInput) -> str:
        calls.append("billing")
        if billing_error is not None:
            raise billing_error
        if calls.count("billing") <= billing_failures:
            raise ApplicationError("billing unavailable", type="BillingUnavailable")
        return "finalized"

    @activity.defn(name=ACT_FOLLOWUP_INTENT)
    async def followup(inp: VisitCompletedInput) -> str:
        calls.append("followup")
        if followup_error is not None:
            raise followup_error
        return "recorded"

    @activity.defn(name=ACT_ANALYTICS_INTENT)
    async def analytics(inp: VisitCompletedInput) -> str:
        calls.append("analytics")
        return "recorded"

    @activity.defn(name=ACT_COMPLETE_APPOINTMENT)
    async def appointment(inp: VisitCompletedInput) -> str:
        calls.append("appointment")
        if appointment_error is not None:
            raise appointment_error
        if calls.count("appointment") <= appointment_failures:
            raise ApplicationError("scheduling unavailable", type="SchedulingUnavailable")
        return "completed"

    return billing, followup, analytics, appointment


async def test_all_four_branches_run_once(temporal_env: WorkflowEnvironment) -> None:
    calls: list[str] = []
    result = await _run(temporal_env, *_stubs(calls))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert sorted(calls) == ["analytics", "appointment", "billing", "followup"]


async def test_billing_business_failure_never_blocks_the_intents(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    error = ApplicationError("billing not finalizable", type="BillingNotFinalizable", non_retryable=True)
    result = await _run(temporal_env, *_stubs(calls, billing_error=error))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="skipped:BillingNotFinalizable",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert calls.count("billing") == 1  # non-retryable: no retry
    assert calls.count("appointment") == 1


async def test_followup_failing_every_attempt_is_skipped_and_others_succeed(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    error = ApplicationError("follow-up unavailable", type="FollowupUnavailable")
    result = await _run(temporal_env, *_stubs(calls, followup_error=error))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="skipped:FollowupUnavailable",
        analytics="recorded",
        appointment="completed",
    )
    # Retries back off 1 s then 2 s, well inside the 30 s schedule-to-close, so
    # the attempt cap (not the timeout) ends the branch; time skipping makes it instant.
    assert calls.count("followup") == NOTIFY_ACTIVITY["retry_policy"].maximum_attempts
    assert calls.count("billing") == 1
    assert calls.count("analytics") == 1


async def test_billing_retryable_failures_are_absorbed(temporal_env: WorkflowEnvironment) -> None:
    calls: list[str] = []
    # COMPENSATION_ACTIVITY retries (up to 20 attempts) absorb transient Billing trouble.
    result = await _run(temporal_env, *_stubs(calls, billing_failures=2))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert calls.count("billing") == 3
    assert calls.count("followup") == 1
    assert calls.count("analytics") == 1


async def test_appointment_not_checked_in_is_skipped_and_never_blocks_the_others(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    error = ApplicationError(
        "appointment not checked in", type="AppointmentNotCheckedIn", non_retryable=True
    )
    result = await _run(temporal_env, *_stubs(calls, appointment_error=error))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="skipped:AppointmentNotCheckedIn",
    )
    assert calls.count("appointment") == 1  # a business refusal is never retried
    assert calls.count("billing") == 1
    assert calls.count("followup") == 1
    assert calls.count("analytics") == 1


async def test_appointment_retryable_failures_are_absorbed(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    # COMPENSATION_ACTIVITY retries absorb a Scheduling outage; the visit is
    # already committed, so the branch must catch up rather than give up.
    result = await _run(temporal_env, *_stubs(calls, appointment_failures=3))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert calls.count("appointment") == 4
    assert COMPENSATION_ACTIVITY["retry_policy"].maximum_attempts >= 4


async def test_billing_failing_every_attempt_never_starves_the_appointment_branch(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    error = ApplicationError("billing unavailable", type="BillingUnavailable")
    result = await _run(temporal_env, *_stubs(calls, billing_error=error))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="skipped:BillingUnavailable",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert calls.count("billing") == COMPENSATION_ACTIVITY["retry_policy"].maximum_attempts
    assert calls.count("appointment") == 1


async def test_appointment_branch_is_scheduled_with_the_compensation_budget(
    temporal_env: WorkflowEnvironment,
) -> None:
    """The fourth branch runs under the long compensation retry budget (the
    visit is committed; Scheduling must catch up), concurrently with the rest:
    its activity is scheduled in the same workflow task as the other three."""
    calls: list[str] = []
    billing, followup, analytics, appointment = _stubs(calls)
    workflow_id = f"visit-completed-{uuid.uuid4()}"
    async with Worker(
        temporal_env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitCompletedWorkflow],
        activities=[billing, followup, analytics, appointment],
    ):
        handle = await temporal_env.client.start_workflow(
            VISIT_COMPLETED_WORKFLOW,
            _input(),
            id=workflow_id,
            task_queue=VISIT_TASK_QUEUE,
            result_type=VisitCompletedResult,
        )
        await handle.result()
    history = await handle.fetch_history()
    scheduled = [
        e.activity_task_scheduled_event_attributes
        for e in history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    ]
    by_type = {a.activity_type.name: a for a in scheduled}
    assert set(by_type) == {
        ACT_FINALIZE_BILLING,
        ACT_FOLLOWUP_INTENT,
        ACT_ANALYTICS_INTENT,
        ACT_COMPLETE_APPOINTMENT,
    }
    assert (
        by_type[ACT_COMPLETE_APPOINTMENT].retry_policy.maximum_attempts
        == COMPENSATION_ACTIVITY["retry_policy"].maximum_attempts
    )
    # One workflow task scheduled all four: the branches are concurrent, not sequenced.
    first_task_event = {
        e.activity_task_scheduled_event_attributes.workflow_task_completed_event_id
        for e in history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    }
    assert len(first_task_event) == 1
