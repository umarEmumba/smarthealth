"""Visit billing finalization retries until it succeeds or Billing refuses
terminally (plan 02.1-10 Task 1; S4, BILL-02, VISIT-04).

Stub activities on the time-skipping server, same harness as
`test_visit_completed.py`. The billing branch runs under
`BILLING_FINALIZE_ACTIVITY` (unlimited attempts, backoff capped at 5 minutes,
`BillingNotFinalizable` non-retryable) behind `workflow.patched`; the old
bounded `COMPENSATION_ACTIVITY` budget was 20 attempts, so 40 transient
failures is twice what used to end the branch as `skipped:activity_failed`.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import COMPENSATION_ACTIVITY, VISIT_TASK_QUEUE
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
    ACT_COMPLETE_APPOINTMENT,
    ACT_FINALIZE_BILLING,
    ACT_FOLLOWUP_INTENT,
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    VisitCompletedResult,
)
from services.visit.domain.policies import BILLING_FINALIZE_ACTIVITY
from services.visit.workflows.visit_completed import VisitCompletedWorkflow
from tests.workflows.test_visit_completed import _input, _run, _stubs

OLD_BUDGET = COMPENSATION_ACTIVITY["retry_policy"].maximum_attempts


def test_the_preset_is_open_ended_capped_and_sandbox_safe() -> None:
    policy = BILLING_FINALIZE_ACTIVITY["retry_policy"]
    assert policy.maximum_attempts == 0  # unlimited
    assert policy.maximum_interval == timedelta(minutes=5)
    assert policy.initial_interval == timedelta(seconds=2)
    assert policy.backoff_coefficient == 2.0
    assert list(policy.non_retryable_error_types) == ["BillingNotFinalizable"]
    assert BILLING_FINALIZE_ACTIVITY["start_to_close_timeout"] == timedelta(seconds=30)
    assert "schedule_to_close_timeout" not in BILLING_FINALIZE_ACTIVITY


async def test_billing_failing_far_beyond_the_old_budget_still_finalizes(
    temporal_env: WorkflowEnvironment,
) -> None:
    """Tracer: 40 retryable failures (twice the old 20-attempt budget), then
    `finalized`. The other three branches complete."""
    calls: list[str] = []
    failures = 2 * OLD_BUDGET
    result = await _run(temporal_env, *_stubs(calls, billing_failures=failures))
    assert result == VisitCompletedResult(
        outcome="completed",
        billing="finalized",
        followup="recorded",
        analytics="recorded",
        appointment="completed",
    )
    assert calls.count("billing") == failures + 1
    for other in ("followup", "analytics", "appointment"):
        assert calls.count(other) == 1


async def test_a_business_refusal_is_terminal_after_exactly_one_attempt(
    temporal_env: WorkflowEnvironment,
) -> None:
    calls: list[str] = []
    error = ApplicationError("billing not finalizable", type="BillingNotFinalizable", non_retryable=True)
    result = await _run(temporal_env, *_stubs(calls, billing_error=error))
    assert result.billing == "skipped:BillingNotFinalizable"
    assert result.appointment == "completed"
    assert calls.count("billing") == 1


async def test_the_business_refusal_is_terminal_even_if_the_raiser_forgets_non_retryable(
    temporal_env: WorkflowEnvironment,
) -> None:
    """The preset itself lists the type, so the step still ends after one attempt
    (an open-ended retry loop on a refusal would otherwise never end)."""
    calls: list[str] = []
    error = ApplicationError("billing not finalizable", type="BillingNotFinalizable")
    result = await _run(temporal_env, *_stubs(calls, billing_error=error))
    assert result.billing == "skipped:BillingNotFinalizable"
    assert calls.count("billing") == 1


async def test_the_other_branches_finish_while_billing_is_still_retrying(
    temporal_env: WorkflowEnvironment,
) -> None:
    """Record the order: at billing's final (successful) attempt the follow-up,
    analytics and appointment branches are long done, so a retrying billing
    branch never delays them."""
    calls: list[str] = []
    seen_at_success: list[set[str]] = []
    failures = OLD_BUDGET + 5

    @activity.defn(name=ACT_FINALIZE_BILLING)
    async def billing(inp: VisitCompletedInput) -> str:
        calls.append("billing")
        if calls.count("billing") <= failures:
            raise ApplicationError("billing unavailable", type="BillingUnavailable")
        seen_at_success.append(set(calls))
        return "finalized"

    _, followup, analytics, appointment = _stubs(calls)
    result = await _run(temporal_env, billing, followup, analytics, appointment)
    assert result.billing == "finalized"
    assert {"followup", "analytics", "appointment"} <= seen_at_success[0]
    assert calls.count("billing") == failures + 1


async def test_the_billing_activity_is_scheduled_with_the_open_ended_policy(
    temporal_env: WorkflowEnvironment,
) -> None:
    """History proof: the scheduled billing activity carries unlimited attempts,
    the 5-minute cap and no schedule-to-close; retries do not add events (one
    scheduled event, one completed event, however many attempts ran)."""
    calls: list[str] = []
    billing, followup, analytics, appointment = _stubs(calls, billing_failures=30)
    async with Worker(
        temporal_env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitCompletedWorkflow],
        activities=[billing, followup, analytics, appointment],
    ):
        handle = await temporal_env.client.start_workflow(
            VISIT_COMPLETED_WORKFLOW,
            _input(),
            id=f"visit-completed-{uuid.uuid4()}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=VisitCompletedResult,
        )
        await handle.result()
    history = await handle.fetch_history()
    scheduled = {
        e.activity_task_scheduled_event_attributes.activity_type.name: e.activity_task_scheduled_event_attributes
        for e in history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    }
    attrs = scheduled[ACT_FINALIZE_BILLING]
    assert attrs.retry_policy.maximum_attempts == 0
    assert attrs.retry_policy.maximum_interval.ToTimedelta() == timedelta(minutes=5)
    assert list(attrs.retry_policy.non_retryable_error_types) == ["BillingNotFinalizable"]
    assert attrs.start_to_close_timeout.ToTimedelta() == timedelta(seconds=30)
    # Not set by the workflow, so the server fills in its ceiling (the workflow has
    # no run timeout): effectively unbounded, never the old 10 minutes.
    assert attrs.schedule_to_close_timeout.ToTimedelta() > timedelta(days=365)
    assert calls.count("billing") == 31
    # 31 attempts, yet exactly one scheduled and one completed event for billing.
    assert sum(1 for e in history.events if e.HasField("activity_task_scheduled_event_attributes")
               and e.activity_task_scheduled_event_attributes.activity_type.name == ACT_FINALIZE_BILLING) == 1
    assert ACT_COMPLETE_APPOINTMENT in scheduled
    assert ACT_FOLLOWUP_INTENT in scheduled and ACT_ANALYTICS_INTENT in scheduled
