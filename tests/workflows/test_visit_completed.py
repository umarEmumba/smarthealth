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

from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
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


async def _run(env: WorkflowEnvironment, billing, followup, analytics) -> VisitCompletedResult:
    async with Worker(
        env.client,
        task_queue=VISIT_TASK_QUEUE,
        workflows=[VisitCompletedWorkflow],
        activities=[billing, followup, analytics],
    ):
        return await env.client.execute_workflow(
            VISIT_COMPLETED_WORKFLOW,
            _input(),
            id=f"visit-completed-{uuid.uuid4()}",
            task_queue=VISIT_TASK_QUEUE,
            result_type=VisitCompletedResult,
        )


def _stubs(calls: list[str], *, billing_error: Exception | None = None):
    @activity.defn(name=ACT_FINALIZE_BILLING)
    async def billing(inp: VisitCompletedInput) -> str:
        calls.append("billing")
        if billing_error is not None:
            raise billing_error
        return "finalized"

    @activity.defn(name=ACT_FOLLOWUP_INTENT)
    async def followup(inp: VisitCompletedInput) -> str:
        calls.append("followup")
        return "recorded"

    @activity.defn(name=ACT_ANALYTICS_INTENT)
    async def analytics(inp: VisitCompletedInput) -> str:
        calls.append("analytics")
        return "recorded"

    return billing, followup, analytics


async def test_all_three_branches_run_once(temporal_env: WorkflowEnvironment) -> None:
    calls: list[str] = []
    result = await _run(temporal_env, *_stubs(calls))
    assert result == VisitCompletedResult(
        outcome="completed", billing="finalized", followup="recorded", analytics="recorded"
    )
    assert sorted(calls) == ["analytics", "billing", "followup"]


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
    )
    assert calls.count("billing") == 1  # non-retryable: no retry
