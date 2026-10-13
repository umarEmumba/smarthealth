"""`VisitCompletedWorkflow`: the downstream fan-out after a visit completes
(plan 02-11; VISIT-04, BILL-02, D-05, D-11, D-16).

Why a workflow: completing a visit must never wait on Billing or anything
else downstream. The API commits the `completed` transition, starts this
workflow under the deterministic id `visit-completed-{visit_id}` and returns
200. From then on the work is durable: Temporal retries each step after the
HTTP request has long ended, survives worker crashes, and a duplicate start
(repeated complete, a sweep) attaches to or is refused by the same id.

Three INDEPENDENT branches, run concurrently with `asyncio.gather` (which is
deterministic inside the workflow sandbox):

* billing finalize (`ACT_FINALIZE_BILLING`, `COMPENSATION_ACTIVITY`): the
  visit is already committed, so billing must catch up; it gets the long
  retry budget;
* follow-up intent and analytics intent (`NOTIFY_ACTIVITY`): best effort.

Each branch catches its own `ActivityError` and reports `skipped:<ErrorType>`
in the result instead of failing the workflow, so one branch failing never
blocks the others. No run timeout: activities are bounded by their policies.

Phase 3 replaces the trigger (commit-then-start + sweeper) with a
transactional outbox, and the two intent stubs with Kafka events / Celery jobs.

Sandbox: only `temporalio`, stdlib, the pure contracts and the policies.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import COMPENSATION_ACTIVITY, NOTIFY_ACTIVITY
    from services.visit.domain.contracts import (
        ACT_ANALYTICS_INTENT,
        ACT_FINALIZE_BILLING,
        ACT_FOLLOWUP_INTENT,
        OUTCOME_COMPLETED,
        SKIPPED_PREFIX,
        VISIT_COMPLETED_WORKFLOW,
        VisitCompletedInput,
        VisitCompletedResult,
    )


def failure_reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    if isinstance(cause, TimeoutError):
        return "activity_timeout"
    return "activity_failed"


async def _branch(name: str, inp: VisitCompletedInput, options: dict) -> str:
    try:
        return await workflow.execute_activity(name, inp, result_type=str, **options)
    except ActivityError as err:
        reason = failure_reason(err)
        workflow.logger.warning("visit.downstream_branch_skipped step=%s reason=%s", name, reason)
        return f"{SKIPPED_PREFIX}{reason}"


@workflow.defn(name=VISIT_COMPLETED_WORKFLOW)
class VisitCompletedWorkflow:
    @workflow.run
    async def run(self, inp: VisitCompletedInput) -> VisitCompletedResult:
        billing, followup, analytics = await asyncio.gather(
            _branch(ACT_FINALIZE_BILLING, inp, COMPENSATION_ACTIVITY),
            _branch(ACT_FOLLOWUP_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_ANALYTICS_INTENT, inp, NOTIFY_ACTIVITY),
        )
        return VisitCompletedResult(
            outcome=OUTCOME_COMPLETED, billing=billing, followup=followup, analytics=analytics
        )
