"""`VisitCompletedWorkflow`: the downstream fan-out after a visit completes
(plan 02-11; VISIT-04, BILL-02, D-05, D-11, D-16).

Why a workflow: completing a visit must never wait on Billing or anything
else downstream. The API commits the `completed` transition, starts this
workflow under the deterministic id `visit-completed-{visit_id}` and returns
200. From then on the work is durable: Temporal retries each step after the
HTTP request has long ended, survives worker crashes, and a duplicate start
(repeated complete, a sweep) attaches to or is refused by the same id.

Four INDEPENDENT branches, run concurrently with `asyncio.gather` (which is
deterministic inside the workflow sandbox):

* billing finalize (`ACT_FINALIZE_BILLING`): the visit is already committed,
  so billing must catch up. Plan 02.1-10 (S4) gives it
  `BILLING_FINALIZE_ACTIVITY`: unlimited attempts, backoff capped at 5 minutes
  and no schedule-to-close, so a Billing outage of any length is waited out
  instead of ending `skipped:activity_failed` with the record stuck at
  `verified`. Retries add no history events, so the open-ended retry is cheap;
  the one early end is the terminal business answer `BillingNotFinalizable`.
  Consequence: while Billing is down the workflow stays open (the other three
  branches finish independently and their results wait for billing). It exists
  only behind `workflow.patched(BILLING_RETRY_PATCH_ID)`: a history recorded
  before the change replays with the previous bounded `COMPENSATION_ACTIVITY`
  options, every new run records the marker and retries open-endedly. The
  visit reconciler Schedule (`workflows/reconciler.py`) covers whatever slips
  through anyway (a terminated workflow, a legacy `skipped` run);
* follow-up intent and analytics intent (`NOTIFY_ACTIVITY`): best effort;
* complete the appointment in Scheduling (`ACT_COMPLETE_APPOINTMENT`,
  `COMPENSATION_ACTIVITY`; plan 02.1-09, M1): same reasoning as billing, the
  visit is committed and Scheduling must catch up. It is a concurrent branch,
  not a step after the gather, so a slow or retrying billing branch can never
  starve it (and vice versa). It exists only behind
  `workflow.patched(VISIT_COMPLETE_APPOINTMENT_PATCH_ID)`: a history recorded
  before the change has no marker and replays the original three branches,
  while every new run records the marker and runs all four. A refusal because
  the appointment was never checked in is the terminal business outcome
  `AppointmentNotCheckedIn`, reported as `skipped:AppointmentNotCheckedIn`.

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
        ACT_COMPLETE_APPOINTMENT,
        ACT_FINALIZE_BILLING,
        ACT_FOLLOWUP_INTENT,
        BILLING_RETRY_PATCH_ID,
        OUTCOME_COMPLETED,
        SKIPPED_PREFIX,
        VISIT_COMPLETE_APPOINTMENT_PATCH_ID,
        VISIT_COMPLETED_WORKFLOW,
        VisitCompletedInput,
        VisitCompletedResult,
    )
    from services.visit.domain.policies import BILLING_FINALIZE_ACTIVITY


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
        # Plan 02.1-10: open-ended billing retry only for runs that carry the
        # marker; evaluated first and once, so the marker order is stable.
        use_open_ended_billing = workflow.patched(BILLING_RETRY_PATCH_ID)
        billing_options = (
            BILLING_FINALIZE_ACTIVITY if use_open_ended_billing else COMPENSATION_ACTIVITY
        )
        branches = [
            _branch(ACT_FINALIZE_BILLING, inp, billing_options),
            _branch(ACT_FOLLOWUP_INTENT, inp, NOTIFY_ACTIVITY),
            _branch(ACT_ANALYTICS_INTENT, inp, NOTIFY_ACTIVITY),
        ]
        # Plan 02.1-09: the fourth branch only for runs that carry the marker.
        patched = workflow.patched(VISIT_COMPLETE_APPOINTMENT_PATCH_ID)
        if patched:
            branches.append(_branch(ACT_COMPLETE_APPOINTMENT, inp, COMPENSATION_ACTIVITY))
        results = await asyncio.gather(*branches)
        billing, followup, analytics = results[0], results[1], results[2]
        appointment = results[3] if patched else ""
        return VisitCompletedResult(
            outcome=OUTCOME_COMPLETED,
            billing=billing,
            followup=followup,
            analytics=analytics,
            appointment=appointment,
        )
