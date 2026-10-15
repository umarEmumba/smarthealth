"""`VisitDownstreamSweeperWorkflow` (plan 02-11; D-16, research Pattern 8).

Started by the `visit-downstream-sweeper` Temporal Schedule
(`services.visit.schedules`, `ScheduleOverlapPolicy.SKIP`). Each run executes
one idempotent activity, `ACT_SWEEP`, which starts `VisitCompletedWorkflow`
for completed visits whose trigger was lost between the API's commit and its
workflow start (crash, Temporal outage). The worker also runs the same sweep
once at start-up.

Sandbox: only `temporalio` plus the pure contracts and policies.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import FORWARD_ACTIVITY
    from services.visit.domain.contracts import ACT_SWEEP, SWEEPER_WORKFLOW, SweepResult


@workflow.defn(name=SWEEPER_WORKFLOW)
class VisitDownstreamSweeperWorkflow:
    @workflow.run
    async def run(self) -> SweepResult:
        return await workflow.execute_activity(ACT_SWEEP, result_type=SweepResult, **FORWARD_ACTIVITY)
