"""`VisitReconcilerWorkflow` (plan 02.1-10; S4, BILL-02, VISIT-04).

Started by the `visit-reconciler` Temporal Schedule (`services.visit.schedules`,
`ScheduleOverlapPolicy.SKIP`). Each run executes one idempotent activity,
`ACT_RECONCILE`, which settles completed visits whose billing or appointment
settlement marker is still empty after a grace period: a legacy run that ended
`skipped`, a terminated `VisitCompletedWorkflow`, a lost trigger. It calls the
same step bodies the workflow uses, so running concurrently with the workflow
(or another run) never doubles an effect. The worker also runs one pass at
start-up.

A new workflow type, so it needs no `workflow.patched`. Sandbox: only
`temporalio` plus the pure contracts and the shared policies.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import MAINTENANCE_ACTIVITY
    from services.visit.domain.contracts import (
        ACT_RECONCILE,
        VISIT_RECONCILER_WORKFLOW,
        SweepResult,
    )


@workflow.defn(name=VISIT_RECONCILER_WORKFLOW)
class VisitReconcilerWorkflow:
    @workflow.run
    async def run(self) -> SweepResult:
        return await workflow.execute_activity(
            ACT_RECONCILE, result_type=SweepResult, **MAINTENANCE_ACTIVITY
        )
