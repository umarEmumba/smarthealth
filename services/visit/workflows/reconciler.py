"""`VisitReconcilerWorkflow` (plan 02.1-10; S4, BILL-02, VISIT-04).

Started by the `visit-reconciler` Temporal Schedule (`services.visit.schedules`,
`ScheduleOverlapPolicy.SKIP`). Each run executes one idempotent activity,
`ACT_RECONCILE`, which settles completed visits whose billing or appointment
settlement marker is still empty after a grace period: a legacy run that ended
`skipped`, a terminated `VisitCompletedWorkflow`. It calls the same step bodies the
workflow uses, so running concurrently with the workflow (or another run) never
doubles an effect. The worker also runs one pass at start-up.

Never-started visits (a completion whose downstream workflow was never begun) are first
the `visit.completed` outbox event's job, consumed by the visit-downstream consumer
(plan 03-09; the sweeper that used to do it was retired in plan 03-12 and stays
retired). The activity ALSO runs a lost-trigger pass (plan 03-22, CR-01): after the same
grace it restarts never-started visits whose outbox event was dead-lettered or dropped,
through the same idempotent `ensure_downstream_started` the consumer uses (deterministic
workflow id, then the conditional marker), so a lost event can no longer leave a completed
visit unbilled for ever. Only the activity body changed: this workflow, its name and the
activity name are as before, so no `workflow.patched` is needed.

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
