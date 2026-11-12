"""`VisitDownstreamSweeperWorkflow`: a RETIRED tombstone (plan 03-12, D-03; created by
plan 02-11, D-16, research Pattern 8).

It used to be started by the `visit-downstream-sweeper` Temporal Schedule and ran one
activity, `ACT_SWEEP`, that started `VisitCompletedWorkflow` for completed visits whose
trigger was lost between the API's commit and its workflow start. Since plan 03-09 the
`visit.completed` outbox event consumed by the visit-downstream consumer is the only
trigger, and the worker deletes the Schedule at start (`retire_sweeper_schedule`).

The class and the activity name stay registered on purpose: Temporal replays a recorded
history against the registered workflow type, and a run that was in flight at the
cutover must still be able to finish. The activity (`VisitActivities.sweep_unstarted`)
is a no-op that logs `visit.sweeper_retired` and returns zero processed. Do not rename
or reshape either.

Safe to remove (this class, the `SWEEPER_WORKFLOW` and `ACT_SWEEP` constants, the
tombstone activity and its registration) once this query returns nothing on every
environment (all closed runs have aged out of retention):

    list_workflows("WorkflowType='VisitDownstreamSweeperWorkflow'")

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
