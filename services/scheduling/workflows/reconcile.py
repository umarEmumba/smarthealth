"""The Scheduling reconciler workflow (plan 02.1-11; S4, S5, M1).

`ReconcilerWorkflow` is started by the `{prefix}-reconciler` Temporal Schedule
(`services.scheduling.schedules`, every 60 s, overlap SKIP) and runs three
idempotent activities in a fixed order:

    1. reconcile.stuck_pending   pending appointments whose booking workflow is
                                 gone, plus the billing orphan lookback
    2. reconcile.stuck_waitlist  waitlist entries stuck `matched`
    3. reconcile.no_shows        confirmed appointments past slot end + grace

One activity's failure is logged and never blocks the others (each is a
separate concern and each converges on the next run). The result sums what the
activities did. A new workflow type: no `workflow.patched` was needed; any
later change to this command sequence is a replay risk and needs one (and a
newly recorded history next to `histories/reconciler_p21.json`).

Sandbox: only `temporalio` plus the pure contracts and policies.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import MAINTENANCE_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_RECONCILE_NO_SHOWS,
        ACT_RECONCILE_PENDING,
        ACT_RECONCILE_WAITLIST,
        RECONCILER_WORKFLOW,
        MaintenanceResult,
        ReconcilePendingResult,
        ReconcileResult,
    )


def _reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    return "activity_failed"


@workflow.defn(name=RECONCILER_WORKFLOW)
class ReconcilerWorkflow:
    @workflow.run
    async def run(self) -> ReconcileResult:
        pending = ReconcilePendingResult(failed=0, billing_voids=0)
        waitlist = MaintenanceResult(processed=0)
        no_shows = MaintenanceResult(processed=0)

        try:
            pending = await workflow.execute_activity(
                ACT_RECONCILE_PENDING,
                result_type=ReconcilePendingResult,
                **MAINTENANCE_ACTIVITY,
            )
        except ActivityError as err:
            workflow.logger.error("reconcile.stuck_pending_failed reason=%s", _reason(err))

        try:
            waitlist = await workflow.execute_activity(
                ACT_RECONCILE_WAITLIST, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
            )
        except ActivityError as err:
            workflow.logger.error("reconcile.stuck_waitlist_failed reason=%s", _reason(err))

        try:
            no_shows = await workflow.execute_activity(
                ACT_RECONCILE_NO_SHOWS, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
            )
        except ActivityError as err:
            workflow.logger.error("reconcile.no_shows_failed reason=%s", _reason(err))

        return ReconcileResult(
            pending=pending.failed,
            billing_voids=pending.billing_voids,
            waitlist=waitlist.processed,
            no_shows=no_shows.processed,
        )
