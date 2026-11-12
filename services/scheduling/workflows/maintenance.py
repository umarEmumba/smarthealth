"""Maintenance workflows started by Temporal Schedules (plan 02-07; D-03,
research G7 / Pattern 8).

Each run executes exactly one idempotent activity and returns its
`MaintenanceResult`; the Schedule (`services.scheduling.schedules`) starts a
new run every interval with `ScheduleOverlapPolicy.SKIP`, so runs never pile
up. A Schedule rather than cron or an in-process loop: it survives worker
restarts, is visible in the Temporal UI, and any worker on `scheduling-tq` can
run it.

* `HoldReaperWorkflow` - releases holds that outlived their saga and fails the
  owning pending appointment (`hold_expired`).
* `PendingBookingStarterWorkflow` - starts booking workflows that were never
  started after the API's commit (lost trigger).

Sandbox: only `temporalio` plus the pure contracts and policies.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import FORWARD_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_EXPIRE_HOLDS,
        ACT_START_PENDING,
        HOLD_REAPER_WORKFLOW,
        PENDING_STARTER_WORKFLOW,
        MaintenanceResult,
    )


@workflow.defn(name=HOLD_REAPER_WORKFLOW)
class HoldReaperWorkflow:
    @workflow.run
    async def run(self) -> MaintenanceResult:
        return await workflow.execute_activity(
            ACT_EXPIRE_HOLDS, result_type=MaintenanceResult, **FORWARD_ACTIVITY
        )


@workflow.defn(name=PENDING_STARTER_WORKFLOW)
class PendingBookingStarterWorkflow:
    @workflow.run
    async def run(self) -> MaintenanceResult:
        return await workflow.execute_activity(
            ACT_START_PENDING, result_type=MaintenanceResult, **FORWARD_ACTIVITY
        )
