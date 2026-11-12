"""Schedule-change workflows (plan 02.1-08; mentor item M2).

* `ScheduleChangeWorkflow` - one run per Provider schedule-change event, started
  by the poller under the deterministic id `schedule-change-{event_id}`.
  Pass 1 applies the change immediately (withdraw the stale available slots, flag
  the bookings that sit on stale booked slots). It then SLEEPS
  `second_pass_after_minutes` (a durable timer: the hold TTL plus one minute, set
  by the poller from settings) and applies the same change as pass 2. A slot held
  by an in-flight booking is not touched in pass 1; by the time pass 2 runs that
  booking has confirmed (and is flagged now), failed (and the slot is withdrawn
  now) or been reaped. The counts of both passes are summed.

  A non-retryable `ReferenceInvalid` (a missing department or clinic) fails the
  workflow at once and pass 2 never runs: the data the apply needs is not there,
  and the operator sees the failure. A retryable outage of an owning service is
  retried inside the `MAINTENANCE_ACTIVITY` budget.

* `ScheduleChangePollerWorkflow` - started by the Temporal Schedule
  (`services.scheduling.schedules`); runs the poll activity once and returns how
  many events it started and acknowledged.

Sandbox: only `temporalio` plus the pure contracts and policies. The workflow
never reads the clock or configuration; the sleep length arrives in its input.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import MAINTENANCE_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_APPLY_SCHEDULE_CHANGE,
        ACT_POLL_SCHEDULE_CHANGES,
        SCHEDULE_CHANGE_POLLER_WORKFLOW,
        SCHEDULE_CHANGE_WORKFLOW,
        MaintenanceResult,
        ScheduleChangeInput,
        ScheduleChangeResult,
    )


@workflow.defn(name=SCHEDULE_CHANGE_WORKFLOW)
class ScheduleChangeWorkflow:
    @workflow.run
    async def run(self, inp: ScheduleChangeInput) -> ScheduleChangeResult:
        first = await workflow.execute_activity(
            ACT_APPLY_SCHEDULE_CHANGE,
            replace(inp, pass_number=1),
            result_type=ScheduleChangeResult,
            **MAINTENANCE_ACTIVITY,
        )
        # Durable timer: survives worker restarts and is skipped by the
        # time-skipping test server while a test awaits the result.
        await workflow.sleep(timedelta(minutes=inp.second_pass_after_minutes))
        second = await workflow.execute_activity(
            ACT_APPLY_SCHEDULE_CHANGE,
            replace(inp, pass_number=2),
            result_type=ScheduleChangeResult,
            **MAINTENANCE_ACTIVITY,
        )
        return ScheduleChangeResult(
            withdrawn=first.withdrawn + second.withdrawn,
            flagged=first.flagged + second.flagged,
        )


@workflow.defn(name=SCHEDULE_CHANGE_POLLER_WORKFLOW)
class ScheduleChangePollerWorkflow:
    @workflow.run
    async def run(self) -> MaintenanceResult:
        return await workflow.execute_activity(
            ACT_POLL_SCHEDULE_CHANGES, result_type=MaintenanceResult, **MAINTENANCE_ACTIVITY
        )
