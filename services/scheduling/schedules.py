"""Idempotent registration of Scheduling's Temporal Schedules (plan 02-07;
D-03, research Pattern 8 / Code Example 4, T-02-35).

The worker calls `ensure_schedules(client)` at every start, before polling.
Creating a Schedule whose id already exists raises
`ScheduleAlreadyRunningError`, which means "already registered", so restarts
and several workers are harmless. An existing Schedule is left as it is (an
operator may have paused or tuned it); changing an interval in settings
therefore needs the Schedule updated or deleted once.

* `{prefix}-hold-reaper`     -> `HoldReaperWorkflow` every
                                `reaper_interval_seconds` (60 s)
* `{prefix}-pending-starter`  -> `PendingBookingStarterWorkflow` every
                                `starter_interval_seconds` (30 s)
* `{prefix}-schedule-change-poller` -> `ScheduleChangePollerWorkflow` every
                                `schedule_change_poll_seconds` (15 s; plan
                                02.1-08, M2: drains Provider's change feed)
* `{prefix}-reconciler`      -> `ReconcilerWorkflow` every
                                `reconciler_interval_seconds` (60 s; plan
                                02.1-11, S4: stuck pending appointments and
                                waitlist entries, billing orphans, no-shows)

All use `ScheduleOverlapPolicy.SKIP`: a run still in progress makes the next
one skip instead of queueing behind it. The start actions use deterministic
workflow ids (`{prefix}-hold-reaper-run`, ...); Temporal appends the scheduled
time to each run's id.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)

from libs.observability.logging import get_logger
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    HOLD_REAPER_WORKFLOW,
    PENDING_STARTER_WORKFLOW,
    RECONCILER_WORKFLOW,
    SCHEDULE_CHANGE_POLLER_WORKFLOW,
)

__all__ = ["ensure_schedules"]

log = get_logger(__name__)


def _definitions(prefix: str) -> list[tuple[str, Schedule]]:
    return [
        (
            f"{prefix}-hold-reaper",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    HOLD_REAPER_WORKFLOW,
                    id=f"{prefix}-hold-reaper-run",
                    task_queue=SCHEDULING_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(every=timedelta(seconds=settings.reaper_interval_seconds))
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
        (
            f"{prefix}-pending-starter",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    PENDING_STARTER_WORKFLOW,
                    id=f"{prefix}-pending-starter-run",
                    task_queue=SCHEDULING_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(every=timedelta(seconds=settings.starter_interval_seconds))
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
        (
            f"{prefix}-schedule-change-poller",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    SCHEDULE_CHANGE_POLLER_WORKFLOW,
                    id=f"{prefix}-schedule-change-poller-run",
                    task_queue=SCHEDULING_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(
                            every=timedelta(seconds=settings.schedule_change_poll_seconds)
                        )
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
        (
            f"{prefix}-reconciler",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    RECONCILER_WORKFLOW,
                    id=f"{prefix}-reconciler-run",
                    task_queue=SCHEDULING_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(
                            every=timedelta(seconds=settings.reconciler_interval_seconds)
                        )
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
    ]


async def ensure_schedules(client: Client, *, id_prefix: str = "scheduling") -> list[str]:
    """Create the reaper, starter, schedule-change poller and reconciler Schedules
    unless they exist.

    Returns the ids created by THIS call (empty when all already existed).
    Other RPC errors propagate: the worker treats them as a failed start.
    """
    created: list[str] = []
    for schedule_id, schedule in _definitions(id_prefix):
        try:
            await client.create_schedule(schedule_id, schedule)
        except ScheduleAlreadyRunningError:
            log.info("schedule.already_registered", schedule_id=schedule_id)
            continue
        log.info("schedule.registered", schedule_id=schedule_id)
        created.append(schedule_id)
    return created
