"""Idempotent registration of Visit's Temporal Schedule (plan 02-11; D-16,
research Pattern 8 / Code Example 4). Mirrors `services.scheduling.schedules`.

The visit worker calls `ensure_visit_schedules(client)` at every start, before
polling. Creating a Schedule whose id already exists raises
`ScheduleAlreadyRunningError`, which means "already registered", so restarts
and several workers are harmless. An existing Schedule is left as it is (an
operator may have paused or tuned it); changing the interval in settings
therefore needs the Schedule updated or deleted once.

* `{prefix}-downstream-sweeper` -> `VisitDownstreamSweeperWorkflow` every
  `sweeper_interval_seconds` (60 s), `ScheduleOverlapPolicy.SKIP` (a run still
  in progress makes the next one skip instead of queueing). The start action
  uses the deterministic id `{prefix}-downstream-sweeper-run`; Temporal appends
  the scheduled time to each run's id.
* `{prefix}-reconciler` (plan 02.1-10, S4) -> `VisitReconcilerWorkflow` every
  `reconcile_interval_seconds` (60 s), overlap SKIP, start id
  `{prefix}-reconciler-run`: settles completed visits whose billing or
  appointment marker is still empty after `reconcile_grace_minutes`.
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
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.config import settings
from services.visit.domain.contracts import SWEEPER_WORKFLOW, VISIT_RECONCILER_WORKFLOW

__all__ = ["ensure_visit_schedules"]

log = get_logger(__name__)


def _definitions(prefix: str) -> list[tuple[str, Schedule]]:
    return [
        (
            f"{prefix}-downstream-sweeper",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    SWEEPER_WORKFLOW,
                    id=f"{prefix}-downstream-sweeper-run",
                    task_queue=VISIT_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(every=timedelta(seconds=settings.sweeper_interval_seconds))
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
        (
            f"{prefix}-reconciler",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    VISIT_RECONCILER_WORKFLOW,
                    id=f"{prefix}-reconciler-run",
                    task_queue=VISIT_TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[
                        ScheduleIntervalSpec(
                            every=timedelta(seconds=settings.reconcile_interval_seconds)
                        )
                    ]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        ),
    ]


async def ensure_visit_schedules(client: Client, *, id_prefix: str = "visit") -> list[str]:
    """Create the sweeper and reconciler Schedules unless they exist. Returns the ids
    created by THIS call (empty when it already existed). Other RPC errors
    propagate: the worker treats them as a failed start."""
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
