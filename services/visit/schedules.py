"""Idempotent registration of Visit's Temporal Schedule (plan 02-11; D-16,
research Pattern 8 / Code Example 4), and the retirement of the downstream
sweeper's Schedule (plan 03-12, D-03). Mirrors `services.scheduling.schedules`.

The visit worker calls `ensure_visit_schedules(client)` at every start, before
polling. Creating a Schedule whose id already exists raises
`ScheduleAlreadyRunningError`, which means "already registered", so restarts
and several workers are harmless. An existing Schedule is left as it is (an
operator may have paused or tuned it); changing the interval in settings
therefore needs the Schedule updated or deleted once.

* `{prefix}-reconciler` (plan 02.1-10, S4) -> `VisitReconcilerWorkflow` every
  `reconcile_interval_seconds` (60 s), `ScheduleOverlapPolicy.SKIP` (a run still
  in progress makes the next one skip instead of queueing), start id
  `{prefix}-reconciler-run`: settles completed visits whose billing or
  appointment marker is still empty after `reconcile_grace_minutes`.

The `{prefix}-downstream-sweeper` Schedule (plan 02-11) is RETIRED (plan 03-12,
D-03): since plan 03-09 the `visit.completed` outbox event, consumed by the
visit-downstream consumer, is the only trigger of the downstream workflow. Because an
existing Schedule survives the removal of the code that created it,
`retire_sweeper_schedule(client)` deletes it explicitly. The worker calls it at every
start (idempotent: an absent Schedule is fine); nothing creates the Schedule again.
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
from temporalio.service import RPCError, RPCStatusCode

from libs.observability.logging import get_logger
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.config import settings
from services.visit.domain.contracts import VISIT_RECONCILER_WORKFLOW

__all__ = ["ensure_visit_schedules", "retire_sweeper_schedule"]

log = get_logger(__name__)


def _definitions(prefix: str) -> list[tuple[str, Schedule]]:
    return [
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
    """Create the reconciler Schedule unless it exists. Returns the ids
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


async def retire_sweeper_schedule(client: Client, *, id_prefix: str = "visit") -> bool:
    """Delete the retired `{prefix}-downstream-sweeper` Schedule (plan 03-12, D-03).

    Only that exact id is touched; every other Schedule (the reconciler, Scheduling's)
    is left as it is. Returns True when this call deleted it, False when it was already
    absent (a second call, a fresh server). Other RPC errors propagate: the worker
    treats them as a failed start."""
    schedule_id = f"{id_prefix}-downstream-sweeper"
    try:
        await client.get_schedule_handle(schedule_id).delete()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            log.info("schedule.already_retired", schedule_id=schedule_id)
            return False
        raise
    log.info("schedule.retired", schedule_id=schedule_id)
    return True
