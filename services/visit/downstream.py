"""Starting a completed visit's downstream workflow, exactly once (plan 02-11;
VISIT-04, D-06, D-16, research Pattern 2 / Pattern 8).

Outbox trigger (plan 03-09, D-03): the API commits the `completed` transition with
`downstream_started_at IS NULL` and a `visit.completed` outbox row; the visit-downstream
consumer (`services.visit.consumer`) calls `ensure_downstream_started`, which starts
`visit-completed-{visit_id}` with `start_once` (deterministic id, USE_EXISTING +
REJECT_DUPLICATE) and only then sets the marker with a conditional UPDATE. A Temporal
outage leaves the marker NULL and the event is retried by the consumer, so an outage
delays a trigger and never drops it. The Phase 2 sweeper (a Temporal Schedule plus a
run-on-start sweep) that used to find such visits was retired in plan 03-12 once the
kill-between-commit-and-publish tests (plan 03-11) proved the outbox path and it stays
retired.

Lost triggers (plan 03-22, CR-01): the consumer can still lose an event, because a
record that is dead-lettered (a poison record, or a transient failure that hit the
`TransientExhausted` cap) or consumed and dropped leaves the visit with a NULL marker for
ever. `restart_lost_triggers` is the safety net: the reconcile activity (the existing
`visit-reconciler` Schedule, and once at worker start) restarts every completed visit
whose marker is still NULL after `VISIT_RECONCILE_GRACE_MINUTES` through the same
idempotent `ensure_downstream_started`. `scripts/backfill_visit_completed_events.py` is
the manual route (visits that completed before the outbox existed, or when the worker is
down). Every path is idempotent: a repeated or concurrent start attaches to the running
execution or is refused once it closed, and the marker update is conditional.

Shared by the visit-downstream consumer, the reconciler's lost-trigger pass and the
backfill (the start is the same function the Phase 2 paths used). No execution/run timeout is ever passed (see `libs.temporal`). Logs carry
ids only.

Reconciling (plan 02.1-10, S4): `reconcile_unsettled` is the safety net behind
`VisitCompletedWorkflow`. A completed visit whose billing or appointment
settlement marker is still empty after a grace period (the workflow ended
`skipped` before billing retried open-endedly, was terminated, or never ran a
branch) is settled by running the SAME idempotent step bodies the workflow
runs, with the workflow's deterministic id, so a reconciler pass racing the
workflow (or another reconciler) produces one Billing transition and one audit
entry per step. It never UPDATEs a marker itself: the step bodies set markers
through the conditional `mark_*_settled` functions. A failing step is logged
(`visit.reconcile_step_failed`: ids, step and exception type only) and the
loop continues; the marker stays empty so the next pass retries.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from libs.observability.logging import get_logger
from libs.temporal import start_once
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.domain.contracts import (
    VISIT_COMPLETED_WORKFLOW,
    VisitCompletedInput,
    visit_completed_workflow_id,
)
from services.visit.repository import visit_repo
from services.visit.repository.models import Visit

__all__ = ["ensure_downstream_started", "reconcile_unsettled", "restart_lost_triggers"]

log = get_logger(__name__)

RECONCILE_LIMIT = 100

# A settlement step body: (visit input, deterministic workflow id) -> outcome.
SettleStep = Callable[[VisitCompletedInput, str], Awaitable[str]]


async def ensure_downstream_started(client: Client, db: AsyncSession, visit: Visit) -> bool:
    """Start the visit's downstream workflow (unless it already ran or runs),
    then set and COMMIT `downstream_started_at`. Returns True once the marker
    is set. Temporal RPC/connection errors propagate before anything is
    written; the caller decides (the consumer retries the event)."""
    inp = VisitCompletedInput(
        visit_id=str(visit.id),
        appointment_id=str(visit.appointment_id),
        clinic_id=str(visit.clinic_id),
    )
    fresh = await start_once(
        client,
        VISIT_COMPLETED_WORKFLOW,
        inp,
        workflow_id=visit_completed_workflow_id(inp.visit_id),
        task_queue=VISIT_TASK_QUEUE,
    )
    await visit_repo.mark_downstream_started(db, visit_id=visit.id)
    await db.commit()
    log.info(
        "visit.downstream_started",
        visit_id=inp.visit_id,
        outcome="started" if fresh else "already_closed",
    )
    return True


async def _open_steps(
    session_factory: Callable[[], AsyncSession], visit_id: uuid.UUID
) -> tuple[bool, bool] | None:
    """(billing marker empty, appointment marker empty) for a completed visit,
    read fresh; None when the visit is gone or not completed."""
    async with session_factory() as db:
        visit = await visit_repo.get_visit(db, visit_id)
        if visit is None or visit.status != "completed":
            return None
        return visit.billing_settled_at is None, visit.appointment_settled_at is None


async def _run_step(
    step: SettleStep, name: str, inp: VisitCompletedInput, workflow_id: str
) -> None:
    try:
        await step(inp, workflow_id)
    except ApplicationError as exc:
        if exc.non_retryable:
            # A terminal business outcome: the step body already wrote its marker.
            log.info("visit.reconcile_step_refused", visit_id=inp.visit_id, step=name, error=exc.type)
        else:
            log.warning(
                "visit.reconcile_step_failed", visit_id=inp.visit_id, step=name, error=exc.type
            )
    except Exception as exc:  # noqa: BLE001 - one bad step never stops the pass
        log.warning(
            "visit.reconcile_step_failed", visit_id=inp.visit_id, step=name, error=type(exc).__name__
        )


async def reconcile_unsettled(
    session_factory: Callable[[], AsyncSession],
    *,
    finalize_billing: SettleStep,
    complete_appointment: SettleStep,
    grace_minutes: int,
    limit: int = RECONCILE_LIMIT,
) -> int:
    """Settle completed visits whose billing or appointment marker is still
    empty and that completed more than `grace_minutes` ago (oldest first, at
    most `limit` visits per pass). The ids are read in one short transaction;
    each unsettled step then runs through the shared step body under the
    workflow's deterministic id, in its own short transactions, so no row lock
    is held across an HTTP call. A step whose marker another actor already set
    is skipped; a failure of one step or visit is logged and the pass continues.
    Returns how many visits ended with both markers set."""
    async with session_factory() as db:
        candidates = [
            VisitCompletedInput(
                visit_id=str(v.id), appointment_id=str(v.appointment_id), clinic_id=str(v.clinic_id)
            )
            for v in await visit_repo.list_unsettled(
                db, older_than_minutes=grace_minutes, limit=limit
            )
        ]

    settled = 0
    for inp in candidates:
        visit_id = uuid.UUID(inp.visit_id)
        workflow_id = visit_completed_workflow_id(inp.visit_id)
        for name, step, index in (
            ("billing", finalize_billing, 0),
            ("appointment", complete_appointment, 1),
        ):
            try:
                open_steps = await _open_steps(session_factory, visit_id)
            except Exception as exc:  # noqa: BLE001 - one bad visit never stops the pass
                log.warning(
                    "visit.reconcile_step_failed",
                    visit_id=inp.visit_id,
                    step=name,
                    error=type(exc).__name__,
                )
                break
            if open_steps is None:
                break
            if open_steps[index]:
                await _run_step(step, name, inp, workflow_id)
        try:
            final = await _open_steps(session_factory, visit_id)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "visit.reconcile_step_failed",
                visit_id=inp.visit_id,
                step="verify",
                error=type(exc).__name__,
            )
            continue
        if final == (False, False):
            settled += 1
    log.info("visit.reconcile", scanned=len(candidates), settled=settled)
    return settled


async def restart_lost_triggers(
    session_factory: Callable[[], AsyncSession],
    client: Client,
    *,
    grace_minutes: int,
    limit: int = RECONCILE_LIMIT,
    clinic_id: uuid.UUID | None = None,
) -> int:
    """Restart the downstream workflow of every completed visit whose marker is still NULL
    more than `grace_minutes` after its completion (oldest first, at most `limit` per
    pass; only visits of `clinic_id` when given). The ids are read in one short
    transaction; each visit then gets its own short session: it is re-read, skipped
    unless it is still completed with a NULL marker (the consumer or another pass may
    have got there first), and started with `ensure_downstream_started` (`start_once` on
    the deterministic id, then the conditional marker), so a pass racing the consumer, a
    second pass or the backfill produces one execution. A failure of one visit is logged
    (id and exception class name only) and the pass continues; its marker stays NULL, so
    the next pass retries. Returns how many visits were restarted."""
    async with session_factory() as db:
        ids = [
            v.id
            for v in await visit_repo.list_lost_triggers(
                db, older_than_minutes=grace_minutes, limit=limit, clinic_id=clinic_id
            )
        ]

    restarted = 0
    for visit_id in ids:
        try:
            async with session_factory() as db:
                visit = await visit_repo.get_visit(db, visit_id)
                if (
                    visit is None
                    or visit.status != "completed"
                    or visit.downstream_started_at is not None
                ):
                    continue
                await ensure_downstream_started(client, db, visit)
        except Exception as exc:  # noqa: BLE001 - one bad visit never stops the pass
            log.warning(
                "visit.lost_trigger_restart_failed",
                visit_id=str(visit_id),
                error=type(exc).__name__,
            )
            continue
        restarted += 1
        log.info("visit.lost_trigger_restarted", visit_id=str(visit_id))
    log.info("visit.lost_triggers", scanned=len(ids), restarted=restarted)
    return restarted
