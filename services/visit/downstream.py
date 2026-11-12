"""Starting a completed visit's downstream workflow, exactly once (plan 02-11;
VISIT-04, D-06, D-16, research Pattern 2 / Pattern 8).

Commit-then-start: the API commits the `completed` transition with
`downstream_started_at IS NULL`, then calls `ensure_downstream_started`, which
starts `visit-completed-{visit_id}` with `start_once` (deterministic id,
USE_EXISTING + REJECT_DUPLICATE) and only then sets the marker with a
conditional UPDATE. A crash or a Temporal outage between the commit and the
start leaves the marker NULL; `sweep_unstarted` (run once when the visit worker
starts and then by the `visit-downstream-sweeper` Temporal Schedule) finds
those visits and calls the same function. Every path is idempotent: a repeated
or concurrent start attaches to the running execution or is refused once it
closed, and the marker update is conditional.

Shared by the API route, the sweep activity and the worker's start-up sweep.
No execution/run timeout is ever passed (see `libs.temporal`). Logs carry ids
only.

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

__all__ = ["ensure_downstream_started", "reconcile_unsettled", "sweep_unstarted"]

log = get_logger(__name__)

SWEEP_LIMIT = 100
RECONCILE_LIMIT = 100

# A settlement step body: (visit input, deterministic workflow id) -> outcome.
SettleStep = Callable[[VisitCompletedInput, str], Awaitable[str]]


async def ensure_downstream_started(client: Client, db: AsyncSession, visit: Visit) -> bool:
    """Start the visit's downstream workflow (unless it already ran or runs),
    then set and COMMIT `downstream_started_at`. Returns True once the marker
    is set. Temporal RPC/connection errors propagate before anything is
    written; the caller decides (the API defers to the sweeper)."""
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


async def sweep_unstarted(
    session_factory: Callable[[], AsyncSession],
    client: Client,
    *,
    grace_seconds: int,
    limit: int = SWEEP_LIMIT,
) -> int:
    """Start the downstream workflow for completed visits whose marker is
    still NULL and that completed more than `grace_seconds` ago. Ids are read
    in one short transaction; each visit is then started in its own short
    transaction, so no row lock is held across a Temporal call. A failure on
    one visit is logged and the sweep continues. Returns how many visits got
    their marker set."""
    async with session_factory() as db:
        ids = [
            v.id
            for v in await visit_repo.list_downstream_unstarted(
                db, older_than_seconds=grace_seconds, limit=limit
            )
        ]

    processed = 0
    for visit_id in ids:
        try:
            async with session_factory() as db:
                visit = await visit_repo.get_visit(db, visit_id)
                if visit is None or visit.downstream_started_at is not None:
                    continue  # another sweeper or the API got there first
                if await ensure_downstream_started(client, db, visit):
                    processed += 1
        except Exception as exc:  # noqa: BLE001 - one bad visit never stops the sweep
            log.warning("visit.sweep_start_failed", visit_id=str(visit_id), error=type(exc).__name__)
    log.info("visit.sweep", scanned=len(ids), processed=processed)
    return processed


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
