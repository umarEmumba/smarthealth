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
"""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client

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

__all__ = ["ensure_downstream_started", "sweep_unstarted"]

log = get_logger(__name__)

SWEEP_LIMIT = 100


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
