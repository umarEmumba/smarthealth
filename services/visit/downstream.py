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
down). The automatic restart has an age ceiling (gap review WR-07,
`VISIT_LOST_TRIGGER_MAX_AGE_HOURS`, 24 h): an older lost trigger (a database restored from
a snapshot, old harness rows) is only counted and reported on every pass line
(`beyond_max_age`) and in a warning that fires when the count changes; the operator
recovers it with the backfill script. `VISIT_LOST_TRIGGER_RESTART_ENABLED=false` turns the
automatic restart off. A visit whose restart fails (any exception, a Temporal RPC error
included) is backed off in process (`LostTriggerBackoff`: skipped for the next 1, 2, 4 ...
up to 32 passes, ids only, lost on a restart), so a visit that fails on every pass cannot
keep newer lost triggers behind the per-pass cap; three consecutive RPC errors in one pass
are treated as a systemic Temporal outage and stop the pass (only the visits attempted are
backed off). Every path is idempotent: a repeated or concurrent start attaches to the running
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
from temporalio.service import RPCError

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

__all__ = [
    "LostTriggerBackoff",
    "ensure_downstream_started",
    "reconcile_unsettled",
    "restart_lost_triggers",
]

log = get_logger(__name__)

RECONCILE_LIMIT = 100
DEFAULT_LOST_TRIGGER_MAX_AGE_MINUTES = 24 * 60
LOST_TRIGGER_MAX_CONSECUTIVE_RPC_ERRORS = 3
LOST_TRIGGER_MAX_SKIP_PASSES = 32
LOST_TRIGGER_BACKOFF_MAX_ENTRIES = 1000

# A settlement step body: (visit input, deterministic workflow id) -> outcome.
SettleStep = Callable[[VisitCompletedInput, str], Awaitable[str]]


class LostTriggerBackoff:
    """Per-process memory of the visits whose lost-trigger restart keeps failing (gap review
    WR-07). After the n-th consecutive failure a visit is left out of the next
    min(2 ** (n - 1), LOST_TRIGGER_MAX_SKIP_PASSES) passes, so newer lost triggers are not
    kept behind it by the per-pass cap. Ids and counters only, no I/O, no lock (one event
    loop per worker process); it is lost on a restart."""

    def __init__(self) -> None:
        self._pass = 0
        self._failures: dict[uuid.UUID, int] = {}
        self._eligible_at: dict[uuid.UUID, int] = {}
        self._last_beyond = 0

    def __len__(self) -> int:
        return len(self._eligible_at)

    def start_pass(self) -> frozenset[uuid.UUID]:
        """Begin a pass; the ids still backing off in it."""
        self._pass += 1
        return frozenset(v for v, at in self._eligible_at.items() if at > self._pass)

    def record_failure(self, visit_id: uuid.UUID) -> None:
        failures = self._failures.get(visit_id, 0) + 1
        self._failures[visit_id] = failures
        skips = min(2 ** (failures - 1), LOST_TRIGGER_MAX_SKIP_PASSES)
        self._eligible_at[visit_id] = self._pass + skips + 1
        while len(self._eligible_at) > LOST_TRIGGER_BACKOFF_MAX_ENTRIES:
            soonest = min(self._eligible_at, key=self._eligible_at.__getitem__)
            del self._eligible_at[soonest]
            self._failures.pop(soonest, None)

    def record_success(self, visit_id: uuid.UUID) -> None:
        self._failures.pop(visit_id, None)
        self._eligible_at.pop(visit_id, None)

    def note_beyond_max_age(self, count: int) -> bool:
        """Store this pass's beyond-the-ceiling count; True only when it is non-zero and
        differs from the previous pass's (the warning fires on a change, not every pass)."""
        changed = count > 0 and count != self._last_beyond
        self._last_beyond = count
        return changed


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
    max_age_minutes: int = DEFAULT_LOST_TRIGGER_MAX_AGE_MINUTES,
    backoff: LostTriggerBackoff | None = None,
) -> int:
    """Restart the downstream workflow of every completed visit whose marker is still NULL
    more than `grace_minutes` and at most `max_age_minutes` after its completion (oldest
    first, at most `limit` per pass; only visits of `clinic_id` when given). The ids are
    read in one short transaction; each visit then gets its own short session: it is
    re-read, skipped unless it is still completed with a NULL marker (the consumer or
    another pass may have got there first), and started with `ensure_downstream_started`
    (`start_once` on the deterministic id, then the conditional marker), so a pass racing
    the consumer, a second pass or the backfill produces one execution.

    A lost trigger older than `max_age_minutes` is never restarted here: it is counted
    (`beyond_max_age` on the pass line, and a warning when the count changes) and the
    operator recovers it with `scripts/backfill_visit_completed_events.py --apply`.
    A failure of one visit is logged (id and exception class name only) and the pass
    continues; its marker stays NULL, so the next pass retries, and `backoff` leaves a visit
    that keeps failing out of the next passes. Three consecutive RPC errors stop the pass
    (a systemic Temporal outage); the visits after the break were never attempted and are
    not backed off. Returns how many visits were restarted."""
    if max_age_minutes <= grace_minutes:
        raise ValueError("max_age_minutes must exceed grace_minutes")
    excluded = backoff.start_pass() if backoff is not None else frozenset()
    async with session_factory() as db:
        ids = [
            v.id
            for v in await visit_repo.list_lost_triggers(
                db,
                older_than_minutes=grace_minutes,
                limit=limit,
                clinic_id=clinic_id,
                max_age_minutes=max_age_minutes,
                exclude_ids=excluded,
            )
        ]
        beyond = await visit_repo.count_lost_triggers_beyond(
            db, older_than_minutes=max_age_minutes, clinic_id=clinic_id
        )
    announce = backoff.note_beyond_max_age(beyond) if backoff is not None else beyond > 0
    if beyond > 0 and announce:
        log.warning(
            "visit.lost_triggers_beyond_max_age", count=beyond, max_age_minutes=max_age_minutes
        )

    restarted = 0
    consecutive_rpc_errors = 0
    aborted = False
    for visit_id in ids:
        try:
            async with session_factory() as db:
                visit = await visit_repo.get_visit(db, visit_id)
                if (
                    visit is None
                    or visit.status != "completed"
                    or visit.downstream_started_at is not None
                ):
                    if backoff is not None:
                        backoff.record_success(visit_id)
                    consecutive_rpc_errors = 0
                    continue
                await ensure_downstream_started(client, db, visit)
        except RPCError as exc:
            log.warning(
                "visit.lost_trigger_restart_failed",
                visit_id=str(visit_id),
                error=type(exc).__name__,
            )
            if backoff is not None:
                backoff.record_failure(visit_id)
            consecutive_rpc_errors += 1
            if consecutive_rpc_errors >= LOST_TRIGGER_MAX_CONSECUTIVE_RPC_ERRORS:
                log.warning(
                    "visit.lost_trigger_pass_aborted",
                    error=type(exc).__name__,
                    consecutive=consecutive_rpc_errors,
                )
                aborted = True
                break
            continue
        except Exception as exc:  # noqa: BLE001 - one bad visit never stops the pass
            log.warning(
                "visit.lost_trigger_restart_failed",
                visit_id=str(visit_id),
                error=type(exc).__name__,
            )
            if backoff is not None:
                backoff.record_failure(visit_id)
            consecutive_rpc_errors = 0
            continue
        if backoff is not None:
            backoff.record_success(visit_id)
        consecutive_rpc_errors = 0
        restarted += 1
        log.info("visit.lost_trigger_restarted", visit_id=str(visit_id))
    log.info(
        "visit.lost_triggers",
        scanned=len(ids),
        restarted=restarted,
        backed_off=len(excluded),
        beyond_max_age=beyond,
        aborted=aborted,
    )
    return restarted
