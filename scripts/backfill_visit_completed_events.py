"""One-shot, idempotent backfill of `visit.completed` events (plan 03-12, D-03).

Why this exists: before plan 03-09 a completed visit was started downstream by the API
(and by the Phase 2 sweeper); since then the `visit.completed` outbox event consumed by
the visit-downstream consumer is the only trigger, and plan 03-12 retired the sweeper.
A visit that completed BEFORE the outbox existed, and whose downstream workflow was never
started (`status = 'completed' AND downstream_started_at IS NULL`), has no event and no
other trigger left. This script writes that event for each such visit, so the consumer
starts the downstream workflow (billing finalize, appointment completion, intents) and
the analytics projection learns about the visit.

How: it selects the candidates with `visit_repo.list_completed_without_downstream`, and
for each one opens its OWN transaction, re-reads the visit, and calls the SAME
`emit_visit_completed` the live `completed` transition calls, so the event it writes has
the id and payload the transition would have written (id = uuid5 of
`visit id : visit.completed : completed`, `occurred_at` = the completion time).

Idempotency: the outbox primary key is that uuid5 id and the insert is `ON CONFLICT DO
NOTHING`; the script also checks for an existing row first, so a visit whose event is
already queued (committed but not consumed yet, or backfilled by an earlier run) is
never written to again. A second `--apply` prints `written=0`. A candidate whose marker
was set meanwhile (the consumer got there first) is skipped.

Stale queued events (plan 03-22, CR-01): an existing outbox row does NOT mean the visit
was handled. A dead-lettered `visit.completed` (a poison record, or a transient failure
that hit the consumer's `TransientExhausted` cap) has a row too, and its visit keeps
`downstream_started_at IS NULL` for ever. A candidate whose event exists but whose
completion is older than `--stale-after-minutes` (default `VISIT_RECONCILE_GRACE_MINUTES`,
10) is therefore counted `stale_queued`, not `already_queued`, and with `--apply` its
downstream work is restarted with the same idempotent `ensure_downstream_started` the
outbox consumer and the visit-reconciler use (`start_once` on `visit-completed-<id>`, then
the conditional marker): `restarted=N`. The visit-reconciler does this restart
automatically after the same grace; this script is the manual route (for example when the
visit worker is down, or to act at once). A restart starts downstream work only (billing
finalize, appointment completion, the intents); no notification is sent by this path.
Restarting needs Temporal; from the host set `TEMPORAL_ADDRESS=localhost:7233`.

CAUTION: a real run starts one downstream workflow per written or restarted visit and
feeds the analytics projection. Run the dry run first and read the counts; do not
`--apply` against a database that holds throw-away test visits you do not want processed.

Usage (repo root, host `.env` / defaults for `VISIT_DATABASE_URL`):

    .venv/bin/python scripts/backfill_visit_completed_events.py            # dry run (default)
    .venv/bin/python scripts/backfill_visit_completed_events.py --apply    # write / restart
        [--limit N]                                                         # default 1000
        [--clinic-id UUID]                                                  # one clinic only
        [--stale-after-minutes N]                                           # default 10

Output is one line of counts only, `mode=... candidates=N already_queued=K written=M
skipped=S stale_queued=Q restarted=R`; no ids, no URLs. Exit codes: 0 on success (including
a dry run), 1 on any database, registry or Temporal error (the exception class name is
printed), 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from dataclasses import dataclass

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client

# Repo root on sys.path so this works as `python scripts/backfill_visit_completed_events.py`
# from the repo root (same convention as scripts/bootstrap_admin.py).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libs.db.postgres import get_session_factory  # noqa: E402
from libs.events.catalog import EventType  # noqa: E402
from libs.events.outbox import event_id_for  # noqa: E402
from libs.events.registry import warm_up  # noqa: E402
from libs.temporal import connect_client  # noqa: E402
from services.visit.config import settings  # noqa: E402
from services.visit.domain.states import VisitStatus  # noqa: E402
from services.visit.downstream import ensure_downstream_started  # noqa: E402
from services.visit.events import COMPLETED_DISCRIMINATOR, emit_visit_completed  # noqa: E402
from services.visit.repository import visit_repo  # noqa: E402

DEFAULT_LIMIT = 1000


@dataclass
class BackfillCounts:
    candidates: int = 0
    already_queued: int = 0
    written: int = 0
    skipped: int = 0
    stale_queued: int = 0
    restarted: int = 0

    def line(self, *, apply: bool) -> str:
        mode = "apply" if apply else "dry-run"
        return (
            f"mode={mode} candidates={self.candidates} already_queued={self.already_queued} "
            f"written={self.written} skipped={self.skipped} "
            f"stale_queued={self.stale_queued} restarted={self.restarted}"
        )


async def _event_exists(db: AsyncSession, visit_id: object) -> bool:
    event_id = event_id_for(str(visit_id), EventType.VISIT_COMPLETED, COMPLETED_DISCRIMINATOR)
    found = await db.scalar(text("SELECT 1 FROM outbox WHERE id = :id"), {"id": event_id})
    return found is not None


async def _is_stale(db: AsyncSession, visit_id: object, stale_after_minutes: int) -> bool:
    """True when the visit's completion is older than the stale window, by the database
    clock (the same clock `completed_at` was written with), never the host's."""
    return bool(
        await db.scalar(
            text(
                "SELECT completed_at < now() - make_interval(secs => :s) FROM visits WHERE id = :v"
            ),
            {"s": float(stale_after_minutes) * 60.0, "v": visit_id},
        )
    )


async def backfill(
    session_factory,
    *,
    apply: bool,
    limit: int = DEFAULT_LIMIT,
    clinic_id: uuid.UUID | None = None,
    stale_after_minutes: int | None = None,
    client: Client | None = None,
) -> BackfillCounts:
    """Select the cutover candidates and (with `apply`) write one `visit.completed` outbox
    row for each that has none (only visits of `clinic_id` when given). A candidate whose
    event already exists is `already_queued`, or `stale_queued` when its completion is older
    than `stale_after_minutes` (default `VISIT_RECONCILE_GRACE_MINUTES`); with `apply` each
    stale one gets `ensure_downstream_started(client, ...)` (`client` is then required).
    Returns the counts; writes and restarts nothing when `apply` is False."""
    stale_after = settings.reconcile_grace_minutes if stale_after_minutes is None else stale_after_minutes
    counts = BackfillCounts()
    async with session_factory() as db:
        ids = [
            v.id
            for v in await visit_repo.list_completed_without_downstream(
                db, limit=limit, clinic_id=clinic_id
            )
        ]
    counts.candidates = len(ids)

    stale_ids: list[uuid.UUID] = []
    for visit_id in ids:
        async with session_factory() as db:
            visit = await visit_repo.get_visit(db, visit_id)
            if (
                visit is None
                or visit.status != VisitStatus.COMPLETED
                or visit.downstream_started_at is not None
                or visit.completed_at is None
            ):
                counts.skipped += 1  # changed since the selection (or unusable): leave it
                continue
            if await _event_exists(db, visit_id):
                if await _is_stale(db, visit_id, stale_after):
                    counts.stale_queued += 1
                    stale_ids.append(visit_id)
                else:
                    counts.already_queued += 1
                continue
            if not apply:
                continue
            await emit_visit_completed(db, visit)
            await db.commit()
            counts.written += 1

    if apply and stale_ids:
        if client is None:
            raise RuntimeError("a Temporal client is required to restart stale queued visits")
        for visit_id in stale_ids:
            async with session_factory() as db:
                visit = await visit_repo.get_visit(db, visit_id)
                if (
                    visit is None
                    or visit.status != VisitStatus.COMPLETED
                    or visit.downstream_started_at is not None
                ):
                    counts.skipped += 1  # the consumer or the reconciler got there first
                    continue
                await ensure_downstream_started(client, db, visit)
                counts.restarted += 1
    return counts


async def _run(
    *, apply: bool, limit: int, clinic_id: uuid.UUID | None, stale_after_minutes: int | None
) -> BackfillCounts:
    factory = get_session_factory(settings.database_url)
    if not apply:
        return await backfill(
            factory,
            apply=False,
            limit=limit,
            clinic_id=clinic_id,
            stale_after_minutes=stale_after_minutes,
        )
    # Resolve the Avro schema id once, off the event loop (the emit path needs it).
    await warm_up([EventType.VISIT_COMPLETED])
    # Connect to Temporal only when a stale queued visit will be restarted.
    preview = await backfill(
        factory,
        apply=False,
        limit=limit,
        clinic_id=clinic_id,
        stale_after_minutes=stale_after_minutes,
    )
    client = await connect_client(lazy=False) if preview.stale_queued > 0 else None
    return await backfill(
        factory,
        apply=True,
        limit=limit,
        clinic_id=clinic_id,
        stale_after_minutes=stale_after_minutes,
        client=client,
    )


def _quiet_logging() -> None:
    """Importing the service modules configures INFO logging on stdout (the registry's HTTP
    client and `ensure_downstream_started` would then print URLs and visit ids). The script's
    stdout is one line of counts: keep the libraries to errors, on stderr."""
    logging.getLogger().setLevel(logging.ERROR)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR),
        logger_factory=structlog.PrintLoggerFactory(sys.stderr),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--apply", action="store_true", help="write the events (default: dry run)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="most visits per run")
    parser.add_argument("--clinic-id", type=uuid.UUID, default=None, help="only this clinic")
    parser.add_argument(
        "--stale-after-minutes",
        type=int,
        default=None,
        help="a queued event whose completion is older than this is stale and its visit is "
        "restarted on --apply (default: VISIT_RECONCILE_GRACE_MINUTES)",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.stale_after_minutes is not None and args.stale_after_minutes < 0:
        parser.error("--stale-after-minutes must be at least 0")

    _quiet_logging()
    try:
        counts = asyncio.run(
            _run(
                apply=args.apply,
                limit=args.limit,
                clinic_id=args.clinic_id,
                stale_after_minutes=args.stale_after_minutes,
            )
        )
    except Exception as exc:  # noqa: BLE001 - class name only: messages can carry URLs
        print(f"backfill failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(counts.line(apply=args.apply))
    return 0


if __name__ == "__main__":
    sys.exit(main())
