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
counted as `already_queued` and written to again never. A second `--apply` prints
`written=0`. A candidate whose marker was set meanwhile (the consumer got there first) is
skipped.

CAUTION: a real run starts one downstream workflow per written event and feeds the
analytics projection. Run the dry run first and read the count; do not `--apply` against a
database that holds throw-away test visits you do not want processed.

Usage (repo root, host `.env` / defaults for `VISIT_DATABASE_URL`):

    .venv/bin/python scripts/backfill_visit_completed_events.py            # dry run (default)
    .venv/bin/python scripts/backfill_visit_completed_events.py --apply    # write the events
        [--limit N]                                                         # default 1000
        [--clinic-id UUID]                                                  # one clinic only

Output is one line of counts only, `mode=... candidates=N already_queued=K written=M
skipped=S`; no ids, no URLs. Exit codes: 0 on success (including a dry run), 1 on any
database or registry error (the exception class name is printed), 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Repo root on sys.path so this works as `python scripts/backfill_visit_completed_events.py`
# from the repo root (same convention as scripts/bootstrap_admin.py).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libs.db.postgres import get_session_factory  # noqa: E402
from libs.events.catalog import EventType  # noqa: E402
from libs.events.outbox import event_id_for  # noqa: E402
from libs.events.registry import warm_up  # noqa: E402
from services.visit.config import settings  # noqa: E402
from services.visit.domain.states import VisitStatus  # noqa: E402
from services.visit.events import COMPLETED_DISCRIMINATOR, emit_visit_completed  # noqa: E402
from services.visit.repository import visit_repo  # noqa: E402

DEFAULT_LIMIT = 1000


@dataclass
class BackfillCounts:
    candidates: int = 0
    already_queued: int = 0
    written: int = 0
    skipped: int = 0

    def line(self, *, apply: bool) -> str:
        mode = "apply" if apply else "dry-run"
        return (
            f"mode={mode} candidates={self.candidates} already_queued={self.already_queued} "
            f"written={self.written} skipped={self.skipped}"
        )


async def _event_exists(db: AsyncSession, visit_id: object) -> bool:
    event_id = event_id_for(str(visit_id), EventType.VISIT_COMPLETED, COMPLETED_DISCRIMINATOR)
    found = await db.scalar(text("SELECT 1 FROM outbox WHERE id = :id"), {"id": event_id})
    return found is not None


async def backfill(
    session_factory,
    *,
    apply: bool,
    limit: int = DEFAULT_LIMIT,
    clinic_id: uuid.UUID | None = None,
) -> BackfillCounts:
    """Select the cutover candidates and (with `apply`) write one `visit.completed` outbox
    row for each that has none (only visits of `clinic_id` when given). Returns the counts;
    writes nothing when `apply` is False."""
    counts = BackfillCounts()
    async with session_factory() as db:
        ids = [
            v.id
            for v in await visit_repo.list_completed_without_downstream(
                db, limit=limit, clinic_id=clinic_id
            )
        ]
    counts.candidates = len(ids)

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
                counts.already_queued += 1
                continue
            if not apply:
                continue
            await emit_visit_completed(db, visit)
            await db.commit()
            counts.written += 1
    return counts


async def _run(*, apply: bool, limit: int, clinic_id: uuid.UUID | None) -> BackfillCounts:
    if apply:
        # Resolve the Avro schema id once, off the event loop (the emit path needs it).
        await warm_up([EventType.VISIT_COMPLETED])
    return await backfill(
        get_session_factory(settings.database_url), apply=apply, limit=limit, clinic_id=clinic_id
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--apply", action="store_true", help="write the events (default: dry run)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="most visits per run")
    parser.add_argument("--clinic-id", type=uuid.UUID, default=None, help="only this clinic")
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")

    try:
        counts = asyncio.run(
            _run(apply=args.apply, limit=args.limit, clinic_id=args.clinic_id)
        )
    except Exception as exc:  # noqa: BLE001 - class name only: messages can carry URLs
        print(f"backfill failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(counts.line(apply=args.apply))
    return 0


if __name__ == "__main__":
    sys.exit(main())
