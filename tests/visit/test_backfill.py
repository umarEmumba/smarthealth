"""The cutover backfill of `visit.completed` events (plan 03-12 Task 3; D-03,
research Runtime State Inventory, T-3-39).

`scripts/backfill_visit_completed_events.py` writes one `visit.completed` outbox row for
each completed visit whose downstream workflow was never started and that has no event
yet. These tests run the real script as a subprocess against the live visit_db, SCOPED to
a unique clinic with `--clinic-id`, so the throw-away visits other tests (and earlier
manual runs) left in the database are never touched. Candidate visits are inserted
directly, as pre-cutover data would be (no outbox row, marker NULL).

Delivering the backfilled event with `deliver_visit_events` (the harness stand-in for
Debezium + the visit-downstream consumer) starts the downstream workflow once.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment

from libs.events.outbox import event_id_for
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import visit_completed_workflow_id
from tests.visit.conftest import VisitWorld, deliver_visit_events, outbox_rows

pytestmark = pytest.mark.integration

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_visit_completed_events.py"
COUNTS = re.compile(
    r"^mode=(?P<mode>[a-z-]+) candidates=(?P<candidates>\d+) already_queued=(?P<already>\d+) "
    r"written=(?P<written>\d+) skipped=(?P<skipped>\d+)$"
)


async def _insert_visit(
    vw: VisitWorld, *, status: str, marked: bool = False, completed: bool = True
) -> str:
    """A visit row as pre-cutover data looks: inserted by SQL, no outbox row."""
    world = vw.world
    visit_id = uuid.uuid4()
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO visits (id, appointment_id, clinic_id, patient_id, provider_id, "
                    "status, started_at, completed_at, downstream_started_at, created_by) "
                    "VALUES (:id, :a, :c, :pt, :pr, :status, "
                    "CASE WHEN :completed THEN now() - interval '2 hours' END, "
                    "CASE WHEN :completed THEN now() - interval '1 hour' END, "
                    "CASE WHEN :marked THEN now() END, :by)"
                ),
                {
                    "id": visit_id,
                    "a": uuid.uuid4(),
                    "c": world.clinic_id,
                    "pt": world.patient_id,
                    "pr": world.provider_id,
                    "status": status,
                    "completed": completed,
                    "marked": marked,
                    "by": uuid.uuid4(),
                },
            )
    finally:
        await engine.dispose()
    return str(visit_id)


async def _run_script(clinic_id: uuid.UUID, *args: str) -> tuple[int, str, str, dict[str, Any]]:
    """Run the real script in a subprocess; (exit code, stdout, stderr, parsed counts)."""
    proc = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(SCRIPT), "--clinic-id", str(clinic_id), *args],
        capture_output=True,
        text=True,
        cwd=str(SCRIPT.parents[1]),
        check=False,
    )
    match = COUNTS.match(proc.stdout.strip())
    counts = (
        {k: (v if k == "mode" else int(v)) for k, v in match.groupdict().items()} if match else {}
    )
    return proc.returncode, proc.stdout, proc.stderr, counts


async def test_apply_writes_one_event_for_an_unstarted_completed_visit(
    visit_world: VisitWorld,
) -> None:
    visit_id = await _insert_visit(visit_world, status="completed")
    assert await outbox_rows(visit_id) == []

    code, out, err, counts = await _run_script(visit_world.clinic_id, "--apply")

    assert code == 0, err
    assert counts == {"mode": "apply", "candidates": 1, "already": 0, "written": 1, "skipped": 0}, out
    rows = await outbox_rows(visit_id)
    assert len(rows) == 1
    # The same id the live transition would have written (uuid5, discriminator `completed`).
    assert rows[0].id == event_id_for(visit_id, "visit.completed", "completed")
    assert rows[0].type == "visit.completed"
    assert rows[0].aggregateid == visit_id


async def test_second_apply_writes_nothing_new(visit_world: VisitWorld) -> None:
    visit_id = await _insert_visit(visit_world, status="completed")

    first = await _run_script(visit_world.clinic_id, "--apply")
    second = await _run_script(visit_world.clinic_id, "--apply")

    assert first[0] == second[0] == 0, (first[2], second[2])
    assert first[3]["written"] == 1
    # The marker is still NULL (no consumer ran), so the visit is a candidate again, but
    # its event exists: counted as already queued, never written twice.
    assert second[3] == {"mode": "apply", "candidates": 1, "already": 1, "written": 0, "skipped": 0}
    assert len(await outbox_rows(visit_id)) == 1


async def test_started_marked_or_unfinished_visits_are_never_selected(
    visit_world: VisitWorld,
) -> None:
    marked = await _insert_visit(visit_world, status="completed", marked=True)
    in_progress = await _insert_visit(visit_world, status="in_progress", completed=False)
    checked_in = await _insert_visit(visit_world, status="checked_in", completed=False)
    candidate = await _insert_visit(visit_world, status="completed")

    code, out, err, counts = await _run_script(visit_world.clinic_id, "--apply")

    assert code == 0, err
    assert counts["candidates"] == 1 and counts["written"] == 1, out
    assert len(await outbox_rows(candidate)) == 1
    for other in (marked, in_progress, checked_in):
        assert await outbox_rows(other) == []


async def test_dry_run_writes_nothing_and_reports_the_candidate_count(
    visit_world: VisitWorld,
) -> None:
    first = await _insert_visit(visit_world, status="completed")
    second = await _insert_visit(visit_world, status="completed")

    code, out, err, counts = await _run_script(visit_world.clinic_id)  # no --apply

    assert code == 0, err
    assert counts == {"mode": "dry-run", "candidates": 2, "already": 0, "written": 0, "skipped": 0}, out
    assert await outbox_rows(first) == []
    assert await outbox_rows(second) == []


async def test_delivering_the_backfilled_event_starts_downstream_work_once(
    visit_world: VisitWorld, temporal_env: WorkflowEnvironment
) -> None:
    visit_id = await _insert_visit(visit_world, status="completed")
    code, _, err, _ = await _run_script(visit_world.clinic_id, "--apply")
    assert code == 0, err

    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    assert await deliver_visit_events(None, visit_id, client=temporal_env.client) == 1
    first = await handle.describe()
    assert first.status == WorkflowExecutionStatus.RUNNING  # no worker polls visit-tq
    await deliver_visit_events(None, visit_id, client=temporal_env.client)  # redelivery

    # One execution: the deterministic id keeps the same run after the redelivery.
    assert (await handle.describe()).run_id == first.run_id
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            marker = await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()
    assert marker is not None
    await handle.terminate(reason="test clean-up")


async def test_output_is_counts_only(visit_world: VisitWorld) -> None:
    visit_id = await _insert_visit(visit_world, status="completed")

    code, out, err, counts = await _run_script(visit_world.clinic_id, "--apply")

    assert code == 0 and counts, (out, err)
    assert len(out.strip().splitlines()) == 1
    for secret in (visit_id, str(visit_world.clinic_id), "postgresql", "://", "@"):
        assert secret not in out and secret not in err
