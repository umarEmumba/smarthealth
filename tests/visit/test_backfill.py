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

Plan 03-22 (CR-01): a queued event whose completion is older than `--stale-after-minutes`
is `stale_queued` (a dead-lettered event has an outbox row too) and `--apply` restarts its
downstream work through `ensure_downstream_started`. The stale tests call `backfill` in
process with the test Temporal server's client and their own `clinic_id`. Candidate visits
complete 2 minutes ago (inside the live visit-reconciler's 10 minute grace), so the live
reconciler cannot race these tests; "stale" is reached with `stale_after_minutes=0`.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
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
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment

from libs.db.postgres import get_session_factory
from libs.events.outbox import event_id_for
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import visit_completed_workflow_id
from tests.visit.conftest import VisitWorld, deliver_visit_events, outbox_rows

pytestmark = pytest.mark.integration

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_visit_completed_events.py"
COUNTS = re.compile(
    r"^mode=(?P<mode>[a-z-]+) candidates=(?P<candidates>\d+) already_queued=(?P<already>\d+) "
    r"written=(?P<written>\d+) skipped=(?P<skipped>\d+)"
    r" stale_queued=(?P<stale_queued>\d+) restarted=(?P<restarted>\d+)$"
)
EXTRA_FIELDS = ("stale_queued", "restarted")


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
                    "CASE WHEN :completed THEN now() - interval '3 minutes' END, "
                    "CASE WHEN :completed THEN now() - interval '2 minutes' END, "
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
    # The original five fields only: the two plan 03-22 fields are read by `_extra_counts`.
    counts = (
        {
            k: (v if k == "mode" else int(v))
            for k, v in match.groupdict().items()
            if k not in EXTRA_FIELDS
        }
        if match
        else {}
    )
    return proc.returncode, proc.stdout, proc.stderr, counts


def _extra_counts(out: str) -> dict[str, int]:
    match = COUNTS.match(out.strip())
    assert match, out
    return {k: int(match.group(k)) for k in EXTRA_FIELDS}


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


# --- plan 03-22: stale queued events are restarted --------------------------------------------


def _backfill_module() -> Any:
    spec = importlib.util.spec_from_file_location("backfill_visit_completed_events", SCRIPT)
    assert spec and spec.loader
    module = sys.modules.get(spec.name)
    if module is None:  # a dataclass in the script resolves its module through sys.modules
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _factory() -> Any:
    return get_session_factory(visit_settings.database_url)


async def _marker_of(visit_id: str) -> Any:
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()


async def _queued_visit(vw: VisitWorld) -> str:
    """A completed unstarted visit whose `visit.completed` event is already queued."""
    visit_id = await _insert_visit(vw, status="completed")
    code, out, err, counts = await _run_script(vw.clinic_id, "--apply")
    assert code == 0 and counts["written"] == 1, (out, err)
    return visit_id


async def test_a_stale_queued_visit_is_counted_stale_and_a_dry_run_restarts_nothing(
    visit_world: VisitWorld, temporal_env: WorkflowEnvironment
) -> None:
    visit_id = await _queued_visit(visit_world)
    module = _backfill_module()

    counts = await module.backfill(
        _factory(),
        apply=False,
        clinic_id=visit_world.clinic_id,
        stale_after_minutes=0,
        client=temporal_env.client,
    )

    assert (counts.candidates, counts.already_queued, counts.written) == (1, 0, 0)
    assert (counts.stale_queued, counts.restarted) == (1, 0)
    assert await _marker_of(visit_id) is None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    with pytest.raises(RPCError) as exc_info:
        await handle.describe()
    assert exc_info.value.status == RPCStatusCode.NOT_FOUND
    # The subprocess dry run reports the same counts line, with the two new fields at the end.
    code, out, err, parsed = await _run_script(
        visit_world.clinic_id, "--stale-after-minutes", "0"
    )
    assert code == 0, err
    assert parsed["mode"] == "dry-run" and parsed["written"] == 0
    assert _extra_counts(out) == {"stale_queued": 1, "restarted": 0}
    assert out.strip().endswith("stale_queued=1 restarted=0")


async def test_apply_restarts_a_stale_queued_visit_once(
    visit_world: VisitWorld, temporal_env: WorkflowEnvironment
) -> None:
    visit_id = await _queued_visit(visit_world)
    module = _backfill_module()

    counts = await module.backfill(
        _factory(),
        apply=True,
        clinic_id=visit_world.clinic_id,
        stale_after_minutes=0,
        client=temporal_env.client,
    )

    assert (counts.stale_queued, counts.restarted, counts.written) == (1, 1, 0)
    assert await _marker_of(visit_id) is not None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    described = await handle.describe()
    assert described.status == WorkflowExecutionStatus.RUNNING  # no worker polls visit-tq

    # A second apply has no candidate left, and the execution is the same run.
    again = await module.backfill(
        _factory(),
        apply=True,
        clinic_id=visit_world.clinic_id,
        stale_after_minutes=0,
        client=temporal_env.client,
    )
    assert (again.candidates, again.stale_queued, again.restarted) == (0, 0, 0)
    assert (await handle.describe()).run_id == described.run_id
    await handle.terminate(reason="test clean-up")


async def test_a_queued_visit_inside_the_stale_window_stays_already_queued(
    visit_world: VisitWorld, temporal_env: WorkflowEnvironment
) -> None:
    visit_id = await _queued_visit(visit_world)
    module = _backfill_module()

    counts = await module.backfill(
        _factory(),
        apply=True,
        clinic_id=visit_world.clinic_id,
        stale_after_minutes=10,  # completed 2 minutes ago
        client=temporal_env.client,
    )

    assert (counts.already_queued, counts.stale_queued, counts.restarted) == (1, 0, 0)
    assert await _marker_of(visit_id) is None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    with pytest.raises(RPCError) as exc_info:
        await handle.describe()
    assert exc_info.value.status == RPCStatusCode.NOT_FOUND


async def test_a_temporal_failure_exits_1_with_the_class_name_only(
    visit_world: VisitWorld,
) -> None:
    visit_id = await _queued_visit(visit_world)

    proc = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            str(SCRIPT),
            "--clinic-id",
            str(visit_world.clinic_id),
            "--stale-after-minutes",
            "0",
            "--apply",
        ],
        capture_output=True,
        text=True,
        cwd=str(SCRIPT.parents[1]),
        check=False,
        env={**os.environ, "TEMPORAL_ADDRESS": "127.0.0.1:1"},
    )

    assert proc.returncode == 1, (proc.stdout, proc.stderr)
    assert proc.stdout == ""
    # Library noise (gRPC, deprecation warnings) may precede it; the failure line is the last.
    assert re.fullmatch(r"backfill failed: \w+", proc.stderr.strip().splitlines()[-1]), proc.stderr
    for leak in (visit_id, "127.0.0.1", "://"):
        assert leak not in proc.stderr
    assert await _marker_of(visit_id) is None
