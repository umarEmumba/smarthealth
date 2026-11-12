---
phase: 03-event-driven-system-observability
plan: 12
type: execute
wave: 6
depends_on: ["03-09", "03-11"]
files_modified:
  - services/visit/worker.py
  - services/visit/schedules.py
  - services/visit/downstream.py
  - services/visit/activities/downstream.py
  - services/visit/workflows/sweeper.py
  - services/visit/config.py
  - services/visit/repository/visit_repo.py
  - scripts/backfill_visit_completed_events.py
  - tests/visit/test_downstream.py
  - tests/visit/test_visit_reconciler.py
  - tests/visit/test_backfill.py
  - tests/workflows/test_visit_schedules.py
  - tests/e2e/test_phase3_visit_downstream.py
autonomous: true
requirements: [EVENT-02, EVENT-03]

estimate:
  tokens: 85000
  raw_tokens: 85000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "The visit-downstream-sweeper Temporal Schedule is deleted from the live server by the visit worker on start (idempotent, not-found ignored), and no new code path recreates it (D-03, research F6)"
    - "The worker's run-on-start sweep and the marker's sweeping role are gone; visit completion downstream work is triggered only by the outbox consumer (D-03)"
    - "VisitDownstreamSweeperWorkflow and its activity name stay registered as a no-op tombstone so an in-flight run or recorded history can still complete; nothing else is renamed (replay safety)"
    - "The visit reconciler, hold reaper, pending-starter and scheduling reconciler keep running; the paused scheduling-schedule-change-poller stays paused"
    - "Completed visits that never started downstream work before the cutover get their visit.completed event through an idempotent backfill script (research Runtime State Inventory)"
  artifacts:
    - path: services/visit/schedules.py
      provides: "ensure_visit_schedules (reconciler only) and retire_sweeper_schedule"
      contains: "retire_sweeper_schedule"
    - path: scripts/backfill_visit_completed_events.py
      provides: "dry-run by default; --apply enqueues visit.completed for completed visits with no downstream start"
      contains: "emit_visit_completed"
    - path: tests/visit/test_backfill.py
      provides: "backfill writes one event per unstarted visit and is idempotent"
  key_links:
    - from: services/visit/worker.py
      to: services/visit/schedules.py
      via: "main() calls ensure_visit_schedules then retire_sweeper_schedule before running the worker"
      pattern: "retire_sweeper_schedule"
    - from: scripts/backfill_visit_completed_events.py
      to: services/visit/events.py
      via: "same emit function and uuid5 discriminator as the live transition, so a backfilled event equals the one the transition would have written"
      pattern: "emit_visit_completed"
---

<objective>
Retire the visit downstream sweeper (second half of D-03) now that plan 03-11 Task 2 proved the outbox trigger with kill-between-commit-and-publish tests on a quiet stack: delete the live Temporal Schedule, remove the start-up sweep and the sweep code path, keep the sweeper workflow registered as a harmless tombstone, and backfill the cutover set.

Purpose: D-03 is user-confirmed and costly; leaving the Schedule running would keep a second trigger alive and make the docs untrue. Removing an activity or workflow name that histories reference would break replay, so the tombstone stays.

Output: schedule retirement on worker start, simplified worker and downstream module, tombstone sweeper (documented in `workflows/sweeper.py`), removed sweeper settings, rewritten tests, the backfill script and its run on the live data. The remaining docstrings that mention the sweeper (`services/visit/app.py`, `services/visit/workflows/__init__.py`, `services/visit/workflows/reconciler.py`) are corrected with the other docs in plan 03-18.

Precondition: the plan 03-11 SUMMARY states the D-03 gate passed (three visit scenarios green). If it does not, stop: the sweeper must not be retired.

Decisions honoured: D-03 and D-03 detail (delete the Schedule explicitly because `ensure_visit_schedules` leaves existing Schedules alone; keep the workflow class registered until that ships; backfill rather than only document the cutover set; the other interim mechanisms stay as safety nets and the docs in plan 03-18 record why), research Pattern 5 steps 3-4 and Pitfall 10. User decision 2026-11-10: the `scheduling-schedule-change-poller` stays paused; this plan never touches it.

Live stack rules: rebuild and recreate only `visit-worker` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-worker` (it is the only process whose behaviour changes: the Schedule deletion at start and the tombstone activity). `visit` and `visit-consumer` are not recreated here: nothing they run changes behaviour (the removed sweep function, sweeper settings and tombstone are used only by the worker); the phase gate (plan 03-20) recreates every application container from the committed code. No migration in this plan. Never touch `tjo-*` containers (record status before and after). The backfill runs from the host against the live visit_db (`.venv/bin/python scripts/backfill_visit_completed_events.py`, then `--apply`). Wave 6 coupling: plan 03-13 runs alongside, recreates the gateway and creates the analytics containers; neither plan touches the other's services; a `visit-worker` restart only delays queued Temporal tasks (they wait on the task queue), and 03-13's e2e does not depend on `visit-worker`.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-09-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-11-SUMMARY.md
@services/visit/worker.py
@services/visit/schedules.py
@services/visit/downstream.py
@services/visit/activities/downstream.py
@services/visit/workflows/sweeper.py
@services/visit/config.py
@tests/visit/test_downstream.py
@tests/workflows/test_visit_schedules.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `retire_sweeper_schedule(client, *, id_prefix="visit") -> bool` | function | deletes `{prefix}-downstream-sweeper`; True when deleted, False when absent |
| `ensure_visit_schedules` | function | creates only `{prefix}-reconciler` now |
| worker start-up | code | `ensure_visit_schedules` then `retire_sweeper_schedule`; no start-up sweep |
| sweeper tombstone | code | `VisitDownstreamSweeperWorkflow` and the `ACT_SWEEP` activity name stay registered; the activity returns `SweepResult` with zero processed and logs `visit.sweeper_retired` |
| removed | code | the sweep function in `downstream.py`, `sweeper_interval_seconds` / `sweeper_grace_seconds` settings (`VISIT_SWEEPER_INTERVAL_SECONDS`, `VISIT_SWEEPER_GRACE_SECONDS` become dead) |
| `list_completed_without_downstream(db, *, limit)` | repository | renamed from the sweep's query; used by the backfill (keeps `ix_visits_downstream_pending` useful) |
| `scripts/backfill_visit_completed_events.py` | script | dry run prints the count; `--apply` writes one `visit.completed` per visit in its own transaction; prints counts only |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the rebuilt visit worker deletes the live sweeper Schedule and leaves every other Schedule as it was</name>
  <reversibility rating="costly">D-03: removes a safety net that Phase 2 verification relied on; the replacement (outbox consumer) was proven in plan 03-11 Task 2. Recreating the Schedule later needs the old definition restored.</reversibility>
  <files>services/visit/schedules.py, services/visit/worker.py, tests/workflows/test_visit_schedules.py, tests/e2e/test_phase3_visit_downstream.py</files>
  <behavior>
    - `retire_sweeper_schedule(client)` deletes an existing `visit-downstream-sweeper` Schedule and returns True; a second call returns False without raising
    - `ensure_visit_schedules(client)` on an empty server creates only `visit-reconciler`
    - Live: after the rebuild, describing `visit-downstream-sweeper` raises NOT_FOUND; `visit-reconciler` exists and is not paused; `scheduling-schedule-change-poller` exists and is still paused; no `VisitDownstreamSweeperWorkflow` execution is running (an in-flight run is allowed 60 s to finish)
  </behavior>
  <read_first>
    - services/visit/schedules.py (module docstring: "An existing Schedule is left as it is"; `_definitions`, `ensure_visit_schedules`)
    - services/visit/worker.py (`main` start-up order, the start-up sweep near lines 138-146, `WORKFLOWS` list)
    - services/scheduling/schedules.py (how the poller Schedule is defined, to know its id; do not modify it)
    - tests/workflows/test_visit_schedules.py (time-skipping Schedule tests to adapt)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Runtime State Inventory, Pitfall 10)
  </read_first>
  <action>
    schedules.py: drop the sweeper entry from `_definitions`; add `retire_sweeper_schedule(client, *, id_prefix="visit")` that calls `client.get_schedule_handle(f"{id_prefix}-downstream-sweeper").delete()` and treats an RPC NOT_FOUND as "already gone" (False); update the module docstring (the sweeper is retired; the function exists because an existing Schedule survives code removal). worker.py: after `ensure_visit_schedules`, call `retire_sweeper_schedule` (log `visit.sweeper_schedule_retired` with the boolean); a failure exits 1 like the existing schedule set-up; delete the start-up sweep block and its import; keep `VisitDownstreamSweeperWorkflow` in `WORKFLOWS`.

    Tests: adapt `tests/workflows/test_visit_schedules.py` (reconciler-only creation, retirement idempotency on a time-skipping server where the old Schedule was created first with the previous definition inline in the test). Add an e2e test `test_sweeper_schedule_retired_on_live_server` to `tests/e2e/test_phase3_visit_downstream.py` for the live behaviors (read-only: it never deletes or pauses anything itself).

    Rebuild and recreate `visit-worker` only, with `--no-deps` (live stack rules), then run the e2e test.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/workflows/test_visit_schedules.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k sweeper_schedule_retired -x -q</automated>
    <fails_when>Either pytest run exits non-zero, the sweeper Schedule still exists on the live server, the reconciler Schedule is missing, or the poller Schedule is no longer paused.</fails_when>
  </verify>
  <acceptance_criteria>
    - the e2e test passes after the rebuild
    - `docker compose -f infra/docker-compose.yml logs visit-worker` contains `visit.sweeper_schedule_retired`
    - `grep -c "retire_sweeper_schedule" services/visit/worker.py` is at least 1
    - tjo-* status lines show no restart
  </acceptance_criteria>
  <done>The live sweeper Schedule is gone, the reconciler remains, and the paused poller is untouched.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Remove the sweep code path, keep a tombstone and rewrite the sweep tests</name>
  <files>services/visit/downstream.py, services/visit/activities/downstream.py, services/visit/workflows/sweeper.py, services/visit/config.py, services/visit/repository/visit_repo.py, tests/visit/test_downstream.py, tests/visit/test_visit_reconciler.py</files>
  <behavior>
    - `VisitDownstreamSweeperWorkflow` still runs under a test worker: it executes its one activity, which returns zero processed and starts nothing
    - `VisitSettings` has no sweeper fields; constructing it with `VISIT_SWEEPER_INTERVAL_SECONDS` set is accepted and ignored (extra env is ignored by the settings config)
    - The former sweep tests are replaced: two concurrent deliveries of the same visit.completed start one run and one billing finalization; a completed visit whose marker is NULL is NOT touched by any worker start-up path
    - Reconciler test docstrings say never-started visits are handled by the outbox consumer; reconciler behaviour is unchanged
  </behavior>
  <read_first>
    - services/visit/downstream.py (module docstring, the sweep function and `SWEEP_LIMIT`)
    - services/visit/activities/downstream.py (`VisitActivities` constructor takes the sweeper grace; the `ACT_SWEEP` activity near line 272)
    - services/visit/workflows/sweeper.py (docstring and registration)
    - services/visit/repository/visit_repo.py (`list_downstream_unstarted`, `list_unsettled` requiring a non-NULL marker)
    - tests/visit/test_downstream.py (sweep tests near lines 210, 302, 361), tests/visit/test_visit_reconciler.py
    - tests/visit/conftest.py (`deliver_visit_events` from plan 03-09)
  </read_first>
  <action>
    Delete the sweep function and its constant from `downstream.py` and fix its module docstring (trigger = outbox event consumed by the visit-downstream consumer; `ensure_downstream_started` stays the shared start). In `activities/downstream.py`, drop the sweeper-grace constructor parameter and make the `ACT_SWEEP` activity a tombstone that logs `visit.sweeper_retired` and returns zero processed (same name, same result type). In `workflows/sweeper.py`, document the tombstone: kept registered so an in-flight run or recorded history completes; safe to remove once `list_workflows("WorkflowType='VisitDownstreamSweeperWorkflow'")` is empty on every environment (record that query in the docstring). Remove the two sweeper fields from `VisitSettings`. Rename `list_downstream_unstarted` to `list_completed_without_downstream` (no behaviour change; Task 3 uses it). Rewrite the three sweep tests as described and update reconciler test docstrings; do not weaken any assertion about single runs or single finalization. The package, reconciler and app docstrings are corrected in plan 03-18 (docs closeout).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit tests/workflows -x -q</automated>
    <fails_when>pytest exits non-zero, the tombstone workflow fails, a replay test fails, or a test still references removed settings or functions.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit tests/workflows -q` exits 0
    - `grep -c "sweeper_grace_seconds" services/visit/config.py` is 0
    - `grep -c "VisitDownstreamSweeperWorkflow" services/visit/worker.py` is at least 1 (tombstone still registered)
  </acceptance_criteria>
  <done>The sweep code path is gone, a tombstone keeps replay safe, and tests describe the outbox-driven design.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Backfill the cutover set of completed visits with no downstream start</name>
  <files>scripts/backfill_visit_completed_events.py, tests/visit/test_backfill.py</files>
  <behavior>
    - A completed visit with a NULL marker and no outbox row (inserted directly as pre-cutover data) gets exactly one `visit.completed` outbox row from `--apply`; the event id equals `event_id_for(visit_id, "visit.completed", "completed")`
    - A second `--apply` writes nothing new; a visit with a non-NULL marker or a non-completed status is never selected
    - Dry run (no flag) writes nothing and prints the count of candidates
    - After the backfill, delivering the visit's events with `deliver_visit_events` starts downstream work once
    - Output contains counts only (no ids, no URLs with credentials)
  </behavior>
  <read_first>
    - services/visit/events.py (`emit_visit_completed`, discriminator `completed`), services/visit/repository/visit_repo.py (`list_completed_without_downstream`)
    - scripts/loadtest_seed.py (host script conventions: argparse, settings import, exit codes)
    - tests/visit/conftest.py
  </read_first>
  <action>
    Script: argparse `--apply`, `--limit` (default 1000); uses `services.visit.config.settings.database_url` (host `.env`/defaults), warms the registry, selects candidates with `list_completed_without_downstream`, and for each opens its own transaction, reads the visit row's event fields, calls `emit_visit_completed`, commits. Idempotent through the uuid5 primary key. Prints `candidates=N written=M` and exits 0; exits 1 on any database or registry error with the exception class name.

    Test (marker `integration`): the behaviors above against the live visit_db with a unique clinic.

    Run on the live data from the host: dry run, then `--apply`; record both outputs in the SUMMARY (expected: dev data only).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit/test_backfill.py -x -q && .venv/bin/python scripts/backfill_visit_completed_events.py</automated>
    <fails_when>pytest exits non-zero, a second apply writes rows, the dry run writes rows, or the script exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit/test_backfill.py -q` exits 0 with at least 4 passed
    - `.venv/bin/python scripts/backfill_visit_completed_events.py --apply` exits 0 and a second run prints `written=0`
    - the SUMMARY records the dry-run and apply outputs
  </acceptance_criteria>
  <done>No completed visit is left without a downstream trigger after the cutover.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| visit worker -> Temporal server (Schedules) | the worker deletes a live recurring registration at start-up |
| host script -> visit_db outbox | a one-shot job writes events for historic rows |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-38 | Tampering | deleting the wrong Schedule | medium | mitigate | Exact id `visit-downstream-sweeper` only; tests assert the reconciler and the paused poller are unchanged afterwards |
| T-3-39 | Repudiation | visits silently left without downstream work | medium | mitigate | Backfill with uuid5 ids (same as the live path), idempotent re-runs, counts recorded in the SUMMARY; reconciler continues for started-but-unsettled visits |
| T-3-40 | Denial of Service | replay failure after removing a workflow | low | mitigate | Workflow and activity names kept as a tombstone; replay suite runs in verification |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/visit tests/workflows -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/visit scripts/backfill_visit_completed_events.py`; record status in the SUMMARY
</verification>

<success_criteria>
- The sweeper Schedule is gone from the live server, the sweep path is removed, a tombstone keeps replay safe
- The cutover set is backfilled; other safety nets and the paused poller are unchanged
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-12-SUMMARY.md` when done
</output>
