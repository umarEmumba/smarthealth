---
phase: 03-event-driven-system-observability
plan: 12
subsystem: visit
tags: [sweeper-retirement, d-03, temporal-schedule, tombstone, backfill, outbox]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-09 visit outbox and visit-downstream consumer (the only downstream trigger), 03-11 D-03 kill-between-commit-and-publish gate (PASSED)"
provides:
  - "retire_sweeper_schedule(client): the visit worker deletes the live visit-downstream-sweeper Schedule on every start (idempotent)"
  - "No start-up sweep, no sweep function, no sweeper settings; VisitDownstreamSweeperWorkflow and the visit.sweep_unstarted activity name stay registered as a no-op tombstone"
  - "list_completed_without_downstream(db, *, limit, clinic_id=None) and scripts/backfill_visit_completed_events.py (dry run by default, --apply, --limit, --clinic-id)"
affects: [03-13, 03-17, 03-18, 03-20]
tech-stack:
  added: []
  patterns:
    - "Delete a recurring registration explicitly when its creating code is removed (an existing Schedule survives code removal); keep the workflow type and activity name as a tombstone so histories still replay"
    - "A host backfill reuses the live emit function and the uuid5 event id, checks for an existing outbox row first, and reports counts only"
key-files:
  created:
    - scripts/backfill_visit_completed_events.py
    - tests/visit/test_backfill.py
  modified:
    - services/visit/schedules.py
    - services/visit/worker.py
    - services/visit/downstream.py
    - services/visit/activities/downstream.py
    - services/visit/workflows/sweeper.py
    - services/visit/config.py
    - services/visit/repository/visit_repo.py
    - infra/docker-compose.yml
    - tests/workflows/test_visit_schedules.py
    - tests/visit/test_downstream.py
    - tests/visit/test_visit_reconciler.py
    - tests/e2e/test_phase3_visit_downstream.py
key-decisions:
  - "The VisitActivities constructor keeps its positional `client` argument (now unused) so the worker, the harness fixture and the tests did not have to change; only the sweeper-grace keyword was dropped"
  - "The backfill gained an optional --clinic-id scope (and the repository function an optional clinic_id): the harness tests run the real script against the live visit_db and must never write events for other rows"
  - "The backfill counts a candidate whose event already exists as already_queued and never writes it again (a visit committed after the cutover and not consumed yet is a candidate too); output is one line of counts"
requirements-completed: []
status: complete
duration: about 1h wall time (two image builds about 70 s each, a 7 minute visit/workflows regression, a 2 minute e2e run)
completed: 2026-11-11
actuals:
  tokens: 17500
  tasks: 3
  commits: 3
plan_head_before: 53e360188048ce0ae7d19797138b1bfaba4de417
plan_head_after: 5efd4eb069237f2c1d4dd90dc6f7891f98797896
---

# Phase 3 Plan 12: Visit Sweeper Retirement Summary

**The visit worker now deletes the `visit-downstream-sweeper` Temporal Schedule on start (done on the live server), the start-up sweep, the sweep function and the two sweeper settings are gone, the sweeper workflow and activity name remain as a no-op tombstone so histories still replay, and an idempotent backfill script exists for completed visits that never started downstream work. On the live databases the backfill dry run finds 0 candidates.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (EVENT-02, EVENT-03 stay for the phase gate).

Precondition checked: the plan 03-11 SUMMARY states the D-03 gate passed (three visit scenarios green: API kill after commit, consumer outage, Connect outage; each proven by the visit-consumer's own `outcome=started` log line).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | e6a6e58 | `retire_sweeper_schedule`, reconciler-only `ensure_visit_schedules`, worker start-up without the sweep, Schedule tests, live e2e `test_sweeper_schedule_retired_on_live_server` |
| 2 | db17d38 | sweep function and settings removed, tombstone activity and workflow, `list_completed_without_downstream`, rewritten sweep tests, compose comment |
| 3 | 5efd4eb | `scripts/backfill_visit_completed_events.py`, `tests/visit/test_backfill.py` (6 tests) |

## Results

### Task 1: tracer (live)

- `tests/workflows/test_visit_schedules.py`: 5 passed (reconciler-only creation plus second-call no-op; retirement of a Schedule created inline with the Phase 2 definition returns True, the reconciler next to it is untouched and not paused, a second call returns False; retirement on a server that never had it returns False; the tombstone workflow runs; the reconciler workflow runs).
- The e2e test was run red first (the live sweeper Schedule still existed), then `visit-worker` was rebuilt and recreated (`up -d --build --no-deps visit-worker`). Worker log: `schedule.retired` for `visit-downstream-sweeper`, `visit.sweeper_schedule_retired deleted=true`, `worker.started`. After the rebuild the e2e test passed.
- Live Schedules after the rebuild: `visit-reconciler`, `scheduling-hold-reaper`, `scheduling-pending-starter`, `scheduling-reconciler` all present and not paused; `scheduling-schedule-change-poller` present and still paused; `visit-downstream-sweeper` gone; 0 running `VisitDownstreamSweeperWorkflow` executions.

### Task 2: sweep path removed

- Removed: `sweep_unstarted` and `SWEEP_LIMIT` (downstream.py), the grace constructor parameter, `sweeper_interval_seconds` and `sweeper_grace_seconds` (config; `VISIT_SWEEPER_*` env variables are now ignored through the settings' `extra="ignore"`, asserted by a test), the worker's start-up sweep.
- Tombstone: `VisitDownstreamSweeperWorkflow` still in `WORKFLOWS`; its activity `visit.sweep_unstarted` logs `visit.sweeper_retired` and returns `SweepResult(processed=0)`. The removal condition and the `list_workflows("WorkflowType='VisitDownstreamSweeperWorkflow'")` query are recorded in `workflows/sweeper.py`.
- `list_downstream_unstarted(older_than_seconds, limit)` is now `list_completed_without_downstream(limit, clinic_id=None)` (the age filter was the sweep's grace and is gone; same partial index).
- Tests rewritten (no assertion weakened): `test_null_marker_visit_is_not_touched_by_any_worker_start_up_path` (an old NULL-marker completed visit survives the reconcile pass and the tombstone untouched, no workflow exists, billing still `verified`; the event delivery then starts it as the positive control), `test_sweeper_workflow_is_a_no_op_tombstone_that_still_runs` (real worker, real activity), `test_no_sweep_code_path_or_settings_remain` (settings, module attributes, `VISIT_SWEEPER_*` env accepted and ignored), `test_concurrent_deliveries_start_one_run_one_finalization` (two racing deliveries: one run id, one finalization row, one audit entry). The grace-window test was removed with the grace. Reconciler test docstrings now say never-started visits belong to the outbox consumer.
- Verification: `pytest tests/visit tests/workflows tests/libs/test_service_isolation.py`: **389 passed, 11 skipped** (the skips are pre-existing opt-in tests), 6 min 53 s, run before `test_backfill.py` existed; `tests/visit/test_backfill.py` then run on its own: 6 passed. All replay tests under `tests/workflows` passed.

### Task 3: backfill

- `tests/visit/test_backfill.py`: **6 passed**: the unstarted completed visit gets exactly one `visit.completed` row whose id equals `event_id_for(visit_id, "visit.completed", "completed")`; a second `--apply` writes nothing (`candidates=1 already_queued=1 written=0`); a visit with a set marker, an in-progress and a checked-in visit are never selected; the dry run writes nothing and reports the count; delivering the backfilled event with `deliver_visit_events` starts one downstream execution (same run id after a redelivery) and sets the marker; output is one line of counts with no ids, URLs or credentials. The tests run the real script as a subprocess against visit_db scoped by `--clinic-id` to a unique clinic.
- **Live dry run (the only run against live data), exact output:** `mode=dry-run candidates=0 already_queued=0 written=0 skipped=0` (exit 0). A read-only count of live visit_db at the time: 17 completed visits, all with `downstream_started_at` set; 14 outbox rows. Nothing was applied.
- **Command for the user to run later (dry run, then apply only if the count is meaningful):**
  `.venv/bin/python scripts/backfill_visit_completed_events.py` then `.venv/bin/python scripts/backfill_visit_completed_events.py --apply` (optional `--clinic-id <uuid>` and `--limit N`). A real apply starts one downstream workflow and one analytics projection update per written event, so do not run it while junk test visits are in visit_db.

### Plan verification

- `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e`: **10 passed** (5 + 5, 130 s) against the rebuilt stack, including the three D-03 kill/outage tests and the new Schedule test.
- The full non-e2e suite was not run (the orchestrator waived it); the named plan tests, `tests/visit`, `tests/workflows` and `tests/libs/test_service_isolation.py` were.
- Semgrep (`semgrep --config auto services/visit scripts/backfill_visit_completed_events.py`, 296 rules, 35 files): 1 finding, `dockerfile.security.missing-user` in `services/visit/Dockerfile` (a file this plan did not change; pre-existing, not fixed, out of scope). No findings in the changed Python code. Semgrep status: Passed for this change set.

## Deviations from Plan

1. **[Rule 2, safety] `--clinic-id` scope on the backfill (and `clinic_id` on the repository function).** The plan's test runs the script "against the live visit_db with a unique clinic", but an unscoped `--apply` would have written events for every unstarted visit in the live database. The scope makes the test touch only its own rows and is a useful operator option.
2. **[Plan wording] The repository function has no age filter.** The plan said "renamed ... no behaviour change", but its stated signature `list_completed_without_downstream(db, *, limit)` has no grace argument (the grace belonged to the sweep), so the age condition was dropped as the signature implies.
3. **[Orchestrator instruction over plan] The backfill was not executed against live data.** The plan said dry run then `--apply` on the live data; per the orchestrator only the dry run was run (it found 0 candidates anyway).
4. **[Rule 3] `visit-worker` was rebuilt twice** (after Task 1 and after Task 2, so the live container runs the committed tombstone code), always with `--no-deps` and only that service. The plan named a single rebuild in Task 1.
5. **[Harness race] `visit-consumer` was stopped around the harness runs.** An early run of `tests/visit/test_downstream.py` failed once in `test_complete_does_not_wait_for_downstream_work` (a test I had not changed) because the live consumer processed the harness visit's real outbox row; per the orchestrator I stopped it with `docker compose stop visit-consumer`, ran the suites, and **started it again** before the e2e run (healthy). A rerun of the same test passed once before the stop; the other runs happened with it stopped.
6. **[Housekeeping] `infra/docker-compose.yml`** (not in the plan's file list): only the comment above `visit-worker` that said it registers the sweeper Schedule was corrected; no configuration changed.
7. The e2e test cannot assert the `visit.sweeper_schedule_retired` log line (the Temporal state is the durable fact); the log line was checked by hand (see Live stack).

## Live stack

- Recreated: `visit-worker` twice (`--no-deps`). Stopped and started: `visit-consumer` (restored, healthy). Deleted on the Temporal server: the `visit-downstream-sweeper` Schedule only (D-03, approved). No other Schedule was created, changed or paused; `scheduling-schedule-change-poller` was never touched and is still paused.
- `docker compose logs visit-worker`: first start `schedule.retired` and `visit.sweeper_schedule_retired deleted=true`; second start `schedule.already_retired` and `deleted=false` (idempotent).
- End state: `scripts/compose_healthcheck.sh` OK; `tjo-api Up 27 hours (unhealthy)` and `tjo-postgres Up 27 hours (healthy)` before and after (`tjo-api` was already unhealthy; no restart). Never run: `down -v`, volume operations, `visit` or `visit-consumer` image rebuilds.
- Live data left behind: the throwaway visits, outbox rows and Kafka copies from the harness runs (the visit rows were cleaned by the fixtures; relayed events stay in Kafka, and the consumer may dead-letter them as unknown visits, as with earlier plans' harness runs).
- No `.env*` or `infra/secrets/*` content was read or printed.

## Known Stubs

None.

## Threat Flags

None new. T-3-38 (wrong Schedule): the exact id only; the test asserts the reconciler Schedule survives and the live e2e asserts the other Schedules exist and the poller is still paused. T-3-39 (visits without downstream work): idempotent backfill with the live event id, dry run reports 0 candidates on live data, the visit reconciler keeps running for started-but-unsettled visits. T-3-40 (replay): the tombstone workflow and activity name are registered; all `tests/workflows` replay tests pass. T-3-SC: no package installed.

## Self-Check: PASSED

- FOUND: scripts/backfill_visit_completed_events.py, tests/visit/test_backfill.py, services/visit/schedules.py (`retire_sweeper_schedule`), services/visit/workflows/sweeper.py.
- FOUND commits e6a6e58, db17d38, 5efd4eb on gsd/phase-02-scheduling-service-workflow (`git rev-list --count 53e3601..HEAD` = 3 before the docs commit).
- `grep -c sweeper_grace_seconds services/visit/config.py` = 0; `grep -c VisitDownstreamSweeperWorkflow services/visit/worker.py` >= 1; `grep -c retire_sweeper_schedule services/visit/worker.py` >= 1.
