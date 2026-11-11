---
phase: 03-event-driven-system-observability
plan: 22
subsystem: visit
tags: [reconciler, lost-trigger, dlq, replay, backfill, temporal, ast-guard, gap-closure]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-09 visit outbox + visit-downstream consumer, 03-12 sweeper retirement + backfill, 03-21 shared classifier and the TransientExhausted cap"
provides:
  - "visit_repo.list_lost_triggers and downstream.restart_lost_triggers: the visit-reconciler restarts a completed visit whose marker is NULL after the grace, through the consumer's own ensure_downstream_started"
  - "VisitActivities.reconcile: settle pass first, then the lost-trigger pass with the worker's Temporal client (no client: skipped and logged)"
  - "backfill: stale_queued / restarted counts and --stale-after-minutes (a dead-lettered event has an outbox row too)"
  - "scripts/replay_dlq.py: dry-run-first DLQ replay to dlq.original_topic, never commits offsets, never prints a payload"
  - "harness isolation from the live reconciler: clinic-scoped lost-trigger calls, recorded reconcile(), session leftover guard, AST scoping gate with positive controls"
affects: [03-23, 03-26]
tech-stack:
  added: []
  patterns:
    - "a safety-net pass added to an existing activity body: no workflow, activity name or result shape changes, so no workflow.patched"
    - "harness tests that share a database with a live scanner: every scan scoped to the test's own tenant, the scanner's own entry point only called with its pass functions recorded, a session guard that removes and fails on leftovers"
key-files:
  created:
    - scripts/replay_dlq.py
    - tests/events/test_replay_dlq.py
    - tests/visit/test_harness_isolation.py
  modified:
    - services/visit/repository/visit_repo.py
    - services/visit/downstream.py
    - services/visit/activities/downstream.py
    - services/visit/workflows/reconciler.py
    - services/visit/worker.py
    - scripts/backfill_visit_completed_events.py
    - tests/visit/conftest.py
    - tests/visit/test_visit_reconciler.py
    - tests/visit/test_downstream.py
    - tests/visit/test_backfill.py
    - tests/e2e/test_phase3_visit_downstream.py
key-decisions:
  - "The recovery route is the existing visit-reconciler Schedule (D-03 kept the reconcilers); the retired visit-downstream-sweeper Schedule is not recreated (test_sweeper_schedule_retired_on_live_server stays green)"
  - "The replay producer uses the murmur2_random partitioner (Java-compatible) so a replayed key lands on the partition Debezium chose; the target check, not the header, bounds where a record can go"
  - "The backfill scripts quiet the service modules' INFO logging (importing services.visit.downstream configures it on stdout): stdout stays one counts line, library logs go to stderr at ERROR"
status: complete
requirements-completed: []
completed: 2026-11-11
duration: about 40 min wall time (one image build, two 7 minute regression runs, e2e runs)
actuals:
  tokens: 28000
  tasks: 3
  commits: 3
plan_head_before: 2982bb33075ee14c9de3593f15212dd467a9f628
plan_head_after: 0da83f647a0243399a8bb848a702fdb2086ffb54
---

# Phase 3 Plan 22: Visit Trigger Recovery Summary

A completed visit whose `visit.completed` event was dead-lettered or consumed and dropped is now restarted by the existing `visit-reconciler` Schedule once it is older than `VISIT_RECONCILE_GRACE_MINUTES`, through the same idempotent `ensure_downstream_started` the outbox consumer calls; the backfill restarts a queued-but-never-acted-on visit on demand; and `scripts/replay_dlq.py` re-produces any group's dead letters (TransientExhausted included) to their original topic. The phase is NOT complete (gap plans 03-23..03-26 follow); REQUIREMENTS.md was not edited and no requirement was ticked.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 0790f13 | `list_lost_triggers`, `restart_lost_triggers`, `VisitActivities.reconcile` lost-trigger pass, corrected docstrings, live e2e `test_lost_visit_trigger_is_recovered_by_the_reconciler` |
| 2 | cc6a599 | clinic-scoped lost-trigger tests, rewritten "never recovered" tests, leftover guard, AST scoping gate with positive controls, reconciler docstring |
| 3 | 0da83f6 | backfill `stale_queued` / `restarted` / `--stale-after-minutes`, `scripts/replay_dlq.py`, their tests |

## I9: effect on existing dev data (the reconcile acts at worker start and every 60 s)

| Measurement | Value |
|---|---|
| Go/no-go dry run BEFORE the first rebuild (orchestrator W-5), `.venv/bin/python scripts/backfill_visit_completed_events.py` at 22:27 PKT | `mode=dry-run candidates=0 already_queued=0 written=0 skipped=0` (the script lists every completed visit with a NULL marker regardless of age, so no visit could be restarted) |
| `visit.lost_trigger_restarted` / `visit.lost_trigger_restart_failed` from the start-up pass and the first periodic passes (first rebuilt container, two minutes after start) | 0 / 0 (`visit.lost_triggers scanned=0 restarted=0` at start-up and at the 60 s pass) |
| Post-deploy dry run (new script, same command) | `mode=dry-run candidates=0 already_queued=0 written=0 skipped=0 stale_queued=0 restarted=0` |
| `visit.lost_trigger_restarted` over the first container's whole life | 1: the e2e test's own visit (restarted by the Schedule trigger), 0 failed |
| Second (final) container after the docstring-only rebuild | 4 passes, 0 restarted, 0 failed; dry run again 0 candidates |

No existing dev visit was ever restarted: the dev visit_db held no completed NULL-marker visit.

## Test results

| Command | Result |
|---|---|
| Task 1 e2e `tests/e2e/test_phase3_visit_downstream.py -m e2e -k "lost_visit_trigger or tracer or sweeper_schedule_retired"` (after the first rebuild) | 3 passed, 38.7 s |
| Task 2 verify: consumer stopped; `tests/visit/test_harness_isolation.py tests/visit tests/workflows -x -q`; consumer started (`--wait`, healthy); exit 0 | **405 passed, 11 skipped**, 455 s (11 skips: pre-existing opt-in tests). All replay tests under `tests/workflows` passed. An earlier identical run stopped at `tests/workflows/test_reconciler.py::test_the_lookback_ignores_other_failure_reasons_and_rows_outside_the_window` (`'voided' == 'verified'`): that test uses a 0.36 s lookback window and I had a Semgrep scan running concurrently; it passed twice alone and the whole command passed on the clean rerun (concurrency artefact, not a code fault) |
| Task 3: `tests/visit/test_backfill.py tests/events/test_replay_dlq.py` (consumer stopped around it) | 21 passed (10 backfill, 11 replay) |
| `tests/libs/test_service_isolation.py tests/docs` | 181 passed |
| `tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e` after the second rebuild | **5 passed, 6 skipped**: the six visit-downstream tests skip (not fail) because `book_slot_inside_checkin_window` refuses to build a slot when fewer than 40 minutes remain before clinic-local midnight (the run was at 23:02 PKT). The same file passed 3/3 for the lost-trigger, tracer and sweeper-retired tests earlier (see row 1); the second rebuild changed one docstring only (`workflows/reconciler.py`). The full file, including the three D-03 kill tests, should be rerun after local midnight in plan 03-26 |

No run spanned a laptop sleep. The known unrelated failures (`test_admin_registers_provider_through_gateway`, `tests/identity/test_email_migration.py`) were neither run nor touched.

## Live dry runs (exact output)

- Backfill: `mode=dry-run candidates=0 already_queued=0 written=0 skipped=0 stale_queued=0 restarted=0` (exit 0).
- `replay_dlq.py --group visit-downstream` (default `--limit 100`): 100 lines of metadata (`dlq_partition=0 dlq_offset=N original=smarthealth.visit.events:P:O error_type=NonRetryableEventError event_id=<uuid>`) then `mode=dry-run scanned=100 selected=100 replayed=0 refused=0`. These are the harness's relayed events for visits whose rows the fixtures deleted (the pre-existing 03-12 note); nothing was applied. `--group notification-trigger` and `--group analytics-projection`: `mode=dry-run scanned=6 selected=6 replayed=0 refused=0` each. Only the last line is parsed by the acceptance regex; no key or payload is printed.
- Header NAMES of a live record on `smarthealth.visit.events` (confirms the `id` header): `__debezium.context.connectorLogicalName`, `__debezium.context.taskId`, `__debezium.context.connectorName`, `__debezium.context.runId`, `id`, `eventType`, `traceparent`, `schemaVersion`.

## What changed

**Lost-trigger pass.** `list_lost_triggers(db, *, older_than_minutes, limit, clinic_id=None)` selects `status='completed' AND downstream_started_at IS NULL AND completed_at < now() - grace` (partial index `ix_visits_downstream_pending`). `restart_lost_triggers(session_factory, client, *, grace_minutes, limit, clinic_id=None)` reads ids in one short transaction, then per visit opens its own session, re-reads, skips unless still completed and unmarked and calls `ensure_downstream_started`; it logs `visit.lost_trigger_restarted` (visit id) per restart, `visit.lost_trigger_restart_failed` (visit id + exception class) per failure without stopping the pass, and `visit.lost_triggers` (scanned, restarted). `VisitActivities.reconcile()` runs the settle pass first, then (client present) the lost-trigger pass; no client logs `visit.lost_trigger_pass_skipped`. `VisitReconcilerWorkflow` (class and call byte-identical), every `ACT_*` name, `SweepResult`, `VisitCompletedWorkflow`, `schedules.py` and `sweeper.py` are untouched (`git diff` empty), so no `workflow.patched`; the replay tests pass.

**Backfill.** A candidate whose outbox row exists and whose completion is older than `--stale-after-minutes` (database clock; default 10) is `stale_queued`; `--apply` restarts it through `ensure_downstream_started` (`restarted=N`), connecting to Temporal only when a stale visit exists. The counts line gained `stale_queued=Q restarted=R` at the end.

**DLQ replay.** `scripts/replay_dlq.py` (dry run by default; `--group` or `--dlq-topic`; `--error-type`, `--key`, `--event-id`, `--limit` default 100, repeatable `--allow-topic`, `--apply`). Reads with assigned partitions up to the start-time high watermark and never commits; with `--apply` re-produces key, value bytes and original headers (all `dlq.*` removed) to `dlq.original_topic` only if it is a catalogue topic or allowed (else counted `refused`); idempotent producer, `acks=all`; output is coordinates, sanitised error type, event id and counts. Recovery map per group is in the module docstring (visit-downstream: reconciler pass, backfill or this script; analytics: this script or `services.analytics.rebuild`; notification: this script). WR-06 is unaffected (a replay keeps the original event id) and stays out of scope.

**Harness isolation (W5, W6, W-1).** Every lost-trigger call in `tests/visit` passes `clinic_id`; `reconcile()` is called only with both pass functions replaced by recorders; no test ages a NULL-marker visit beyond 120 s (the backfill fixtures place completion 2 minutes ago); `_visit_world` registers its provider ids and a session-scoped autouse guard removes and fails on surviving NULL-marker harness visits. The guard's decision (`guard_null_marker_leftovers`) takes injected count and clean-up callables and is unit-tested for the failing path; a positive-control test proves `null_marker_leftovers` sees such a row before teardown.

## Deviations from Plan

### Auto-fixed / orchestrator-directed

1. **[Orchestrator W-1] New test file `tests/visit/test_harness_isolation.py`** (not in the plan's file list). It holds the AST scoping gate and the guard tests. The gate fails on (a) any `restart_lost_triggers` / `list_lost_triggers` call without a `clinic_id` keyword, (b) any `.reconcile()` call in a test function that does not monkeypatch both `reconcile_unsettled` and `restart_lost_triggers` (string names), (c) a `VisitActivities(...)` built with a non-None Temporal client under the same condition. Five positive controls (unscoped restart, unscoped list, unrecorded reconcile, half-recorded reconcile, activities with a client) each must report exactly one finding; a compliant sample reports none. It runs in Task 2's `<automated>` verify command (explicitly named, and implicitly through `tests/visit`). Known scope limit: `ActivityEnvironment().run(acts.reconcile_unsettled)` in the pre-existing `test_the_reconcile_activity_runs_a_pass_and_returns_the_count` is not matched (the activity is referenced, not `.reconcile()`-called); it uses `client=None`, so only the unchanged settle pass runs, as the plan states for pre-existing tests.
2. **[Orchestrator W-3] Evidence binding.** The verify commands run the measuring step themselves: the e2e test reads the Schedule trigger, offsets, markers and logs within its own window (log lookups use `--since` the test start), the Task 2/3 verifies run pytest and the dry runs in the same command, and the dry-run counts quoted above come from commands run in this execution (timestamps in the transcript), not from a file.
3. **[Orchestrator W-5] Go/no-go before the live reconcile.** Dry run before the first `visit-worker` rebuild returned `candidates=0`; the rebuild proceeded.
4. **[Rule 1 - Bug] Backfill stdout contained library log lines.** Importing `services.visit.downstream` (needed for `ensure_downstream_started`) configures INFO logging on stdout, so the registry's httpx line and `visit.downstream_started` (visit id) would have appeared next to the counts line and broken "counts only". Fixed with `_quiet_logging()` (libraries to ERROR on stderr); `test_output_is_counts_only` and the original assertions pass unchanged.
5. **[Design] Tests check the stale and failure paths in process / by subprocess as the plan asks, but the Temporal-failure test asserts the LAST stderr line** is `backfill failed: <ClassName>` (gRPC and deprecation noise precede it); stdout is empty and no address or id leaks.
6. **[Design] The visit-worker was rebuilt twice** (after Task 1, and once more after Task 2's docstring change so the live image matches the commits); always `--no-deps`, only that service.
7. **[Design] `restart_lost_triggers` counts a visit as restarted only after `ensure_downstream_started` returns,** and logs `visit.lost_trigger_restarted` after it, so the log line cannot precede a failure.

### Not done

- The post-second-rebuild e2e run of the visit-downstream file was skipped by the environment (clinic-midnight rule, see Test results); the lost-trigger e2e proof predates a docstring-only rebuild.

No auth gates, no checkpoints, no fix-attempt limits reached.

## Live stack

- Recreated: `visit-worker` twice (`up -d --build --no-deps`). `visit-consumer` was stopped around the `tests/visit` runs (three times) and started again each time with `start --wait --wait-timeout 180` (healthy); the e2e test stops and restores it itself.
- Moved one consumer-group offset as designed: the e2e test set `visit-downstream`'s committed offset on one partition from the test event's offset to offset+1, only after asserting it equalled that offset (T-3-70).
- Gateway left RAISED at 1000 / 100000 (plan 03-21's setting; plan 03-26 restores 20 / 300).
- No `down -v`, no volume removed; `tjo-api Up 35 hours (unhealthy)` and `tjo-postgres Up 35 hours (healthy)` identical before and after (it was already unhealthy). The retired sweeper Schedule was not recreated (asserted by the e2e test, which passed in the first run). No workflow or activity name or shape changed. No package installed, no `.env*` / `infra/secrets/*` read.
- End state: `scripts/compose_healthcheck.sh` prints OK; replication slots `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all `active=true`.
- Live data left behind: relayed harness events in Kafka (some dead-lettered as unknown visits, as before); no visit rows.

## Semgrep

`semgrep --config p/python --metrics=off services/visit scripts/backfill_visit_completed_events.py scripts/replay_dlq.py` (151 rules, 33 files, run after the files were committed, because Semgrep only scans git-tracked files): 0 findings. `--config p/security-audit --metrics=off` (80 rules, 36 files): 0 findings. `--config auto` (metrics on): 1 finding, `dockerfile.security.missing-user` in `services/visit/Dockerfile`, pre-existing and not touched by this plan. Status: passed for this change set.

## Known Stubs

None.

## Threat Flags

None new. T-3-67 (selection + re-read + `start_once` + conditional marker; tests prove one execution across passes, deliveries and backfill), T-3-68 (log lines, counts, live e2e), T-3-69 and T-3-73 (counts only; a test asserts the payload marker, keys, values and traceparent never reach the output), T-3-70 (offset only moved when it equals the event's offset), T-3-71 (clinic scoping, recorded reconcile, guard, AST gate) and T-3-72 (dry run default, catalogue/allow-list target check, limit, idempotent producer, no group commits) are mitigated as planned. Note for later: the live `visit-downstream` DLQ holds 100+ `NonRetryableEventError` records for deleted harness visits; a real `--apply` of that DLQ is not advised.

## Self-Check: PASSED

- Created files exist: `scripts/replay_dlq.py`, `tests/events/test_replay_dlq.py`, `tests/visit/test_harness_isolation.py`.
- Commits in `git log`: 0790f13, cc6a599, 0da83f6 (`git rev-list --count 2982bb3..HEAD` = 3 before the docs commit).
- Acceptance greps: `def list_lost_triggers` 1, `def restart_lost_triggers` 1, `left_to_the_outbox_consumer` 0, `_age_completion(visit["id"], 600)` 0, `ensure_downstream_started` in the backfill script >= 1, `enable.idempotence` in `scripts/replay_dlq.py` >= 1, AST clinic_id check prints `[]`; `git diff --stat HEAD -- services/visit/workflows/visit_completed.py services/visit/workflows/sweeper.py services/visit/domain/contracts.py services/visit/schedules.py` empty; `tests/docs/test_phase3_docs.py` 73 passed; `visit-consumer` healthy.
