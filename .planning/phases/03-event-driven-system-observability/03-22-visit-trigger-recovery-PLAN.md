---
phase: 03-event-driven-system-observability
plan: 22
type: execute
wave: 15
depends_on: ["03-21"]
files_modified:
  - services/visit/repository/visit_repo.py
  - services/visit/downstream.py
  - services/visit/activities/downstream.py
  - services/visit/workflows/reconciler.py
  - scripts/backfill_visit_completed_events.py
  - tests/visit/test_visit_reconciler.py
  - tests/visit/test_downstream.py
  - tests/visit/test_backfill.py
  - tests/e2e/test_phase3_visit_downstream.py
autonomous: true
gap_closure: true
requirements: [EVENT-02, EVENT-03, EVENT-05]

estimate:
  tokens: 95000
  raw_tokens: 95000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 (CR-01 consequence, D-03 replacement): a visit that is completed with downstream_started_at IS NULL and completed_at older than the reconcile grace is found by the existing visit-reconciler Schedule and its downstream workflow is started with ensure_downstream_started, without the retired visit-downstream-sweeper Schedule"
    - "On the live stack, a visit.completed that was consumed and dropped (the visit-downstream group's offset moved past it, as after a dead-letter) leaves the visit unstarted, and the live reconciler then starts exactly one visit-completed-<visit id> execution, sets the marker, and billing is finalized and the appointment completed; the visit-worker logs visit.lost_trigger_restarted for that visit id and the consumer never started it"
    - "Recovery is idempotent: a second reconcile pass, a racing consumer delivery or a backfill restart attaches to or is refused by the same deterministic workflow id; the marker is set once"
    - "scripts/backfill_visit_completed_events.py reports a visit whose outbox row exists but whose completion is older than --stale-after-minutes as stale_queued, and with --apply restarts its downstream work through ensure_downstream_started (restarted=N), instead of skipping it as already_queued"
    - "No workflow, workflow name, activity name or SweepResult shape changes (no workflow.patched needed); the retired sweeper Schedule is not recreated; every replay test passes"
    - "No harness test ages a NULL-marker visit past the reconcile grace, so the live reconciler (which shares visit_db) cannot race the suite"
  artifacts:
    - path: services/visit/repository/visit_repo.py
      provides: "list_lost_triggers(db, *, older_than_minutes, limit, clinic_id=None)"
      contains: "def list_lost_triggers"
    - path: services/visit/downstream.py
      provides: "restart_lost_triggers(session_factory, client, *, grace_minutes, limit, clinic_id=None) -> int"
      contains: "def restart_lost_triggers"
    - path: services/visit/activities/downstream.py
      provides: "VisitActivities.reconcile runs the unsettled pass, then the lost-trigger pass with the worker's Temporal client"
      contains: "restart_lost_triggers"
    - path: scripts/backfill_visit_completed_events.py
      provides: "stale_queued / restarted counts and the --stale-after-minutes option"
      contains: "stale_queued"
    - path: tests/e2e/test_phase3_visit_downstream.py
      provides: "test_lost_visit_trigger_is_recovered_by_the_reconciler"
  key_links:
    - from: "visit-reconciler Schedule (VisitReconcilerWorkflow, activity visit reconcile)"
      to: services/visit/downstream.py
      via: "VisitActivities.reconcile -> restart_lost_triggers -> ensure_downstream_started (start_once on visit-completed-<id>, then the conditional marker)"
      pattern: "restart_lost_triggers"
    - from: services/visit/downstream.py
      to: services/visit/repository/visit_repo.py
      via: "list_lost_triggers selects completed visits with a NULL marker older than the grace (partial index ix_visits_downstream_pending)"
      pattern: "list_lost_triggers"
    - from: scripts/backfill_visit_completed_events.py
      to: services/visit/downstream.py
      via: "a stale queued candidate is restarted with the same ensure_downstream_started the consumer and the reconciler use"
      pattern: "ensure_downstream_started"
---

<objective>
Close the recovery half of verification gap 1 (CR-01): give a lost `visit.completed` trigger (dead-lettered, or consumed and dropped) a recovery route, so a completed visit can never stay with `downstream_started_at IS NULL` for ever (billing never finalized, appointment never completed). The route is the existing `visit-reconciler` Temporal Schedule, which stays (D-03 kept the reconcilers): its activity gains a lost-trigger pass that calls the same idempotent `ensure_downstream_started` the consumer calls. The one-shot backfill script also learns to restart a visit whose event was queued long ago and never acted on.

Purpose: D-03 retired the sweeper on the condition that the outbox path is a full replacement; the kill tests proved the commit-to-publish path but not the dead-letter branch. With this plan the dead-letter branch is covered automatically after the reconcile grace (`VISIT_RECONCILE_GRACE_MINUTES`, default 10), and by hand at any time through the backfill. Do NOT re-introduce the retired `visit-downstream-sweeper` Schedule or a start-up sweep: only the reconciler activity body changes, so no workflow, workflow name, activity name or result type changes and no `workflow.patched` is needed.

Output: `list_lost_triggers`, `restart_lost_triggers`, the reconciler wiring, corrected docstrings and tests, the backfill's stale handling, and a live e2e proof.

Live stack rules (in front of every live step):
- Rebuild only `visit-worker` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-worker` (the only process whose behaviour changes; the script runs on the host). Never `down -v`, never remove a volume, never touch `tjo-*` containers (record their status before and after).
- Stop `visit-consumer` around every `tests/visit` run (`docker compose -f infra/docker-compose.yml stop visit-consumer`, then `start visit-consumer` and wait for `healthy`).
- The live `visit-reconciler` (every 60 s, grace 10 min) shares visit_db with the harness from this plan on: a harness test must never age a completed visit whose marker is NULL by more than 5 minutes, and every lost-trigger call in a harness test is scoped to the test's clinic.
- Gateway budget rule: before the first e2e run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`. Plan 03-25 restores the defaults (20 and 300) as its last step.
- The dev Postgres has `max_connections=300`: one pytest process at a time. Long suites (`tests/visit tests/workflows`, about 7 minutes) run in the background with polling; a run that spans a laptop sleep (wall-clock gap above 60 s in a 20 s heartbeat loop) is invalid and is rerun.
- No `.env*` or `infra/secrets/*` reads, no package installs, ids only in logs and script output (PHI rule).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-VERIFICATION.md
@.planning/phases/03-event-driven-system-observability/03-REVIEW.md
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-09-visit-outbox-downstream-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-12-visit-sweeper-retirement-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-SUMMARY.md
@services/visit/downstream.py
@services/visit/activities/downstream.py
@services/visit/repository/visit_repo.py
@scripts/backfill_visit_completed_events.py
@tests/e2e/test_phase3_visit_downstream.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `list_lost_triggers(db, *, older_than_minutes, limit=100, clinic_id=None) -> list[Visit]` | repository | `status='completed' AND downstream_started_at IS NULL AND completed_at < now() - grace`, oldest first; partial index `ix_visits_downstream_pending` |
| `restart_lost_triggers(session_factory, client, *, grace_minutes, limit=RECONCILE_LIMIT, clinic_id=None) -> int` | function | one short session per visit: re-read, skip if no longer a lost trigger, `ensure_downstream_started`; logs `visit.lost_trigger_restarted` (visit id) and `visit.lost_triggers` (scanned, restarted); one failure never stops the pass |
| `VisitActivities.reconcile()` | method | unsettled pass (unchanged) first, then the lost-trigger pass with the worker's Temporal client; returns settled + restarted; with no client the lost-trigger pass is skipped and `visit.lost_trigger_pass_skipped` is logged |
| backfill counts line | script output | `mode=... candidates=N already_queued=K written=M skipped=S stale_queued=Q restarted=R` (new fields appended at the end) |
| `--stale-after-minutes` | script option | default `VISIT_RECONCILE_GRACE_MINUTES` (10); restart needs Temporal (host: `TEMPORAL_ADDRESS=localhost:7233`) |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a visit whose visit.completed was consumed and dropped is restarted by the live visit-reconciler</name>
  <reversibility rating="reversible">Adds a pass to an existing activity body; removing it restores the previous behaviour without touching workflows or data.</reversibility>
  <precondition>The live stack is up: `docker compose -f infra/docker-compose.yml ps visit-worker visit-consumer --format '{{.Service}} {{.Health}}'` shows both healthy and `visit-reconciler` exists unpaused (asserted by `test_sweeper_schedule_retired_on_live_server`).</precondition>
  <files>services/visit/repository/visit_repo.py, services/visit/downstream.py, services/visit/activities/downstream.py, tests/e2e/test_phase3_visit_downstream.py</files>
  <behavior>
    - e2e `test_lost_visit_trigger_is_recovered_by_the_reconciler`: with `visit-consumer` stopped, a visit is checked in, started and completed through the gateway; its `visit.completed` message is found on `smarthealth.visit.events` (key = visit id; partition p, offset o); the `visit-downstream` group's committed offset on p equals o (otherwise the test fails and refuses to skip other events); the group's offset on `smarthealth.visit.events:p` is set to o+1 (what the consumer does after a dead-letter); the consumer is started and healthy; 10 s later the marker is NULL and no `visit-completed-<id>` execution exists
    - Recovery: the test moves the visit's `checked_in_at`, `started_at` and `completed_at` 30 minutes into the past (its own row, fresh clinic), triggers the `visit-reconciler` Schedule, and within 120 s the marker is set, exactly one execution exists, the visit-worker log has `visit.lost_trigger_restarted` with this visit id since the test began, the visit-consumer log has no `visit.downstream_started` for it, billing is `finalized` and the appointment `completed`
    - The tracer, the three D-03 kill tests and the sweeper-retired test in the same file still pass
  </behavior>
  <read_first>
    - services/visit/repository/visit_repo.py (`list_completed_without_downstream` lines 216-241, `list_unsettled` lines 284-312 with the `make_interval` grace pattern)
    - services/visit/downstream.py (module docstring, `ensure_downstream_started`, `reconcile_unsettled`, `RECONCILE_LIMIT`)
    - services/visit/activities/downstream.py (constructor: `client` kept "unused since the sweeper retired"; `reconcile`, `reconcile_unsettled`)
    - services/visit/worker.py (`build_activities(client)` passes the worker's connected client; the start-up reconcile pass)
    - tests/e2e/test_phase3_visit_downstream.py (`_restored`, `_complete_visit_through_gateway`, `_downstream_marker`, `_executions`, `_wait_one_execution`, `_consumer_started_it`, `_downstream_finishes`, `TEMPORAL_LOCAL_ADDRESS`)
    - tests/e2e/test_phase3_redelivery.py (`_reset_offsets` and `_group_drained`: the `kafka-consumer-groups` exec pattern and its retry while a stopped member expires)
    - tests/e2e/phase3_stack.py (`consume_matching` returns partition and offset; `compose`)
  </read_first>
  <action>
    Repository: add `list_lost_triggers` (artifacts table) next to `list_completed_without_downstream`, using the same `make_interval(secs => :grace)` bound parameter as `list_unsettled` and an optional clinic filter; update the docstrings of `list_unsettled` and `list_completed_without_downstream` (a never-started visit is the consumer's job first and the reconciler's after the grace; the backfill is the manual route).

    Downstream module: add `restart_lost_triggers` (artifacts table). Per candidate open a short session, re-read with `get_visit`, skip unless it is still completed with a NULL marker, then call `ensure_downstream_started(client, db, visit)`; log `visit.lost_trigger_restarted` with the visit id, and on any exception log `visit.lost_trigger_restart_failed` with the visit id and the exception class name only and continue. Update the module docstring: the outbox consumer is the trigger; the reconciler's lost-trigger pass is the safety net for an event that was dead-lettered or dropped (CR-01), after `VISIT_RECONCILE_GRACE_MINUTES`; the sweeper stays retired.

    Activities: `reconcile()` runs `reconcile_unsettled` first (so a pass never races a workflow it just started), then, when `self._client` is not None, `restart_lost_triggers` with the same grace; it returns settled plus restarted; with no client it logs `visit.lost_trigger_pass_skipped` and returns the settled count. Correct the constructor docstring (the Temporal client is used again, by the lost-trigger pass). Do not change `VisitReconcilerWorkflow`, any `ACT_*` name, `SweepResult` or `VisitCompletedWorkflow`.

    Before deploying, run the current script's dry run on the live data (`.venv/bin/python scripts/backfill_visit_completed_events.py`) and record its exact output (how many completed visits without a marker exist now). Rebuild and recreate `visit-worker`; from `docker compose -f infra/docker-compose.yml logs visit-worker` record how many `visit.lost_trigger_restarted` lines its start-up pass produced (dev junk visits may be restarted; that is the intended behaviour, record the count only).

    E2E: add `test_lost_visit_trigger_is_recovered_by_the_reconciler` to `tests/e2e/test_phase3_visit_downstream.py` per the behaviors, under a new "Plan 03-22" marker comment. Read the committed offset with `kafka-consumer-groups --describe --group visit-downstream` and move it with `kafka-consumer-groups --group visit-downstream --topic smarthealth.visit.events:<p> --reset-offsets --to-offset <o+1> --execute`, both through `stack.compose("exec", "-T", "kafka", ...)` with the retry pattern of `_reset_offsets`. Backdate with one UPDATE on the test's own visit id through `create_async_engine(visit_settings.database_url)` (disposed). Trigger with `temporal.get_schedule_handle("visit-reconciler").trigger()` (the Schedule also fires every 60 s by itself). Read visit-worker log lines the same way `_consumer_started_it` reads the consumer's. Wrap the consumer stop in `_restored("visit-consumer")`. Apply the gateway budget rule, then run the file's tracer, the new test and the sweeper-retired test.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k "lost_visit_trigger or tracer or sweeper_schedule_retired" -x -q</automated>
    <fails_when>pytest exits non-zero: the dropped trigger is not restarted within 120 s of the Schedule trigger, more than one execution exists, the restart is attributed to the consumer instead of the reconciler, billing or the appointment does not settle, the retired sweeper Schedule exists again, or the precondition on the group's committed offset fails.</fails_when>
  </verify>
  <acceptance_criteria>
    - the three selected e2e tests pass after the `visit-worker` rebuild
    - `grep -c "def list_lost_triggers" services/visit/repository/visit_repo.py` prints 1 and `grep -c "def restart_lost_triggers" services/visit/downstream.py` prints 1
    - `git diff --stat HEAD -- services/visit/workflows/visit_completed.py services/visit/workflows/sweeper.py services/visit/domain/contracts.py services/visit/schedules.py` is empty
    - the SUMMARY records the pre-deploy dry-run output and the count of `visit.lost_trigger_restarted` lines from the start-up pass
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>A dropped or dead-lettered visit trigger is recovered by the reconciler that already runs, proven on the live stack, with no workflow change and no sweeper.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Harness tests for the lost-trigger pass; rewrite the tests that encoded "never recovered"; reconciler docstring</name>
  <files>services/visit/workflows/reconciler.py, tests/visit/test_visit_reconciler.py, tests/visit/test_downstream.py</files>
  <behavior>
    - `restart_lost_triggers(factory, temporal_env.client, grace_minutes=0, clinic_id=<test clinic>)` on a freshly completed visit (consumer stopped, marker NULL) returns 1, sets the marker and leaves exactly one `visit-completed-<id>` execution; a second pass returns 0 and the run id is unchanged
    - With `grace_minutes=10` the same fresh visit is not selected by `list_lost_triggers`; a visit with a marker, a checked-in visit and an in-progress visit are never selected
    - A delivery through `deliver_visit_events` after the restart attaches to or is refused by the same id (one execution, marker unchanged)
    - `VisitActivities(..., client=None).reconcile()` leaves a lost trigger alone (marker NULL) and logs `visit.lost_trigger_pass_skipped`; with `client=temporal_env.client` and `reconcile_grace_minutes=0` the `reconcile_unsettled` activity run through `ActivityEnvironment` returns `processed >= 1` and the marker is set
    - `test_a_visit_whose_downstream_was_never_started_is_left_to_the_outbox_consumer` is replaced by `test_a_visit_whose_trigger_was_lost_is_restarted_after_the_grace` (grace 0, clinic-scoped, temporal_env client); `test_null_marker_visit_is_not_touched_by_any_worker_start_up_path` is rewritten without SQL aging past 5 minutes: the client-less start-up pass and the sweep tombstone leave the visit, the consumer delivery (positive control) starts it; the tombstone test ages its visit by 120 s instead of 600 s
    - `services/visit/workflows/reconciler.py` docstring still contains "outbox" and "never-started" (tests/docs requires both) and now says the lost-trigger pass restarts never-started visits after the grace
    - Every replay test under tests/workflows passes
  </behavior>
  <read_first>
    - tests/visit/test_visit_reconciler.py (module docstring on grace and the live reconciler; `_stuck_visit`, `_reconcile`, `_unsettled_ids`, the test at lines 281-296, `test_the_reconcile_activity_runs_a_pass_and_returns_the_count`)
    - tests/visit/test_downstream.py (`_completed_visit`, `_age_completion` line 146, `_activities` line 517, the tests at lines 218 and 252)
    - services/visit/workflows/reconciler.py (docstring lines 1-19)
    - tests/docs/test_phase3_docs.py (`test_the_three_named_visit_docstrings_say_what_replaced_the_sweeper`, `test_the_visit_sweeper_is_not_described_as_a_live_trigger_anywhere`)
  </read_first>
  <action>
    Write the new and rewritten tests per the behaviors; no other assertion is weakened or removed. Every lost-trigger call passes the test's clinic id; no harness test ages a NULL-marker completed visit past 5 minutes (the live reconciler would restart it). Rewrite the `VisitReconcilerWorkflow` module docstring (text only; the class and its single `execute_activity` call stay byte-identical): the reconcile activity settles started-but-unsettled visits and, after the grace, restarts never-started visits whose outbox event was dead-lettered or dropped, through the same idempotent start the outbox consumer uses; the sweeper stays retired. Stop `visit-consumer`, run `tests/visit tests/workflows` in the background with polling, then start the consumer again.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit tests/workflows -x -q</automated>
    <fails_when>pytest exits non-zero, a replay test fails, a lost trigger is restarted twice or not at all, or a test still asserts that a never-started visit is left alone after the grace with a Temporal client available.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit tests/workflows -q` exits 0 (run with `visit-consumer` stopped; restarted and healthy afterwards)
    - `grep -c "left_to_the_outbox_consumer" tests/visit/test_visit_reconciler.py` prints 0
    - `grep -c "_age_completion(visit\[\"id\"\], 600)" tests/visit/test_downstream.py` prints 0
    - `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -q` exits 0
  </acceptance_criteria>
  <done>The harness proves the lost-trigger pass is selective, idempotent and clinic-scoped, and no test encodes the old "never recovered" behaviour.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: The backfill restarts a queued-but-dead-lettered visit</name>
  <files>scripts/backfill_visit_completed_events.py, tests/visit/test_backfill.py</files>
  <behavior>
    - The counts line is `mode=... candidates=N already_queued=K written=M skipped=S stale_queued=Q restarted=R`; the test's COUNTS regex is extended for the two new trailing fields and every existing assertion stays as it is
    - A completed, unmarked visit whose outbox row exists and whose completion is older than `--stale-after-minutes` is counted `stale_queued` (not `already_queued`); a dry run writes nothing and restarts nothing
    - In-process `backfill(factory, apply=True, stale_after_minutes=0, clinic_id=<test clinic>, client=temporal_env.client)` restarts such a visit through `ensure_downstream_started`: `restarted=1`, marker set, one execution; a second apply has no candidate left
    - A queued visit inside the stale window stays `already_queued` and is not restarted
    - `_insert_visit` places `started_at` 3 minutes and `completed_at` 2 minutes in the past (inside the live reconciler's grace), so the live reconciler cannot race the backfill tests
    - Output is counts only; a Temporal or database failure exits 1 with the exception class name only
  </behavior>
  <read_first>
    - scripts/backfill_visit_completed_events.py (whole file: `BackfillCounts.line`, `_event_exists`, `backfill`, `_run`, `main`, the CAUTION block)
    - tests/visit/test_backfill.py (`COUNTS` regex lines 39-42, `_insert_visit`, `_run_script`, `test_delivering_the_backfilled_event_starts_downstream_work_once`)
    - libs/temporal.py (`connect_client` reads `TEMPORAL_ADDRESS` from the shared settings)
    - services/visit/downstream.py (`ensure_downstream_started`, Task 1)
  </read_first>
  <action>
    Add `--stale-after-minutes` (integer, at least 0, default `settings.reconcile_grace_minutes`). In `backfill`, a candidate with an existing outbox row is `stale_queued` when its `completed_at` is older than the stale window (compare against `now()` from the database in the same session, not the host clock), else `already_queued`. `backfill` gains keyword arguments `stale_after_minutes` and `client` (a Temporal client or None). With `apply` and at least one stale candidate, `_run` connects once with `libs.temporal.connect_client(lazy=False)`; each stale visit gets its own session, a re-read (skip unless still completed and unmarked) and `ensure_downstream_started(client, db, visit)`, counted `restarted`. Append the two counts to the end of the line so existing parsers keep matching their prefix. Update the docstring: why `already_queued` is not enough (a dead-lettered event has a row too), that the visit-reconciler does the same restart automatically after its grace and the script is the manual route (for example when the worker is down), that a restart starts downstream work only (billing finalize, appointment completion; no notification is sent by this path), and that the host needs `TEMPORAL_ADDRESS=localhost:7233`.

    Tests: extend the regex, move `_insert_visit`'s timestamps inside the grace, and add the stale tests by importing the script module in-process (`importlib.util.spec_from_file_location` on `SCRIPT`) and calling `backfill` with `temporal_env.client` (no subprocess for the new tests; terminate the started execution at the end). Run with `visit-consumer` stopped, then start it again. Afterwards run the script's dry run against the live visit_db and record the exact output.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit/test_backfill.py -x -q && .venv/bin/python scripts/backfill_visit_completed_events.py</automated>
    <fails_when>pytest exits non-zero, the dry run writes or restarts anything, a stale queued visit is not restarted on apply, a fresh queued visit is restarted, the output carries anything but counts, or the script exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit/test_backfill.py -q` exits 0 with at least 8 passed
    - the live dry run prints one line matching `^mode=dry-run candidates=\d+ already_queued=\d+ written=0 skipped=\d+ stale_queued=\d+ restarted=0$`
    - `grep -c "ensure_downstream_started" scripts/backfill_visit_completed_events.py` prints at least 1
  </acceptance_criteria>
  <done>The backfill no longer treats a dead-lettered visit as handled: it reports it and, on apply, restarts its downstream work idempotently.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| visit worker (reconciler activity) -> Temporal and visit_db | an automatic pass starts downstream work for rows it selects |
| host script -> visit_db and Temporal | an operator tool restarts downstream work |
| e2e test -> Kafka consumer-group offsets | the test moves a real group's committed offset |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-67 | Tampering | `restart_lost_triggers` (starting downstream work for a visit that is not completed, or twice) | medium | mitigate | Selects only `status='completed' AND marker IS NULL` past the grace, re-reads each row before acting, uses `start_once` on the deterministic id and the conditional marker; tests prove one execution across passes, deliveries and backfill |
| T-3-68 | Repudiation | a lost trigger left unstarted with no trace (CR-01) | high | mitigate | The reconciler pass runs every 60 s and at worker start; `visit.lost_trigger_restarted` / `visit.lost_triggers` log lines and the backfill's `stale_queued` / `restarted` counts make the recovery visible; the e2e test proves it live |
| T-3-69 | Information Disclosure | backfill output and new log lines | low | mitigate | Counts only in script output; logs carry visit ids and exception class names only (no payload, no URL, no credential) |
| T-3-70 | Tampering | e2e offset move on the live `visit-downstream` group (could skip other events) | medium | mitigate | The test moves the offset only when the committed offset equals the test event's offset, only for that partition, only by one, with the consumer stopped; otherwise it fails without moving anything |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/visit tests/workflows -q` with `visit-consumer` stopped, then started again and healthy
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests/docs -q`
- Semgrep: `semgrep --config p/python --metrics=off services/visit scripts/backfill_visit_completed_events.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- A dead-lettered or dropped `visit.completed` is recovered by the existing reconciler after the grace, and by the backfill on demand (CR-01 recovery route, D-03 replacement)
- No workflow change, no sweeper Schedule, replay suite green
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-22-visit-trigger-recovery-SUMMARY.md` when done
</output>
