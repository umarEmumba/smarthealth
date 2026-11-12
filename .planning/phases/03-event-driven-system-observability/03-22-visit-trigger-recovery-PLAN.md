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
  - scripts/replay_dlq.py
  - tests/visit/conftest.py
  - tests/visit/test_visit_reconciler.py
  - tests/visit/test_downstream.py
  - tests/visit/test_backfill.py
  - tests/events/test_replay_dlq.py
  - tests/e2e/test_phase3_visit_downstream.py
autonomous: true
gap_closure: true
requirements: [EVENT-02, EVENT-03, EVENT-05]

estimate:
  tokens: 110000
  raw_tokens: 110000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 (CR-01 consequence, D-03 replacement): a visit that is completed with downstream_started_at IS NULL and completed_at older than the reconcile grace is found by the existing visit-reconciler Schedule and its downstream workflow is started with ensure_downstream_started, without the retired visit-downstream-sweeper Schedule"
    - "On the live stack, a visit.completed that was consumed and dropped (the visit-downstream group's offset moved past it, as after a dead-letter) leaves the visit unstarted, and the live reconciler then starts exactly one visit-completed-<visit id> execution, sets the marker, and billing is finalized and the appointment completed; the visit-worker logs visit.lost_trigger_restarted for that visit id and the consumer never started it"
    - "Recovery is idempotent: a second reconcile pass, a racing consumer delivery or a backfill restart attaches to or is refused by the same deterministic workflow id; the marker is set once"
    - "scripts/backfill_visit_completed_events.py reports a visit whose outbox row exists but whose completion is older than --stale-after-minutes as stale_queued, and with --apply restarts its downstream work through ensure_downstream_started (restarted=N), instead of skipping it as already_queued"
    - "No workflow, workflow name, activity name or SweepResult shape changes (no workflow.patched needed); the retired sweeper Schedule is not recreated; every replay test passes"
    - "Harness isolation from the live reconciler (W5/W6): every lost-trigger call in a harness test passes the test's own clinic_id; VisitActivities.reconcile() is exercised in harness tests only with its two pass functions replaced by recorders, so no harness test runs an unscoped lost-trigger pass against the shared visit_db; no harness test ages a NULL-marker visit past 5 minutes; and a session guard in tests/visit/conftest.py fails the suite (after removing them) if any completed NULL-marker visit of the suite's own providers survives teardown"
    - "TransientExhausted and other dead letters are recoverable by a tool that exists (W4): scripts/replay_dlq.py, dry-run by default, selects DLQ records by group or DLQ topic, error type, key, event id and --limit, never changes a consumer group's offsets and never prints a payload; with --apply it re-produces the original key, value bytes and original headers (dlq.* removed, so the event id header and traceparent are the original ones) to the record's dlq.original_topic, only if that topic is a catalogue topic (or explicitly allowed), and the idempotent consumers absorb the duplicate (D-08)"
    - "The start-up and periodic reconcile acting on existing dev visits with a lost trigger is intended (I9): the SUMMARY reports the pre-deploy dry-run counts, the number of visit.lost_trigger_restarted lines after the deploy, and the post-deploy dry-run counts"
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
    - path: scripts/replay_dlq.py
      provides: "DLQ replay: dry run by default, --apply re-produces original key, value and headers to dlq.original_topic"
      contains: "dlq.original_topic"
    - path: tests/events/test_replay_dlq.py
      provides: "replay tests against throwaway topics on the live broker"
    - path: tests/visit/conftest.py
      provides: "null_marker_leftovers helper and the session guard against surviving NULL-marker harness visits"
      contains: "null_marker_leftovers"
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
    - from: scripts/replay_dlq.py
      to: "the dead-lettered record's original topic"
      via: "reads dlq.original_topic from the DLQ record headers and produces the original key, value and non-dlq headers there with an idempotent producer"
      pattern: "dlq.original_topic"
---

<objective>
Close the recovery half of verification gap 1 (CR-01): give a lost `visit.completed` trigger (dead-lettered, or consumed and dropped) a recovery route, so a completed visit can never stay with `downstream_started_at IS NULL` for ever (billing never finalized, appointment never completed). The route is the existing `visit-reconciler` Temporal Schedule, which stays (D-03 kept the reconcilers): its activity gains a lost-trigger pass that calls the same idempotent `ensure_downstream_started` the consumer calls. The one-shot backfill script also learns to restart a visit whose event was queued long ago and never acted on.

Purpose: D-03 retired the sweeper on the condition that the outbox path is a full replacement; the kill tests proved the commit-to-publish path but not the dead-letter branch. With this plan the dead-letter branch is covered automatically after the reconcile grace (`VISIT_RECONCILE_GRACE_MINUTES`, default 10), and by hand at any time through the backfill. Do NOT re-introduce the retired `visit-downstream-sweeper` Schedule or its separate run-on-start sweep: only the existing reconcile activity body changes (it already runs on the `visit-reconciler` Schedule and once at worker start), so no workflow, workflow name, activity name or result type changes and no `workflow.patched` is needed.

The plan also ships the recovery tool that plan 03-21's `TransientExhausted` records need for the two groups the reconciler does not cover (W4): `scripts/replay_dlq.py` re-produces a dead-lettered record's original bytes to its original topic, where every consumer group on that topic absorbs the duplicate through its idempotency key (D-08: processed-event claims for analytics, dedupe keys and deterministic workflow ids for notifications, `start_once` plus the conditional marker for visit-downstream).

Intended effect on existing data (I9): from the `visit-worker` rebuild on, the reconcile pass (at worker start and every 60 s) restarts every completed visit in the dev visit_db whose marker is NULL and whose completion is older than the grace, including dev or harness leftovers from earlier runs. That is the behaviour this plan exists to add; record the counts (Task 1) and do not try to prevent it.

Output: `list_lost_triggers`, `restart_lost_triggers`, the reconciler wiring, corrected docstrings and tests, the harness guard, the backfill's stale handling, the DLQ replay script, and a live e2e proof.

Live stack rules (in front of every live step):
- Rebuild only `visit-worker` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-worker` (the only process whose behaviour changes; the scripts run on the host). Never `down -v`, never remove a volume, never touch `tjo-*` containers (record their status before and after).
- Stop `visit-consumer` around every `tests/visit` run: `docker compose -f infra/docker-compose.yml stop visit-consumer` before, `docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer` after (returns once healthy). The `<verify>` commands of Tasks 2 and 3 do this themselves.
- The live `visit-reconciler` (every 60 s, grace 10 min) shares visit_db with the harness from this plan on (W5/W6): every lost-trigger call in a harness test passes the test's own clinic id; `VisitActivities.reconcile()` runs in harness tests only with its pass functions replaced by recorders; no harness test ages a completed visit whose marker is NULL by more than 5 minutes; the visit_world teardown removes the suite's visits and the new session guard fails the suite if a NULL-marker harness visit survives.
- Gateway budget rule: before the first e2e run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`. Plan 03-26 restores the defaults (20 and 300) as its last automated step.
- The dev Postgres has `max_connections=300`: one pytest process at a time. Long suites (`tests/visit tests/workflows`, about 7 minutes) run in the background with polling; a run that spans a laptop sleep (wall-clock gap above 60 s in a 20 s heartbeat loop) is invalid and is rerun.
- No direct reads of `.env*` or `infra/secrets/*` (settings modules loading `.env` implicitly is fine), no package installs, ids only in logs and script output (PHI rule).
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
| `null_marker_leftovers(provider_ids) -> int` | test helper in `tests/visit/conftest.py` | completed visits with a NULL marker among the given provider ids; used by the session guard and its positive control |
| session guard | autouse session fixture in `tests/visit/conftest.py` | `_visit_world` registers its final provider ids in a module-level set; at session end, if `null_marker_leftovers` is above 0 for that set, the guard removes them (outbox, history, visits, as the existing cleanup helpers do) and then fails the session |
| `scripts/replay_dlq.py` | operator script | one of `--group <consumer group>` (DLQ topic from `libs.events.catalog.DLQ_TOPICS`, and the record's `dlq.consumer_group` header must match) or `--dlq-topic <name>`; filters `--error-type`, `--key`, `--event-id`, `--limit` (default 100); `--allow-topic <name>` (repeatable) adds a replay target beyond the catalogue topics; `--apply` to produce; default dry run |
| replay output | script output | one line per selected record `dlq_partition=P dlq_offset=O original=<topic>:<p>:<o> error_type=<type> event_id=<uuid or ->` and a final `mode=dry-run\|apply scanned=N selected=M replayed=R refused=F`; never a key, value or header value other than the event id |

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

    Before deploying, run the current script's dry run on the live data (`.venv/bin/python scripts/backfill_visit_completed_events.py`) and record its exact output (how many completed visits without a marker exist now). Rebuild and recreate `visit-worker`; two minutes later, from `docker compose -f infra/docker-compose.yml logs visit-worker` record how many `visit.lost_trigger_restarted` and `visit.lost_trigger_restart_failed` lines the start-up pass and the first periodic passes produced, and run the dry run again and record it (per I9 in the objective: restarting existing dev visits whose trigger was lost is the intended behaviour; record the counts, never the ids).

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
    - the SUMMARY records the pre-deploy dry-run output, the counts of `visit.lost_trigger_restarted` and `visit.lost_trigger_restart_failed` lines from the start-up and first periodic passes, and the post-deploy dry-run output (I9)
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>A dropped or dead-lettered visit trigger is recovered by the reconciler that already runs, proven on the live stack, with no workflow change and no sweeper.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Harness tests for the lost-trigger pass (clinic-scoped, guarded against leftovers); rewrite the tests that encoded "never recovered"; reconciler docstring</name>
  <files>services/visit/workflows/reconciler.py, tests/visit/conftest.py, tests/visit/test_visit_reconciler.py, tests/visit/test_downstream.py</files>
  <behavior>
    - `restart_lost_triggers(factory, temporal_env.client, grace_minutes=0, clinic_id=<test clinic>)` on a freshly completed visit (consumer stopped, marker NULL) returns 1, sets the marker and leaves exactly one `visit-completed-<id>` execution; a second pass with the same clinic id returns 0 and the run id is unchanged
    - With `grace_minutes=10` the same fresh visit is not selected by `list_lost_triggers(..., clinic_id=<test clinic>)`; a visit with a marker, a checked-in visit and an in-progress visit of the same clinic are never selected; a lost trigger of a second clinic created by the same test is not selected when the first clinic's id is passed (the clinic filter is real)
    - A delivery through `deliver_visit_events` after the restart attaches to or is refused by the same id (one execution, marker unchanged)
    - Activity wiring without touching visit_db (W5): with `reconcile_unsettled` and `restart_lost_triggers` replaced in `services.visit.activities.downstream` by recorders (monkeypatch), `VisitActivities(..., client=<sentinel>).reconcile()` calls the unsettled recorder first and then the lost-trigger recorder with that client, the activity's grace and no clinic id, and returns the sum of their counts; with `client=None` the lost-trigger recorder is never called and `visit.lost_trigger_pass_skipped` is logged
    - `test_a_visit_whose_downstream_was_never_started_is_left_to_the_outbox_consumer` is replaced by `test_a_visit_whose_trigger_was_lost_is_restarted_after_the_grace` (grace 0, clinic-scoped `restart_lost_triggers`, temporal_env client); `test_null_marker_visit_is_not_touched_by_any_worker_start_up_path` is rewritten without SQL aging past 5 minutes: the client-less reconcile (pass functions recorded, as above) and the sweep tombstone leave the visit, the consumer delivery (positive control) starts it; the tombstone test ages its visit by 120 s instead of 600 s
    - Leftover guard (W6): a positive-control test creates a completed NULL-marker visit in its `visit_world`, asserts `null_marker_leftovers(visit_world.provider_ids)` is 1 before teardown (the guard can see such rows); after the whole run the session guard finds 0 (the run passes), and the guard's failure path is exercised by a unit test that feeds it a fake count above 0 and asserts it calls the clean-up helpers and raises
    - `services/visit/workflows/reconciler.py` docstring still contains "outbox" and "never-started" (tests/docs requires both) and now says the lost-trigger pass restarts never-started visits after the grace
    - Every replay test under tests/workflows passes
  </behavior>
  <read_first>
    - tests/visit/test_visit_reconciler.py (module docstring on grace and the live reconciler; `_stuck_visit`, `_reconcile`, `_unsettled_ids`, the test at lines 281-296, `test_the_reconcile_activity_runs_a_pass_and_returns_the_count`)
    - tests/visit/test_downstream.py (`_completed_visit`, `_age_completion` line 146, `_activities` line 517 (it passes no Temporal client), the tests at lines 218 and 252)
    - tests/visit/conftest.py (`_visit_world`, `cleanup_visit_rows`, `cleanup_visit_outbox`: the suite's visits are already deleted by provider id at teardown)
    - services/visit/workflows/reconciler.py (docstring lines 1-19)
    - tests/docs/test_phase3_docs.py (`test_the_three_named_visit_docstrings_say_what_replaced_the_sweeper`, `test_the_visit_sweeper_is_not_described_as_a_live_trigger_anywhere`)
  </read_first>
  <action>
    Write the new and rewritten tests per the behaviors; no other assertion is weakened or removed. Scoping rules (W5): every `restart_lost_triggers` and `list_lost_triggers` call in a test passes the test's own `visit_world.clinic_id`; `VisitActivities.reconcile()` is only called with both pass functions monkeypatched to recorders, so no harness test runs an unscoped lost-trigger pass against the shared visit_db (an unscoped pass with the test Temporal server would mark live dev visits as started and start their workflows on a throw-away server). The pre-existing tests that call `reconcile_unsettled` directly keep their behaviour (unsettled pass only, started visits only, documented as lower-bound assertions in that module's docstring); say so in the SUMMARY. No harness test ages a NULL-marker completed visit past 5 minutes (the live reconciler's grace is 10).

    Leftover guard (W6) in `tests/visit/conftest.py`: add `null_marker_leftovers(provider_ids)` (count of `status='completed' AND downstream_started_at IS NULL` visits for those provider ids, own short-lived engine, disposed); a module-level set that `_visit_world` fills with its final `provider_ids` just before its clean-up; and a session-scoped autouse fixture (synchronous, driving its async work with `asyncio.run`) that at session end counts leftovers for that set and, when above 0, removes them with `cleanup_visit_outbox` and `cleanup_visit_rows` and then fails with the count only. Keep the guard's decision in a small function the unit test can call with an injected count and recorded clean-up callables.

    Rewrite the `VisitReconcilerWorkflow` module docstring (text only; the class and its single `execute_activity` call stay byte-identical): the reconcile activity settles started-but-unsettled visits and, after the grace, restarts never-started visits whose outbox event was dead-lettered or dropped, through the same idempotent start the outbox consumer uses; the sweeper stays retired. Run `tests/visit tests/workflows` in the background with polling (the verify command below stops `visit-consumer` first and starts it again afterwards, whatever the result).
  </action>
  <verify>
    <automated>docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit tests/workflows -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; }</automated>
    <fails_when>The consumer cannot be stopped or is not healthy again within 180 s, pytest exits non-zero (including a session-guard failure: a NULL-marker harness visit survived), a replay test fails, a lost trigger is restarted twice or not at all, a lost-trigger call selects a visit of another clinic, or a test still asserts that a never-started visit is left alone after the grace with a Temporal client available.</fails_when>
  </verify>
  <acceptance_criteria>
    - the verify command exits 0 and `docker compose -f infra/docker-compose.yml ps visit-consumer --format '{{.Health}}'` prints `healthy` afterwards
    - `grep -c "left_to_the_outbox_consumer" tests/visit/test_visit_reconciler.py` prints 0
    - `grep -c "_age_completion(visit\[\"id\"\], 600)" tests/visit/test_downstream.py` prints 0
    - every call of `restart_lost_triggers` or `list_lost_triggers` under tests/visit passes a `clinic_id` keyword: `.venv/bin/python -c "import ast,pathlib,sys; bad=[f'{p}:{n.lineno}' for p in pathlib.Path('tests/visit').rglob('*.py') for n in ast.walk(ast.parse(p.read_text())) if isinstance(n, ast.Call) and getattr(n.func, 'id', getattr(n.func, 'attr', None)) in ('restart_lost_triggers', 'list_lost_triggers') and not any(k.arg == 'clinic_id' for k in n.keywords)]; print(bad); sys.exit(1 if bad else 0)"` prints `[]` and exits 0
    - `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -q` exits 0
  </acceptance_criteria>
  <done>The harness proves the lost-trigger pass is selective, idempotent and clinic-scoped, cannot touch other visits in the shared visit_db, leaves no NULL-marker visit behind, and no test encodes the old "never recovered" behaviour.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Manual recovery tools — the backfill restarts a queued-but-dead-lettered visit, and a DLQ replay script re-produces dead-lettered records</name>
  <files>scripts/backfill_visit_completed_events.py, tests/visit/test_backfill.py, scripts/replay_dlq.py, tests/events/test_replay_dlq.py</files>
  <behavior>
    - The counts line is `mode=... candidates=N already_queued=K written=M skipped=S stale_queued=Q restarted=R`; the test's COUNTS regex is extended for the two new trailing fields and every existing assertion stays as it is
    - A completed, unmarked visit whose outbox row exists and whose completion is older than `--stale-after-minutes` is counted `stale_queued` (not `already_queued`); a dry run writes nothing and restarts nothing
    - In-process `backfill(factory, apply=True, stale_after_minutes=0, clinic_id=<test clinic>, client=temporal_env.client)` restarts such a visit through `ensure_downstream_started`: `restarted=1`, marker set, one execution; a second apply has no candidate left
    - A queued visit inside the stale window stays `already_queued` and is not restarted
    - `_insert_visit` places `started_at` 3 minutes and `completed_at` 2 minutes in the past (inside the live reconciler's grace), so the live reconciler cannot race the backfill tests
    - Output is counts only; a Temporal or database failure exits 1 with the exception class name only
    - Replay (tests/events/test_replay_dlq.py, live broker, throw-away topics and groups from tests/events/conftest.py, script imported in-process and driven through its `main(argv)` with stdout captured): three records are produced to a throw-away DLQ topic, shaped like the consumer base's DLQ records (original headers `id`, `eventType`, `traceparent` plus the `dlq.*` headers; `dlq.original_topic` = a throw-away source topic; two with `dlq.error_type=TransientExhausted`, one `HandlerError`; distinct keys and event ids; each value contains a marker string)
    - The default run (no `--apply`) produces nothing to the source topic and its final line is `mode=dry-run ... selected=2 replayed=0` for `--error-type TransientExhausted`
    - `--apply --error-type TransientExhausted --event-id <e1> --allow-topic <source>` produces exactly one record to the source topic whose key and value bytes equal the dead-lettered record's and whose headers equal its original headers in order, with no `dlq.` header; `--key` selects by key; `--limit 1` with two matches replays one
    - A record whose `dlq.original_topic` is neither a catalogue topic nor allowed is refused (`refused=1`) and nothing is produced; the throw-away consumer groups' committed offsets are unchanged by the script (it assigns partitions and never commits)
    - Neither the marker string from the values nor any key appears in the captured output
  </behavior>
  <read_first>
    - scripts/backfill_visit_completed_events.py (whole file: `BackfillCounts.line`, `_event_exists`, `backfill`, `_run`, `main`, the CAUTION block)
    - tests/visit/test_backfill.py (`COUNTS` regex lines 39-42, `_insert_visit`, `_run_script`, `test_delivering_the_backfilled_event_starts_downstream_work_once`)
    - libs/temporal.py (`connect_client` reads `TEMPORAL_ADDRESS` from the shared settings)
    - services/visit/downstream.py (`ensure_downstream_started`, Task 1)
    - libs/events/consumer.py (`_dead_letter` and `_write_dlq`: the exact `dlq.*` header names; the DLQ producer configuration)
    - libs/events/catalog.py (`AGGREGATE_TOPICS`, `DLQ_TOPICS`, `CONSUMER_GROUPS`) and libs/config.py (`kafka_bootstrap_servers`)
    - tests/events/conftest.py (`kafka_admin`, `unique_topic`, `dlq_topic`, `unique_group`, `poll_until`) and tests/events/test_consumer_dlq.py (how DLQ records are read back)
  </read_first>
  <action>
    Add `--stale-after-minutes` (integer, at least 0, default `settings.reconcile_grace_minutes`). In `backfill`, a candidate with an existing outbox row is `stale_queued` when its `completed_at` is older than the stale window (compare against `now()` from the database in the same session, not the host clock), else `already_queued`. `backfill` gains keyword arguments `stale_after_minutes` and `client` (a Temporal client or None). With `apply` and at least one stale candidate, `_run` connects once with `libs.temporal.connect_client(lazy=False)`; each stale visit gets its own session, a re-read (skip unless still completed and unmarked) and `ensure_downstream_started(client, db, visit)`, counted `restarted`. Append the two counts to the end of the line so existing parsers keep matching their prefix. Update the docstring: why `already_queued` is not enough (a dead-lettered event has a row too), that the visit-reconciler does the same restart automatically after its grace and the script is the manual route (for example when the worker is down), that a restart starts downstream work only (billing finalize, appointment completion; no notification is sent by this path), and that the host needs `TEMPORAL_ADDRESS=localhost:7233`.

    Tests: extend the regex, move `_insert_visit`'s timestamps inside the grace, and add the stale tests by importing the script module in-process (`importlib.util.spec_from_file_location` on `SCRIPT`) and calling `backfill` with `temporal_env.client` (no subprocess for the new tests; terminate the started execution at the end).

    DLQ replay script (W4), `scripts/replay_dlq.py`, options and output per the artifacts table. Reading: a confluent `Consumer` with a throw-away `group.id`, `enable.auto.commit=False`, partitions `assign`ed explicitly from each partition's low watermark up to the high watermark captured at start (`get_watermark_offsets`), so no consumer group's committed offsets ever change and the run ends. Selection: skip a record without `dlq.original_topic`; with `--group`, require `dlq.consumer_group` equal to it; apply `--error-type`, `--key` (compared to the record key bytes), `--event-id` (the `id` header the Debezium outbox router sets; only when that header is absent, decode the value with `libs.events.registry.deserializer()` to read `event_id`, never logging the record), stop at `--limit`. Before writing, list the header NAMES of one live record on `smarthealth.visit.events` (names only, no values) and record them in the SUMMARY to confirm the `id` header exists. Target check: the original topic must be one of `libs.events.catalog.AGGREGATE_TOPICS` values or named by `--allow-topic`; otherwise count it `refused` and produce nothing for it. Apply: one `Producer` with `enable.idempotence=True` and `acks=all`; produce the record's key and value bytes unchanged with its headers minus every header whose name starts with `dlq.` (order kept), flush, and exit 1 with the KafkaError name only on any delivery error. Without `--apply` nothing is produced. The module docstring says: dry run by default; a replay reaches every consumer group on the original topic, and each absorbs the duplicate through its idempotency key (D-08); replaying a record that fails again dead-letters it again; the per-group recovery map (visit-downstream: the reconciler's lost-trigger pass and this plan's backfill, or this script; analytics-projection: this script or `services.analytics.rebuild`; notification-trigger: this script); WR-06 (follow-ups for old visits after a backfill) is unaffected because a replay keeps the original event id.

    Write `tests/events/test_replay_dlq.py` per the behaviors. Run both test files, then the dry runs of both scripts against the live stack (the replay dry run on the live `visit-downstream` DLQ prints metadata and counts only), and record their exact output. The verify command stops `visit-consumer` around the `tests/visit` file and starts it again whatever the result.
  </action>
  <verify>
    <automated>docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit/test_backfill.py tests/events/test_replay_dlq.py -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; } && .venv/bin/python scripts/backfill_visit_completed_events.py && .venv/bin/python scripts/replay_dlq.py --group visit-downstream</automated>
    <fails_when>The consumer cannot be stopped or is not healthy again within 180 s, pytest exits non-zero, a backfill or replay dry run writes, restarts or produces anything, a stale queued visit is not restarted on apply, a fresh queued visit is restarted, a replayed record differs from the original in key, value or non-dlq headers or keeps a dlq header, a refused target is produced to, a key or payload reaches the output, or either live dry run exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit/test_backfill.py -q` exits 0 with at least 8 passed, and `.venv/bin/python -m pytest tests/events/test_replay_dlq.py -q` exits 0 with at least 6 passed
    - the live backfill dry run prints one line matching `^mode=dry-run candidates=\d+ already_queued=\d+ written=0 skipped=\d+ stale_queued=\d+ restarted=0$`
    - the live replay dry run's last line matches `^mode=dry-run scanned=\d+ selected=\d+ replayed=0 refused=\d+$`
    - `grep -c "ensure_downstream_started" scripts/backfill_visit_completed_events.py` prints at least 1 and `grep -c "enable.idempotence" scripts/replay_dlq.py` prints at least 1
    - the SUMMARY records the header names of the inspected live record (names only)
  </acceptance_criteria>
  <done>The backfill no longer treats a dead-lettered visit as handled, and any group's dead-lettered record (TransientExhausted included) can be replayed safely to its original topic with a dry run first.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| visit worker (reconciler activity) -> Temporal and visit_db | an automatic pass starts downstream work for rows it selects |
| host script -> visit_db and Temporal | an operator tool restarts downstream work |
| e2e test -> Kafka consumer-group offsets | the test moves a real group's committed offset |
| harness tests -> shared visit_db | tests run against the database the live reconciler also scans |
| DLQ record headers -> replay script -> Kafka topics | header values written by any producer on the dev network decide where a replay goes |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-67 | Tampering | `restart_lost_triggers` (starting downstream work for a visit that is not completed, or twice) | medium | mitigate | Selects only `status='completed' AND marker IS NULL` past the grace, re-reads each row before acting, uses `start_once` on the deterministic id and the conditional marker; tests prove one execution across passes, deliveries and backfill |
| T-3-68 | Repudiation | a lost trigger left unstarted with no trace (CR-01) | high | mitigate | The reconciler pass runs every 60 s and at worker start; `visit.lost_trigger_restarted` / `visit.lost_triggers` log lines and the backfill's `stale_queued` / `restarted` counts make the recovery visible; the e2e test proves it live |
| T-3-69 | Information Disclosure | backfill output and new log lines | low | mitigate | Counts only in script output; logs carry visit ids and exception class names only (no payload, no URL, no credential) |
| T-3-70 | Tampering | e2e offset move on the live `visit-downstream` group (could skip other events) | medium | mitigate | The test moves the offset only when the committed offset equals the test event's offset, only for that partition, only by one, with the consumer stopped; otherwise it fails without moving anything |
| T-3-71 | Tampering | harness lost-trigger calls on the shared visit_db (an unscoped pass marks live dev visits as started and starts their workflows on a throw-away test server; leftovers get restarted by the live reconciler) | medium | mitigate | Every lost-trigger call passes the test's clinic id (AST-checked in the acceptance criteria); `VisitActivities.reconcile()` only with recorded pass functions; no NULL-marker visit aged past 5 minutes; visit_world teardown plus a session guard that removes and fails on any surviving NULL-marker harness visit |
| T-3-72 | Tampering | `scripts/replay_dlq.py` target (a forged `dlq.original_topic` header redirects a replay; a careless apply re-sends a batch) | medium | mitigate | Dry run by default; target must be a catalogue topic or explicitly allowed, else refused; `--limit` default 100; idempotent producer; original key, value and event id kept so every consumer group absorbs the duplicate (D-08); reads with assigned partitions and never commits group offsets |
| T-3-73 | Information Disclosure | replay script output | low | mitigate | Output is DLQ coordinates, error type, event id and counts only; never key, value or other header values; a test asserts a marker in the values and the keys never reach stdout |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit tests/workflows -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; }`
- `.venv/bin/python -m pytest tests/events/test_replay_dlq.py -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests/docs -q`
- Semgrep: `semgrep --config p/python --metrics=off services/visit scripts/backfill_visit_completed_events.py scripts/replay_dlq.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- A dead-lettered or dropped `visit.completed` is recovered by the existing reconciler after the grace, and by the backfill on demand (CR-01 recovery route, D-03 replacement)
- Any group's dead-lettered record, `TransientExhausted` included, can be replayed to its original topic by a dry-run-first script (W4)
- Harness tests cannot act on other visits in the shared visit_db and leave no NULL-marker visit behind (W5, W6)
- No workflow change, no sweeper Schedule, replay suite green
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-22-visit-trigger-recovery-SUMMARY.md` when done
</output>
