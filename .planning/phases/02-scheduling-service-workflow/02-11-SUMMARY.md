---
phase: 02-scheduling-service-workflow
plan: 11
subsystem: visit
tags: [visit, temporal, workflow, billing, sweeper, schedule, idempotency, worker]

# Dependency graph
requires:
  - phase: 02-05
    provides: "Billing POST /internal/billing/records/{appointment_id}/transitions (visit may target only finalized; same Idempotency-Key = no-op), POST /internal/billing/prechecks"
  - phase: 02-06
    provides: "libs.temporal connect_client / start_once, libs.temporal_policies VISIT_TASK_QUEUE + presets, tests/temporal_fixtures.py (temporal_env)"
  - phase: 02-07
    provides: "Schedule registration pattern (ensure_schedules), one-activity maintenance workflow pattern, worker entrypoint shape"
  - phase: 02-10
    provides: "Visit service, visits.downstream_started_at + partial ix_visits_downstream_pending, _transition APPLIED branch, tests/visit/conftest helpers"
provides:
  - "services/visit/domain/contracts.py: VisitCompletedInput, VisitCompletedResult, SweepResult, VISIT_COMPLETED_WORKFLOW, SWEEPER_WORKFLOW, ACT_FINALIZE_BILLING / ACT_FOLLOWUP_INTENT / ACT_ANALYTICS_INTENT / ACT_SWEEP, visit_completed_workflow_id"
  - "VisitCompletedWorkflow (workflows/visit_completed.py) and VisitDownstreamSweeperWorkflow (workflows/sweeper.py) on visit-tq"
  - "VisitActivities(session_factory, http, tokens, mongo, client) in activities/downstream.py"
  - "services/visit/downstream.py: ensure_downstream_started(client, db, visit), sweep_unstarted(session_factory, client, *, grace_seconds, limit=100)"
  - "visit_repo.mark_downstream_started / list_downstream_unstarted"
  - "services/visit/schedules.ensure_visit_schedules(client, *, id_prefix='visit')"
  - "python -m services.visit.worker (WORKFLOWS list, heartbeat /tmp/visit-worker.alive, schedule registration, start-up sweep)"
  - "api.deps.get_temporal_client; app.state.temporal_client; completion hook in POST /api/visits/{id}/complete"
  - "VisitSettings.sweeper_interval_seconds (60) / sweeper_grace_seconds (30)"
  - "Fixtures: running_visit_worker / visit_worker (tests/temporal_fixtures.py), autouse _visit_temporal_down + _DownTemporal (tests/visit/conftest.py), confirmed_appointment(..., with_billing=True, mesh=)"
affects: [02-12]

actuals:
  tokens: 110000
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Commit-then-start with a NULL marker: transition commits, start_once(deterministic id) runs, then a conditional marker UPDATE; a sweeper (start-up + Temporal Schedule) recovers lost starts"
    - "Fan-out workflow: asyncio.gather over branches that each catch ActivityError and report skipped:<ErrorType>, so no branch blocks another"

key-files:
  created:
    - services/visit/domain/contracts.py
    - services/visit/workflows/__init__.py
    - services/visit/workflows/visit_completed.py
    - services/visit/workflows/sweeper.py
    - services/visit/activities/__init__.py
    - services/visit/activities/downstream.py
    - services/visit/downstream.py
    - services/visit/schedules.py
    - services/visit/worker.py
    - tests/visit/test_downstream.py
    - tests/workflows/test_visit_schedules.py
    - tests/workflows/test_visit_completed.py
    - tests/workflows/histories/visit_completed_v1.json
  modified:
    - services/visit/config.py
    - services/visit/app.py
    - services/visit/api/deps.py
    - services/visit/api/visits.py
    - services/visit/repository/visit_repo.py
    - tests/temporal_fixtures.py
    - tests/visit/conftest.py
    - tests/workflows/test_contracts.py
    - tests/workflows/test_replay.py

key-decisions:
  - "The completion hook resolves the Temporal client INSIDE its try (not as a FastAPI Depends), so even a client-construction failure cannot turn a committed completion into an error; any exception logs visit.downstream_start_deferred (visit id + error type) and the route returns 200"
  - "ensure_downstream_started commits the marker itself (it is a service-level helper, not repository code) and marks even when start_once returns False (the run already closed, so downstream work happened)"
  - "Billing finalize: 404 -> 'no_record' (success, nothing to finalize); 409 -> non-retryable ApplicationError type BillingNotFinalizable; everything else propagates and is retried under COMPENSATION_ACTIVITY. NON_RETRYABLE in libs/temporal_policies was left unchanged (the error is raised with non_retryable=True)"
  - "Worker exits 1 if the Schedule cannot be registered (mirrors Scheduling); the start-up sweep failure is only logged (visit.startup_sweep_failed)"

patterns-established:
  - "Visit tests run with Temporal 'down' by default (autouse _visit_temporal_down in tests/visit/conftest.py); use the visit_worker fixture, or set harness_apps()['visit'].state.temporal_client yourself (the autouse fixture restores it)"

requirements-completed: [VISIT-04, BILL-02]

coverage:
  - id: D1
    description: "Tracer: complete through the gateway -> 200 completed; visit-completed-{id} finalizes billing, records both intents; marker set; one followup.requested and one analytics.intent_recorded audit"
    requirement: VISIT-04, BILL-02
    test: tests/visit/test_downstream.py::test_complete_returns_200_and_downstream_finalizes_billing
  - id: D2
    description: "Non-blocking: no worker on visit-tq -> 200, workflow RUNNING, billing still verified, marker set"
    requirement: VISIT-04
    test: tests/visit/test_downstream.py::test_complete_does_not_wait_for_downstream_work
  - id: D3
    description: "Lost trigger: Temporal down -> 200 with marker NULL; sweep inside grace skips it; after aging completed_at by SQL the sweep starts it, billing finalized, one finalize audit after a second sweep"
    requirement: VISIT-04
    test: tests/visit/test_downstream.py::test_lost_trigger_is_recovered_by_the_sweep_exactly_once
  - id: D4
    description: "ensure_visit_schedules creates {prefix}-downstream-sweeper (SKIP, interval, visit-tq) then is a no-op; sweeper workflow runs ACT_SWEEP once"
    requirement: VISIT-04
    test: tests/workflows/test_visit_schedules.py
  - id: D5
    description: "Branch independence with stubs; Visit contracts round-trip ids-only; visit_completed_v1.json replays and is free of coverage/Bearer/DENY-"
    requirement: VISIT-04, BILL-02
    test: tests/workflows/test_visit_completed.py, tests/workflows/test_contracts.py, tests/workflows/test_replay.py
---

# Phase 2 Plan 11: Visit Completion Downstream Summary

**Completing a visit commits the transition and returns 200 at once. After the commit the API starts `visit-completed-{visit_id}` on `visit-tq` (start-once) and sets `downstream_started_at`. `VisitCompletedWorkflow` finalizes the appointment's billing record (`verified -> finalized`) and records follow-up and analytics intent stubs, as independent branches. If Temporal is unreachable, the completion still answers 200 with the marker NULL. A sweeper finds those visits and starts their workflow; it runs once when the new visit worker starts and then on the `visit-downstream-sweeper` Temporal Schedule.**

## Performance
- Tasks: 3 of 3
- Files: 13 created, 9 modified

## Accomplishments
- Workflow: three branches under `asyncio.gather`. Billing uses `COMPENSATION_ACTIVITY` and the intents use `NOTIFY_ACTIVITY`. Each branch reports `finalized` / `no_record` / `recorded` or `skipped:<ErrorType>`. There is no run timeout.
- Billing finalize: an aud=billing token for `service:visit`, `Idempotency-Key {workflow_id}:finalize`, `reason_code visit_completed`. The audit `visit.billing_finalize_requested` (dedupe `{workflow_id}:finalize`) is written last.
- Intents: log lines `followup.requested` / `analytics.visit_completed` and audits `followup.requested` / `analytics.intent_recorded` (ids only). These are the Phase 3 seams.
- Sweeper: `list_downstream_unstarted` uses the partial index and holds no row locks across Temporal calls. Each visit is started in its own short transaction, and a failure on one visit is logged (`visit.sweep_start_failed`) without stopping the sweep. The batch limit is 100.
- Worker smoke against the Compose Temporal: it registered `visit-downstream-sweeper`, ran the start-up sweep (0 scanned), touched `/tmp/visit-worker.alive` and exited 0 on SIGINT. I deleted the schedule afterwards so Compose is clean.

## Task Commits
1. **Task 1: Tracer, non-blocking completion and downstream fan-out** - `aaf56aa` (feat)
2. **Task 2: Sweeper Schedule idempotency and sweeper workflow** - `a24eddc` (test)
3. **Task 3: Branch independence, contracts, replay history** - `4f7ca91` (test)

**Plan metadata:** see the `docs(02-11): complete plan summary` commit

## Deviations from Plan

**1. [Real code] Fixtures live in `tests/temporal_fixtures.py`, not `tests/workflows/fixtures.py`.** That module is the shared plugin since 02-06. `running_visit_worker` / `visit_worker` were added there.

**2. [Commit grouping] The sweeper workflow, `schedules.py` and the worker's schedule registration and start-up sweep landed in the Task 1 commit.** The worker (a Task 1 file) imports them, and the plan's Task 1 tracer already needed the sweep. Task 2's commit carries its tests.

**3. [Test hygiene] An autouse `_visit_temporal_down` fixture in `tests/visit/conftest.py`.** Without it, every existing visit test that completes a visit would lazily connect to the Compose Temporal and leave unpolled `visit-completed-*` workflows there. In those tests the start is now deferred and the marker stays NULL (their visit rows are cleaned up anyway).

**4. [Signature] `confirmed_appointment(..., with_billing=True, mesh=...)`.** It needs the mesh to call Billing's pre-check, so `mesh` is a keyword argument.

**5. [Hook placement] The completion hook takes `request` and resolves the Temporal client inside its own `try`, rather than through a `Depends(get_temporal_client)`.** This guarantees a 200 after a committed completion. `get_temporal_client` still exists in `api/deps.py`, same shape as Scheduling's.

**6. [Scope cut, user instruction UPDATE 2] Only minimal tests were written; the rest are deferred (below).** Two test-count acceptance criteria are not met: Task 2's "`def test_` in test_downstream.py >= 8" (there are 3) and Task 3's ">= 4 in test_visit_completed.py" (there are 2).

**7. [Human-verify] temporalio 1.34.0 was already approved in 02-06.** No new packages.

## Deferred tests
Deferred at the user's request (UPDATE 2). Planned names:
- tests/visit/test_downstream.py
  - `test_grace_window_skips_recent_completion` (partly covered inside the lost-trigger test)
  - `test_repeat_complete_after_success_is_noop_single_run`
  - `test_repeat_complete_with_null_marker_starts_workflow`
  - `test_concurrent_sweeps_start_one_run_one_finalization`
  - `test_voided_billing_reports_not_finalizable_and_intents_still_recorded`
  - `test_deferred_start_logs_downstream_start_deferred`
- tests/workflows/test_visit_completed.py
  - `test_followup_failing_every_attempt_is_skipped_and_others_succeed`
  - `test_billing_retryable_failures_are_absorbed`

## Verification
- Task 1 verify `pytest tests/visit/test_downstream.py -x`: 3 passed (16 s). Whole `tests/visit`: 32 passed (2 min 42 s).
- Task 2 verify `pytest tests/visit/test_downstream.py tests/workflows/test_visit_schedules.py`: all passed. The Compose Temporal was already healthy, so `docker compose up --wait` was not re-run.
- Task 3: `pytest tests/workflows/test_replay.py test_visit_completed.py test_contracts.py test_visit_schedules.py`: 85 passed, 4 skipped (the history recorders). The fixture was recorded with `SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay.py -k record_visit`.
- `tests/libs/test_service_isolation.py`: 14 passed.
- Acceptance greps: all pass except the two deferred test-count criteria (deviation 6).
- The full regression (`pytest tests -m "not e2e"`) and `pytest tests/workflows` as a whole were NOT run; the full regression was deferred at the user's request.
- Semgrep: not run (not installed in executor environment).
