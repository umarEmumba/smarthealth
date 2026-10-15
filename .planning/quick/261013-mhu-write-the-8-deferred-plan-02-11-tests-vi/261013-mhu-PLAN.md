---
phase: 261013-mhu-write-the-8-deferred-plan-02-11-tests-vi
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - tests/visit/test_downstream.py
  - tests/workflows/test_visit_completed.py
autonomous: true
requirements: [VISIT-04, BILL-02]

estimate:
  tokens: 45000
  raw_tokens: 45000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "A completed visit whose marker is NULL and that completed 5 s ago is left alone by a sweep with a 30 s grace (no workflow exists), and is started by a sweep with a 1 s grace (D-16)"
    - "Repeating POST /api/visits/{id}/complete after the downstream workflow finished is a 200 no-op: same run id, marker unchanged, exactly one finalized billing history row, one completed visit history row (D-06, D-16)"
    - "Repeating POST /api/visits/{id}/complete while the marker is still NULL (first start lost) starts visit-completed-{id}, sets the marker, and billing ends finalized exactly once (D-16)"
    - "Two concurrent sweep_unstarted calls over the same NULL-marker visit produce one run id and one finalized billing history row (D-06 deterministic id, start_once)"
    - "A voided billing record makes the workflow report billing 'skipped:BillingNotFinalizable' while followup and analytics are still 'recorded' (BILL-02, D-11)"
    - "When Temporal is down at completion the API answers 200 and emits exactly one warning log event visit.downstream_start_deferred carrying the visit id and the error TYPE only (D-16)"
    - "In the workflow, a follow-up activity failing every attempt is reported as skipped while billing and analytics succeed; billing failing twice with a retryable error then succeeding reports 'finalized' (VISIT-04, BILL-02)"
    - "No file under services/ or libs/ is modified"
  artifacts:
    - path: tests/visit/test_downstream.py
      provides: "9 tests: the existing 3 plus the 6 deferred downstream tests"
      contains: "test_concurrent_sweeps_start_one_run_one_finalization"
    - path: tests/workflows/test_visit_completed.py
      provides: "4 tests: the existing 2 plus the 2 deferred workflow branch tests"
      contains: "test_billing_retryable_failures_are_absorbed"
  key_links:
    - from: tests/visit/test_downstream.py
      to: services/visit/downstream.py
      via: "sweep_unstarted(factory, temporal_env.client, grace_seconds=...) called directly and concurrently via asyncio.gather"
      pattern: "sweep_unstarted"
    - from: tests/visit/test_downstream.py
      to: services/visit/api/visits.py
      via: "monkeypatch of the module-level structlog logger `log` so the visit.downstream_start_deferred line lands in capsys"
      pattern: "visit.downstream_start_deferred"
    - from: tests/workflows/test_visit_completed.py
      to: services/visit/workflows/visit_completed.py
      via: "stub activities registered under ACT_* names on visit-tq in the time-skipping env"
      pattern: "NOTIFY_ACTIVITY"
---

<objective>
Write the 8 tests deferred from plan 02-11 (02-11-SUMMARY.md, "Deferred tests" and deviation 6), exactly under the planned names, by appending to the two existing test files:

- tests/visit/test_downstream.py (6): test_grace_window_skips_recent_completion, test_repeat_complete_after_success_is_noop_single_run, test_repeat_complete_with_null_marker_starts_workflow, test_concurrent_sweeps_start_one_run_one_finalization, test_voided_billing_reports_not_finalizable_and_intents_still_recorded, test_deferred_start_logs_downstream_start_deferred
- tests/workflows/test_visit_completed.py (2): test_followup_failing_every_attempt_is_skipped_and_others_succeed, test_billing_retryable_failures_are_absorbed

Purpose: Close the two unmet test-count acceptance criteria of 02-11 (test_downstream.py >= 8 `def test_`, test_visit_completed.py >= 4) and prove the D-16 / D-06 idempotency and branch-independence claims that the 02-11 must_haves state (repeated completes, repeated and concurrent sweeps give one run and one finalization; one failing branch never blocks the others).

Output: the two test files with 9 and 4 tests. TESTS ONLY: no file under services/ or libs/ changes. If a test exposes a real production defect, the executor stops and records it as a finding in the SUMMARY (see the Findings rule in Task 2).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/phases/02-scheduling-service-workflow/02-11-SUMMARY.md
@.planning/phases/02-scheduling-service-workflow/02-11-PLAN.md
@tests/visit/test_downstream.py
@tests/workflows/test_visit_completed.py
@tests/visit/conftest.py
@tests/temporal_fixtures.py
@services/visit/downstream.py
@services/visit/workflows/visit_completed.py
@services/visit/activities/downstream.py
@services/visit/api/visits.py
@libs/temporal_policies.py

Hard rules for the executor:
- Never read .env files (or any secrets file). Tests take settings from the already-loaded config objects only.
- Do not modify anything under services/ or libs/. Do not modify tests/visit/conftest.py, tests/temporal_fixtures.py or tests/billing/conftest.py; import from them.
- Leave the existing 3 tests in test_downstream.py and the existing 2 in test_visit_completed.py unchanged. Adding new private helpers to the two test files is fine; extending `_stubs` in test_visit_completed.py with NEW keyword-only parameters whose defaults keep today's behaviour is fine.
- Infra: the local Compose stack (Postgres, Mongo, Redis, Temporal) is up and migrated. Do not run docker compose down or any migration.

## Interfaces already available (do not re-derive)

tests/visit/test_downstream.py (module-private helpers):
- `_completed_visit(client, mesh, vw) -> (visit: dict, appointment_id: str, completed: httpx.Response)` — confirmed appointment WITH a verified billing record, check-in, start, complete through the gateway.
- `_billing_status(client, vw, appointment_id) -> str` — GET /api/billing/records as the world's front desk.
- `_marker(visit_id) -> datetime | None` — reads visits.downstream_started_at.
- `_result(env, visit_id) -> VisitCompletedResult` — awaits `visit-completed-{id}` inside `env.time_skipping_unlocked()`.
- Already imported: httpx, AsyncClient, text, create_async_engine, WorkflowExecutionStatus, WorkflowEnvironment, get_session_factory, visit_settings, VisitCompletedResult, visit_completed_workflow_id, sweep_unstarted, harness_apps, auth, VisitWorld, audit_count, check_in, confirmed_appointment.

tests/visit/conftest.py:
- `confirmed_appointment(vw, *, status="confirmed", provider_id=None, with_billing=False, mesh=None) -> str`
- `check_in(client, tokens, appointment_id) -> httpx.Response`, `audit_count(entity_id, action) -> int`, `history_rows(visit_id) -> list[tuple[from_status, to_status]]`
- `_DownTemporal` (its `start_workflow` raises `temporalio.service.RPCError("temporal unavailable", UNAVAILABLE)`).
- autouse `_visit_temporal_down`: sets the visit app's `state.temporal_client = _DownTemporal()` for EVERY test in tests/visit and restores it after. Autouse fixtures are set up before requested ones, so in a test that requests `visit_worker`, `app.state.temporal_client` is `temporal_env.client` (the worker fixture overrode it). To simulate "Temporal down at completion" inside such a test, copy the lost-trigger test's pattern: save `app.state.temporal_client`, assign `_DownTemporal()`, run `_completed_visit`, restore in `finally`.

tests/billing/conftest.py:
- `service_token(mesh, service, audience="billing") -> str`
- `transition(mesh, token, appointment_id, target, *, key=None, reason_code=None) -> httpx.Response` — Scheduling may target `voided` from `verified` (services/billing/domain/states.py SERVICE_TRANSITION_TARGETS); `voided` is terminal, so a later `finalized` gets 409, which VisitActivities.finalize_billing maps to non-retryable `BillingNotFinalizable`.

tests/temporal_fixtures.py fixtures: `temporal_env` (fresh time-skipping server per test), `visit_worker` (real Worker on visit-tq, VisitActivities wired to the mesh, swaps the visit app's temporal client to `temporal_env.client` for its block), `mongo_client`.

Production behaviour the tests pin (read-only):
- `sweep_unstarted(session_factory, client, *, grace_seconds, limit=100) -> int` scans ALL of visit_db (`status='completed' AND downstream_started_at IS NULL AND completed_at < now() - make_interval(secs => grace)`), oldest first. Never assert an exact processed count; assert on THIS visit (marker, workflow handle) and use `>= 1` where a count is needed.
- `ensure_downstream_started` -> `start_once` (USE_EXISTING + REJECT_DUPLICATE) then a conditional marker UPDATE + commit; it returns True also when the run already closed.
- The completion route only re-attempts the start when `visit.downstream_started_at is None`; on any exception it rolls back and logs `log.warning("visit.downstream_start_deferred", visit_id=..., error=type(exc).__name__)` on the module-level `log` of services/visit/api/visits.py.
- The database clock never skips under the time-skipping env (research Pitfall 8): age `completed_at` with SQL.
- NOTIFY_ACTIVITY: maximum_attempts 3, schedule_to_close 30 s. COMPENSATION_ACTIVITY: maximum_attempts 20, schedule_to_close 10 min. Both have `non_retryable_error_types=NON_RETRYABLE` = ["SlotUnavailable", "BillingRejected", "ReferenceInvalid", "ConfirmLostHold"].
- `failure_reason(err)` returns `ApplicationError.type` when the cause carries one. A plain Python exception raised in an activity reaches the workflow as an ApplicationError typed with its class name, so the 02-11 spec wording "skipped:activity_failed" is not what a raised exception produces; the tests below use explicitly typed ApplicationErrors so the expected strings are exact. This is documented behaviour ("skipped:<ErrorType>"), not a defect.
</context>

<tasks>

<task type="tracer">
  <name>Task 1: Tracer — repeat complete with a NULL marker starts the downstream workflow end to end</name>
  <files>tests/visit/test_downstream.py</files>
  <read_first>
    - tests/visit/test_downstream.py (whole file: helpers and the lost-trigger test's _DownTemporal swap pattern)
    - tests/visit/conftest.py lines 168-206 (audit_count, history_rows) and 246-269 (_DownTemporal, autouse fixture)
    - services/visit/api/visits.py lines 307-368 (_start_downstream, _transition: the NULL-marker re-attempt branch)
    - tests/billing/test_transitions.py lines 91-110 (lazy `from services.billing.config import settings as billing_settings  # noqa: PLC0415` + engine pattern)
  </read_first>
  <action>
    Per D-16 (commit-then-start; a repeated complete while the marker is NULL retries the start) and D-06 (deterministic id), append ONE test, test_repeat_complete_with_null_marker_starts_workflow, plus one helper. This single test crosses every layer the plan touches: gateway -> Visit API -> Temporal start -> visit worker -> Billing transition -> Mongo audit -> visit_db marker.

    1. Helper `_finalized_rows(appointment_id: str) -> int` placed with the other private helpers (after `_marker`): lazily import Billing's settings inside the function exactly like tests/billing/test_transitions.py does (`from services.billing.config import settings as billing_settings` with `# noqa: PLC0415`), open `create_async_engine(billing_settings.database_url)`, run `SELECT count(*) FROM billing_status_history h JOIN billing_records r ON r.id = h.record_id WHERE r.appointment_id = :a AND h.to_status = 'finalized'`, dispose the engine in `finally`. One-line docstring: the number of finalize moves Billing recorded for the appointment.
    2. Test signature: `client: AsyncClient, mesh: httpx.AsyncClient, visit_world: VisitWorld, visit_worker: object, temporal_env: WorkflowEnvironment`. Docstring (2-3 lines, same tone as the existing ones): the first start was lost (Temporal down) so the marker is NULL; a repeated complete is a no-op transition but retries the start, and the workflow then finalizes billing once.
    3. Body: `app = harness_apps()["visit"]`; save `app.state.temporal_client`; import `_DownTemporal` the same way the lost-trigger test does (local import with `# noqa: PLC0415`) or add it to the existing `from tests.visit.conftest import ...` line (also add `history_rows` there); assign `_DownTemporal()`; call `_completed_visit` inside try; restore in finally. Assert 200, status "completed", `_marker` is None.
    4. Repeat: POST `/api/visits/{id}/complete` with `auth(visit_world.world.provider)` -> 200 and `json()["status"] == "completed"`. Then assert: `_marker` is not None; `_result(temporal_env, id)` equals `VisitCompletedResult(outcome="completed", billing="finalized", followup="recorded", analytics="recorded")`; `_billing_status` == "finalized"; `_finalized_rows(appointment_id) == 1`; exactly one `history_rows` entry whose to_status is "completed"; `audit_count(id, "visit.complete") == 1`.
    Match the file's style: `from __future__ import annotations` already present, type hints on helpers, short comments only where the step is non-obvious (e.g. one comment on the swap: "Temporal down for the first completion only"). No production edits.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit/test_downstream.py -q -p no:cacheprovider -k "null_marker or lost_trigger"</automated>
    <fails_when>non-zero exit, any "failed"/"error" in the summary line, or fewer than "2 passed"</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -c "^async def test_repeat_complete_with_null_marker_starts_workflow" tests/visit/test_downstream.py` returns 1
    - `grep -c "^async def _finalized_rows" tests/visit/test_downstream.py` returns 1
    - `git status --porcelain -- services libs` prints nothing
    - The verify command exits 0 with 2 passed
  </acceptance_criteria>
  <done>The NULL-marker repeat-complete path is proven end to end (run started on the second complete, one finalization) and committed as `test(quick-261013-mhu): ...`.</done>
</task>

<task type="auto">
  <name>Task 2: Remaining five downstream tests — grace window, repeat after success, concurrent sweeps, voided billing, deferred-start log</name>
  <files>tests/visit/test_downstream.py</files>
  <read_first>
    - tests/workflows/test_maintenance.py lines 336-358 (run_id stability and RPCError NOT_FOUND assertions — copy this pattern)
    - tests/gateway/test_ratelimit.py lines 1-30 (imports) and 230-263 (configure_logging + monkeypatch the module `log` with a fresh structlog logger + parse capsys JSON lines)
    - libs/observability/logging.py lines 39-75 (PrintLoggerFactory with cache_logger_on_first_use=True: a module logger first used in an earlier test stays bound to that test's stdout, which is why the monkeypatch is required)
    - tests/billing/conftest.py lines 40-90 (service_token, transition)
    - services/visit/repository/visit_repo.py lines 193-214 (list_downstream_unstarted SQL, make_interval with an int bind)
  </read_first>
  <action>
    Per D-16 and D-06 (and BILL-02 for the voided case), append the five remaining tests to tests/visit/test_downstream.py after the Task 1 test, plus one helper. Add needed imports at the top in the file's existing import groups: `asyncio`, `json`, `pytest`, `structlog`, `from temporalio.service import RPCError, RPCStatusCode`, `from libs.observability.logging import configure_logging`, `from services.visit.api import visits as visits_api`. Import `service_token` and `transition` from tests.billing.conftest lazily inside the voided test (mirrors tests/visit/conftest.py), aliasing `transition` as `billing_transition` to avoid confusion with visit transitions.

    Helper `_age_completion(visit_id: str, seconds: int) -> None`: one short `engine.begin()` transaction running `UPDATE visits SET completed_at = now() - make_interval(secs => :s) WHERE id = :v` (int bind, same form the repository uses), engine disposed in finally. Comment: the database clock never skips (Pitfall 8).

    A. test_grace_window_skips_recent_completion (fixtures: client, mesh, visit_world, temporal_env; NO visit_worker, so the autouse _DownTemporal is in effect at completion). Complete via `_completed_visit` -> 200, marker None. `_age_completion(id, 5)` (completed 5 s ago). `factory = get_session_factory(visit_settings.database_url)`; `await sweep_unstarted(factory, temporal_env.client, grace_seconds=30)`; assert marker still None and `temporal_env.client.get_workflow_handle(visit_completed_workflow_id(id)).describe()` raises RPCError with `status == RPCStatusCode.NOT_FOUND` (pytest.raises). Positive control so the skip is attributable to the grace window and nothing else: sweep again with `grace_seconds=1`; assert marker is not None and the handle's `describe().status == WorkflowExecutionStatus.RUNNING` (no worker polls visit-tq); then `terminate(reason="test clean-up")` as the non-blocking test does.

    B. test_repeat_complete_after_success_is_noop_single_run (client, mesh, visit_world, visit_worker, temporal_env). `_completed_visit` -> 200; `_result` -> billing "finalized"; `marker = await _marker(id)`; `first_run = (await handle.describe()).run_id`. Repeat POST complete -> 200, status "completed". Assert: `_marker(id) == marker` (unchanged timestamp), `(await handle.describe()).run_id == first_run`, `_finalized_rows(appointment_id) == 1`, exactly one history row to "completed", `audit_count(id, "visit.billing_finalize_requested") == 1`.

    C. test_concurrent_sweeps_start_one_run_one_finalization (client, mesh, visit_world, visit_worker, temporal_env). Complete with the _DownTemporal swap (as in Task 1) -> marker None. `_age_completion(id, 60)`. `results = await asyncio.gather(sweep_unstarted(factory, temporal_env.client, grace_seconds=30), sweep_unstarted(factory, temporal_env.client, grace_seconds=30))`; assert `sum(results) >= 1` (both may count this visit: USE_EXISTING attaches and the conditional marker UPDATE is a no-op for the loser; other stale rows in visit_db may also be counted). Assert marker not None; `first_run = describe().run_id`; `_result` -> billing "finalized"; `describe().run_id == first_run` after completion; `_finalized_rows == 1`; `audit_count(id, "visit.billing_finalize_requested") == 1`; `_billing_status == "finalized"`. A short comment explains why the run-id check (not the processed count) is the one-run proof.

    D. test_voided_billing_reports_not_finalizable_and_intents_still_recorded (client, mesh, visit_world, visit_worker, temporal_env). Do NOT use `_completed_visit` (billing must be voided BEFORE completion, while the worker is already running). Inline: `appointment_id = await confirmed_appointment(visit_world, with_billing=True, mesh=mesh)`; `token = await service_token(mesh, "scheduling")`; `resp = await billing_transition(mesh, token, appointment_id, "voided", reason_code="booking_compensated")` -> 200 and status "voided"; then check_in (201), start (200), complete (200) through `client` with the same headers `_completed_visit` uses. Assert `_result` equals `VisitCompletedResult(outcome="completed", billing="skipped:BillingNotFinalizable", followup="recorded", analytics="recorded")`; `_billing_status == "voided"`; `_finalized_rows == 0`; `audit_count(id, "visit.billing_finalize_requested") == 0`; `audit_count(id, "followup.requested") == 1`; `audit_count(id, "analytics.intent_recorded") == 1`; marker not None.

    E. test_deferred_start_logs_downstream_start_deferred (client, mesh, visit_world, monkeypatch, capsys; NO temporal_env, NO visit_worker: the autouse _DownTemporal provides the outage). `configure_logging()`; `monkeypatch.setattr(visits_api, "log", structlog.get_logger("test-visit-deferred"))` with a one-line comment why (cached logger bound to an earlier test's stdout); `capsys.readouterr()` to discard. `_completed_visit` -> 200, status "completed"; marker None. Parse `capsys.readouterr().out.splitlines()` with `json.loads`, skipping JSONDecodeError lines (as test_ratelimit does); `deferred = [e for e in events if e.get("event") == "visit.downstream_start_deferred"]`; assert `len(deferred) == 1`, `level == "warning"`, `visit_id == visit["id"]`, `error == "RPCError"`; and ids-only hygiene: the serialized event contains neither the appointment id nor the RPC error's message text (the error TYPE only is logged).

    Findings rule (applies to every test in Tasks 1-3): if a test fails, first rule out test-side causes (fixture order, the DB clock, stale rows from other tests, time skipping). If the cause is a genuine production defect (behaviour contradicts the 02-11 contract above), STOP: do not edit services/ or libs/, do not mark the test xfail or skip it; record under a "Findings" heading in the SUMMARY the test name, expected vs observed, and the suspected production location, and return to the user.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit/test_downstream.py -q -p no:cacheprovider</automated>
    <fails_when>non-zero exit, any "failed"/"error" in the summary line, or fewer than "9 passed"</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -cE "^async def test_(grace_window_skips_recent_completion|repeat_complete_after_success_is_noop_single_run|concurrent_sweeps_start_one_run_one_finalization|voided_billing_reports_not_finalizable_and_intents_still_recorded|deferred_start_logs_downstream_start_deferred)\(" tests/visit/test_downstream.py` returns 5
    - `grep -c "^async def test_" tests/visit/test_downstream.py` returns 9
    - `grep -c "asyncio.gather" tests/visit/test_downstream.py` returns at least 1
    - `git status --porcelain -- services libs` prints nothing
    - The verify command exits 0 with 9 passed
  </acceptance_criteria>
  <done>All six deferred downstream tests exist under their planned names and pass against the live Compose infra; committed as `test(quick-261013-mhu): ...`.</done>
</task>

<task type="auto">
  <name>Task 3: Workflow branch tests — follow-up failing every attempt, billing retryable failures absorbed</name>
  <files>tests/workflows/test_visit_completed.py</files>
  <read_first>
    - tests/workflows/test_visit_completed.py (whole file: _input, _run, _stubs)
    - services/visit/workflows/visit_completed.py (failure_reason, _branch, which preset each branch uses)
    - libs/temporal_policies.py (NOTIFY_ACTIVITY / COMPENSATION_ACTIVITY retry policies, NON_RETRYABLE)
  </read_first>
  <action>
    Per VISIT-04 / BILL-02 and the 02-11 branch-independence design (D-16; each branch catches its own ActivityError), extend `_stubs` and append two tests.

    1. Extend `_stubs(calls, *, billing_error=None)` with two NEW keyword-only parameters, defaults preserving current behaviour: `billing_failures: int = 0` (the billing stub raises a RETRYABLE `ApplicationError("billing unavailable", type="BillingUnavailable")` while `calls.count("billing") <= billing_failures`, i.e. on its first N attempts, then returns "finalized") and `followup_error: Exception | None = None` (the follow-up stub raises it on every call after appending to `calls`). The append to `calls` stays the first statement of each stub so attempts are counted. Error messages carry no values. Do not use a type listed in NON_RETRYABLE for these errors.
    2. test_followup_failing_every_attempt_is_skipped_and_others_succeed: `followup_error=ApplicationError("follow-up unavailable", type="FollowupUnavailable")` (retryable). Assert the result equals `VisitCompletedResult(outcome="completed", billing="finalized", followup="skipped:FollowupUnavailable", analytics="recorded")`; `calls.count("followup") == NOTIFY_ACTIVITY["retry_policy"].maximum_attempts` (import NOTIFY_ACTIVITY from libs.temporal_policies next to VISIT_TASK_QUEUE; do not hard-code 3); billing and analytics each called once. One comment: retries back off 1 s then 2 s, well inside the 30 s schedule-to-close, so the attempt cap (not the timeout) ends the branch and time skipping makes it instant.
    3. test_billing_retryable_failures_are_absorbed: `billing_failures=2`. Assert the result equals `VisitCompletedResult(outcome="completed", billing="finalized", followup="recorded", analytics="recorded")`; `calls.count("billing") == 3`; followup and analytics each once. One comment: COMPENSATION_ACTIVITY retries (up to 20 attempts) absorb transient Billing trouble.
    Keep the existing two tests passing unchanged; the file's module docstring stays as is.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/workflows/test_visit_completed.py -q -p no:cacheprovider</automated>
    <fails_when>non-zero exit, any "failed"/"error" in the summary line, or fewer than "4 passed"</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -cE "^async def test_(followup_failing_every_attempt_is_skipped_and_others_succeed|billing_retryable_failures_are_absorbed)\(" tests/workflows/test_visit_completed.py` returns 2
    - `grep -c "^async def test_" tests/workflows/test_visit_completed.py` returns 4
    - `grep -c "NOTIFY_ACTIVITY" tests/workflows/test_visit_completed.py` returns at least 1
    - `git status --porcelain -- services libs` prints nothing
    - The verify command exits 0 with 4 passed
  </acceptance_criteria>
  <done>Both deferred workflow tests exist under their planned names and pass; then the plan-level command `.venv/bin/python -m pytest tests/visit tests/workflows -q -p no:cacheprovider` passes with no failures; committed as `test(quick-261013-mhu): ...`.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| test process -> shared local Compose infra | tests write synthetic rows to visit_db / billing_db / Mongo audit_log and start workflows |
| test log capture | captured JSON log lines are asserted on and printed on failure |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-261013-01 | Information Disclosure | test_deferred_start_logs_downstream_start_deferred | low | mitigate | Test asserts the deferred-start log carries the visit id and the error type only (no appointment id, no error message), pinning T-02-58's ids-only logging |
| T-261013-02 | Tampering | shared visit_db / Temporal state across tests | low | mitigate | Workflows run on the per-test time-skipping server (never the Compose Temporal; autouse _DownTemporal stays in force); rows are cleaned by the visit_world fixture; sweeps assert on the test's own visit, not global counts; the one RUNNING workflow is terminated |
| T-261013-03 | Information Disclosure | test data | low | accept | Only synthetic seeded worlds (seed_bookable_world); no real patient or client data; .env files are never read |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/visit tests/workflows -q -p no:cacheprovider` passes (all tests; record the passed/skipped counts in the SUMMARY).
- `grep -c "^async def test_" tests/visit/test_downstream.py` is 9 and `grep -c "^async def test_" tests/workflows/test_visit_completed.py` is 4, so the two 02-11 test-count criteria (>= 8, >= 4) are now met.
- `git status --porcelain -- services libs` prints nothing (tests-only change).
- Semgrep: not required for tests-only changes; record "not run (tests only)" in the SUMMARY unless it is installed, in which case run `semgrep --config auto tests/visit/test_downstream.py tests/workflows/test_visit_completed.py` and record the result.
</verification>

<success_criteria>
- The 8 deferred 02-11 tests exist under exactly the names listed in 02-11-SUMMARY.md and pass against the live local infra.
- Existing tests in both files are unchanged and still pass; no production file changed.
- Any production defect surfaced is recorded as a finding, not fixed.
</success_criteria>

<output>
Create `.planning/quick/261013-mhu-write-the-8-deferred-plan-02-11-tests-vi/261013-mhu-SUMMARY.md` when done. Include: the 8 test names, the verification command output counts, a "Findings" section (or "None"), and a note that 02-11 deviation 6 (deferred tests) is now closed.
</output>
