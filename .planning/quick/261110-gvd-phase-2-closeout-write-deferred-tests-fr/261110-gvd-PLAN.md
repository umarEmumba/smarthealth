---
phase: 261110-gvd-phase-2-closeout-write-deferred-tests-fr
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - tests/visit/test_downstream.py
  - tests/workflows/test_visit_completed.py
  - tests/e2e/test_phase2_flows.py
  - tests/docs/test_requirement_traceability.py
autonomous: true
requirements: [VISIT-04, BILL-02, SCHED-05]

estimate:
  tokens: 40000
  raw_tokens: 40000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "Each of the 8 deferred 02-11 tests exists exactly once under its planned name (6 in tests/visit/test_downstream.py, 2 in tests/workflows/test_visit_completed.py) and passes on the CURRENT code (after the 02.1-09 appointment branch and the 02.1-10 open-ended billing retry) (VISIT-04, BILL-02, D-06, D-16)"
    - "Every other test in tests/visit/test_downstream.py and tests/workflows/test_visit_completed.py still passes in the same run (no drift left behind by Phase 02.1)"
    - "Only 02-11-SUMMARY.md and 02-12-SUMMARY.md list deferred or unwritten tests; the grep evidence is recorded in the SUMMARY, and the only 02-12 items are the optional gateway reschedule e2e and the SKIPPED containerized worker-restart e2e"
    - "When the stack is reachable, a reschedule through the published gateway moves a confirmed appointment to the new slot (status confirmed, version + 1, last history confirmed -> confirmed 'rescheduled'), frees the old slot, books the new one, and keeps billing 'verified' with a verified -> verified 'rescheduled' move and no refund moves (SCHED-05, BILL-02)"
    - "No file under services/, libs/ or infra/ is modified; Docker Compose is never brought up, restarted or stopped by this plan; no .env* or infra/secrets/* file is read"
  artifacts:
    - path: tests/visit/test_downstream.py
      provides: "the 6 deferred downstream tests (already present since quick 261013-mhu), re-verified"
      contains: "test_concurrent_sweeps_start_one_run_one_finalization"
    - path: tests/workflows/test_visit_completed.py
      provides: "the 2 deferred branch tests (already present since quick 261013-mhu), re-verified"
      contains: "test_billing_retryable_failures_are_absorbed"
    - path: tests/e2e/test_phase2_flows.py
      provides: "real-stack reschedule flow through the gateway (only if it passes twice on the live stack)"
      contains: "test_reschedule_moves_the_booking_and_keeps_billing_verified"
  key_links:
    - from: tests/e2e/test_phase2_flows.py
      to: services/scheduling/api/appointments.py
      via: "POST /api/appointments/{id}/reschedule with {new_slot_id} through the published gateway -> 202 operation 'reschedule'; RescheduleWorkflow on the containerized scheduling-worker"
      pattern: "/reschedule"
    - from: tests/docs/test_requirement_traceability.py
      to: tests/e2e/test_phase2_flows.py
      via: "PHASE2_TRACE['SCHED-05'] lists the e2e file once the reschedule flow is committed"
      pattern: "SCHED-05"
---

<objective>
Close out the Phase 2 deferred-test items.

Starting fact (found while planning, verify before acting): the 8 tests deferred by 02-11 deviation 6 are ALREADY written. Quick task 261013-mhu added them (commits 9d522c6, f8f13c3, 84302fe; 158 passed at that time). Phase 02.1-09 and 02.1-10 then changed `VisitCompletedWorkflow` (a fourth `appointment` branch, open-ended billing retry behind `workflow.patched`) and touched both test files. So the remaining work is: re-verify those 8 tests and their neighbours on the current code (fixing only test drift, reporting any real defect), record that no other 02-0x SUMMARY defers a plain test, and, only if it is cheap and reliable, add the 02-12 gateway reschedule e2e. The containerized worker-restart e2e is out of scope (it needs killing containers; the stack is being brought up separately and must not be restarted).

Purpose: Phase 2 verification can treat "deferred tests" as closed with evidence from today's code, not from a run made before Phase 02.1.
Output: a green re-verification run of the 8 tests (plus any drift fixes, each justified), optionally one new e2e test with its SCHED-05 traceability entry, and the quick SUMMARY.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/phases/02-scheduling-service-workflow/02-11-SUMMARY.md
@.planning/phases/02-scheduling-service-workflow/02-12-SUMMARY.md
@.planning/quick/261013-mhu-write-the-8-deferred-plan-02-11-tests-vi/261013-mhu-SUMMARY.md
@tests/visit/test_downstream.py
@tests/workflows/test_visit_completed.py
@tests/e2e/test_phase2_flows.py
@tests/e2e/phase2_stack.py

# Facts the executor relies on (checked while planning):
# - tests/temporal_fixtures.py: `temporal_env` is WorkflowEnvironment.start_time_skipping(), a private
#   Temporal test server, NOT the Compose Temporal, so containerized workers can never steal its tasks.
# - In-process visit tests DO share Postgres `visit_db` (localhost:5433, VisitSettings.database_url) with
#   the containerized visit service. The containerized visit-worker's sweeper runs every 60 s with a 30 s
#   grace, so it could, rarely, pick up a test visit whose completed_at a test aged by SQL.
# - scripts/compose_healthcheck.sh only reads `docker compose ps`; with service names it checks ONLY those.
# - Reschedule contract: POST /api/appointments/{id}/reschedule, body {"new_slot_id"}, BOOKING_ROLES,
#   202 ChangeAccepted{appointment_id, operation:"reschedule", ...}; AppointmentOut has slot_id, version,
#   history[{from_status, to_status, reason}]; the billing record history items carry
#   from_status / to_status / reason_code (see tests/scheduling/test_cancel.py::billing_moves).
# - In-process reference for the e2e assertions: tests/scheduling/test_reschedule.py::
#   test_reschedule_swaps_slots_atomically_and_relinks_billing.
</context>

<tasks>

<task type="tracer">
  <name>Task 1: Re-verify the 8 deferred 02-11 tests end-to-end on the current code (tracer: one test first, then all)</name>
  <files>tests/visit/test_downstream.py, tests/workflows/test_visit_completed.py</files>
  <precondition>Compose postgres, mongo and redis report healthy (`scripts/compose_healthcheck.sh --wait 300 postgres mongo redis` exits 0); the executor only waits on this read-only check and never runs docker compose up/restart/stop.</precondition>
  <read_first>
    - tests/visit/test_downstream.py (helpers `_finalized_rows`, `_age_completion`, and the 6 deferred tests at the `def test_` lines for grace_window, repeat_complete_after_success, repeat_complete_with_null_marker, concurrent_sweeps, voided_billing, deferred_start)
    - tests/workflows/test_visit_completed.py (`_stubs` with `billing_failures` / `followup_error`, and the 2 deferred tests)
    - services/visit/domain/contracts.py (VisitCompletedResult now has the `appointment` field, plan 02.1-09)
    - .planning/quick/261013-mhu-write-the-8-deferred-plan-02-11-tests-vi/261013-mhu-SUMMARY.md (what was written and why)
  </read_first>
  <action>
    Per D-06 and D-16 (02-11's idempotency and lost-trigger guarantees), which these tests prove. Do NOT rewrite, rename or duplicate any of the 8 tests; they exist.
    1. Wait on the precondition check. If it times out, stop and report "infra not ready". Do not start containers yourself.
    2. Existence gate: each of the 8 planned names appears exactly once as an `async def test_...(` line (6 in tests/visit/test_downstream.py, 2 in tests/workflows/test_visit_completed.py). If a name is missing, report it. Re-adding it is in scope, matching the 261013-mhu plan's behavior for that test.
    3. Tracer slice: run `tests/visit/test_downstream.py::test_repeat_complete_with_null_marker_starts_workflow` alone first. It crosses every layer: in-process gateway harness, the Visit API completion hook, visit_db marker, the time-skipping Temporal with the in-process `visit_worker`, and Billing finalize. A pass proves the harness path works end to end before the wider run.
    4. Run the 8 by name (pytest `-k` with the 8 names joined by ` or `), then both files in full (the verify command).
    5. Triage each failure into exactly one class and record it in the SUMMARY:
       (a) Environment. Infra not ready, or cross-talk on the shared visit_db: the containerized visit-worker's sweeper started or marked a test visit first. Symptoms are an unexpected sweep count, a marker already set, or a run id that is not the test's. Rerun that single test once. If it passes, record it as environmental flake with the evidence. If it persists while `visit-worker` is healthy, record it and leave the container alone (no stop/restart).
       (b) Test drift. The test asserts pre-02.1 behavior that 02.1-09/02.1-10 deliberately changed, for example a VisitCompletedResult equality missing `appointment=`, or billing retry semantics now open-ended. Update only the assertion or stub to the documented post-02.1 behavior. Cite the plan (02.1-09 or 02.1-10) in a one-line test comment and in the SUMMARY.
       (c) Real production defect. Make no change under services/ or libs/. Mark that one test `@pytest.mark.xfail(strict=True, reason="BUG: <one line>, see quick 261110-gvd SUMMARY Findings")`, then write a Findings entry: test, observed vs expected, suspected file/function, repro command. Return it to the caller, and never change behavior silently.
    6. Record the other-summaries evidence. List the 02-*-SUMMARY.md files in .planning/phases/02-scheduling-service-workflow/ that mention deferred tests or tests not written (case-insensitive grep). Planning found only 02-11 and 02-12. Paste the command and its output into the SUMMARY, and state that no other 02-0x plan deferred a plain (no-human-step) test.
    Commit only if a test file changed: `test(quick-261110-gvd): ...`. If nothing changed, there is nothing to commit for this task; the run is the deliverable.
  </action>
  <verify>
    <automated>scripts/compose_healthcheck.sh --wait 300 postgres mongo redis && .venv/bin/python -m pytest tests/visit/test_downstream.py tests/workflows/test_visit_completed.py -q -p no:cacheprovider</automated>
    <fails_when>non-zero exit; "failed" or "error" in the pytest summary line; "0 passed"; or any xfail that has no matching Findings entry in the SUMMARY</fails_when>
  </verify>
  <acceptance_criteria>
    - For each of the 8 names, `grep -c "^async def <name>(" <its file>` returns 1 (test_grace_window_skips_recent_completion, test_repeat_complete_after_success_is_noop_single_run, test_repeat_complete_with_null_marker_starts_workflow, test_concurrent_sweeps_start_one_run_one_finalization, test_voided_billing_reports_not_finalizable_and_intents_still_recorded, test_deferred_start_logs_downstream_start_deferred in tests/visit/test_downstream.py; test_followup_failing_every_attempt_is_skipped_and_others_succeed, test_billing_retryable_failures_are_absorbed in tests/workflows/test_visit_completed.py)
    - The `-k` run of the 8 reports "8 passed" (or N passed + xfails that each have a Findings entry)
    - The verify command exits 0
    - `git status --porcelain -- services libs infra` prints nothing
  </acceptance_criteria>
  <done>All 8 deferred 02-11 tests and their file neighbours pass on today's code. Every failure seen is classified and recorded (environment, drift fixed with a citation, or a reported defect). The other-summaries grep evidence is in the SUMMARY.</done>
</task>

<task type="auto">
  <name>Task 2 (optional, keep only if reliable): Real-stack reschedule flow through the published gateway</name>
  <files>tests/e2e/test_phase2_flows.py, tests/docs/test_requirement_traceability.py</files>
  <precondition>The app stack is up: `scripts/compose_healthcheck.sh --wait 300 gateway identity organization provider patient scheduling billing scheduling-worker` exits 0 (read-only check; never bring the stack up, restart or stop it). If this times out, this task is SKIPPED, not failed.</precondition>
  <read_first>
    - tests/e2e/test_phase2_flows.py (fixtures `gateway` / `ew`, `_first_slot`, style of the cancel and booking flows)
    - tests/e2e/phase2_stack.py (`day_slots`, `book`, `get_appointment`, `appointment_in`, `billing_record`, `poll`)
    - tests/scheduling/test_reschedule.py::test_reschedule_swaps_slots_atomically_and_relinks_billing (assertions to mirror) and `history_pairs`
    - tests/docs/test_requirement_traceability.py (PHASE2_TRACE["SCHED-05"])
  </read_first>
  <action>
    Closes the 02-12 "not written" item "an e2e reschedule flow through the gateway" (SCHED-05, BILL-02 relink). Add `test_reschedule_moves_the_booking_and_keeps_billing_verified(gateway, ew)` to tests/e2e/test_phase2_flows.py, after the cancel flow, in the file's existing style (module `pytestmark = pytest.mark.e2e` already applies). Optionally add "SCHED-05" to the module docstring's flow list.
    Steps inside the test:
    1. Take two available slots on `ew.world.last_day` via `day_slots`: old = index 0, new = index 1 (assert at least 2).
    2. `book` old for `ew.patient_ids[0]`, then wait with `appointment_in(..., {"confirmed", "failed"})` and assert confirmed. Keep `before = await get_appointment(...)`.
    3. POST `/api/appointments/{id}/reschedule` with JSON `{"new_slot_id": new}` and `ew.front_desk` headers. Expect 202, `operation == "reschedule"` and `appointment_id` equal to the booked id.
    4. Poll `get_appointment` with `poll` until `slot_id == new`, or until the last history reason starts with `reschedule_failed`. Then assert `slot_id == new`, so a failed swap shows its reason. Also assert `status == "confirmed"`, `version == before["version"] + 1`, and that the last history entry's (from_status, to_status, reason) is ("confirmed", "confirmed", "rescheduled").
    5. Fetch `day_slots(..., status="any")` as an id -> status map. Old must be "available" and new must be "booked". The fresh world has no waitlist entries, so the post-swap waitlist advance leaves the old slot free.
    6. Poll `billing_record` until its last history item has reason_code "rescheduled". Then assert status "verified", the last (from_status, to_status, reason_code) is ("verified", "verified", "rescheduled"), and no history to_status is in {"refund_pending", "refunded", "no_refund"}.
    Synthetic data only (the seed's "Zzyzx E2E Synthetic" patients). Never print tokens.
    Reliability gate: run the verify's pytest command twice in a row. Each seed makes about 7 auth-route calls, so two runs stay under the default 20/min auth budget. Both runs must pass. If a run fails ONLY with a 429, record it as 02-12 deviation 2 (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE) and treat the other pass as sufficient. Classify any other failure as in Task 1 step 5: test bug (fix it), or real defect (report it in Findings and do not commit the test).
    If it is reliable, append "tests/e2e/test_phase2_flows.py" to PHASE2_TRACE["SCHED-05"] and run tests/docs. Then commit both files as `test(quick-261110-gvd): real-stack reschedule flow through the gateway`.
    If the precondition times out, or the test cannot pass twice for environmental reasons, revert both files (`git checkout -- tests/e2e/test_phase2_flows.py tests/docs/test_requirement_traceability.py`). Record "e2e reschedule: not added, <reason>" in the SUMMARY. Do not commit an unproven test.
    The containerized worker-restart e2e stays out of scope; record it in the SUMMARY as skipped by user instruction.
  </action>
  <verify>
    <automated>scripts/compose_healthcheck.sh --wait 300 gateway identity organization provider patient scheduling billing scheduling-worker && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase2_flows.py -m e2e -k reschedule -q -p no:cacheprovider && .venv/bin/python -m pytest tests/docs -q -p no:cacheprovider</automated>
    <fails_when>pytest exits non-zero, or reports "failed"/"error", or "0 passed"/"deselected" with no test selected. Exception: a healthcheck timeout means the task is SKIPPED (files reverted, reason recorded), not failed.</fails_when>
  </verify>
  <acceptance_criteria>
    - If added: `grep -c "^async def test_reschedule_moves_the_booking_and_keeps_billing_verified(" tests/e2e/test_phase2_flows.py` returns 1, and the e2e pytest command passed in two consecutive runs (or one pass plus a run that failed only with 429, recorded)
    - If added: `grep -A4 '"SCHED-05"' tests/docs/test_requirement_traceability.py | grep -c "tests/e2e/test_phase2_flows.py"` returns 1, and `pytest tests/docs` passes
    - If not added: `git status --porcelain -- tests/e2e tests/docs` prints nothing, and the SUMMARY states the reason
    - `git status --porcelain -- services libs infra` prints nothing
  </acceptance_criteria>
  <done>Either a twice-proven gateway reschedule e2e is committed and traced to SCHED-05, or the SUMMARY says why it was not added and the repo carries no unproven test.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| test process -> shared Compose Postgres/Mongo | in-process tests write synthetic rows into the same visit_db / billing_db the containerized services use |
| test client -> published gateway | e2e requests cross the real gateway auth, scope and rate-limit path |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-gvd-01 | Tampering | test-result integrity: shared visit_db with the containerized visit-worker sweeper | low | mitigate | Task 1 step 5(a): classify count, marker or run-id mismatches seen while visit-worker is healthy as environmental. Rerun the single test once and record the evidence; never stop or restart containers; never weaken an assertion to hide cross-talk |
| T-gvd-02 | Information Disclosure | test data, assertions, logs | medium | mitigate | Synthetic data only (seed "Zzyzx E2E Synthetic" patients); the new test never prints tokens; no .env* or infra/secrets/* file is read; Findings entries carry ids and error types only |
| T-gvd-03 | Denial of Service | gateway auth budget (20/min) during e2e | low | accept | At most two e2e runs (about 14 auth calls); a 429-only failure is recorded as 02-12 deviation 2, and no gateway setting is changed |
| T-gvd-04 | Elevation of Privilege | production auth code | low | mitigate | Tests only. `git status --porcelain -- services libs infra` must be empty; the e2e uses the existing front-desk role (BOOKING_ROLES) and adds no bypass |
| T-gvd-SC | Tampering | npm/pip/cargo installs | high | accept | No package is installed by this plan; if an install seems needed, stop and report instead |
</threat_model>

<verification>
- Task 1 verify passes: both visit downstream files green on current code; the 8 names exist once each.
- Task 2: either its verify passed twice and the files are committed, or they are reverted with the reason recorded.
- `git status --porcelain -- services libs infra` is empty.
- Semgrep: tests-only change. Record "not run: no production code changed" (or run `semgrep scan --config auto tests/e2e/test_phase2_flows.py` if installed and allowed, and record the result).
</verification>

<success_criteria>
- The 02-11 "Deferred tests" list is closed with evidence from a run on today's code, with every failure classified and none silently papered over.
- No other 02-0x SUMMARY defers a plain test (grep evidence recorded).
- The 02-12 gateway reschedule e2e is either added and proven twice, or explicitly not added with a reason; the worker-restart e2e is recorded as skipped by instruction.
- Zero production-code changes; Docker Compose untouched.
</success_criteria>

<output>
Create `.planning/quick/261110-gvd-phase-2-closeout-write-deferred-tests-fr/261110-gvd-SUMMARY.md` when done. It must contain:
- The Task 1 run results: the 8-name `-k` run and the full two-file run, each with its pass/xfail counts.
- Every failure, with its class (environment, drift, or defect) and evidence.
- The other-summaries grep command and its output.
- The e2e reschedule outcome (added after 2 passes, or not added with the reason).
- The worker-restart e2e marked as skipped by user instruction.
- A Findings section (or "None").
- The Semgrep status.
- A note for the orchestrator that 02-11-SUMMARY "Deferred tests" and STATE's pending "deferred tests" item can be marked closed. This plan does not edit those historic files.
</output>
