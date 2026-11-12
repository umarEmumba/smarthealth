---
phase: 261013-mhu-write-the-8-deferred-plan-02-11-tests-vi
plan: 01
subsystem: testing
tags: [pytest, temporal, visit, billing, idempotency, d-06, d-16]
requires:
  - phase: 02-scheduling-service-workflow
    provides: "02-11 downstream workflow, sweep_unstarted, VisitCompletedWorkflow"
provides:
  - "6 deferred downstream tests in tests/visit/test_downstream.py (file now 9 tests)"
  - "2 deferred workflow branch tests in tests/workflows/test_visit_completed.py (file now 4 tests)"
key-files:
  modified:
    - tests/visit/test_downstream.py
    - tests/workflows/test_visit_completed.py
decisions:
  - "Tests use explicitly typed ApplicationErrors so expected 'skipped:<ErrorType>' strings are exact"
requirements: [VISIT-04, BILL-02]
status: complete
commits: 3
plan_head_before: b8ae5578a8716f086f0d2b44db8d0db3f4de6df0
plan_head_after: 84302fe34fabb1dec3737d53900e16b2e75aeac8
actuals:
  tasks: 3
  commits: 3
---

# Quick 261013-mhu: the 8 deferred plan 02-11 tests

Eight tests deferred from plan 02-11 now exist under their planned names and pass against the live Compose infra; tests only, nothing under services/ or libs/ changed.

## Tests written

tests/visit/test_downstream.py (3 -> 9), plus helpers `_finalized_rows` and `_age_completion`:
1. test_repeat_complete_with_null_marker_starts_workflow (tracer, commit 9d522c6)
2. test_grace_window_skips_recent_completion (f8f13c3)
3. test_repeat_complete_after_success_is_noop_single_run (f8f13c3)
4. test_concurrent_sweeps_start_one_run_one_finalization (f8f13c3)
5. test_voided_billing_reports_not_finalizable_and_intents_still_recorded (f8f13c3)
6. test_deferred_start_logs_downstream_start_deferred (f8f13c3)

tests/workflows/test_visit_completed.py (2 -> 4), `_stubs` extended with keyword-only `billing_failures` and `followup_error` (defaults keep prior behaviour) (84302fe):
7. test_followup_failing_every_attempt_is_skipped_and_others_succeed
8. test_billing_retryable_failures_are_absorbed

## Verification

- `tests/visit/test_downstream.py`: 9 passed
- `tests/workflows/test_visit_completed.py`: 4 passed
- `.venv/bin/python -m pytest tests/visit tests/workflows -q -p no:cacheprovider`: **158 passed, 4 skipped** (0 failed; the 4 skips are pre-existing `skipif` markers in tests/workflows/test_replay.py, not from these tests)
- `grep -c "^async def test_"`: 9 (test_downstream.py), 4 (test_visit_completed.py); the 02-11 criteria (>= 8, >= 4) are met
- `git status --porcelain -- services libs`: empty
- Semgrep: installed, but `--config auto` requires metrics/network (refused with metrics off); not run. Tests-only change.

## Deviations from Plan

None. The plan was executed as written.

## Findings

None. No production defect surfaced; all tests passed on first run.

## Notes

- 02-11 deviation 6 (deferred tests) is now closed.
- Visit-test infra note: concurrent-sweep and grace tests age `completed_at` via SQL because the DB clock does not skip under the time-skipping env.

## Self-Check: PASSED

Files modified exist; commits 9d522c6, f8f13c3, 84302fe present on gsd/phase-02-scheduling-service-workflow.
