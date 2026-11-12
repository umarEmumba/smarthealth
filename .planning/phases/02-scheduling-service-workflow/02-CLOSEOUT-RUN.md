# Phase 2 close-out run
Started: 2026-11-10T07:17:35Z  Commit: 740e505

- **01-stack-health**: exit 0 in 0s — OK: every required service and every service with a healthcheck is healthy (`.planning/phases/02-scheduling-service-workflow/closeout-logs/01-stack-health.log`)
- **02-regression**: exit 1 in 1123s — 7 failed, 1349 passed, 9 skipped, 9 deselected, 108 warnings in 1119.74s (0:18:39) (`.planning/phases/02-scheduling-service-workflow/closeout-logs/02-regression.log`)
- **03-e2e**: exit 0 in 88s — 9 passed, 5 deselected in 84.43s (0:01:24) (`.planning/phases/02-scheduling-service-workflow/closeout-logs/03-e2e.log`)
- **04-semgrep**: exit 1 in 19s —           The object is passed strictly to jwt.encode(...) Make sure that sensitive information is not exposed (`.planning/phases/02-scheduling-service-workflow/closeout-logs/04-semgrep.log`)

Finished: 2026-11-10T07:38:05Z

## Triage (2026-11-10)

**Regression, 7 failures: script artifact, not code.** The script exported `OTEL_SDK_DISABLED=true`, which disables the tracing these tests assert. All 7 are trace-propagation tests (`tests/e2e/test_trace_propagation.py` x4, `tests/libs/test_internal_client.py`, `tests/libs/test_service_client.py`, `tests/workflows/test_trace.py`). Rerun without the variable: the 3 unit/workflow tests pass (3 passed) and `tests/e2e/test_trace_propagation.py` passes (5 passed). The export was removed from `scripts/phase2_closeout.sh`; the other 1349 tests passed in this run. A single clean full-regression run after the script fix was not repeated (about 19 min).

**e2e:** 9 passed on the live stack (gateway auth budget 1000/min).

**Semgrep: 13 findings (`--config auto`, 219 files).**
- 8 x `dockerfile.security.missing-user` (billing, gateway, identity, organization, patient, provider, scheduling, visit Dockerfiles): ACCEPTED for now by the user. These are local-dev images. Production fix: add a non-root `USER` to each Dockerfile. Deferred, not fixed.
- 5 judged false positives, no code change:
  - `jwt.encode` in `services/identity/security/signing.py`: the `kid` header is public and the payload is the token's own claims.
  - `os.chmod(output_dir, 0o700)` in `scripts/generate_service_secrets.py`: the permission is deliberately restrictive.
  - f-string SQL in `services/scheduling/migrations/versions/scheduling_0004_appointment_states.py` and `services/scheduling/repository/idempotency_repo.py`: only internal constants (`active_predicate`, `KEY_TTL_HOURS`) are interpolated, never request input. The two raw-query rules on the same statements are the same finding.

**Human checks:** dashboard visual check (02-12 Task 5) done by the user 2026-11-10. Temporal poller `scheduling-schedule-change-poller` intentionally left paused by user decision.
