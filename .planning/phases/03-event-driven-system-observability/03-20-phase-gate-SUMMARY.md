---
phase: 03-event-driven-system-observability
plan: 20
subsystem: verification
tags: [traceability, phase-gate, regression, e2e, semgrep]
requires:
  - phase: 03-event-driven-system-observability
    provides: "plans 03-01..03-19"
provides:
  - "PHASE3_TRACE and PHASE3_TRACE_NOTES in tests/docs/test_requirement_traceability.py (20 requirements -> 35 existing test files)"
  - "a re-runnable snapshot-probe e2e test"
  - "phase-gate logs under closeout-logs/ (left untracked: two of them contain dev-default DB passwords in pytest tracebacks) and the Semgrep triage"
  - "capacity limitations recorded in docs/architecture.md 10.9"
status: complete
requirements-completed: []
completed: 2026-11-11
actuals:
  tokens: 9000
  tasks: 2
  commits: 2
plan_head_before: 25237074a71c67ce141338beeb7650d0cd16b95b
plan_head_after: 46767f9322a32b2343a6176af5d87faeeff4f49b
---

# Phase 3 Plan 20: Phase Gate Summary

The traceability test, the phase-gate regression runs, the Semgrep triage and the human sign-off are done. The gate was NOT clean (known provider failure, test-environment capacity failures, macOS fork crashes; see below) and the user accepted and recorded that. REQUIREMENTS.md was not edited and no requirement was ticked (`git diff HEAD -- .planning/REQUIREMENTS.md` is empty).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | e41c96a | `PHASE3_TRACE` (20 ids), `PHASE3_TRACE_NOTES` (EVENT-04, NOTIF-04), four parametrized checks |
| 2 (fix found by the gate) | 46767f9 | snapshot-probe e2e test uses a per-run connector name (rerunnable) |
| 3 (human sign-off, docs) | see the final docs commit `docs(03-20): complete phase-gate plan with human sign-off` | this SUMMARY, limitations in docs/architecture.md 10.7 and 10.9, Phase 3 follow-ups in STATE.md |

## Task 1: traceability

`tests/docs/test_requirement_traceability.py` now has `PHASE3_TRACE` with exactly EVENT-01..05, NOTIF-01..04, ANLY-01..06, OBS-01..03, DASH-03, DASH-04, mapped to the plan's file list (all 35 distinct files exist and define tests). `PHASE3_TRACE_NOTES["EVENT-04"]` names `analytics-projection`, says projections are not Celery tasks, and records the user's answer (ACCEPTED, "Accept all six (Recommended)", plan 03-01 Task 2, item 2 of the User-veto list). `PHASE3_TRACE_NOTES["NOTIF-04"]` names the idempotency key and dedupe key and the real-provider caveat. Result: 40 passed in the file (15 Phase 2 cases unchanged plus the Phase 3 cases); `tests/docs` 164 passed. The check reads REQUIREMENTS.md only; the plan's "git diff is empty" criterion is a command, not a test (a test would break when the verifier later ticks requirements): verified empty.

## Phase gate

Stack: not rebuilt (orchestrator instruction: restart nothing); the running containers were the ones from the earlier plans, all healthy and the four connectors RUNNING before and after. `scripts/compose_healthcheck.sh` OK before every run. `OTEL_SDK_DISABLED` never set. No laptop sleep gap was detected (a 20 s wall-clock watcher logged none; run durations are consistent).

### Results

| Run | Command | Result | Log |
|---|---|---|---|
| non-e2e, run 1 | `pytest tests -m "not e2e" -q` | 2 failed, 1854 passed, 11 skipped, 71 deselected, 1 error, 26m40s | closeout-logs/non-e2e-run1.txt |
| non-e2e, run 2 (the gate log) | same | 5 failed, 1850 passed, 11 skipped, 71 deselected, 1 error, 26m17s | closeout-logs/non-e2e.txt |
| e2e, run 1 | `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e -m e2e -q` | 2 failed, 68 passed, 1 error, 18m25s (Jaeger container had been OOM-killed) | closeout-logs/e2e-run1-jaeger-oom.txt |
| e2e, run 2 (the gate log, after the Jaeger restart and the probe-test fix) | same | 71 passed, 5 deselected, 13m49s | closeout-logs/e2e.txt |
| Semgrep | `semgrep scan --config auto --error services libs scripts infra` (metrics ON, as scripts/phase2_closeout.sh) | 24 findings, 335 rules, 302 files | closeout-logs/semgrep.txt |

Neither non-e2e run was clean, so the plan's "no failed or errored tests" criterion is NOT met for the full run. Every failure other than the known provider one passed when rerun alone (see below).

### Non-e2e failures

| Test | Run | Class | Evidence |
|---|---|---|---|
| tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway | 1 and 2 | KNOWN (baseline specialty ordering, leftover provider data; deferred-items.md) | same assertion as in 03-01 |
| tests/workflows/test_reconciler.py::test_the_lookback_marks_an_appointment_with_no_billing_record_as_checked (failed plus teardown error) | 1 | NEW, environmental: Postgres `TooManyConnectionsError` (max_connections 300) | reran the whole file alone: 38 passed |
| tests/visit/test_visit_list.py::test_clinic_scope_for_front_desk_and_admin (setup error) | 2 | NEW, environmental: same TooManyConnectionsError | passes alone |
| tests/identity/test_email_migration.py::test_downgrade_drops_the_index_and_upgrade_restores_it, tests/identity/test_signing_key.py::test_identity_refuses_to_build_without_a_signing_key, tests/visit/test_backfill.py x2 | 2 | NEW to the list but the known flaky class: subprocess exit -5 (SIGTRAP) | all pass alone (27 passed for email_migration, signing_key, backfill, visit_list together) |

Root causes found:

- **Connection ceiling.** Sampling `pg_stat_activity` every 10-15 s during run 2: containers hold about 110 connections idle, the pytest process grows from about 50 to about 190-200 as fixtures open engines; total reached 304 (of 300, 3 reserved) between 92 and 100 percent of the run. The late tests of the run hit the ceiling. This grows with every added service and test file; it is a suite-capacity issue, not a product defect. Suggested fix (not done: it needs a Postgres restart or test-engine disposal work): raise `max_connections` for the dev Postgres, or dispose per-test engines.
- **SIGTRAP exit -5.** macOS diagnostic reports (`~/Library/Logs/DiagnosticReports/python3.12-*.ips`) show "multi-threaded process forked / BUG IN CLIENT OF LIBDISPATCH: trying to lock recursively / crashed on child side of fork pre-exec": a `subprocess` fork from the threaded pytest process (confluent-kafka and gRPC threads) crashes in the child before exec. Intermittent, macOS-specific, the same class as the one seen once in tests/visit/test_backfill.py in 03-17.
- The email-migration upgrade test named as known did not fail in either run.

### e2e failures (run 1 only)

| Test | Class | Evidence |
|---|---|---|
| tests/e2e/test_phase3_trace.py (setup error) | environmental: the `smarthealth-jaeger-1` container was OOM-killed (exit 137, OOMKilled=true, 13:30:04Z, during non-e2e run 1; the Docker VM has 7.65 GiB) so Jaeger was unreachable | restarted with `docker compose up -d --no-deps jaeger` (the one service I restarted; reported); test passes (and prints a fresh trace id) |
| tests/e2e/test_phase3_metrics.py::test_nfr_histograms_present (histograms did not grow within 120 s) | not reproduced: passes alone and in the full run 2; cause not established (it ran while Jaeger was down; the NFR histograms are Prometheus-side) | |
| tests/e2e/test_phase3_outbox_cdc.py::test_snapshot_delivers_row_committed_before_connector_first_start | REAL test defect from 03-05, fixed (46767f9) | Kafka Connect keeps a deleted connector's stored offsets; a rerun under the same name resumed from the dropped slot's stale LSN ("Last recorded offset is no longer available"), task FAILED. Fix: per-run connector name; two consecutive runs pass, no leftover connector or slot |

### Fixes

1. tests/e2e/test_phase3_outbox_cdc.py (46767f9): per-run connector name. No service code changed. No test weakened.
2. Jaeger container restarted (the in-memory traces, including the 03-17 trace id, were lost with it).

### Semgrep triage (`--config auto`, 24 findings)

Total 24: 10 missing-user (accepted for now by the user) and 14 judged false positives with the reasons below.

| Findings | Location | Class |
|---|---|---|
| 8 x dockerfile.security.missing-user | billing, gateway, identity, organization, patient, provider, scheduling, visit Dockerfiles | ACCEPTED by the user for now (Phase 2 closeout); deferred |
| 2 x dockerfile.security.missing-user | services/analytics/Dockerfile, services/notification/Dockerfile (new in Phase 3) | same class as the accepted 8; ACCEPTED by the user for now at the phase gate ("Accept for now (Recommended)"); production hardening deferred |
| 6 (3 x formatted-sql-query + 3 x sqlalchemy-execute-raw-query) | libs/events/migration.py lines 59, 60, 70 | false positive: Alembic DDL built from module constants (`PUBLICATION_NAME`, `DEBEZIUM_ROLE`), no request input reaches it |
| 2 (formatted-sql-query + execute-raw-query) | services/analytics/migrations/versions/analytics_0001_projections.py:193 `DROP VIEW IF EXISTS {view}` | false positive: `view` iterates the module constant `_VIEWS` |
| 1 python37-compatibility-importlib2 | libs/events/registry.py:23 `from importlib import resources` | false positive: stdlib; the project pins Python >=3.12 |
| 2 (formatted-sql-query + execute-raw-query) | services/scheduling/migrations/versions/scheduling_0004_appointment_states.py:77 | Phase 2 false positive (only the internal constant `active_predicate` is interpolated) |
| 1 avoid-sqlalchemy-text | services/scheduling/repository/idempotency_repo.py:26 | Phase 2 false positive (only `KEY_TTL_HOURS`, an integer constant) |
| 1 jwt-python-exposed-data | services/identity/security/signing.py:57 | Phase 2 false positive (the kid header is public, the payload is the token's own claims) |
| 1 insecure-file-permissions | scripts/generate_service_secrets.py:55 `os.chmod(dir, 0o700)` | Phase 2 false positive (0700 is the restrictive mode) |

No code changed for Semgrep; nothing trivially and really fixable was found. The Phase 3 libs/events and analytics findings are the only new Python ones (all constants-only DDL).

### tjo-* comparison

Before: `tjo-api Up 30 hours (unhealthy)`, `tjo-postgres Up 30 hours (healthy)`. After: `tjo-api Up 32 hours (unhealthy)`, `tjo-postgres Up 32 hours (healthy)`: not restarted, not touched.

### Stack state at hand-off

All containers healthy (`scripts/compose_healthcheck.sh` OK), four connectors and tasks RUNNING, four replication slots active (5.6 MB retained each), Jaeger up with a fresh trace.

## Human sign-off

**Checkpoint reply (verbatim):** "approved, proceed"

What the user checked, 2026-11-11: Jaeger trace `8132991350448d3158960a29412aa965` (61 spans, 12 services), the four Grafana dashboards, and `docs/architecture-dashboard.html` offline at full and narrow width. The manual k6 smoke was skipped as optional because the smoke had already passed three times.

**Decisions recorded**

| Decision | Question | User answer (verbatim) | Result |
|---|---|---|---|
| (a) Gate not clean | The gate was not clean: the known provider test failure; environmental `TooManyConnectionsError` at the 300-connection Postgres ceiling in two non-e2e runs; macOS fork-SIGTRAP (exit -5) subprocess crashes in identity/visit tests; each passed alone; e2e run 2 was 71 passed after the Jaeger restart and the snapshot-probe test fix. | "Accept and record (Recommended)" | Recorded as known test-environment limitations (docs/architecture.md 10.9) with a follow-up to raise the dev Postgres `max_connections` and cap Jaeger memory. The follow-up is NOT done. |
| (b) Two new root-user Dockerfiles | `services/analytics` and `services/notification` Dockerfiles have no `USER` (Semgrep missing-user), the same class as the eight accepted earlier. | "Accept for now (Recommended)" | Accepted; production hardening deferred (ten Dockerfiles in total). |

### Test numbers

| Run | Failed | Passed | Skipped | Errors | Duration |
|---|---|---|---|---|---|
| non-e2e run 1 | 2 | 1854 | 11 | 1 | 26m40s |
| non-e2e run 2 | 5 | 1850 | 11 | 1 | 26m17s |
| e2e run 1 | 2 | 68 | 0 | 1 | 18m25s |
| e2e run 2 | 0 | 71 | 0 (5 deselected) | 0 | 13m49s |

`tests/docs`: 164 passed after the docs edits.

## Deviations from Plan

1. The stack was not rebuilt with `up -d --build` (orchestrator instruction). The tests ran against the containers that were already up, which the earlier plans rebuilt as they changed code; no service code changed after 03-17.
2. Both non-e2e and e2e were run twice (see the table); the first logs are kept under `-run1` names so nothing is hidden.
3. [Rule 1] The snapshot-probe test fix is a file outside the plan's `files_modified`: tests/e2e/test_phase3_outbox_cdc.py.
4. [Rule 3] Jaeger restarted after an OOM kill.
5. [Rule 3] The Jaeger container was OOM-killed (exit 137) during non-e2e run 1 and restarted with `docker compose up -d --no-deps jaeger`. Jaeger stores traces in memory, so the trace id quoted earlier in docs/architecture.md 10.7 (`e839d1a9ec7d48e435d09abb68881b00`, 57 spans) was lost; the gate evidence is the fresh trace `8132991350448d3158960a29412aa965`, and 10.7 now says so.
6. docs/architecture.md was edited beyond the limitations section: 10.7 (lost trace id and fresh one; the dashboards and the Jaeger UI were checked by the user, so "nobody has looked at them in a browser" became a dated statement and the "Not verified: dashboards in a browser" item was dropped from 10.9). The wording `nobody has looked at` remains in the sentence, so the docs-vs-code test still passes unweakened.
7. Semgrep scanned `services libs scripts infra`: the orchestrator named the first three directories, `infra` comes from the plan text (it produced no findings).

## Known Stubs

None.

## Threat Flags

None.
