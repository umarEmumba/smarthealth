---
phase: 03-event-driven-system-observability
plan: 24
subsystem: observability
tags: [prometheus, multiprocess, gunicorn, fastapi, gateway, scheduling, CR-02]

requires:
  - phase: 03-event-driven-system-observability
    provides: "03-15/03-16 instrument_http and finer buckets; 03-21 raised gateway budget; 03-23 pool settings in x-app-env"
provides:
  - "libs/observability/gunicorn_conf.py: on_starting (create and empty the multiprocess dir) and child_exit (mark_process_dead)"
  - "exposition_registry() and render_latest() in libs/observability/http_metrics.py"
  - "scheduling and gateway run two gunicorn workers in Prometheus multiprocess mode: every scrape is the sum over both workers"
  - "e2e no-reset / never-decrease check with driven traffic, parametrized over MULTIPROCESS_API_JOBS"
affects: [03-25-multiprocess-rollout, 03-26]

actuals:
  tokens: 5400
  tasks: 2
  commits: 2

plan_head_before: 27274b8928892c83d2cd50b012df58d48da20f04
plan_head_after: 9126f14df5a2f7e2fbb937d84dfc27e8b95b48ef

tech-stack:
  added: []
  patterns:
    - "PROMETHEUS_MULTIPROC_DIR exported only by the API image default command (never Compose), so command-override processes of the same image stay single-process"
    - "hand-written /metrics routes render through render_latest(); instrumentator expose() switches to MultiProcessCollector by itself"

key-files:
  created:
    - libs/observability/gunicorn_conf.py
    - tests/libs/test_multiprocess_metrics.py
  modified:
    - libs/observability/http_metrics.py
    - services/gateway/app.py
    - services/gateway/Dockerfile
    - services/scheduling/Dockerfile
    - tests/e2e/test_phase3_metrics.py

key-decisions:
  - "Two gunicorn workers kept (D-14); multiprocess mode, not WEB_CONCURRENCY=1"
  - "The directory variable is set in the Dockerfile shell before exec gunicorn, never in the config module (prometheus_client picks its value class at import) and never in Compose"
  - "A static test pins that Compose never sets the directory variable (test_the_variable_is_exported_only_by_image_commands_never_in_compose)"

requirements-completed: []  # OBS-01 is the plan's requirement; per instruction no requirement is ticked and REQUIREMENTS.md is not edited (gap closure continues in 03-25)

status: complete
---

# Phase 3 Plan 24: Multiprocess Metrics Summary

**Scheduling and the gateway keep two gunicorn workers and now answer every /metrics scrape with the sum over both workers (Prometheus multiprocess mode); driven traffic is fully counted and the HTTP counters no longer reset, proven red before and green after the live rebuilds.**

## What was done

- Verified the installed instrumentator myself: `prometheus_fastapi_instrumentator/instrumentation.py` `expose()` (lines ~290-292) builds a fresh `CollectorRegistry` plus `MultiProcessCollector` when `PROMETHEUS_MULTIPROC_DIR` is in the environment, and the constructor raises if the variable points at a non-directory (hence `mkdir -p` in the command).
- Task 1 (tracer, commit 21b4fbc): `libs/observability/gunicorn_conf.py` (`on_starting`, `child_exit`; does not set the variable), scheduling Dockerfile command (Alembic step first, then `export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus-multiproc`, `mkdir -p`, `gunicorn -c python:libs.observability.gunicorn_conf ... -w ${WEB_CONCURRENCY:-2}`), hook and static Dockerfile tests, e2e helpers (`MULTIPROCESS_API_JOBS`, `_drive_requests`, `prom_query_range`) and the parametrized no-reset test.
- Task 2 (commit 9126f14): `exposition_registry()` / `render_latest()` in `http_metrics.py`, gateway `metrics_endpoint` returns `render_latest()` (listener check untouched), gateway Dockerfile command (no Alembic step, both binds), aggregation test (two per-pid mmap files 3 + 4 render 7), gateway added to the static and e2e lists.

## Red / green evidence (from this execution)

Test: `test_http_request_counters_have_no_resets_and_never_decrease` (40 `/health` requests driven inside the container, fresh-scrape waits, resets over W, range series at 15 s step).

| Run | Job | W | before | after | increase | resets | decreases |
|-----|-----|---|--------|-------|----------|--------|-----------|
| RED (before scheduling rebuild) | smarthealth-scheduling | 300 s | 4746 | 4785 | 39 (< 40, FAIL) | 48 | 4711 -> 1423, 4716 -> 1427, 4735 -> 1431, 4740 -> 1436 |
| RED (before gateway rebuild) | smarthealth-gateway | 300 s | 1378 | 662 | -716 (FAIL) | 14 | 1340 -> 658, 1359 -> 659, 1419 -> 662 |
| GREEN (after scheduling rebuild) | smarthealth-scheduling | 105 s (uptime 135 s) | 23 | 68 | 45 | 0 | none |
| GREEN (after gateway rebuild) | smarthealth-gateway | 105 s (uptime 135 s) | 25 | 70 | 45 | 0 | none |
| GREEN (both jobs together, later run) | scheduling / gateway | 300 s (scheduling, uptime 527 s) | 128 | 172 | 44 | 0 | none |

The increase exceeds 40 because the Compose healthcheck also hits `/health` every 10 s during the 30 s wait; the minimum of 40 is what proves every driven request reached the job. Before the change the sums jumped between two per-process series (the live CR-02 evidence), after it they are monotonic.

Other live checks: `/proc/1/environ` of the rebuilt `scheduling` and `gateway` containers contains `PROMETHEUS_MULTIPROC_DIR` (count 1); `scheduling-worker` (same image, command override, not recreated) count 0. `/tmp/prometheus-multiproc` in scheduling held `counter_10.db`, `counter_11.db`, `histogram_10/11.db`, `summary_10/11.db` (one set per worker pid). Gateway `:9100/metrics` serves `http_requests_total` and `:8000/metrics` is 404; the gateway still publishes only 8000 (no new host port).

## Verification

- `.venv/bin/python -m pytest tests/libs tests/infra tests/gateway -q`: 325 passed.
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_gateway_smoke.py -m e2e -q`: 14 passed (includes `test_every_job_and_target_is_up`, the NFR histogram test and the gateway smoke).
- `scripts/compose_healthcheck.sh`: OK; `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all active.
- `docker compose ps`: gateway, scheduling, scheduling-worker healthy.
- Grep check (T-3 no default collectors): no file under `infra/grafana`, `infra/prometheus*`, `tests` or `docs` referenced `process_resident_memory_bytes`, `process_cpu_seconds_total`, `python_gc_`, `process_*` or `python_info` for an API job, before the rebuilds (grep ran with no hits; `docs` has no such references). The default `process_*` / `python_*` collectors are therefore not relied on by any dashboard, test or doc.
- Gateway rate-limit values after the rebuild: `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000`, `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` (restored to 20/300 by plan 03-26, not here).
- `tjo-*` status before and after identical: `tjo-api (unhealthy)` (pre-existing), `tjo-postgres (healthy)`.

## Accept-path overhead (D-04)

Per-request mmap writes are the known cost of multiprocess mode. What was done to keep it small: no new metrics and no extra labels (the same counter and histogram updates as before, now into mmap files); the in-progress gauge stays off, so there are no live-gauge files and `child_exit` has nothing heavy to do; no `max_requests` recycling, so no file churn; each worker writes only its own pid's files (no cross-process locking on the request path); aggregation happens only on a scrape (every 15 s), never on a request; and the directory lives on the container filesystem under `/tmp` (page-cache writes, no fsync). The measured effect is not claimed here: the k6 smoke rerun in plan 03-25 Task 3, with all nine APIs switched, is the check.

## Semgrep (after commit, tracked files only)

- `p/python` on `libs/observability/gunicorn_conf.py`, `libs/observability/http_metrics.py`, `services/gateway/app.py`, `tests/libs/test_multiprocess_metrics.py`, `tests/e2e/test_phase3_metrics.py`: 0 findings.
- `p/security-audit` on the same files: 4 findings, all `dynamic-urllib-use-detected` in `tests/e2e/test_phase3_metrics.py` (the existing `urlopen` calls on fixed local URLs at lines 87, 105 and 165, plus the new `prom_query_range` at line 96, same pattern and the same `# noqa: S310` justification: fixed local Prometheus URL, test-only). The four source files have 0 findings.
- Dockerfile `USER` (`missing-user`) findings on the two touched Dockerfiles stay accepted by the user and untouched.

## Deviations from Plan

None - plan executed as written. Minor notes: the plan's verify command passes several paths to Semgrep in one shell variable; it was run with explicit separate arguments. The e2e test waits for a fresh scrape after the driven requests (a timestamp check) in addition to the 30 s sleep, which is stricter than the plan text.

## Known Stubs

None.

## Threat Flags

None: no new endpoint or port; the gateway metrics listener check is unchanged (T-3-76); the directory is emptied by the master before any worker starts (T-3-77).

## Issues Encountered

- First draft of the aggregation test used the wrong `mmap_key` signature (labelnames and labelvalues are separate lists); fixed before committing Task 2.
- A first red run attempt used `timeout`, which is not installed on this macOS host; rerun without it.

## Self-Check: PASSED

- libs/observability/gunicorn_conf.py, tests/libs/test_multiprocess_metrics.py: found.
- Commits 21b4fbc and 9126f14 present; measured commits since plan start: 2 (`27274b8..HEAD`).
