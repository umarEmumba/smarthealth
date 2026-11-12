---
phase: 03-event-driven-system-observability
plan: 25
subsystem: observability
tags: [prometheus, multiprocess, gunicorn, k6, smoke, CR-02]

requires:
  - phase: 03-event-driven-system-observability
    provides: "03-24 gunicorn_conf, render_latest, scheduling and gateway multiprocess; 03-23 pool settings"
provides:
  - "billing, analytics, identity, patient, provider, organization and visit run two gunicorn workers in Prometheus multiprocess mode (all nine APIs now)"
  - "static check of nine Dockerfile commands and of every resolved Compose environment (no directory or worker-count variable)"
  - "e2e no-reset test and pid-1 environment tests over all nine API jobs / seven single-process roles"
  - "smoke run on the corrected stack with the accept-path comparison, recorded in docs/nfr.md and loadtest/README.md"
affects: [03-26]

actuals:
  tokens: 6400
  tasks: 3
  commits: 3

plan_head_before: 8449ea870a1e4adf5ab75af36c0724a0e6ef84a0
plan_head_after: 34c9954d3053a247b14b39c1f08fba9b1ef7a9f2

tech-stack:
  added: []
  patterns:
    - "directory variable exported only by the API image default command; command-override roles stay single-process"

key-files:
  created: []
  modified:
    - services/billing/Dockerfile
    - services/analytics/Dockerfile
    - services/identity/Dockerfile
    - services/patient/Dockerfile
    - services/provider/Dockerfile
    - services/organization/Dockerfile
    - services/visit/Dockerfile
    - tests/libs/test_multiprocess_metrics.py
    - tests/e2e/test_phase3_metrics.py
    - docs/nfr.md
    - loadtest/README.md

key-decisions:
  - "Same image command shape as plan 03-24 for the seven APIs; two workers kept (D-14)"
  - "Compose is not edited; a static test parses it with yaml.safe_load and rejects PROMETHEUS_MULTIPROC_DIR, WEB_CONCURRENCY and GUNICORN_WORKERS in any resolved service environment"

requirements-completed: []  # OBS-01 is the plan's requirement; per instruction no requirement is ticked and REQUIREMENTS.md is not edited

status: complete
---

# Phase 3 Plan 25: Multiprocess Rollout Summary

**All nine APIs now answer every /metrics scrape with the sum over both gunicorn workers; driven traffic is fully counted with zero counter resets on every job (red on the seven before the rebuild, green on all nine after), the seven single-process roles are untouched, and the k6 smoke passes on the corrected stack (exit 0, accept p95 41.85 ms / p99 245.49 ms).**

`plan_head_before` is `8449ea8` (the 03-24 summary commit); `plan_head_after` is `34c9954`. Three commits measured: 50c65a9, 5ee7667, 34c9954.

## What was done

- Task 1 (tracer, commit 50c65a9): the seven Dockerfile commands now run the Alembic step, export `PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus-multiproc`, `mkdir -p` it and `exec gunicorn -c python:libs.observability.gunicorn_conf ... -w ${WEB_CONCURRENCY:-2}` with the existing bind and app path, with the same comment block as scheduling. `tests/libs/test_multiprocess_metrics.py`: `API_DOCKERFILES` now has all nine; new `test_no_compose_service_carries_the_directory_or_a_worker_count_variable` (resolves 33 service environments, mapping or list form). `MULTIPROCESS_API_JOBS` in `tests/e2e/test_phase3_metrics.py` lists all nine jobs, plus a `SINGLE_PROCESS_ROLES` tuple.
- Task 2 (commit 5ee7667): `test_api_pid1_has_the_multiprocess_directory` (9 cases) and `test_single_process_role_pid1_has_no_multiprocess_directory` (7 cases); the environment is read inside the container and matched in Python, only the variable name's presence is asserted.
- Task 3 (commit 34c9954): admin password procedure, seed, smoke, read-back, docs.

## Red / green evidence (Task 1)

Test `test_http_request_counters_have_no_resets_and_never_decrease` (40 `/health` requests driven in the container).

RED, run against the unmodified seven containers before the rebuild (all 7 failed):

| Job | W | before | after | increase | resets | first decreases |
|-----|---|--------|-------|----------|--------|-----------------|
| analytics | 300 s | 4039 | 1396 | -2643 | 12 | 4017 -> 1380 |
| billing | 300 s | 5970 | 6003 | 33 | 14 | 5935 -> 2299 |
| identity | 300 s | 2370 | 2372 | 2 | 18 | 7851 -> 2369 |
| organization | 300 s | 10537 | 10567 | 30 | 30 | 10507 -> 3117 |
| patient | 300 s | 6482 | 6525 | 43 | 30 | 6456 -> 2307 |
| provider | 300 s | 7769 | 7805 | 36 | 24 | 7744 -> 2473 |
| visit | 300 s | 2297 | 739 | -1558 | 25 | 2274 -> 726 |

(The failing assertion was the increase for analytics, billing, identity, organization, provider and visit; patient's increase happened to reach 43 but its 30 resets fail the next assertion.)

GREEN, after the rebuild, all nine passed in one run (`9 passed in 487.57 s`):

| Job | W | uptime | before | after | increase | resets | decreases |
|-----|---|--------|--------|-------|----------|--------|-----------|
| analytics | 105 s | 135 s | 23 | 68 | 45 | 0 | none |
| billing | 144 s | 175 s | 30 | 75 | 45 | 0 | none |
| gateway | 300 s | 1327 s | 428 | 473 | 45 | 0 | none |
| identity | 234 s | 265 s | 44 | 89 | 45 | 0 | none |
| organization | 274 s | 304 s | 51 | 96 | 45 | 0 | none |
| patient | 300 s | 344 s | 58 | 103 | 45 | 0 | none |
| provider | 300 s | 390 s | 66 | 111 | 45 | 0 | none |
| scheduling | 300 s | 1894 s | 471 | 516 | 45 | 0 | none |
| visit | 300 s | 466 s | 78 | 123 | 45 | 0 | none |

The increase is 45 (40 driven plus the Compose healthcheck probes) for every job: every driven request reached its own API and was counted.

## Live checks

- Rebuilt with `up -d --build --no-deps billing analytics identity patient provider organization visit`; all nine API services healthy afterwards.
- pid 1 environment: the nine API containers have `PROMETHEUS_MULTIPROC_DIR`, `scheduling-worker`, `visit-worker`, `notification-temporal-worker`, `visit-consumer`, `analytics-consumer`, `notification-consumer`, `notification-celery` do not: 16 passed (9 API + 7 single-process cases, `-rA` output).
- `tests/infra tests/libs`: 253 passed. `tests/docs/test_phase3_docs.py tests/loadtest`: 172 passed.
- `tests/e2e/test_phase3_metrics.py` full file: 33 passed, 3 failed in the first run, because Kafka had been OOM-killed (see Incident below); after restarting Kafka the three (`test_operations_panels_return_rows_as_grafana_ro`, `test_replication_slot_query_as_grafana_ro`, `test_nfr_histograms_present`) were rerun alone and passed (3 passed). `test_every_job_and_target_is_up` passed in the first run. `tests/e2e/test_phase2_flows.py` + `test_gateway_smoke.py`: 6 passed.
- Postgres connections: 93 before the rebuild (idle), 45 right after the recreate (pools are lazy), peak 91 sampled during the smoke. No sharp rise (the 108 peak of earlier test load is not exceeded).
- Final state: `scripts/compose_healthcheck.sh` OK; `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all active; gateway `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000`, `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` (never recreated; plan 03-26 restores 20/300); `tjo-api (unhealthy, pre-existing)` and `tjo-postgres (healthy)` before and after, identical apart from uptime. The `visit-downstream-sweeper` Schedule was not touched.

## Incident during this plan (not caused by a code defect)

About 00:28 Asia/Karachi (19:28 UTC), while the seven images were building, the Docker VM (7.65 GiB) ran short of memory and the OOM killer stopped `smarthealth-kafka-1` (`OOMKilled=true`, exit 137). The seven rebuilt APIs came up healthy and the no-reset tests were unaffected, but the first full metrics run (about 00:30 to 00:40) hit the missing broker: `dbz_*` replication slots were inactive and three e2e tests failed. `scripts/compose_healthcheck.sh` had still printed OK (it reports healthy services, a stopped one is not flagged). Found at about 00:48, I started Kafka again with `docker compose start kafka` (a non-destructive start of an exited container, no recreate, no volume touched); within about 2 minutes Kafka was healthy, all eight connectors RUNNING and all four slots active again; the three tests then passed on rerun. Recommendation: rebuild images one or a few at a time on this host, or free Docker memory first; Kafka has no memory limit set in Compose.

## Task 3: smoke on the corrected stack

Procedure: `scripts/compose_healthcheck.sh` OK, no pytest process running, load average 2.68 / 4.31 / 4.40. Password for `loadtest-admin@example.com` only: `secrets.token_urlsafe(24)`, hashed with `services.identity.security.passwords.hash_password`, single UPDATE where `email` and `role = 'admin'` both match, `UPDATE 1` and a count of 1 confirmed; password held in a 0600 file in the scratchpad, never printed, deleted right after seeding (before the run: the run itself does not read it); `loadtest/data/plan.json` deleted after the run and both confirmed absent. Seed `--providers 2 --patients 250 --days 2` (768 slots, 767 bookings). Started 2026-11-11T19:56:21Z, ended 19:56:52Z (31 s). `gap-smoke.txt` ends with `k6_exit=0` and holds no `eyJ` or `Bearer ` value. Only `smoke` was run.

k6 figures: 151 iterations, 0 dropped, 0 of 252 requests failed (0 of 151 accepts), 213 of 213 checks passed, 0 confirm timeouts.

| Metric | Value | Threshold |
|--------|-------|-----------|
| accept (POST /api/appointments) p95 / p99 | 41.85 ms / 245.49 ms (max 789.5 ms) | 300 / 800 ms |
| confirm time (`booking_confirm_seconds`) p95 / p99 | 0.533 s / 0.543 s | 5 s (p99 15 s) |
| read appointment p95 / p99 | 15.56 / 24.98 ms | 200 / 500 ms |
| read slots p95 / p99 | 14.74 / 31.73 ms | 200 / 500 ms |
| booking error rate | 0.00% | < 1% |

No threshold was crossed.

Prometheus (first figures read from multiprocess counters; bucket-interpolated; window 91 s at run end, and 121 s at end plus 30 s): accept p50 about 13.5 ms, p95 about 36 to 47 ms, p99 about 172 to 542 ms (the p99 sits in the 0.3 to 0.8 s buckets with few samples, so it is mostly interpolation); `booking_saga_seconds` p50 about 0.10 s, p95 about 0.45 s, p99 about 0.71 to 0.72 s; gateway 5xx: no 5xx series (0). Resets over the window (`closeout-logs/gap-smoke-resets.txt`, W = 91 s, evaluated at the run's end): 0 for each of the nine API jobs (analytics, billing, gateway, identity, organization, patient, provider, scheduling, visit). The plan's verify command (k6_exit=0, nine zero resets, plan file gone, docs tests) exits 0.

### Comparison with all three baselines (accept = POST /api/appointments, k6)

| Run | accept p95 / p99 | saga p95 | Host load average |
|-----|------------------|----------|-------------------|
| 2026-11-10 pre-Phase 3 | 21.8 / 78.9 ms | about 0.52 s | quiet |
| 2026-11-11 after producers (03-11) | 29.3 / 170.0 ms | about 0.52 s | about 3 |
| 2026-11-11 finished stack (03-16) | 49.1 / 200.8 ms | about 0.52 s | 3.4 / 5.8 / 6.3 |
| 2026-11-12 nine APIs multiprocess (this run) | 41.85 / 245.49 ms | 0.533 s (k6), 0.45 s (Prometheus) | 2.68 / 4.31 / 4.40 |

Did the multiprocess mmap writes change the accept path measurably? Not measurably. p95 is between the two earlier Phase 3 runs (below 49.1, above 29.3) and about 1.9 times the quiet baseline; the host was no busier than for the 03-16 run. The p99 is the highest of the three Phase 3 runs (245.5 against 170.0 and 200.8 ms), but with 151 samples it is the slowest one or two requests and this run's max was a single 789.5 ms outlier. There was no A/B run, so a small per-request cost cannot be excluded, and a tail increase cannot be ruled out either: the honest reading is "no p95 change visible, p99 within the noise of one outlier". Saga time is unchanged at about 0.5 s. Accept latency remains the number to watch.

## Deviations from Plan

### Auto-fixed / handled issues

**1. [Rule 3 - Blocking, environment] Kafka OOM-killed during the rebuild**
- **Found during:** Task 2 (first full metrics run, three failures)
- **Issue:** Docker VM memory pressure during the seven image builds killed `smarthealth-kafka-1` (exit 137, OOMKilled); replication slots inactive; compose_healthcheck still printed OK.
- **Fix:** `docker compose start kafka` (no recreate, no volume change); verified Kafka healthy, 8 connectors RUNNING, 4 slots active, `compose_healthcheck.sh` OK; reran only the three affected tests (passed). Raised here because the instruction asked to stop and report if a service is unhealthy after a recreate: the recreated services were healthy, Kafka was an indirect victim and the action was a plain start of an exited container.
- **Files modified:** none. **Commit:** none.

**2. [Minor] Task 1 and 2 commits split the single e2e test file**
The pid-1 tests were written before the Task 1 commit; they were set aside and committed in Task 2's own commit (5ee7667). `SINGLE_PROCESS_ROLES` is defined in the Task 1 commit and first used in Task 2.

**3. [Minor] Read-back time**
The resets file is evaluated at the run's end as specified; the extra quantile read at end plus 30 s was only for a final scrape and is not the file's content. The earlier procedure script `setpw.py` in the scratchpad (from 03-16) was reused for the password step.

Otherwise the plan was executed as written; REQUIREMENTS.md untouched, no requirement ticked, Compose not edited, no package installed, `.env*` and `infra/secrets/*` not read.

## Semgrep (after commit)

`/Library/Frameworks/Python.framework/Versions/3.14/bin/semgrep --metrics=off`, separate file arguments:
- `p/python` on `tests/libs/test_multiprocess_metrics.py` and `tests/e2e/test_phase3_metrics.py`: 0 findings.
- `p/security-audit` on the same two files: 4 findings, all `dynamic-urllib-use-detected` in `tests/e2e/test_phase3_metrics.py` (the existing fixed-local-URL `urlopen` calls, unchanged from plan 03-24, `# noqa: S310`). My new tests add none.
- Dockerfile `missing-user` findings on the seven Dockerfiles: accepted by the user, untouched.
- No Python source changed beyond tests.

## Known Stubs

None.

## Threat Flags

None: no new endpoint, port or schema; T-3-79 mitigated by the static Compose test and the live pid-1 checks; T-3-80 followed (password never printed or stored in the repo, deleted); T-3-81 tjo-* unchanged.

## Self-Check: PASSED

- Commits 50c65a9, 5ee7667, 34c9954 present; measured commits since plan start: 3.
- All seven Dockerfiles, both test files, `docs/nfr.md`, `loadtest/README.md` modified as listed; `closeout-logs/gap-smoke.txt` and `gap-smoke-resets.txt` exist (untracked); password file and `loadtest/data/plan.json` are gone.
