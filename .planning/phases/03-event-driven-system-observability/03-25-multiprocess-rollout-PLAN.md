---
phase: 03-event-driven-system-observability
plan: 25
type: execute
wave: 18
depends_on: ["03-24"]
files_modified:
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
autonomous: true
gap_closure: true
requirements: [OBS-01]

estimate:
  tokens: 80000
  raw_tokens: 80000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 2 (CR-02, OBS-01) for all nine APIs: billing, analytics, identity, patient, provider, organization and visit run the same multiprocess image command as plan 03-24's scheduling and gateway (two workers kept, D-14), so every one of the nine gunicorn-served API jobs answers each scrape with the sum over both workers"
    - "Traffic reached every one of the nine API jobs and was fully counted (W9): for each job the e2e test drives 40 requests into that API container's own /health and the job's summed http_requests_total rises by at least 40; over the window sum(resets(http_requests_total{job=J}[W])) is 0 and the summed counter never decreases; the seven newly switched jobs are run before their rebuild and the failure is recorded (red)"
    - "No Compose service carries the multiprocess directory variable or a gunicorn worker-count variable; pid 1 of each of the nine API containers has the directory variable, pid 1 of scheduling-worker, visit-worker, notification-temporal-worker, visit-consumer, analytics-consumer, notification-consumer and notification-celery does not; every Prometheus target is up and the existing metrics and Phase 2 flow e2e tests pass"
    - "The k6 smoke profile (only smoke; sustained, burst and contention stay deferred, D-04) passes on the stack with all nine APIs in multiprocess mode, run with the plan 03-16 admin procedure; its exit status (0) and the per-job resets over the smoke window (0 for each of the nine API jobs) are written to log files that the verify reads; the run is also the accept-path overhead check for multiprocess metrics (I10), recorded next to the 2026-11-10 baseline and the plan 03-16 runs in docs/nfr.md and loadtest/README.md"
  artifacts:
    - path: tests/e2e/test_phase3_metrics.py
      provides: "MULTIPROCESS_API_JOBS covering all nine API jobs; pid-1 environment checks for API and single-process roles"
      contains: "smarthealth-organization"
    - path: tests/libs/test_multiprocess_metrics.py
      provides: "static checks of all nine API Dockerfile commands and of Compose (no directory or worker-count variable in any service)"
    - path: docs/nfr.md
      provides: "the gap-closure smoke run, dated, next to the earlier runs, with the accept-path comparison"
    - path: .planning/phases/03-event-driven-system-observability/closeout-logs/gap-smoke-resets.txt
      provides: "one line per API job: resets over the smoke window (untracked log, read by the verify)"
  key_links:
    - from: "services/{billing,analytics,identity,patient,provider,organization,visit}/Dockerfile default command"
      to: libs/observability/gunicorn_conf.py
      via: "gunicorn -c python:libs.observability.gunicorn_conf, with the directory variable exported in the same shell before exec"
      pattern: "python:libs.observability.gunicorn_conf"
    - from: "Prometheus jobs of the seven APIs"
      to: "both gunicorn workers"
      via: "the instrumentator's expose() switches to MultiProcessCollector when the directory variable is set"
      pattern: "MultiProcessCollector"
    - from: "loadtest/run.sh smoke"
      to: ".planning/phases/03-event-driven-system-observability/closeout-logs/gap-smoke.txt"
      via: "k6 output followed by a final k6_exit=<status> line"
      pattern: "k6_exit="
---

<objective>
Finish closing verification gap 2 (CR-02) for the remaining seven APIs and rerun the k6 smoke on the corrected stack. Plan 03-24 built the shared pieces (`libs/observability/gunicorn_conf.py`, the exposition helper, the e2e no-reset test with driven traffic) and proved them on scheduling and the gateway; this plan applies the same image command to billing, analytics, identity, patient, provider, organization and visit, proves that traffic reached each of the nine jobs and was counted in full, proves the single-process roles are untouched, and reruns the `smoke` profile (user decision: smoke only).

Purpose: OBS-01 and D-11 require metrics that are correct for every API, not two of them; D-04 requires the smoke profile to keep passing after the Phase 3 changes, and CR-02 made the earlier smoke's Prometheus view untrustworthy, so this rerun is the first one read from correct counters. It is also the accept-path overhead check for multiprocess mode (I10): each metric update in a worker is now a write to a memory-mapped file, and the smoke's accept p95 / p99 are compared with the 2026-11-10 baseline (p95 21.8 ms) and the plan 03-16 runs. The seven rebuilt APIs also pick up the code of plans 03-21..03-23 and the `x-app-env` pool values of plan 03-23.

Output: seven Dockerfile commands, the static and e2e tests extended to nine jobs, pid-1 environment checks, seven rebuilt API containers, a recorded smoke run with its exit status and per-job resets, and dated entries in `docs/nfr.md` and `loadtest/README.md`.

Out of scope and accepted (record in the SUMMARY): the Semgrep `dockerfile.security.missing-user` findings on these Dockerfiles stay accepted by the user and are not touched (deferred in 03-CONTEXT.md).

Live stack rules:
- Rebuild only the seven APIs with `docker compose -f infra/docker-compose.yml up -d --build --no-deps billing analytics identity patient provider organization visit`. The visit and analytics images are shared with `visit-worker`, `visit-consumer` and `analytics-consumer` (command overrides): `--no-deps` leaves those containers as they are, and they never receive the directory variable because only the API image's default command exports it. Never `down -v`, never remove a volume, never touch `tjo-*` containers (record their status before and after). Compose is not edited by this plan.
- Gateway budget rule: the e2e runs and the smoke need the raised gateway budget. Before the first run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`. Plan 03-26 restores the defaults (20 and 300) as its last automated step; do not restore them here.
- One pytest process at a time, and none while the smoke runs; long runs in the background with polling and a 20 s wall-clock heartbeat (a gap above 60 s means the laptop slept: rerun).
- Load test credentials: the plan 03-16 procedure only (Task 3). Never `sustained`, `burst` or `contention`.
- No direct reads of `.env*` or `infra/secrets/*` (settings modules loading `.env` implicitly is fine), no package installs, no REQUIREMENTS.md edits. Logs under `closeout-logs/` stay untracked (the directory is not committed).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-VERIFICATION.md
@.planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md
@libs/observability/gunicorn_conf.py
@tests/libs/test_multiprocess_metrics.py
@tests/e2e/test_phase3_metrics.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| seven API image default commands | Dockerfile CMD | the plan 03-24 shape: the existing Alembic step, then export the directory variable as `/tmp/prometheus-multiproc`, `mkdir -p` it, `exec gunicorn -c python:libs.observability.gunicorn_conf -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2}` with the existing bind and app path |
| `MULTIPROCESS_API_JOBS` | e2e constant | all nine: gateway, identity, organization, provider, patient, scheduling, billing, visit, analytics (job `smarthealth-<name>`, Compose service `<name>`) |
| `test_api_pid1_has_the_multiprocess_directory` / `test_single_process_role_pid1_has_no_multiprocess_directory` | e2e tests | pid 1 environment read with `docker compose -f infra/docker-compose.yml exec -T <service> sh -c 'tr "\0" "\n" < /proc/1/environ'`, matched in Python; nine present, seven absent |
| `closeout-logs/gap-smoke.txt` | log (untracked) | k6 stdout and stderr, then one final line `k6_exit=<status>` |
| `closeout-logs/gap-smoke-resets.txt` | log (untracked) | exactly nine lines `resets job=<job> value=<integer>` from `sum by (job)(resets(http_requests_total{job=~"<the nine API jobs>"}[<W>s]))` evaluated at the smoke's end time, W = smoke duration plus 60 s |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the other seven APIs in multiprocess mode, and traffic driven into each of the nine jobs is counted in full with no resets</name>
  <reversibility rating="reversible">Image commands only; going back is the previous CMD line per Dockerfile.</reversibility>
  <precondition>Plan 03-24 is complete: `grep -l "python:libs.observability.gunicorn_conf" services/gateway/Dockerfile services/scheduling/Dockerfile | wc -l` prints 2, `libs/observability/gunicorn_conf.py` exists, and all nine API targets are up in Prometheus.</precondition>
  <files>services/billing/Dockerfile, services/analytics/Dockerfile, services/identity/Dockerfile, services/patient/Dockerfile, services/provider/Dockerfile, services/organization/Dockerfile, services/visit/Dockerfile, tests/libs/test_multiprocess_metrics.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Static: all nine API Dockerfile commands have the multiprocess shape (artifacts table), each with its own Alembic step (where it had one), bind and app path unchanged
    - Static: `infra/docker-compose.yml`, parsed with merge keys resolved, gives no service an environment that contains the multiprocess directory variable or a gunicorn worker-count variable (so the command-override roles sharing an API image stay single-process)
    - Red: `test_http_request_counters_have_no_resets_and_never_decrease` parametrized over the seven new jobs, run BEFORE their rebuild, fails for at least one of them (resets above 0 or an increase below the 40 driven requests); recorded with the per-job values
    - Green: after the rebuild the same test passes for all nine jobs: each job's summed counter rose by at least 40 after its own 40 driven requests, zero resets in W, non-decreasing over W
  </behavior>
  <read_first>
    - services/billing/Dockerfile, services/analytics/Dockerfile, services/identity/Dockerfile, services/patient/Dockerfile, services/provider/Dockerfile, services/organization/Dockerfile, services/visit/Dockerfile (the CMD lines differ only by Alembic path and app module)
    - services/scheduling/Dockerfile (the plan 03-24 command to copy, with its comment lines)
    - tests/libs/test_multiprocess_metrics.py (the Dockerfile list the static check is parametrized over)
    - tests/e2e/test_phase3_metrics.py (`MULTIPROCESS_API_JOBS`, `_drive_requests`, `prom_query_range`, the no-reset test)
    - infra/docker-compose.yml (which services override `command`; `environment: *visit-env` style aliases share the API's environment, which is why the variable must not live in Compose)
  </read_first>
  <action>
    Apply the plan 03-24 command shape to the seven Dockerfiles (artifacts table), keeping each one's Alembic step, bind and app module, and update their comment lines the way plan 03-24 did for scheduling. Extend the static Dockerfile check to all nine Dockerfiles and add the Compose check from the behaviors (parse with `yaml.safe_load`; look at every service's resolved environment, whether a mapping or a list).

    Extend `MULTIPROCESS_API_JOBS` to the nine jobs. Run the no-reset e2e test for the seven new jobs BEFORE rebuilding and record the per-job result (red). Rebuild the seven (live stack rules), wait until each is healthy, then run the static tests and the no-reset e2e test for all nine jobs in the background with polling (each case waits for 135 s of uptime by itself).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k no_resets -x -q</automated>
    <fails_when>Either pytest run exits non-zero: a Dockerfile lacks the shape or lost its Alembic step, bind or app path, a Compose service carries the directory or a worker-count variable, or for any of the nine API jobs the summed counter rose by less than the 40 requests driven into it, a reset appears in the window, or the sum decreases.</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -l "python:libs.observability.gunicorn_conf" services/*/Dockerfile | wc -l | tr -d ' '` prints 9
    - `docker compose -f infra/docker-compose.yml ps gateway billing analytics identity patient provider organization scheduling visit --format '{{.Service}} {{.Health}}'` shows nine healthy lines
    - the SUMMARY records the red run for the seven jobs and, per job, the window W, the measured increase and the resets value (0) of the green run
  </acceptance_criteria>
  <done>All nine APIs report correct HTTP metrics with two workers each, and the test proves that traffic reached every job.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Single-process roles unaffected; the whole metrics suite and the Phase 2 flows green on the rebuilt APIs</name>
  <files>tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Live: pid 1 of each of the nine API containers has the multiprocess directory variable; pid 1 of `scheduling-worker`, `visit-worker`, `notification-temporal-worker`, `visit-consumer`, `analytics-consumer`, `notification-consumer` and `notification-celery` does not (two parametrized e2e tests, artifacts table)
    - Live: every Prometheus target is up and the existing metrics e2e tests (`test_every_job_and_target_is_up`, `test_nfr_histograms_present`, the Temporal SDK series, operations and provisioning checks) pass unchanged
    - Live: `tests/e2e/test_phase2_flows.py` passes through the rebuilt APIs (quick end-to-end regression)
  </behavior>
  <read_first>
    - tests/e2e/test_phase3_metrics.py (`EXPECTED_JOBS`, `TEMPORAL_WORKER_JOBS`, the tests named in the behaviors)
    - tests/e2e/phase3_stack.py (`compose`)
  </read_first>
  <action>
    Add the two pid-1 environment tests (artifacts table) to `tests/e2e/test_phase3_metrics.py`; the environment is read and matched in Python, and only the variable's presence is asserted (no environment value is printed or asserted, so nothing secret reaches the output). Then run `tests/infra`, the whole `tests/e2e/test_phase3_metrics.py` file and `tests/e2e/test_phase2_flows.py` in the background with polling (live stack rules; gateway budget rule applied first).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_phase2_flows.py -m e2e -q</automated>
    <fails_when>Either pytest run exits non-zero: an API's pid 1 lacks the directory variable, a single-process role's pid 1 has it, a Prometheus target is down, an existing metrics test fails, or a Phase 2 flow fails through the rebuilt APIs.</fails_when>
  </verify>
  <acceptance_criteria>
    - the e2e run shows the nine API and seven single-process parametrized cases passed (`-rA` lines or the pytest summary counts, quoted in the SUMMARY)
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>Every single-process role is unchanged, every target is up, and the rebuilt APIs still serve the Phase 2 flows.</done>
</task>

<task type="auto">
  <name>Task 3: k6 smoke on the corrected stack, exit status and per-job resets recorded to files the verify reads, docs updated</name>
  <precondition>Tasks 1 and 2 passed (nine API containers healthy in multiprocess mode); the live gateway prints 1000 and 100000 for its two rate-limit variables; no pytest process is running.</precondition>
  <files>docs/nfr.md, loadtest/README.md</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md (deviation 1: the admin procedure; the k6 and Prometheus figures and how they were read)
    - loadtest/README.md (seed options, `run.sh` profiles, the earlier smoke entries) and docs/nfr.md (the smoke section)
    - scripts/loadtest_seed.py (reads `LOADTEST_ADMIN_EMAIL` / `LOADTEST_ADMIN_PASSWORD`, prints counts only)
    - services/identity/security/passwords.py (`hash_password`) and services/identity/repository/models.py (`users.hashed_password`, `role`)
    - tests/docs/test_phase3_docs.py (`test_the_nfr_page_records_the_phase3_smoke_runs_and_keeps_the_unmeasured_status`)
    - tests/e2e/test_phase3_metrics.py (`prom_query`: the instant-query helper to reuse for the resets read-back)
  </read_first>
  <action>
    Quiet stack first: `scripts/compose_healthcheck.sh` OK, no pytest process running; record the load average. Admin credential, plan 03-16 procedure only: with `.venv/bin/python`, generate a password with `secrets.token_urlsafe(24)`, hash it with `services.identity.security.passwords.hash_password`, and run one UPDATE of `users.hashed_password` WHERE `email = 'loadtest-admin@example.com' AND role = 'admin'` on identity_db through the host `IdentitySettings` URL; abort unless exactly one row changed; write the password to a 0600 file in the executor's scratchpad directory; never print it. Seed with `LOADTEST_ADMIN_EMAIL=loadtest-admin@example.com LOADTEST_ADMIN_PASSWORD="$(cat <that file>)" .venv/bin/python scripts/loadtest_seed.py --providers 2 --patients 250 --days 2`.

    Run the smoke with its output captured and its exit status recorded as the last line of the log: with `L=.planning/phases/03-event-driven-system-observability/closeout-logs`, run `loadtest/run.sh smoke > "$L/gap-smoke.txt" 2>&1; echo "k6_exit=$?" >> "$L/gap-smoke.txt"`, and note the UTC start and end times. Delete the password file and `loadtest/data/plan.json` immediately afterwards and confirm both are gone. Check that `gap-smoke.txt` contains neither the password, nor a `Bearer ` value, nor a JWT (`eyJ`); the directory is untracked and the file is never committed. Do not run `sustained`, `burst` or `contention` (D-04, deferred by the user).

    Read back from Prometheus for the run window (reuse the e2e module's `prom_query` with the instant query's `time` set to the run's end): write `$L/gap-smoke-resets.txt` with exactly nine lines `resets job=<job> value=<integer>` from `sum by (job)(resets(http_requests_total{job=~"<the nine API jobs>"}[<W>s]))`, W = run duration plus 60 s (a job with no series is written as `value=missing`, which fails the verify); also read the accept p95 / p99 (`http_request_duration_seconds`, scheduling, `POST /api/appointments`), saga p95 / p99 (`booking_saga_seconds`) and the gateway 5xx ratio. Add a dated entry for this run to `docs/nfr.md` and `loadtest/README.md` next to the earlier runs: k6 figures, the Prometheus figures (the first ones read from multiprocess counters), host load, and the I10 comparison of accept p95 / p99 with the 2026-11-10 baseline (p95 21.8 ms) and the plan 03-16 runs; keep every existing figure and the statements that the NFRs are not yet measured and that the three other profiles were not run.
  </action>
  <verify>
    <automated>L=.planning/phases/03-event-driven-system-observability/closeout-logs && tail -n 1 "$L/gap-smoke.txt" | grep -qx "k6_exit=0" && .venv/bin/python -c "import re,sys; rows=[re.fullmatch(r'resets job=(\S+) value=(\S+)', l) for l in open(sys.argv[1]).read().splitlines() if l.strip()]; jobs=sorted('smarthealth-' + j for j in 'gateway identity organization provider patient scheduling billing visit analytics'.split()); ok=all(rows) and sorted(m.group(1) for m in rows) == jobs and all(m.group(2) == '0' for m in rows); print('resets ok' if ok else 'resets NOT ok'); sys.exit(0 if ok else 1)" "$L/gap-smoke-resets.txt" && test ! -e loadtest/data/plan.json && .venv/bin/python -m pytest tests/docs/test_phase3_docs.py tests/loadtest -q</automated>
    <fails_when>The smoke log is missing or its last line is not k6_exit=0 (k6 failed a threshold or errored), the resets file is missing, lacks one of the nine API jobs, repeats a job, or shows any value other than 0, the plan file was left behind, or the docs tests fail (a missing earlier figure, a missing "not run" / "not yet measured" statement).</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY records the k6 accept p95 / p99, saga p95 / p99, error rate, iterations, the Prometheus figures, the nine resets values and the I10 comparison
    - the password file no longer exists and was never printed; only the `loadtest-admin@example.com` row was updated (UPDATE 1)
    - `git diff HEAD -- docs/nfr.md loadtest/README.md` adds figures and dates only (no credential, password or token), checked by reading the diff before the commit
  </acceptance_criteria>
  <done>The smoke profile passes on the stack with correct metrics for all nine APIs, its Prometheus view is trustworthy, and the accept-path cost of multiprocess mode is measured and recorded.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Prometheus -> API /metrics | internal scrape over the Compose network |
| image default command -> processes sharing the image | the directory variable decides which exposition a process uses |
| operator host -> identity_db | the smoke procedure rotates one admin password |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-79 | Tampering | the multiprocess directory variable reaching a single-process role (its custom metrics would vanish from its scrape, hiding consumer lag, DLQ and saga metrics) | medium | mitigate | Exported only by the API images' default command, never in Compose; a static test rejects it in any Compose environment; pid 1 of the seven single-process roles is checked live |
| T-3-80 | Information Disclosure | load-test admin password | high | mitigate | Random `token_urlsafe(24)`, hashed with the service's own function, one row (`email` and `role` both matched, UPDATE 1 required), 0600 file in the scratchpad, never printed, deleted with `loadtest/data/plan.json` right after the run; the smoke log is checked for the password, bearer values and JWTs and stays untracked |
| T-3-81 | Denial of Service | smoke load on the shared dev host disturbing unrelated containers | low | accept | Only the short, low-rate `smoke` profile on a quiet stack; tjo-* status recorded before and after |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/libs tests/infra -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_phase2_flows.py tests/e2e/test_gateway_smoke.py -m e2e -q`
- the Task 3 verify command (smoke exit status, nine zero resets, plan file gone, docs tests)
- Semgrep: the seven Dockerfiles keep the user-accepted `missing-user` findings (record, do not fix); no Python source changes beyond tests
</verification>

<success_criteria>
- CR-02 closed for all nine APIs: two workers each, every scrape is the sum, traffic reached and was fully counted for every job, no counter resets, monotonic sums
- Workers, consumers and Celery unaffected; all targets up
- Smoke passes on the corrected stack with its exit status and zero resets recorded; the multiprocess accept-path cost is measured; sustained, burst and contention still deferred
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-25-multiprocess-rollout-SUMMARY.md` when done
</output>
