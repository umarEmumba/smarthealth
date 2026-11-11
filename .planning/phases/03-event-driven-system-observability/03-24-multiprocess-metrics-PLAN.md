---
phase: 03-event-driven-system-observability
plan: 24
type: execute
wave: 17
depends_on: ["03-23"]
files_modified:
  - libs/observability/gunicorn_conf.py
  - libs/observability/http_metrics.py
  - services/gateway/app.py
  - services/gateway/Dockerfile
  - services/scheduling/Dockerfile
  - services/billing/Dockerfile
  - services/analytics/Dockerfile
  - services/identity/Dockerfile
  - services/patient/Dockerfile
  - services/provider/Dockerfile
  - services/organization/Dockerfile
  - services/visit/Dockerfile
  - tests/libs/test_multiprocess_metrics.py
  - tests/e2e/test_phase3_metrics.py
autonomous: true
gap_closure: true
requirements: [OBS-01]

estimate:
  tokens: 85000
  raw_tokens: 85000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "Gap 2 (CR-02, OBS-01): each of the nine gunicorn-served APIs (gateway, billing, analytics, identity, patient, provider, organization, scheduling, visit) keeps two workers (user decision: multiprocess mode, not one worker) and answers every /metrics scrape with the SUM over both workers, built from a prometheus_client MultiProcessCollector registry"
    - "The multiprocess directory is exported only by the API image's default command (never in Compose, so the workers and consumers that reuse the same image with a command override keep their single-process registry), is created if missing and emptied by the gunicorn master before any worker starts, and a gunicorn child_exit hook calls multiprocess.mark_process_dead"
    - "The gateway's hand-written /metrics route (internal :9100 listener only, 404 on :8000) renders the multiprocess registry through the shared helper in libs/observability/http_metrics.py"
    - "After traffic and with no container restart in the window, sum by job of resets(http_requests_total[W]) is 0 for all nine API jobs (W up to 5 minutes, bounded by container uptime) and sum(http_requests_total) per job never decreases across the scrapes of the window; before the fix the same check fails (recorded red)"
    - "The Temporal workers, the three Kafka consumers and the Celery worker are unaffected: their processes have no multiprocess directory in their environment and every Prometheus target stays up"
  artifacts:
    - path: libs/observability/gunicorn_conf.py
      provides: "on_starting (create and empty the multiprocess directory) and child_exit (mark_process_dead)"
      contains: "mark_process_dead"
    - path: libs/observability/http_metrics.py
      provides: "exposition_registry() and render_latest(): MultiProcessCollector registry when the directory variable is set, the process registry otherwise"
      contains: "MultiProcessCollector"
    - path: tests/libs/test_multiprocess_metrics.py
      provides: "hook, aggregation and static Dockerfile/Compose tests"
    - path: tests/e2e/test_phase3_metrics.py
      provides: "test_http_request_counters_have_no_resets_and_never_decrease (parametrized over the nine API jobs)"
  key_links:
    - from: "services/*/Dockerfile default command"
      to: libs/observability/gunicorn_conf.py
      via: "gunicorn -c python:libs.observability.gunicorn_conf, with the directory variable exported in the same shell before exec"
      pattern: "python:libs.observability.gunicorn_conf"
    - from: services/gateway/app.py
      to: libs/observability/http_metrics.py
      via: "metrics_endpoint returns render_latest()"
      pattern: "render_latest"
    - from: "Prometheus job smarthealth-<api>"
      to: "both gunicorn workers"
      via: "MultiProcessCollector sums the per-pid files in the directory"
      pattern: "MultiProcessCollector"
---

<objective>
Close verification gap 2 (CR-02): every gunicorn-served API runs two workers and `prometheus_client` keeps one registry per process, so each scrape is answered by whichever worker accepts it; Prometheus sees thousands of counter resets per job and the HTTP rate, 5xx ratio and latency panels (including the NFR dashboard D-04 relies on) are wrong. Per the user's decision, keep two workers and switch the nine APIs to Prometheus multiprocess mode.

Purpose: OBS-01 and D-11 require metrics that are correct, not just exposed; D-04 needs trustworthy histograms before the deferred `sustained` / `burst` / `contention` runs. This plan also redeploys the nine APIs, so they pick up the code of plans 03-21 and 03-23 and the `x-app-env` pool values of plan 03-23.

Output: one gunicorn config and one exposition helper in `libs/observability`, nine Dockerfile commands, the gateway route, unit/static tests, a live no-reset e2e check, nine rebuilt API containers.

Out of scope and accepted (record in the SUMMARY): the Semgrep `dockerfile.security.missing-user` findings on these Dockerfiles stay accepted by the user and are not touched; the k6 smoke rerun is plan 03-25.

Live stack rules:
- Rebuild only the API services with `docker compose -f infra/docker-compose.yml up -d --build --no-deps <names>`; the gateway with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` in front of that command (the e2e runs need the raised budget; plan 03-25 restores 20 and 300 as its last step). Never `down -v`, never remove a volume, never touch `tjo-*` containers (record their status before and after). Compose is not edited by this plan.
- The API containers run Alembic on start (already applied; a no-op) and become healthy within the existing healthcheck.
- One pytest process at a time; the e2e no-reset check waits for at least 2 minutes of container uptime by itself (see Task 1), so run it in the background with polling if the wait is long.
- No `.env*` or `infra/secrets/*` reads, no package installs (prometheus-client 0.26.0, prometheus-fastapi-instrumentator 8.1.0 and gunicorn 26.2.0 are already installed; the instrumentator switches to `MultiProcessCollector` by itself when the directory variable is set, see its `expose()`).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-VERIFICATION.md
@.planning/phases/03-event-driven-system-observability/03-REVIEW.md
@.planning/phases/03-event-driven-system-observability/03-15-metrics-scrape-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md
@libs/observability/http_metrics.py
@services/gateway/app.py
@services/gateway/Dockerfile
@services/scheduling/Dockerfile
@tests/e2e/test_phase3_metrics.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `libs/observability/gunicorn_conf.py` | gunicorn config module | `on_starting(server)`: when the multiprocess directory variable is set, create the directory and delete every file in it (master, before workers fork); `child_exit(server, worker)`: `multiprocess.mark_process_dead(worker.pid)` when the variable is set |
| `exposition_registry()` | function in `http_metrics.py` | a fresh `CollectorRegistry` with `MultiProcessCollector` when the variable is set, else the default `REGISTRY` |
| `render_latest() -> bytes` | function in `http_metrics.py` | `generate_latest(exposition_registry())` |
| API image default command | Dockerfile CMD | existing Alembic step (where present), then export the directory variable as `/tmp/prometheus-multiproc`, `mkdir -p` it, `exec gunicorn -c python:libs.observability.gunicorn_conf -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2}` with the existing binds and app path |
| `test_http_request_counters_have_no_resets_and_never_decrease` | e2e test | parametrized over the nine API jobs |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the gateway and scheduling scrapes sum both workers and their HTTP counters stop resetting</name>
  <reversibility rating="reversible">An image command and a helper; going back is the previous CMD line.</reversibility>
  <precondition>All nine API targets are up in Prometheus (`tests/e2e/test_phase3_metrics.py::test_every_job_and_target_is_up` passes) before any change.</precondition>
  <files>libs/observability/gunicorn_conf.py, libs/observability/http_metrics.py, services/gateway/app.py, services/gateway/Dockerfile, services/scheduling/Dockerfile, tests/libs/test_multiprocess_metrics.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - `on_starting` with the directory variable pointing at a temporary directory that holds stale `counter_123.db` and `histogram_456.db` files leaves the directory existing and empty; with the variable unset it does nothing
    - `child_exit` calls `prometheus_client.multiprocess.mark_process_dead` with the worker's pid when the variable is set (monkeypatched recorder), and nothing otherwise
    - With the variable set to a temporary directory holding two per-pid counter files for the same `http_requests_total` series (values 3 and 4, written with `prometheus_client.mmap_dict.MmapedDict` and `mmap_key`), `render_latest()` contains that series with value 7; with the variable unset `exposition_registry()` is the default `REGISTRY`
    - The gateway still answers `/metrics` only on its internal metrics port and 404 on the public port (existing gateway tests pass)
    - Static: the gateway and scheduling Dockerfile commands export the directory variable, run `mkdir -p` on it, pass `-c python:libs.observability.gunicorn_conf`, keep `-w ${WEB_CONCURRENCY:-2}` and keep their binds and app path
    - Live e2e `test_http_request_counters_have_no_resets_and_never_decrease[smarthealth-gateway]` and `[smarthealth-scheduling]`: FAIL before the rebuild (recorded), PASS after it
  </behavior>
  <read_first>
    - libs/observability/http_metrics.py (whole file; `instrument_http` and its registry argument)
    - services/gateway/app.py (`metrics_endpoint` lines 51-58, the route registration near line 133)
    - services/gateway/Dockerfile and services/scheduling/Dockerfile (the CMD lines and their comments)
    - .venv/lib/python3.12/site-packages/prometheus_fastapi_instrumentator/instrumentation.py lines 160-170 and 280-300 (the directory check at construction and the multiprocess branch of `expose`)
    - .venv/lib/python3.12/site-packages/prometheus_client/multiprocess.py and mmap_dict.py (`MultiProcessCollector`, `mark_process_dead`, `MmapedDict`, `mmap_key`)
    - tests/e2e/test_phase3_metrics.py (`prometheus_url`, `prom_query`, `wait_until`, `_stack_ready`, the `gateway` fixture, `test_every_job_and_target_is_up`)
    - tests/gateway (the tests that assert /metrics is 404 on the public port and served on the metrics port)
  </read_first>
  <action>
    Create `libs/observability/gunicorn_conf.py` with the two hooks from the artifacts table and a module docstring (why: two workers, one registry each, CR-02; the variable is exported by the image's default command only, so command-override processes of the same image stay single-process; it must be set before gunicorn starts because `prometheus_client` picks its value class at import). The module must not set the variable itself.

    In `libs/observability/http_metrics.py` add `exposition_registry()` and `render_latest()` (artifacts table) and extend the module docstring. In `services/gateway/app.py`, `metrics_endpoint` returns `Response(content=render_latest(), media_type=CONTENT_TYPE_LATEST)`; the listener check stays exactly as it is.

    Change the gateway and scheduling Dockerfile commands to the shape in the artifacts table (gateway: no Alembic step, both `-b` binds kept) and update their comment lines (two workers stay; multiprocess mode is why each scrape is the sum).

    Tests: `tests/libs/test_multiprocess_metrics.py` covers the hook, aggregation and static behaviors in-process (monkeypatch the environment; no subprocess). In `tests/e2e/test_phase3_metrics.py` add a `prom_query_range` helper next to `prom_query` and the parametrized e2e test (gateway and scheduling in this task; Task 2 extends it to all nine): read the container start time of the API (`docker compose -f infra/docker-compose.yml ps -q <service>` then `docker inspect -f '{{.State.StartedAt}}'`), wait until it has been up at least 135 s, generate traffic through the gateway with the `gateway` fixture (a seeded world plus a few authenticated reads), wait two scrape intervals (30 s), set W = min(300, uptime - 30) seconds, require `count(http_requests_total{job="J"}) > 0`, `sum(resets(http_requests_total{job="J"}[Ws])) == 0`, and a non-decreasing series from a range query of `sum(http_requests_total{job="J"})` over the last W seconds at a 15 s step.

    Run the new e2e test for the two jobs BEFORE rebuilding and record the failure (red). Then rebuild: `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --build --no-deps gateway scheduling`, wait for both healthy, confirm with `docker compose -f infra/docker-compose.yml exec -T scheduling sh -c 'tr "\0" "\n" < /proc/1/environ | grep -c PROMETHEUS_MULTIPROC_DIR'` that the gunicorn master has the variable, and run the e2e test again (green).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/libs/test_http_metrics.py tests/gateway -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "no_resets and (gateway or scheduling)" -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a gateway or scheduling scrape still shows a counter reset or a decreasing sum inside the window, the gateway serves /metrics on its public port, or the gunicorn master of the rebuilt container lacks the directory variable.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY quotes the red run (resets above 0 for at least one of the two jobs) and the green run
    - `grep -c "python:libs.observability.gunicorn_conf" services/gateway/Dockerfile services/scheduling/Dockerfile` prints 1 for each file
    - `grep -c "render_latest" services/gateway/app.py` prints at least 1
    - `docker compose -f infra/docker-compose.yml ps gateway scheduling --format '{{.Service}} {{.Health}}'` shows both healthy
  </acceptance_criteria>
  <done>The two APIs that feed the NFR dashboard report correct, monotonic counters with two workers each.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The other seven APIs in multiprocess mode, single-process roles unaffected, the whole metrics suite green</name>
  <files>services/billing/Dockerfile, services/analytics/Dockerfile, services/identity/Dockerfile, services/patient/Dockerfile, services/provider/Dockerfile, services/organization/Dockerfile, services/visit/Dockerfile, tests/libs/test_multiprocess_metrics.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Static: all nine API Dockerfile commands have the multiprocess shape (artifacts table) with their own Alembic step and app path unchanged; `infra/docker-compose.yml` sets neither the directory variable nor a gunicorn worker-count variable, and no Compose service that overrides `command` gets the directory variable
    - Live: the e2e no-reset test passes for all nine jobs
    - Live: the gunicorn master (pid 1) of each of the nine API containers has the variable; `scheduling-worker`, `visit-worker`, `notification-temporal-worker`, `visit-consumer`, `analytics-consumer`, `notification-consumer` and `notification-celery` have no such variable in pid 1's environment (a parametrized e2e check)
    - Live: every Prometheus target is up and the existing metrics e2e tests (`test_every_job_and_target_is_up`, `test_nfr_histograms_present`, the operations and provisioning checks) pass
  </behavior>
  <read_first>
    - services/billing/Dockerfile, services/analytics/Dockerfile, services/identity/Dockerfile, services/patient/Dockerfile, services/provider/Dockerfile, services/organization/Dockerfile, services/visit/Dockerfile (the CMD lines differ only by Alembic path and app module)
    - infra/docker-compose.yml (which services override `command`; `environment: *visit-env` style aliases share the API's environment, which is why the variable must not live in Compose)
    - tests/libs/test_multiprocess_metrics.py and the e2e test from Task 1
  </read_first>
  <action>
    Apply the Task 1 command shape to the seven Dockerfiles. Extend the static test to all nine Dockerfiles and to Compose (parse with merge keys; assert no service's environment carries the directory variable or a worker-count variable) and the e2e parametrization to the nine jobs, plus a parametrized check of pid 1's environment in the nine API containers (present) and the seven single-process containers listed in the behaviors (absent), read with `docker compose -f infra/docker-compose.yml exec -T <service> sh -c 'tr "\0" "\n" < /proc/1/environ'` and matched in Python.

    Rebuild the seven: `docker compose -f infra/docker-compose.yml up -d --build --no-deps billing analytics identity patient provider organization visit`, wait until each is healthy, then run the static tests and the whole `tests/e2e/test_phase3_metrics.py` file (background with polling; the no-reset test waits for uptime by itself) and `tests/e2e/test_phase2_flows.py` as a quick end-to-end regression through the rebuilt APIs.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/infra -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_phase2_flows.py -m e2e -q</automated>
    <fails_when>Either pytest run exits non-zero, any of the nine API jobs shows a counter reset or a decreasing sum in the window, a single-process role has the directory variable, or a Prometheus target is down.</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -l "python:libs.observability.gunicorn_conf" services/*/Dockerfile | wc -l` prints 9
    - `docker compose -f infra/docker-compose.yml ps gateway billing analytics identity patient provider organization scheduling visit --format '{{.Service}} {{.Health}}'` shows nine healthy lines
    - the SUMMARY records, per job, the window W used and the resets value (0) from the final run
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>All nine APIs report correct HTTP metrics with two workers each, and every single-process role is unchanged.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Prometheus -> API /metrics | internal scrape over the Compose network; the gateway's metrics listener is never published |
| gunicorn workers -> multiprocess directory | each worker writes its metric values to files the scrape aggregates |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-73 | Information Disclosure | gateway `/metrics` after the switch to the multiprocess registry | medium | mitigate | The listener check is unchanged (internal :9100 only, 404 on :8000); existing gateway tests rerun; labels stay handler / method / status / job (no ids, T-3-48) |
| T-3-74 | Tampering | stale per-pid files from an earlier run inflating counters | medium | mitigate | `on_starting` empties the directory in the master before any worker starts; `child_exit` marks dead workers; tested in-process |
| T-3-75 | Denial of Service | file growth in the directory (one file per pid and metric type) | low | accept | Bounded by worker restarts between container starts; the directory is emptied at every start; no `max_requests` recycling is configured |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed; all three libraries already pinned and installed |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/libs tests/infra tests/gateway -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_phase2_flows.py tests/e2e/test_gateway_smoke.py -m e2e -q`
- Semgrep: `semgrep --config p/python --metrics=off libs/observability services/gateway/app.py`; the nine Dockerfiles keep the user-accepted `missing-user` findings (record, do not fix)
</verification>

<success_criteria>
- CR-02 closed: two workers per API, every scrape is the sum, no counter resets, monotonic sums, proven live for all nine jobs
- Workers, consumers and Celery unaffected; all targets up
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-SUMMARY.md` when done
</output>
