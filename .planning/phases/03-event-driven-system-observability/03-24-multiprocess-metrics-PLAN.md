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
  - tests/libs/test_multiprocess_metrics.py
  - tests/e2e/test_phase3_metrics.py
autonomous: true
gap_closure: true
requirements: [OBS-01]

estimate:
  tokens: 70000
  raw_tokens: 70000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "Gap 2 (CR-02, OBS-01), first two APIs: scheduling and the gateway (the two that feed the NFR dashboard) keep two gunicorn workers (user decision D-14: multiprocess mode, not one worker) and answer every /metrics scrape with the SUM over both workers, built from a prometheus_client MultiProcessCollector registry; plan 03-25 applies the same command to the other seven APIs"
    - "The multiprocess directory is exported only by the API image's default command (never in Compose, so the workers and consumers that reuse the same image with a command override keep their single-process registry), is created if missing and emptied by the gunicorn master before any worker starts, and a gunicorn child_exit hook calls multiprocess.mark_process_dead"
    - "The gateway's hand-written /metrics route (internal :9100 listener only, 404 on :8000) renders the multiprocess registry through the shared helper in libs/observability/http_metrics.py"
    - "For the scheduling and gateway jobs, after the test drives a known number of requests into that API and with no container restart in the window: sum(http_requests_total{job=J}) rises by at least that number (the traffic really reached the job), sum(resets(http_requests_total{job=J}[W])) is 0 (W up to 5 minutes, bounded by uptime), and the summed counter never decreases across the window's scrapes; before each rebuild the same check fails (recorded red)"
    - "No dashboard, test or doc relies on the default process_* or python_* collectors of an API scrape (they are not part of a multiprocess exposition); checked by grep and recorded"
  artifacts:
    - path: libs/observability/gunicorn_conf.py
      provides: "on_starting (create and empty the multiprocess directory) and child_exit (mark_process_dead)"
      contains: "mark_process_dead"
    - path: libs/observability/http_metrics.py
      provides: "exposition_registry() and render_latest(): MultiProcessCollector registry when the directory variable is set, the process registry otherwise"
      contains: "MultiProcessCollector"
    - path: tests/libs/test_multiprocess_metrics.py
      provides: "hook, aggregation and static Dockerfile tests (gateway and scheduling; plan 03-25 extends to nine)"
    - path: tests/e2e/test_phase3_metrics.py
      provides: "test_http_request_counters_have_no_resets_and_never_decrease parametrized over MULTIPROCESS_API_JOBS (gateway and scheduling here), with per-job driven traffic and a minimum increase"
      contains: "MULTIPROCESS_API_JOBS"
  key_links:
    - from: "services/gateway/Dockerfile and services/scheduling/Dockerfile default command"
      to: libs/observability/gunicorn_conf.py
      via: "gunicorn -c python:libs.observability.gunicorn_conf, with the directory variable exported in the same shell before exec"
      pattern: "python:libs.observability.gunicorn_conf"
    - from: services/gateway/app.py
      to: libs/observability/http_metrics.py
      via: "metrics_endpoint returns render_latest()"
      pattern: "render_latest"
    - from: "Prometheus jobs smarthealth-scheduling and smarthealth-gateway"
      to: "both gunicorn workers"
      via: "MultiProcessCollector sums the per-pid files in the directory"
      pattern: "MultiProcessCollector"
---

<objective>
Close verification gap 2 (CR-02) for the first two APIs: every gunicorn-served API runs two workers and `prometheus_client` keeps one registry per process, so each scrape is answered by whichever worker accepts it; Prometheus sees thousands of counter resets per job and the HTTP rate, 5xx ratio and latency panels (including the NFR dashboard D-04 relies on) are wrong. Per the user's decision (D-14), keep two workers and switch to Prometheus multiprocess mode. This plan builds the shared pieces and proves them on the two APIs that feed the NFR dashboard: scheduling (whose `/metrics` comes from the instrumentator's `expose()`, which switches to the multiprocess collector by itself) and the gateway (whose hand-written `/metrics` route needs the shared exposition helper). Plan 03-25 applies the same image command to the other seven APIs and reruns the k6 smoke.

Purpose: OBS-01 and D-11 require metrics that are correct, not just exposed; D-04 needs trustworthy histograms before the deferred `sustained` / `burst` / `contention` runs. The two rebuilt APIs also pick up the code of plans 03-21 and 03-23 and the `x-app-env` pool values of plan 03-23.

Accept-path cost (I10): multiprocess mode turns each metric update in a worker into a write to a memory-mapped file. The k6 smoke rerun in plan 03-25 Task 3, on the stack with all nine APIs switched, is the check that this adds no measurable latency to the booking accept path (compared with the 2026-11-10 baseline and the plan 03-16 runs); this plan does not claim it.

Output: one gunicorn config and one exposition helper in `libs/observability`, two Dockerfile commands, the gateway route, unit and static tests, a live no-reset e2e check with driven traffic, two rebuilt API containers.

Out of scope and accepted (record in the SUMMARY): the Semgrep `dockerfile.security.missing-user` findings on these Dockerfiles stay accepted by the user and are not touched.

Live stack rules:
- Rebuild only `scheduling` (`docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling`) and the gateway with the raised budget in front of the command (`GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --build --no-deps gateway`; plan 03-26 restores 20 and 300 as its last automated step). Never `down -v`, never remove a volume, never touch `tjo-*` containers (record their status before and after). Compose is not edited by this plan.
- The scheduling image is shared with `scheduling-worker` (command override): rebuilding the image does not recreate the worker here (`--no-deps`), and the worker keeps its single-process registry because the directory variable is exported only by the API image's default command.
- The API containers run Alembic on start (already applied; a no-op) and become healthy within the existing healthcheck.
- One pytest process at a time; the e2e no-reset check waits for at least 135 s of container uptime by itself, so run it in the background with polling if the wait is long.
- No direct reads of `.env*` or `infra/secrets/*` (settings modules loading `.env` implicitly is fine), no package installs (prometheus-client 0.26.0, prometheus-fastapi-instrumentator 8.1.0 and gunicorn 26.2.0 are already installed; the instrumentator switches to `MultiProcessCollector` by itself when the directory variable is set, see its `expose()`).
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
| `MULTIPROCESS_API_JOBS` | e2e constant | Prometheus job to Compose service map of the APIs already switched: `smarthealth-scheduling: scheduling` (Task 1), `smarthealth-gateway: gateway` (Task 2); plan 03-25 adds the other seven |
| `_drive_requests(service, n)` | e2e helper | runs `n` GET requests to `http://127.0.0.1:8000/health` inside the API container through `stack.compose("exec", "-T", service, "python", "-c", ...)` (the image has Python and no curl, like the Compose healthcheck), so the traffic reaches that API itself whatever the gateway routes |
| `test_http_request_counters_have_no_resets_and_never_decrease` | e2e test | parametrized over `MULTIPROCESS_API_JOBS`: uptime >= 135 s; read `sum(http_requests_total{job=J})` after a fresh scrape; drive N = 40 requests; wait 30 s (two scrape intervals); require the sum to have risen by at least 40, `count(...) > 0`, `sum(resets(http_requests_total{job=J}[Ws])) == 0` with W = min(300, uptime - 30), and a non-decreasing range series (15 s step) over W |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the scheduling scrape sums both workers: driven traffic is fully counted and the HTTP counter stops resetting</name>
  <reversibility rating="reversible">An image command and a gunicorn config module; going back is the previous CMD line.</reversibility>
  <precondition>All nine API targets are up in Prometheus (`tests/e2e/test_phase3_metrics.py::test_every_job_and_target_is_up` passes) before any change.</precondition>
  <files>libs/observability/gunicorn_conf.py, services/scheduling/Dockerfile, tests/libs/test_multiprocess_metrics.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - `on_starting` with the directory variable pointing at a temporary directory that holds stale `counter_123.db` and `histogram_456.db` files leaves the directory existing and empty; with the variable unset it does nothing
    - `child_exit` calls `prometheus_client.multiprocess.mark_process_dead` with the worker's pid when the variable is set (monkeypatched recorder), and nothing otherwise
    - Static: the scheduling Dockerfile command keeps its Alembic step, exports the directory variable, runs `mkdir -p` on it, passes `-c python:libs.observability.gunicorn_conf`, keeps `-w ${WEB_CONCURRENCY:-2}`, its bind and its app path
    - Live e2e `test_http_request_counters_have_no_resets_and_never_decrease[smarthealth-scheduling]` (artifacts table: 40 requests driven into the scheduling container, sum rises by at least 40, zero resets in W, non-decreasing): FAIL before the rebuild (recorded), PASS after it
    - Grep check: no file under `infra/grafana`, `tests` or `docs` references `process_resident_memory_bytes`, `process_cpu_seconds_total` or `python_gc_` for an API job (a multiprocess exposition does not include the default process collectors); recorded in the SUMMARY
  </behavior>
  <read_first>
    - services/scheduling/Dockerfile (the CMD line and its comments)
    - .venv/lib/python3.12/site-packages/prometheus_fastapi_instrumentator/instrumentation.py lines 160-170 and 280-300 (the directory check at construction and the multiprocess branch of `expose`)
    - .venv/lib/python3.12/site-packages/prometheus_client/multiprocess.py (`MultiProcessCollector`, `mark_process_dead`)
    - tests/e2e/test_phase3_metrics.py (`prometheus_url`, `prom_query`, `wait_until`, `_stack_ready`, `test_every_job_and_target_is_up`) and tests/e2e/phase3_stack.py (`compose`)
    - infra/docker-compose.yml `x-app-healthcheck` (the python-only `/health` probe pattern the traffic helper mirrors)
  </read_first>
  <action>
    Create `libs/observability/gunicorn_conf.py` with the two hooks from the artifacts table and a module docstring (why: two workers, one registry each, CR-02; the variable is exported by the image's default command only, so command-override processes of the same image stay single-process; it must be set before gunicorn starts because `prometheus_client` picks its value class at import). The module must not set the variable itself.

    Change the scheduling Dockerfile command to the shape in the artifacts table (Alembic step first, unchanged) and update its comment lines (two workers stay; multiprocess mode is why each scrape is the sum).

    Tests: `tests/libs/test_multiprocess_metrics.py` covers the hook and static behaviors in-process (monkeypatch the environment; no subprocess); write the static Dockerfile check as a helper parametrized over a module-level list of Dockerfiles (scheduling now; Task 2 and plan 03-25 extend it). In `tests/e2e/test_phase3_metrics.py` add `MULTIPROCESS_API_JOBS`, `_drive_requests`, a `prom_query_range` helper next to `prom_query`, and the parametrized e2e test exactly as the artifacts table describes (container start time from `docker compose -f infra/docker-compose.yml ps -q <service>` and `docker inspect -f '{{.State.StartedAt}}'`; wait for a fresh scrape before the first read). The minimum increase (40) is what makes the test prove that the traffic reached the job and that both workers' counts are in the scrape.

    Run the new e2e test for scheduling BEFORE rebuilding and record the failure (red). Then rebuild scheduling (live stack rules), wait for healthy, confirm with `docker compose -f infra/docker-compose.yml exec -T scheduling sh -c 'tr "\0" "\n" < /proc/1/environ | grep -c PROMETHEUS_MULTIPROC_DIR'` that the gunicorn master has the variable and with the same check on `scheduling-worker` that it does not, run the grep check from the behaviors, and run the e2e test again (green).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/libs/test_http_metrics.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "no_resets and scheduling" -x -q</automated>
    <fails_when>Either pytest run exits non-zero: the driven 40 requests do not all appear in the scheduling job's summed counter, the scrape still shows a counter reset or a decreasing sum inside the window, a hook test fails, or the static Dockerfile check fails; or (recorded in the action) the gunicorn master of the rebuilt container lacks the directory variable or scheduling-worker has it.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY quotes the red run (resets above 0 or an increase below 40) and the green run
    - `grep -c "python:libs.observability.gunicorn_conf" services/scheduling/Dockerfile` prints 1
    - `docker compose -f infra/docker-compose.yml ps scheduling scheduling-worker --format '{{.Service}} {{.Health}}'` shows both healthy
  </acceptance_criteria>
  <done>The busiest API reports correct, monotonic counters that include every request it served, with two workers.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The gateway — its hand-written /metrics renders the multiprocess registry through a shared helper, and its counters stop resetting</name>
  <reversibility rating="reversible">A helper, one route body and an image command; going back is the previous route body and CMD line.</reversibility>
  <files>libs/observability/http_metrics.py, services/gateway/app.py, services/gateway/Dockerfile, tests/libs/test_multiprocess_metrics.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - With the variable set to a temporary directory holding two per-pid counter files for the same `http_requests_total` series (values 3 and 4, written with `prometheus_client.mmap_dict.MmapedDict` and `mmap_key`), `render_latest()` contains that series with value 7; with the variable unset `exposition_registry()` is the default `REGISTRY`
    - The gateway still answers `/metrics` only on its internal metrics port and 404 on the public port (existing gateway tests pass unchanged)
    - Static: the gateway Dockerfile command (no Alembic step) exports the directory variable, runs `mkdir -p` on it, passes `-c python:libs.observability.gunicorn_conf`, keeps `-w ${WEB_CONCURRENCY:-2}`, both `-b` binds and its app path
    - Live e2e `test_http_request_counters_have_no_resets_and_never_decrease[smarthealth-gateway]` (40 requests driven into the gateway container's own `/health`, sum rises by at least 40, zero resets, non-decreasing): FAIL before the rebuild (recorded), PASS after it; the scheduling case from Task 1 still passes
  </behavior>
  <read_first>
    - libs/observability/http_metrics.py (whole file; `instrument_http` and its registry argument)
    - services/gateway/app.py (`metrics_endpoint` lines 51-58, the route registration near line 133)
    - services/gateway/Dockerfile (the CMD line and its comments)
    - .venv/lib/python3.12/site-packages/prometheus_client/multiprocess.py and mmap_dict.py (`MultiProcessCollector`, `MmapedDict`, `mmap_key`)
    - tests/gateway (the tests that assert /metrics is 404 on the public port and served on the metrics port)
    - tests/libs/test_multiprocess_metrics.py and the e2e test from Task 1
  </read_first>
  <action>
    In `libs/observability/http_metrics.py` add `exposition_registry()` and `render_latest()` (artifacts table) and extend the module docstring (multiprocess exposition when the directory variable is set; the instrumentator's `expose()` does the same by itself for the other APIs). In `services/gateway/app.py`, `metrics_endpoint` returns `Response(content=render_latest(), media_type=CONTENT_TYPE_LATEST)`; the listener check stays exactly as it is.

    Change the gateway Dockerfile command to the artifacts-table shape (no Alembic step, both binds kept) and update its comment lines. Add the aggregation test and the gateway Dockerfile to `tests/libs/test_multiprocess_metrics.py`, and `smarthealth-gateway: gateway` to `MULTIPROCESS_API_JOBS`.

    Run the e2e test for the gateway BEFORE rebuilding and record the failure (red). Then rebuild the gateway with the raised budget (live stack rules), wait for healthy, confirm pid 1 of the gateway has the directory variable (same `/proc/1/environ` check as Task 1), and run the e2e test for both jobs (green).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/libs/test_http_metrics.py tests/gateway -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "no_resets and (gateway or scheduling)" -x -q</automated>
    <fails_when>Either pytest run exits non-zero: the aggregation test does not sum the two per-pid files, the gateway serves /metrics on its public port, the driven 40 requests do not all appear in the gateway's (or scheduling's) summed counter, or a reset or decrease appears inside the window.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY quotes the gateway's red run and the green run for both jobs, with the window W, the measured increase and the resets value per job
    - `grep -c "python:libs.observability.gunicorn_conf" services/gateway/Dockerfile` prints 1 and `grep -c "render_latest" services/gateway/app.py` prints at least 1
    - `docker compose -f infra/docker-compose.yml ps gateway scheduling --format '{{.Service}} {{.Health}}'` shows both healthy
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>The two APIs that feed the NFR dashboard report correct, monotonic counters with two workers each, and the gateway's internal-only metrics listener is unchanged.</done>
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
| T-3-76 | Information Disclosure | gateway `/metrics` after the switch to the multiprocess registry | medium | mitigate | The listener check is unchanged (internal :9100 only, 404 on :8000); existing gateway tests rerun; labels stay handler / method / status / job (no ids, T-3-48) |
| T-3-77 | Tampering | stale per-pid files from an earlier run inflating counters | medium | mitigate | `on_starting` empties the directory in the master before any worker starts; `child_exit` marks dead workers; tested in-process |
| T-3-78 | Denial of Service | file growth in the directory (one file per pid and metric type) | low | accept | Bounded by worker restarts between container starts; the directory is emptied at every start; no `max_requests` recycling is configured |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed; all three libraries already pinned and installed |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/libs tests/infra tests/gateway -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_gateway_smoke.py -m e2e -q`
- Semgrep: `semgrep --config p/python --metrics=off libs/observability services/gateway/app.py`; the two Dockerfiles keep the user-accepted `missing-user` findings (record, do not fix)
</verification>

<success_criteria>
- CR-02 closed for scheduling and the gateway: two workers each, every scrape is the sum, driven traffic fully counted, no counter resets, monotonic sums, proven live (red before, green after)
- The shared gunicorn config, the exposition helper and the test scaffolding are ready for plan 03-25 to roll out to the other seven APIs; scheduling-worker unaffected
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-SUMMARY.md` when done
</output>
