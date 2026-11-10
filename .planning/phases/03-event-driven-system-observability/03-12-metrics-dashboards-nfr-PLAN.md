---
phase: 03-event-driven-system-observability
plan: 12
type: execute
wave: 6
depends_on: ["03-09", "03-10", "03-11"]
files_modified:
  - libs/config.py
  - libs/temporal.py
  - services/scheduling/worker.py
  - services/scheduling/activities/booking.py
  - services/visit/worker.py
  - services/notification/temporal_worker.py
  - infra/prometheus.yml
  - infra/docker-compose.yml
  - infra/postgres-init/create-service-databases.sh
  - infra/grafana/provisioning/datasources/datasources.yml
  - infra/grafana/provisioning/dashboards/smarthealth.yml
  - infra/grafana/dashboards/smarthealth-booking-nfr.json
  - infra/grafana/dashboards/smarthealth-events.json
  - infra/grafana/dashboards/smarthealth-notifications.json
  - infra/grafana/dashboards/smarthealth-operations.json
  - tests/infra/test_grafana_provisioning.py
  - tests/infra/test_prometheus_config.py
  - tests/e2e/test_phase3_metrics.py
autonomous: true
requirements: [OBS-01, ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05]

estimate:
  tokens: 110000
  raw_tokens: 110000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Every service process exposes Prometheus metrics: the HTTP APIs, the four consumers, the Celery worker, the three Temporal workers (application metrics and Temporal SDK metrics) and RabbitMQ; Prometheus scrapes them all and reports every target up (OBS-01)"
    - "Grafana dashboards and datasources are provisioned from files in the repo, not built by hand (D-11), with critical-flow dashboards for booking/NFR, events/consumers, notifications and the five operational metrics"
    - "The NFR-relevant paths have histograms: booking accept HTTP latency, saga completion (booking_saga_seconds), outbox insert, event end-to-end lag and notification delivery; the NFR dashboard's thresholds come from loadtest/nfr.json (D-04)"
    - "The k6 smoke profile passes on the finished Phase 3 stack (D-04); sustained, burst and contention stay deferred"
    - "No new metrics port is published to the host; the Grafana Postgres datasource uses the read-only role with a file-provided password"
  artifacts:
    - path: infra/grafana/provisioning/datasources/datasources.yml
      provides: "Prometheus (uid prometheus), analytics Postgres via grafana_ro (uid analytics-pg), Jaeger (uid jaeger)"
      contains: "$__file{"
    - path: infra/grafana/dashboards/smarthealth-booking-nfr.json
      provides: "NFR-03/04/05 latency panels, error rate, outbox insert, saga histogram"
      contains: "booking_saga_seconds"
    - path: infra/prometheus.yml
      provides: "scrape jobs for every process"
      contains: "notification-celery"
    - path: libs/temporal.py
      provides: "metrics_runtime(bind_address) and connect_client(..., runtime=...)"
      contains: "PrometheusConfig"
  key_links:
    - from: infra/grafana/dashboards/smarthealth-booking-nfr.json
      to: loadtest/nfr.json
      via: "threshold steps equal book_p95_ms, read_p95_ms, confirm_p95_s, error_rate_max (test-enforced)"
      pattern: "nfr.json"
    - from: services/scheduling/activities/booking.py
      to: infra/prometheus.yml
      via: "booking_saga_seconds served on scheduling-worker:9100, scraped by job smarthealth-scheduling-worker"
      pattern: "scheduling-worker:9100"
---

<objective>
Make the whole system observable through metrics: application and Temporal SDK metrics from every worker, scrape jobs for every process, file-provisioned Grafana datasources and dashboards for the critical flows and the five operational metrics, NFR histograms, and a smoke run on the finished stack.

Purpose: roadmap success criterion 4 (metrics half) and OBS-01; D-04 asks for histograms on the NFR paths so the deferred k6 runs can be read in Grafana, and for the smoke profile to keep passing. The operations dashboard is the operator's Grafana view of ANLY-01..05 (the API from plan 03-09 is the other).

Output: Temporal metrics runtime helper, worker metrics servers, `booking_saga_seconds`, Prometheus scrape jobs, Grafana provisioning and four dashboards, static and e2e tests, k6 smoke result.

Decisions honoured: D-04 (histograms on NFR paths; smoke must pass; sustained/burst/contention deferred by the user), D-11 (every service exposes /metrics; dashboards as code), research Pattern 10 and Pitfall 4 (replication-slot retained-WAL panel through a SQL datasource, assumption A9). Claude's discretion: internal ports 9100 (application metrics) and 9464 (Temporal SDK metrics), dashboards authored as classic JSON (assumption A1), `allowUiUpdates: false`.

Live stack rules: rebuild the three Temporal workers with `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling-worker visit-worker notification-temporal-worker`; recreate Grafana with `docker compose -f infra/docker-compose.yml up -d --no-deps grafana` and restart Prometheus with `docker compose -f infra/docker-compose.yml restart prometheus` (both scoped to this project). Never touch `tjo-*` containers (record status before and after). This plan runs alone in wave 6 so the smoke measurement is on a quiet stack.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@infra/prometheus.yml
@infra/docker-compose.yml
@libs/temporal.py
@services/scheduling/worker.py
@services/scheduling/activities/booking.py
@services/gateway/app.py
@loadtest/nfr.json
@loadtest/README.md
@docs/nfr.md
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `SharedSettings.worker_metrics_port` (9100), `.temporal_sdk_metrics_bind` (`0.0.0.0:9464`) | settings | env `WORKER_METRICS_PORT`, `TEMPORAL_SDK_METRICS_BIND` |
| `metrics_runtime(bind_address) -> Runtime`, `connect_client(*, lazy=True, runtime=None)` | functions | `libs/temporal.py`; one runtime per process |
| `booking_saga_seconds{outcome}` | histogram | observed in the confirm activity (`confirmed`) and the mark-failed activity (`failed`) from `appointments.created_at`; buckets cover 0.1 s to 900 s |
| worker metrics | code | scheduling-worker, visit-worker, notification-temporal-worker: `start_http_server(worker_metrics_port)` + SDK metrics on 9464 |
| Prometheus jobs | config | `smarthealth-analytics` (analytics:8000), `smarthealth-scheduling-worker` (:9100, :9464), `smarthealth-visit-worker` (:9100, :9464), `smarthealth-notification-temporal-worker` (:9100, :9464), `smarthealth-visit-consumer`, `smarthealth-analytics-consumer`, `smarthealth-notification-consumer`, `smarthealth-notification-celery` (each :9100), `rabbitmq` (rabbitmq:15692) |
| Grafana provisioning | infra | `infra/grafana/provisioning/{datasources/datasources.yml,dashboards/smarthealth.yml}`, dashboards under `infra/grafana/dashboards/` with uids `smarthealth-booking-nfr`, `smarthealth-events`, `smarthealth-notifications`, `smarthealth-operations` |
| Compose grafana | infra | mounts provisioning and dashboards read-only, secret `grafana_ro_password` as `/run/secrets/grafana_ro.secret`, depends on prometheus and analytics |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — one booking's saga time is recorded in the worker, scraped by Prometheus and charted by a provisioned Grafana dashboard</name>
  <files>libs/config.py, libs/temporal.py, services/scheduling/worker.py, services/scheduling/activities/booking.py, infra/prometheus.yml, infra/docker-compose.yml, infra/grafana/provisioning/datasources/datasources.yml, infra/grafana/provisioning/dashboards/smarthealth.yml, infra/grafana/dashboards/smarthealth-booking-nfr.json, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - After a booking through the gateway confirms, Prometheus (`localhost:9090/api/v1/query`) returns `booking_saga_seconds_count{job="smarthealth-scheduling-worker",outcome="confirmed"}` greater than 0 within 60 s
    - The scheduling worker's SDK endpoint is scraped: at least one series whose name starts with `temporal_` has `job="smarthealth-scheduling-worker"`
    - The Grafana container log contains no provisioning error lines and reports the dashboards provider as loaded; `GET localhost:3000/api/health` returns 200
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 10, Code Examples "Temporal client with Prometheus metrics" and "Grafana provisioning")
    - libs/temporal.py (`connect_client`), services/scheduling/worker.py (`_connect_with_retry`, start-up order)
    - services/scheduling/activities/booking.py (confirm activity `ACT_CONFIRM` and mark-failed activity `ACT_MARK_FAILED`; names and signatures must not change)
    - infra/prometheus.yml (job naming and the closing comment that says workers have no scrape job)
    - infra/docker-compose.yml (`grafana` and `prometheus` blocks; secrets)
    - loadtest/nfr.json (threshold values)
  </read_first>
  <action>
    Settings: add `worker_metrics_port` and `temporal_sdk_metrics_bind` to `SharedSettings`. `libs/temporal.py`: `metrics_runtime(bind_address)` returns a cached `Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=...)))`; `connect_client` gains an optional `runtime` argument passed to `Client.connect`. Scheduling worker: connect with `runtime=metrics_runtime(settings.temporal_sdk_metrics_bind)` and call `prometheus_client.start_http_server(settings.worker_metrics_port)` once before running. booking.py: define `booking_saga_seconds` (Histogram, label `outcome`) and observe `now - appointments.created_at` inside the confirm activity when it confirms and inside the mark-failed activity when it marks failed (read `created_at` in the activity's existing session; no new query on the API path). Activity names, inputs and outputs unchanged; no workflow change.

    Prometheus: add the two `smarthealth-scheduling-worker` targets (`scheduling-worker:9100`, `scheduling-worker:9464`) and replace the closing "no scrape job" comment with the current truth.

    Grafana: `datasources.yml` (apiVersion 1) with the Prometheus datasource (uid `prometheus`, url `http://prometheus:9090`, default); `dashboards/smarthealth.yml` provider per the research example (`type: file`, `allowUiUpdates: false`, path `/var/lib/grafana/dashboards`); `smarthealth-booking-nfr.json` (classic dashboard JSON, uid `smarthealth-booking-nfr`) with panels: booking accept p95 and p99 from the scheduling job's HTTP request duration histogram for the POST `/api/appointments` handler (NFR-03), read p95 for the appointment and slot GET handlers (NFR-04), saga p95/p99 from `booking_saga_seconds` (NFR-05), 5xx ratio at the gateway (NFR-02), each with threshold steps set to the `loadtest/nfr.json` values (`book_p95_ms`, `book_p99_ms`, `read_p95_ms`, `confirm_p95_s`, `confirm_p99_s`, `error_rate_max`) converted to seconds. Compose: mount `./grafana/provisioning:/etc/grafana/provisioning:ro` and `./grafana/dashboards:/var/lib/grafana/dashboards:ro` on `grafana`.

    e2e (marker e2e): book through the gateway, then query Prometheus and read `docker compose -f infra/docker-compose.yml logs grafana` for the behaviors.

    Apply: rebuild `scheduling-worker` with `--no-deps`, recreate `grafana`, restart `prometheus`.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, the saga histogram has no sample after a confirmed booking, no `temporal_` series is scraped, or the Grafana log shows a provisioning error.</fails_when>
  </verify>
  <acceptance_criteria>
    - the tracer test passes
    - `grep -c "scheduling-worker:9100" infra/prometheus.yml` is 1 and `grep -c "scheduling-worker:9464" infra/prometheus.yml` is 1
    - `.venv/bin/python -m pytest tests/workflows -q` exits 0 (activity changes did not alter any workflow)
    - tjo-* status lines show no restart
  </acceptance_criteria>
  <done>Saga latency flows from an activity through Prometheus into a provisioned Grafana dashboard.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Every process scraped, all datasources and dashboards provisioned, and the provisioning checked by tests</name>
  <files>services/visit/worker.py, services/notification/temporal_worker.py, infra/prometheus.yml, infra/docker-compose.yml, infra/postgres-init/create-service-databases.sh, infra/grafana/provisioning/datasources/datasources.yml, infra/grafana/dashboards/smarthealth-events.json, infra/grafana/dashboards/smarthealth-notifications.json, infra/grafana/dashboards/smarthealth-operations.json, tests/infra/test_grafana_provisioning.py, tests/infra/test_prometheus_config.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Static: every dashboard JSON parses, has a unique uid and title, and every panel's datasource uid exists in `datasources.yml`; every Prometheus metric name used in a panel expression is in a known list of names this codebase emits (consumer base, notification task, outbox, saga, HTTP instrumentator, Temporal SDK prefix, RabbitMQ prefix); the NFR dashboard thresholds equal `loadtest/nfr.json`; the operations dashboard has one panel per metric (total patients, appointments booked per day, completed visits, cancellation rate, average wait) querying the analytics views; the provider path matches the Compose mount
    - Static: every `prometheus.yml` target host is a Compose service and every scraped port has no host mapping in Compose (except the services already published before Phase 3)
    - Live: `localhost:9090/api/v1/targets` reports every job's targets `up`
    - Live: the Postgres datasource can run the replication-slot query (`slot_name`, `active`, retained bytes from `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) as `grafana_ro`
  </behavior>
  <read_first>
    - infra/grafana/* and infra/prometheus.yml (Task 1)
    - services/visit/worker.py, services/notification/temporal_worker.py (connection code to switch to `metrics_runtime`)
    - libs/events/consumer.py, services/notification/tasks.py, libs/events/outbox.py (exact metric names)
    - services/analytics/migrations/versions/analytics_0001_projections.py (view names and columns)
    - infra/postgres-init/create-service-databases.sh (grafana_ro grants from plan 03-02)
  </read_first>
  <action>
    Workers: visit-worker and notification-temporal-worker connect with `metrics_runtime(...)` and start the application metrics server, like the scheduling worker.

    Prometheus: add the remaining jobs from the artifacts table. Datasources: add `analytics-pg` (type `grafana-postgresql-datasource`, url `postgres:5432`, database `analytics_db`, user `grafana_ro`, `secureJsonData.password: $__file{/run/secrets/grafana_ro.secret}`, `sslmode: disable` for the Compose network, documented) and `jaeger` (url `http://jaeger:16686`). Compose: give `grafana` the `grafana_ro_password` secret and a dependency on `analytics` healthy.

    Dashboards: `smarthealth-events.json` (consumer lag per group and partition, processing p95, end-to-end lag p95, DLQ counts by group and error type, outbox insert p95 by event type, replication-slot retained bytes and active flag from `analytics-pg`), `smarthealth-notifications.json` (sent, skipped, retried, dead-lettered rates by type, delivery p95, RabbitMQ queue depth for the four notification queues from the RabbitMQ exporter, Temporal workflow and activity rates and latencies from the SDK metrics), `smarthealth-operations.json` (the five metrics from the analytics views with a clinic variable sourced from `select distinct clinic_id from appointments`, plus the definitions in panel descriptions). If `grafana_ro` cannot read `pg_replication_slots` or call `pg_current_wal_lsn()`, add `GRANT pg_monitor TO grafana_ro` in db-init with a comment (read-only monitoring role; no table data) and re-run db-init with `docker compose -f infra/docker-compose.yml up -d db-init`.

    Tests: `tests/infra/test_grafana_provisioning.py` and `tests/infra/test_prometheus_config.py` (static, no stack) for the static behaviors; add the live target and slot-query checks to `tests/e2e/test_phase3_metrics.py`.

    Apply: rebuild `visit-worker` and `notification-temporal-worker` with `--no-deps`, recreate `grafana`, restart `prometheus`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a dashboard references a missing datasource or unknown metric, an NFR threshold differs from nfr.json, any Prometheus target is down, or the slot query is refused.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/infra -q` exits 0 with at least 8 passed
    - `curl -s localhost:9090/api/v1/targets | .venv/bin/python -c "import json,sys; t=json.load(sys.stdin)['data']['activeTargets']; bad=[x['labels']['job'] for x in t if x['health']!='up']; print(len(t), bad); sys.exit(1 if bad else 0)"` exits 0 and prints at least 20 targets
    - `ls infra/grafana/dashboards/*.json | wc -l` prints 4
    - `grep -c '$__file{/run/secrets/grafana_ro.secret}' infra/grafana/provisioning/datasources/datasources.yml` is 1
  </acceptance_criteria>
  <done>Every process is scraped and up, and four provisioned dashboards cover booking/NFR, events, notifications and operations.</done>
</task>

<task type="auto">
  <name>Task 3: NFR histograms present and the k6 smoke profile on the finished Phase 3 stack</name>
  <files>tests/e2e/test_phase3_metrics.py</files>
  <read_first>
    - loadtest/README.md (smoke run, raised gateway budget) and tests/e2e/test_loadtest_smoke.py (self-seeding wrapper)
    - docs/nfr.md (what is a target versus measured; sustained/burst/contention stay deferred)
  </read_first>
  <action>
    Add `test_nfr_histograms_present` to `tests/e2e/test_phase3_metrics.py`: after one booking, cancel and visit completion, Prometheus has samples for the scheduling HTTP request duration histogram on POST `/api/appointments`, `booking_saga_seconds_bucket`, `outbox_insert_seconds_bucket`, `event_end_to_end_lag_seconds_bucket` for each of the three consumer groups, and `notification_delivery_seconds_bucket`.

    Then run the smoke profile on the quiet stack with the gateway budget raised for the run only: `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`, `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q`, then `docker compose -f infra/docker-compose.yml up -d --no-deps gateway` to restore the defaults. The wrapper creates its own test admin and world; do not create or hardcode any other admin credential (a manual `loadtest/run.sh smoke` with a user-provided admin is part of the end-of-phase human check in plan 03-15). Record in the SUMMARY: k6 accept p95/p99, saga p95/p99 and error rate, the same quantiles read from Prometheus for the run window, and the 2026-11-10 baseline (accept p95 21.8 ms, saga p95 0.52 s). Do not run sustained, burst or contention (deferred by the user until after Phase 3).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q</automated>
    <fails_when>pytest exits non-zero, a named NFR histogram has no samples, or k6 reports a crossed smoke threshold.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -q` exits 0 with at least 4 passed
    - `tests/e2e/test_loadtest_smoke.py` passes on the Phase 3 stack
    - the SUMMARY records k6 and Prometheus quantiles next to the baseline
    - the gateway runs with its default rate budget after the run
  </acceptance_criteria>
  <done>The NFR paths are instrumented end to end and the smoke profile passes on the complete Phase 3 stack.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Prometheus -> every process's metrics port | internal scrape over the Compose network |
| host browser -> Grafana (:3000) | dashboards over aggregate data |
| Grafana -> analytics_db | read-only SQL |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-45 | Information Disclosure | metrics endpoints | medium | mitigate | No new host port mappings (static test); labels carry service, type and outcome names, never ids or PHI |
| T-3-46 | Elevation of Privilege | Grafana database access | medium | mitigate | `grafana_ro` with SELECT by default privileges on analytics_db only (plus `pg_monitor` only if the slot panel needs it); password from a Compose secret via `$__file{}`; no write role exists for Grafana |
| T-3-47 | Tampering | dashboards edited in the UI drifting from the repo | low | mitigate | `allowUiUpdates: false`, read-only mounts, static tests on the JSON |
| T-3-48 | Denial of Service | metrics cardinality | low | mitigate | No per-id labels; partition-level lag only; histogram bucket sets fixed |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed; `prometheus-client` was approved in plan 03-01 |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/infra tests/workflows -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto libs/temporal.py services/scheduling services/visit services/notification infra`; record status in the SUMMARY
</verification>

<success_criteria>
- All processes scraped and up; four file-provisioned dashboards including the five operational metrics
- NFR histograms present; smoke passes on the finished stack
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-12-SUMMARY.md` when done
</output>
