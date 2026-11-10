---
phase: 03-event-driven-system-observability
plan: 15
type: execute
wave: 8
depends_on: ["03-12", "03-13", "03-14"]
files_modified:
  - libs/config.py
  - libs/temporal.py
  - services/scheduling/worker.py
  - services/scheduling/activities/booking.py
  - services/visit/worker.py
  - services/notification/temporal_worker.py
  - infra/prometheus.yml
  - infra/docker-compose.yml
  - infra/grafana/provisioning/datasources/datasources.yml
  - infra/grafana/provisioning/dashboards/smarthealth.yml
  - infra/grafana/dashboards/smarthealth-booking-nfr.json
  - tests/infra/test_prometheus_config.py
  - tests/e2e/test_phase3_metrics.py
autonomous: true
requirements: [OBS-01]

estimate:
  tokens: 85000
  raw_tokens: 85000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "Every service process exposes Prometheus metrics: the HTTP APIs (including analytics), the three Kafka consumers (visit-consumer, analytics-consumer, notification-consumer), the Celery worker (notification-celery), the three Temporal workers (application metrics and Temporal SDK metrics) and RabbitMQ; Prometheus scrapes them all and reports every target up (OBS-01)"
    - "Grafana's provisioning is file-based, not built by hand (D-11): the Prometheus datasource, the dashboards provider and the booking/NFR dashboard here; the events, notifications and operations dashboards and the analytics and Jaeger datasources in plan 03-16"
    - "Saga completion has a histogram (booking_saga_seconds) beside the existing HTTP request histograms; the NFR dashboard's thresholds come from loadtest/nfr.json (D-04)"
    - "No new metrics port is published to the host (static test)"
  artifacts:
    - path: infra/grafana/provisioning/datasources/datasources.yml
      provides: "Prometheus datasource (uid prometheus); plan 03-16 adds analytics-pg and jaeger"
      contains: "uid: prometheus"
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
Make every process measurable: application and Temporal SDK metrics from every worker, scrape jobs for every process, file-based Grafana provisioning with the Prometheus datasource, and the booking/NFR dashboard fed by the new saga histogram. Plan 03-16 adds the remaining datasources and dashboards (events, notifications, the five operational metrics), the NFR histogram check and the smoke run.

Purpose: roadmap success criterion 4 (metrics half) and OBS-01; D-04 asks for histograms on the NFR paths so the deferred k6 runs can be read in Grafana.

Output: Temporal metrics runtime helper, worker metrics servers, `booking_saga_seconds`, Prometheus scrape jobs for every process, Grafana provisioning (Prometheus datasource, dashboards provider, booking/NFR dashboard), a static Prometheus-config test and e2e checks.

Decisions honoured: D-04 (histograms on NFR paths), D-11 (every service exposes /metrics; dashboards as code), research Pattern 10. Claude's discretion: internal ports 9100 (application metrics) and 9464 (Temporal SDK metrics), dashboards authored as classic JSON (assumption A1), `allowUiUpdates: false`.

Live stack rules: rebuild the three Temporal workers with `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling-worker visit-worker notification-temporal-worker`; recreate Grafana with `docker compose -f infra/docker-compose.yml up -d --no-deps grafana` and restart Prometheus with `docker compose -f infra/docker-compose.yml restart prometheus` (both scoped to this project). Never touch `tjo-*` containers (record status before and after). This plan runs alone in wave 8 (and is the only wave-8 editor of `infra/docker-compose.yml`); the smoke measurement is in plan 03-16, alone in wave 9.
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
| Grafana provisioning | infra | `infra/grafana/provisioning/{datasources/datasources.yml,dashboards/smarthealth.yml}`, dashboard `infra/grafana/dashboards/smarthealth-booking-nfr.json` (uid `smarthealth-booking-nfr`); plan 03-16 adds `smarthealth-events`, `smarthealth-notifications`, `smarthealth-operations` |
| Compose grafana | infra | mounts provisioning and dashboards read-only (plan 03-16 adds the `grafana_ro_password` secret and the analytics dependency) |
| `tests/infra/test_prometheus_config.py` | test | every target host is a Compose service; scraped ports have no host mapping |

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
  <name>Task 2: Every process scraped and up, checked by a static Prometheus-config test</name>
  <files>services/visit/worker.py, services/notification/temporal_worker.py, infra/prometheus.yml, tests/infra/test_prometheus_config.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Static: every `prometheus.yml` target host is a Compose service, every job in the artifacts table is present, and every scraped port has no host mapping in Compose (except the services already published before Phase 3)
    - Live: `localhost:9090/api/v1/targets` reports every job's targets `up`, including the three Kafka consumers, `notification-celery`, the three Temporal workers (both ports each), analytics and RabbitMQ
    - Live: at least one `temporal_` series is scraped for each of the three worker jobs
  </behavior>
  <read_first>
    - infra/prometheus.yml (Task 1)
    - services/visit/worker.py, services/notification/temporal_worker.py (connection code to switch to `metrics_runtime`)
    - libs/events/consumer.py (consumer `metrics_port`), services/notification/celery_app.py (worker_init metrics server), services/analytics/app.py (`/metrics`)
    - infra/docker-compose.yml (service names and existing host port mappings)
  </read_first>
  <action>
    Workers: visit-worker and notification-temporal-worker connect with `metrics_runtime(...)` and start the application metrics server, like the scheduling worker in Task 1.

    Prometheus: add the remaining jobs from the artifacts table (analytics API, visit and notification Temporal workers on both ports, the three Kafka consumers and the Celery worker on 9100, RabbitMQ on 15692).

    Tests: `tests/infra/test_prometheus_config.py` (static, no stack) for the static behaviors; add the live target and per-worker `temporal_` series checks to `tests/e2e/test_phase3_metrics.py`.

    Apply: rebuild `visit-worker` and `notification-temporal-worker` with `--no-deps`, restart `prometheus`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra/test_prometheus_config.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a target host is not a Compose service, a scraped port is published to the host, any Prometheus target is down, or a worker job has no `temporal_` series.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/infra/test_prometheus_config.py -q` exits 0 with at least 3 passed
    - `curl -s localhost:9090/api/v1/targets | .venv/bin/python -c "import json,sys; t=json.load(sys.stdin)['data']['activeTargets']; bad=[x['labels']['job'] for x in t if x['health']!='up']; print(len(t), bad); sys.exit(1 if bad else 0)"` exits 0 and prints at least 20 targets
    - `grep -c "notification-celery" infra/prometheus.yml` is at least 1
    - `.venv/bin/python -m pytest tests/workflows -q` exits 0 (worker changes did not alter any workflow)
  </acceptance_criteria>
  <done>Every process, including the three Kafka consumers, the Celery worker and the three Temporal workers, is scraped and up.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Prometheus -> every process's metrics port | internal scrape over the Compose network |
| host browser -> Grafana (:3000) | dashboards over aggregate data |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-45 | Information Disclosure | metrics endpoints | medium | mitigate | No new host port mappings (static test); labels carry service, type and outcome names, never ids or PHI |
| T-3-48 | Denial of Service | metrics cardinality | low | mitigate | No per-id labels; partition-level lag only; histogram bucket sets fixed |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed; `prometheus-client` was approved in plan 03-01 |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/infra tests/workflows -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto libs/temporal.py services/scheduling services/visit services/notification infra`; record status in the SUMMARY
</verification>

<success_criteria>
- All processes scraped and up, with no new host port
- The saga histogram reaches a file-provisioned booking/NFR dashboard whose thresholds equal loadtest/nfr.json
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-15-SUMMARY.md` when done
</output>
