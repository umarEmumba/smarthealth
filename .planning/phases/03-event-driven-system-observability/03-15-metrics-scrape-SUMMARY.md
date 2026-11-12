---
phase: 03-event-driven-system-observability
plan: 15
subsystem: observability
tags: [prometheus, grafana, temporal-sdk-metrics, histogram, provisioning, nfr, compose]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-04 EventConsumer metrics (:9100); 03-10/03-13 analytics service; 03-14 notification-celery (:9100) and the three notification Compose services; 03-01 prometheus-client"
provides:
  - "booking_saga_seconds{outcome} histogram (confirm and mark-failed activities), served by the scheduling worker"
  - "libs/temporal.py: metrics_runtime(bind_address), connect_client(..., runtime=), start_worker_metrics_server()"
  - "SharedSettings.worker_metrics_port (9100) and temporal_sdk_metrics_bind (0.0.0.0:9464)"
  - "20 Prometheus targets across 17 jobs, all up; no new host port"
  - "File-provisioned Grafana: Prometheus datasource (uid prometheus), dashboards provider, booking/NFR dashboard (uid smarthealth-booking-nfr)"
affects: [03-16, 03-17, 03-18, 03-19, 03-20]
status: complete
duration: about 1h20 wall time
completed: 2026-11-11
actuals:
  tokens: 19000
  tasks: 2
  commits: 2
plan_head_before: 7f5d701f6e57c52b2736d400f403f80ca9986b04
plan_head_after: a40cd40ee5af3b8115a973d8c3876a0e4e0c9ec7
tech-stack:
  added: []
  patterns:
    - "One cached Temporal SDK Runtime per bind address per process; workers pass it to Client.connect and the Worker inherits it"
    - "A metric observation never fails a saga step: read and observe after the state change commits, swallow and log a failed read"
    - "Empty provisioning directories carry a valid placeholder (apiVersion: 1) so a clean Grafana start logs no provisioning error"
key-files:
  created:
    - infra/grafana/provisioning/datasources/datasources.yml
    - infra/grafana/provisioning/dashboards/smarthealth.yml
    - infra/grafana/provisioning/plugins/plugins.yml
    - infra/grafana/provisioning/alerting/alerting.yml
    - infra/grafana/dashboards/smarthealth-booking-nfr.json
    - tests/infra/test_prometheus_config.py
    - tests/infra/test_grafana_provisioning.py
    - tests/e2e/test_phase3_metrics.py
  modified:
    - libs/config.py
    - libs/temporal.py
    - services/scheduling/worker.py
    - services/scheduling/activities/booking.py
    - services/visit/worker.py
    - services/notification/temporal_worker.py
    - infra/prometheus.yml
    - infra/docker-compose.yml
    - tests/workflows/test_reminder_workflow.py
key-decisions:
  - "Application metrics on 9100 and Temporal SDK metrics on 9464 per worker, both under ONE Prometheus job per worker (instance label tells them apart)"
  - "booking_saga_seconds measures now() - created_at on the DATABASE clock, so worker clock skew cannot distort NFR-05"
  - "Dashboard JSON generated once from loadtest/nfr.json and committed; a static test re-checks every threshold line against nfr.json"
requirements-completed: []
---

# Phase 3 Plan 15: Metrics Scrape Summary

**Every process is now measurable: Prometheus scrapes 20 targets (8 HTTP APIs plus analytics, the three Temporal workers on both an application and a Temporal SDK port, the three Kafka consumers, the Celery worker and RabbitMQ), all `up`, with no new host port; a booking's saga time flows from the confirm activity into `booking_saga_seconds`, through Prometheus, onto a booking/NFR Grafana dashboard that is provisioned entirely from files and whose threshold lines equal `loadtest/nfr.json`.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (the plan addresses OBS-01).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | e8b6ba4 | settings, `metrics_runtime` and `start_worker_metrics_server`, scheduling worker wiring, `booking_saga_seconds`, scheduling-worker scrape job, Grafana datasource/provider/dashboard, Compose mounts, tracer e2e, static dashboard test |
| 2 | a40cd40 | visit and notification Temporal worker wiring, the remaining 14 scrape jobs/targets, static Prometheus-config test, live target / `temporal_` / label-cardinality e2e |

## What was built

- **Saga histogram.** `BOOKING_SAGA_SECONDS` (label `outcome` only, buckets 0.1 s to 900 s) is observed in `confirm` (after its commit, outcome `confirmed`) and in `mark_failed` (only when it really moved the row, outcome `failed`), from `extract(epoch from now() - created_at)` read in the activity's existing session. Activity names, inputs and outputs are unchanged and no workflow changed. A failed read is logged (`booking.saga_seconds_unavailable`) and never fails the step.
- **Temporal SDK metrics.** `metrics_runtime(bind)` returns a cached `Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=bind)))`; the three workers connect with it, so `temporal_*` series (poll latency, task execution, sticky cache) are served on 9464. `start_worker_metrics_server()` serves the default prometheus_client registry on 9100, once per process, and tolerates an already-bound port.
- **Prometheus.** One job per process, the closing "no scrape job" comment replaced by a header listing which port serves what. Targets: `scheduling-worker`, `visit-worker`, `notification-temporal-worker` (each `:9100` and `:9464`), `visit-consumer`, `analytics-consumer`, `notification-consumer`, `notification-celery` (each `:9100`), `analytics:8000`, `rabbitmq:15692`.
- **Grafana as code.** `datasources.yml` (Prometheus, uid `prometheus`, default), `dashboards/smarthealth.yml` (`type: file`, `allowUiUpdates: false`, path `/var/lib/grafana/dashboards`), and the dashboard with eight panels: booking accept p95/p99 (NFR-03), read p95/p99 (NFR-04), saga p95/p99 (NFR-05), gateway 5xx ratio (NFR-02), outbox insert p95. Threshold steps: 0.3 s, 0.8 s, 0.2 s, 0.5 s, 5 s, 15 s, 0.01. Compose mounts `./grafana/provisioning` and `./grafana/dashboards` read-only on `grafana`.

## Live-stack results

- Rebuilt with `--no-deps`: `scheduling-worker`, `visit-worker`, `notification-temporal-worker` (all healthy). Grafana recreated (three times: mounts, then the two placeholder directories, see Deviations); Prometheus restarted (twice). Nothing else touched; no volume removed; the Prometheus data volume survived.
- `GET /api/v1/targets`: 20 active targets, zero not `up`.
- `booking_saga_seconds_count{job="smarthealth-scheduling-worker",outcome="confirmed"}` appeared in Prometheus after one gateway booking; `temporal_` series are present for all three worker jobs.
- Grafana log: "starting to provision dashboards" / "finished to provision dashboards", zero `level=error` or `level=warn` lines from provisioning; `/api/health` 200. A one-off authenticated read (compose default admin credential, nothing read from `.env`) confirmed `GET /api/dashboards/uid/smarthealth-booking-nfr` returns the provisioned dashboard and `GET /api/datasources` lists `prometheus`.
- `tjo-*`: `tjo-api` (unhealthy before and after, unrelated), `tjo-postgres` (healthy); both `RestartCount 0` and `StartedAt` unchanged (2026-11-10T06:58:29Z) before and after.

## Tests

| Run | Result |
|-----|--------|
| `tests/e2e/test_phase3_metrics.py -m e2e` (SMARTHEALTH_E2E_REQUIRE_STACK=1), tracer alone | 1 passed |
| same file in full (tracer, target list, per-worker `temporal_` x3, label cardinality) | 6 passed |
| `tests/infra tests/workflows tests/libs/test_service_isolation.py` (after Task 2) | 335 passed, 11 skipped |
| `tests/workflows` (after Task 1) | 277 passed, 11 skipped |
| `tests/libs tests/notification -m "not e2e"` | 220 passed |
| `tests/libs/test_config.py test_shared_settings.py`, `tests/scheduling/test_slot_state_steps.py test_appointment_states.py` | 72 passed |
| `tests/infra/test_prometheus_config.py` | 8 passed (acceptance needs at least 3) |

A full non-e2e regression was not run (orchestrator: not required). No `OTEL_SDK_DISABLED`. The known unrelated failure `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` was not touched or run.

Semgrep: `semgrep --config auto` refuses to run with `--metrics=off` (and metrics were not enabled, to avoid sending data), so `p/python` + `p/security-audit` were run over `libs/temporal.py libs/config.py services/scheduling services/visit services/notification infra/prometheus.yml infra/docker-compose.yml infra/grafana`: one finding, `avoid-sqlalchemy-text` on `services/scheduling/repository/idempotency_repo.py` line 26 (an interpolated integer constant `KEY_TTL_HOURS`, pre-existing, not in this plan's diff, not fixed). Nothing in the files this plan changed. The new SQL in `booking.py` is a constant `text()` with a bound parameter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Grafana logged provisioning errors for missing `plugins/` and `alerting/` directories**
- **Found during:** Task 1 (first Grafana recreate)
- **Issue:** the plan requires "no provisioning error lines", but Grafana 13 logs `level=error ... no such file or directory` for each missing provisioning subdirectory, and `.gitkeep` files add a `level=warn ... invalid suffix`.
- **Fix:** added `provisioning/plugins/plugins.yml` and `provisioning/alerting/alerting.yml`, each only `apiVersion: 1` with a comment; a static test (`test_grafana_finds_every_provisioning_directory`) keeps them.
- **Files modified:** infra/grafana/provisioning/plugins/plugins.yml, infra/grafana/provisioning/alerting/alerting.yml
- **Commit:** e8b6ba4

**2. [Rule 3 - Blocking] The e2e assumed Grafana on host port 3000 and Prometheus on 9090**
- **Found during:** Task 1
- **Issue:** the live Grafana is published on host port 3001 (host ports are configurable in `.env`), so `localhost:3000` is refused.
- **Fix:** the e2e asks Compose for the published port (`docker compose port <service> <port>`), with `SMARTHEALTH_PROMETHEUS_URL` / `SMARTHEALTH_GRAFANA_URL` overrides.
- **Files modified:** tests/e2e/test_phase3_metrics.py
- **Commit:** e8b6ba4

**3. [Rule 2 - Missing critical functionality] The NFR-threshold equality had no test**
- **Found during:** Task 1
- **Issue:** the plan's key link says the dashboard thresholds equal `loadtest/nfr.json` "(test-enforced)" but lists no test file for it.
- **Fix:** added `tests/infra/test_grafana_provisioning.py` (datasource, provider, Compose mounts, uid, seven threshold parametrizations against `nfr.json`). Not in the plan's `files_modified`.
- **Commit:** e8b6ba4

**4. [Rule 1 - Bug] An existing test would have started a real metrics server**
- **Found during:** Task 2
- **Issue:** `tests/workflows/test_reminder_workflow.py::test_main_...` runs `temporal_worker.main()` in the test process; main now serves metrics on port 9100, which the test must not bind.
- **Fix:** the test monkeypatches `start_worker_metrics_server`. Not in the plan's file list.
- **Commit:** a40cd40

### Plan-ordering note

The visit and notification worker edits were made while the scheduling worker was being wired, but were committed in Task 2, where the plan assigns them; each commit therefore matches its task's file list, and the live tracer run exercised only the scheduling worker.

## Design notes and limits

- **Histogram resolution.** The per-handler HTTP latency histograms come from `prometheus-fastapi-instrumentator`'s default `http_request_duration_seconds`, whose buckets are only 0.1, 0.5 and 1 s. `histogram_quantile` interpolates inside those buckets, so the NFR-03/04 panels are coarse for the 0.2 s and 0.3 s thresholds (a p95 below 0.1 s reads as at most 0.1 s). Finer buckets would need a change to every service's Instrumentator call; not in this plan's scope, flagged for 03-16's NFR histogram check.
- **Repeated observations.** A retried `confirm` activity (it is idempotent) observes the same appointment's saga time again; the effect on a percentile is negligible and the alternative is a per-appointment state check on the confirm path.
- **Gateway handlers.** The 5xx panel reads the gateway's grouped `status="5xx"` label from its internal `:9100` listener.
- **Known limit, not new:** the notification-celery and notification-consumer containers were not rebuilt (they do not import the changed code paths); only the two workers named by the plan were.

## Known Stubs

None.

## Threat Flags

None. Threat register: T-3-45 mitigated (no host mapping for any scraped or metrics port, enforced by `test_no_scraped_port_is_published_to_the_host` and `test_no_compose_service_publishes_a_metrics_port_added_in_phase_3`; metric labels are `outcome`, service, job and event type only); T-3-48 mitigated (no per-id labels, checked live by `test_no_worker_consumer_or_celery_series_is_labelled_by_an_entity_id`; fixed bucket sets); T-3-SC accepted (no package installed).

## Self-Check: PASSED
