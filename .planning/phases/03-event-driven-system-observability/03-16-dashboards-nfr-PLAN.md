---
phase: 03-event-driven-system-observability
plan: 16
type: execute
wave: 9
depends_on: ["03-15"]
files_modified:
  - infra/docker-compose.yml
  - infra/postgres-init/create-service-databases.sh
  - infra/grafana/provisioning/datasources/datasources.yml
  - infra/grafana/dashboards/smarthealth-events.json
  - infra/grafana/dashboards/smarthealth-notifications.json
  - infra/grafana/dashboards/smarthealth-operations.json
  - tests/infra/test_grafana_provisioning.py
  - tests/e2e/test_phase3_metrics.py
autonomous: true
requirements: [OBS-01, ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05]

estimate:
  tokens: 80000
  raw_tokens: 80000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "An operator can see the five operational metrics (total patients, appointments booked per day, completed visits, cancellation rate, average wait) per clinic in a provisioned Grafana dashboard that reads the analytics views as the read-only grafana_ro role (ANLY-01..05, D-10)"
    - "Grafana dashboards and datasources are provisioned from files in the repo, not built by hand (D-11): four dashboards (booking/NFR, events, notifications, operations) and three datasources (Prometheus, analytics Postgres, Jaeger), checked by a static test that every panel's datasource and metric name exists"
    - "The NFR-relevant paths have samples: booking accept HTTP latency, saga completion, outbox insert, event end-to-end lag per consumer group and notification delivery; the NFR dashboard's thresholds equal loadtest/nfr.json (D-04)"
    - "The k6 smoke profile passes on the finished Phase 3 stack (D-04); sustained, burst and contention stay deferred"
    - "The Grafana Postgres datasource uses the read-only role with a file-provided password; no write role exists for Grafana"
  artifacts:
    - path: infra/grafana/provisioning/datasources/datasources.yml
      provides: "Prometheus (uid prometheus), analytics Postgres via grafana_ro (uid analytics-pg), Jaeger (uid jaeger)"
      contains: "$__file{"
    - path: infra/grafana/dashboards/smarthealth-operations.json
      provides: "the five operational metrics from the analytics views, clinic variable, definitions in panel descriptions"
      contains: "v_visit_wait"
    - path: infra/grafana/dashboards/smarthealth-events.json
      provides: "consumer lag, processing and end-to-end lag p95, DLQ counts, outbox insert p95, replication-slot retained bytes"
      contains: "event_end_to_end_lag_seconds"
    - path: tests/infra/test_grafana_provisioning.py
      provides: "static checks: JSON parses, unique uids, datasource uids exist, known metric names, NFR thresholds equal nfr.json, provider path matches the mount"
  key_links:
    - from: infra/grafana/dashboards/smarthealth-operations.json
      to: services/analytics/migrations/versions/analytics_0001_projections.py
      via: "panels query v_active_patients, v_appointments_booked_daily, v_completed_visits_daily, v_cancellation, v_visit_wait through analytics-pg"
      pattern: "analytics-pg"
    - from: infra/grafana/dashboards/smarthealth-booking-nfr.json
      to: loadtest/nfr.json
      via: "threshold steps equal book_p95_ms, book_p99_ms, read_p95_ms, confirm_p95_s, confirm_p99_s, error_rate_max (test-enforced here)"
      pattern: "nfr.json"
---

<objective>
Finish the Grafana side of observability and measure the NFR paths: the analytics and Jaeger datasources, the operations dashboard (the operator's Grafana view of the five metrics), the events and notifications dashboards, a static provisioning test, the check that every NFR histogram has samples, and the k6 smoke run on the finished stack.

Purpose: roadmap success criteria 3 and 4 (metrics visible in Grafana for critical flows; operators can view the five metrics) and D-04 (histograms on the NFR paths; smoke must still pass; sustained, burst and contention stay deferred). The `/api/analytics` endpoints from plan 03-13 are the other view of ANLY-01..05.

Output: three datasources, three dashboards, the Grafana secret and dependency in Compose, an optional `pg_monitor` grant, `tests/infra/test_grafana_provisioning.py`, e2e checks and the smoke result.

Decisions honoured: D-04, D-10 (metrics served from the projection store), D-11 (dashboards as code), research Pattern 10 and Pitfall 4 (replication-slot retained-WAL panel through a SQL datasource, assumption A9). Claude's discretion: classic dashboard JSON (assumption A1), `allowUiUpdates: false`, `sslmode: disable` on the Compose network (documented).

Live stack rules: recreate Grafana with `docker compose -f infra/docker-compose.yml up -d --no-deps grafana` and restart Prometheus only if its config changed (it does not in this plan). If the `pg_monitor` grant is needed, re-run db-init with `docker compose -f infra/docker-compose.yml up -d db-init` (idempotent). Never touch `tjo-*` containers (record status before and after). This plan runs alone in wave 9 so the smoke measurement is on a quiet stack, and it is the only wave-9 editor of `infra/docker-compose.yml`.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-15-SUMMARY.md
@infra/grafana/provisioning/datasources/datasources.yml
@infra/grafana/dashboards/smarthealth-booking-nfr.json
@infra/docker-compose.yml
@services/analytics/migrations/versions/analytics_0001_projections.py
@loadtest/nfr.json
@loadtest/README.md
@docs/nfr.md
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| datasources | Grafana | `analytics-pg` (type `grafana-postgresql-datasource`, url `postgres:5432`, database `analytics_db`, user `grafana_ro`, `secureJsonData.password: $__file{/run/secrets/grafana_ro.secret}`, `sslmode: disable`), `jaeger` (url `http://jaeger:16686`), beside plan 03-15's `prometheus` |
| dashboards | Grafana | `smarthealth-operations` (five metrics, clinic variable from `select distinct clinic_id from appointments`), `smarthealth-events`, `smarthealth-notifications` (plus plan 03-15's `smarthealth-booking-nfr`) |
| Compose grafana | infra | secret `grafana_ro_password` mounted as `/run/secrets/grafana_ro.secret`, `depends_on` analytics healthy |
| `GRANT pg_monitor TO grafana_ro` | db-init | only if the slot query is refused (read-only monitoring role; no table data) |
| tests | tests | `tests/infra/test_grafana_provisioning.py`; `test_operations_panels_return_rows_as_grafana_ro`, `test_grafana_ro_cannot_write`, `test_grafana_provisioning_clean` (Task 1), `test_replication_slot_query_as_grafana_ro` (Task 2), `test_nfr_histograms_present` (Task 3) in `tests/e2e/test_phase3_metrics.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the operations dashboard's five metric queries return a test clinic's numbers as grafana_ro through the provisioned analytics datasource</name>
  <precondition>`infra/secrets/infra/grafana_ro.secret` exists (plan 03-03) and the grafana_ro role has SELECT on analytics_db by default privileges (plan 03-05); check with `test -f`, never print it.</precondition>
  <files>infra/docker-compose.yml, infra/grafana/provisioning/datasources/datasources.yml, infra/grafana/dashboards/smarthealth-operations.json, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - After a booking, a cancel and a visit completion in a fresh clinic through the gateway, each of the five panel SQL queries in `smarthealth-operations.json`, run as `grafana_ro` (password read from the secret file, never printed) with the clinic variable set, returns a row for that clinic within 90 s, and the values match `/api/analytics/summary` for the same clinic and window
    - The Grafana container log after the recreate contains no provisioning error lines and reports the dashboards provider as loaded; `GET localhost:3000/api/health` returns 200
    - `grafana_ro` cannot write: an `INSERT` into `patients` as `grafana_ro` gets `permission denied`
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 10, Code Examples "Grafana provisioning")
    - infra/grafana/provisioning/datasources/datasources.yml and infra/grafana/dashboards/smarthealth-booking-nfr.json (plan 03-15: file layout and JSON style)
    - services/analytics/migrations/versions/analytics_0001_projections.py (view names, columns and `COMMENT ON VIEW` definitions)
    - services/analytics/repository/queries.py (plan 03-13: the API's window semantics to match)
    - infra/docker-compose.yml (`grafana` block, secrets block)
  </read_first>
  <action>
    Datasource: add `analytics-pg` per the artifacts table to `datasources.yml`. Compose: give `grafana` the `grafana_ro_password` secret (target `grafana_ro.secret`) and `depends_on` analytics healthy.

    Dashboard `smarthealth-operations.json` (classic JSON, uid `smarthealth-operations`, title `SmartHealth · Operations`): a `clinic` template variable sourced from `select distinct clinic_id from appointments`, and one panel per metric reading only the views (`v_active_patients`, `v_appointments_booked_daily`, `v_completed_visits_daily`, `v_cancellation` as cancelled / booked over the time range, `v_visit_wait` as the mean over the time range), each panel description quoting the view's definition (wait time = check-in to visit start; cancellation rate denominator = appointments booked in the window).

    e2e (marker e2e): extract the five panel queries from the JSON, substitute the clinic variable and the time range, and run them with psycopg/asyncpg as `grafana_ro` against `localhost:5433/analytics_db`; compare with the summary endpoint; read `docker compose -f infra/docker-compose.yml logs grafana` for provisioning errors.

    Apply: recreate `grafana` (live stack rules).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "operations or grafana_ro_cannot or provisioning_clean" -x -q</automated>
    <fails_when>pytest exits non-zero, a panel query returns no row for the test clinic within 90 s, a value differs from the summary endpoint, grafana_ro can insert, or the Grafana log shows a provisioning error.</fails_when>
  </verify>
  <acceptance_criteria>
    - the tracer tests pass
    - `grep -c '$__file{/run/secrets/grafana_ro.secret}' infra/grafana/provisioning/datasources/datasources.yml` is 1
    - `grep -c "v_visit_wait" infra/grafana/dashboards/smarthealth-operations.json` is at least 1
    - tjo-* status lines show no restart
  </acceptance_criteria>
  <done>The five operational metrics are visible in a provisioned Grafana dashboard, read through a read-only role.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Events and notifications dashboards, the Jaeger datasource, the replication-slot panel, and the static provisioning test</name>
  <files>infra/postgres-init/create-service-databases.sh, infra/grafana/provisioning/datasources/datasources.yml, infra/grafana/dashboards/smarthealth-events.json, infra/grafana/dashboards/smarthealth-notifications.json, tests/infra/test_grafana_provisioning.py, tests/e2e/test_phase3_metrics.py</files>
  <behavior>
    - Static: every dashboard JSON parses, has a unique uid and title, and every panel's datasource uid exists in `datasources.yml`; every Prometheus metric name used in a panel expression is in a known list of names this codebase emits (consumer base, notification task, outbox, saga, HTTP instrumentator, Temporal SDK prefix, RabbitMQ prefix); the NFR dashboard thresholds equal `loadtest/nfr.json`; the operations dashboard has exactly one panel per metric querying the analytics views; the dashboards provider path matches the Compose mount
    - Live: the replication-slot query (`slot_name`, `active`, retained bytes from `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) runs as `grafana_ro` and lists the four `dbz_*` slots
  </behavior>
  <read_first>
    - infra/grafana/* (Task 1 and plan 03-15)
    - libs/events/consumer.py, services/notification/tasks.py, libs/events/outbox.py, services/scheduling/activities/booking.py (exact metric names)
    - infra/postgres-init/create-service-databases.sh (grafana_ro grants from plan 03-05)
    - loadtest/nfr.json
  </read_first>
  <action>
    Datasource: add `jaeger` (url `http://jaeger:16686`).

    Dashboards: `smarthealth-events.json` (consumer lag per group and partition, processing p95, end-to-end lag p95 per group, DLQ counts by group and error type, outbox insert p95 by event type, replication-slot retained bytes and active flag from `analytics-pg`), `smarthealth-notifications.json` (sent, skipped, retried, dead-lettered and provider-replayed rates by type, delivery p95, RabbitMQ queue depth for the four notification queues from the RabbitMQ exporter, Temporal workflow and activity rates and latencies from the SDK metrics).

    If `grafana_ro` cannot read `pg_replication_slots` or call `pg_current_wal_lsn()`, add `GRANT pg_monitor TO grafana_ro` in db-init with a comment (read-only monitoring role; no table data) and re-run db-init (live stack rules). Recreate `grafana`.

    Tests: `tests/infra/test_grafana_provisioning.py` (static, no stack) for the static behaviors; add the live slot-query check to `tests/e2e/test_phase3_metrics.py`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "slot" -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a dashboard references a missing datasource or unknown metric, an NFR threshold differs from nfr.json, or the slot query is refused or lists fewer than four `dbz_*` slots.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/infra -q` exits 0 with at least 8 passed
    - `ls infra/grafana/dashboards/*.json | wc -l` prints 4
    - `grep -c "uid: jaeger" infra/grafana/provisioning/datasources/datasources.yml` is 1
  </acceptance_criteria>
  <done>Four provisioned dashboards cover booking/NFR, events, notifications and operations, and a static test keeps their datasources, metric names and thresholds honest.</done>
</task>

<task type="auto">
  <name>Task 3: NFR histograms present and the k6 smoke profile on the finished Phase 3 stack</name>
  <files>tests/e2e/test_phase3_metrics.py</files>
  <read_first>
    - loadtest/README.md (smoke run, raised gateway budget) and tests/e2e/test_loadtest_smoke.py (self-seeding wrapper)
    - docs/nfr.md (what is a target versus measured; sustained/burst/contention stay deferred)
    - .planning/phases/03-event-driven-system-observability/03-11-SUMMARY.md (the earlier smoke numbers)
  </read_first>
  <action>
    Add `test_nfr_histograms_present` to `tests/e2e/test_phase3_metrics.py`: after one booking, cancel and visit completion, Prometheus has samples for the scheduling HTTP request duration histogram on POST `/api/appointments`, `booking_saga_seconds_bucket`, `outbox_insert_seconds_bucket`, `event_end_to_end_lag_seconds_bucket` for each of the three consumer groups, and `notification_delivery_seconds_bucket`.

    Then run the smoke profile on the quiet stack with the gateway budget raised for the run only: `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`, `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q`, then `docker compose -f infra/docker-compose.yml up -d --no-deps gateway` to restore the defaults. The wrapper creates its own test admin and world; do not create or hardcode any other admin credential (a manual `loadtest/run.sh smoke` with a user-provided admin is part of the end-of-phase human check in plan 03-20). Record in the SUMMARY: k6 accept p95/p99, saga p95/p99 and error rate, the same quantiles read from Prometheus for the run window, the plan 03-11 numbers and the 2026-11-10 baseline (accept p95 21.8 ms, saga p95 0.52 s). Do not run sustained, burst or contention (deferred by the user until after Phase 3).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k rate_budget -x -q</automated>
    <fails_when>Any pytest run exits non-zero, a named NFR histogram has no samples, k6 reports a crossed smoke threshold, or the gateway still runs with the raised budget.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -q` exits 0 with at least 6 passed
    - `tests/e2e/test_loadtest_smoke.py` passes on the Phase 3 stack
    - the SUMMARY records k6 and Prometheus quantiles next to the plan 03-11 numbers and the baseline
    - the gateway runs with its default rate budget after the run (plan 03-11's `test_gateway_default_rate_budget_restored` passes)
  </acceptance_criteria>
  <done>The NFR paths are instrumented end to end and the smoke profile passes on the complete Phase 3 stack.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Grafana -> analytics_db and pg_catalog | read-only SQL for panels and the slot view |
| host browser -> Grafana (:3000) | dashboards over aggregate data |
| repository files -> Grafana provisioning | dashboards and datasources come only from the repo |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-46 | Elevation of Privilege | Grafana database access | medium | mitigate | `grafana_ro` with SELECT by default privileges on analytics_db only (plus `pg_monitor` only if the slot panel needs it); password from a Compose secret via `$__file{}`; e2e proves an INSERT is refused; no write role exists for Grafana |
| T-3-47 | Tampering | dashboards edited in the UI drifting from the repo | low | mitigate | `allowUiUpdates: false`, read-only mounts, static tests on the JSON (datasource uids, metric names, thresholds) |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/infra -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto infra`; record status in the SUMMARY
</verification>

<success_criteria>
- Four file-provisioned dashboards, including the five operational metrics read as a read-only role
- Every NFR histogram has samples; the smoke profile passes on the finished stack
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-16-SUMMARY.md` when done
</output>
