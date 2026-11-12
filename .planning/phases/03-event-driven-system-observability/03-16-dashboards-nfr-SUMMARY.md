---
phase: 03-event-driven-system-observability
plan: 16
subsystem: observability
tags: [grafana, provisioning, dashboards-as-code, analytics-datasource, jaeger, prometheus-buckets, k6-smoke, nfr]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-15 Prometheus scrape of every process, Grafana provisioned from files, booking/NFR dashboard; 03-10/03-13 analytics views and API; 03-05 grafana_ro role and secret; 03-04/03-14 consumer and notification metrics"
provides:
  - "Four file-provisioned dashboards (booking/NFR, events, notifications, operations) and three datasources (Prometheus, analytics Postgres as grafana_ro, Jaeger)"
  - "The five operational metrics per clinic in Grafana, read from the analytics views as the read-only role, equal to /api/analytics/summary"
  - "libs.observability.http_metrics.instrument_http: finer HTTP latency buckets with an edge on every NFR latency threshold, used by every service"
  - "tests/infra/test_grafana_provisioning.py (static: uids, datasources, known metric names, read-only secret, one operations panel per metric) and e2e checks for the panel SQL, the role's read-only boundary, the slot query and every NFR histogram"
  - "k6 smoke on the finished Phase 3 stack: all thresholds pass"
affects: [03-17, 03-18, 03-19, 03-20]
status: complete
duration: about 2h wall time (19 min of it a background regression run)
completed: 2026-11-11
actuals:
  tokens: 21000
  tasks: 3
  commits: 5
plan_head_before: 27818a950ffbb3b5808eaf42a89f91ecfdfb271b
plan_head_after: 5fffc95deda2204a5d8247a94b4d1c86bcad3381
tech-stack:
  added: []
  patterns:
    - "One shared helper (instrument_http) owns the per-handler HTTP histogram buckets; services never build their own Instrumentator"
    - "Dashboard tests parse the PromQL and check every metric name against what the source declares (Counter/Histogram/Gauge scan) plus the Temporal and RabbitMQ prefixes"
    - "Panel SQL is tested by substituting Grafana's variable and time macros and running it as the real read-only role"
key-files:
  created:
    - infra/grafana/dashboards/smarthealth-operations.json
    - infra/grafana/dashboards/smarthealth-events.json
    - infra/grafana/dashboards/smarthealth-notifications.json
    - libs/observability/http_metrics.py
    - tests/libs/test_http_metrics.py
  modified:
    - infra/docker-compose.yml
    - infra/grafana/provisioning/datasources/datasources.yml
    - infra/grafana/dashboards/smarthealth-booking-nfr.json
    - services/{analytics,billing,gateway,identity,organization,patient,provider,scheduling,visit}/app.py
    - tests/infra/test_grafana_provisioning.py
    - tests/e2e/test_phase3_metrics.py
key-decisions:
  - "No pg_monitor grant: grafana_ro can read pg_replication_slots and call pg_current_wal_lsn() as it is (checked through the provisioned datasource), so the least-privilege role stays SELECT-only"
  - "HTTP latency buckets 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 2.5, 5 s, owned by one helper; same labels and same single observe() per request, so the booking accept path gains no work"
  - "RabbitMQ depth is the broker-wide aggregate (rabbitmq_queue_messages_ready/unacked); the per-queue view needs a Prometheus scrape of the plugin's detailed endpoint, which would change infra/prometheus.yml and is out of this plan"
requirements-completed: []
---

# Phase 3 Plan 16: Dashboards and NFR Summary

**Grafana now shows the five operational metrics per clinic (read as the SELECT-only `grafana_ro` role, equal to `/api/analytics/summary`), plus events, notifications and booking/NFR dashboards, all provisioned from files and guarded by a static test; the per-handler HTTP histogram got finer buckets from one shared helper; every NFR histogram has samples; and the k6 smoke profile passes every threshold on the finished Phase 3 stack (accept p95 49.1 ms, p99 200.8 ms, saga p95 0.519 s, 0 errors).**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (the plan addresses OBS-01, ANLY-01..05).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 42252ac | `analytics-pg` datasource (file-provided password), Compose secret and `depends_on analytics healthy` for grafana, `smarthealth-operations` dashboard, three e2e tests (panel SQL as grafana_ro, no write path, clean provisioning log) |
| 2 | 741a0ed | `jaeger` datasource, `smarthealth-events` and `smarthealth-notifications` dashboards, static provisioning tests, slot-query e2e |
| 3a (requested fix) | cd41e11 | `libs/observability/http_metrics.py`, nine services call it, NFR panel descriptions, gateway 5xx shows 0 not no data |
| 3b | 9282542 | `test_nfr_histograms_present` |
| 3c | 5fffc95 | honest wording for the bucket note; health probe off urllib |

Task 3's smoke run changed no file (see Deviations).

## What was built

- **Datasources** (`datasources.yml`): Prometheus (`prometheus`, from 03-15), `analytics-pg` (`grafana-postgresql-datasource`, user `grafana_ro`, `postgres:5432`, `analytics_db`, `sslmode: disable` on the private network, password `$__file{/run/secrets/grafana_ro.secret}`), `jaeger` (`http://jaeger:16686`). Compose gives grafana only the `grafana_ro_password` secret and waits for `analytics` healthy. Live check through Grafana's API: `analytics-pg` health "Database Connection OK", `jaeger` health "Data source is working", dashboards reported `provisioned: true`.
- **`smarthealth-operations`**: a `clinic` variable (clinics with a projected patient or appointment) and one panel per metric over its view only (`v_active_patients`, `v_appointments_booked_daily`, `v_completed_visits_daily`, `v_cancellation`, `v_visit_wait`), each description quoting the view's definition (wait = check-in to visit start; cancellation denominator = appointments booked in the window; counts start at the Phase 3 cutover, D-13) plus an explanatory text panel. The variable is bound with `${clinic:sqlstring}` (quoted and escaped by Grafana).
- **`smarthealth-events`**: consumer lag per group/topic/partition, events consumed by outcome, processing p95, end-to-end lag p95/p99 per group, dead-lettered events by group and error type, outbox insert p95 by event type, and the replication-slot table (slot, active flag, retained WAL bytes from `pg_replication_slots`) through `analytics-pg`.
- **`smarthealth-notifications`**: sent, skipped, retried, dead-lettered and provider-replayed rates, delivery p95/p99, RabbitMQ ready/unacked, Temporal SDK workflow completions, activity rates, activity p95 and workflow end-to-end p95 (the SDK records milliseconds; panels use unit `ms`).
- **Static test** (`tests/infra/test_grafana_provisioning.py`, 29 tests): exactly four dashboards, unique uids/titles/panel ids, `editable: false`, the three datasources, the read-only role with a file-provided password and no literal password anywhere in provisioning, Compose secret and provider path vs mount, every panel/target/variable datasource uid exists, every PromQL metric is one the code emits (a regex scan of `Counter|Histogram|Gauge|Summary(` declarations in `libs` and `services`, histogram suffixes expanded, plus the HTTP Instrumentator names and the `temporal_`/`rabbitmq_` prefixes; the extractor has its own unit test), operations has exactly one panel per metric reading only its view and always scoped to the clinic, thresholds equal `loadtest/nfr.json` (from 03-15).
- **HTTP bucket fix.** `instrument_http(app)` is `Instrumentator().instrument(app, latency_lowr_buckets=HTTP_LATENCY_BUCKETS)`; the nine services replaced their bare `Instrumentator().instrument(app)` line with it (a one-line, mechanical change each). Buckets 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 2.5, 5 s put an edge on every latency threshold in `nfr.json` (a unit test asserts it). Labels (`handler`, `method`) are unchanged, so cardinality grows only by the extra bucket series per handler.

## Live-stack results

- Recreated: `grafana` (three times, the third with `--force-recreate` because datasources are read only at start), `scheduling` (rebuilt with `--no-deps`, healthy). Not rebuilt: every other service (their code now calls the helper but their running images still use the old buckets until their next rebuild; only the scheduling handlers feed the NFR panels). The gateway was **not** recreated: its live environment already had `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000` and `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` (verified before and after). No volume removed, no `down -v`.
- Final state: `scripts/compose_healthcheck.sh` OK; `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all active.
- `tjo-*`: `tjo-api` (unhealthy before and after, unrelated) and `tjo-postgres` (healthy), `RestartCount 0`, `StartedAt` unchanged (2026-11-10T06:58:29Z).
- Observation worth the mentor's attention: `dbz_provider` retains about 517 MB of WAL (the other slots 23 to 77 MB) because provider_db is quiet and its slot's restart position only advances on activity. The new slot panel is what exposes this; nothing was done about it here.

## Tests

| Run | Result |
|-----|--------|
| `tests/e2e/test_phase3_metrics.py -m e2e` (SMARTHEALTH_E2E_REQUIRE_STACK=1), final | 11 passed (acceptance needs at least 6) |
| Task 1 tracer subset (`-k "operations or grafana_ro_cannot or provisioning_clean"`) | 3 passed |
| `tests/infra tests/libs/test_http_metrics.py` | 64 passed |
| `tests/infra tests/libs/test_service_isolation.py` (after Task 2) | 75 passed |
| Regression over `tests/{gateway,analytics,libs,infra,organization,identity,patient,provider,billing,visit,scheduling,events}` (not e2e, background, 19 min) | 1192 passed, 2 failed (below) |
| `tests/identity/test_email_migration.py` rerun alone | 2 passed |

The two failures: `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` is the known pre-Phase-3 failure (specialty list order), not touched. `tests/identity/test_email_migration.py::test_upgrade_refuses_over_case_insensitive_duplicates_without_leaking_emails` failed in the loaded full run (its alembic subprocess printed nothing to match) and passed on rerun in isolation; it runs real `alembic downgrade`/`upgrade` against the live identity_db, so it is timing-sensitive on a busy laptop. After the run identity_db was at `identity_0002` with `uq_users_email_lower` present. No `OTEL_SDK_DISABLED`.

Semgrep (`p/python` + `p/security-audit`, metrics off, over the changed Python, Compose and Grafana files): 3 findings, all `dynamic-urllib-use-detected` on `tests/e2e/test_phase3_metrics.py` urllib calls to fixed local Prometheus and Grafana URLs (written in plan 03-15, marked `noqa: S310`). The one I added was switched to httpx, so this plan adds none. Nothing in `libs/observability/http_metrics.py`, the services, Compose or the dashboards.

## k6 smoke on the finished Phase 3 stack (D-04)

Run with `loadtest/run.sh smoke` on a freshly seeded small world (`--providers 2 --patients 250 --days 2`, 491-entry plan, 150 used), 2026-11-11 11:29:51 to 11:30:22 UTC. Host: load average 3.4 / 5.8 / 6.3 at the start (it had been above 7 during the regression run just before; about the same as 03-11's "about 3", worse than the 2026-11-10 quiet host). Gateway at 100000/min default and 1000/min auth budget. **Exit 0, every threshold passed, 0 errors, 0 dropped iterations, 0 confirm timeouts** (151 iterations, 244 requests).

| Metric | This run (2026-11-11, after rebuild) | 03-11 (after producers) | Baseline 2026-11-10 (quiet host) | Threshold |
|---|---|---|---|---|
| booking accept p95 | 49.1 ms | 29.3 ms | 21.8 ms | 300 ms |
| booking accept p99 | 200.8 ms | 170.0 ms | 78.9 ms | 800 ms |
| saga completion p95 (k6, from 202 to final status) | 0.519 s | 0.519 s | 0.52 s | 5 s |
| saga completion p99 | 0.520 s | 0.521 s | not recorded | 15 s |
| read appointment p95 / p99 | 9.2 / 23.8 ms | 9.7 / 11.8 ms | 9 to 13 ms (p95) | 200 / 500 ms |
| read slots p95 / p99 | 28.7 / 68.5 ms | not recorded | not recorded | 200 / 500 ms |
| booking error rate | 0.00% | 0 | 0.00% | 1% |

Honest reading: accept latency is higher than both baselines (p95 about 2.2x the quiet-host figure, p99 about 2.5x), but still about 6x and 4x under the thresholds. The host was busier than the quiet baseline and the 30 s run has 151 booking samples, so a p99 here is the slowest one or two requests; I did not isolate how much of the difference is host load and how much is the outbox insert plus the finer histogram, and the outbox insert itself measured p95 about 1 ms, p99 about 3 ms for `appointment.booked` in Prometheus, so it cannot explain tens of milliseconds. The saga time is unchanged.

The same quantiles read from Prometheus for the run window (`increase(...[90s])`, interpolated inside buckets): accept p95 about 41 ms, p99 about 171 ms (finer buckets, scheduling job, `POST /api/appointments`); read appointment p95/p99 about 24 / 25 ms and read slots 46 / 81 ms (bucket-interpolated, so coarser than k6's figures); saga p95 about 0.46 s and p99 about 0.60 s (`booking_saga_seconds`, database clock, buckets 0.5 and 1 s, so approximate; it measures row creation to confirm, k6 measures 202 to observed final status, polling included); end-to-end event lag p95 about 2.1 s for analytics-projection and 2.9 s for notification-trigger (Debezium plus consumer batching); notification delivery p95 about 0.05 s.

**Grafana shows the run.** The NFR dashboard's panel queries, run through Grafana's own query API over 11:28 to 11:31 UTC, return series for every panel (accept, read, saga, gateway 5xx ratio 0, outbox) with all peaks below their threshold lines. Two caveats: the 5-minute `rate()` window at that moment still straddled the scheduling rebuild (old 0.1/0.5/1 s series next to the new ones), and a quantile with few samples in a bucket reads near its upper edge (accept p95 plotted about 0.098 s while k6 measured 49 ms), so the panels are reliable for "above or below the threshold line", not for the third digit. The panels were not looked at in a browser.

## Deviations from Plan

### Auto-fixed Issues

**1. [Orchestrator instruction over plan] Smoke through the seed script and `loadtest/run.sh smoke`, not `tests/e2e/test_loadtest_smoke.py`; the gateway was not recreated and the plan's default-budget restore was not done.**
- The wrapper test inserts an admin with a hard-coded password; the orchestrator authorised a new random password on the existing `loadtest-admin@example.com` account only (same as 03-11). `secrets.token_urlsafe(24)`, hashed with `hash_password`, one `UPDATE ... WHERE email = 'loadtest-admin@example.com' AND role = 'admin'` (result `UPDATE 1`), kept in a 0600 file under the scratchpad, passed through `LOADTEST_ADMIN_*`, never printed. The seed script logging in successfully was the proof the update saved. Password file and `loadtest/data/plan.json` deleted afterwards; no other account touched.
- The live gateway already runs the raised budgets (user's standing setting, restored by the user at phase end), so there was no recreate and no "restore defaults" step. The plan's verify command `test_phase3_booking_events.py -k rate_budget` selects no test (none exists; 03-11 recorded the same), so it was not run; the plan's "gateway back on its default budget" criterion is intentionally not met.

**2. [Orchestrator-requested fix, outside the plan's file list] HTTP latency buckets** (`libs/observability/http_metrics.py`, nine `services/*/app.py` one-line changes, `tests/libs/test_http_metrics.py`). Only `scheduling` was rebuilt; the other eight services will pick the buckets up on their next rebuild. Not done: re-running the historic coarse data (old samples keep their old edges).

**3. [Rule 1 - Bug] The grafana recreate did not load new datasources.** A plain `up -d --no-deps grafana` reported "Running" (no Compose config change) and left the Jaeger datasource unloaded; `--force-recreate` fixed it. Also, the first version of my panel-count assertion expected six query panels (it is five plus one text panel); corrected.

**4. [Rule 2 - Missing critical functionality] Gateway 5xx ratio showed "no data" when there were no 5xx.** 03-15's panel divided a possibly empty vector; wrapped the numerator in `or vector(0)` so it shows 0. Touches 03-15's dashboard (not in this plan's `files_modified`); thresholds unchanged and still test-enforced.

**5. [Plan wording] Clinic variable includes patients.** The query is `patients UNION appointments` rather than `appointments` only, so a clinic that has registered patients but no booking is selectable for the total-patients panel.

**6. [Plan limitation] RabbitMQ queue depth is broker-wide, not per notification queue.** The plan asks for the four notification queues "from the RabbitMQ exporter"; the scrape in 03-15 uses the plugin's aggregated endpoint, which carries no queue label. Per-queue depth needs a second scrape job (`/metrics/detailed?family=queue_coarse_metrics`), a change to `infra/prometheus.yml` the plan says it does not make. The panel says "all queues" in its description.

**7. [Rule 2, test strength] More static and live checks than the plan listed**: read-only role cannot create a table, delete, or connect to an OLTP database; Compose secret/depends_on; provider path vs mount; the PromQL extractor's own unit test; every NFR histogram must have *grown* after the flow (not merely exist), including the three consumer groups; and `test_nfr_histograms_present` also asserts the 0.2/0.3/0.8 s bucket edges are live on the accept handler.

### Not done
- `pg_monitor` grant and db-init re-run: not needed (see key decisions).
- No change to `loadtest/README.md`/`docs/nfr.md` result sections (plan 03-18 owns the docs); the numbers above are the record for it.

## Known Stubs

None.

## Threat Flags

None new. Threat register: T-3-46 mitigated (SELECT-only `grafana_ro`, password only from the Compose secret file via `$__file{}`, e2e proves INSERT, CREATE and DELETE are refused and that the role cannot connect to `scheduling_db`; no write role exists for Grafana); T-3-47 mitigated (`allowUiUpdates: false`, read-only mounts, `editable: false` on every dashboard, static tests on uids, metric names, thresholds); T-3-SC accepted (no package installed).

## Self-Check: PASSED

All created files exist (the three dashboards, `libs/observability/http_metrics.py`, `tests/libs/test_http_metrics.py`); commits 42252ac, 741a0ed, cd41e11, 9282542, 5fffc95 exist on `gsd/phase-02-scheduling-service-workflow`; `commits: 5` is measured from the ledger base `27818a9`.
