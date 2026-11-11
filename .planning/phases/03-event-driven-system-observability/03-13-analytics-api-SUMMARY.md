---
phase: 03-event-driven-system-observability
plan: 13
subsystem: analytics
tags: [fastapi, rbac, kafka-consumer, compose, gateway, replay-rebuild, d-13]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-10 projections, five views, apply_event/handle_event/build_consumer; 03-09 consumer-container pattern; 03-07/03-08 producers; 03-01 analytics_service_url"
provides:
  - "analytics read API: GET /api/analytics/summary and /timeseries, admin (any clinic or all) and front desk (own clinic) only"
  - "analytics and analytics-consumer Compose services, gateway route /api/analytics, gateway depends_on analytics"
  - "ANALYTICS_CONSUMER_START (latest in Compose) plus pin_start_offsets: the live group counts from go-live (D-13)"
  - "python -m services.analytics.rebuild [--apply] [--group] [--topics]: replay rebuild that refuses under a live member"
affects: [03-16, 03-17, 03-18, 03-19, 03-20]
status: complete
duration: about 1h15 wall time
completed: 2026-11-11
actuals:
  tokens: 19000
  tasks: 3
  commits: 5
plan_head_before: 2a4c2a4dfd144233f9e6d508afbdfb06f1b01dd2
plan_head_after: 0d86dc80293f8695abc6e224b901a662fe63c630
tech-stack:
  added: []
  patterns:
    - "View declarations with sa.table/sa.column and Core selects: the API only filters views by clinic and window, no raw SQL text"
    - "Pin the go-live offset explicitly (commit the high watermark of a group with no position) instead of trusting auto.offset.reset at first assignment"
    - "A destructive tool is tested against a throw-away database created per test, never the shared dev database"
key-files:
  created:
    - services/analytics/app.py
    - services/analytics/Dockerfile
    - services/analytics/api/__init__.py
    - services/analytics/api/analytics.py
    - services/analytics/repository/queries.py
    - services/analytics/rebuild.py
    - tests/analytics/test_api.py
    - tests/analytics/test_replay_rebuild.py
    - tests/analytics/test_consumer_start.py
    - tests/e2e/test_phase3_analytics.py
  modified:
    - services/analytics/config.py
    - services/analytics/consumer.py
    - services/gateway/routing.py
    - infra/docker-compose.yml
    - tests/conftest.py
    - tests/gateway/test_routing.py
    - tests/libs/test_service_isolation.py
key-decisions:
  - "D-13 (orchestrator): the live group starts at LATEST. ANALYTICS_CONSUMER_START=latest|earliest maps to auto.offset.reset; Compose default latest, code default earliest (tests and replays read history)"
  - "First-start race closed by pin_start_offsets: before subscribing, main() commits the current high watermark for every partition the group has no offset for; a restart keeps its position"
  - "Roles: admin and front desk (there is no separate ops role). Front desk is forced to its token's clinic, a different clinic_id is 403 (also for a clinic with no data, so it is not an existence oracle)"
  - "Window = inclusive UTC days from..to, default last 30 days ending today, at most 366 days; 422 otherwise. total_patients is a current count, not windowed; timeseries is zero-filled per day"
  - "Rebuild deletes the claims of consumer analytics-projection (the processed_events.consumer value) whatever --group is, and tests run it against a throw-away database"
requirements-completed: []
---

# Phase 3 Plan 13: Analytics API Summary

**The five metrics are served through the gateway from the event-derived projections: a read-only, role- and clinic-scoped analytics API, two new Compose containers (API and consumer), a live consumer group that counts from go-live, and a replay rebuild that cannot run under a live consumer.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (ANLY-01..06 stay for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | cf24df0 | app, Dockerfile, summary endpoint (admin), queries, gateway route, two Compose services, `ANALYTICS_CONSUMER_START` and `pin_start_offsets`, e2e tracer |
| 2 | 8a7e39f | front-desk clinic scoping, timeseries, harness registration, 11 API tests, isolation test, e2e full flow and RBAC |
| 3 | 0027e35 | `rebuild.py` and 4 tests against a throw-away database |
| refactor | f9768fe | queries rewritten with SQLAlchemy Core (Semgrep `avoid-sqlalchemy-text`, 6 findings to 0) |
| test | 0d86dc8 | 3 tests for the start policy and the pinned offset |

## What was built

- **API.** `GET /api/analytics/summary?clinic_id=&from=&to=` returns `clinic_id, from, to, total_patients, appointments_booked, completed_visits, cancellation_rate, average_wait_seconds`. `cancellation_rate` and `average_wait_seconds` are null (not 0) when there is no booking or no wait sample. `GET /api/analytics/timeseries?metric=appointments_booked|completed_visits&...` returns a zero-filled `{day, value}` per UTC day. Only aggregates and clinic ids are returned (a test asserts no `*_id` field other than `clinic_id`). Auth runs before parameter validation (no token 401, provider/patient 403 even for a bad window).
- **Queries.** View declarations via `sa.table`/`sa.column` and Core selects; only the five views are touched. No OLTP name appears in the package (the plan 03-10 grep test still passes).
- **Compose.** `analytics` (API, Alembic on start, python healthcheck, no host port) and `analytics-consumer` (same image, `python -m services.analytics.consumer`, heartbeat healthcheck younger than 30 s, `start_period: 40s`, no host port; the 0.0.0.0:9100 metrics server is unpublished, asserted by a test). The gateway `depends_on` analytics. Only this plan edited `infra/docker-compose.yml` in wave 6.
- **Gateway.** `("/api/analytics", "analytics")` in `ROUTE_TABLE`; `/api/analyticsX` and `/internal/analytics/...` are not routed (routing tests).
- **D-13 start policy.** See below.
- **Rebuild.** Refuses (exit 1, nothing touched) while `describe_consumer_groups` shows a member; otherwise one transaction truncates the three tables and deletes this projection's claims, then `alter_consumer_group_offsets` sets every partition to its `list_offsets` earliest. Dry run without `--apply`. Docstring carries the operator procedure (stop consumer, run, start consumer). Output is counts and names only.

## D-13: the live group starts at LATEST (for plan 03-18, the docs plan)

- `ANALYTICS_CONSUMER_START` (`latest` | `earliest`) in `AnalyticsSettings` maps to the consumer's `auto.offset.reset`, which only matters for a group with no committed offset. Compose sets `${ANALYTICS_CONSUMER_START:-latest}` on both analytics services; the code default is `earliest` because every test produces events BEFORE its unique group joins and the replay path reads history.
- **First-start race.** With `latest`, `consumer.main()` calls `pin_start_offsets` before it subscribes: for each partition the group has no committed offset for, it commits the current high watermark (a probe consumer, synchronous commit). The go-live position is therefore recorded at container start, so an event produced right after the container is healthy is consumed, and the retained throw-away events are not. A restarted container keeps its committed position (second call pins nothing); a broker error is logged and falls back to plain `latest`.
- **Measured on the live stack.** First start logged `start=latest, pinned_partitions=9`; `processed_events`, `patients`, `appointments` and `visits_completed` in analytics_db were all 0 after start, so the backlog (earlier plans' test events) was not projected. The e2e events produced afterwards were consumed (3 e2e tests, 6 to 12 s).
- **Rebuild and D-13.** Rebuild resets to the earliest RETAINED offset with its own explicit offsets, independent of the start setting. A rebuild therefore replays whatever the topics still hold, including the throw-away test events that the live consumer skipped. Docs should say so, and that events from before the cutover or removed by retention cannot be replayed.
- Metrics count from go-live; `total_patients` counts only patients registered after go-live (D-13 limitation to state on the dashboard).

## Verification (measured)

- `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_analytics.py -m e2e`: **3 passed** (plan: at least 2), after the last rebuild of the `analytics` image: tracer (two patients reach `total_patients == 2`, `/api/analyticsX` 404); full flow (three bookings, one cancel, one check-in, start and complete show `appointments_booked == 3`, `cancellation_rate` about 0.333, `completed_visits == 1`, a non-null wait); RBAC through the gateway (front desk own clinic 200, other clinic 403, admin any clinic 200, provider 403, patient 403, no token 401).
- `tests/analytics/test_api.py`: **11 passed** (plan: at least 8). Rows seeded for two real clinics, read through the in-process gateway mesh: all five numbers (total 2, booked 4, completed 3, rate 0.25, wait 450 s), window filtering, empty window (null rate and wait), admin all-clinics, zero-filled timeseries, front desk own/other/unknown clinic, provider and patient 403 and no or invalid token 401 on both endpoints, 422 for from>to, window over 366 days (366 allowed, 367 not), unknown or missing metric and a bad clinic id, default window, aggregates only.
- `tests/analytics/test_replay_rebuild.py`: **4 passed** (plan: at least 3): replay reproduces every view and every table exactly (25-event fixed sequence), dry run changes nothing, refusal under an active member leaves the data and offsets untouched, `--help`.
- `tests/analytics/test_consumer_start.py`: **3 passed** (settings default and validation, `build_consumer` passes the policy, a pinned group skips history but consumes the first event after the pin).
- Final regression: `pytest tests/analytics tests/infra tests/libs/test_service_isolation.py tests/gateway -q`: **150 passed**, `OTEL_SDK_DISABLED` unset (this includes `test_routing.py` 59 and the isolation suite 17). The plan's full `-m "not e2e"` run was not done (orchestrator: not required). The known unrelated `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` failure was not touched.
- Acceptance: both analytics containers `healthy`; `grep -c '"/api/analytics"' services/gateway/routing.py` = 1; `analytics-consumer` is in the Compose file; `tjo-*` status lines (`tjo-api` unhealthy, `tjo-postgres` healthy, `tjo-migrate` exited) identical before and after.
- `python -m services.analytics.rebuild --help` exits 0. A read-only dry run against the live stack printed the plan and correctly saw 1 active member (the live consumer); `--apply` was never run on the live group or on the dev analytics_db.
- Semgrep: `semgrep --config p/python --metrics=off services/analytics services/gateway`: 151 rules, 20 files, **0 findings** (after the Core rewrite; the first run had 6 `avoid-sqlalchemy-text`). `--config auto` is refused with `--metrics=off` (as in earlier Phase 3 plans).

## Deviations from Plan

1. **[Orchestrator decision] D-13 latest-start** as described above; the plan said the consumer reads every retained event from the earliest offset on first start (stated as "expected, idempotent and harmless"). The orchestrator overrode it; added the setting, `pin_start_offsets` and tests.
2. **[Rule 1 - safety] Rebuild tests use a throw-away database, not the shared dev analytics_db.** The plan runs `rebuild --apply` against the dev database with a unique clinic, but rebuild truncates the whole table, which would wipe the live container's projections. Each test creates `analytics_rebuild_<hex>` (owner `analytics_svc`, through the Postgres superuser in the Compose container), runs the real Alembic migration on it and drops it at teardown. The consumer, handler and the rebuild CLI (a subprocess with `ANALYTICS_DATABASE_URL`) are the real ones. No leftover database remained.
3. **[Plan wording] Task split.** The Task 1 commit already carries the complete `summary` (all five fields) and the full queries module; the plan listed only `total_patients` for Task 1. Task 2 added front desk, timeseries and the tests. The tracer still ran red-to-green on the live stack.
4. **[Plan wording] "ops/admin roles".** There is no ops role in the system; the allowed roles are admin and front desk, as the plan's must-haves say.
5. **[Rule 2] Queries rewritten with SQLAlchemy Core** after Semgrep flagged `text()` with concatenated constant fragments (see commit f9768fe).
6. **[Scope] The Task 2 isolation work was mostly done in 03-10.** `OWNED_TABLES`, `BASES`, `SETTINGS` and the "no OLTP database name, one URL field" test already existed; this plan added `analytics` to `HARNESS_SERVICES` and one test that the package imports no other `services.<name>` and that both Compose containers get only `ANALYTICS_DATABASE_URL` and no published port.
7. **[Process] Tests were written alongside, not strictly red-first** (as in 03-10).
8. **Claims in rebuild are deleted by consumer name** `analytics-projection` (the value `handle_event` writes), not by `--group`, because the claim key is independent of the group name.

No auth gates, no package installs, no architectural changes, no files outside the plan list except the extra `tests/analytics/test_consumer_start.py`.

## Live stack

- Built and started `analytics` and `analytics-consumer` with `up -d --build --no-deps`; rebuilt `gateway` once with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 up -d --build --no-deps gateway`; rebuilt `analytics` (API) twice with `--no-deps` after the API and query changes. Nothing else was rebuilt or recreated; no volume was touched; no `.env*` or `infra/secrets/*` was read.
- `analytics-consumer` and `visit-consumer` were NOT stopped around test runs. The harness tests use unique topics, unique groups, unique clinics or a throw-away database, so none of them touches the live `analytics-projection` group; the e2e tests need the live consumer running. After the final run all services in `docker compose ps` are healthy (including `analytics`, `analytics-consumer`, `gateway`, `visit-consumer`, `visit-worker`).
- Side effects in the live analytics_db: e2e test clinics, patients, appointments and one completed visit (synthetic, left in place by suite convention). Test topics `p3test.*` were created and deleted by fixtures.

## Notes for later plans

- **03-16 (dashboards):** Grafana reads the same views through `grafana_ro`; the API reads them through the same definitions. The consumer metrics port (9100) is internal to the Compose network and unpublished; Prometheus must scrape it as `analytics-consumer:9100`.
- **03-18 (docs):** state D-13 as above (latest start, pinned offset, rebuild replays retained history), the definitions (cancellation rate = cancelled / booked over the window; wait = check-in to visit start; UTC days), `ANALYTICS_DATABASE_URL` for `.env.example` (host form `localhost:5433`), the rebuild operator procedure, and that `processed_events` has no retention.
- A rebuild of the live group, if ever run, replays the retained throw-away test events too.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-3-33: `require_role(ADMIN, FRONT_DESK)`, front desk forced to its own clinic, tests for every role and the denied paths (unit and live gateway). T-3-36: one database URL, no cross-service import, the two containers receive only `ANALYTICS_DATABASE_URL`, enforced by tests. T-3-37: 366-day cap, view queries by clinic and day, read-only API. T-3-59: `--apply` required, refusal under an active member, one transaction, offsets reset after the commit, procedure documented in the module docstring.

## Self-Check: PASSED

- FOUND: services/analytics/{app.py,Dockerfile,rebuild.py}, services/analytics/api/{__init__,analytics}.py, services/analytics/repository/queries.py, tests/analytics/{test_api,test_replay_rebuild,test_consumer_start}.py, tests/e2e/test_phase3_analytics.py.
- FOUND commits cf24df0, 8a7e39f, 0027e35, f9768fe, 0d86dc8 (`git rev-list --count 2a4c2a4..HEAD` = 5 before the docs commit).
