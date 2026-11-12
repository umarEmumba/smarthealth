---
phase: 03-event-driven-system-observability
plan: 13
type: execute
wave: 6
depends_on: ["03-07", "03-08", "03-09", "03-10", "03-11"]
files_modified:
  - services/analytics/app.py
  - services/analytics/api/__init__.py
  - services/analytics/api/analytics.py
  - services/analytics/repository/queries.py
  - services/analytics/Dockerfile
  - services/analytics/rebuild.py
  - services/gateway/routing.py
  - infra/docker-compose.yml
  - tests/conftest.py
  - tests/analytics/test_api.py
  - tests/analytics/test_replay_rebuild.py
  - tests/e2e/test_phase3_analytics.py
  - tests/libs/test_service_isolation.py
  - tests/gateway/test_routing.py
autonomous: true
requirements: [ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06]

estimate:
  tokens: 95000
  raw_tokens: 95000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "An operator can read total patients, appointments booked over time, completed visits, cancellation rate and average wait time for a clinic from /api/analytics through the gateway (ANLY-01..05)"
    - "The numbers come from plan 03-10's views, maintained by the analytics-consumer container from Kafka; the analytics service has no URL, role or import for any OLTP database, enforced by the isolation tests (ANLY-06)"
    - "Admins see any clinic; front desk sees only its own clinic; provider and patient tokens get 403; no token gets 401"
    - "Projections can be rebuilt by replaying the topics and the rebuilt views equal the original ones; the rebuild refuses while the consumer group has active members (D-10)"
  artifacts:
    - path: services/analytics/api/analytics.py
      provides: "GET /api/analytics/summary, GET /api/analytics/timeseries with role and clinic scoping"
      contains: "require_role"
    - path: services/analytics/repository/queries.py
      provides: "summary(db, clinic_id, start, end) and timeseries(db, metric, clinic_id, start, end) over the views only"
    - path: services/analytics/rebuild.py
      provides: "python -m services.analytics.rebuild [--apply] [--group] [--topics]"
      contains: "describe_consumer_groups"
    - path: infra/docker-compose.yml
      provides: "analytics (API) and analytics-consumer services; gateway depends on analytics"
      contains: "analytics-consumer"
  key_links:
    - from: services/gateway/routing.py
      to: services/analytics/app.py
      via: "ROUTE_TABLE entry /api/analytics -> analytics"
      pattern: "/api/analytics"
    - from: services/analytics/api/analytics.py
      to: services/analytics/repository/queries.py
      via: "endpoints read only the metric views through queries"
      pattern: "queries"
---

<objective>
Serve the five operational metrics: the read-only analytics API behind the gateway, the `analytics` and `analytics-consumer` Compose containers (running plan 03-10's consumer entrypoint), role and clinic scoping, the service-isolation proof, and the replay-based rebuild tool.

Purpose: roadmap success criterion 3 (an operator can view all five metrics, computed from the event stream) and ANLY-06 (no polling of the transactional databases).

Output: `app.py`, the analytics router and queries, the Dockerfile, the rebuild CLI, the gateway route, two Compose services, harness and isolation registration, and tests including an end-to-end metric check through the gateway.

Decisions honoured: D-10 / D-10 revised (read-only analytics API over the projection store; rebuild by replay), database-per-service, research Pattern 9 and Pitfall 12. Claude's discretion: summary windows default to `ANALYTICS_DEFAULT_WINDOW_DAYS` (30), windows capped at 366 days.

Live stack rules: build and start the new containers with `docker compose -f infra/docker-compose.yml up -d --build analytics analytics-consumer`, then recreate the gateway so it gains the new route and dependency: `docker compose -f infra/docker-compose.yml up -d --build --no-deps gateway`. The analytics image runs `alembic upgrade head` at start against the existing volume (plan 03-10 already applied it from the host). On first start the consumer reads every retained event from the earliest offset (events written by earlier plans' tests): expected, idempotent and harmless. Never touch `tjo-*` containers (record status before and after). This is the only wave-6 plan that edits `infra/docker-compose.yml`. Wave 6 coupling: plan 03-12 runs alongside and recreates only `visit-worker`; this plan's e2e does not depend on `visit-worker` (completed visits reach analytics from the Visit API's outbox through Kafka), and neither plan touches the other's services.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-10-SUMMARY.md
@services/billing/app.py
@services/billing/Dockerfile
@services/gateway/routing.py
@services/analytics/consumer.py
@services/analytics/migrations/versions/analytics_0001_projections.py
@libs/auth/rbac.py
@tests/conftest.py
@tests/libs/test_service_isolation.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| API | HTTP | `GET /api/analytics/summary?clinic_id=&from=&to=` -> `{clinic_id, from, to, total_patients, appointments_booked, completed_visits, cancellation_rate, average_wait_seconds}`; `GET /api/analytics/timeseries?metric=appointments_booked|completed_visits&clinic_id=&from=&to=` -> list of `{day, value}`; `/health`; `/metrics` |
| `summary()`, `timeseries()` | functions | `services/analytics/repository/queries.py`; read only the views from plan 03-10 |
| rebuild | CLI | `python -m services.analytics.rebuild [--apply] [--group ...] [--topics ...]`: refuse when the group has members; truncate projections and the group's claims in one transaction; reset the group's offsets to the low watermarks |
| gateway | routing | `("/api/analytics", "analytics")` in `ROUTE_TABLE` (`resolve_upstream` uses `analytics_service_url`, added to SharedSettings in plan 03-01) |
| Compose | infra | `analytics` (API, Alembic on start, healthcheck, no host port), `analytics-consumer` (same image, `python -m services.analytics.consumer`, heartbeat healthcheck, no host port), gateway `depends_on` analytics healthy |
| harness and isolation | tests | `"analytics"` in `HARNESS_SERVICES`; `OWNED_TABLES["analytics"] = {"patients", "appointments", "visits_completed"}`, `BASES`, `SETTINGS` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a patient registered through the gateway appears in /api/analytics/summary total_patients via the analytics-consumer container and the projection</name>
  <reversibility rating="reversible">New containers and a new gateway prefix; removing them is a Compose and routing change with no data migration.</reversibility>
  <files>services/analytics/app.py, services/analytics/api/__init__.py, services/analytics/api/analytics.py, services/analytics/repository/queries.py, services/analytics/Dockerfile, services/gateway/routing.py, infra/docker-compose.yml, tests/e2e/test_phase3_analytics.py</files>
  <behavior>
    - An admin registers two patients in a fresh clinic through the gateway; within 60 s `GET /api/analytics/summary?clinic_id=<clinic>` through the gateway returns `total_patients == 2`
    - The analytics API answers `/health` 200 and exposes `/metrics` on its internal port; the `analytics-consumer` container is healthy
    - `/api/analyticsX` is not routed to analytics (prefix boundary)
  </behavior>
  <read_first>
    - services/billing/app.py, services/billing/Dockerfile (smallest existing service to mirror: lifespan, health router, Instrumentator, Alembic before gunicorn)
    - services/analytics/config.py, db.py, consumer.py, migrations (plan 03-10)
    - services/gateway/routing.py (ROUTE_TABLE; `resolve_upstream`)
    - infra/docker-compose.yml (`billing` and `visit-consumer` blocks to mirror; gateway `depends_on`)
    - libs/auth/rbac.py (`require_role`)
  </read_first>
  <action>
    `app.py` with lifespan, health router, `Instrumentator().instrument(app).expose(app)`, `setup_tracing(app, "smarthealth-analytics")` and the analytics router. `queries.py`: `summary(db, clinic_id | None, start, end)` reading only the views (in this task `total_patients` from `v_active_patients`; the other fields come in Task 2). `api/analytics.py`: `GET /api/analytics/summary` behind `require_role` for admin only in this task (front desk and the clinic rule land in Task 2). Dockerfile mirroring Billing's with `alembic -c services/analytics/migrations/alembic.ini upgrade head` before gunicorn.

    Gateway: add `("/api/analytics", "analytics")` to `ROUTE_TABLE` and update the module docstring's prefix list. Compose: `analytics` and `analytics-consumer` services per the artifacts table (`ANALYTICS_DATABASE_URL: postgresql+asyncpg://analytics_svc:${ANALYTICS_DB_PASSWORD:-changeme-analytics-local-dev-only}@postgres:5432/analytics_db`, `*app-env`, `x-app-depends-on`), the consumer with heartbeat healthcheck (file younger than 30 s, `start_period: 40s`), and `analytics` added to the gateway's `depends_on`. The host `ANALYTICS_DATABASE_URL` line for `.env.example` is added by the docs plan (03-18).

    e2e (marker e2e): register two patients through the gateway in a fresh clinic, poll the summary until `total_patients == 2` (60 s budget); assert the prefix boundary with one request to `/api/analyticsX`.

    Apply: the live stack rules' build and recreate commands, then the test.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, the summary never reaches 2 within 60 s, the gateway answers 404 for /api/analytics, or the analytics containers are unhealthy.</fails_when>
  </verify>
  <acceptance_criteria>
    - `docker compose -f infra/docker-compose.yml ps analytics analytics-consumer --format '{{.Service}} {{.Health}}'` prints both as `healthy`
    - `grep -c '"/api/analytics"' services/gateway/routing.py` is 1
    - `grep -c "analytics-consumer" infra/docker-compose.yml` is at least 1
    - the tracer test passes and tjo-* status lines show no restart
  </acceptance_criteria>
  <done>The analytics service derives total patients from the event stream and serves it through the gateway.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: All five metrics in the API, role and clinic scoping, timeseries, service isolation (ANLY-06) and the end-to-end metric check</name>
  <files>services/analytics/api/analytics.py, services/analytics/repository/queries.py, tests/conftest.py, tests/analytics/test_api.py, tests/libs/test_service_isolation.py, tests/gateway/test_routing.py, tests/e2e/test_phase3_analytics.py</files>
  <behavior>
    - Summary returns all five fields; cancellation rate is cancelled / booked for appointments booked in the window (null when none were booked); average wait is the mean over visits completed in the window
    - Admin: summary and timeseries for any clinic, or across all clinics when `clinic_id` is omitted
    - Front desk: the token's clinic only; a different `clinic_id` returns 403; omitted `clinic_id` means the token's clinic
    - Provider and patient tokens: 403; no token: 401
    - `from > to`, an unknown `metric`, or a window over 366 days: 422
    - Isolation: `analytics` appears in `OWNED_TABLES` (`patients`, `appointments`, `visits_completed`), `BASES`, `SETTINGS`; `test_no_cross_service_imports` and `test_database_url_is_service_prefixed` pass with it; a new test asserts the analytics package imports no `services.<other>` module and its settings expose exactly one database URL field
    - Routing test: `/api/analytics` maps to `analytics` and `/api/analyticsX` does not
    - e2e: in a fresh clinic, book three appointments, cancel one, check in, start and complete one visit through the gateway; within 90 s the summary shows `appointments_booked == 3`, `cancellation_rate` approximately 0.333, `completed_visits == 1` and a non-null `average_wait_seconds`
  </behavior>
  <read_first>
    - libs/auth/rbac.py and libs/auth/roles.py (`require_role`, role names), services/scheduling/api/appointments.py (clinic-scope pattern for front desk)
    - tests/conftest.py (`HARNESS_SERVICES`, `bootstrap_admin_and_login`), tests/libs/test_service_isolation.py (`OWNED_TABLES`, `BASES`, `SETTINGS`), tests/gateway/test_routing.py
    - services/analytics/repository/models.py (plan 03-10 `AnalyticsBase`)
    - tests/e2e/phase2_stack.py (`seed_phase2_world`, `book`, `book_slot_inside_checkin_window`), tests/e2e/test_phase2_flows.py (visit flow)
  </read_first>
  <action>
    queries.py: complete `summary` with all five fields and add `timeseries(db, metric, clinic_id | None, start, end)`, both reading only the views. API: enforce the clinic rule above with the existing `require_role` and current-user dependencies; validate query parameters with Pydantic; add `GET /api/analytics/timeseries`. Responses carry aggregate numbers and ids only (never a patient id).

    Harness and isolation: add `"analytics"` to `HARNESS_SERVICES` in `tests/conftest.py`; extend `OWNED_TABLES`, `BASES`, `SETTINGS` in `tests/libs/test_service_isolation.py` and add the analytics-specific isolation test; extend `tests/gateway/test_routing.py` with the new prefix cases. `tests/analytics/test_api.py` (marker `integration`) seeds projection rows for two clinics directly and calls through the in-process gateway mesh with admin, front desk, provider and patient tokens.

    e2e: add the full-flow case to `tests/e2e/test_phase3_analytics.py` using the Phase 2 helpers. Rebuild `analytics` with `--no-deps` after the API change.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_api.py tests/libs/test_service_isolation.py tests/gateway/test_routing.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a front desk user reads another clinic, the isolation test finds a cross-service import or a second database URL, or the e2e numbers do not match within 90 s.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/analytics/test_api.py -q` exits 0 with at least 8 passed
    - `.venv/bin/python -m pytest tests/libs/test_service_isolation.py tests/gateway/test_routing.py -q` exits 0
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -q` exits 0 with at least 2 passed
  </acceptance_criteria>
  <done>The five metrics are served with role and clinic scoping, and the analytics service is provably isolated from the OLTP databases.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Replay rebuild of the projections</name>
  <files>services/analytics/rebuild.py, tests/analytics/test_replay_rebuild.py</files>
  <behavior>
    - Rebuild: the fixed event sequence from plan 03-10 is produced to unique topics, consumed with `EventConsumer` running the real handler under a unique group; all views are snapshotted (filtered to the clinic); `rebuild --apply --group <g> --topics <t...>` runs with the consumer stopped; re-consuming to the end gives views identical to the snapshot
    - `rebuild --apply` refuses (exit 1, nothing truncated) while the group has an active member
    - Without `--apply`, rebuild prints what it would do and changes nothing
  </behavior>
  <read_first>
    - services/analytics/consumer.py and projections.py (plan 03-10), tests/analytics/conftest.py (plan 03-10 fixtures)
    - libs/events/consumer.py (EventConsumer for the re-consume) and confluent-kafka AdminClient `describe_consumer_groups`, `list_offsets`, `alter_consumer_group_offsets`
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9 rebuild notes)
  </read_first>
  <action>
    rebuild.py: `--group` (default `analytics-projection`), `--topics` (default the three event topics), `--apply` (without it, print what would happen); with `--apply` it refuses when the group has active members (AdminClient `describe_consumer_groups`), truncates the three projection tables and deletes the group's `processed_events` rows in one transaction, then sets the group's committed offsets to each partition's low watermark. Print counts and names only. Module docstring gives the operator procedure: stop `analytics-consumer`, run rebuild, start the consumer.

    Tests (marker `integration`): the behaviors above with unique topics, a unique group and a unique clinic; the active-member case keeps a consumer polling in a thread while rebuild runs.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_replay_rebuild.py -x -q && .venv/bin/python -m services.analytics.rebuild --help</automated>
    <fails_when>pytest exits non-zero, the rebuilt views differ from the snapshot, rebuild truncates while the group has a member, or `--help` exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/analytics/test_replay_rebuild.py -q` exits 0 with at least 3 passed
    - `.venv/bin/python -m services.analytics.rebuild --help` exits 0
    - `.venv/bin/python -m pytest tests/analytics -q` exits 0
  </acceptance_criteria>
  <done>A replay rebuild reproduces every metric view exactly and cannot run under a live consumer.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| client -> gateway -> analytics API | operators read aggregates scoped by role and clinic |
| operator -> rebuild CLI -> analytics_db and consumer offsets | a destructive maintenance action on derived data |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-33 | Elevation of Privilege | `/api/analytics` | high | mitigate | `require_role` admin/front desk; front desk forced to its own clinic (403 otherwise); provider and patient 403; no token 401; tests per role |
| T-3-36 | Elevation of Privilege | cross-database access | medium | mitigate | One database URL (`ANALYTICS_DATABASE_URL`) for `analytics_svc`, which has CONNECT on analytics_db only (db-init revokes PUBLIC); isolation tests enforce no cross-service imports and one URL field |
| T-3-37 | Denial of Service | unbounded analytics queries | low | mitigate | Window capped at 366 days, view queries indexed by clinic and timestamp, read-only API |
| T-3-59 | Tampering | rebuild run against a live consumer | medium | mitigate | `--apply` required; refuses while the group has active members; truncation and claim deletion in one transaction; offsets reset only after the commit; documented operator procedure |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/analytics tests/libs/test_service_isolation.py tests/gateway -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/analytics services/gateway`; record status in the SUMMARY
</verification>

<success_criteria>
- Five metrics served from event-derived projections in analytics_db, role- and clinic-scoped, through the gateway
- Analytics provably isolated from the OLTP databases; a replay rebuild reproduces the views
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-13-SUMMARY.md` when done
</output>
