---
phase: 03-event-driven-system-observability
plan: 09
type: execute
wave: 4
depends_on: ["03-03", "03-05", "03-06", "03-07"]
files_modified:
  - services/analytics/__init__.py
  - services/analytics/config.py
  - services/analytics/db.py
  - services/analytics/app.py
  - services/analytics/consumer.py
  - services/analytics/projections.py
  - services/analytics/rebuild.py
  - services/analytics/api/__init__.py
  - services/analytics/api/analytics.py
  - services/analytics/repository/__init__.py
  - services/analytics/repository/models.py
  - services/analytics/repository/queries.py
  - services/analytics/migrations/alembic.ini
  - services/analytics/migrations/env.py
  - services/analytics/migrations/script.py.mako
  - services/analytics/migrations/versions/analytics_0001_projections.py
  - services/analytics/Dockerfile
  - services/gateway/routing.py
  - infra/docker-compose.yml
  - .env.example
  - tests/conftest.py
  - tests/analytics/__init__.py
  - tests/analytics/conftest.py
  - tests/analytics/test_projections.py
  - tests/analytics/test_api.py
  - tests/analytics/test_replay_rebuild.py
  - tests/e2e/test_phase3_analytics.py
  - tests/libs/test_service_isolation.py
  - tests/gateway/test_routing.py
autonomous: true
requirements: [ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06, EVENT-03]

estimate:
  tokens: 120000
  raw_tokens: 120000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "An operator can read total patients, appointments booked over time, completed visits, cancellation rate and average wait time for a clinic from /api/analytics (ANLY-01..05)"
    - "Every number is computed from projections that only the analytics consumer writes from Kafka events, in analytics_db; the analytics service has no URL, role or import for any OLTP database (ANLY-06, D-10)"
    - "Average wait time is check-in to visit start from visit.completed; cancellation rate is cancelled among appointments booked in the window; both definitions are written next to the query (D-10 revised)"
    - "Each projection update and its processed_events claim commit in one transaction, so a redelivered event changes no number (EVENT-03, D-08 refined)"
    - "Projections can be rebuilt by replaying the topics and the rebuilt tables equal the original ones (D-10)"
    - "Admins see any clinic; front desk sees only its own clinic; other roles get 403"
  artifacts:
    - path: services/analytics/projections.py
      provides: "apply_event(db, envelope) for patient, appointment and visit events, order-tolerant upserts"
      contains: "claim_event"
    - path: services/analytics/migrations/versions/analytics_0001_projections.py
      provides: "processed_events, patients, appointments, visits_completed tables and the five metric views"
      contains: "CREATE VIEW"
    - path: services/analytics/api/analytics.py
      provides: "GET /api/analytics/summary, GET /api/analytics/timeseries with RBAC"
    - path: services/analytics/consumer.py
      provides: "analytics-projection consumer process"
      contains: "analytics-projection"
  key_links:
    - from: services/analytics/consumer.py
      to: services/analytics/projections.py
      via: "handler opens one transaction: claim_event then apply_event then commit"
      pattern: "apply_event"
    - from: services/gateway/routing.py
      to: services/analytics/app.py
      via: "ROUTE_TABLE entry /api/analytics -> analytics"
      pattern: "/api/analytics"
---

<objective>
Build the Analytics service: an event consumer that maintains projections in its own `analytics_db`, a read-only API behind the gateway, and a replay-based rebuild. This is the source for the five operational metrics.

Purpose: roadmap success criterion 3 and ANLY-01..06: the metrics come from the Kafka event stream, never from polling Scheduling, Visit or Patient databases.

Output: the `services/analytics` package (settings, models, migration with views, projections, consumer, rebuild tool, API, Dockerfile), gateway route, Compose services `analytics` and `analytics-consumer`, harness registration, tests including an e2e proof.

Decisions honoured: D-10 / D-10 revised (relational projection store in its own Postgres database; wait time = visit `started_at` minus `checked_in_at`; `patient.deactivated` lowers total patients; rebuild by replay), D-08 refined (processed-event table in the effect's transaction), database-per-service (analytics_db and `analytics_svc` created by plan 03-02's db-init; `grafana_ro` gets read access by default privileges), research Pattern 9 and Pitfall 12 (assert on a unique clinic per test). Claude's discretion: day buckets in UTC, cancellation rate denominator = appointments booked in the window, summary windows default to the last 30 days.

Live stack rules: build and start the new containers with `docker compose -f infra/docker-compose.yml up -d --build analytics analytics-consumer`, then recreate the gateway so it gains the new route and dependency: `docker compose -f infra/docker-compose.yml up -d --build --no-deps gateway`. The analytics image runs `alembic upgrade head` at start against the existing volume; host tests run `.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head`. On first start the consumer reads every retained event from the earliest offset (events written by earlier plans' tests): expected, idempotent and harmless. Never touch `tjo-*` containers (record status before and after). This is the only wave-4 plan that edits `infra/docker-compose.yml`.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-03-SUMMARY.md
@services/billing/app.py
@services/billing/config.py
@services/billing/migrations/env.py
@services/billing/Dockerfile
@services/gateway/routing.py
@libs/events/consumer.py
@libs/events/idempotency.py
@libs/auth/rbac.py
@tests/conftest.py
@tests/libs/test_service_isolation.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `AnalyticsSettings(SharedSettings)` | settings | `ANALYTICS_DATABASE_URL` (local-dev default on `localhost:5433/analytics_db`, same placeholder convention as the other services), `ANALYTICS_CONSUMER_METRICS_PORT` (9100), `ANALYTICS_CONSUMER_HEARTBEAT_PATH` (`/tmp/analytics-consumer.alive`), `ANALYTICS_DEFAULT_WINDOW_DAYS` (30) |
| tables | analytics_db | `processed_events` (from `create_processed_events_table`), `patients(patient_id PK, clinic_id, registered_at, deactivated_at)`, `appointments(appointment_id PK, clinic_id, patient_id, booked_at, slot_start, cancelled_at, checked_in_at, completed_at, no_show_at, rescheduled_count, last_event_at)`, `visits_completed(visit_id PK, appointment_id, clinic_id, checked_in_at, started_at, completed_at)` |
| views | analytics_db | `v_active_patients`, `v_appointments_booked_daily`, `v_completed_visits_daily`, `v_cancellation`, `v_visit_wait` |
| `apply_event(db, envelope)` | function | `services/analytics/projections.py` |
| consumer | process | `python -m services.analytics.consumer`; group `analytics-projection`; topics patient, appointment, visit; DLQ `smarthealth.dlq.analytics-projection`; tracer `smarthealth-analytics-consumer` |
| rebuild | CLI | `python -m services.analytics.rebuild [--apply] [--group ...]`: truncate projections and the group's claims, reset the group's offsets to the low watermarks (the live consumer must be stopped) |
| API | HTTP | `GET /api/analytics/summary?clinic_id=&from=&to=` -> `{clinic_id, from, to, total_patients, appointments_booked, completed_visits, cancellation_rate, average_wait_seconds}`; `GET /api/analytics/timeseries?metric=appointments_booked|completed_visits&clinic_id=&from=&to=` -> list of `{day, value}`; `/health`; `/metrics` |
| gateway | routing | `("/api/analytics", "analytics")` in `ROUTE_TABLE` |
| Compose | infra | `analytics` (API, Alembic on start, healthcheck, no host port), `analytics-consumer` (same image, heartbeat healthcheck), gateway `depends_on` analytics healthy |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a patient registered through the gateway appears in /api/analytics/summary total_patients via Kafka and the projection</name>
  <files>services/analytics/__init__.py, services/analytics/config.py, services/analytics/db.py, services/analytics/app.py, services/analytics/consumer.py, services/analytics/projections.py, services/analytics/api/__init__.py, services/analytics/api/analytics.py, services/analytics/repository/__init__.py, services/analytics/repository/models.py, services/analytics/migrations/alembic.ini, services/analytics/migrations/env.py, services/analytics/migrations/script.py.mako, services/analytics/migrations/versions/analytics_0001_projections.py, services/analytics/Dockerfile, services/gateway/routing.py, infra/docker-compose.yml, .env.example, tests/e2e/test_phase3_analytics.py</files>
  <behavior>
    - An admin registers two patients in a fresh clinic through the gateway; within 60 s `GET /api/analytics/summary?clinic_id=<clinic>` through the gateway returns `total_patients == 2`
    - The analytics API answers `/health` 200 and exposes `/metrics` on its internal port; the consumer container is healthy
    - The analytics process connects only to analytics_db (its settings expose no other database URL)
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9, Pitfall 12)
    - services/billing/app.py, services/billing/config.py, services/billing/db.py, services/billing/migrations/env.py and alembic.ini, services/billing/Dockerfile (smallest existing service to mirror: lifespan, health router, Instrumentator, Alembic async env)
    - services/gateway/routing.py (ROUTE_TABLE; `resolve_upstream` uses `<name>_service_url`, already added to SharedSettings in plan 03-01)
    - libs/events/consumer.py, libs/events/idempotency.py
    - services/visit/consumer.py (consumer process entrypoint pattern from plan 03-07)
    - infra/docker-compose.yml (`billing` and `visit-consumer` blocks to mirror; gateway `depends_on`)
  </read_first>
  <action>
    Scaffold the service by mirroring Billing: `config.py` (`AnalyticsSettings` per the artifacts table, alias-only binding), `db.py`, `repository/models.py` (`AnalyticsBase` with the three projection tables; `processed_events` is created by the migration helper and is not an ORM table), Alembic directory with an async `env.py` copied from Billing's pattern, `analytics_0001_projections.py` (revision `analytics_0001`) creating `processed_events` via `create_processed_events_table(op)`, the three tables, and for now `v_active_patients` (patients with `deactivated_at IS NULL`, grouped by clinic). `projections.py`: `apply_event(db, envelope)` dispatching on event type; Task 1 implements `patient.registered` (`INSERT ... ON CONFLICT (patient_id) DO UPDATE` keeping the earliest `registered_at`) and `patient.deactivated` (upsert setting `deactivated_at`, creating the row if the registration has not arrived). `consumer.py`: handler opens one session, `if await claim_event(db, event_id=..., consumer="analytics-projection")` then `apply_event`, then commits (a False claim commits nothing and returns); `main()` like the visit consumer with group `analytics-projection`, topics `smarthealth.patient.events`, `smarthealth.appointment.events`, `smarthealth.visit.events`, unknown types ignored by the base. `app.py` with lifespan, health router, `Instrumentator().instrument(app).expose(app)`, `setup_tracing(app, "smarthealth-analytics")` and the analytics router; `api/analytics.py` with `GET /api/analytics/summary` returning `total_patients` from `v_active_patients` (other fields come in Task 2) behind `require_role` for admin and front desk (the full clinic rule lands in Task 3; until then admin only). Dockerfile mirroring Billing's with `alembic -c services/analytics/migrations/alembic.ini upgrade head` before gunicorn.

    Gateway: add `("/api/analytics", "analytics")` to `ROUTE_TABLE` and update the module docstring's prefix list. Compose: `analytics` and `analytics-consumer` services per the artifacts table (`ANALYTICS_DATABASE_URL: postgresql+asyncpg://analytics_svc:${ANALYTICS_DB_PASSWORD:-changeme-analytics-local-dev-only}@postgres:5432/analytics_db`), `analytics` added to the gateway's `depends_on`. `.env.example`: add the host `ANALYTICS_DATABASE_URL` line beside the other service URLs (if the harness blocks the edit, list it in the SUMMARY).

    e2e (marker e2e): register two patients through the gateway in a fresh clinic, poll the summary until `total_patients == 2` (60 s budget).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, the summary never reaches 2 within 60 s, the gateway answers 404 for /api/analytics, or the analytics containers are unhealthy.</fails_when>
  </verify>
  <acceptance_criteria>
    - `docker compose -f infra/docker-compose.yml ps analytics analytics-consumer --format '{{.Service}} {{.Health}}'` prints both as `healthy`
    - `.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head` exits 0 on the host
    - `grep -c '"/api/analytics"' services/gateway/routing.py` is 1
    - the tracer test passes and tjo-* status lines show no restart
  </acceptance_criteria>
  <done>A new service derives total patients from the event stream and serves it through the gateway.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: All five metrics from appointment and visit events, order tolerance, idempotency, and replay rebuild</name>
  <files>services/analytics/projections.py, services/analytics/repository/queries.py, services/analytics/rebuild.py, services/analytics/migrations/versions/analytics_0001_projections.py, services/analytics/api/analytics.py, tests/analytics/__init__.py, tests/analytics/conftest.py, tests/analytics/test_projections.py, tests/analytics/test_replay_rebuild.py</files>
  <behavior>
    - A fixed sequence for one clinic (3 patients registered, 1 deactivated; 4 appointments booked on two days, 1 cancelled, 1 rescheduled, 2 checked in, 2 completed; 3 visit.completed events, two with waits of 300 s and 900 s and one with no `started_at`) yields: total_patients 2, appointments_booked 4 (2 per day in the timeseries), completed_visits 3, cancellation_rate 0.25, average_wait_seconds 600 (the visit without `started_at` is excluded from the average only)
    - The same events applied in a different order across aggregates (and a cancel that arrives before its booked) give the same numbers
    - Applying any event twice changes nothing (claim returns False; no double count)
    - Another clinic's events never change this clinic's numbers
    - Rebuild: projections from the sequence, snapshot all views, `rebuild --apply` against a unique test group and topics (stop-the-consumer precondition simulated), re-consume to the end: every view's rows are identical to the snapshot
  </behavior>
  <read_first>
    - services/analytics/projections.py and the migration (Task 1)
    - libs/events/schemas/*.avsc (the exact field names of appointment and visit events)
    - libs/events/consumer.py (EventConsumer for the rebuild re-consume) and confluent-kafka AdminClient `alter_consumer_group_offsets` / `list_offsets` usage
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9 view definitions and rebuild notes)
  </read_first>
  <action>
    projections.py: handlers for `appointment.booked` (upsert `booked_at`, `slot_start`, ids, keeping existing non-null timestamps), `appointment.cancelled` (`cancelled_at`), `appointment.rescheduled` (`slot_start` = new start only when the event's `occurred_at` is newer than `last_event_at`; `rescheduled_count + 1` guarded by the claim), `appointment.checked_in` (`checked_in_at`), `appointment.completed` (`completed_at`), `appointment.no_show` (`no_show_at`), `visit.completed` (upsert into `visits_completed`); every upsert creates the row if absent so arrival order never matters, and timestamps are set with `COALESCE(existing, new)` for first-write-wins facts.

    Migration: add the remaining views to `analytics_0001` (no new revision: nothing is deployed beyond Task 1's dev stack; downgrade the dev database with `alembic downgrade base` and upgrade again, recording that in the SUMMARY): `v_appointments_booked_daily` (clinic, UTC day of `booked_at`, count), `v_completed_visits_daily` (clinic, UTC day of `completed_at`, count), `v_cancellation` (clinic, UTC day of `booked_at`, booked count, cancelled count), `v_visit_wait` (clinic, `completed_at`, `extract(epoch from started_at - checked_in_at)` where both are set and the difference is non-negative). Put each metric's definition as a SQL comment on the view (`COMMENT ON VIEW`).

    queries.py: `summary(db, clinic_id | None, start, end)` and `timeseries(db, metric, clinic_id | None, start, end)` reading only the views; cancellation rate is cancelled / booked for appointments booked in the window (None when none were booked); average wait is the mean over visits completed in the window. Extend the summary endpoint to return all five fields.

    rebuild.py: `--group` (default `analytics-projection`), `--topics` (default the three), `--apply` (without it, print what would happen); with `--apply` it refuses when the group has active members (AdminClient `describe_consumer_groups`), truncates the three projection tables and deletes the group's `processed_events` rows in one transaction, then sets the group's committed offsets to each partition's low watermark. Module docstring gives the operator procedure: stop `analytics-consumer`, run rebuild, start the consumer.

    Tests (marker `integration`): `tests/analytics/conftest.py` gives a unique clinic id per test and an envelope factory; projection tests drive `apply_event` through the consumer handler on the live analytics_db; the rebuild test uses unique topics and a unique group, produces the fixed sequence with `produce_event`, consumes with `EventConsumer` running the real handler, snapshots views filtered to the clinic, runs rebuild with `--apply` on that group, consumes again, compares.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_projections.py tests/analytics/test_replay_rebuild.py -x -q</automated>
    <fails_when>pytest exits non-zero, any metric differs from the expected value, order changes a number, a duplicate is counted, or the rebuilt views differ from the snapshot.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/analytics/test_projections.py -q` exits 0 with at least 8 passed
    - `.venv/bin/python -m pytest tests/analytics/test_replay_rebuild.py -q` exits 0
    - `grep -c "COMMENT ON VIEW" services/analytics/migrations/versions/analytics_0001_projections.py` is at least 4
    - `.venv/bin/python -m services.analytics.rebuild --help` exits 0
  </acceptance_criteria>
  <done>All five metrics are projected idempotently and order-tolerantly from events, and a replay rebuild reproduces them.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Access control, timeseries, service isolation (ANLY-06) and the end-to-end metric check</name>
  <files>services/analytics/api/analytics.py, tests/conftest.py, tests/analytics/test_api.py, tests/libs/test_service_isolation.py, tests/gateway/test_routing.py, tests/e2e/test_phase3_analytics.py</files>
  <behavior>
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
    - tests/conftest.py (`HARNESS_SERVICES`, `bootstrap_admin_and_login`), tests/libs/test_service_isolation.py, tests/gateway/test_routing.py
    - tests/e2e/phase2_stack.py (`seed_phase2_world`, `book`, `book_slot_inside_checkin_window`), tests/e2e/test_phase2_flows.py (visit flow)
  </read_first>
  <action>
    API: enforce the clinic rule above with the existing `require_role` and current-user dependencies; validate query parameters with Pydantic; add `GET /api/analytics/timeseries`. Responses carry aggregate numbers and ids only.

    Harness and isolation: add `"analytics"` to `HARNESS_SERVICES` in `tests/conftest.py`; extend `OWNED_TABLES`, `BASES`, `SETTINGS` in `tests/libs/test_service_isolation.py` and add the analytics-specific isolation test; extend `tests/gateway/test_routing.py` with the new prefix cases. `tests/analytics/test_api.py` (marker `integration`) seeds projection rows for two clinics directly and calls through the in-process gateway mesh with admin, front desk, provider and patient tokens.

    e2e: add the full-flow case to `tests/e2e/test_phase3_analytics.py` using the Phase 2 helpers. Rebuild `analytics` with `--no-deps` after the API change.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics tests/libs/test_service_isolation.py tests/gateway/test_routing.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a front desk user reads another clinic, the isolation test finds a cross-service import or a second database URL, or the e2e numbers do not match within 90 s.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/analytics/test_api.py -q` exits 0 with at least 8 passed
    - `.venv/bin/python -m pytest tests/libs/test_service_isolation.py tests/gateway/test_routing.py -q` exits 0
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -q` exits 0 with at least 2 passed
  </acceptance_criteria>
  <done>The five metrics are served with role and clinic scoping, and the analytics service is provably isolated from the OLTP databases.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| client -> gateway -> analytics API | operators read aggregates scoped by role and clinic |
| Kafka -> analytics-consumer -> analytics_db | events become projection rows |
| Grafana (grafana_ro) -> analytics_db | read-only dashboards (plan 03-12) |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-33 | Elevation of Privilege | `/api/analytics` | high | mitigate | `require_role` admin/front desk; front desk forced to its own clinic (403 otherwise); provider and patient 403; tests per role |
| T-3-34 | Information Disclosure | projections and API responses | medium | mitigate | Tables hold ids and timestamps only (no PHI arrives in events); API returns aggregates; `patient_id` never returned |
| T-3-35 | Tampering | replayed or duplicated events | medium | mitigate | `claim_event` in the same transaction as each upsert; order-tolerant first-write-wins upserts; rebuild refuses while the group has members |
| T-3-36 | Elevation of Privilege | cross-database access | medium | mitigate | One database URL (`ANALYTICS_DATABASE_URL`) for `analytics_svc`, which has CONNECT on analytics_db only (db-init revokes PUBLIC); isolation tests enforce no cross-service imports |
| T-3-37 | Denial of Service | unbounded analytics queries | low | mitigate | Window capped at 366 days, view queries indexed by clinic and timestamp, read-only API |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/analytics tests/libs/test_service_isolation.py tests/gateway -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/analytics services/gateway`; record status in the SUMMARY
</verification>

<success_criteria>
- Five metrics served from event-derived projections in analytics_db, role- and clinic-scoped
- Idempotent, order-tolerant projections with a proven replay rebuild
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-09-SUMMARY.md` when done
</output>
