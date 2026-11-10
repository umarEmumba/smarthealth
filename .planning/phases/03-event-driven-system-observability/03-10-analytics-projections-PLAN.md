---
phase: 03-event-driven-system-observability
plan: 10
type: execute
wave: 4
depends_on: ["03-03", "03-04", "03-05"]
files_modified:
  - services/analytics/__init__.py
  - services/analytics/config.py
  - services/analytics/db.py
  - services/analytics/consumer.py
  - services/analytics/projections.py
  - services/analytics/repository/__init__.py
  - services/analytics/repository/models.py
  - services/analytics/migrations/alembic.ini
  - services/analytics/migrations/env.py
  - services/analytics/migrations/script.py.mako
  - services/analytics/migrations/versions/analytics_0001_projections.py
  - tests/analytics/__init__.py
  - tests/analytics/conftest.py
  - tests/analytics/test_projections.py
autonomous: true
requirements: [ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06, EVENT-03]

estimate:
  tokens: 80000
  raw_tokens: 80000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "Every analytics number is computed from projections that only the analytics-projection consumer writes from Kafka events, in its own analytics_db; the analytics package holds exactly one database URL and reads no OLTP database (ANLY-06, D-10)"
    - "The five metric views give, for a fixed event sequence, the exact total patients, appointments booked per day, completed visits, cancellation rate and average wait time (ANLY-01..05)"
    - "Average wait time is check-in to visit start from visit.completed; cancellation rate is cancelled among appointments booked in the window; each definition is a COMMENT ON VIEW next to its query (D-10 revised)"
    - "Each projection update and its processed_events claim commit in one transaction, so a redelivered event changes no number, and events applied in any cross-aggregate order give the same numbers (EVENT-03, D-08 refined)"
    - "Projections run on a Kafka consumer, not a Celery task: the EVENT-04 deviation put to the user in plan 03-01 Task 2 (projections need the idempotency claim in the effect's own transaction)"
  artifacts:
    - path: services/analytics/projections.py
      provides: "apply_event(db, envelope) for patient, appointment and visit events, order-tolerant first-write-wins upserts"
      contains: "COALESCE"
    - path: services/analytics/consumer.py
      provides: "handle_event (claim_event then apply_event in one transaction) and the analytics-projection process entrypoint (its container is added by plan 03-13)"
      contains: "analytics-projection"
    - path: services/analytics/migrations/versions/analytics_0001_projections.py
      provides: "processed_events, patients, appointments, visits_completed tables and the five metric views with COMMENT ON VIEW"
      contains: "CREATE VIEW"
    - path: tests/analytics/test_projections.py
      provides: "live-Kafka tracer through the real handler; fixed-sequence metric math; order tolerance; idempotency; clinic isolation"
  key_links:
    - from: services/analytics/consumer.py
      to: services/analytics/projections.py
      via: "handler opens one transaction: claim_event then apply_event then commit"
      pattern: "apply_event"
    - from: services/analytics/consumer.py
      to: libs/events/idempotency.py
      via: "claim_event(db, event_id=..., consumer='analytics-projection') in the same session as the upsert"
      pattern: "claim_event"
---

<objective>
Build the analytics projection store: an `analytics_db` schema with projection tables and the five metric views, and the analytics-projection consumer handler that maintains them from Kafka events idempotently and order-tolerantly. This is the computing half of the five operational metrics; plan 03-13 adds the read API, the Compose containers, the gateway route, the isolation tests and the replay rebuild.

Purpose: roadmap success criterion 3 and ANLY-01..06: the metrics come from the Kafka event stream, never from polling Scheduling, Visit or Patient databases.

Output: the `services/analytics` package core (settings, database helper, ORM mirror of the tables, Alembic directory and `analytics_0001`, projections, consumer handler and entrypoint) and integration tests on the live Kafka broker and the live analytics_db.

Decisions honoured: D-10 / D-10 revised (relational projection store in its own Postgres database; wait time = visit `started_at` minus `checked_in_at`; `patient.deactivated` lowers total patients; rebuild by replay is plan 03-13), D-08 refined (processed-event table in the effect's transaction), database-per-service (analytics_db and `analytics_svc` created by plan 03-05's db-init; `grafana_ro` reads by default privileges), research Pattern 9 and Pitfall 12 (assert on a unique clinic per test). The EVENT-04 deviation (projections on a Kafka consumer, not Celery) is as answered at plan 03-01 Task 2; if that SUMMARY records a veto, stop before Task 1 and return for replanning. Claude's discretion: day buckets in UTC, cancellation-rate denominator = appointments booked in the window.

Live stack rules: no Compose change and no container start in this plan. Host-side migration: `.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head` against the existing analytics_db. Tests use the live Kafka with a unique topic and group per test (so nothing reaches the real topics) and the live analytics_db with a unique clinic per test. Wave 4 coupling: plans 03-07, 03-08 and 03-09 run alongside and rebuild only their own services; this plan touches no container, so their rebuilds cannot affect it and it cannot affect them.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-04-SUMMARY.md
@services/billing/config.py
@services/billing/migrations/env.py
@libs/events/consumer.py
@libs/events/idempotency.py
@libs/events/schemas/VisitCompleted.avsc
@tests/events/conftest.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `AnalyticsSettings(SharedSettings)` | settings | `ANALYTICS_DATABASE_URL` (local-dev default on `localhost:5433/analytics_db`, same placeholder convention as the other services), `ANALYTICS_CONSUMER_METRICS_PORT` (9100), `ANALYTICS_CONSUMER_HEARTBEAT_PATH` (`/tmp/analytics-consumer.alive`), `ANALYTICS_DEFAULT_WINDOW_DAYS` (30) |
| tables | analytics_db | `processed_events` (from `create_processed_events_table`), `patients(patient_id PK, clinic_id, registered_at, deactivated_at)`, `appointments(appointment_id PK, clinic_id, patient_id, booked_at, slot_start, cancelled_at, checked_in_at, completed_at, no_show_at, rescheduled_count, last_event_at)`, `visits_completed(visit_id PK, appointment_id, clinic_id, checked_in_at, started_at, completed_at)` |
| views | analytics_db | `v_active_patients`, `v_appointments_booked_daily`, `v_completed_visits_daily`, `v_cancellation`, `v_visit_wait`, each with `COMMENT ON VIEW` stating its definition |
| `AnalyticsBase` + three ORM classes | models | `services/analytics/repository/models.py`; mirror of the three projection tables (`processed_events` is not an ORM table), used by plan 03-13's isolation harness |
| `apply_event(db, envelope)` | function | `services/analytics/projections.py` |
| `handle_event(envelope, *, session_factory=None)`, `main()` | process | `services/analytics/consumer.py`; group `analytics-projection`; topics `smarthealth.patient.events`, `smarthealth.appointment.events`, `smarthealth.visit.events`; DLQ `smarthealth.dlq.analytics-projection`; tracer `smarthealth-analytics-consumer` |
| tests | tests | `tests/analytics/conftest.py` (unique clinic id, envelope factory, migrated session), `tests/analytics/test_projections.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a patient.registered event on Kafka reaches the projection through the real consumer handler and shows in v_active_patients</name>
  <reversibility rating="reversible">A new database schema owned by a new service; dropping analytics_0001 loses only derived data that a replay rebuilds.</reversibility>
  <precondition>analytics_db and the analytics_svc role exist (plan 03-05 db-init); check with `docker compose -f infra/docker-compose.yml exec -T postgres psql -U smarthealth -d postgres -Atc "select 1 from pg_database where datname='analytics_db'"` printing 1.</precondition>
  <files>services/analytics/__init__.py, services/analytics/config.py, services/analytics/db.py, services/analytics/consumer.py, services/analytics/projections.py, services/analytics/migrations/alembic.ini, services/analytics/migrations/env.py, services/analytics/migrations/versions/analytics_0001_projections.py, tests/analytics/__init__.py, tests/analytics/test_projections.py</files>
  <behavior>
    - Two `patient.registered` events for a fresh clinic, produced with the registered Avro schema to a unique topic, are consumed by an in-process `EventConsumer` running the real `handle_event`; within 30 s `SELECT active_patients FROM v_active_patients WHERE clinic_id = :c` returns 2
    - A `patient.deactivated` for one of them lowers it to 1, even when the deactivation is applied before the registration
    - `AnalyticsSettings` exposes exactly one database URL field (`database_url`)
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9, Pitfall 12)
    - services/billing/config.py, services/billing/db.py, services/billing/migrations/env.py and alembic.ini (smallest existing service to mirror: settings, Alembic async env)
    - libs/events/consumer.py (EventConsumer, EventEnvelope), libs/events/idempotency.py (`claim_event`, `create_processed_events_table`)
    - tests/events/conftest.py (`unique_topic`, `produce_event`, `mock_registry` fixtures from plan 03-04)
    - libs/events/schemas/PatientRegistered.avsc, PatientDeactivated.avsc (field names)
  </read_first>
  <action>
    Scaffold by mirroring Billing: `config.py` (`AnalyticsSettings` per the artifacts table, alias-only binding), `db.py`, Alembic directory with an async `env.py` copied from Billing's pattern and `alembic.ini`, `analytics_0001_projections.py` (revision `analytics_0001`) creating `processed_events` via `create_processed_events_table(op)`, the three tables with indexes on `(clinic_id, booked_at)`, `(clinic_id, completed_at)` and `(clinic_id)`, and for now the view `v_active_patients` (count of patients with `deactivated_at IS NULL`, grouped by clinic).

    projections.py: `apply_event(db, envelope)` dispatching on event type; in this task `patient.registered` (`INSERT ... ON CONFLICT (patient_id) DO UPDATE` keeping the earliest `registered_at` via `LEAST`/`COALESCE`) and `patient.deactivated` (upsert setting `deactivated_at`, creating the row if the registration has not arrived). Unknown types return without effect.

    consumer.py: `handle_event(envelope, *, session_factory=None)` opens one session (factory created lazily inside the consumer's persistent loop), and in one transaction: `if await claim_event(db, event_id=envelope.event_id, consumer="analytics-projection")` then `apply_event`, then commit (a False claim commits nothing and returns). `main()` mirrors the visit consumer from plan 03-09 (`configure_logging()`, `configure_tracer_provider("smarthealth-analytics-consumer")`, registry warm-up, `EventConsumer(group_id="analytics-projection", topics=[...], handlers={t: handle_event for t in the seven projected types}, dlq_topic=DLQ_TOPICS["analytics-projection"], heartbeat_path=..., metrics_port=...)`). The container that runs `main()` is plan 03-13's.

    Tests (marker `integration`, names starting `test_tracer_` so the verify selector picks them): migrate the live analytics_db on the host first (`alembic upgrade head`), then the behaviors above with the real handler, a unique topic and group, and a unique clinic id. The consumer runs in a thread with a stop event so the test ends cleanly.
  </action>
  <verify>
    <automated>.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head && .venv/bin/python -m pytest tests/analytics/test_projections.py -k tracer -x -q</automated>
    <fails_when>The migration exits non-zero, pytest exits non-zero, the view never reaches 2 within 30 s, or the deactivation applied first leaves a count of 2.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head` exits 0 on the host
    - `.venv/bin/python -m pytest tests/analytics/test_projections.py -k tracer -q` exits 0 with at least 2 passed
    - `grep -c "analytics-projection" services/analytics/consumer.py` is at least 1
    - `grep -rlE "scheduling_db|visit_db|patient_db|provider_db" services/analytics` prints nothing
  </acceptance_criteria>
  <done>A patient event on Kafka becomes a projection row through the real handler, and the active-patients view counts it.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: All five metric views from appointment and visit events, order tolerance, idempotency and clinic isolation</name>
  <files>services/analytics/projections.py, services/analytics/repository/__init__.py, services/analytics/repository/models.py, services/analytics/migrations/script.py.mako, services/analytics/migrations/versions/analytics_0001_projections.py, tests/analytics/conftest.py, tests/analytics/test_projections.py</files>
  <behavior>
    - A fixed sequence for one clinic (3 patients registered, 1 deactivated; 4 appointments booked on two days, 1 cancelled, 1 rescheduled, 2 checked in, 2 completed; 3 visit.completed events, two with waits of 300 s and 900 s and one with no `started_at`) yields through the views: active patients 2, appointments booked 4 (2 per day in `v_appointments_booked_daily`), completed visits 3, cancellation rate 0.25 (cancelled / booked from `v_cancellation`), average wait 600 s from `v_visit_wait` (the visit without `started_at` is excluded from the wait only)
    - The same events applied in a different order across aggregates (and a cancel that arrives before its booked) give the same numbers
    - Applying any event twice changes nothing (claim returns False; no double count, including `rescheduled_count`)
    - Another clinic's events never change this clinic's numbers
    - `AnalyticsBase.metadata.tables` contains exactly `patients`, `appointments` and `visits_completed`
  </behavior>
  <read_first>
    - services/analytics/projections.py and the migration (Task 1)
    - libs/events/schemas/*.avsc (the exact field names of appointment and visit events)
    - services/billing/repository/models.py (declarative base style to mirror) and services/billing/migrations/script.py.mako
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9 view definitions)
  </read_first>
  <action>
    projections.py: handlers for `appointment.booked` (upsert `booked_at`, `slot_start`, ids, keeping existing non-null timestamps), `appointment.cancelled` (`cancelled_at`), `appointment.rescheduled` (`slot_start` = new start only when the event's `occurred_at` is newer than `last_event_at`; `rescheduled_count + 1` guarded by the claim), `appointment.checked_in` (`checked_in_at`), `appointment.completed` (`completed_at`), `appointment.no_show` (`no_show_at`), `visit.completed` (upsert into `visits_completed`); every upsert creates the row if absent so arrival order never matters, and first-write-wins facts use `COALESCE(existing, new)`.

    Migration: add the remaining views to `analytics_0001` (no new revision: nothing beyond the dev stack has it; run `alembic downgrade base` then `upgrade head` on the dev analytics_db and record that in the SUMMARY): `v_appointments_booked_daily` (clinic, UTC day of `booked_at`, count), `v_completed_visits_daily` (clinic, UTC day of `completed_at`, count), `v_cancellation` (clinic, UTC day of `booked_at`, booked count, cancelled count), `v_visit_wait` (clinic, `completed_at`, `extract(epoch from started_at - checked_in_at)` where both are set and the difference is non-negative). Put each metric's definition in a `COMMENT ON VIEW` (five comments, `v_active_patients` included).

    models.py: `AnalyticsBase` with ORM classes mirroring the three projection tables (column names and types identical to the migration); `repository/__init__.py` re-exports them. `script.py.mako`: copy of Billing's.

    Tests (marker `integration`): `tests/analytics/conftest.py` gives a unique clinic id per test, an envelope factory built from the catalogue schemas, and a session on the migrated analytics_db; projection tests drive `handle_event` directly with envelopes (the tracer already proved the Kafka path) and read the views with SQL filtered to the clinic.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_projections.py -x -q</automated>
    <fails_when>pytest exits non-zero, any metric differs from the expected value, order changes a number, a duplicate is counted, another clinic's events change the numbers, or the ORM tables differ from the three projection tables.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/analytics/test_projections.py -q` exits 0 with at least 10 passed
    - `grep -c "COMMENT ON VIEW" services/analytics/migrations/versions/analytics_0001_projections.py` is at least 5
    - `.venv/bin/alembic -c services/analytics/migrations/alembic.ini downgrade base && .venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head` exits 0 (dev database only)
  </acceptance_criteria>
  <done>All five metrics are projected idempotently and order-tolerantly from events into views with written definitions.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka -> analytics-projection handler -> analytics_db | events become projection rows |
| analytics_db -> Grafana (grafana_ro) and the read API | derived data leaves the store (plans 03-13 and 03-16) |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-34 | Information Disclosure | projection tables and views | medium | mitigate | Tables hold ids and timestamps only (no PHI arrives in events, plan 03-03 scan); views return counts and durations per clinic; no free-text column exists |
| T-3-35 | Tampering | replayed, duplicated or reordered events | medium | mitigate | `claim_event` in the same transaction as each upsert; order-tolerant first-write-wins upserts; `rescheduled_count` increments only under a fresh claim; tests cover duplicates and reordering |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/analytics -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/analytics`; record status in the SUMMARY
</verification>

<success_criteria>
- Five metric views over event-derived projections in analytics_db, with written definitions
- Idempotent, order-tolerant, clinic-isolated projections driven by the real consumer handler
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-10-SUMMARY.md` when done
</output>
