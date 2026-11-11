---
phase: 03-event-driven-system-observability
plan: 10
subsystem: analytics
tags: [kafka-consumer, projections, postgres-views, idempotency, order-tolerance, analytics_db]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-03 event schemas, 03-04 EventConsumer and claim_event, 03-05 analytics_db / analytics_svc / grafana_ro, 03-09 consumer service pattern"
provides:
  - services/analytics package core (settings, db helper, ORM mirror, Alembic tree, analytics_0001 applied to the live analytics_db)
  - apply_event (order-tolerant first-write-wins upserts for nine event types) and handle_event (claim_event + apply_event in one transaction)
  - analytics-projection consumer entrypoint (build_consumer, main), no container yet (plan 03-13)
  - five metric views with COMMENT ON VIEW definitions
  - 14 analytics tests and the service-isolation suite extended to cover analytics
affects: [03-11, 03-13, 03-16, 03-17, 03-18]
tech-stack:
  added: []
  patterns:
    - "Projection handler = one transaction: claim_event, then upserts, then commit; a failed effect rolls the claim back with it"
    - "Every upsert creates the row if absent; facts are COALESCE first-write-wins; the one mutable value (slot_start) is versioned by (occurred_at, slot_start) so arrival order never matters"
    - "Metric math lives in views; the API and Grafana only filter by clinic and window"
key-files:
  created:
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
  modified:
    - tests/libs/test_service_isolation.py
key-decisions:
  - "All appointment events (not just booked and rescheduled) carry a slot_start, so every one of them takes part in the same (occurred_at, slot_start) versioning of appointments.slot_start; last_event_at is therefore the latest occurred_at of any appointment event, not only of reschedules"
  - "The consumer subscribes to and handles nine event types (patient x2, appointment x6, visit.completed), not the 'seven' the plan text says; the plan's own apply_event list names nine"
  - "v_visit_wait is one row per completed visit (clinic_id, completed_at, wait_seconds) so a window average is plain avg(); v_cancellation returns per-day booked and cancelled counts so a window rate is sum(cancelled)/sum(booked)"
  - "Events for unknown ids are absorbed, not dead-lettered: they create a row that the views exclude (no booked_at, no registered_at)"
requirements-completed: []
status: complete
duration: about 1h20 wall time
completed: 2026-11-11
actuals:
  tokens: 15500
  tasks: 2
  commits: 2
plan_head_before: 5de2bfb1d90e8f62c34e7370cea5ef450e7d256e
plan_head_after: 4879a0bda0c9cc5ab02a90d3e4839a9fd0cc15ff
---

# Phase 3 Plan 10: Analytics Projections Summary

**An `analytics_db` projection store (three order-tolerant tables plus five metric views with written definitions) maintained by an `analytics-projection` Kafka consumer handler that claims the event id and applies the upsert in one transaction; proven on the live broker and live analytics_db with 14 tests.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (ANLY-01..06 and EVENT-03 stay for plan 03-13 and the phase gate). The read API, the Compose container, the gateway route and the replay rebuild are plan 03-13.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 0af62d2 | settings, db helper, ORM mirror, Alembic tree, `analytics_0001` (processed_events, three tables, `v_active_patients`), `projections.py`, `consumer.py`, three tracer tests |
| 2 | 4879a0b | the other four views and all five `COMMENT ON VIEW`, 11 handler-driven tests, conftest harness, isolation suite extension |

## What was built

- **Package.** `AnalyticsSettings` holds exactly one database URL (`ANALYTICS_DATABASE_URL`, default `localhost:5433/analytics_db`), `ANALYTICS_CONSUMER_METRICS_PORT` (9100), `ANALYTICS_CONSUMER_HEARTBEAT_PATH` (`/tmp/analytics-consumer.alive`), `ANALYTICS_DEFAULT_WINDOW_DAYS` (30). No OLTP database name appears anywhere in the package (grep and a test).
- **Schema (`analytics_0001`).** `processed_events` via `create_processed_events_table(op)`; `patients(patient_id PK, clinic_id, registered_at, deactivated_at)`; `appointments(...)` with `rescheduled_count` and `last_event_at`; `visits_completed(...)`; the indexes the plan names; five views, each with `COMMENT ON VIEW`:
  - `v_active_patients`: registered and not deactivated per clinic.
  - `v_appointments_booked_daily`: appointments with a `booked_at`, per clinic per UTC day.
  - `v_completed_visits_daily`: `visits_completed` per clinic per UTC day of `completed_at`.
  - `v_cancellation`: per clinic per UTC day of `booked_at`, `booked` and `cancelled` counts. Rate = sum(cancelled) / sum(booked); the denominator is appointments booked in the window.
  - `v_visit_wait`: one row per completed visit, `wait_seconds` = start minus check-in, only where both are present and the difference is non-negative (D-10 revised).
- **Projections.** `apply_event` dispatches nine event types. Core-API `INSERT ... ON CONFLICT DO UPDATE` via SQLAlchemy `pg_insert` (no SQL string building). Facts are `COALESCE(existing, new)`; patient timestamps use `LEAST`; `appointments.slot_start` is replaced only by a strictly newer `(occurred_at, slot_start)`; `rescheduled_count` adds 1 per applied reschedule (safe because the claim guards duplicates).
- **Consumer.** `handle_event(envelope, *, session_factory=None)`: `async with factory() as db, db.begin()`, `claim_event(consumer="analytics-projection")` (False returns with nothing to commit), then `apply_event`. A malformed payload (`KeyError`/`ValueError`/`TypeError`) is `NonRetryableEventError("InvalidEvent")`; DB outages are `TransientEventError`; the rest goes to the base's bounded retries. `build_consumer` / `main()` mirror plan 03-09 (`configure_logging`, `configure_tracer_provider("smarthealth-analytics-consumer")`, `warm_up`, heartbeat, metrics port, DLQ `smarthealth.dlq.analytics-projection`, topics patient, appointment and visit).

## Verification (measured)

- `.venv/bin/alembic -c services/analytics/migrations/alembic.ini upgrade head`: exit 0. After Task 2 added the views to `analytics_0001` (no new revision, as planned), `downgrade base` then `upgrade head` was run on the dev analytics_db: both exit 0. The dev analytics_db held only test rows at that point (no live consumer exists yet); no OLTP database or other service's data was touched.
- `tests/analytics/test_projections.py -k tracer`: 3 passed (plan: at least 2). Real `EventConsumer` + real `handle_event` on live Kafka (unique topic, DLQ topic and group per test, mock registry), unique clinic: two registrations reach `v_active_patients` = 2 within 30 s; a deactivation produced before its registration leaves the count at 1; settings expose exactly one database field.
- `tests/analytics/test_projections.py`: **14 passed** (plan: at least 10). The 11 Task 2 tests: fixed-sequence metric math (active 2; booked 4 as 2+2 across a 23:30 / 00:30 UTC day boundary; completed visits 3; cancellation 0.25; average wait 600 s from 2 samples, the visit without a start excluded from the wait only); reversed order (every cancel, completion and visit event before its booking) and a seeded shuffle for a second clinic give identical numbers; every event applied twice changes nothing (`rescheduled_count` stays 1, one claim per event); cancel-before-booked; a late reschedule never replaces a newer slot (applied newest, older, booking last: slot, count 2 and `booked_at` correct); another clinic's events change nothing; unknown ids and an unconsumed event type are absorbed; negative and missing waits are excluded from the wait only; a malformed event raises `NonRetryableEventError` and its claim is rolled back, then the well-formed event with the same id applies; ORM metadata equals the three tables and their exact columns; every view carries a comment.
- `.venv/bin/python -m pytest tests/infra tests/analytics tests/libs/test_service_isolation.py -q`: **51 passed**, `OTEL_SDK_DISABLED` unset (21 infra, 14 analytics, 16 isolation). The isolation suite now also asserts analytics owns exactly its three tables, has no cross-service import or foreign key, a prefixed database URL, no PHI columns, the role isolation to and from `analytics_db`, and a new test that no OLTP database name appears in `services/analytics` and the settings hold one database URL.
- Acceptance greps: `grep -c "analytics-projection" services/analytics/consumer.py` = 5; `grep -rlE "scheduling_db|visit_db|patient_db|provider_db" services/analytics` prints nothing; `grep -c "COMMENT ON VIEW"` = 6 (five statements plus one docstring mention).
- Semgrep: `semgrep --config p/python --metrics=off services/analytics`: 151 rules, 9 files, **0 findings** (narrower than the plan's `--config auto`, which is refused with `--metrics=off`, as in earlier Phase 3 plans). `tests/analytics` was not scanned.
- Not run: the plan's full `pytest tests -m "not e2e"` (the orchestrator said it is not required). The known unrelated `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` failure was not touched.
- After the last run `patients`, `appointments`, `visits_completed` and `processed_events` in analytics_db are empty (tests delete their own rows and claims; 20 stale claims from an earlier run before the cleanup fixture existed were deleted from the dev database by hand).

## Deviations from Plan

1. **[Plan wording] `models.py` and `repository/__init__.py` were created in Task 1, not Task 2.** The Alembic `env.py` imports the models for `target_metadata`, so the tracer needed them. Task 2 added nothing to them.
2. **[Plan wording] `tests/analytics/conftest.py` was created in Task 1** (live-Kafka fixtures are re-exported from `tests/events/conftest.py`, the unique clinic, the consumer-in-a-thread helper); Task 2 appended the envelope factory and projector harness.
3. **[Rule 1 - design] `slot_start` versioning covers all six appointment events.** The plan versions only reschedules by `last_event_at`. Cancel, check-in, completion and no-show events also carry `slot_start`; treating all of them as `(occurred_at, slot_start)` versions makes a row created by a late event converge to the same slot in any order, with a deterministic tie-break on equal millisecond timestamps (a strictly greater tuple wins).
4. **[Plan wording] nine handled event types, not seven.** The plan's handler list names nine (two patient, six appointment, `visit.completed`); the consumer registers all nine.
5. **[Plan wording] The Task 1 migration carried the three tables and `v_active_patients` only; Task 2 edited `analytics_0001` in place** (the plan's instruction), with a `downgrade base` then `upgrade head` recorded above.
6. **[Process] Tests were written alongside the implementation, not red-first** (the tracer commit contains the handler and projection code).
7. **Semgrep ruleset** `p/python --metrics=off`, not `--config auto`.
8. **Isolation suite extended in this plan.** The plan text assigns the analytics isolation tests to plan 03-13; the orchestrator asked for the extension here, so `tests/libs/test_service_isolation.py` was extended now. Plan 03-13 can add its API-level checks on top.

No auth gates, no package installs, no architectural changes.

## Live stack

No Compose change, no container started, stopped, rebuilt or recreated; `gateway`, `visit-consumer` and the other services were not touched. The only live-stack effects: Alembic migrations on the existing `analytics_db` (database objects named by the plan), test topics `p3test.*` created and deleted on the live broker by the fixtures (nothing reached a real event topic or DLQ), and test rows created and deleted in `analytics_db` (unique clinic per test). No `.env*` or `infra/secrets/*` file was read; the host-side `analytics_svc` default placeholder credential from `AnalyticsSettings` was used (it matches the live database). `tjo-*` containers were not touched. No live consumer exists for this group yet, so no harness test raced one; plan 03-13 will add the container and the tests that write appointment or visit events must then stop it around the run (as with `visit-consumer`).

## Notes for plan 03-13 and later

- The live topics already carry throwaway test events (patient, provider, scheduling and visit ids that no longer exist, clinics created by tests). The projection absorbs them (rows that views exclude, no DLQ), so when the container starts the consumer will first drain the backlog from `earliest`; D-13 (analytics start at cutover) is not enforced by the consumer, so per-clinic numbers on the real topics will include those test clinics. Dashboards and the read API filter by clinic.
- `processed_events` grows without bound (one row per event per consumer); a retention policy is not part of this plan.
- `v_visit_wait` is per visit, `v_cancellation` per day: the read API computes `avg(wait_seconds)` and `sum(cancelled)/sum(booked)` over the window and treats an empty denominator as "no data", not 0.
- The metrics port 9100 binds 0.0.0.0 inside the container; the Compose service must NOT publish it.
- Replay rebuild (truncate the three tables and `processed_events` rows for this consumer, then reset the group offsets) is plan 03-13.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-3-34: the tables hold ids and timestamps only (no free-text column, a test pins the table columns) and the views return counts and durations per clinic. T-3-35: the claim shares the effect's transaction (a test proves a failed effect rolls the claim back and the retry applies it), upserts are first-write-wins or versioned, `rescheduled_count` moves only under a fresh claim, and tests cover duplicates, reversed order and a seeded shuffle.

## Self-Check: PASSED

- FOUND: services/analytics/{__init__,config,db,consumer,projections}.py, services/analytics/repository/{__init__,models}.py, services/analytics/migrations/{alembic.ini,env.py,script.py.mako,versions/analytics_0001_projections.py}, tests/analytics/{__init__,conftest,test_projections}.py (all committed).
- FOUND commits 0af62d2, 4879a0b (`git rev-list --count 5de2bfb..HEAD` = 2 before the docs commit).
