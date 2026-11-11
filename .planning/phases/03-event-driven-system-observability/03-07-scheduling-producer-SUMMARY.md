---
phase: 03-event-driven-system-observability
plan: 07
subsystem: scheduling
tags: [transactional-outbox, avro, debezium, kafka, opentelemetry, appointments]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-01 enqueue_event / warm_up, 03-03 the six appointment Avro schemas, 03-05 scheduling_0006 outbox + scheduling-outbox connector"
provides:
  - services/scheduling/events.py (SCHEDULING_EVENT_TYPES, six PHI-free payload builders, emit_appointment_event)
  - outbox emission in six appointment transitions, same transaction as the state change and history row
  - schema-id warm-up in the Scheduling API lifespan and before the Scheduling worker polls
  - 33 live-DB outbox tests and 3 gateway e2e tests
affects: [03-08, 03-09, 03-10, 03-11, 03-13, 03-14, 03-17]
tech-stack:
  added: []
  patterns:
    - "Hook at the transition functions (not append_status_history): each applied branch RETURNs the appointment columns and emits right after its history row"
    - "Event id discriminator = the post-transition appointments.version, so a retried activity (already committed) emits nothing and a duplicate emission collides on the outbox primary key"
    - "Payload builders read ids and timestamps only; coverage_ref and free-text columns are never selected into an event"
key-files:
  created:
    - services/scheduling/events.py
    - tests/scheduling/test_outbox.py
    - tests/e2e/test_phase3_booking_events.py
  modified:
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/app.py
    - services/scheduling/worker.py
key-decisions:
  - "occurred_at of every event is the row's updated_at (the database transaction time of the transition), not the producer's wall clock; the typed timestamps in each payload (booked_at, cancelled_at, checked_in_at, ...) are the same instant"
  - "A worker that cannot warm the registry after the retries exits 1 (restart policy retries it), like other start-up failures"
  - "Workflows, activities and activity names are unchanged: no workflow.patched, recorded-history replay tests pass"
requirements-completed: []
status: complete
duration: about 75 min wall time (about 35 of them waiting on the 10-minute scheduling/workflows regression and image builds)
completed: 2026-11-11
actuals:
  tokens: 13500
  tasks: 3
  commits: 2
plan_head_before: 0a2e3a4bb669e3cb3187a53bc4d0fae44d15af74
plan_head_after: 84444c41a0e0a273cf16ef8d91f88a961e887897
---

# Phase 3 Plan 07: Scheduling Producer Summary

**Scheduling is now an event producer: confirm, cancel, reschedule, check-in, complete and no-show each commit an Avro outbox row with the state change and history row (uuid5 id from the post-transition version, traceparent of the producing span, PHI-free payload), the 202 accept path is untouched, and a gateway booking appears on `smarthealth.appointment.events` keyed by the appointment id inside the request's own trace.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (EVENT-01, EVENT-02, NOTIF-01, ANLY-02, ANLY-04 stay for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 1067b35 | `events.py`; `confirm_booking` emits `appointment.booked`; warm-up in API lifespan and worker; e2e test file |
| 2 | 84444c4 | The other five transitions; `tests/scheduling/test_outbox.py` (33 tests) |
| 3 | none | No file of its own: its three scenarios were already in the e2e file committed with Task 1, and no code changed after Task 2 (see Deviations) |

## What was built

- `services/scheduling/events.py`: the six event-type constants, `SCHEDULING_EVENT_TYPES`, builders `booked_payload`, `cancelled_payload`, `rescheduled_payload`, `checked_in_payload`, `completed_payload`, `no_show_payload`, and `emit_appointment_event(db, event_type, row, **extra)`, which calls `enqueue_event(..., aggregate_id=str(row.id), discriminator=str(row.version), occurred_at=row.updated_at)`.
- `appointment_repo.py`: each of `confirm_booking`, `cancel_appointment`, `swap_slot`, `check_in_appointment`, `complete_appointment`, `mark_no_show` changes its UPDATE to `RETURNING id, clinic_id, patient_id, provider_id, slot_id, slot_start, slot_end, version, updated_at` (literal SQL, no f-strings) and calls `emit_appointment_event` right after `append_status_history`. Every early-return branch (repeat, refused, not found, lost hold) returns before any emission. `swap_slot` also reads `slot_start` in its locking SELECT to give the rescheduled payload its old start. Return values and signatures are unchanged. `accept_booking`, `mark_appointment_failed`, `record_history_note` and `flag_schedule_change` are untouched.
- `app.py` and `worker.py`: `await warm_up(SCHEDULING_EVENT_TYPES)` (API: after the audit indexes in the lifespan; worker: before `ensure_schedules`, so before polling).
- Payload contents: ids (appointment, clinic, patient, provider, slot) and timestamps only. Rescheduled carries old and new slot ids and starts plus `new_slot_end`; checked_in carries `slot_start` and `checked_in_at`; no builder touches `coverage_ref`, `failure_reason` or `schedule_change_reason`.

## Test results (honest)

- `tests/scheduling/test_outbox.py`: **33 passed** (plan needed at least 12). Covers all six transitions (row count, type, `aggregatetype`, uuid5 id from the new version, version bump, one history row), per-type payload contents, repeat writes no second row (six types), refusals emit nothing (cancel on pending, complete on pending, no-show on pending, check-in and no-show too early, swap with a slot it does not hold, missing appointment, lost hold), `accept_booking` writes no row (and the following confirm writes exactly `appointment.booked`), history notes, schedule-change flags and `mark_appointment_failed` write none, payload bytes contain no `coverage_ref` value, no schedule-change reason and no PHI-style field names for all six types (every non-timestamp value must parse as a UUID), the row's `traceparent` is the active span's (and null without a span), and two cross-connection tests on the committed engine: a rolled-back cancel leaves no outbox row and no history row, a committed cancel makes the outbox row, history row and new status visible together.
- Regression `tests/scheduling tests/workflows tests/libs/test_service_isolation.py`: **545 passed, 11 skipped, 0 failed** (10 min 8 s, OTEL_SDK_DISABLED unset, one uninterrupted run). Recorded-history replay tests pass. **No existing test needed a change.** Extra, beyond what was asked: `tests/visit` **77 passed** (it drives check-in and complete through Scheduling's real code).
- Tracer: `tests/e2e/test_phase3_booking_events.py -k tracer` **passed on the first run** against the rebuilt stack: the booking sent through the gateway with client `traceparent` trace id T produced one `appointment.booked` message (key = appointment id, `id` header = uuid5(appointment:type:1), `traceparent` header in trace T), no `coverage_ref` field and the seeded coverage reference bytes absent from the raw value.
- `tests/e2e/test_phase3_booking_events.py`: **3 passed** (47 s): booked, then exactly one `appointment.cancelled` (id = uuid5(...:2), same partition, booked offset before cancelled offset), and exactly one `appointment.rescheduled` (old and new slot ids and starts match the API's, same partition, after booked). Counts are of distinct `id` headers after a settle period, with a second read.
- `tests/e2e/test_phase2_flows.py`: **5 passed** on the rebuilt services (these flows also drive check-in and complete through Visit; the live `scheduling_db.outbox` afterwards holds rows of all six appointment types).
- Semgrep (`--config p/python --metrics=off`) on `events.py`, `appointment_repo.py`, `app.py`, `worker.py`: 151 rules, 4 files, **0 findings**. (`--config auto` is refused with metrics off, same as earlier Phase 3 plans.) The tests were not scanned.
- The full non-e2e suite (`pytest tests -m "not e2e"`) was **not** run: the orchestrator said it is not required for this plan. The known unrelated failure `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` was not touched.

## Live-stack operations record

- One command: `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling scheduling-worker`. No other service was recreated, stopped or killed; `gateway`, `connect`, `postgres`, `kafka` were not touched (so the gateway rate budgets were not reset), no volume was touched, no migration was needed (`scheduling_0006` ran in plan 03-05).
- After the recreate: `scheduling-1` and `scheduling-worker-1` healthy, `scripts/compose_healthcheck.sh` printed OK (every required service and every service with a healthcheck healthy). The logs since the rebuild show only the expected `BillingRejected` traceback from the Phase 2 forced-billing-failure e2e test.
- `tjo-*` before and after the whole plan: `tjo-api Up 25 hours (unhealthy)` and `tjo-postgres Up 25 hours (healthy)`; identical, no restart. (`tjo-api` was already unhealthy before this plan.)
- Debezium relayed the new rows with no intervention (`scheduling-outbox` was RUNNING throughout; the e2e fixture asserts it).
- Live outbox rows created by tests: the e2e runs and the Phase 2 flows commit real appointment events for throwaway clinics (synthetic ids), and the committed-cancel test in `test_outbox.py` committed one `appointment.cancelled` row; the outbox is insert-only by design (D-09, accepted veto item 5), so they remain and will be read by downstream consumers as ordinary events for unknown clinics. All other `test_outbox.py` tests run in a rolled-back outer transaction and persist nothing.
- No `.env*` or `infra/secrets/*` content was read or printed.

## Deviations from Plan

1. **[Plan wording] No second rebuild for Task 3.** The plan's Task 3 says to rebuild `scheduling` and `scheduling-worker` first because Task 2 changed cancel and reschedule. I wrote all six transitions before the single rebuild (the Task 1 commit was reduced to the confirm hunk only afterwards, so the commits stay atomic), so the running images already contained the final code and no code changed after the build. A second rebuild would have been a no-op; the Task 3 e2e run is against the final code.
2. **[Plan wording] No Task 3 commit.** The three e2e scenarios (tracer, cancel, reschedule) were written together and committed in the Task 1 commit, since the test file is one file; Task 3 produced no further change to commit. The cancel and reschedule scenarios in that commit were only run after Task 2 (all 3 pass).
3. **[Test design] `test_outbox.py` mostly uses the rolled-back `scheduling_db_session`** rather than committing every seed, so the live Debezium relay and Kafka are not polluted by the 33 tests; only the committed-engine atomicity tests commit, and one of them leaves one real `appointment.cancelled` row (see above).
4. **[Plan wording] Warm-up failure in the worker** is logged as `worker.event_registry_unavailable` (error class name only) and exits 1, as the plan specified; the API lifespan lets `warm_up`'s exception propagate (the app fails to start; Compose restarts it).
5. **Semgrep ruleset** `p/python` with `--metrics=off` instead of `--config auto` (see above).

Otherwise executed as written. No auth gates, no architectural changes, no package installs.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-3-20 (PHI): builders copy ids and timestamps only, tests assert coverage_ref, schedule-change reason and PHI-style field names are absent for all six types. T-3-21 (duplicates): uuid5 from the post-transition version, only applied branches emit, tests for repeats and rollbacks. T-3-22 (booking-path latency): nothing added to `accept_booking`; the only synchronous addition is the outbox insert at the confirm step in the Temporal activity, with serializers warmed at start-up. The k6 smoke on a quiet stack remains plan 03-11's. T-3-23 (client traceparent) accepted as planned.

## Notes for later plans

- Plans 03-08, 03-09 and 03-10 can rely on the live registry having all six appointment subjects and on `scheduling_db.outbox` carrying every appointment transition from now on.
- Appointment version history for ids: a booking starts at version 0, so `appointment.booked` has id `uuid5(<appt>:appointment.booked:1)`, a later cancel `...:appointment.cancelled:2`; consumers must dedupe on the `id` header (Debezium delivery is at-least-once, see 03-05).
- A reschedule through `record_history_note(bump_version=True)` (the failed-reschedule note) bumps the version without an event, so event ids may skip version numbers; the ids stay unique.
- `libs.events.outbox.outbox_insert_seconds` is observed in the process that writes the row (the Scheduling worker for confirm, cancel and reschedule; the API process for check-in and complete requests). Whether and how the worker's registry is scraped is plan 03-15's; this plan did not verify the metric on Prometheus.

## Self-Check: PASSED

- FOUND: services/scheduling/events.py, tests/scheduling/test_outbox.py, tests/e2e/test_phase3_booking_events.py (committed).
- FOUND commits 1067b35 and 84444c4 on gsd/phase-02-scheduling-service-workflow (`git rev-list --count 0a2e3a4..HEAD` = 2 before the docs commit).
- Acceptance greps: `emit_appointment_event` count in appointment_repo.py = 7 (import and six calls, at least 6 required); `warm_up` present in app.py (2) and worker.py (2); `git diff --stat 0a2e3a4..HEAD -- services/scheduling/workflows` is empty.
