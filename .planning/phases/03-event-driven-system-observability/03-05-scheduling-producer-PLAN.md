---
phase: 03-event-driven-system-observability
plan: 05
type: execute
wave: 3
depends_on: ["03-01", "03-02"]
files_modified:
  - services/scheduling/events.py
  - services/scheduling/repository/appointment_repo.py
  - services/scheduling/app.py
  - services/scheduling/worker.py
  - tests/scheduling/test_outbox.py
  - tests/e2e/test_phase3_booking_events.py
autonomous: true
requirements: [EVENT-01, EVENT-02, NOTIF-01, ANLY-02, ANLY-04]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Every appointment transition that is a domain event (confirm, cancel, reschedule, check-in, complete, no-show) writes its outbox row in the same transaction as the state change and history row; a rollback leaves neither (EVENT-02, D-06)"
    - "The booking accept path (the 202) writes no outbox row and gains no new work; appointment.booked is emitted at the saga's confirm step (D-04)"
    - "A repeated or retried transition (activity retry after commit, duplicate call) writes no second row: the event id is uuid5 of appointment id, type and the post-transition version (D-06)"
    - "Killing the committing process right after the commit does not lose the event, and an event committed while Connect is stopped is delivered after it starts (roadmap success criterion 1, delivery half)"
    - "One booking request's trace id is carried into the appointment.booked event's traceparent header (OBS-02)"
    - "Existing Temporal workflows keep their shapes and activity names; no workflow.patched is added; recorded-history replay tests still pass"
    - "The k6 smoke profile still passes after this change (D-04)"
  artifacts:
    - path: services/scheduling/events.py
      provides: "SCHEDULING_EVENT_TYPES, payload builders and emit_appointment_event(db, event_type, row)"
      contains: "enqueue_event"
    - path: tests/scheduling/test_outbox.py
      provides: "atomicity, rollback, no-op repeat, accept-path-untouched and PHI-free payload tests for all six transitions"
    - path: tests/e2e/test_phase3_booking_events.py
      provides: "gateway booking -> appointment.booked on Kafka with the request's trace id; kill-after-commit and Connect-down delivery"
  key_links:
    - from: services/scheduling/repository/appointment_repo.py
      to: services/scheduling/events.py
      via: "each transition calls emit_appointment_event after append_status_history, inside the caller's transaction"
      pattern: "emit_appointment_event"
---

<objective>
Make Scheduling a producer: each appointment transition that is a domain event commits its outbox row with the state change, so the CDC relay from plan 03-02 publishes it. The booking accept path stays untouched.

Purpose: EVENT-01/EVENT-02 for the appointment aggregate; these events drive notifications (NOTIF-01, plan 03-11), reminders (plan 03-08/11) and the analytics projections for appointments over time and cancellation rate (ANLY-02/04, plan 03-09).

Output: `services/scheduling/events.py`, outbox emission in six repository transitions, registry warm-up in the API lifespan and the worker, integration tests, and an e2e test that books through the gateway and reads the event off Kafka.

Decisions honoured: D-04 (only the outbox insert is added, and not on the accept path), D-06 revised (aggregate `appointment`, key = appointment id, uuid5 event id from the version, traceparent of the producing span), D-07 (PHI-free payloads; `coverage_ref` never leaves the appointments table), D-10 revised (events carry `slot_start` and the check-in timestamp), D-09 revised and research F5 (the Phase 2 notification intent activities stay registered and unchanged; no workflow changes, no `workflow.patched`). Hook points are the transition functions, NOT `append_status_history` or `record_history_note` (history notes are not domain events).

Live stack rules: rebuild only `scheduling` and `scheduling-worker` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling scheduling-worker`; the migration (`scheduling_0006`) already ran in plan 03-02. Never touch `tjo-*` containers (record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before and after). Other wave-3 plans rebuild visit, patient and provider at the same time: if an e2e test fails because a service was restarting, re-run after `scripts/compose_healthcheck.sh` passes. From this plan on, host-side scheduling and workflow tests need the live Schema Registry (`localhost:8081`) with the subjects registered by `schema-init`.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-02-SUMMARY.md
@services/scheduling/repository/appointment_repo.py
@services/scheduling/repository/models.py
@services/scheduling/app.py
@services/scheduling/worker.py
@libs/events/outbox.py
@tests/e2e/phase2_stack.py
@tests/e2e/phase3_stack.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `SCHEDULING_EVENT_TYPES` | constant | the six appointment event types, used by `warm_up` |
| `emit_appointment_event(db, event_type, row, **extra)` | function | builds the payload from the RETURNING row and calls `enqueue_event(..., discriminator=str(row.version))` |
| payload builders | functions | `booked_payload`, `cancelled_payload`, `rescheduled_payload`, `checked_in_payload`, `completed_payload`, `no_show_payload` in `services/scheduling/events.py` |
| transition hooks | code | `confirm_booking`, `cancel_appointment`, `swap_slot`, `check_in_appointment`, `complete_appointment`, `mark_no_show` in `appointment_repo.py` |
| warm-up | code | `await warm_up(SCHEDULING_EVENT_TYPES)` in the API lifespan and before the worker starts polling |
| tests | tests | `tests/scheduling/test_outbox.py`, `tests/e2e/test_phase3_booking_events.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a booking through the gateway produces appointment.booked on Kafka, keyed by the appointment, carrying the request's trace id</name>
  <files>services/scheduling/events.py, services/scheduling/repository/appointment_repo.py, services/scheduling/app.py, services/scheduling/worker.py, tests/e2e/test_phase3_booking_events.py</files>
  <behavior>
    - A booking request sent to the gateway with a client `traceparent` header (known trace id T) reaches `confirmed`
    - Within 60 s, `smarthealth.appointment.events` carries one message with key = appointment id, header `eventType=appointment.booked`, header `id` = uuid5 of `{appointment_id}:appointment.booked:{version}`, and a `traceparent` header whose trace id is T
    - The decoded value has the appointment, clinic, patient, provider and slot ids, `slot_start`, `slot_end`, `booked_at`, and no `coverage_ref`
  </behavior>
  <read_first>
    - services/scheduling/repository/appointment_repo.py (module contract "flushes, never commits"; `confirm_booking` lines ~174-244; `accept_booking` ~89-171)
    - services/scheduling/repository/models.py (Appointment columns)
    - services/scheduling/activities/booking.py (the confirm activity that calls `confirm_booking`; do not change its activity name)
    - services/scheduling/app.py (lifespan) and services/scheduling/worker.py (start-up order before `Worker.run`)
    - libs/events/outbox.py, libs/events/registry.py (`enqueue_event`, `warm_up`)
    - tests/e2e/phase2_stack.py (`seed_phase2_world`, `book`, `appointment_in`, `poll`) and tests/e2e/phase3_stack.py (`consume_matching`)
  </read_first>
  <action>
    events.py: `SCHEDULING_EVENT_TYPES` and the six payload builders (each returns a dict of the schema's non-envelope fields from a row mapping: uuids as strings, timestamps as aware datetimes), plus `async emit_appointment_event(db, event_type, row, **extra)` that calls `enqueue_event(db, event_type=..., aggregate_id=str(row.id), payload=..., discriminator=str(row.version))`.

    confirm_booking: change the appointment UPDATE's `RETURNING id` to return `id, clinic_id, patient_id, provider_id, slot_id, slot_start, slot_end, version, updated_at`, keep the existing `None` guard, and after `append_status_history` call `emit_appointment_event(db, "appointment.booked", row)` (booked_at = `updated_at`). The repeat path (already committed) returns before any emission, so a retried confirm activity writes nothing new. Do not touch `accept_booking`.

    Warm-up: in `services/scheduling/app.py` lifespan, `await warm_up(SCHEDULING_EVENT_TYPES)` after the audit indexes; in `services/scheduling/worker.py`, the same call before the worker starts polling (a failure after the retries exits 1 like other start-up failures).

    e2e test (marker e2e): seed a world with the Phase 2 helpers, POST the booking through the gateway with header `traceparent: 00-<T>-<random 16 hex>-01`, wait for `confirmed`, then `consume_matching("smarthealth.appointment.events", key == appointment id and eventType == appointment.booked)` and assert the behaviors.

    Rebuild: `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling scheduling-worker`, wait healthy, run the test.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, no appointment.booked message arrives within 60 s, the traceparent trace id is not T, or the value contains coverage_ref.</fails_when>
  </verify>
  <acceptance_criteria>
    - the tracer test passes on the rebuilt stack
    - `grep -c "emit_appointment_event" services/scheduling/repository/appointment_repo.py` is at least 1
    - `grep -c "warm_up" services/scheduling/app.py services/scheduling/worker.py` reports at least 1 in each file
    - tjo-* status lines before and after show no restart
  </acceptance_criteria>
  <done>A real booking's event reaches Kafka through the outbox and CDC, in the booking's trace.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The other five transitions, atomicity, the untouched accept path, and PHI-free payloads</name>
  <files>services/scheduling/repository/appointment_repo.py, services/scheduling/events.py, tests/scheduling/test_outbox.py</files>
  <behavior>
    - `cancel_appointment`, `swap_slot`, `check_in_appointment`, `complete_appointment` and `mark_no_show` each add exactly one outbox row of their type (`appointment.cancelled`, `.rescheduled`, `.checked_in`, `.completed`, `.no_show`) with discriminator = new version; the rescheduled payload has old and new slot ids and starts; checked_in has `checked_in_at` and `slot_start`
    - A transition rolled back by the caller leaves no outbox row and no history row
    - A repeated call that changes nothing (already cancelled, already checked in, and so on) adds no row
    - `accept_booking` followed by commit adds no outbox row for that appointment; `record_history_note` and `flag_schedule_change` add none; `mark_appointment_failed` adds none (not a D-07 event)
    - For every event type, the payload bytes contain none of the seeded patient's PHI values and not the appointment's `coverage_ref` value
    - The row's `traceparent` is the active span's when one is active
  </behavior>
  <read_first>
    - services/scheduling/repository/appointment_repo.py (transitions at the `append_status_history(` call sites: cancel ~468, swap ~583, check-in ~720, complete ~767, no-show ~827)
    - tests/scheduling/conftest.py and tests/scheduling/seed.py (live-DB session fixtures and seeding)
    - tests/scheduling/test_appointment_states.py, test_cancel.py, test_reschedule.py (how transitions are exercised directly)
  </read_first>
  <action>
    For each of the five transitions: extend the UPDATE's RETURNING to the columns its payload needs (plus `version`), and right after its `append_status_history` call `emit_appointment_event` with the matching type; for `swap_slot` capture the old slot id and start that the function already reads before the update. Only the applied branch emits; every early-return branch (repeat, refused, not found) emits nothing. Keep each function's return values and signatures unchanged.

    Tests (tests/scheduling/test_outbox.py, marker `integration`, live scheduling_db with the real outbox table) for every behavior, querying `outbox` by `aggregateid`. Use a unique clinic per test (the rows will also be relayed to Kafka; that is expected and harmless).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/scheduling/test_outbox.py -x -q && .venv/bin/python -m pytest tests/scheduling tests/workflows -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a rolled-back transition leaves a row, a repeat writes a second row, accept_booking writes a row, or PHI or coverage_ref bytes appear in a payload.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/scheduling/test_outbox.py -q` exits 0 with at least 12 passed
    - `.venv/bin/python -m pytest tests/scheduling tests/workflows -q` exits 0 (replay tests included: workflows unchanged)
    - `grep -c "emit_appointment_event" services/scheduling/repository/appointment_repo.py` is at least 6
    - `git diff --stat HEAD -- services/scheduling/workflows` shows no change in this plan
  </acceptance_criteria>
  <done>All six appointment events commit atomically with their transitions, and nothing else writes to the outbox.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Kill-after-commit and Connect-down delivery for appointment events, plus the k6 smoke check</name>
  <files>tests/e2e/test_phase3_booking_events.py</files>
  <behavior>
    - Cancel through the gateway; as soon as the appointment reads `cancelled`, `docker compose kill scheduling-worker` (the process that committed); the `appointment.cancelled` event id arrives on Kafka; the worker is started again and becomes healthy
    - With `connect` stopped, a booking confirms; no booked event arrives while Connect is down; after Connect starts and its tasks are RUNNING, the booked event arrives once by event id (duplicates are counted, not asserted zero)
    - Reschedule through the gateway yields one `appointment.rescheduled` event with the new slot id
  </behavior>
  <read_first>
    - tests/e2e/test_phase3_booking_events.py (Task 1), tests/e2e/phase3_stack.py (kill/stop/start helpers), tests/e2e/test_phase2_flows.py (cancel and reschedule calls through the gateway)
    - loadtest/README.md (smoke profile, raised gateway budget) and tests/e2e/test_loadtest_smoke.py (seeds its own admin and world)
  </read_first>
  <action>
    Add the three scenarios; every test that kills or stops a container restarts it in `finally` and waits for health (and, for Connect, for all four connector tasks RUNNING).

    Then run the k6 smoke profile through the existing wrapper test with the gateway budget raised for the run only: `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`, then `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q`, then recreate the gateway without the overrides (`docker compose -f infra/docker-compose.yml up -d --no-deps gateway`). The wrapper seeds its own world with a test-created admin; do not create or hardcode any other admin credential. If the smoke fails while other wave-3 executors are running tests, re-run when `docker stats --no-stream` shows the host quiet; record the accept p95 and saga p95 from the k6 summary in the SUMMARY (baseline 2026-11-10: accept p95 21.8 ms, saga p95 0.52 s).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py tests/e2e/test_phase2_flows.py -m e2e -q</automated>
    <fails_when>Either pytest run exits non-zero, an event id is missing after the kill or after Connect restarts, or k6 reports a crossed threshold.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -q` exits 0 with at least 4 passed
    - `tests/e2e/test_loadtest_smoke.py` passes and the SUMMARY records accept p95 and saga p95
    - after the run the gateway runs with the default rate budget again (`docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` prints the default from the Compose file or `infra/.env`, not 100000)
  </acceptance_criteria>
  <done>Appointment events survive a kill of the committing process and a Connect outage, and the booking path still meets the smoke thresholds.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Scheduling transaction -> outbox -> Kafka | appointment state becomes a replayable event read by three consumer groups |
| client traceparent header -> event headers | an external trace id is carried into internal events |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-20 | Information Disclosure | appointment event payloads | high | mitigate | Builders copy ids and timestamps only; `coverage_ref` is never read into a payload; test asserts seeded PHI values and coverage_ref bytes are absent for all six types |
| T-3-21 | Tampering | duplicate or out-of-band emission | medium | mitigate | uuid5 event id from the post-transition version plus `ON CONFLICT (id) DO NOTHING`; only applied branches emit; tests cover repeats and rollbacks |
| T-3-22 | Denial of Service | booking path latency / registry outage | medium | mitigate | Nothing added to `accept_booking`; serializers warmed at start-up; `outbox_insert_seconds` measured; k6 smoke must pass |
| T-3-23 | Spoofing | client-supplied traceparent | low | accept | Trace context is observability metadata only; it grants nothing and carries no data; standard W3C behaviour of the existing FastAPI instrumentation |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/scheduling tests/workflows -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/scheduling`; record status in the SUMMARY
</verification>

<success_criteria>
- Six appointment events committed atomically, PHI-free, idempotent at the table
- Accept path untouched; smoke thresholds still met; workflows unchanged
- Delivery proven across a kill of the committing process and a Connect outage
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-05-SUMMARY.md` when done
</output>
