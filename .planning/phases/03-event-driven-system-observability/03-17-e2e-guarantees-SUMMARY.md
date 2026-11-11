---
phase: 03-event-driven-system-observability
plan: 17
subsystem: observability
tags: [jaeger, trace-propagation, redelivery, idempotency, debezium, heartbeat, wal, structlog, consumer]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-02/03-04 traceparent through outbox, Kafka headers and Celery headers; 03-05 CDC connectors; 03-09/03-10/03-14 the three idempotent consumers; 03-15/03-16 metrics and dashboards"
provides:
  - "One booking is ONE connected trace in Jaeger across gateway, Scheduling, Temporal worker, Kafka consumers and Celery, with the consumer spans CHILD_OF a span of the trace"
  - "Forced redelivery proof: offset rewind of all three consumer groups and a Connect SIGKILL leave every effect at exactly one"
  - "event.consumed log line now carries trace_id/span_id (it was emitted after the processing span closed)"
  - "Debezium heartbeat action query on all four connectors: quiet databases keep confirming WAL, so dbz_provider no longer pins WAL"
  - "JSON log lines with trace_id for booking, availability, billing, Celery and consumers, PHI-free, proven in process and on the live stack"
affects: [03-18, 03-19, 03-20]
status: complete
duration: about 3h wall time (about 25 min of it background test runs)
completed: 2026-11-11
actuals:
  tokens: 18100
  tasks: 3
  commits: 4
plan_head_before: e187ecc37dc1448e1ed9a02a94169e9dd7d03018
plan_head_after: c2ead524c733b668a0fc592452f511c3c916bff0
tech-stack:
  added: []
  patterns:
    - "Heartbeat action query = pg_logical_emit_message(true, prefix, ...) with the prefix in message.prefix.exclude.list: writes WAL in the monitored database without a table, a migration or an event"
    - "Redelivery is forced (stop service, kafka-consumer-groups --reset-offsets --to-datetime, start) and evidenced by the service's own event.consumed log lines, then every effect is compared before and after"
    - "Live trace and log tests send a known W3C traceparent and then look the trace id up in Jaeger and in docker compose logs"
key-files:
  created:
    - tests/events/test_trace_chain.py
    - tests/e2e/test_phase3_trace.py
    - tests/e2e/test_phase3_redelivery.py
    - tests/e2e/test_phase3_logs.py
    - tests/e2e/test_phase3_heartbeat_wal.py
  modified:
    - libs/events/consumer.py
    - tests/libs/test_observability.py
    - tests/infra/test_cdc_config.py
    - infra/connect/connectors/patient-outbox.json
    - infra/connect/connectors/provider-outbox.json
    - infra/connect/connectors/scheduling-outbox.json
    - infra/connect/connectors/visit-outbox.json
key-decisions:
  - "heartbeat.interval.ms alone does not move a quiet slot (it was already set to 10000 on all four connectors since 03-05 and dbz_provider was still stuck); the fix is heartbeat.action.query with a logical-decoding message, excluded from routing, no table and no schema change"
  - "event.consumed is logged inside the consumer processing span so consumer logs are findable by trace id; the three consumer images were rebuilt with --no-deps"
  - "Heartbeat topics keep Debezium's default names (__debezium-heartbeat.smarthealth-<service>); they already existed, nothing subscribes to them (consumers subscribe to explicit topic lists), so renaming would only orphan four topics"
requirements-completed: []
---

# Phase 3 Plan 17: End-to-end guarantees Summary

**One booking is one connected Jaeger trace across HTTP, Temporal, Kafka and Celery; offset rewinds of all three consumer groups and a Connect SIGKILL leave every effect at exactly one (one on-the-wire duplicate observed and absorbed); the consumer log line was moved inside its span so logs correlate by trace id; and a Debezium heartbeat action query stops quiet databases pinning WAL (dbz_provider 83-87 MB retained, stuck, to 2-5 MB, moving).**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (plan addresses OBS-02, OBS-03, EVENT-02, EVENT-03).

## Commits

| Part | Commit | What |
|------|--------|------|
| Heartbeat (orchestrator's extra task, done first) | 17042a3 | `heartbeat.action.query` + `message.prefix.exclude.list` on all four connectors, static config test, e2e slot test |
| 1 (tracer) | 78bdd3f | in-memory chain test, Jaeger e2e |
| 2 | b78f632 | forced-redelivery e2e (offset rewind, Connect hard kill) |
| 3 | c2ead52 | `event.consumed` inside the span (fix), unit log tests, live log e2e |

## Heartbeat and WAL retention (extra task)

The plan text said to add `heartbeat.interval.ms`. It was already `10000` on all four connector files (added in 03-05) and the heartbeat topics `__debezium-heartbeat.smarthealth-{patient,provider,scheduling,visit}` already existed, yet `dbz_provider.confirmed_flush_lsn` was frozen. A bare heartbeat does not advance a slot whose database has no change to confirm. Fix: each connector now also runs

`heartbeat.action.query = SELECT pg_logical_emit_message(true, 'smarthealth-heartbeat', clock_timestamp()::text)`

every 10 s, which writes a tiny transactional WAL message INTO the monitored database; `message.prefix.exclude.list = smarthealth-heartbeat` keeps that message from ever being routed as an event. No table, no migration, no permission change (`debezium` can execute the function), nothing for any consumer to read. The four connectors were re-registered by name with `register-connectors.sh` (idempotent PUT); all four report connector and task RUNNING.

Retained WAL (restart_lsn behind pg_current_wal_lsn), from the read-only superuser query inside the postgres container; `max_slot_wal_keep_size` is 2GB:

| Slot | Before | After (about 3 min) | After (end of the plan, stack idle) |
|---|---|---|---|
| dbz_provider | 83 MB retained, 83-87 MB unconfirmed, confirmed_flush_lsn frozen at 0/7FA53728 | 2.4-5 MB retained, 0.2-3 MB unconfirmed | 1.4 MB / 0.3 MB |
| dbz_visit | 62-66 MB retained | 12-15 MB | 1.4 MB |
| dbz_patient | 10-14 MB | 12-15 MB | 1.4 MB |
| dbz_scheduling | 8 MB | 5-7 MB | 1.4 MB |

The orchestrator's figure was about 517 MB; by the time I measured, the provider slot held 83 MB (the cluster had since recycled WAL), but `confirmed_flush_lsn` was not moving, which is the actual defect. It started moving within one Connect offset-flush interval (60 s; the confirm happens at offset commit) of the re-registration. Tests: `tests/infra/test_cdc_config.py` (23 more lines, parametrized over every connector file: interval at most 30 s, action query is the logical message, no table write words, excluded prefix equals the emitted prefix) and `tests/e2e/test_phase3_heartbeat_wal.py` (5 passed: the deployed config equals the file; for each of the four slots with no outbox write in the window, `confirmed_flush_lsn` strictly advances within 150 s, unconfirmed is at most 48 MB, retained at most 96 MB, wal_status reserved or extended).

Heartbeat topics: no new topic is created. The existing default-named ones stay (`__debezium-heartbeat.smarthealth-<service>`); no consumer subscribes to them (the consumers subscribe to explicit topic lists), and they were not renamed because that would orphan four topics for no benefit.

## Task 1: one booking is one trace

- `tests/events/test_trace_chain.py` (1 test, 22 s): a producer span writes an `appointment.booked` row to a TEMP outbox on scheduling_db (nothing reaches the real table), the row's payload and columns are produced to a unique live topic with Debezium-shaped headers (`eventType`, `traceparent`, `schemaVersion`), the real `EventConsumer` handles it, the handler publishes a real `notifications.send` through Celery on a throwaway RabbitMQ vhost, and an in-process threads worker runs it. Asserted: one trace id for producer, consumer, `apply_async/notifications.send` and `run/notifications.send`; consumer parent = producer span with no links; publish parent = consumer span; task parent = publish span. The plan said Celery eager; I used the real broker and worker instead because an eager call never publishes, so it cannot prove the publish-to-task parent hop.
- `tests/e2e/test_phase3_trace.py` (1 test): booking through the gateway with `traceparent: 00-<T>-<span>-01`, then Jaeger `/api/traces/<T>`. Asserts all six required services are in T, the `smarthealth.appointment.events process` spans of BOTH `smarthealth-notification-consumer` and `smarthealth-analytics-consumer` have a `CHILD_OF` reference to a span id that is in T (and no `FOLLOWS_FROM` link), and the Celery `run/notifications.send` span is a child of `apply_async/notifications.send`, which is a child of the notification-consumer's process span.
- No fix was needed in `consumer.py`, `notification/consumer.py` or `tasks.py` for tracing: the chain worked the first time.

**For the human Jaeger check in plan 03-20:** trace id `e839d1a9ec7d48e435d09abb68881b00` (appointment `7d307192-f57e-4661-9394-1e82ad2a9297`), 57 spans, appears in Jaeger within 7 s of the booking. Span count per service: analytics-consumer 1, billing 4, gateway 5, identity 16, notification-celery 1, notification-consumer 3, notification-temporal-worker 1, organization 3, patient 3, provider 3, scheduling 5, scheduling-worker 12. A fresh run of the test prints a new `JAEGER_TRACE_EVIDENCE` line with another trace id.

**Reminder trace, labelled honestly:** the reminder is a long-lived Temporal timer. The notification-consumer starts it inside the booking's trace (so `notification-temporal-worker` already contributes 1 span to T), but the timer fires days before the appointment; the later reminder activity is expected to continue that same trace through Temporal's tracing interceptor (not exercised here: it would take days), so the trace can be long-lived with a wide gap in Jaeger, and the docs should say so. The test does not require the future firing.

## Task 2: forced redelivery

`tests/e2e/test_phase3_redelivery.py` (2 passed, 195 s). Every assertion is scoped to the test's own fresh clinic and ids.

1. **Offset rewind.** A fresh clinic with three patients and three confirmed bookings (one stays booked, one cancelled, one with a checked-in, started and completed visit); effects awaited: analytics summary (3 booked, 1 completed visit, 3 patients), one `sent` delivery with one attempt and one provider call for each booked key and for the cancelled one, one execution each of `reminder-{kept}`, `reminder-{cancelled}`, `reminder-{visited}`, `visit-completed-{visit}` and `followup-{visit}`. For each group the service is stopped, `kafka-consumer-groups --reset-offsets --to-datetime <test start - 120 s, UTC ms> --execute` is run on its topics, the service is started and its health awaited, and the group is awaited at lag 0 with an active member. The reprocessing evidence is the service's own `event.consumed` log lines since the restart naming every one of the test's events on that group's topics. Afterwards the whole snapshot (summary, deliveries with attempts and provider calls, Temporal execution counts) is `==` the snapshot before.

   Reprocessed per group (from the run's `REDELIVERY_EVIDENCE` line): `visit-downstream` 1 of the test's own events (1 consumed line in total); `analytics-projection` 10 of the test's own events (12 consumed lines in total; 2 are from events older than the test inside the 120 s margin); `notification-trigger` 7 own events (9 lines in total). All of them were no-ops: effects unchanged.
2. **Connect hard kill.** After booking, the moment the `appointment.booked` event is on Kafka (read with a tailing consumer, so the reaction is under a second), `docker compose kill connect`, start it, wait healthy and all four tasks RUNNING (24.7 s). **On the wire the event appeared 2 times with 1 distinct event id** (so the at-least-once duplicate really happened, and nothing was lost). The booking still has one delivery (attempts 1, one provider call), one reminder execution, and `appointments_booked` rose by exactly one (baseline + 1, stable after a 15 s settle). This is the evidence for the docs: no loss on the wire, effect-level exactly-once.

Restored afterwards: all consumers and Connect healthy, four connector tasks RUNNING, four replication slots active; `tjo-api` (Up, unhealthy) and `tjo-postgres` (Up, healthy) were identical before and after (the unhealthy state predates this plan).

## Task 3: structured logs

- **Fix (Rule 1/2, `libs/events/consumer.py`, 5 lines):** `event.consumed` was written after the consumer's processing span had closed, so consumer logs had no `trace_id`/`span_id` and could not be found by trace id (the live test failed for exactly this on `notification-consumer`; the unit test failed the same way). It is now written inside the span, right after the commit. The three consumer images (`visit-consumer`, `analytics-consumer`, `notification-consumer`) were rebuilt and recreated with `up -d --build --no-deps`.
- No log line had to be added for availability or billing: `availability.update.success` and `billing.precheck` already existed inside their request spans, so the plan's expected candidates needed no new logging.
- `tests/libs/test_observability.py`: 7 passed (2 existing, 5 new): booking request plus saga steps carry the request's trace id (a known `traceparent` is sent, so the id is checked, not just present); billing pre-check; availability template update; Celery send (real task body via `apply`, trace id equals the task span's); consumer processing (a fake message through the real consumer base, line carries the producer's trace id). Every test also asserts that none of six seeded PHI values (names, dates of birth, phone, address of two patients, one created with phone and address) appears anywhere in the captured output.
- `tests/e2e/test_phase3_logs.py` (1 passed): the booking's trace id is found as a JSON line in `scheduling` (2 lines), `scheduling-worker` (13), `billing` (1), `notification-consumer` (1), `notification-celery` (1) and `analytics-consumer` (1); no seeded PHI value in any of those logs. Celery prefixes its worker lines with `[time: WARNING/MainProcess] ` before the JSON object; the test parses from the first `{`.

## Test results

| Command | Result |
|---|---|
| `tests/events/test_trace_chain.py` | 1 passed |
| `tests/libs/test_observability.py` | 7 passed |
| `tests/events` (whole folder) | 117 passed (with observability) |
| e2e: trace + logs + heartbeat + analytics + notifications + booking_events + visit_downstream (final pass after the rebuild) | 28 passed |
| `tests/e2e/test_phase3_redelivery.py` | 2 passed |
| `tests/e2e/test_phase3_heartbeat_wal.py` | 5 passed |
| `tests/infra` + `tests/libs/test_service_isolation.py` + `tests/libs/test_observability.py` + `tests/events` + `tests/notification` + `tests/analytics` + `tests/visit` (not e2e) | 395 passed, 3 failed; the 3 failures are `tests/visit/test_backfill.py` (a subprocess of the real backfill script exited -5 / SIGTRAP during the 10-minute run); the same file re-run alone: 6 passed. Treated as a flaky native crash of the script subprocess, unrelated to these changes (the script does not touch the consumer base) |

Not run: a full `tests -m "not e2e"` and the whole e2e suite (the orchestrator said a full regression was not required); `test_admin_registers_provider_through_gateway` and the email-migration test were not run (known unrelated).

## Deviations from Plan

**1. [Rule 1 - Bug] Consumer `event.consumed` log had no trace context.** Found by the Task 3 unit and live tests. Fixed in `libs/events/consumer.py`; three consumer images rebuilt. Commit c2ead52.

**2. [Rule 3 - Blocking, orchestrator extra task] The heartbeat interval was already configured.** The task asked to add `heartbeat.interval.ms`; it was present since 03-05 and the slot was still stuck, so the real fix is the heartbeat action query (see above). Commit 17042a3. Heartbeat topics were not renamed (see key decisions).

**3. [Test design] Celery leg uses a real worker, not eager.** An eager Celery call never publishes, so it cannot show the publish-to-task parent hop; the chain test reuses the notification tests' throwaway-vhost broker and in-process worker fixtures.

**4. [Scope] Four commits instead of three** (the heartbeat fix has its own commit, as the orchestrator allowed). The plan's `files_modified` list did not include `tests/infra/test_cdc_config.py`, `tests/e2e/test_phase3_heartbeat_wal.py` or the connector JSON files; they belong to the extra task. `services/notification/consumer.py`, `services/notification/tasks.py`, `libs/observability/logging.py`, `services/provider/api/availability.py` and `services/billing/api/internal.py` needed no change.

**5. Mid-run launcher artifacts.** Two shell wait-loops matched their own command line and had to be killed (exit 144); no effect on any test or on the stack.

## Known Stubs

None.

## Threat Flags

None. No new endpoint, auth path or schema; the heartbeat query is a read-only function call against each outbox database (no table write).

## Self-Check: PASSED

Created files exist (`tests/events/test_trace_chain.py`, `tests/e2e/test_phase3_trace.py`, `tests/e2e/test_phase3_redelivery.py`, `tests/e2e/test_phase3_logs.py`, `tests/e2e/test_phase3_heartbeat_wal.py`); commits 17042a3, 78bdd3f, b78f632 and c2ead52 exist; `commits: 4` measured with `git rev-list --count` from the ledger base; the working tree has no uncommitted tracked changes; `scripts/compose_healthcheck.sh` OK, four slots active, four connectors RUNNING.
