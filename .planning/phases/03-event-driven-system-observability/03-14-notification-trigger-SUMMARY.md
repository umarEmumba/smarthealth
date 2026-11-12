---
phase: 03-event-driven-system-observability
plan: 14
subsystem: notification
tags: [kafka-consumer, celery, rabbitmq, temporal, compose, broker-provisioning, dead-letter, d-13]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-02 send task and enqueue_send; 03-04 EventConsumer; 03-06 reminder helpers and Temporal worker; 03-07/03-09 producers; 03-13 pin_start_offsets pattern"
provides:
  - "notification-trigger consumer (group notification-trigger, DLQ smarthealth.dlq.notification-trigger) with one handler per event type"
  - "broker provisioning: vhost smarthealth, tag-less user smarthealth_notification, permissions on that vhost only (rabbitmq-init one-shot)"
  - "notification image and four Compose services (rabbitmq-init, notification-celery, notification-consumer, notification-temporal-worker), secret notification_broker_password"
  - "libs/events/start_offsets.py: pin_start_offsets shared by analytics and notification"
affects: [03-15, 03-16, 03-17, 03-18, 03-20]
status: complete
duration: about 1h50 wall time
completed: 2026-11-11
actuals:
  tokens: 21000
  tasks: 3
  commits: 3
plan_head_before: b4e8f5d1839c61ddd55c66bf1ac4d1bf8cf2d3e4
plan_head_after: ce8a1ceb390c39a19f78c30f6f47b93497cab19e
tech-stack:
  added: []
  patterns:
    - "Handlers take injected ports (publisher, Temporal source, Mongo, helper module) so routing is unit-tested without broker or Temporal"
    - "Event-driven idempotency without a processed-event table: dedupe key at delivery log and provider, deterministic workflow ids, upserted closed-appointment guard"
    - "The consumer declares the broker topology itself before publishing (a message to a queue that does not exist yet is dropped as unroutable)"
key-files:
  created:
    - services/notification/triggers.py
    - services/notification/consumer.py
    - services/notification/broker_setup.py
    - services/notification/Dockerfile
    - libs/events/start_offsets.py
    - tests/notification/test_triggers.py
    - tests/notification/test_broker_setup.py
    - tests/notification/test_compose_wiring.py
    - tests/e2e/test_phase3_notifications.py
  modified:
    - services/notification/config.py
    - services/notification/deliveries.py
    - services/analytics/consumer.py
    - infra/docker-compose.yml
    - tests/libs/test_service_isolation.py
key-decisions:
  - "D-13 start policy: NOTIFICATION_CONSUMER_START=latest in Compose (code default earliest), go-live offset pinned at container start; the plan said earliest"
  - "pin_start_offsets moved to libs/events so notification does not import the analytics service (the isolation test forbids it); analytics re-imports it, behaviour unchanged"
  - "Temporal client connects lazily on the consumer loop (visit-consumer pattern), not eagerly as the plan wrote: a client must live on the loop that uses it"
  - "The consumer declares the broker topology at start (declare_topology) in addition to the Celery worker"
  - "Stale guard covers all three booking-type events (booked, cancelled, rescheduled), not only booked"
requirements-completed: []
---

# Phase 3 Plan 14: Notification Trigger Summary

**A booking, cancellation or reschedule through the gateway now produces exactly one `sent` delivery and one fake-provider record, published by the `notification-consumer` container and delivered by the `notification-celery` container over a dedicated, tag-less broker user on vhost `smarthealth`; reminders and follow-ups are started, moved and stopped by the same events; both dead-letter paths and redelivery were proven on the live stack.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (the plan addresses NOTIF-01..04, EVENT-03, EVENT-04, EVENT-05).

## The open RabbitMQ vhost issue (answered)

Read-only first: before this plan `rabbitmqctl list_vhosts` showed only `/` and `list_users` only `smarthealth [administrator]`. Nothing was lost or broken: **no `smarthealth` vhost or notification user had ever been provisioned**, because this plan owns broker provisioning. `rabbitmq-init` created it (`broker user ready`); afterwards `list_vhosts` shows `smarthealth` and `/`, `list_users` shows `smarthealth_notification []` (empty tag list), and `list_permissions -p smarthealth` shows `.*` configure/write/read for the new user. (The management API also grants the administrator `smarthealth` user permissions on the new vhost, which is RabbitMQ's own behaviour for the creating user.)

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | a13e4ad | consumer with the booked handler, `broker_setup`, Dockerfile, four Compose services and secret, shared `pin_start_offsets`, settings, e2e tracer, 7 broker-setup unit tests |
| 2 | 4ad0188 | the other five handlers, outage mapping, stale guard, 23 trigger tests, 19 Compose wiring checks |
| 3 | ce8a1ce | e2e: cancel, reschedule, follow-up, both dead-letter paths, redelivery, transient retry; `notification` in the isolation list |

The tracer feedback gate: Task 1's `<verify>` passed end to end after the commit (7 unit tests, 1 e2e passed in 4.5 s), so expansion proceeded.

## Verification (measured)

- `tests/notification/test_broker_setup.py`: 7 passed (plan: at least 4). Includes captured stdout/stderr never containing the user or admin password and exit code 1 for missing admin credentials or password file.
- `tests/notification/test_triggers.py`: 23 passed (plan: at least 10).
- `tests/notification/test_compose_wiring.py`: 19 passed (crash switch absent, admin password in exactly one service, no host ports, secret mounted, builds from the notification image, restart and healthcheck, `latest` default).
- `tests/notification` whole directory: 68 passed in 194 s (one uninterrupted run, no sleep gap).
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_notifications.py -m e2e`: **8 passed** (plan: at least 6) in 47 s on the final images; the first run after the Task 2 deploy was 6 passed in 34 s, and the redelivery and transient tests were added afterwards (2 passed in 13 s, then 8 passed).
- `tests/e2e/test_phase2_flows.py -m e2e`: 5 passed (16 s), run after the notification services were live (no double effect from the workflows' notification stubs).
- `tests/workflows`: 277 passed, 11 skipped (replays included), 184 s, single run.
- `tests/libs/test_service_isolation.py tests/infra`: 38 passed. `tests/analytics/test_consumer_start.py` (affected by the `pin_start_offsets` move): 3 passed.
- Acceptance greps: `grep -c RABBITMQ_ADMIN_PASSWORD infra/docker-compose.yml` = 1; `grep -c NOTIFICATION_FAKE_CRASH_ENABLED infra/docker-compose.yml` = 0; `docker exec notification-celery env` shows no crash switch and no `*BROKER_PASSWORD=` variable; `docker port` for all four notification containers shows no published port.
- `rabbitmq-init`: `exited 0`. `notification-celery`, `notification-consumer`, `notification-temporal-worker`: `healthy`. `tjo-*` status lines before and after are identical (`tjo-api` unhealthy since before, `tjo-postgres` healthy, `tjo-migrate` exited).
- Semgrep: `semgrep --config p/python --metrics=off services/notification libs/events/start_offsets.py infra/docker-compose.yml`: 151 rules, 16 files, **0 findings**. Narrower than the plan's `--config auto` (refused with `--metrics=off`, as in earlier Phase 3 plans). No Dockerfile or YAML ruleset was run; `tests/` not scanned.
- Not run: the plan's full `pytest tests -m "not e2e"` (the orchestrator said it is not required). `ruff` is not installed in `.venv`.

## What the e2e tests prove (live stack)

- **Tracer**: a booking reaches `confirmed`; within 60 s one delivery with key `appointment.booked:{appointment_id}:{event_id}` is `sent`, `notification_provider_calls` has exactly one record for that key, and `reminder-{appointment_id}` is RUNNING.
- **Cancel**: exactly one `appointment.cancelled` delivery `sent` and one provider record; the reminder completes with result `cancelled`; the appointment is recorded closed; the booking notice is still exactly one.
- **Reschedule**: exactly one `appointment.rescheduled` delivery `sent` and one provider record; the reminder's history contains a `reschedule` signal whose slot equals the new slot's start, and the workflow is still RUNNING.
- **Follow-up**: check-in, start, complete through the gateway leave `followup-{visit_id}` RUNNING, and the check-in event had already completed the appointment's reminder.
- **Celery poison**: `enqueue_send` from the host to the live vhost with the dedicated user (password read from the secret file by the settings machinery, never printed) and a `FAIL-INVALID-` key raised `notifications.dlq` depth by exactly one; the delivery is `failed` and has no provider record.
- **Kafka poison**: non-Avro bytes with key `poison-<uuid>` on `smarthealth.appointment.events` appear on `smarthealth.dlq.notification-trigger` with `dlq.error_type = DeserializationError` and `dlq.consumer_group = notification-trigger`; a booking made afterwards is still notified exactly once and the consumer stays healthy. (The analytics DLQ also receives it, as expected.)
- **Redelivery (T-3-41, extra)**: the booked event's original bytes were re-produced to the same partition; after the group's committed offset passed the duplicate, the delivery still has `attempts == 1`, one provider record, and the same reminder run id. The worker log showed the duplicate task finishing with `'duplicate'`.
- **Transient failure (extra)**: a `FAIL-TRANSIENT-` key on the live worker is retried with backoff and ends `sent` on attempt 3 with one provider record.

## Decisions to report

- **Consumer start policy (D-13): `latest`.** The plan says the first start reads retained events from the earliest offset. That would have turned the live topics' throw-away test events (unknown ids, old appointments) into junk `skipped_stale` records and reminder workflows for non-existent appointments: with 6 partitions of retained events from earlier plans' tests, one Temporal workflow per old booked event and one stale-delivery record each. I therefore followed the 03-13 approach: `NOTIFICATION_CONSUMER_START` (`latest` in Compose via `${NOTIFICATION_CONSUMER_START:-latest}`, `earliest` as the code default so tests and replays read history) maps to `auto.offset.reset`, and `pin_start_offsets` commits the high watermark of the six partitions at first start. Measured: first start logged `start=latest, pinned_partitions=6`; the rebuilt container logged `pinned_partitions=0` (a restart keeps its position). Only e2e-produced events were processed (10 consumed after the first run). The stale guard is still in place for any later replay.
- **`pin_start_offsets` moved to `libs/events/start_offsets.py`.** It lived in `services/analytics/consumer.py`; `services/notification` may not import another service (isolation test). The function is byte-for-byte the same; analytics imports it from libs and `from services.analytics.consumer import pin_start_offsets` still works (its 3 tests pass). The analytics container was NOT rebuilt (its running image has the old copy, same behaviour); it picks up the move at its next rebuild.
- **Reminder behaviour on `appointment.checked_in`**: stops the reminder and marks the appointment closed with reason `checked_in` (plan's planner choice); `provider.schedule_changed` and `appointment.completed` have no handler.

## Deviations from Plan

**1. [Orchestrator decision] `latest` start with a pinned offset** instead of earliest (see above).

**2. [Rule 3 - Blocking, layering] Shared `pin_start_offsets` in libs.** Required by the service isolation rule; touches `services/analytics/consumer.py` (outside the plan's file list) with a pure move.

**3. [Rule 2 - Missing critical functionality] Consumer declares the broker topology at start.** `declare_topology` previously ran only in the Celery worker's `worker_init`. A message published by the consumer to a queue not yet declared would be dropped as unroutable, and `depends_on` only orders the consumer after `rabbitmq-init`, not after the worker. `main()` calls it (idempotent) before consuming. The Temporal worker does not: its sends fire later and the consumer and worker are normally up first (plan 03-06's worker `main` is unchanged).

**4. [Design] Lazy Temporal client** (visit-consumer pattern) instead of the plan's eager connection with retry: the SDK client must live on the loop that uses it, and the consumer base owns a persistent runner loop. The process boots while Temporal is down; an outage is retried per event as a transient failure.

**5. [Design] Stale guard on all booking-type events.** The plan's handler text names booked; the plan's decision text says booking-type events. Cancelled and rescheduled notices older than `NOTIFICATION_MAX_EVENT_AGE_MINUTES` are recorded `skipped_stale` too (cancel still closes the appointment and stops the reminder).

**6. [Rule 2 - Missing critical functionality] Broker outages cannot dead-letter an event.** `KombuError` and `AMQPError` (which include authentication and vhost errors) map to `TransientEventError`, so a misconfigured or down broker holds the offset instead of sending real events to the DLQ. Poison ids (missing field, non-UUID) map to `NonRetryableEventError`.

**7. [Plan wording] Wait-for-health retry in `broker_setup.main`.** The management listener can answer a few seconds after the broker answers a ping; `main` retries transport errors 15 times at 2 s before failing (HTTP error statuses are not retried).

**8. [Scope addition] Extra e2e tests** (redelivery, transient retry) and `tests/notification/test_compose_wiring.py` (the orchestrator asked that the crash-switch absence be asserted in a test).

**9. [Plan wording] No `env_file` on the notification services.** Nothing there needs a host-oriented `.env` value, and it avoids a host `NOTIFICATION_*` value leaking in; `RABBITMQ_ADMIN_*` still use Compose interpolation as the `rabbitmq` service does.

**10. [Process] Tests written alongside the code, not strictly red-first** (as in 03-10 and 03-13).

No auth gates, no package installs, no architectural changes.

## Live stack

- Built and started, with `--no-deps`: `rabbitmq-init` (first, waited for exit 0), then `notification-celery` and `notification-temporal-worker`, then `notification-consumer`. After Task 2 the consumer was rebuilt; at the end all three long-running containers were rebuilt once more so every notification image carries the same code. Nothing else was rebuilt or recreated (`gateway`, `analytics*`, `visit*` untouched, so the gateway rate-limit variables were not needed). No volume was removed, no `down -v`, no `tjo-*` container touched. No `.env*` or `infra/secrets/*` contents were read or printed (the secret file was only mounted and read by path through the settings code).
- The live `notification-consumer` was NOT stopped around the test runs: the harness tests (`tests/notification`, `tests/workflows`) use throwaway vhosts, databases and unique groups, and my e2e tests need the live consumer. The live `analytics-consumer` and `visit-consumer` were not stopped either.
- Side effects in live stores: synthetic e2e clinics, patients, appointments and visits; about 10 delivery documents and 8 provider-call records in Mongo (including the intentional `FAIL-INVALID-` document with status `failed`); one message in `notifications.dlq`; one poison message on each of `smarthealth.dlq.notification-trigger` and `smarthealth.dlq.analytics-projection`; running reminder workflows for the e2e appointments (future slots) and one follow-up workflow. Left in place by suite convention.
- Final state: every container in `docker compose ps -a` is healthy or an exited one-shot (`rabbitmq-init`, `db-init`, `kafka-init`, `schema-init`, `connect-init`) or an unhealthchecked scaffold service (grafana, jaeger, prometheus, temporal-ui).

## Known Stubs

None. The fake provider is the phase's intended provider (a real provider must accept an idempotency key for the same guarantee; plan 03-18 states it).

## Threat Flags

None beyond the plan's register. T-3-41: key per type, appointment and event id, enforced at the delivery log and the provider store; stale and redelivered events proven live. T-3-42: only `rabbitmq-init` holds admin credentials (grep and a test), the user has no tags and one vhost, no password is printed (unit tests), the crash switch is not set. T-3-43: both dead-letter paths proven live. T-3-44: Celery kwargs hold ids and timestamps only (a test asserts the exact key set). Note for 03-15: both the Celery worker and the consumer serve `/metrics` on 9100 inside the Compose network (`notification-celery:9100`, `notification-consumer:9100`), unpublished.

## Notes for later plans

- **03-15 (metrics scrape):** scrape `notification-celery:9100` (notification counters) and `notification-consumer:9100` (consumer base metrics, group `notification-trigger`); the temporal worker serves no metrics port.
- **03-18 (docs):** state D-13 for this group as above, that vhost `smarthealth` and the `smarthealth_notification` user are created by `rabbitmq-init` (and that the management API also grants the admin), that containers run as root (Celery warns), the stale-event rule and `NOTIFICATION_MAX_EVENT_AGE_MINUTES`, and that old notification state (`notification_appointment_state`) has no retention.
- A future full replay (consumer group reset to earliest) would send nothing older than the stale limit but WOULD start reminders for retained events.

## Self-Check: PASSED

- FOUND: services/notification/{triggers,consumer,broker_setup}.py, services/notification/Dockerfile, libs/events/start_offsets.py, tests/notification/{test_triggers,test_broker_setup,test_compose_wiring}.py, tests/e2e/test_phase3_notifications.py (checked with `ls` before commit).
- FOUND commits a13e4ad, 4ad0188, ce8a1ce (`git rev-list --count b4e8f5d..HEAD` = 3 before the docs commit).
