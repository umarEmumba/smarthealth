---
phase: 03-event-driven-system-observability
plan: 14
type: execute
wave: 7
depends_on: ["03-02", "03-04", "03-06", "03-07", "03-09", "03-13"]
files_modified:
  - services/notification/consumer.py
  - services/notification/triggers.py
  - services/notification/config.py
  - services/notification/deliveries.py
  - services/notification/broker_setup.py
  - services/notification/Dockerfile
  - infra/docker-compose.yml
  - tests/notification/test_triggers.py
  - tests/notification/test_broker_setup.py
  - tests/e2e/test_phase3_notifications.py
  - tests/libs/test_service_isolation.py
autonomous: true
requirements: [NOTIF-01, NOTIF-02, NOTIF-03, NOTIF-04, EVENT-03, EVENT-04, EVENT-05]

estimate:
  tokens: 105000
  raw_tokens: 105000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Booking, cancellation and reschedule each produce exactly one sent delivery and exactly one fake-provider call record for its dedupe key, sent asynchronously by Celery workers after the event, never inside the booking transaction (NOTIF-01, roadmap success criterion 2); the provider-side record is unique because the fake provider honours the dedupe key as an idempotency key (plan 03-02)"
    - "A booked appointment gets a running ReminderWorkflow; cancel and no-show stop it; reschedule moves it; a completed visit starts a follow-up workflow (NOTIF-02, NOTIF-03, D-02)"
    - "Notification sending is event-driven only: the Phase 2 workflow notification stubs stay unchanged intent records, so nothing double-sends (D-09 revised, research F5)"
    - "A redelivered event never reaches the provider twice (dedupe key = type, appointment id, event id, held at the delivery log and at the provider's idempotency store) and never starts a second reminder (deterministic ids) (NOTIF-04, EVENT-03)"
    - "Poison Kafka messages go to smarthealth.dlq.notification-trigger and poison Celery messages to notifications.dlq on the live broker, and processing continues (EVENT-05)"
    - "The notification image builds; the three notification processes and the broker provisioning job run in Compose with credentials from a Compose secret and no host ports; the provisioning job creates a dedicated non-admin broker user (EVENT-04)"
  artifacts:
    - path: services/notification/broker_setup.py
      provides: "idempotent vhost/user/permission provisioning through the management API (python -m services.notification.broker_setup)"
      contains: "RABBITMQ_ADMIN_PASSWORD"
    - path: services/notification/Dockerfile
      provides: "notification image: libs + services/notification, default CMD = Celery worker command"
    - path: services/notification/triggers.py
      provides: "NotificationTriggers with one handler per event type, ports for Celery, Temporal and Mongo"
      contains: "appointment.rescheduled"
    - path: services/notification/consumer.py
      provides: "notification-trigger consumer process"
      contains: "notification-trigger"
    - path: infra/docker-compose.yml
      provides: "notification-celery, notification-consumer, notification-temporal-worker, rabbitmq-init, secret notification_broker_password"
      contains: "notification-consumer"
    - path: tests/e2e/test_phase3_notifications.py
      provides: "one notification per booking/cancel/reschedule, reminder and follow-up workflows, live DLQ paths"
  key_links:
    - from: services/notification/triggers.py
      to: services/notification/tasks.py
      via: "enqueue_send with dedupe key {type}:{appointment_id}:{event_id}"
      pattern: "enqueue_send"
    - from: services/notification/triggers.py
      to: services/notification/reminders.py
      via: "start_reminder / reschedule_reminder / cancel_reminder / start_followup"
      pattern: "start_reminder"
---

<objective>
Close the notification loop: a notification-trigger consumer turns appointment and visit events into Celery sends and Temporal reminder control, and Compose runs the Celery worker, the consumer, the notification Temporal worker and a broker-provisioning job built from the new notification image.

Purpose: roadmap success criterion 2 end to end (NOTIF-01..04, EVENT-04/05) on the real stack, with idempotency against redelivery (EVENT-03).

Output: `services/notification/triggers.py`, `services/notification/consumer.py`, a stale-event guard, `services/notification/broker_setup.py` and `services/notification/Dockerfile` (moved here from plan 03-02, where nothing used them yet), four Compose services and a secret, handler and provisioning unit tests, and e2e proofs including the live dead-letter paths.

Decisions honoured: D-02 (Temporal reminders, Celery delivery), D-09 / D-09 revised (event-driven sending only; dedupe key = notification type + appointment id + event id; Temporal starts with deterministic ids), D-08 refined (no processed-event table here: Celery and Temporal effects carry their own idempotency), research Pattern 6 (send-time guard: the consumer records cancelled and no-show appointments as closed). Planner choices: `provider.schedule_changed` is not subscribed (the event has no patient recipient; affected patients are flagged by Scheduling, whose poller stays paused per the user); `appointment.checked_in` stops reminders; booking-type events older than `NOTIFICATION_MAX_EVENT_AGE_MINUTES` (default 1440) are recorded `skipped_stale` instead of sent, so a first start that replays history does not send day-old confirmations (reminders for past leads are already skipped by the workflow).

Live stack rules: create the new containers with `docker compose -f infra/docker-compose.yml up -d --build rabbitmq-init notification-celery notification-temporal-worker notification-consumer`; nothing else needs a rebuild. `rabbitmq-init` reuses the admin credentials the `rabbitmq` service already receives from Compose interpolation and the new user's password from the Compose secret. Never touch `tjo-*` containers (record status before and after). On first start the consumer reads retained events from the earliest offset (events from earlier plans' tests): expected and idempotent (old booking-type events are recorded `skipped_stale`). This is the only wave-7 plan, and the only plan in its wave that edits `infra/docker-compose.yml` (plan 03-13 edited it in wave 6).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-02-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-06-SUMMARY.md
@services/notification/tasks.py
@services/notification/deliveries.py
@services/notification/reminders.py
@services/notification/celery_app.py
@services/notification/config.py
@services/visit/Dockerfile
@services/visit/consumer.py
@libs/events/consumer.py
@infra/docker-compose.yml
@tests/e2e/phase3_stack.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `NotificationTriggers(publisher, temporal_client, deliveries_db, settings)` | class | handlers `on_booked`, `on_cancelled`, `on_rescheduled`, `on_no_show`, `on_checked_in`, `on_visit_completed`; `handlers()` returns the mapping for `EventConsumer` |
| consumer | process | `python -m services.notification.consumer`; group `notification-trigger`; topics `smarthealth.appointment.events`, `smarthealth.visit.events`; DLQ `smarthealth.dlq.notification-trigger`; tracer `smarthealth-notification-consumer`; heartbeat `/tmp/notification-consumer.alive` |
| settings | config | `NOTIFICATION_MAX_EVENT_AGE_MINUTES` (1440), `NOTIFICATION_CONSUMER_HEARTBEAT_PATH` |
| `record_skipped(db, request, reason)` | function | `deliveries.py`; writes a `skipped_stale` delivery document without a provider call |
| `ensure_broker_user(client, *, vhost, user, password)`, `main()` | module | `services/notification/broker_setup.py`: PUT `/api/vhosts/smarthealth`, PUT `/api/users/smarthealth_notification` (no tags), PUT `/api/permissions/smarthealth/smarthealth_notification`; prints `broker user ready` |
| `services/notification/Dockerfile` | image | python:3.12-slim, libs + services/notification, default CMD = the Celery worker command from plan 03-02; runs no migrations |
| Compose `rabbitmq-init` | one-shot | notification image, `python -m services.notification.broker_setup`, depends on rabbitmq healthy |
| Compose `notification-celery` | worker | worker command from plan 03-02, healthcheck on its internal `/metrics`, `restart: unless-stopped` |
| Compose `notification-consumer`, `notification-temporal-worker` | processes | heartbeat healthchecks, `restart: unless-stopped`, no host ports |
| Compose secret `notification_broker_password` | infra | `./secrets/infra/notification_broker.secret`, mounted as `/run/secrets/notification_broker.secret` in the four services |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a booking through the gateway reaches the fake provider exactly once through the Compose Celery worker, and its reminder workflow is running</name>
  <files>services/notification/triggers.py, services/notification/consumer.py, services/notification/config.py, services/notification/broker_setup.py, services/notification/Dockerfile, infra/docker-compose.yml, tests/notification/test_broker_setup.py, tests/e2e/test_phase3_notifications.py</files>
  <behavior>
    - After a booking through the gateway reaches `confirmed`, within 60 s `notification_deliveries` holds exactly one document with `dedupe_key = appointment.booked:{appointment_id}:{event_id}` and `status = sent`, and `notification_provider_calls` holds exactly one record for that key
    - `reminder-{appointment_id}` is RUNNING on Temporal (slot in the future, leads from settings)
    - The four new Compose services are healthy (rabbitmq-init exited 0); `rabbitmqctl list_users` shows `smarthealth_notification` without the administrator tag
    - Unit (no live call, `httpx.MockTransport`): `ensure_broker_user(...)` issues the three PUTs, is idempotent, never logs the password; `main()` exits 1 with a clear message when `RABBITMQ_ADMIN_USER`/`RABBITMQ_ADMIN_PASSWORD` or the password file is missing
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6, Pattern 7 "Started by the notification consumer")
    - services/notification/tasks.py (`enqueue_send`, `QUEUE_FOR_TYPE`), services/notification/reminders.py, services/notification/deliveries.py, services/notification/provider.py (plan 03-02: idempotent provider)
    - services/visit/consumer.py (consumer process entrypoint to mirror) and libs/events/consumer.py (`TransientEventError`)
    - services/notification/celery_app.py (`instrument_publisher`), services/notification/config.py (`NOTIFICATION_RABBITMQ_MANAGEMENT_URL`, `NOTIFICATION_BROKER_PASSWORD_FILE`)
    - services/visit/Dockerfile (image layout to mirror)
    - infra/docker-compose.yml (`visit-consumer`, `rabbitmq` and secrets blocks)
  </read_first>
  <action>
    broker_setup.py: `ensure_broker_user(client: httpx.Client, *, vhost, user, password)` with the three PUTs (permissions `.*` on the dedicated vhost only, no user tags), raising on non-2xx with the status code only; `main()` reads `RABBITMQ_ADMIN_USER`, `RABBITMQ_ADMIN_PASSWORD` from the environment and the user's password from `NOTIFICATION_BROKER_PASSWORD_FILE`, uses `NOTIFICATION_RABBITMQ_MANAGEMENT_URL`, prints `broker user ready` on success; `tests/notification/test_broker_setup.py` covers the unit behaviors (captured stdout and stderr never contain the password).

    Dockerfile: copy of the visit service Dockerfile pattern (python:3.12-slim, build-essential, `COPY pyproject.toml README.md`, `libs`, `services/__init__.py`, `services/notification`, `pip install --no-cache-dir .`) with the worker command from plan 03-02 as CMD; it runs no migrations (no Postgres database).

    triggers.py: `NotificationTriggers` holds a publisher callable (wrapping `enqueue_send` with the module Celery app, invoked through `asyncio.to_thread`), the Temporal client and a sync Mongo database handle for `deliveries` calls (also through `asyncio.to_thread`). `on_booked(envelope)`: build the send kwargs (type `appointment.booked`, dedupe key above, patient and appointment ids, event id, `requested_at` = now), skip-as-stale when `occurred_at` is older than the max age, else publish; then `start_reminder(...)` with the slot from the payload and the configured leads. Broker or Temporal connectivity errors -> `TransientEventError`. Task 1 wires only `appointment.booked`.

    consumer.py: `main()` mirroring the visit consumer: logging, `configure_tracer_provider("smarthealth-notification-consumer")`, `instrument_publisher()` (so the Celery publish joins the consumer span), eager Temporal connection with retry, registry warm-up, `EventConsumer(group_id="notification-trigger", topics=[...], handlers=triggers.handlers(), dlq_topic=DLQ_TOPICS["notification-trigger"], heartbeat_path=..., metrics_port=settings.metrics_port)`.

    Compose: add the four services and the secret from the artifacts table. Shared environment for the notification services: `<<: *app-env`, `NOTIFICATION_BROKER_HOST: rabbitmq`, `NOTIFICATION_BROKER_PASSWORD_FILE: /run/secrets/notification_broker.secret`, `TEMPORAL_ADDRESS: temporal:7233`; `rabbitmq-init` additionally gets `RABBITMQ_ADMIN_USER: ${RABBITMQ_DEFAULT_USER:-smarthealth}`, `RABBITMQ_ADMIN_PASSWORD: ${RABBITMQ_DEFAULT_PASS:-changeme-local-dev-only}` and `NOTIFICATION_RABBITMQ_MANAGEMENT_URL: http://rabbitmq:15672`. Dependencies: the three processes depend on `rabbitmq-init` completed and `mongo` healthy; consumer and Temporal worker also on `temporal` healthy and (consumer) `kafka` healthy and `schema-init` completed. All four notification services build from `services/notification/Dockerfile`. The fake-crash switch from plan 03-02 stays unset everywhere in Compose (it exists only for the crash test).

    e2e (marker e2e): seed with the Phase 2 helpers, book, wait confirmed, poll Mongo (`localhost:27017`, `MONGO_DB_NAME`) and Temporal (`localhost:7233`) for the behaviors, counting `notification_provider_calls` documents for the dedupe key.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification/test_broker_setup.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, no `sent` delivery appears within 60 s, more than one provider record exists for the dedupe key, the reminder workflow is not running, a provisioning test finds the password in captured output, or any new Compose service is unhealthy.</fails_when>
  </verify>
  <acceptance_criteria>
    - the tracer test passes and asserts `notification_provider_calls.count_documents({"dedupe_key": K}) == 1`
    - `.venv/bin/python -m pytest tests/notification/test_broker_setup.py -q` exits 0 with at least 4 passed
    - `docker compose -f infra/docker-compose.yml ps -a rabbitmq-init --format '{{.State}} {{.ExitCode}}'` shows `exited 0`; `notification-celery`, `notification-consumer`, `notification-temporal-worker` report `healthy`
    - `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl list_users` lists `smarthealth_notification` with an empty tag list
    - `grep -c "RABBITMQ_ADMIN_PASSWORD" infra/docker-compose.yml` is 1 (only the provisioning job receives admin credentials)
    - `grep -c "NOTIFICATION_FAKE_CRASH_ENABLED" infra/docker-compose.yml` is 0
    - tjo-* status lines show no restart
  </acceptance_criteria>
  <done>A booking reaches the fake provider exactly once through a containerized Celery worker, and its reminder is scheduled on Temporal.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Every trigger, the stale guard, and idempotency under redelivery (unit level)</name>
  <files>services/notification/triggers.py, services/notification/deliveries.py, tests/notification/test_triggers.py</files>
  <behavior>
    - booked -> one publish (booking queue, key `appointment.booked:{appointment_id}:{event_id}`) and `start_reminder`; cancelled -> one publish (`appointment.cancelled:...`), `mark_appointment_closed(reason="cancelled")`, `cancel_reminder`; rescheduled -> one publish (`appointment.rescheduled:...`) and `reschedule_reminder` with the new slot; no_show -> closed + `cancel_reminder`, no publish; checked_in -> `cancel_reminder` and closed (reason `checked_in`), no publish; visit.completed -> `start_followup` with the visit's ids and `completed_at`; appointment.completed and provider events -> no handler (ignored by the base)
    - The same envelope handled twice publishes twice with the same dedupe key (the send task's delivery log and the provider's idempotency store dedupe it) and calls the Temporal helpers twice (they are idempotent); no exception
    - A booked event older than the max age records `skipped_stale` and publishes nothing, but still starts the reminder (the workflow skips past leads itself)
    - A publisher raising a kombu connection error or a Temporal RPC unavailable error raises `TransientEventError`
    - Publish kwargs are strings only and contain no payload field besides ids and timestamps
  </behavior>
  <read_first>
    - services/notification/triggers.py (Task 1)
    - services/notification/reminders.py (helper signatures and return values)
    - libs/events/consumer.py (`EventEnvelope` shape)
  </read_first>
  <action>
    Implement the remaining handlers and `record_skipped` in `deliveries.py` (insert with status `skipped_stale`, ignore a duplicate key). Tests (no broker, no Temporal): fake publisher, fake Temporal helpers (inject them into `NotificationTriggers`), mongomock-free approach using the live Mongo with unique keys for the two deliveries calls, envelopes built from catalogue sample payloads.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification -x -q</automated>
    <fails_when>pytest exits non-zero, a handler publishes the wrong type or key, a no-show publishes, or an outage is not mapped to TransientEventError.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification/test_triggers.py -q` exits 0 with at least 10 passed
    - `.venv/bin/python -m pytest tests/notification -q` exits 0
  </acceptance_criteria>
  <done>Every notification-worthy event maps to exactly the intended sends and reminder operations, idempotently.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: End-to-end proofs on the stack — cancel, reschedule, follow-up, both dead-letter paths, isolation</name>
  <files>tests/e2e/test_phase3_notifications.py, tests/libs/test_service_isolation.py</files>
  <behavior>
    - Cancel through the gateway: exactly one `appointment.cancelled` delivery `sent` and exactly one provider record for its key; `reminder-{appointment_id}` closes with result `cancelled`
    - Reschedule through the gateway: exactly one `appointment.rescheduled` delivery `sent` and exactly one provider record for its key; the reminder workflow is running and its history contains a `reschedule` signal with the new slot
    - Check in, start and complete a visit: `followup-{visit_id}` is running
    - Celery poison: `enqueue_send` from the host to the live vhost `smarthealth` (dedicated user, password read from the secret file, never printed) with a `FAIL-INVALID-` key lands one message in `notifications.dlq` (`rabbitmqctl list_queues -p smarthealth name messages`)
    - Kafka poison: non-Avro bytes produced to `smarthealth.appointment.events` with key `poison-<uuid>` appear on `smarthealth.dlq.notification-trigger` with `dlq.error_type` set, and a booking made afterwards is still notified
    - Service isolation: `notification` is listed in `ALL_SERVICE_PACKAGES` and imports no other service package
  </behavior>
  <read_first>
    - tests/e2e/test_phase3_notifications.py (Task 1), tests/e2e/phase2_stack.py, tests/e2e/phase3_stack.py
    - tests/libs/test_service_isolation.py (`ALL_SERVICE_PACKAGES`)
  </read_first>
  <action>
    Add the scenarios; the Kafka poison message also reaches the analytics DLQ (expected; assert only the notification DLQ here). Use a unique clinic per test. Add `"notification"` to `ALL_SERVICE_PACKAGES` in the isolation test (the package owns no Postgres tables, so it is not added to `OWNED_TABLES`). Draining `notifications.dlq` is not required; record the DLQ depth before and after and assert the increase.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -x -q && .venv/bin/python -m pytest tests/libs/test_service_isolation.py -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a cancel or reschedule yields zero or two sent deliveries or provider records, a DLQ stays empty after its poison message, or the consumer stops processing after the poison.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -q` exits 0 with at least 6 passed
    - `.venv/bin/python -m pytest tests/libs/test_service_isolation.py -q` exits 0
  </acceptance_criteria>
  <done>Notifications, reminders and follow-ups work end to end on the stack, and both dead-letter paths are proven live.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka -> notification-consumer -> RabbitMQ / Temporal | events cause sends and long-lived timers |
| Compose -> notification containers | broker credentials and admin credentials for provisioning |
| rabbitmq-init -> RabbitMQ management API | the one-shot job creates the dedicated user and its permissions |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-41 | Repudiation / Tampering | replayed events causing duplicate sends | high | mitigate | Dedupe key per type, appointment and event id, unique at the delivery log and at the provider's idempotency store (plan 03-02); deterministic reminder and follow-up ids; stale booking events skipped; e2e proves one sent delivery and one provider record per action |
| T-3-42 | Elevation of Privilege | broker admin credentials and the provisioned user | medium | mitigate | Only `rabbitmq-init` receives admin credentials (acceptance grep); `broker_setup` creates `smarthealth_notification` with no tags and permissions on vhost `smarthealth` only, never logs the password (unit test); long-running processes use that user via a Compose secret; no host ports on new services; the crash-injection switch is absent from Compose (acceptance grep) |
| T-3-43 | Denial of Service | poison Kafka or Celery messages | medium | mitigate | Kafka: consumer base DLQ, partition continues (e2e); Celery: Reject without requeue into `notifications.dlq` (e2e) |
| T-3-44 | Information Disclosure | sends, logs, DLQs | medium | mitigate | Ids and types only; recipient is a patient id; fake provider records no contact data |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/notification tests/libs/test_service_isolation.py -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/notification infra/docker-compose.yml`; record status in the SUMMARY
</verification>

<success_criteria>
- One sent delivery and one provider record per booking, cancel and reschedule; reminders and follow-ups controlled by events
- Live dead-letter paths on both Kafka and RabbitMQ; credentials least-privilege and file-provided
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-14-SUMMARY.md` when done
</output>
