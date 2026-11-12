---
phase: 03-event-driven-system-observability
plan: 02
type: execute
wave: 2
depends_on: ["03-01"]
files_modified:
  - services/notification/__init__.py
  - services/notification/config.py
  - services/notification/celery_app.py
  - services/notification/tasks.py
  - services/notification/provider.py
  - services/notification/deliveries.py
  - tests/notification/__init__.py
  - tests/notification/conftest.py
  - tests/notification/broker.py
  - tests/notification/test_tasks.py
  - tests/notification/test_dedupe.py
  - tests/notification/test_dlq.py
autonomous: true
requirements: [EVENT-04, EVENT-05, NOTIF-04, OBS-02]

estimate:
  tokens: 95000
  raw_tokens: 95000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "A notification send runs as a Celery task on RabbitMQ, outside any user-facing request, with acks_late, manual ack semantics and a threads pool (EVENT-04, D-09)"
    - "Celery 5.6.3 starts on RabbitMQ 4.3 because remote control, task events, mingle, gossip and heartbeat are disabled (Pitfall 7)"
    - "The same send delivered twice, retried after a transient failure, or redelivered after the worker is killed between the provider call and mark_sent reaches the fake provider once per dedupe key: the provider's call record has a unique dedupe_key index and is checked and written atomically at the call, so a repeat call returns the recorded message id instead of sending again (NOTIF-04, D-09)"
    - "The fake provider models a provider that honours an idempotency key; the docs (plan 03-18) state that a real SMS/email provider must accept one for the same guarantee"
    - "A transient provider failure is retried with exponential backoff; retries exhausted or an invalid message is rejected without requeue and lands in notifications.dlq through the dead-letter exchange (EVENT-05)"
    - "A reminder for an appointment already recorded as closed (cancelled or no-show) is skipped, not sent (send-time guard, assumption A8)"
    - "The task span is a child of the publisher's span (Celery instrumentation), and task arguments are plain ids and strings, never a Pydantic object or PHI (D-11, CLAUDE.md)"
  artifacts:
    - path: services/notification/celery_app.py
      provides: "create_celery_app(), queues notifications.{booking,reminder,followup,dlq}, DLX smarthealth.dlx, RabbitMQ 4.3 settings, worker_init hooks"
      contains: "x-dead-letter-exchange"
    - path: services/notification/tasks.py
      provides: "notifications.send task, enqueue_send(), QUEUE_FOR_TYPE, SendRequest validation"
      contains: "notifications.send"
    - path: services/notification/deliveries.py
      provides: "ensure_notification_indexes (unique dedupe_key on deliveries AND provider calls), claim_delivery, mark_sent, mark_appointment_closed, is_appointment_closed"
      contains: "dedupe_key"
    - path: services/notification/provider.py
      provides: "FakeProvider.deliver(request, attempt) -> ProviderResult(message_id, replayed): idempotency-key store with atomic check-and-write"
      contains: "replayed"
  key_links:
    - from: services/notification/tasks.py
      to: services/notification/deliveries.py
      via: "claim_delivery before provider.deliver, mark_sent after"
      pattern: "claim_delivery"
    - from: services/notification/tasks.py
      to: services/notification/provider.py
      via: "a redelivered send calls deliver again and receives the recorded result (replayed=True), not a second delivery"
      pattern: "replayed"
    - from: services/notification/celery_app.py
      to: services/notification/tasks.py
      via: "Reject(requeue=False) dead-letters through smarthealth.dlx"
      pattern: "smarthealth.dlx"
---

<objective>
Build the notification delivery layer: a Celery app on RabbitMQ with per-type queues and a dead-letter exchange, one `notifications.send` task that reaches a fake provider exactly once per dedupe key (the provider honours the dedupe key as an idempotency key, so even a worker killed between the provider call and `mark_sent` cannot cause a second delivery), retries transient failures with backoff, dead-letters poison messages, and refuses reminders for closed appointments.

Purpose: EVENT-04 (background tasks on Celery + RabbitMQ), EVENT-05 (retry + dead-letter on the Celery side), NOTIF-04 (retry without double send). Plans 03-06 (Temporal reminders) and 03-14 (event-driven trigger) both enqueue this one task.

Output: the `services/notification` package (config, Celery app, task, idempotent fake provider, Mongo delivery log), plus tests against the live RabbitMQ (a throwaway vhost per test session) and live Mongo. The notification image (Dockerfile), the broker provisioning module and the Compose containers are added by plan 03-14, where they are first used.

Decisions honoured: D-02 (delivery stays on Celery even for Temporal-timed reminders), D-09 / D-09 revised (queues per type, `acks_late`, exponential retry, DLX for poison, fake provider that records sends, dedupe key = notification type + appointment id + event id for event-driven sends, unique `dedupe_key`), research Pattern 6 and Pitfall 7. Claude's discretion: queue layout `notifications.booking` / `.reminder` / `.followup` / `.dlq`, threads pool with concurrency 8, classic queues (quorum queues documented as the production path, assumption A10), failure injection by dedupe-key prefix (mirrors `BILLING_PRECHECK_FAIL_PREFIX`), a crash-injection switch that is off by default and only ever set by the crash test, the provider's idempotency store as the `notification_provider_calls` collection with a unique `dedupe_key` index.

Live stack note: tests create a throwaway RabbitMQ vhost and user per session through `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl ...` (random test-only credentials, deleted afterwards) and use the live Mongo. They touch no `tjo-*` container. Wave 2 coupling: plans 03-03 and 03-04 run alongside and neither recreates a container (the first stack rebuild is plan 03-05 in wave 3).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@libs/config.py
@libs/db/mongo.py
@libs/audit/mongo_indexes.py
@libs/observability/otel.py
@libs/observability/logging.py
@services/visit/config.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `NotificationSettings(SharedSettings)` | settings | `NOTIFICATION_BROKER_HOST` (default `localhost`), `NOTIFICATION_BROKER_PORT` (5672), `NOTIFICATION_BROKER_VHOST` (`smarthealth`), `NOTIFICATION_BROKER_USER` (`smarthealth_notification`), `NOTIFICATION_BROKER_PASSWORD_FILE` (default `infra/secrets/infra/notification_broker.secret`), `NOTIFICATION_FAKE_FAIL_PREFIX` (`FAIL-`), `NOTIFICATION_FAKE_CRASH_ENABLED` (false; only the crash test sets it), `NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS` (2), `NOTIFICATION_MAX_RETRIES` (5), `NOTIFICATION_METRICS_PORT` (9100), `NOTIFICATION_RABBITMQ_MANAGEMENT_URL` (`http://localhost:15672`, used by plan 03-14's provisioning job); `broker_url` built lazily, password never in repr |
| `create_celery_app(broker_url=None)` and module `app` | Celery | exchange `notifications` (direct), DLX `smarthealth.dlx`, queues `notifications.booking` (rk `booking`), `notifications.reminder` (rk `reminder`), `notifications.followup` (rk `followup`), `notifications.dlq`; json only; `task_acks_late`, `task_reject_on_worker_lost`, `worker_prefetch_multiplier=1`, remote control/events off; worker_init hooks for logging, tracer `smarthealth-notification-celery`, `CeleryInstrumentor`, metrics server, Mongo indexes |
| Worker command | CLI | `celery -A services.notification.celery_app worker -P threads -c 8 -Q notifications.booking,notifications.reminder,notifications.followup --without-mingle --without-gossip --without-heartbeat` |
| `notifications.send` task | Celery | kwargs `notification_type`, `dedupe_key`, `patient_id`, `appointment_id`, `visit_id`, `event_id`, `requested_at`; validated by `SendRequest` inside the task |
| `NOTIFICATION_TYPES`, `QUEUE_FOR_TYPE`, `enqueue_send(app, **kwargs)` | module | types `appointment.booked`, `appointment.cancelled`, `appointment.rescheduled`, `appointment.reminder`, `visit.followup` |
| Mongo collections | data | `notification_deliveries` (unique `dedupe_key`; status `sending`/`sent`/`skipped_closed`/`skipped_stale`/`failed`), `notification_provider_calls` (the fake provider's idempotency store: unique `dedupe_key`, one doc per delivered key with `message_id`, `notification_type`, `patient_id`, `attempt`, `delivered_at`), `notification_appointment_state` (unique `appointment_id`, `closed_reason`, `closed_at`) |
| `FakeProvider`, `ProviderResult(message_id, replayed)`, `TransientDeliveryError`, `PermanentDeliveryError` | module | `services/notification/provider.py` |
| metrics | Prometheus | `notifications_sent_total{notification_type}`, `notifications_skipped_total{notification_type,reason}`, `notifications_retried_total{notification_type}`, `notifications_dead_lettered_total{notification_type,error_type}`, `notifications_provider_replayed_total{notification_type}` (a repeat call answered from the idempotency store), `notification_delivery_seconds{notification_type}` (requested_at to provider success) |
| `tests/notification/broker.py` | test helper | `throwaway_broker()` context manager (vhost + user via rabbitmqctl, removed afterwards) |
| crash injection | test hook | dedupe-key prefix `<fail_prefix>CRASH-AFTER-PROVIDER-` makes the worker process exit right after the provider call and before `mark_sent`, on attempt 1 only, and only when `NOTIFICATION_FAKE_CRASH_ENABLED` is true |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — one send published inside a span runs on an in-process worker over the live RabbitMQ and is recorded as sent in Mongo, in the same trace</name>
  <files>services/notification/__init__.py, services/notification/config.py, services/notification/celery_app.py, services/notification/tasks.py, services/notification/provider.py, services/notification/deliveries.py, tests/notification/__init__.py, tests/notification/conftest.py, tests/notification/broker.py, tests/notification/test_tasks.py</files>
  <behavior>
    - `enqueue_send(app, notification_type="appointment.booked", dedupe_key=K, patient_id=..., appointment_id=..., event_id=..., requested_at=...)` inside span P is consumed by an in-process threads worker bound to a throwaway vhost
    - `notification_deliveries` then holds one document with `dedupe_key=K`, `status="sent"`, `attempts=1`, ids only; `notification_provider_calls` holds one document for K
    - The task's span has P's trace id and a parent inside P's trace (Celery instrumentation, both publisher and worker sides)
    - The message went through the `notifications.booking` queue (the worker consumes only the listed queues)
    - Task kwargs contain only strings (no Pydantic object crosses the broker); an unknown `notification_type` is refused by `SendRequest`
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6 verbatim configuration and worker command, Pitfall 7)
    - libs/config.py (SharedSettings alias-only binding) and services/visit/config.py (service settings style)
    - libs/audit/mongo_indexes.py (index creation pattern) and libs/db/mongo.py (database name settings)
    - libs/observability/otel.py, libs/observability/logging.py
    - tests/workflows/test_trace.py (InMemorySpanExporter attachment)
  </read_first>
  <action>
    config.py: `NotificationSettings` with the fields in the artifacts table; `broker_url` reads the password file at call time, URL-quotes user and password, and the password is held as `SecretStr` so it never appears in repr or logs; a missing file raises a clear error naming the setting, not the path contents.

    celery_app.py: `create_celery_app(broker_url=None) -> Celery` applying the Pattern 6 configuration (queues with `x-dead-letter-exchange=smarthealth.dlx` and `x-dead-letter-routing-key=notifications.dlq`, the DLQ queue bound to the durable direct exchange `smarthealth.dlx`, `task_acks_late=True`, `task_reject_on_worker_lost=True`, `worker_prefetch_multiplier=1`, `task_serializer="json"`, `accept_content=["json"]`, `worker_enable_remote_control=False`, `worker_send_task_events=False`, `task_send_sent_event=False`, `result_backend=None`, `task_ignore_result=True`, `broker_connection_retry_on_startup=True`). Importing the module must not read the password file (tests build their own app); the module-level `app` resolves `settings.broker_url` lazily when a worker or publisher starts. Connect a `worker_init` handler that runs `configure_logging()`, `configure_tracer_provider("smarthealth-notification-celery")`, `CeleryInstrumentor().instrument()`, `ensure_notification_indexes()`, and `prometheus_client.start_http_server(settings.metrics_port)` once. Expose `instrument_publisher()` (idempotent `CeleryInstrumentor().instrument()` for publishing processes: the consumer and the Temporal worker in later plans).

    deliveries.py (sync `pymongo.MongoClient`: the threads pool runs sync tasks): `ensure_notification_indexes(db)` (unique `dedupe_key` on `notification_deliveries`, unique `dedupe_key` on `notification_provider_calls`, unique `appointment_id` on `notification_appointment_state`), `claim_delivery(db, request) -> Claim` (insert `{dedupe_key, notification_type, patient_id, appointment_id, visit_id, event_id, status: "sending", attempts: 1, created_at}`; on `DuplicateKeyError` read it: `sent`/`skipped_*` -> `Claim.DONE`; `sending`/`failed` -> increment `attempts` and return `Claim.RETRY`), `mark_sent`, `mark_failed`, `mark_skipped(reason)`, `mark_appointment_closed(db, appointment_id, reason)` and `is_appointment_closed(db, appointment_id)`.

    provider.py: `FakeProvider` models a provider that honours an idempotency key (the dedupe key). `deliver(request, attempt) -> ProviderResult(message_id, replayed)` first applies the failure injection by dedupe-key prefix, which models requests that never reached the provider and therefore records nothing: `<fail_prefix>TRANSIENT-` raises `TransientDeliveryError` on attempts 1 and 2, `<fail_prefix>PERMANENT-` always raises `TransientDeliveryError` (so retries exhaust), `<fail_prefix>INVALID-` raises `PermanentDeliveryError`. Then it checks and writes its call record in ONE atomic operation, before any delivery is acknowledged: `insert_one` into `notification_provider_calls` of `{dedupe_key, message_id (new uuid4), notification_type, patient_id, attempt, delivered_at}` under the unique `dedupe_key` index. A successful insert IS the fake's delivery (the record is the delivered message, so there is no window inside the fake between "delivered" and "recorded") and returns `replayed=False`. A `DuplicateKeyError` means the key was already delivered: read the stored document and return its `message_id` with `replayed=True` without delivering again, counting `notifications_provider_replayed_total`. Recipient is the patient id only. Real keys start with the notification type, so they never match an injection prefix. Module docstring: a real SMS/email provider gives the same guarantee only if it accepts an idempotency key; this fake demonstrates the pattern.

    tasks.py: `NOTIFICATION_TYPES`, `QUEUE_FOR_TYPE` (booked/cancelled/rescheduled -> `notifications.booking`, reminder -> `notifications.reminder`, followup -> `notifications.followup`), Pydantic `SendRequest` (types restricted, uuid-shaped id strings, `dedupe_key` max 200 chars) validated INSIDE the task from plain kwargs, `enqueue_send(app, **kwargs)` (`app.send_task("notifications.send", kwargs=..., queue=QUEUE_FOR_TYPE[type])`), and the bound task `notifications.send` (`max_retries` from settings). In Task 1 implement the happy path: validate, `claim_delivery`, `FakeProvider.deliver`, `mark_sent`, metrics `notifications_sent_total` and `notification_delivery_seconds`, one structlog line `notification.sent` with type, dedupe key and ids only.

    tests/notification/broker.py: `throwaway_broker()` creates vhost and user `p3test-<hex>` with a random password via `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl add_vhost/add_user/set_permissions`, yields `amqp://...@localhost:5672/<vhost>`, and deletes both on exit (fixed argv lists, no shell). conftest: session fixture around it, a `celery_worker` fixture using `celery.contrib.testing.worker.start_worker(app, pool="threads", concurrency=2, perform_ping_check=False)`, a Mongo fixture that clears the three collections for the test's keys only, and `in_memory_spans`. Tests marked `integration`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification/test_tasks.py -x -q</automated>
    <fails_when>pytest exits non-zero, the worker fails with `transient_nonexcl_queues`, the delivery document is missing or not `sent`, or the task span's trace id differs from the publisher's.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification/test_tasks.py -q` exits 0 with at least 5 passed
    - `grep -c "worker_enable_remote_control" services/notification/celery_app.py` is at least 1 and `grep -c "accept_content" services/notification/celery_app.py` is at least 1
    - `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl list_vhosts` lists no `p3test-` vhost after the run
  </acceptance_criteria>
  <done>A send travels publisher span -> RabbitMQ -> threads worker -> fake provider -> Mongo as one trace.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Exactly-once delivery per dedupe key (including a worker killed between the provider call and mark_sent), retry with backoff, and the closed-appointment guard</name>
  <files>services/notification/config.py, services/notification/tasks.py, services/notification/deliveries.py, tests/notification/test_dedupe.py</files>
  <behavior>
    - The same send enqueued twice (same dedupe key) results in one `sent` delivery and exactly one `notification_provider_calls` document
    - Kill between the provider call and `mark_sent`: a Celery worker started as a SUBPROCESS (threads pool, the throwaway vhost, `NOTIFICATION_FAKE_CRASH_ENABLED=true`, a free metrics port) takes a `FAIL-CRASH-AFTER-PROVIDER-` send; the provider writes its call record, then the worker process exits before `mark_sent` (the test asserts the subprocess exit code is the injected one); because of `acks_late` the unacknowledged message returns to the queue when the connection drops; an in-process worker (crash switch off) then takes it: `claim_delivery` returns RETRY, `deliver` returns the recorded `message_id` with `replayed=True`, and the delivery ends `sent` with `attempts=2`; `notification_provider_calls.count_documents({"dedupe_key": K}) == 1` and `notifications_provider_replayed_total` rose by 1
    - Deterministic variant of the same window: a delivery left in `sending` plus an existing provider record for K (exactly the state the kill leaves) is redelivered and ends `sent` with one provider record and `replayed=True`
    - A `FAIL-TRANSIENT-` key is retried with countdowns `base * 2**retries` (base 1 s in the test via settings) and ends `sent` on attempt 3; the two failed attempts recorded nothing, so exactly one provider record exists with `attempt=3`; `notifications_retried_total` counts two retries
    - An `appointment.reminder` send for an appointment marked closed is recorded `skipped_closed` with zero provider records; a booking-type send for the same appointment is unaffected
    - Concurrent duplicates (two workers, same key) give one provider record
  </behavior>
  <read_first>
    - services/notification/tasks.py, deliveries.py, provider.py, config.py (Task 1)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6 "Dedupe + fake provider" and "Send-time guard")
    - tests/notification/broker.py and tests/notification/conftest.py (throwaway vhost credentials to pass to the subprocess worker)
  </read_first>
  <action>
    Complete the task body: before claiming, if the type is `appointment.reminder` and `is_appointment_closed(appointment_id)`, `mark_skipped("closed")`, count `notifications_skipped_total{reason="closed"}` and return. `claim_delivery`: `Claim.DONE` returns without calling the provider; `Claim.RETRY` proceeds with the stored attempt number. Call `FakeProvider.deliver`; on `replayed=True` log `notification.provider_replayed` (type, dedupe key) and continue to `mark_sent` as for a fresh delivery. `TransientDeliveryError` with retries left: `mark_failed` (status `failed`, keeps the key claimable), count a retry, `raise self.retry(exc=exc, countdown=settings.retry_backoff_base_seconds * 2 ** self.request.retries)`.

    Crash injection (config.py + tasks.py): add the boolean setting `fake_crash_enabled` (env `NOTIFICATION_FAKE_CRASH_ENABLED`, default false). Right after `deliver` returns and before `mark_sent`, when the setting is true, the attempt is 1 and the dedupe key starts with `<fail_prefix>CRASH-AFTER-PROVIDER-`, log `notification.fake_crash` and end the process with `os._exit(70)` (no cleanup, no ack: the closest in-process model of a killed worker). Nothing in Compose ever sets the switch (plan 03-14 asserts that).

    Module docstring of tasks.py: one send per dedupe key holds at two layers, the delivery log (no new attempt once `sent`) and the provider's idempotency store (unique `dedupe_key`, checked and written atomically at the call), so a worker killed between the provider call and `mark_sent` replays the recorded result instead of sending again. A real external provider needs to accept an idempotency key for this to hold; without one, each crash at exactly that point could cause one duplicate (stated in the docs, plan 03-18).

    Tests (marker `integration`, throwaway vhost, real workers) for every behavior. The subprocess worker runs `.venv/bin/celery -A services.notification.celery_app worker -P threads -c 1 -Q notifications.booking --without-mingle --without-gossip --without-heartbeat` with fixed argv and an environment holding the throwaway vhost, user, a 0600 temp password file, the Mongo database name of the test and the crash switch; the test waits for its exit (timeout 60 s, then terminates it and fails). The concurrency case uses two in-process workers on the same vhost.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification/test_dedupe.py -x -q</automated>
    <fails_when>pytest exits non-zero; any test finds more than one `notification_provider_calls` document for a dedupe key (including after the worker kill between the provider call and `mark_sent`); the killed send does not end `sent`; or the subprocess worker exits with a code other than the injected one.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification/test_dedupe.py -q` exits 0 with at least 6 passed
    - the duplicate-enqueue, crash-after-provider (subprocess kill), deterministic-window, transient-retry and concurrent tests each assert `notification_provider_calls.count_documents({"dedupe_key": K}) == 1`
    - the crash test asserts the subprocess exit code is 70 and the final delivery has `status == "sent"` and `attempts == 2`
    - `grep -c "fake_crash_enabled" services/notification/config.py` is at least 1
  </acceptance_criteria>
  <done>Duplicates, transient retries and a worker killed between the provider call and mark_sent all reach the fake provider once per dedupe key, and reminders for closed appointments are skipped.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Dead-letter path through the dead-letter exchange</name>
  <files>services/notification/tasks.py, tests/notification/test_dlq.py</files>
  <behavior>
    - A `FAIL-PERMANENT-` key exhausts its retries, is marked `failed`, and the message appears in `notifications.dlq` on the test vhost (read back with a kombu `SimpleQueue`/`basic_get`) with its original kwargs; `notifications_dead_lettered_total{error_type="TransientDeliveryError"}` increments; no provider record exists for the key (every attempt failed before reaching the provider)
    - A `FAIL-INVALID-` key (PermanentDeliveryError) and a message with invalid kwargs (fails `SendRequest`) are dead-lettered on the first attempt with no retry
    - Declared queues carry `x-dead-letter-exchange=smarthealth.dlx` (checked through `rabbitmqctl list_queues name arguments -p <vhost>`)
  </behavior>
  <read_first>
    - services/notification/tasks.py (Task 2), services/notification/celery_app.py (queue declarations)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6: `Reject(requeue=False)` only dead-letters with acks_late)
  </read_first>
  <action>
    tasks.py: retries exhausted, `PermanentDeliveryError`, or a `SendRequest` validation error -> `mark_failed` when a delivery exists, count `notifications_dead_lettered_total{error_type=<class name>}`, log `notification.dead_lettered` with type, dedupe key and error type only, then `raise Reject(exc, requeue=False)` so RabbitMQ routes the message through `smarthealth.dlx` to `notifications.dlq`.

    Tests (tests/notification/test_dlq.py, marker `integration`, throwaway vhost, in-process worker) for every behavior above.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification -x -q</automated>
    <fails_when>pytest exits non-zero, a poison message is requeued instead of dead-lettered, the DLQ stays empty, or an invalid message is retried.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification -q` exits 0 with at least 14 passed
    - `grep -c "requeue=False" services/notification/tasks.py` is at least 1
    - `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl list_vhosts` lists no `p3test-` vhost after the run
  </acceptance_criteria>
  <done>Poison and exhausted sends reach the dead-letter queue without ever reaching the provider twice.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| publisher (consumer, Temporal activity) -> RabbitMQ -> Celery worker | task messages are deserialized by the worker |
| worker -> fake provider / Mongo | delivery log and the provider's idempotency store |
| notification processes -> broker credentials | the broker password read from a file |
| crash-injection switch -> worker process | a test-only setting that can end the worker process |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-15 | Tampering | Celery message deserialization | high | mitigate | `task_serializer="json"`, `accept_content=["json"]` (no pickle); kwargs revalidated by `SendRequest` inside the task; invalid messages dead-lettered, never executed |
| T-3-16 | Information Disclosure | task kwargs, delivery log, provider records, logs, DLQ | high | mitigate | Ids, types, dedupe keys and timestamps only; recipient is a patient id (no contact data); no payload text in logs; DLQ holds the same PHI-free kwargs |
| T-3-17 | Elevation of Privilege | RabbitMQ credentials in the notification settings and tests | medium | mitigate | Password read from a file at call time and held as SecretStr (never in repr or logs); tests use throwaway vhosts and users with random passwords, deleted afterwards; the dedicated non-admin Compose user and its provisioning job are plan 03-14 (T-3-42) |
| T-3-18 | Repudiation / Tampering | replayed, retried or crash-redelivered sends | high | mitigate | Two layers: unique `dedupe_key` on the delivery log with its status machine, and the provider's idempotency store (unique `dedupe_key`, atomic check-and-write at the call) that answers a repeat call with the recorded message id; a subprocess-kill test between the provider call and `mark_sent` proves one provider record; residual risk for a REAL provider without idempotency keys documented in plan 03-18 |
| T-3-19 | Denial of Service | poison task looping | medium | mitigate | Bounded retries with exponential backoff, `Reject(requeue=False)` into `notifications.dlq`, prefetch 1; quorum queues with `delivery-limit` documented as the production answer |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | Celery, kombu and the Celery instrumentation were approved and installed in plan 03-01 Task 1; nothing new installed here |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/notification -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/notification`; record status in the SUMMARY
</verification>

<success_criteria>
- Celery on RabbitMQ 4.3 reaches the fake provider once per dedupe key, including after a worker kill between the provider call and mark_sent, with retries, a closed-appointment guard and a working DLQ
- The task, settings and idempotent provider are ready for plans 03-06 and 03-14
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-02-SUMMARY.md` when done
</output>
