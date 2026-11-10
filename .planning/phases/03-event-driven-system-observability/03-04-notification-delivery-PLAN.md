---
phase: 03-event-driven-system-observability
plan: 04
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
  - services/notification/broker_setup.py
  - services/notification/Dockerfile
  - tests/notification/__init__.py
  - tests/notification/conftest.py
  - tests/notification/broker.py
  - tests/notification/test_tasks.py
  - tests/notification/test_dedupe.py
  - tests/notification/test_dlq.py
  - tests/notification/test_broker_setup.py
autonomous: true
requirements: [EVENT-04, EVENT-05, NOTIF-04, OBS-02]

estimate:
  tokens: 100000
  raw_tokens: 100000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "A notification send runs as a Celery task on RabbitMQ, outside any user-facing request, with acks_late, manual ack semantics and a threads pool (EVENT-04, D-09)"
    - "Celery 5.6.3 starts on RabbitMQ 4.3 because remote control, task events, mingle, gossip and heartbeat are disabled (Pitfall 7)"
    - "The same send delivered twice, or retried after a crash, reaches the fake provider once per dedupe key (unique Mongo index; NOTIF-04, D-09)"
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
      provides: "ensure_notification_indexes, claim_delivery, mark_sent, mark_appointment_closed, is_appointment_closed"
      contains: "dedupe_key"
    - path: services/notification/broker_setup.py
      provides: "idempotent vhost/user/permission provisioning through the management API"
  key_links:
    - from: services/notification/tasks.py
      to: services/notification/deliveries.py
      via: "claim_delivery before provider.deliver, mark_sent after"
      pattern: "claim_delivery"
    - from: services/notification/celery_app.py
      to: services/notification/tasks.py
      via: "Reject(requeue=False) dead-letters through smarthealth.dlx"
      pattern: "smarthealth.dlx"
---

<objective>
Build the notification delivery layer: a Celery app on RabbitMQ with per-type queues and a dead-letter exchange, one `notifications.send` task that sends through a fake provider exactly once per dedupe key, retries transient failures with backoff, dead-letters poison messages, and refuses reminders for closed appointments.

Purpose: EVENT-04 (background tasks on Celery + RabbitMQ), EVENT-05 (retry + dead-letter on the Celery side), NOTIF-04 (no double send). Plans 03-08 (Temporal reminders) and 03-11 (event-driven trigger) both enqueue this one task.

Output: the `services/notification` package (config, Celery app, task, fake provider, Mongo delivery log, broker provisioning) and its Dockerfile, plus tests against the live RabbitMQ (a throwaway vhost per test session) and live Mongo. The Compose containers are added by plan 03-11.

Decisions honoured: D-02 (delivery stays on Celery even for Temporal-timed reminders), D-09 / D-09 revised (queues per type, `acks_late`, exponential retry, DLX for poison, fake provider that records sends, dedupe key = notification type + appointment id + event id for event-driven sends, unique `dedupe_key`), research Pattern 6 and Pitfall 7. Claude's discretion: queue layout `notifications.booking` / `.reminder` / `.followup` / `.dlq`, threads pool with concurrency 8, classic queues (quorum queues documented as the production path, assumption A10), a dedicated non-admin broker user on vhost `smarthealth` with its password from a Compose secret file, failure injection by dedupe-key prefix (mirrors `BILLING_PRECHECK_FAIL_PREFIX`).

Live stack note: tests create a throwaway RabbitMQ vhost and user per session through `docker compose -f infra/docker-compose.yml exec -T rabbitmq rabbitmqctl ...` (random test-only credentials, deleted afterwards) and use the live Mongo. They touch no `tjo-*` container. Plan 03-02 recreates containers in the same wave; re-run tests after `scripts/compose_healthcheck.sh` passes if they fail during its rebuild.
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
@services/visit/Dockerfile
@services/visit/config.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `NotificationSettings(SharedSettings)` | settings | `NOTIFICATION_BROKER_HOST` (default `localhost`), `NOTIFICATION_BROKER_PORT` (5672), `NOTIFICATION_BROKER_VHOST` (`smarthealth`), `NOTIFICATION_BROKER_USER` (`smarthealth_notification`), `NOTIFICATION_BROKER_PASSWORD_FILE` (default `infra/secrets/infra/notification_broker.secret`), `NOTIFICATION_FAKE_FAIL_PREFIX` (`FAIL-`), `NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS` (2), `NOTIFICATION_MAX_RETRIES` (5), `NOTIFICATION_METRICS_PORT` (9100), `NOTIFICATION_RABBITMQ_MANAGEMENT_URL` (`http://localhost:15672`); `broker_url` built lazily, password never in repr |
| `create_celery_app(broker_url=None)` and module `app` | Celery | exchange `notifications` (direct), DLX `smarthealth.dlx`, queues `notifications.booking` (rk `booking`), `notifications.reminder` (rk `reminder`), `notifications.followup` (rk `followup`), `notifications.dlq`; json only; `task_acks_late`, `task_reject_on_worker_lost`, `worker_prefetch_multiplier=1`, remote control/events off; worker_init hooks for logging, tracer `smarthealth-notification-celery`, `CeleryInstrumentor`, metrics server, Mongo indexes |
| Worker command | CLI | `celery -A services.notification.celery_app worker -P threads -c 8 -Q notifications.booking,notifications.reminder,notifications.followup --without-mingle --without-gossip --without-heartbeat` |
| `notifications.send` task | Celery | kwargs `notification_type`, `dedupe_key`, `patient_id`, `appointment_id`, `visit_id`, `event_id`, `requested_at`; validated by `SendRequest` inside the task |
| `NOTIFICATION_TYPES`, `QUEUE_FOR_TYPE`, `enqueue_send(app, **kwargs)` | module | types `appointment.booked`, `appointment.cancelled`, `appointment.rescheduled`, `appointment.reminder`, `visit.followup` |
| Mongo collections | data | `notification_deliveries` (unique `dedupe_key`; status `sending`/`sent`/`skipped_closed`/`skipped_stale`/`failed`), `notification_provider_calls` (one doc per fake-provider call), `notification_appointment_state` (unique `appointment_id`, `closed_reason`, `closed_at`) |
| `FakeProvider`, `TransientDeliveryError`, `PermanentDeliveryError` | module | `services/notification/provider.py` |
| metrics | Prometheus | `notifications_sent_total{notification_type}`, `notifications_skipped_total{notification_type,reason}`, `notifications_retried_total{notification_type}`, `notifications_dead_lettered_total{notification_type,error_type}`, `notification_delivery_seconds{notification_type}` (requested_at to provider success) |
| `services/notification/Dockerfile` | image | python:3.12-slim, libs + services/notification, default CMD = worker command |
| `tests/notification/broker.py` | test helper | `throwaway_broker()` context manager (vhost + user via rabbitmqctl, removed afterwards) |

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

    deliveries.py (sync `pymongo.MongoClient`: the threads pool runs sync tasks): `ensure_notification_indexes(db)` (unique `dedupe_key` on `notification_deliveries`, unique `appointment_id` on `notification_appointment_state`), `claim_delivery(db, request) -> Claim` (insert `{dedupe_key, notification_type, patient_id, appointment_id, visit_id, event_id, status: "sending", attempts: 1, created_at}`; on `DuplicateKeyError` read it: `sent`/`skipped_*` -> `Claim.DONE`; `sending`/`failed` -> increment `attempts` and return `Claim.RETRY`), `mark_sent`, `mark_failed`, `mark_skipped(reason)`, `mark_appointment_closed(db, appointment_id, reason)` and `is_appointment_closed(db, appointment_id)`.

    provider.py: `FakeProvider.deliver(request, attempt)` inserts one `notification_provider_calls` document per call (dedupe key, type, attempt, timestamp; recipient is the patient id only) and simulates failures by dedupe-key prefix: `<fail_prefix>TRANSIENT-` raises `TransientDeliveryError` on attempts 1 and 2, `<fail_prefix>PERMANENT-` always raises `TransientDeliveryError` (so retries exhaust), `<fail_prefix>INVALID-` raises `PermanentDeliveryError`. Real keys start with the notification type, so they never match.

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
  <name>Task 2: Exactly-once send per dedupe key, retry with backoff, and the closed-appointment guard</name>
  <files>services/notification/tasks.py, services/notification/deliveries.py, tests/notification/test_dedupe.py</files>
  <behavior>
    - The same send enqueued twice (same dedupe key) results in one `sent` delivery and exactly one provider call
    - A delivery left in `sending` (simulating a worker that died after the provider call but before `mark_sent`) is sent again on redelivery and ends `sent` with `attempts=2`: the at-least-once window is at the provider, and the test documents it
    - A `FAIL-TRANSIENT-` key is retried with countdowns `base * 2**retries` (base 1 s in the test via settings) and ends `sent` after three provider calls; `notifications_retried_total` counts two retries
    - An `appointment.reminder` send for an appointment marked closed is recorded `skipped_closed` with zero provider calls; a booking-type send for the same appointment is unaffected
    - Concurrent duplicates (two workers, same key) still give one provider call for a key that succeeds first time
  </behavior>
  <read_first>
    - services/notification/tasks.py, deliveries.py, provider.py (Task 1)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6 "Dedupe + fake provider" and "Send-time guard")
  </read_first>
  <action>
    Complete the task body: before claiming, if the type is `appointment.reminder` and `is_appointment_closed(appointment_id)`, `mark_skipped("closed")`, count `notifications_skipped_total{reason="closed"}` and return. `claim_delivery`: `Claim.DONE` returns without calling the provider; `Claim.RETRY` proceeds with the stored attempt number. `TransientDeliveryError` with retries left: `mark_failed` (status `failed`, keeps the key claimable), count a retry, `raise self.retry(exc=exc, countdown=settings.retry_backoff_base_seconds * 2 ** self.request.retries)`. Document in the module docstring that dedupe holds at the delivery log; a crash between the provider call and `mark_sent` can resend once (provider-side idempotency would close it with a real provider).

    Tests (marker `integration`, throwaway vhost, real worker) for every behavior; the concurrency case uses two in-process workers on the same vhost.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification/test_dedupe.py -x -q</automated>
    <fails_when>pytest exits non-zero or any test counts more than one provider call for a key that succeeded on its first attempt.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification/test_dedupe.py -q` exits 0 with at least 5 passed
    - the duplicate-enqueue test asserts `notification_provider_calls.count_documents({"dedupe_key": K}) == 1`
  </acceptance_criteria>
  <done>Retries and duplicates never double-send, and reminders for closed appointments are skipped.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Dead-letter path, broker provisioning module, and the notification image</name>
  <files>services/notification/tasks.py, services/notification/broker_setup.py, services/notification/Dockerfile, tests/notification/test_dlq.py, tests/notification/test_broker_setup.py</files>
  <behavior>
    - A `FAIL-PERMANENT-` key exhausts its retries, is marked `failed`, and the message appears in `notifications.dlq` on the test vhost (read back with a kombu `SimpleQueue`/`basic_get`) with its original kwargs; `notifications_dead_lettered_total{error_type="TransientDeliveryError"}` increments
    - A `FAIL-INVALID-` key (PermanentDeliveryError) and a message with invalid kwargs (fails `SendRequest`) are dead-lettered on the first attempt with no retry
    - Declared queues carry `x-dead-letter-exchange=smarthealth.dlx` (checked through `rabbitmqctl list_queues name arguments -p <vhost>`)
    - `broker_setup.ensure_broker_user(...)` issues PUT `/api/vhosts/smarthealth`, PUT `/api/users/smarthealth_notification` (no admin tag) and PUT `/api/permissions/smarthealth/smarthealth_notification`, is idempotent, never logs the password, and `main()` exits 1 with a clear message when `RABBITMQ_ADMIN_USER`/`RABBITMQ_ADMIN_PASSWORD` or the password file is missing (tested with `httpx.MockTransport`, no live call)
  </behavior>
  <read_first>
    - services/notification/tasks.py (Task 2), services/notification/celery_app.py (queue declarations)
    - services/visit/Dockerfile (image layout to mirror)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 6: `Reject(requeue=False)` only dead-letters with acks_late)
  </read_first>
  <action>
    tasks.py: retries exhausted, `PermanentDeliveryError`, or a `SendRequest` validation error -> `mark_failed` when a delivery exists, count `notifications_dead_lettered_total{error_type=<class name>}`, log `notification.dead_lettered` with type, dedupe key and error type only, then `raise Reject(exc, requeue=False)` so RabbitMQ routes the message through `smarthealth.dlx` to `notifications.dlq`.

    broker_setup.py: `ensure_broker_user(client: httpx.Client, *, vhost, user, password)` with the three PUTs (permissions `.*` on the dedicated vhost only), raising on non-2xx with the status code only; `main()` reads `RABBITMQ_ADMIN_USER`, `RABBITMQ_ADMIN_PASSWORD` from the environment and the user's password from `NOTIFICATION_BROKER_PASSWORD_FILE`, uses `NOTIFICATION_RABBITMQ_MANAGEMENT_URL`, prints `broker user ready` on success. Plan 03-11 runs it as the Compose `rabbitmq-init` job.

    Dockerfile: copy of the visit service Dockerfile pattern (python:3.12-slim, build-essential, `COPY pyproject.toml README.md`, `libs`, `services/__init__.py`, `services/notification`, `pip install --no-cache-dir .`) with the worker command from the artifacts table as CMD; it runs no migrations (no Postgres database). Verify the image builds: `docker build -f services/notification/Dockerfile -t smarthealth-notification:plan-03-04 .` (a local tag; no container is started, nothing else is touched).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/notification -x -q && docker build -q -f services/notification/Dockerfile -t smarthealth-notification:plan-03-04 .</automated>
    <fails_when>pytest exits non-zero, a poison message is requeued instead of dead-lettered, the DLQ stays empty, or the image build exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/notification -q` exits 0 with at least 15 passed
    - `grep -c "requeue=False" services/notification/tasks.py` is at least 1
    - `docker image inspect smarthealth-notification:plan-03-04` exits 0
    - `grep -c "RABBITMQ_ADMIN_PASSWORD" services/notification/broker_setup.py` is at least 1 and no test output contains a generated password (tests assert the captured stdout/stderr lacks it)
  </acceptance_criteria>
  <done>Poison and exhausted sends reach the dead-letter queue, broker provisioning is ready for Compose, and the notification image builds.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| publisher (consumer, Temporal activity) -> RabbitMQ -> Celery worker | task messages are deserialized by the worker |
| worker -> fake provider / Mongo | delivery log and provider-call records |
| notification processes -> broker credentials | dedicated broker user's password |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-15 | Tampering | Celery message deserialization | high | mitigate | `task_serializer="json"`, `accept_content=["json"]` (no pickle); kwargs revalidated by `SendRequest` inside the task; invalid messages dead-lettered, never executed |
| T-3-16 | Information Disclosure | task kwargs, delivery log, logs, DLQ | high | mitigate | Ids, types, dedupe keys and timestamps only; recipient is a patient id (no contact data); no payload text in logs; DLQ holds the same PHI-free kwargs |
| T-3-17 | Elevation of Privilege | RabbitMQ credentials | medium | mitigate | Dedicated non-admin user with permissions on vhost `smarthealth` only; its password from a Compose secret file (0600, gitignored), held as SecretStr; admin credentials used only by the one-shot provisioning job; tests use throwaway vhost/users that are deleted |
| T-3-18 | Repudiation / Tampering | replayed or duplicated sends | medium | mitigate | Unique `dedupe_key` index and the delivery status machine; provider-call records make every real send countable; residual crash window documented |
| T-3-19 | Denial of Service | poison task looping | medium | mitigate | Bounded retries with exponential backoff, `Reject(requeue=False)` into `notifications.dlq`, prefetch 1; quorum queues with `delivery-limit` documented as the production answer |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | Celery, kombu and the Celery instrumentation were approved and installed in plan 03-01 Task 1; nothing new installed here |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/notification -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/notification`; record status in the SUMMARY
</verification>

<success_criteria>
- Celery on RabbitMQ 4.3 delivers through a fake provider once per dedupe key, with retries, a closed-appointment guard and a working DLQ
- The notification image builds; broker provisioning is ready for the Compose job in plan 03-11
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-04-SUMMARY.md` when done
</output>
