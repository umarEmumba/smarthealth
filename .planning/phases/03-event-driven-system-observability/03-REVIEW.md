---
phase: 03-event-driven-system-observability
reviewed: 2026-11-11T12:00:00Z
depth: standard
files_reviewed: 62
files_reviewed_list:
  - libs/events/__init__.py
  - libs/events/catalog.py
  - libs/events/consumer.py
  - libs/events/idempotency.py
  - libs/events/migration.py
  - libs/events/outbox.py
  - libs/events/register.py
  - libs/events/registry.py
  - libs/events/start_offsets.py
  - libs/events/tracing.py
  - libs/events/schemas/*.avsc
  - libs/config.py
  - libs/temporal.py
  - libs/temporal_policies.py
  - libs/observability/http_metrics.py
  - libs/db/postgres.py
  - services/scheduling/events.py
  - services/scheduling/repository/appointment_repo.py
  - services/scheduling/activities/booking.py
  - services/scheduling/app.py
  - services/scheduling/worker.py
  - services/scheduling/migrations/versions/scheduling_0006_outbox.py
  - services/patient/events.py
  - services/patient/repository/patient_repo.py
  - services/patient/app.py
  - services/provider/events.py
  - services/provider/repository/provider_repo.py
  - services/provider/app.py
  - services/visit/events.py
  - services/visit/consumer.py
  - services/visit/downstream.py
  - services/visit/repository/visit_repo.py
  - services/visit/schedules.py
  - services/visit/worker.py
  - services/visit/workflows/sweeper.py
  - services/visit/activities/downstream.py
  - services/visit/api/visits.py
  - services/analytics/api/analytics.py
  - services/analytics/app.py
  - services/analytics/config.py
  - services/analytics/consumer.py
  - services/analytics/projections.py
  - services/analytics/rebuild.py
  - services/analytics/repository/queries.py
  - services/analytics/migrations/versions/analytics_0001_projections.py
  - services/notification/activities.py
  - services/notification/broker_setup.py
  - services/notification/celery_app.py
  - services/notification/config.py
  - services/notification/consumer.py
  - services/notification/contracts.py
  - services/notification/deliveries.py
  - services/notification/provider.py
  - services/notification/reminders.py
  - services/notification/tasks.py
  - services/notification/temporal_worker.py
  - services/notification/triggers.py
  - services/notification/workflows/reminder.py
  - services/gateway/app.py
  - services/gateway/routing.py
  - infra/docker-compose.yml
  - infra/postgres-init/create-service-databases.sh
  - infra/connect/register-connectors.sh
  - infra/connect/connectors/*.json
  - infra/kafka/create-topics.sh
  - infra/prometheus.yml
  - infra/grafana/provisioning/**
  - infra/grafana/dashboards/*.json
  - scripts/backfill_visit_completed_events.py
  - scripts/generate_infra_secrets.py
findings:
  critical: 2
  warning: 7
  info: 6
  total: 15
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-11-11
**Depth:** standard (with targeted cross-file tracing of the consumer, outbox, Debezium and notification paths)
**Files Reviewed:** 62 (118 non-test, non-doc files in scope; the remainder are mechanical repeats such as the four connector JSONs, the Avro schemas and the per-service outbox migrations, spot-checked via diff)
**Status:** issues_found

## Summary

The core delivery design is sound and I could not break it on the paths the phase brief names first: the outbox INSERT shares the caller's transaction at every emit site (`confirm_booking`, `cancel_appointment`, `swap_slot`, check-in, complete, no-show, patient create and deactivate, schedule change, visit complete), event ids are uuid5 of `aggregate:type:discriminator` and the discriminator (`appointments.version`) is bumped by every applied transition, the Avro schemas carry ids and timestamps only (no PHI field exists to be serialized), the consumer commits only after the effect / a confirmed DLQ write, the analytics claim and projection share one transaction, the reminder workflow uses only `workflow.now()`, `wait_condition` and `execute_activity`, the analytics read API scopes by clinic for non-admin roles, and no new host ports were published. I found no per-call engine or connection creation in the new service code (consumers use one persistent `asyncio.Runner` and the loop-keyed registry; `rebuild.py` uses `NullPool` plus `dispose()`; the backfill script runs one loop and exits).

The weaknesses are at the edges the design chose not to cover: (1) the retired sweeper left `visit.completed` as the only downstream trigger while database-connection failures are misclassified as poison messages, and nothing can recover a dead-lettered event; (2) every HTTP service runs two gunicorn workers with no Prometheus multiprocess mode, so the HTTP/NFR dashboards read a random half of the traffic and `rate()` sees counter resets; (3) a handful of classification, privilege and robustness issues listed below.

## Critical Issues

### CR-01: A dead-lettered `visit.completed` event can never be recovered; DB connection failures are classified as poison and dead-lettered

**File:** `services/visit/consumer.py:101-150`, `libs/events/consumer.py:320-326`, `services/visit/repository/visit_repo.py` (`list_unsettled`, `downstream_started_at IS NOT NULL`), `scripts/backfill_visit_completed_events.py:121-123`
**Issue:** Plan 03-12 removed the only safety net for a lost downstream start, so after `visit.completed` is dead-lettered nothing ever starts the workflow:
- the sweeper Schedule and the start-up sweep are gone;
- the reconciler only selects visits with `downstream_started_at IS NOT NULL`, so it skips this visit;
- the backfill script treats a visit whose outbox row already exists as `already_queued` and writes nothing;
- there is no DLQ replay tool anywhere in `scripts/` or the services.

What gets a healthy event dead-lettered: `_is_transient` in the visit consumer (and in the analytics consumer) only recognises `sqlalchemy.exc.OperationalError`, `InterfaceError`, `connection_invalidated`, `TimeoutError`, `ConnectionError` and `OSError`. The asyncpg dialect (`sqlalchemy/dialects/postgresql/asyncpg.py:_asyncpg_error_translate`) maps every other `asyncpg.PostgresError` to the generic `Error`, so `TooManyConnectionsError` (SQLSTATE 53300), `CannotConnectNowError` (57P03, server starting or shutting down) and `DeadlockDetectedError` (40P01) surface as a plain `DBAPIError` (or a raw asyncpg exception when it is raised while opening the connection). None is transient by this test, so the base consumer retries three times over about 1.5 s and then writes the event to `smarthealth.dlq.visit-downstream` and commits.
**Failure scenario:** Dev Postgres hits `max_connections` (already observed at 300 during the full test run) or restarts while a visit is being completed. The consumer gets `TooManyConnectionsError` on `factory()`, retries three times inside two seconds, dead-letters the event and moves on. The visit stays `completed` with `downstream_started_at IS NULL` for ever: billing is never finalized, the appointment is never completed, no follow-up intent is recorded. D-03 allowed removing the sweeper only because the replacement was proven by kill tests; those tests do not cover the DLQ branch.
**Fix:** Classify by SQLSTATE rather than by SQLAlchemy exception class, and give the dead-letter path a recovery route.
```python
_TRANSIENT_SQLSTATE_CLASSES = ("08", "53", "57", "40")  # connection, resources, operator intervention, rollback

def _is_transient(exc: BaseException) -> bool:
    code = getattr(exc, "sqlstate", None) or getattr(getattr(exc, "orig", None), "sqlstate", None)
    if isinstance(code, str) and code.startswith(_TRANSIENT_SQLSTATE_CLASSES):
        return True
    ...  # existing checks, plus isinstance(exc, asyncpg.PostgresError) with those classes
```
Put it in one shared helper (see IN-01). In addition, either extend `list_unsettled` (or add a small reconciler query) to also pick `status='completed' AND downstream_started_at IS NULL AND completed_at < now() - grace` and call `ensure_downstream_started` (idempotent), or ship a DLQ replay script that re-produces a dead-lettered record to its source topic. D-03 explicitly said to keep the other safety nets; this one needs a replacement.

### CR-02: Prometheus HTTP metrics are wrong for every gunicorn-served service (two workers, no multiprocess mode)

**File:** `services/*/Dockerfile` (`gunicorn ... -w ${WEB_CONCURRENCY:-2}`), `libs/observability/http_metrics.py:31-35`, `infra/prometheus.yml`, `infra/grafana/dashboards/smarthealth-booking-nfr.json` (all `http_request_duration_seconds` / `http_requests_total` panels)
**Issue:** Every API container, including the gateway, runs two gunicorn worker processes. `prometheus_client` keeps one in-memory registry per process, and `PROMETHEUS_MULTIPROC_DIR` is not set anywhere in the repository (confirmed by grep over `libs`, `services`, `infra`, `scripts` and `README.md`; `prometheus_fastapi_instrumentator` only switches to `MultiProcessCollector` when that variable exists, `instrumentation.py:165,290`). Each scrape of `/metrics` is answered by whichever worker accepts the connection.
**Failure scenario:** During the post-phase `sustained` / `burst` run, consecutive scrapes alternate between worker A (for example 1,200 requests) and worker B (800). `rate()` treats each downward step as a counter reset and each upward step as a jump, so the request rate, the 5xx ratio on the gateway panel and the `histogram_quantile` p95/p99 on the NFR dashboard are all unreliable, and `outbox_insert_seconds` from the patient and provider APIs has the same problem. The Phase 3 deliverable for D-04 and OBS-01 (measuring NFR-03/04 afterwards) therefore cannot be trusted. The Temporal workers and the consumers are single-process and are not affected.
**Fix:** Set the multiprocess directory in the image or in Compose and expose from the multiprocess collector; clean it at start.
```dockerfile
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prom-multiproc
CMD ["sh", "-c", "rm -rf $PROMETHEUS_MULTIPROC_DIR && mkdir -p $PROMETHEUS_MULTIPROC_DIR && exec gunicorn ... -c gunicorn_conf.py ..."]
```
with `child_exit` in a `gunicorn_conf.py` calling `multiprocess.mark_process_dead(worker.pid)`. For the gateway, whose `/metrics` is a hand-written route using `generate_latest()`, build the response from a `CollectorRegistry` with `MultiProcessCollector`. The cheaper alternative is `WEB_CONCURRENCY=1` for the services that Prometheus scrapes, which also removes the connection-budget pressure in WR-05.

## Warnings

### WR-01: The reminder workflow drops a `reschedule` signal that arrives while the last lead's enqueue activity is running

**File:** `services/notification/workflows/reminder.py:93-135`
**Issue:** `_new_slot_iso` is only consulted at the top of each lead iteration and right after `wait_condition`. After `await workflow.execute_activity(ACT_ENQUEUE, ...)` for the final lead the loop ends with `moved == False`, falls to `break`, and returns `COMPLETED` while `self._new_slot_iso` is still set. A `cancel` signal is re-checked after the loop (line 133) but a reschedule is not.
**Failure scenario:** The 2-hour reminder fires and the enqueue activity retries because RabbitMQ is briefly unreachable (the activity is retried with the shared forward policy, so the window can be minutes). The patient's appointment is rescheduled during that time: the consumer sends `reschedule_reminder` (signal-with-start, `USE_EXISTING`), which signals the still-running workflow. When the activity finishes the workflow completes and the signal is discarded. The new slot never gets reminders, and no new run is started because the signal was accepted by the old one.
**Fix:** Treat a pending reschedule like `moved` after the lead loop.
```python
if moved or self._new_slot_iso is not None:
    slot_iso, self._new_slot_iso = self._new_slot_iso, None
    sent = []
    continue
break
```
Because this changes the command sequence for in-flight histories, gate it with `workflow.patched("reminder-pending-reschedule-v1")` and add a recorded-history replay test, as the module docstring requires.

### WR-02: Analytics projection permanently loses events on database connection errors; the consumer base also dead-letters retryable Schema Registry responses

**File:** `services/analytics/consumer.py:81-87`, `libs/events/consumer.py:347-351`
**Issue:** Same root cause as CR-01 in the analytics consumer: `TooManyConnectionsError`, `CannotConnectNowError` and deadlocks are not classified transient and are dead-lettered after three quick attempts. Separately, `_decode` treats every 4xx Schema Registry status as a poison message (`400 <= status < 500`), so a 408 or 429 from an overloaded registry dead-letters a perfectly valid event.
**Failure scenario:** During a Postgres connection spike the analytics consumer dead-letters a `patient.registered` or `visit.completed` event. The projection undercounts (total patients, completed visits, wait time) until an operator runs `services.analytics.rebuild`, which also replays any throw-away events still retained. Nothing alerts on the loss beyond the `dlq_events_total` counter.
**Fix:** Use the shared SQLSTATE-based helper from CR-01, and exempt 408 and 429 (and 5xx) from the poison path: `if status in (408, 429): raise _TransientDecodeError`.

### WR-03: Over-broad "transient" classification can block a partition for ever with no cap

**File:** `services/notification/triggers.py:69-104`, `services/visit/consumer.py:78-109`, `libs/events/consumer.py:392-402`
**Issue:** `RuntimeError`, `OSError` and the Temporal statuses `UNKNOWN`, `INTERNAL`, `ABORTED` and `CANCELLED` are all mapped to `TransientEventError`, which by design is never dead-lettered and never counted against a limit (`_transient_counts` only drives the backoff). Any deterministic bug that surfaces as a `RuntimeError`, or a Temporal `INTERNAL` error for one malformed input, therefore retries the same offset every 30 s for ever.
**Failure scenario:** One `appointment.booked` event produces a `RuntimeError` inside the Temporal client while building the workflow input (for example a closed-loop error after a code change). The partition holding that appointment's key is stuck behind it; every later booking, cancellation and reminder event on that partition is delayed indefinitely, and the only signal is a rising `kafka_consumer_lag`.
**Fix:** Cap transient retries per message (for example after N consecutive failures spanning more than 15 minutes) and then dead-letter with `error_type="TransientExhausted"`, or narrow the transient set to real infrastructure types (`RPCError` with `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED`, `ConnectionError`, `TimeoutError`) and drop the bare `RuntimeError` / `OSError` / `UNKNOWN` / `INTERNAL`.

### WR-04: The "least-privileged" `debezium` role can decode the whole patient database, and the Connect REST API plus an unrestricted FileConfigProvider make its password extractable

**File:** `infra/postgres-init/create-service-databases.sh:89-93`, `libs/events/migration.py:5-10`, `infra/docker-compose.yml` (`connect`: `CONNECT_CONFIG_PROVIDERS_FILE_CLASS`, no `allowed.paths`)
**Issue:** The role is created `LOGIN REPLICATION` and granted `CONNECT` on `patient_db`, `scheduling_db`, `visit_db` and `provider_db`. The design relies on "SELECT on the outbox only", but a REPLICATION role can open a logical replication connection and create its own slot with any installed output plugin (`test_decoding` ships with the official image), which streams every row change in the database regardless of table grants. `patient_db` holds the PHI (names, date of birth, phone, address). In addition, the Connect REST API (`:8083`, internal network, no authentication) accepts a connector whose `database.hostname` is attacker-chosen and whose password is `${file:/run/secrets/debezium.properties:password}`; `FileConfigProvider` has no `allowed.paths`, so any process on the Compose network can make Connect send that credential to a host it controls, then use it as above. The legacy `smarthealth` database (left untouched by db-init) and `postgres` / `template1` also keep PUBLIC `CONNECT`.
**Failure scenario:** A compromised container on the Compose network (for example through the gateway-facing services) registers a connector aimed at itself, captures the Debezium password, connects to `patient_db` with `replication=database`, creates a `test_decoding` slot and reads every patient insert and update. This contradicts the stated PHI-handling rule and the "outbox only" claim in the migration docstring.
**Fix:** Restrict the config provider (`CONNECT_CONFIG_PROVIDERS_FILE_ALLOWED_PATHS=/run/secrets/debezium.properties`), put basic auth or a network policy in front of Connect REST (or bind it to the Connect container only and register connectors through `docker exec`), `REVOKE CONNECT ... FROM PUBLIC` on `smarthealth`, `postgres` and `template1`, and correct the docstring. A stronger design change (outbox in a separate database or schema that holds no PHI) is worth recording as the production path.

### WR-05: Connection budget can exceed `max_connections=300`; the loop-keyed engine registry orphans engines instead of disposing them

**File:** `libs/db/postgres.py:43-57`, `infra/docker-compose.yml` (all `gunicorn -w 2` services), `libs/events/consumer.py` (one connection per consumer is enough)
**Issue:** Each DB-using process gets `create_async_engine(url, pool_pre_ping=True)` with the default pool (5 + 10 overflow = 15). There are 8 APIs at 2 workers each, the scheduling worker, the visit worker, the visit consumer and the analytics consumer (about 20 processes, a 300-connection ceiling on their own), plus roughly 40 for Temporal, four Debezium replication connections and Grafana's 5. Phase 3 added the analytics API and consumer, the visit consumer and the four walsenders to a budget that was already described as exhausted by a normal run. Separately, `_entry_for` replaces the registry entry whenever the running loop differs and never calls `dispose()` on the replaced engine, so per-test event loops leave pools of connections attached to closed loops until garbage collection; that is the most likely source of the 300-connection exhaustion seen during the full test run (the file predates Phase 3, but the new loop-per-test consumer and projection tests exercise it heavily).
**Failure scenario:** Under the `burst` profile, 16 API workers each open up to 15 connections while Debezium and Grafana hold more; new connections fail with `TooManyConnectionsError`, which then surfaces as CR-01 / WR-02 in the consumers.
**Fix:** Make pool size a setting and set it small for single-flow processes (`pool_size=2, max_overflow=2` for consumers and workers), keep `WEB_CONCURRENCY` consistent with `max_connections`, and in `_entry_for` schedule `await old.engine.dispose()` (or `old.engine.sync_engine.dispose(close=False)` when the old loop is already closed) before replacing an entry.

### WR-06: Backfilling or replaying `visit.completed` sends follow-up notifications for arbitrarily old visits

**File:** `services/notification/triggers.py:317-335`, `scripts/backfill_visit_completed_events.py:126`, `services/notification/config.py:79-81`
**Issue:** `on_visit_completed` has no staleness guard (the `max_event_age_minutes` check applies only to booked, cancelled and rescheduled). `FollowUpReminderWorkflow` fires immediately when `completed_at + delay` is already in the past. The backfill deliberately writes events with `occurred_at = completed_at` of visits that completed before the cutover, and the notification-trigger group is pinned to `latest`, so it consumes them. The code default for `NOTIFICATION_CONSUMER_START` is also `earliest`, so any deployment that does not set it (or a group reset for replay) re-sends history.
**Failure scenario:** An operator runs `backfill_visit_completed_events.py --apply` to repair visits whose downstream start was lost. For each such visit a `visit.followup` send is enqueued immediately, to patients whose visit completed days or weeks ago. The script's CAUTION block warns about workflows and analytics but not about notifications.
**Fix:** In `on_visit_completed`, skip (record `skipped_stale`) when `now - completed_at > max_event_age_minutes`, mirroring the booking handlers, and mention notifications in the backfill caution. Consider defaulting `consumer_start` to `latest` outside tests.

### WR-07: A failed offset commit terminates the consumer process

**File:** `libs/events/consumer.py:213-243, 389-390`
**Issue:** `_commit` calls `Consumer.commit(message=..., asynchronous=False)` and lets `KafkaException` propagate (for example `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR` after a broker restart, or `ILLEGAL_GENERATION` after a rebalance). Nothing in `poll_once` or `run` catches it, so the loop exits, `close()` runs and the container restarts; the same happens for `DeadLetterWriteError`.
**Failure scenario:** The Kafka broker restarts (or the coordinator moves) while the effect of an event has just completed. The commit raises, all three consumers crash within seconds of each other, and each restart re-runs its start-up (`warm_up`, `pin_start_offsets`, Temporal connect) and then re-handles the event. At-least-once is preserved, but every broker blip becomes a crash loop that the heartbeat healthcheck reports as unhealthy for up to 40 s.
**Fix:** Retry the commit a few times with `_sleep` backoff for retryable error codes, and treat a non-retryable commit failure after a handled message as "log and continue" (the next successful commit covers it, and the event is idempotent on redelivery); keep raising only for `error.fatal()`.

## Info

### IN-01: Failure classification is copy-pasted three times and has already drifted
**File:** `services/notification/triggers.py:97-104`, `services/visit/consumer.py:101-109`, `services/analytics/consumer.py:81-87`
**Issue:** Three similar `_is_transient` functions and two copies of `_TRANSIENT_RPC_STATUSES`; the analytics version lacks the Temporal branch and the notification version lacks the SQLAlchemy branch. The fixes for CR-01, WR-02 and WR-03 should land once.
**Fix:** One `libs/events/classify.py` with `is_transient_db`, `is_transient_temporal` and `is_transient_broker`, imported by the three consumers.

### IN-02: Duplicate code in the notification delivery log and task
**File:** `services/notification/deliveries.py:152-200`, `services/notification/tasks.py:200-208`
**Issue:** `mark_skipped` and `record_skipped` build the same document and differ only in the duplicate-key branch; `send()` has two `except` clauses (`PermanentDeliveryError`, `Exception`) with identical bodies.
**Fix:** Extract `_skipped_document(request, status, now)`; merge the identical `except` clauses.

### IN-03: Metric cardinality and counter semantics in the consumer base
**File:** `libs/events/consumer.py:287-290, 506-517`
**Issue:** `events_consumed_total` takes its `event_type` label from the decoded payload for ignored events, so a producer with Kafka access (the broker has no authentication) can create unbounded series; the counter also increments on every `RETRY_LATER` pass, so a long outage inflates `rate(events_consumed_total)`.
**Fix:** Collapse unknown types to `"other"` before labelling; count retries in a separate counter.

### IN-04: Operational hygiene items in Compose and RabbitMQ provisioning
**File:** `infra/docker-compose.yml:750-766`, `services/notification/celery_app.py:48-65`
**Issue:** The `rabbitmq-init` job receives the broker administrator password as an environment value (visible through `docker inspect`) although the surrounding comments state broker credentials never travel in environment variables (only the notification user's password is a secret). The dead-letter queue `notifications.dlq` has no length or TTL limit and no consumer, so poison messages accumulate without bound.
**Fix:** Mount the admin password as a Compose secret too; add `x-max-length` / `x-message-ttl` (or a documented drain procedure) on the DLQ.

### IN-05: Metrics servers record "started" even when the bind failed
**File:** `libs/temporal.py:93-100`, `services/notification/celery_app.py:129-137`
**Issue:** On `OSError` the helpers log a warning and still set the `started` flag, so a later call cannot retry; the target silently shows as down in Prometheus.
**Fix:** Set the flag only after `start_http_server` succeeds, or fail the worker start when the port is required for the scrape job.

### IN-06: Slot invalidation and Grafana scoping notes
**File:** `infra/docker-compose.yml:64-76`, `infra/connect/connectors/*-outbox.json` (`snapshot.mode: initial`), `infra/grafana/dashboards/smarthealth-operations.json` (variable `clinic`)
**Issue:** `max_slot_wal_keep_size=2GB` can invalidate a slot after a long Connect outage; with `snapshot.mode=initial` and stored offsets the connector then fails on restart and needs a manual re-snapshot (no outbox row is lost, but relay stops until an operator acts, and the docs should say so). Separately, the Grafana operations dashboard lists every clinic from `analytics_db`, so any Grafana user sees all clinics' aggregates regardless of the API's clinic scoping, and the admin password defaults to `changeme-local-dev-only`.
**Fix:** Document the recovery step (or use `snapshot.mode=when_needed`, accepting a full idempotent replay); state that Grafana is an operator-only surface and set a non-default admin password outside local dev.

---

_Reviewed: 2026-11-11_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
