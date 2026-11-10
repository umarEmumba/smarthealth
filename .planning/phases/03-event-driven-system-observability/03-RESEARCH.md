# Phase 3: Event-Driven System + Observability - Research

**Researched:** 2026-11-10
**Domain:** Transactional outbox + CDC (Debezium) -> Kafka/Avro/Schema Registry -> idempotent Python consumers -> Celery/RabbitMQ delivery + Temporal reminder timers; analytics projections; OpenTelemetry/Prometheus/Grafana
**Confidence:** HIGH on the CDC/Avro/Celery/Temporal mechanics (each proven by an isolated spike in this session, see "Spike evidence"); MEDIUM on Grafana dashboard JSON details and sizing numbers.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**User-confirmed (2026-11-10)**
- **D-01:** The transactional outbox is relayed to Kafka by **CDC (Debezium reading the Postgres WAL), not by a polling publisher.** Services only insert an outbox row in the same transaction as the state change; no service-side relay loop. — **Reversibility:** costly — needs `wal_level=logical`, replication slots, and a Kafka Connect service in Compose; switching to a poller later means writing a relay and removing the connector config. Note: Compose Postgres currently runs with default `wal_level` (infra/docker-compose.yml sets only `max_connections=300`), and Scheduling/Visit/Billing each have their own database, so the plan must cover the config change, a connector per outbox database (or one connector instance with several), and what the existing data volume needs.
- **D-02:** **Appointment reminders (and follow-up reminders) are scheduled with Temporal** (durable timers / workflows), not Celery ETA or beat. Delivery of the actual send still goes through Celery + RabbitMQ workers (see D-09) because ROADMAP success criterion 2 and EVENT-04 require background delivery on Celery. — **Reversibility:** reversible — only the scheduling layer changes; delivery tasks stay the same.
- **D-03:** **Retire the visit sweeper.** The visit-completion downstream trigger moves to the outbox: `visit.completed` is committed with the transition and consumed to start the downstream work, replacing the `visit-downstream-sweeper` Temporal Schedule, its run-on-start pass, and the `downstream_started_at IS NULL` marker's sweeping role (02-11, D-16). The other interim mechanisms (hold reaper, pending-starter, scheduling and visit reconcilers) are NOT retired by this decision; keep them as safety nets unless planning shows one is fully subsumed, and record the reason in the docs. — **Reversibility:** costly — removes a safety net that Phase 2 verification relied on; the replacement must be proven with a kill-between-commit-and-publish test.
- **D-04:** **NFR measurement happens after Phase 3** (k6 `sustained`, `burst`, `contention` run afterwards on the finished stack). While building, keep NFR-01..08 in mind: no extra synchronous work on the booking accept path (the outbox insert is the only addition), saga completion stays within NFR-05, and the `smoke` profile must still pass after the Phase 3 changes (it passed 2026-11-10: accept p95 21.8 ms, saga p95 0.52 s). Add Prometheus histograms on the NFR-relevant paths so Phase 3 observability can measure them later. — **Reversibility:** reversible.

**Auto-defaults (`[auto-default]`, chosen by Claude, not user-confirmed; overrule by editing CONTEXT.md)**
- **D-05 `[auto-default]`:** Event payloads are **Avro with Confluent Schema Registry** (stack docs already point at `confluent_kafka.schema_registry`); compatibility mode BACKWARD; one subject per event type. Registration happens in code/CI, not by hand. — **Reversibility:** one-way — a registered subject and consumers depend on it; changing format later needs migration.
- **D-06 `[auto-default]`:** **One outbox table per service, in that service's own database** (no cross-database writes, matching the Phase 1.1 database-per-service rule). Debezium's outbox event router publishes to topics by event type; message key = aggregate id (appointment id, visit id) so per-aggregate order is preserved. Rows carry event id (UUID, the consumer idempotency key), type, aggregate id, occurred_at, schema version and the W3C `traceparent` of the producing request.
- **D-07 `[auto-default]`:** Event set: appointment booked / cancelled / rescheduled, appointment checked_in / completed / no_show, provider schedule changed, visit completed, plus what analytics needs (patient registered). Payloads are **PHI-free: ids, types, timestamps only** (same rule as logs and audit).
- **D-08 `[auto-default]`:** **Consumers are idempotent** via a processed-event table keyed on event id, written in the same transaction as the consumer's effect; consumer groups are one per role (analytics, notification-trigger, visit-downstream), each with a dead-letter topic.
- **D-09 `[auto-default]`:** **Notification delivery:** a Kafka consumer turns notification-worthy events into Celery tasks on RabbitMQ (queues per type, manual ack, `acks_late`, exponential retry, dead-letter exchange for poison messages). The Temporal reminder workflow (D-02) enqueues the same Celery send task when its timer fires. The channel is a **fake provider** that records sends (no real SMS/email). Dedupe key = (notification type, appointment id, event id) so a retry never double-sends. The Phase 2 stub (decision D-11 in 02-CONTEXT.md, the notification activity called from `services/scheduling/workflows/`) is the single swap point; workflows must not change shape.
- **D-10 `[auto-default]`:** **Analytics projections** live in a small relational projection store (Postgres, own database) updated by an event consumer; the five metrics (total patients, appointments over time, completed visits, cancellation rate, average wait time) are served by a read-only analytics API and graphed in Grafana. Average wait time is defined as appointment start time minus check-in time (actual wait in clinic); document the definition. Projections can be rebuilt by replaying the topics.
- **D-11 `[auto-default]`:** **Tracing:** `traceparent` travels in the outbox row, Kafka headers and Celery headers so one booking is one connected trace in Jaeger (HTTP -> Temporal -> Kafka -> Celery). **Metrics:** every service exposes `/metrics`; Grafana dashboards are provisioned from files in the repo, not built by hand. **Logs:** structlog JSON with trace ids for booking, availability sync, billing, notifications and workers; PHI-free.
- **D-12 `[auto-default]`:** Close out the dashboard (DASH-03/04): event-flow diagrams for booking, cancel/reschedule, visit lifecycle, notifications and analytics, the "why CDC outbox" and "why Temporal reminders" reasoning, and an updated evaluation Q&A. Docs state only what the code does; interim mechanisms stay labelled.

### Claude's Discretion
- Whether the S3 follow-up (post-commit audit writes that are best effort today, 02.1-VERIFICATION known limitation 1) moves into the outbox in this phase; it is the natural fix but must not widen the booking accept path.
- Debezium and Kafka Connect image and version pins (must be compatible with the pinned `cp-kafka:7.6.13` and `cp-schema-registry:7.6.13`), connector config details, topic partition counts, retention.
- Celery queue layout and worker concurrency.

### Deferred Ideas (OUT OF SCOPE)
- k6 `sustained`, `burst` and `contention` runs and NFR-01..08 measurement: after Phase 3 (user decision 2026-11-10).
- Temporal poller `scheduling-schedule-change-poller` is paused on purpose; decide when to unpause (user decision, 2026-11-10).
- Dockerfiles run as root (Semgrep missing-user x8): production hardening, not in this phase's scope.
- SCHEDX-01/02 (v2).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| EVENT-01 | Domain events published to Kafka with a registered schema | Avro schemas as `.avsc` files, registered by a `schema-init` job; strict serializer (`auto.register.schemas=False`); one topic per aggregate (Pattern 1, 2) |
| EVENT-02 | Transactional outbox, no dual write | Outbox row inserted by the same `AsyncSession` as the state change; Debezium relays (verified in spike, incl. Connect-down commit then delivery) |
| EVENT-03 | Idempotent consumers, duplicate-safe | Debezium is at-least-once on the wire (verified: `docker kill` re-emitted 5 events); processed-event table / deterministic workflow ids / dedupe key (Pattern 4) |
| EVENT-04 | Background tasks on Celery + RabbitMQ | Celery 5.6.3 + RabbitMQ 4.3 working configuration (Pattern 6), threads pool |
| EVENT-05 | Retries + dead-letter path | `acks_late` + `self.retry` + `Reject(requeue=False)` -> DLX -> `notifications.dlq` (verified in spike); Kafka DLQ topics per group |
| NOTIF-01 | Booking/cancel/reschedule notify asynchronously | Event-driven only (outbox -> consumer -> Celery); workflows unchanged (Pattern 6, Finding F5) |
| NOTIF-02 | Reminders scheduled and delivered ahead | `ReminderWorkflow` durable timer, start-with-signal idempotency (Pattern 7, verified in time-skipping spike) |
| NOTIF-03 | Visit completion schedules a follow-up reminder | `FollowUpReminderWorkflow` started by the notification consumer on `visit.completed` (Pattern 7) |
| NOTIF-04 | Retry without double-send | Mongo `notification_deliveries` unique index on `dedupe_key` + status machine (Pattern 6) |
| ANLY-01..05 | Five metrics | Projection tables + SQL views; Grafana PostgreSQL datasource + read-only API (Pattern 9) |
| ANLY-06 | Derived from the event stream, not polling the OLTP DB | Analytics consumer only reads Kafka; analytics DB is its own database |
| OBS-01 | Prometheus metrics for all services in Grafana | Existing `/metrics`; add worker/consumer endpoints, scrape jobs, file-provisioned dashboards (Pattern 10) |
| OBS-02 | One connected trace across async boundaries | traceparent column -> Kafka header (verified) -> manual extraction as PARENT (OTel confluent-kafka instrumentation only LINKS: Pitfall 6) -> Celery instrumentation (verified) |
| OBS-03 | Structured logs for booking, availability sync, billing, notifications, workers | Reuse `libs/observability/logging.py`; new processes call `configure_logging()` + `configure_tracer_provider(...)` |
| DASH-03 | Dashboard shows data/event flows | Event-flow content derived from this document's diagram; `tests/docs/test_dashboard.py` extended |
| DASH-04 | Evaluation Q&A updated at the phase boundary | Q&A prompts listed under Open Questions / Findings (why CDC, why Temporal reminders, at-least-once honesty) |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

Extracted from `/Users/emumba/smart Health/.claude/CLAUDE.md` and `/Users/emumba/.claude/CLAUDE.md` (global Atlas rules). Treat as locked:

- Mandated stack: Python 3.12, FastAPI, PostgreSQL, a NoSQL DB, Redis, **Celery + RabbitMQ**, **Kafka + Schema Registry**, Temporal; Docker Compose; Prometheus + Grafana, Jaeger, OpenTelemetry. Deviations must be justified (this research justifies the Debezium Connect image deviation, Finding F1).
- Celery is for discrete background jobs (notification sends, reminders sends, follow-ups), **NOT the booking saga** (Temporal's job).
- confluent-kafka (not aiokafka). Consumers are **single-purpose sync `poll()` loops in their own process, never in the `api` process**; producing from request paths via callback pattern or executor.
- KRaft Kafka, no ZooKeeper. **Pin `cp-kafka` and `cp-schema-registry` to the identical CP version** (currently both 7.6.13).
- Export traces via OTLP only (never the legacy Jaeger exporter). Pin OTel contrib packages exactly.
- Never pass raw Pydantic models as Celery task args: serialize to dict/JSON and revalidate in the task.
- Do not use Motor, `aioredis`, sync `psycopg2` in async routes.
- `temporalio/auto-setup` is non-production: label it in docs.
- Use structlog JSON logs with trace ids; **never log PHI** (ids only); PHI-free events.
- GSD workflow enforcement: file edits happen through a GSD command.
- Global Atlas rules: never push to main/master, never expose secrets, never log PHI, never weaken auth, document Semgrep status for BE changes, code-review gate before finish, destructive DB changes need a rollback script.
- Repo convention (STATE.md, 01.1-04 and 02-06): every new third-party package needs a blocking human-approval checkpoint with **exact pins** (see Package Legitimacy Audit).

## Summary

CDC via Debezium works in this Compose stack, but not with the obvious image. `confluentinc/cp-kafka-connect:7.6.13` runs OpenJDK 11 and the Debezium 3.x connector fails to load there (`UnsupportedClassVersionError ... class file version 61.0 ... only recognizes up to 55.0`). The working choice is the official `quay.io/debezium/connect:3.7.1.Final` image (Java 25, Kafka Connect 4.3.1), which connected to the unchanged `cp-kafka:7.6.13` broker and ran the Outbox `EventRouter` end to end. It needs no Confluent converter, because the Avro bytes are produced inside the Python services (Avro `AvroSerializer` with `auto.register.schemas=False`, stored in a `bytea` `payload` column) and Debezium passes them through with `BinaryDataConverter`. That is the simplest approach that really works with Avro + Schema Registry; Debezium cannot produce controlled, BACKWARD-evolvable Avro schemas from a JSON payload column.

Four facts shape the plan. (1) Debezium is at-least-once on the wire: a hard-killed Connect re-emitted every event since its last offset flush (6 -> 11 -> 16 messages in the spike), so "never duplicates" holds at the effect level through idempotent consumers, not in the topic. (2) Raising `wal_level` from `replica` to `logical` needs a Postgres restart but keeps the existing data volume intact (verified by recreating the container on the same volume). (3) The OpenTelemetry confluent-kafka instrumentation links, it does not parent, so a single HTTP -> Temporal -> Kafka -> Celery trace requires about 30 lines of manual extraction in the consumer; do not add that package. (4) Celery 5.6.3 cannot start on RabbitMQ 4.3 with defaults (`transient_nonexcl_queues` is refused); it needs remote control, events, mingle, gossip and heartbeat disabled, after which acks_late + retry + `Reject(requeue=False)` dead-letters into a DLQ queue (verified).

**Primary recommendation:** Add an `outbox` table (Avro `bytea` payload, `traceparent`, deterministic UUIDv5 event id) to the scheduling, visit, provider and patient databases; relay with four Debezium connectors on `quay.io/debezium/connect:3.7.1.Final` (`snapshot.mode=initial`, insert-only table kept as the replay source); publish one topic per aggregate (not per event type) so per-aggregate order survives; consume with three sync confluent-kafka processes (visit-downstream, notification-trigger, analytics) that start Temporal workflows with deterministic ids, enqueue Celery tasks with a dedupe key, or upsert projections with a same-transaction processed-event table.

## Findings that flag or refine CONTEXT.md decisions

None of these silently overrides D-01..D-04. D-05..D-12 are `[auto-default]`; F2..F7 propose changes the user can accept or reject in plan-phase.

| # | Finding | Evidence | Recommendation |
|---|---------|----------|----------------|
| F1 | D-01 is feasible, but `cp-kafka-connect:7.6.13` cannot host Debezium 3.x (Java 11). CLAUDE.md's "same CP line" rule is about broker vs Schema Registry; Connect is only a Kafka client | Spike: `UnsupportedClassVersionError ... 61.0 ... 55.0` on cp-kafka-connect 7.6.13 `[VERIFIED: spike p3spike, this session]`; Debezium 3.7/3.6 require Java 17+ `[CITED: debezium.io/releases/]`; `quay.io/debezium/connect:3.7.1.Final` (Kafka Connect 4.3.1, Java 25, arm64) ran against `cp-kafka:7.6.13` `[VERIFIED: spike]` | Use `quay.io/debezium/connect:3.7.1.Final`; record the deviation and its reason in docs. Do NOT use Debezium 2.x on cp-kafka-connect (EOL line) |
| F2 | D-05 Avro: only service-side serialization works cleanly. Debezium cannot generate registered, evolvable Avro from the outbox JSON payload; table.expand.json.payload infers a schema | Debezium docs: binary payload must be `bytea` + `BinaryDataConverter` `[CITED: debezium.io/documentation/reference/stable/transformations/outbox-event-router.html]`; round trip proven `[VERIFIED: spike]` | Python services serialize (strict mode), Debezium forwards bytes. Cost: Schema Registry is a (cached) dependency of the write path, see Pitfall 8 |
| F3 | D-06 says topics by event type AND per-aggregate order. Kafka orders only within one topic partition, so booked/cancelled in different topics are unordered relative to each other | Kafka semantics; reminders/analytics need booked-before-cancelled | One topic per aggregate (`smarthealth.appointment.events`, `.visit.`, `.provider.`, `.patient.`), key = aggregate id; event type in a header and in the Avro record name (subject `com.smarthealth.events.AppointmentBooked`, `record_subject_name_strategy`, verified). Keeps "one subject per event type" (D-05) |
| F4 | "Never loses or duplicates" (roadmap SC1) cannot hold on the wire with CDC: hard kill of Connect re-emitted events | `docker kill` test: 6 -> 11 (5 new) -> 16 (the same 5 re-emitted) `[VERIFIED: spike]` | Define SC1 as: no loss, and at-least-once with effect-level exactly-once. The test asserts one effect after a forced re-delivery. State this honestly in docs |
| F5 | D-09 asks for both "consumer turns events into Celery tasks" and "the Phase 2 stub is the single swap point". Doing both double-sends (different dedupe keys) | `schedule_notification`, `schedule_change_notification`, `_request_notification` are intent-only (log + audit) `[VERIFIED: services/scheduling/activities/booking.py:341-359]` | Delivery is event-driven only. Leave the activities registered and unchanged (histories already recorded reference them; removing a name breaks replay), keep workflows unchanged, relabel the stubs as "intent record" in docs. No patch needed |
| F6 | D-03: the Temporal Schedule `visit-downstream-sweeper` already exists on the server and `ensure_visit_schedules` leaves an existing Schedule alone; removing the code does not stop it. Completed visits with `downstream_started_at IS NULL` at cutover have no outbox row | `schedules.py` docstring: "An existing Schedule is left as it is" `[VERIFIED: services/visit/schedules.py:7-9]` | Plan must (a) delete the Schedule idempotently on worker start, (b) keep the class registered until that is deployed, (c) backfill or document the cutover set. See Runtime State Inventory |
| F7 | D-10 definition "appointment start minus check-in" is zero or negative for on-time/late arrivals; it measures early arrival, not time waited | Definition as written in CONTEXT.md | Implement D-10 literally (clamped at 0 is a design choice to confirm) and carry `slot_start` + `checked_in_at` in the event so the better definition (visit `started_at` - `checked_in_at`, needs an optional `visit.started` event) can be added by replay. Ask the user (Open Question 1) |
| F8 | D-08 "processed-event table in the same transaction as the effect" is only exact when the effect is a database write (analytics). Celery enqueue and Temporal start are not in that transaction | Architecture | Three idempotency flavors (Pattern 4): DB effect -> processed-event table; Temporal start -> deterministic workflow id (+ REJECT_DUPLICATE); Celery -> dedupe key at the send |

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Outbox row creation (state change + event atomic) | Database / Storage (each service's own Postgres DB) | API / Backend (repository layer) | Atomicity comes from the owning transaction; repository functions already own the transaction boundary |
| Avro encoding of the payload | API / Backend (service process) | Schema Registry | Needs registered schema ids; must happen before the row is written |
| Relay to Kafka | Kafka Connect + Debezium (WAL reader) | Postgres logical replication | No relay code in services (D-01) |
| Event log, replay | Kafka | Schema Registry | Durable ordered log, per-aggregate key order |
| Visit downstream trigger | Consumer process (visit-consumer) | Temporal (`start_once`) | Event consumer starts the existing workflow idempotently |
| Reminder timing | Temporal (durable timers) | Consumer (starts/signals) | D-02; timers survive restarts |
| Notification delivery, retry, DLQ | Celery workers + RabbitMQ | Mongo (dedupe log) | D-09; at-least-once with dedupe |
| Analytics projection | Consumer process + analytics Postgres | Read-only API, Grafana | ANLY-06: derived from events, own database |
| Traces / metrics / logs | OpenTelemetry + Jaeger / Prometheus + Grafana | structlog | Existing libs, extended to workers |
| Dashboard / docs | CDN / Static (self-contained HTML) | - | DASH-03/04 |

## Standard Stack

### Core
| Library / Image | Version | Purpose | Why Standard |
|-----------------|---------|---------|--------------|
| `quay.io/debezium/connect` image | `3.7.1.Final` | Kafka Connect + Debezium Postgres connector + Outbox `EventRouter` | Official image; includes `PostgresConnector`, `EventRouter`, `BinaryDataConverter`; runs on Java 25 so Java-17+ connector loads. Verified against `cp-kafka:7.6.13`. 3.6.4.Final is the conservative alternative (same Java/Connect requirements) `[VERIFIED: docker pull + spike; CITED: debezium.io/releases]` |
| `confluentinc/cp-kafka` / `cp-schema-registry` | `7.6.13` (unchanged) | Broker (Kafka 3.6, KRaft) and registry | Already pinned and healthy in Compose `[VERIFIED: infra/docker-compose.yml:148,177]` |
| `confluent-kafka[avro,schemaregistry]` | `2.15.0` | Producer/consumer, `AvroSerializer`/`AvroDeserializer`, `SchemaRegistryClient` (incl. `mock://`) | CLAUDE.md pin. Installed and exercised against Kafka 3.6 + SR 7.6.13 in the spike. (PyPI latest is 2.16.1, 2026-10-28; do not float) `[VERIFIED: PyPI + spike]` |
| `fastavro` | `1.13.1` (resolved by the extra; `<2`) | Avro encode/decode used by the serializer | Pulled by the `avro` extra; pin it explicitly. `1.12.2` is the older fallback `[VERIFIED: PyPI + spike]` |
| `celery` | `5.6.3` | Notification delivery workers | CLAUDE.md pin (5.7.0 was released 2026-11-05, five days ago: do not take it) `[VERIFIED: PyPI + spike]` |
| `kombu` | `5.6.2` | AMQP layer | CLAUDE.md pin; Celery 5.6.3 requires `kombu>=5.6.0` `[VERIFIED: PyPI requires_dist]` |
| `opentelemetry-instrumentation-celery` | `0.65b0` | Injects/extracts trace context in Celery headers (parent/child by default) | Same 0.65b0 line as the already-pinned FastAPI/httpx instrumentation; requires `opentelemetry-api~=1.12`, `opentelemetry-instrumentation==0.65b0` `[VERIFIED: PyPI + spike: trace id carried publisher -> worker]` |
| `prometheus-client` | `0.26.0` (already installed transitively) | `start_http_server` + counters/histograms in workers/consumers | Official client; pin it directly since new code imports it `[VERIFIED: .venv pip list]` |
| `temporalio` | `1.34.0` (existing pin) | `ReminderWorkflow`, `start_signal`, `wait_condition`, `PrometheusConfig` runtime | Already pinned for the repo `[VERIFIED: pyproject.toml; SDK API checked in scratch venv]` |
| PostgreSQL | 15 with `wal_level=logical` | Outbox source; analytics projection store | Existing image `postgres:15-trixie` |
| RabbitMQ | `4.3-management` (existing) | Celery broker; `rabbitmq_prometheus` plugin is already enabled | `[VERIFIED: docker exec rabbitmq-plugins list -E -m]` |
| Grafana | `13.0.8` (existing) | File-provisioned datasources + dashboards (classic JSON) | `[CITED: grafana.com/docs/grafana/latest/administration/provisioning/]` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `SchemaRegistryClient.new_client({"url": "mock://"})` | in confluent-kafka 2.15.0 | In-memory registry for unit tests (returns `MockSchemaRegistryClient`) | All unit tests of serialization; no live SR needed `[VERIFIED: scratch venv round trip]` |
| `kafka-init` job on `confluentinc/cp-kafka:7.6.13` | same tag | `kafka-topics --create --if-not-exists` for event, DLQ topics | Compose one-shot, so partitions/retention are explicit |
| `asyncpg`, `SQLAlchemy 2.0.52`, `alembic 1.19.2` (existing) | existing | Outbox/projection writes, migrations | Unchanged |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `quay.io/debezium/connect:3.7.1.Final` | `cp-kafka-connect:7.6.13` + Debezium 2.7.x | Java 11 fits, but 2.x is an end-of-line series; extra Dockerfile; keeps the "CP line" look. Not recommended |
| Service-side Avro (`bytea`) | `table.expand.json.payload` + Confluent `AvroConverter` | Schema inferred from JSON, unstable names/nullability, defeats BACKWARD governance. Not recommended |
| Debezium CDC | Polling publisher | Locked out by D-01 (user-confirmed); documented as the cheaper alternative with relay code and polling load |
| `opentelemetry-instrumentation-confluent-kafka` | Manual extract/inject (about 30 lines) | The package makes the consumer span a Link, not a child; adds a dependency for no gain (Pitfall 6) |
| Classic RabbitMQ queues | Quorum queues (`x-queue-type: quorum`, `delivery-limit`) | Quorum is the production answer for poison messages that kill workers (default `delivery-limit` 20 since RabbitMQ 4.0), but needs Celery native delayed delivery and no global QoS, adding moving parts for a single-node dev stack. Recommend classic now, document quorum as the production path |

**Installation (add to `pyproject.toml` `dependencies`, exact pins):**
```bash
# after the human-approval checkpoint (see Package Legitimacy Audit)
confluent-kafka[avro,schemaregistry]==2.15.0
fastavro==1.13.1
celery==5.6.3
kombu==5.6.2
opentelemetry-instrumentation-celery==0.65b0
prometheus-client==0.26.0
```
Docker: `quay.io/debezium/connect:3.7.1.Final` (already pulled locally during research; 789 MB, arm64 and amd64 published).

**Version verification (run this session):** `curl pypi.org/pypi/<pkg>/json` for each package (first-release dates and latest versions below), `docker pull quay.io/debezium/connect:3.7.1.Final`, `curl repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/maven-metadata.xml` (3.6.4.Final and 3.7.1.Final present), Docker Hub tag list for `confluentinc/cp-kafka-connect` (7.6.13 present, not used).

## Package Legitimacy Audit

The `gsd-tools query package-legitimacy check` seam returned **SUS for every package**, with reasons only `too-new` (the package's newest release is days old) and `unknown-downloads` (no download statistic available). Neither reason reflects the pinned versions or the package age. Per repo convention (01.1-04, 02-06) the planner must insert a **blocking `checkpoint:human-verify`** task before the first `pip install`, listing the exact pins below. Registry existence alone is not treated as verification; the packages below are the ones named in CLAUDE.md (confluent-kafka, celery, kombu) or are official OpenTelemetry/Prometheus/Apache projects, and each pin was installed and exercised in a scratch environment this session.

| Package | Registry | Age (first release) | Downloads | Source Repo | Seam Verdict | Disposition |
|---------|----------|---------------------|-----------|-------------|--------------|-------------|
| confluent-kafka 2.15.0 | PyPI | 2016-05-24 | n/a (seam: unknown) | github.com/confluentinc/confluent-kafka-python | SUS (too-new, unknown-downloads) | Flagged: human checkpoint, exact pin (CLAUDE.md-named) |
| fastavro 1.13.1 | PyPI | 2012-01-05 | n/a | github.com/fastavro/fastavro | SUS (unknown-downloads) | Flagged: checkpoint, exact pin |
| celery 5.6.3 | PyPI | 2009-04-27 | n/a | docs.celeryq.dev (celery/celery) | SUS (too-new from 5.7.0, unknown-downloads) | Flagged: checkpoint, exact pin (CLAUDE.md-named, released 2026-03-26) |
| kombu 5.6.2 | PyPI | 2010-07-22 | n/a | github.com/celery/kombu | SUS | Flagged: checkpoint, exact pin (CLAUDE.md-named, released 2025-12-29) |
| opentelemetry-instrumentation-celery 0.65b0 | PyPI | 2020-08-14 | n/a | github.com/open-telemetry/opentelemetry-python-contrib | SUS (too-new, unknown-downloads) | Flagged: checkpoint; same 0.65b0 line as approved FastAPI/httpx instrumentation |
| prometheus-client 0.26.0 | PyPI | 2015-02-09 | n/a | github.com/prometheus/client_python | SUS (unknown-downloads) | Already installed transitively; promote to a direct pin; checkpoint |
| avro 1.12.2, authlib 1.9.5, cachetools 8.0.0, attrs 26.1.0, requests 2.34.2, certifi | PyPI | 2010 / 2017 / 2014 / 2015 / 2011 / - | n/a | apache/avro, authlib, tkem/cachetools, python-attrs, psf/requests | not individually run | Transitive dependencies of `confluent-kafka[avro,schemaregistry]`; list them in the checkpoint |
| opentelemetry-instrumentation-confluent-kafka 0.65b0 | PyPI | 2022-07-04 | n/a | github.com/open-telemetry/opentelemetry-python-contrib | SUS | **Not recommended / not installed** (Pitfall 6); no checkpoint needed |

**Packages removed due to [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** all of the above except the not-installed one. Planner inserts one `checkpoint:human-verify` before the dependency task. Seam signals are `[VERIFIED: gsd-tools package-legitimacy]`; ages are `[VERIFIED: PyPI JSON API]`.

Notes: `pip install` of these produced no `postinstall`; wheels exist for cp312 manylinux x86_64 and aarch64 (`confluent_kafka-2.15.0-cp312-cp312-manylinux_2_28_{x86_64,aarch64}`, `fastavro-1.13.1-cp312-...-manylinux...`), so the `python:3.12-slim` service images need no compiler for them `[VERIFIED: PyPI file list]`. `authlib` printed a deprecation warning about `httpx` vs `httpx2` on import; it is a warning only.

## Architecture Patterns

### System Architecture Diagram

```
                    HTTP (gateway) -----------------------------------------------+
                                                                                  | traceparent (W3C)
 Scheduling API --(commit)--> scheduling_db: appointments + status_history + OUTBOX row
 Temporal activity (confirm/cancel/swap/check-in/complete/no_show)  ^   (Avro bytes, traceparent,
 Visit API --(commit)--> visit_db: visits + OUTBOX (visit.completed)      uuid5 event id)
 Provider API --(commit)--> provider_db: schedule_change_events + OUTBOX
 Patient API  --(commit)--> patient_db: patients + OUTBOX (patient.registered)
                                |  Postgres WAL (wal_level=logical, pgoutput, 1 slot per DB)
                                v
        Kafka Connect (quay.io/debezium/connect:3.7.1.Final)  4 connectors, EventRouter SMT
           key = aggregateid | headers: id, eventType, traceparent, schemaVersion
           value = Avro wire format bytes (BinaryDataConverter passthrough)
                                |
                                v
   Kafka (cp-kafka 7.6.13)  topics: smarthealth.{appointment,visit,provider,patient}.events (3 partitions)
        |                       |                         |                     DLQ: smarthealth.dlq.<group>
        |                       |                         |
        v                       v                         v
 visit-consumer          notification-consumer       analytics-consumer
 (group visit-downstream) (group notification-trigger) (group analytics-projection)
   | start_once(visit-completed-{id})   |                        | upsert + processed_events (one txn)
   v                                    |-- start/signal/cancel ---> Temporal: ReminderWorkflow,
 Temporal VisitCompletedWorkflow        |      (id reminder-{appointment_id})   FollowUpReminderWorkflow
 (unchanged; sweeper retired)           |                              | timer fires -> activity
                                        v                              v
                                 Celery tasks on RabbitMQ  <-----------+
                                 queues notifications.{booking,reminder,followup}, DLX -> notifications.dlq
                                        |  acks_late, retry+backoff, Reject(requeue=False)
                                        v
                                 fake provider -> Mongo notification_deliveries (unique dedupe_key)

 analytics_db (projection) --> read-only analytics API (/api/analytics via gateway) and Grafana PostgreSQL datasource
 Every process: /metrics (Prometheus) | OTLP spans (Jaeger) | structlog JSON with trace_id
```

### Recommended Project Structure
```
libs/events/
├── schemas/                 # *.avsc, one per event type (record name com.smarthealth.events.X)
├── registry.py              # SchemaRegistryClient factory ("mock://" in tests), warm_up(), register_all()
├── outbox.py                # enqueue_event(db, ...): serialize + INSERT INTO outbox (never commits)
├── migration.py             # create_outbox_table(op), create_publication(op) shared by 4 migrations
├── consumer.py              # poll loop, manual commit, retry/DLQ, trace extraction, metrics
├── tracing.py               # inject_traceparent(), extract_context(headers)
└── events.py                # event type -> (aggregate_type, record schema, topic) registry
services/notification/       # consumer.py, tasks.py (Celery), celery_app.py, provider.py, temporal_worker.py, workflows/
services/analytics/          # consumer.py, projections.py, api/, migrations/, app.py
services/visit/consumer.py   # visit-downstream consumer (replaces downstream sweep)
infra/connect/               # connectors/*.json templates, register-connectors.sh
infra/grafana/               # provisioning/{datasources,dashboards}/*.yml, dashboards/*.json
infra/prometheus.yml         # + worker/consumer/RabbitMQ scrape jobs
scripts/register_schemas.py  # used by the schema-init job and CI
tests/events/ tests/notification/ tests/analytics/ tests/e2e/test_phase3_*.py
```

### Pattern 1: Outbox write with service-side Avro (verified end to end)
**What:** Inside the repository function that applies a transition, after the state change and history row, call `enqueue_event(db, ...)`. It serializes the payload with a strict `AvroSerializer` and inserts one outbox row using the SAME `AsyncSession`. It never commits; the caller owns the transaction (matches the existing repo contract "Every function takes the caller's `AsyncSession`, flushes, and never commits" `[VERIFIED: services/scheduling/repository/appointment_repo.py:4]`).
**Hook points (verbatim existing call sites):** `append_status_history(` at `appointment_repo.py:237` (confirm), `:468` (cancel), `:583` (swap/reschedule), `:720` (check-in), `:767` (complete), `:827` (no_show). Emit in these transition functions, NOT inside `append_status_history`/`record_history_note` (notes are `from_status == to_status` and not domain events). Visit: `transition_visit` (`visit_repo.py:113`). Provider: `record_schedule_change` (`provider_repo.py:366`, adds the event "BEFORE the function's existing commit"). Patient: `create_patient` (`patient_repo.py`, adds then commits).
**Why booking accept stays untouched:** `appointment.booked` is emitted at `confirm_booking` (saga step), not at `accept_booking`, so NFR-03's accept path gets no new work (D-04). `accept_booking` is the only function the 202 path commits `[VERIFIED: appointment_repo.py:89-171]`.
**Outbox DDL (per service DB):**
```sql
CREATE TABLE outbox (
  id             uuid PRIMARY KEY,            -- deterministic uuid5: dedupe at the data layer
  aggregatetype  text NOT NULL,               -- "appointment" | "visit" | "provider" | "patient"
  aggregateid    text NOT NULL,               -- Kafka key; per-aggregate order
  type           text NOT NULL,               -- "appointment.booked" ...
  payload        bytea NOT NULL,              -- Avro wire format (magic byte + schema id + body)
  traceparent    text,                        -- W3C traceparent of the producing span
  schema_version integer NOT NULL DEFAULT 1,
  occurred_at    timestamptz NOT NULL DEFAULT now()
);
CREATE PUBLICATION dbz_outbox FOR TABLE outbox;   -- by the owner role (the service role owns its DB): verified
```
**Event id:** `uuid5(NAMESPACE, f"{aggregate_id}:{type}:{version}")` where `version` is `appointments.version` after the transition (it is incremented in every transition UPDATE `[VERIFIED: appointment_repo.py:228-230 "version = version + 1"]`) or the visit `to_status`. The PK then makes a double emission impossible even if a bug calls the function twice; use `INSERT ... ON CONFLICT (id) DO NOTHING`.
**Example (spike-verified serializer configuration):**
```python
# Source: scratch spike + confluent-kafka 2.15.0 source (schema_registry/_sync/avro.py _default_conf keys)
from confluent_kafka.schema_registry import SchemaRegistryClient, record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

sr = SchemaRegistryClient.new_client({"url": settings.schema_registry_url})   # "mock://" in tests
ser = AvroSerializer(
    sr, schema_str,
    conf={"auto.register.schemas": False,                # strict: registry is written only by schema-init/CI
          "subject.name.strategy": record_subject_name_strategy},
)
payload = ser(event_dict, SerializationContext("smarthealth.appointment.events", MessageField.VALUE))
await db.execute(text("""INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload, traceparent, schema_version)
                         VALUES (:id, :agg, :key, :type, :payload, :tp, :ver) ON CONFLICT (id) DO NOTHING"""), {...})
```
`warm_up()` (Pitfall 8): resolve every subject's schema id once at startup with retries, before serving.

### Pattern 2: Debezium connector (one per outbox database), verified
Four connectors: `scheduling-outbox`, `visit-outbox`, `provider-outbox`, `patient-outbox`. The Postgres connector connects to exactly one database, so one connector per database; all four run in one Connect worker. Billing and identity have no events in D-07 and need no outbox.
```json
{
 "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
 "tasks.max": "1",
 "database.hostname": "postgres", "database.port": "5432",
 "database.user": "debezium", "database.password": "${DEBEZIUM_DB_PASSWORD}", "database.dbname": "scheduling_db",
 "topic.prefix": "smarthealth-scheduling",
 "plugin.name": "pgoutput",
 "slot.name": "dbz_scheduling", "slot.drop.on.stop": "false",
 "publication.name": "dbz_outbox", "publication.autocreate.mode": "disabled",
 "table.include.list": "public.outbox",
 "snapshot.mode": "initial",
 "tombstones.on.delete": "false",
 "key.converter": "org.apache.kafka.connect.storage.StringConverter",
 "value.converter": "io.debezium.converters.BinaryDataConverter",
 "value.converter.delegate.converter.type": "org.apache.kafka.connect.json.JsonConverter",
 "value.converter.delegate.converter.type.schemas.enable": "false",
 "transforms": "outbox",
 "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
 "transforms.outbox.table.field.event.id": "id",
 "transforms.outbox.table.field.event.key": "aggregateid",
 "transforms.outbox.table.field.event.payload": "payload",
 "transforms.outbox.route.by.field": "aggregatetype",
 "transforms.outbox.route.topic.replacement": "smarthealth.${routedByValue}.events",
 "transforms.outbox.table.fields.additional.placement": "type:header:eventType,traceparent:header:traceparent,schema_version:header:schemaVersion",
 "heartbeat.interval.ms": "10000"
}
```
`[VERIFIED: spike p3spike: connector RUNNING, message on smarthealth.appointment.events with key appt-0 and headers id, eventType, traceparent, schemaVersion; Avro value decoded by AvroDeserializer]`. Option names `[CITED: debezium.io outbox-event-router docs]`. Register with `PUT /connectors/<name>/config` (idempotent). The `${...}` placeholder is rendered by the registration script; do not publish the Connect REST port to the host (Security).

Compose service (`connect`): `image: quay.io/debezium/connect:3.7.1.Final`, env `BOOTSTRAP_SERVERS=kafka:29092`, `GROUP_ID`, `CONFIG_STORAGE_TOPIC`, `OFFSET_STORAGE_TOPIC`, `STATUS_STORAGE_TOPIC`, `*_STORAGE_REPLICATION_FACTOR=1`, healthcheck `curl -f http://localhost:8083/connectors` (reached `healthy`), cap heap (`KAFKA_HEAP_OPTS`; the idle JVM used about 950 MiB uncapped), `depends_on` kafka healthy. A `connect-init` one-shot (same image has `curl`/bash) depends on `connect` healthy and on the four owning APIs healthy (their Alembic run creates the table and publication), PUTs the four configs, then **polls task state, not connector state** (Pitfall 2).

Postgres changes: `command: ["postgres", "-c", "max_connections=300", "-c", "wal_level=logical", "-c", "max_slot_wal_keep_size=2GB"]`. `max_replication_slots` and `max_wal_senders` are already 10 (four needed) `[VERIFIED: docker exec psql, current stack]`. The `db-init` job adds an idempotent role `debezium LOGIN REPLICATION`, `GRANT CONNECT` on the four databases (the script revokes PUBLIC connect, `create-service-databases.sh`), plus `analytics` in `SERVICES=(identity organization provider patient scheduling visit billing)` `[VERIFIED: infra/postgres-init/create-service-databases.sh:20]`. Each outbox migration (run as the owner role) creates the table, `CREATE PUBLICATION dbz_outbox FOR TABLE outbox`, and `GRANT SELECT ON outbox TO debezium` (guarded by role existence) because `snapshot.mode=initial` reads the table `[VERIFIED: owner role could create the publication; a REPLICATION role connected from another container over the default pg_hba]`.

**Replay property:** the table is insert-only and kept. With `snapshot.mode=initial`, rows that existed before the connector's first start are emitted by the initial snapshot (verified with a pre-existing row), closing the gap between "migration deployed" and "connector registered". If the Connect offsets topic is ever lost while the slot remains, a re-snapshot re-emits retained rows; consumers dedupe by event id. (`[ASSUMED]` behavior of re-snapshot after offset loss: inferred from the snapshot test, not exercised.)

### Pattern 3: Consumer base (sync poll loop, persistent event loop)
One class in `libs/events/consumer.py`, used by all three consumers:
- `Consumer({"group.id": ..., "enable.auto.commit": False, "auto.offset.reset": "earliest", ...})`; commit synchronously after the effect (`consumer.commit(message=msg, asynchronous=False)`) `[ASSUMED: standard confluent-kafka API]`.
- Effects are async (SQLAlchemy async, Temporal client). `libs/db/postgres.py` caches engines per running loop (`entry.loop is not current_loop` rebuilds the engine `[VERIFIED: libs/db/postgres.py:43-57]`), so run ONE persistent loop (`asyncio.Runner` / one `loop.run_until_complete` per message on the same loop), never `asyncio.run` per message.
- Deserialize with `AvroDeserializer(sr)` (writer schema id comes from the message). A deserialization/validation failure is non-retryable: produce the original key/value/headers to `smarthealth.dlq.<group>` with error-type headers, then commit. A transient failure retries in-process with backoff (3 attempts), then DLQ. Never block a partition on a poison message.
- Trace: extract `traceparent` from headers and start the processing span as a CHILD (Code Examples).
- Metrics: `kafka_consumer_lag` gauge, `event_processing_seconds` histogram, `event_end_to_end_lag_seconds` (`now - occurred_at`), `dlq_events_total`.
- Liveness: heartbeat file touched in the loop, same Compose healthcheck pattern as the Temporal workers `[VERIFIED: infra/docker-compose.yml:420-429]`.
- Keep `max.poll.interval.ms` above the worst-case handler time (Temporal/DB calls are short).

### Pattern 4: Three idempotency flavors (refines D-08)
| Consumer | Effect | Idempotency mechanism |
|----------|--------|-----------------------|
| analytics-projection | Postgres upsert | `processed_events(event_id PK, consumer)` inserted in the same transaction as the upsert; projection upserts keyed by ids and order-insensitive |
| visit-downstream | Temporal workflow start + `downstream_started_at` marker | `start_once(...)` with `visit-completed-{visit_id}` (USE_EXISTING + REJECT_DUPLICATE) and the conditional marker `mark_downstream_started` `[VERIFIED: services/visit/downstream.py:63-87]`; no extra table needed |
| notification-trigger | Celery enqueue + Temporal start/signal | Enqueue is at-least-once; duplicates are absorbed at the SEND by a unique `dedupe_key = (notification_type, appointment_id, event_id)`; Temporal starts use deterministic ids |

### Pattern 5: Retiring the visit sweeper (D-03)
1. `visit.completed` is emitted inside `transition_visit` when `target == VisitStatus.COMPLETED` (same transaction as the status UPDATE and the history row).
2. New `visit-consumer` process (image = visit image, command override, heartbeat healthcheck): on `visit.completed` load the visit, call the existing `ensure_downstream_started(client, db, visit)` (starts `visit-completed-{id}`, then commits the marker).
3. Remove `_start_downstream` and its call from `services/visit/api/visits.py` (`_transition`, lines 393-401 call it) and the `request` plumbing it needs; remove `sweep_unstarted`, `ACT_SWEEP`, the sweeper workflow and its Schedule definition, `sweeper_*` settings, the worker's run-on-start sweep (`worker.py` lines 138-146). **Keep `VisitDownstreamSweeperWorkflow` registered until the Schedule is deleted** (Finding F6), then remove.
4. Keep the reconciler: `list_unsettled` requires `downstream_started_at IS NOT NULL` `[VERIFIED: visit_repo.py:271-276]`, which the consumer still sets, and its docstring says never-started visits "stay with the sweeper", so update that docstring and the reconcile tests to say "with the outbox consumer".
5. Keep `VisitCompletedWorkflow` unchanged. Its follow-up and analytics intent stubs (`ACT_FOLLOWUP_INTENT`, `ACT_ANALYTICS_INTENT`) stay as intent records (changing them is a command-sequence change needing `workflow.patched`); the analytics intent is superseded by the outbox event and the follow-up by `FollowUpReminderWorkflow`.

### Pattern 6: Celery + RabbitMQ 4.3 (verified configuration)
```python
# Source: spike (celery 5.6.3 + kombu 5.6.2 + rabbitmq:4.3-management); behavior of Reject/acks_late [CITED: docs.celeryq.dev/en/stable/userguide/tasks.html]
app.conf.update(
    task_default_queue="notifications.booking",
    task_queues=[
        Queue("notifications.booking", Exchange("notifications", type="direct"), routing_key="booking",
              queue_arguments={"x-dead-letter-exchange": "smarthealth.dlx",
                               "x-dead-letter-routing-key": "notifications.dlq"}),
        # notifications.reminder, notifications.followup declared the same way
        Queue("notifications.dlq", Exchange("smarthealth.dlx", type="direct", durable=True),
              routing_key="notifications.dlq"),
    ],
    task_acks_late=True, task_reject_on_worker_lost=True, worker_prefetch_multiplier=1,
    task_serializer="json", accept_content=["json"],        # never pickle
    # REQUIRED on RabbitMQ 4.3 (otherwise Queue.declare (541) transient_nonexcl_queues is deprecated ...):
    worker_enable_remote_control=False, worker_send_task_events=False, task_send_sent_event=False,
    result_backend=None, task_ignore_result=True,
    broker_connection_retry_on_startup=True,
)
# worker: celery -A services.notification.celery_app worker -P threads -c 8 -Q notifications.booking,notifications.reminder,notifications.followup \
#         --without-mingle --without-gossip --without-heartbeat
@app.task(bind=True, name="notifications.send", max_retries=5)
def send(self, dedupe_key: str, ...):
    try: ...deliver...
    except TransientError as exc:
        if self.request.retries >= self.max_retries:
            raise Reject(exc, requeue=False)       # -> DLX -> notifications.dlq (verified: dlq ready=1)
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)
```
- **Why threads pool:** one process, so `prometheus_client.start_http_server` and OTel instrumentation work without multiprocess mode or `worker_process_init`. Trade: no `task_time_limit`/soft limits in the threads pool; bound I/O with client timeouts. OTel Celery instrumentation: call `CeleryInstrumentor().instrument()` at module import in the worker process (verified: the task's trace id equals the publisher's).
- **Retries:** use `self.retry(countdown=...)` (backoff computed, jitter optional) or `autoretry_for` + `retry_backoff=True` (`retry_backoff_max` default 600 s, `retry_jitter` default True) `[CITED: docs.celeryq.dev tasks]`. Final failure raises `Reject(requeue=False)`; requeue=False only dead-letters with `acks_late` `[CITED]`.
- **Dedupe + fake provider:** collection `notification_deliveries` in the existing Mongo (CLAUDE.md: Mongo for "visit/notification logs") with a unique index on `dedupe_key`. Task: insert `{dedupe_key, status: "sending", ...}`; `DuplicateKeyError` -> read; `sent` -> return (no double send); `sending` -> a previous attempt died mid-send, send again (at-least-once, noted); on success set `sent`. Recipient is a patient id only (no contact data); failure injection by a settings prefix, mirroring `BILLING_PRECHECK_FAIL_PREFIX`.
- **Send-time guard:** the notification consumer also records cancelled appointment ids; the reminder send task refuses to send for a cancelled appointment (closes the window where the cancel event lags behind a firing timer).
- **Queue layout:** `notifications.booking` (booked/cancelled/rescheduled/schedule-changed), `notifications.reminder`, `notifications.followup`, `notifications.dlq`. Concurrency 8 threads is ample for the fake provider.
- Celery args are plain dicts/strings (CLAUDE.md rule); ids and a notification type only.

### Pattern 7: Temporal reminders (verified in a time-skipping spike)
`ReminderWorkflow(appointment_id, slot_start_iso, lead_minutes)`, id `reminder-{appointment_id}`, task queue `notification-tq` (new, one per service per D-05), hosted by a `notification-temporal-worker` process. Logic: compute `fire_at = slot_start - lead`; `await workflow.wait_condition(lambda: cancelled or changed, timeout=fire_at - workflow.now())`; timeout -> execute activity `notification.enqueue_reminder` (Celery `send_task`, dedupe key `reminder:{appointment_id}:{lead}`) then complete; `cancel` signal -> complete; `reschedule(new_slot_iso)` signal -> recompute. A lead already in the past completes `skipped_past`. Lead times are inputs (settings), never read inside the workflow.
- **Started by the notification consumer, not by `BookingWorkflow`.** On `appointment.booked`: `start_workflow(..., id_conflict_policy=USE_EXISTING, id_reuse_policy=REJECT_DUPLICATE)`. On `appointment.rescheduled`: the same call with `start_signal="reschedule"`, `start_signal_args=[new_slot_iso]` (signal-with-start; robust to ordering) and `ALLOW_DUPLICATE`. On `appointment.cancelled`/`no_show`: signal `cancel` (ignore "not found"). Existing workflows are untouched, so no `workflow.patched` is required in Phase 3. `FollowUpReminderWorkflow` (on `visit.completed`) is the same shape with a fixed delay.
- **Spike results:** duplicate start attaches to the same run (`result_run_id` equal); reschedule signal-with-start on a running reminder fired for the new slot; duplicate start after close with `REJECT_DUPLICATE` raised `WorkflowAlreadyStartedError`. `[VERIFIED: spike, temporalio 1.34.0 time-skipping]`
- **Replay convention (existing):** recorded histories live in `tests/workflows/histories/*.json`, replayed by `Replayer`; re-record flag `SMARTHEALTH_RECORD_HISTORIES=1` `[VERIFIED: tests/workflows/test_replay_booking_patches.py:25-27]`. Add `reminder_v1.json`/`followup_v1.json`; any later change to a reminder workflow's command sequence needs a new `workflow.patched` id plus a new recorded history, exactly like the booking patches. Keep activity names stable forever.
- **Sandbox:** the workflow module imports only `temporalio` + pure contracts (same rule as `booking.py`); define workflows outside any module that imports SDK test helpers (the spike failed sandbox validation when the workflow lived in the script that also imported `WorkflowEnvironment`).

### Pattern 8: Trace chain (OBS-02)
HTTP (gateway) -> Scheduling API -> `TracingInterceptor` Temporal workflow/activity (already working `[VERIFIED: libs/temporal.py:48-53]`) -> activity writes the outbox row: `inject_traceparent()` copies the CURRENT span context into `outbox.traceparent` -> Debezium header `traceparent` -> consumer extracts and starts a CONSUMER span as child -> Celery publish inside that span (instrumentation injects) -> worker task span is a child (verified identical trace id) -> reminder path: the Temporal activity that calls `send_task` is itself inside the reminder workflow's trace. A reminder workflow started by the consumer inherits the consumer span through `TracingInterceptor`, so the reminder timer is in the booking's trace too (its fire time is days later; the trace is long-lived by design, label it).

### Pattern 9: Analytics projection (D-10)
`analytics_db` (new service `analytics`, role `analytics_svc`, added to `SERVICES`; read-only role `grafana_ro` with default SELECT). Tables: `processed_events`, `patients(patient_id, clinic_id, registered_at, deactivated_at)`, `appointments(appointment_id, clinic_id, booked_at, slot_start, status, cancelled_at, checked_in_at, completed_at)`, `visits_completed(visit_id, clinic_id, completed_at)`. All upserts keyed by ids and tolerant of cross-topic order (a `completed` before the `booked` row exists creates the row). Metrics as SQL views: total patients (registered minus deactivated if `patient.deactivated` is added, Open Question 2), appointments booked per day/hour, completed visits, cancellation rate = cancelled / booked in the window (document), average wait per D-10. A FastAPI read-only API (`/api/analytics/summary`, `/timeseries`) with RBAC (admin all clinics; front desk own clinic) added to `ROUTE_TABLE` `[VERIFIED: services/gateway/routing.py:28-40 has no analytics prefix]` and a Grafana PostgreSQL datasource on `analytics_db` for the five panels. Rebuild: a new consumer group id + truncated tables + `auto.offset.reset=earliest` (script + a test). Topics get `retention.ms=-1` so replay is possible (`[ASSUMED]` acceptable for a learning build; disk is small).

### Pattern 10: Prometheus/Grafana as code
- Existing services already expose `/metrics` via `Instrumentator().instrument(app).expose(app)` `[VERIFIED: services/scheduling/app.py:67]`. Add scrape jobs to `infra/prometheus.yml` for: `analytics`, `notification-celery` (`:9100` from `start_http_server`), `notification-consumer`, `visit-consumer`, `analytics-consumer`, the two Temporal workers (SDK metrics via `Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address="0.0.0.0:9464")))` passed as `runtime=` to `Client.connect`; both constructors verified in the scratch venv), and `rabbitmq:15692` (the `rabbitmq_prometheus` plugin is enabled).
- The current file says workers have "no scrape job" `[VERIFIED: infra/prometheus.yml final comment "scheduling-worker and visit-worker have no HTTP listener in Phase 2, so no scrape job"]` (that comment must be replaced).
- Grafana: mount `infra/grafana/provisioning` and `infra/grafana/dashboards`; provider `type: file`, `options.path`, `allowUiUpdates: false`; datasources by file with stable `uid`s, PostgreSQL password via `$ENV_VAR` `[CITED: grafana.com provisioning docs]`. Author classic JSON dashboards (file provisioning supports classic JSON; v2 needs the Kubernetes resource format `[CITED]`).
- NFR histograms (D-04): the Instrumentator already records HTTP latency per handler; add `booking_saga_seconds` (observed in the `confirm` activity from `appointments.created_at`), `outbox_to_consume_seconds`, `notification_delivery_seconds`, `outbox_insert_seconds`.

### Anti-Patterns to Avoid
- **Per-event-type topics** with an ordering claim (F3).
- **`asyncio.run` per Kafka message** (engine cache is per-loop, Pattern 3).
- **Deleting an activity or workflow name** that recorded histories reference (replay failure).
- **Polling the Debezium connector state only** (Pitfall 2).
- **Auto-registering schemas at runtime** from services (governance; D-05 says registration in code/CI).
- **Adding the OTel confluent-kafka instrumentation** (Pitfall 6).
- **Sending notifications from both the workflow stub and the consumer** (F5).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Outbox relay | A polling loop with `SELECT ... FOR UPDATE SKIP LOCKED` in each service | Debezium Postgres connector + `EventRouter` (D-01) | User-locked; ordering from the WAL, no relay code |
| Avro wire format | Manual magic byte + schema-id framing | `AvroSerializer` / `AvroDeserializer` | Handles id caching, subject strategy, schema resolution; proven in spike |
| Schema compatibility | Custom diff of `.avsc` files | Registry compatibility check (`/compatibility/subjects/.../versions/latest`, global BACKWARD is already set `[VERIFIED: GET /config -> {"compatibilityLevel":"BACKWARD"}]`) in `register_schemas.py` and a test | Registry is the authority |
| Reminder timers | Celery `eta`/`countdown`/beat, or `asyncio.sleep` in a consumer | Temporal durable timers (`wait_condition` with timeout) | D-02; survive restarts, cancellable, reschedulable |
| Dead-lettering | A "failed_tasks" table + custom redelivery | RabbitMQ DLX + `Reject(requeue=False)` | Broker-native, keeps the original message intact |
| Retry backoff | Hand-rolled sleep loops | `self.retry(countdown=...)` / `autoretry_for` + `retry_backoff` | Built in; tenacity only for in-process HTTP retries |
| Trace propagation | Hand-built `traceparent` strings | `opentelemetry.propagate.inject/extract` | Header format and sampling flags handled; verified |
| Dedupe store | An in-memory set | Unique index (Mongo `dedupe_key`; Postgres `processed_events` PK) | Survives restarts and concurrency |
| Dashboards | Click-built Grafana panels | File provisioning (`infra/grafana`) | D-11 requires dashboards-as-code |
| Kafka lag metric | Scraping with ad-hoc scripts | `consumer.get_watermark_offsets` vs committed offsets in the consumer base | One implementation, shared |

**Key insight:** the dangerous parts of this phase (atomic publish, ordering, dedupe, timers, dead-lettering) each have a broker-native or library-native mechanism; hand-rolled versions reintroduce exactly the failure windows the phase exists to close.

## Runtime State Inventory

(Applies to D-03's retirement of the sweeper and the `wal_level` change; this is a migration-style phase for those parts.)

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | `visits` rows with `status='completed' AND downstream_started_at IS NULL` at cutover have no outbox row. `ix_visits_downstream_pending` exists for the old sweep `[VERIFIED: services/visit/repository/models.py:102-106]`. Existing Postgres volume `smarthealth_postgres_data` keeps all data when `wal_level` changes `[VERIFIED: spike: row survived the restart]` | **Data migration:** one-shot backfill script (uses the same `enqueue_event`) for completed-unstarted visits, or document that cutover data is dev-only. **Code edit:** stop writing the marker from the API |
| Live service config | Temporal Schedule `visit-downstream-sweeper` (and its `-run` workflow id) is registered on the live server and is left in place by `ensure_visit_schedules` `[VERIFIED: schedules.py:7-9]`. Schedule `scheduling-schedule-change-poller` is paused on purpose (user decision) and must stay paused | **Live-config change:** idempotent `get_schedule_handle("visit-downstream-sweeper").delete()` on worker start (ignore not-found). Do not touch the poller |
| OS-registered state | None: no host cron/launchd/systemd units; Temporal Schedules are the only recurring registrations | None, verified by reading Compose and the worker start-up code |
| Secrets / env vars | New: `DEBEZIUM_DB_PASSWORD`, `ANALYTICS_DB_PASSWORD`, RabbitMQ credentials for Celery (existing `RABBITMQ_DEFAULT_USER/PASS` in Compose), `GRAFANA_RO` password. Settings `VISIT_SWEEPER_INTERVAL_SECONDS`, `VISIT_SWEEPER_GRACE_SECONDS` become dead | Add to `db-init` env block and `.env.example` (`.env.example` edits were harness-blocked before, see 01.1-09: list variables in the SUMMARY if blocked); remove the dead settings |
| Build artifacts | Every service image installs `pip install .`, so adding dependencies grows all images and needs `docker compose up -d --build`; `smarthealth.egg-info/` in the repo root is stale metadata only | Rebuild images; migrations run at container start, and host-side tests need `alembic upgrade head` against the live DBs |

## Common Pitfalls

### Pitfall 1: `cp-kafka-connect:7.6.13` cannot load Debezium 3.x
**What goes wrong:** Connect starts, `/connector-plugins` lists only MirrorMaker, `curl .../connectors` accepts nothing. **Why:** the image runs Zulu OpenJDK 11; Debezium 3.x is class-file 61 (Java 17). **Avoid:** use `quay.io/debezium/connect:3.7.1.Final`. **Warning sign:** `UnsupportedClassVersionError` in the Connect log during plugin scan `[VERIFIED: spike]`.

### Pitfall 2: Connector `RUNNING` but task `FAILED`
**What goes wrong:** a missing publication/table leaves `connector.state=RUNNING` and `tasks[0].state=FAILED` ("Publication autocreation is disabled, please create one and restart the connector") `[VERIFIED: spike]`. **Avoid:** `connect-init` and tests must check `tasks[*].state == RUNNING`; order it after the four APIs are healthy (their migrations create table + publication).

### Pitfall 3: Duplicates on the wire after a hard kill
**What goes wrong:** `docker kill` of Connect re-emits events since the last offset flush `[VERIFIED: spike 6 -> 11 -> 16]`. **Avoid:** idempotent consumers (Pattern 4); tests must force a re-delivery and assert one effect; do not promise exactly-once on the topic.

### Pitfall 4: Replication slot retains WAL
**What goes wrong:** while Connect is down the slot stays inactive and WAL accumulates (spike showed `active=f`, retained bytes growing); deleting a connector does NOT drop its slot `[VERIFIED: spike: dbz_scheduling remained]`. **Avoid:** `max_slot_wal_keep_size=2GB`, `heartbeat.interval.ms=10000`, an operations note, and a Grafana panel/alert on `pg_replication_slots` retained bytes if a Postgres exporter is not added (use a query panel on the Postgres datasource of a metrics role, `[ASSUMED]` acceptable).

### Pitfall 5: Ordering claim across topics (F3)
Use one topic per aggregate with key = aggregate id.

### Pitfall 6: OTel confluent-kafka instrumentation breaks the single trace
**What goes wrong:** `wrap_poll` starts a new "recv" span and links the producer context (`_get_links_from_records` builds `Link`s); the consumer span is not a child `[VERIFIED: opentelemetry-instrumentation-confluent-kafka 0.65b0 source, __init__.py wrap_poll and utils.py]`. The success criterion demands ONE trace. **Avoid:** do not install it; extract `traceparent` manually and start the span with `context=ctx` (verified: same trace id, parent = producer span).

### Pitfall 7: Celery 5.6.3 on RabbitMQ 4.3 refuses to start with defaults
**What goes wrong:** `Queue.declare: (541) INTERNAL_ERROR - Feature transient_nonexcl_queues is deprecated. By default, this feature is not permitted anymore` (worker restart loop `RestartFreqExceeded`) `[VERIFIED: spike]`. **Avoid:** the configuration in Pattern 6 (remote control, events, mingle, gossip, heartbeat off). Alternative: permit the deprecated feature in `rabbitmq.conf` (not recommended). `[ASSUMED: which of the settings is strictly necessary; the full set was verified together]`.

### Pitfall 8: Schema Registry in the write path, and a blocking client on the event loop
**What goes wrong:** the first `AvroSerializer` call per subject does a synchronous HTTP lookup (httpx), blocking the event loop and failing the transaction if Schema Registry is down. **Avoid:** `warm_up()` at startup (in an executor, retried), readiness only after warm; steady-state encoding is CPU-only. Document the residual risk: a Schema Registry outage after warm-up does not affect writes; a cold start during an outage cannot become ready. Measure `outbox_insert_seconds` (D-04).

### Pitfall 9: Engines are bound to the creating event loop
See Pattern 3; also keep `get_session_factory(url)` calls inside the single consumer loop.

### Pitfall 10: The old sweeper Schedule keeps firing
See F6 and Runtime State Inventory.

### Pitfall 11: Time-skipping tests advance time for everyone
In the Temporal spike a slot "3 days ahead" had already passed (`skipped_past`) after an earlier `await handle.result()` advanced virtual time 5 days. **Avoid:** compute slot times from `env.get_current_time()` per test, or one workflow per environment.

### Pitfall 12: Test pollution of global analytics
Integration and e2e suites create real events in the shared Compose stack once Connect runs, so global counts shift. **Avoid:** analytics assertions scoped to a unique `clinic_id` per test, or delta assertions; a replay test uses its own consumer group and database.

### Pitfall 13: Integration tests need the new schema
Host-side tests hit the live Compose databases (`tests/scheduling/conftest.py`: "Requires the live Postgres container with scheduling migrations applied"). Wave 0 must rebuild images and run `alembic upgrade head` for scheduling, visit, provider, patient, analytics before tests that touch `outbox`.

### Pitfall 14: Postgres restart blip
Recreating `postgres` for `wal_level=logical` drops every service connection and Temporal's. Services use `pool_pre_ping=True` `[VERIFIED: libs/db/postgres.py:50]`; Temporal and workers have restart policies. Do it once, early, and re-run the health check script.

### Pitfall 15: PHI in schemas
Add a test that scans every `.avsc` for banned field names (name, dob/birth, phone, address, email, coverage) and a test that serialized payload bytes contain none of the seed PHI values.

### Pitfall 16: NULL `traceparent` header
Debezium emits the header with a null value when the column is NULL; the extractor must tolerate `None` (the OTel getter does `if value is not None`; our own helper must too).

### Pitfall 17: Topic auto-creation yields one partition
Debezium's first publish auto-created `smarthealth.appointment.events` with defaults. Pre-create topics in `kafka-init` with explicit partitions (recommend 3) and retention; create DLQ topics there too.

## Code Examples

### Consumer span as a child of the producer's trace (verified)
```python
# Source: scratch spike (otel_extract.py): same trace id, parent span = producer span
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind

def extract_context(headers):                     # headers: list[(str, bytes|None)] from Message.headers()
    carrier = {k: v.decode() for k, v in (headers or []) if v is not None}
    return propagate.extract(carrier)

with tracer.start_as_current_span(f"{msg.topic()} process", context=extract_context(msg.headers()),
                                  kind=SpanKind.CONSUMER, attributes={"messaging.system": "kafka"}):
    handler(event)         # Celery .apply_async / Temporal start inside here inject the same trace
```

### Producer side: capture the traceparent into the outbox row
```python
from opentelemetry import propagate
def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")
```

### Strict serializer test without a live registry
```python
# Source: verified in scratch venv (confluent-kafka 2.15.0)
sr = SchemaRegistryClient.new_client({"url": "mock://"})
sr.register_schema("com.smarthealth.events.AppointmentBooked", Schema(SCHEMA, "AVRO"))
```

### Temporal client with Prometheus metrics (signature verified)
```python
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
runtime = Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address="0.0.0.0:9464")))
client = await Client.connect(address, namespace=ns, runtime=runtime, interceptors=[TracingInterceptor()], lazy=False)
```

### Grafana provisioning
```yaml
# infra/grafana/provisioning/dashboards/smarthealth.yml   [CITED: grafana.com provisioning docs]
apiVersion: 1
providers:
  - name: smarthealth
    orgId: 1
    type: file
    allowUiUpdates: false
    updateIntervalSeconds: 30
    options: { path: /var/lib/grafana/dashboards }
```

### Kill-between-commit-and-publish tests (shape)
1. Commit a transition + outbox row with Connect running; `docker kill` the owning API container immediately after the commit; assert exactly one message with that event id appears (no relay lives in the service, so the kill is irrelevant to delivery).
2. `docker stop connect`; commit 3 transitions; assert the topic count is unchanged; `docker start connect`; assert 3 messages in order, count +3 `[VERIFIED: spike]`.
3. `docker kill connect` after delivery; restart; assert the topic gets duplicates AND the consumer effect count is still 1 (processed-event / deterministic workflow id).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Debezium on Java 11 / CP Connect images | Debezium 3.x needs Java 17+; official image ships Connect 4.x on Java 25 | Debezium 3.0 | Cannot reuse `cp-kafka-connect:7.6.13` `[CITED: debezium.io/releases]` |
| `snapshot.mode=never` | `snapshot.mode=no_data` / `initial` | Debezium 3.x naming | Use `initial` here (covers pre-start outbox rows) |
| Celery default control/event queues | Disable them on RabbitMQ 4.x; quorum queues + native delayed delivery for prod | RabbitMQ 4.0 (transient non-exclusive queues disallowed by default) | Worker would not start otherwise |
| RabbitMQ classic mirrored queues | Quorum queues (`delivery-limit` default 20 since 4.0) | RabbitMQ 4.0 | Production recommendation, documented |
| `opentelemetry-exporter-jaeger` | OTLP only | already in repo | unchanged |
| Motor | PyMongo `AsyncMongoClient` | May 2025 | Use for the notification log (`libs/db/mongo.py`) |

**Deprecated/outdated:** Debezium 2.x on Kafka Connect 3.x as the default choice; ZooKeeper Kafka; per-event-type topics where ordering matters.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Grafana 13 file provisioning accepts classic dashboard JSON with an older `schemaVersion` and migrates it on load | Pattern 10 | Dashboards render empty; export once from the UI to refresh the model |
| A2 | `snapshot.locking.mode` default does not block writers during the first snapshot of a tiny outbox table | Pattern 2 | Brief write stall at first connector start; set `snapshot.locking.mode=none` explicitly |
| A3 | `consumer.commit(message=msg, asynchronous=False)` per message is acceptable throughput for this scale | Pattern 3 | Batch commits needed; correctness unchanged |
| A4 | `max_slot_wal_keep_size=2GB`, 3 partitions, `retention.ms=-1` are sensible for a learning build | Pattern 2, 9 | Tune later; no correctness impact |
| A5 | A re-snapshot after Connect offset loss re-emits retained outbox rows (inferred) | Pattern 2 | Replay story weaker; consumers are idempotent anyway |
| A6 | Which subset of the Celery settings is strictly required on RabbitMQ 4.3 (full set verified) | Pitfall 7 | Over-disabled features (remote control); harmless |
| A7 | Reminder leads (24 h and 2 h) and follow-up delay (3 days) are acceptable product defaults | Pattern 7 | Config values only |
| A8 | The residual window (cancel event lagging a firing reminder) is closed well enough by the send-time guard | Pattern 6 | A rare reminder for a cancelled appointment |
| A9 | Retention of Postgres retained-WAL visibility via a SQL panel is acceptable instead of a Postgres exporter | Pitfall 4 | Extra container if an alert is wanted |
| A10 | Recommending classic over quorum queues is acceptable for a single-node dev stack | Standard Stack | Mentor may ask about production; documented answer exists |

## Open Questions

1. **Average wait time definition (D-10).**
   - Known: "appointment start minus check-in" is <= 0 for on-time/late arrivals.
   - Unclear: whether the user wants the literal formula (clamped to 0?) or "time from check-in to visit start".
   - Recommendation: implement D-10 literally in the API and document it; carry `slot_start` and `checked_in_at` in the event; offer `visit.started` as an optional extra event for the better definition. Ask during plan-phase.
2. **Patient deactivation.** "Total patients" from `patient.registered` alone never decreases. Recommend adding `patient.deactivated` (cheap, same hook as registration) and defining total = active patients. Confirm.
3. **Provider schedule-change consumption.** EVENT-01 requires the event to be published. Whether Scheduling should consume it (start `ScheduleChangeWorkflow` via deterministic id, replacing the paused poller) is a separate decision tied to the deferred poller idea. Recommendation: publish only; keep the poller paused; list consumption as an optional follow-up plan flagged for the user.
4. **Outbox retention.** Recommendation: insert-only, no pruning in Phase 3 (keeps the replay source and the debuggable pending state; about 200 bytes per row); document a prune SQL. Alternative (insert then delete in the same transaction) keeps the table empty but loses replay-from-table.
5. **Connect image deviation from "CP line".** Confirm the user accepts `quay.io/debezium/connect` for Connect only (broker and registry stay on 7.6.13).
6. **S3 follow-up (audit via outbox).** Recommendation: defer; moving audit writes into the outbox needs an audit-writing consumer and changes the audit contract, which widens scope. Keep the best-effort audits and record this honestly.
7. **Backfill for already-completed, unstarted visits** (dev data only): do or document.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker + Compose | whole phase | yes | Docker 29.1.5, daemon up; 7.65 GiB VM memory | - |
| Compose stack (21 smarthealth containers) | e2e, spikes | yes, all healthy | current use about 3.1 GiB | - |
| Postgres `wal_level=logical` | D-01 | **no** (currently `replica`) | 15.19 | Required change: Compose `command` + container recreate (data safe) |
| `quay.io/debezium/connect:3.7.1.Final` | D-01 | pulled during research | Connect 4.3.1, Java 25, arm64 | `3.6.4.Final` |
| Schema Registry 7.6.13, Kafka 7.6.13 KRaft | EVENT-01 | yes (healthy); SR global compat BACKWARD; 0 subjects | - | - |
| RabbitMQ 4.3.7 + `rabbitmq_prometheus` | EVENT-04 | yes | 4.3.7 | - |
| Temporal server auto-setup 1.29.7 | NOTIF-02 | yes | SDK 1.34.0 | no official matrix (STATE.md blocker) |
| Jaeger 1.76.0, Prometheus v3.13.3, Grafana 13.0.8 | OBS | yes | - | - |
| Python 3.12 (`.venv` 3.12.15), pytest 9.1.1, pytest-asyncio 1.4.0 | tests | yes (system `python3` is 3.14, not used) | - | - |
| PyPI / Maven Central / Docker Hub / quay.io reachability | installs | yes | - | - |
| `timeout` command | scripts | no on macOS | - | use `python`/`sleep` loops |

**Missing dependencies with no fallback:** none. **Missing with fallback:** `wal_level=logical` (apply the Compose change). Memory headroom: Connect JVM (cap heap) plus about six small Python containers fits within 7.65 GiB.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 + pytest-asyncio 1.4.0 (`asyncio_mode = "auto"`) `[VERIFIED: pyproject.toml, pip list]` |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]`, markers `integration`, `e2e` |
| Quick run command | `.venv/bin/pytest tests/events tests/notification tests/analytics tests/visit -x -q` |
| Full suite command | `.venv/bin/pytest tests -m "not e2e" -q` (last full run: 1349 passed, 2026-11-10 closeout) |
| E2E command | `.venv/bin/pytest tests/e2e -m e2e -q` with the Compose stack up (`SMARTHEALTH_E2E_REQUIRE_STACK=1` to fail instead of skip) |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| EVENT-01 | Every `.avsc` registers and is BACKWARD compatible; no PHI fields | unit/integration | `pytest tests/events/test_schemas.py -x` | Wave 0 |
| EVENT-02 | Transition + outbox row commit atomically; rollback leaves no row; double call is a no-op (uuid5 PK) | integration | `pytest tests/scheduling/test_outbox.py tests/visit/test_outbox.py -x` | Wave 0 |
| EVENT-02 | Commit then kill, Connect stop/start: delivered once in order | e2e | `pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e` | Wave 0 |
| EVENT-03 | Forced redelivery yields one effect per consumer | integration + e2e | `pytest tests/events/test_consumer_idempotency.py -x` | Wave 0 |
| EVENT-04 | Task runs on the worker via RabbitMQ (eager mode unit; real broker e2e) | unit + e2e | `pytest tests/notification/test_tasks.py -x` | Wave 0 |
| EVENT-05 | Retry with backoff; poison -> `notifications.dlq`; Kafka poison -> DLQ topic | integration | `pytest tests/notification/test_dlq.py tests/events/test_consumer_dlq.py -x` | Wave 0 |
| NOTIF-01 | booked/cancelled/rescheduled events produce one task each; booking transaction unaffected | integration | `pytest tests/notification/test_triggers.py -x` | Wave 0 |
| NOTIF-02/03 | Reminder fires at lead, cancel stops it, reschedule moves it, replay of recorded history | workflow (time-skipping) | `pytest tests/workflows/test_reminder_workflow.py tests/workflows/test_replay_reminder.py -x` | Wave 0 |
| NOTIF-04 | Same task twice sends once (Mongo unique index) | unit | `pytest tests/notification/test_dedupe.py -x` | Wave 0 |
| ANLY-01..05 | Projection math for each metric from a fixed event sequence (incl. out-of-order, D-10 definition) | unit/integration | `pytest tests/analytics/test_projections.py tests/analytics/test_api.py -x` | Wave 0 |
| ANLY-06 | Analytics process never imports/queries an OLTP DB; replay rebuilds identical tables | integration | `pytest tests/analytics/test_replay_rebuild.py tests/libs/test_service_isolation.py -x` | Wave 0 (extend isolation test) |
| OBS-01 | Every scrape target answers `/metrics`; dashboards JSON parse; provisioning files reference existing UIDs | unit + e2e | `pytest tests/infra/test_grafana_provisioning.py tests/e2e/test_phase3_metrics.py` | Wave 0 |
| OBS-02 | One trace id across HTTP -> Temporal -> outbox -> consumer -> Celery (in-memory exporter; real Jaeger query in e2e) | integration + e2e | `pytest tests/events/test_trace_chain.py tests/e2e/test_phase3_trace.py` | Wave 0 |
| OBS-03 | Log lines for each flow carry `trace_id`, no PHI | unit | `pytest tests/libs/test_observability.py -x` | exists, extend |
| D-03 | Visit completion starts downstream via consumer; sweeper Schedule absent; backfill | integration + e2e | `pytest tests/visit/test_downstream.py tests/e2e/test_phase3_visit_downstream.py` | exists, rewrite |
| D-04 | `smoke` k6 profile still passes; histograms exist | e2e | `tests/e2e/test_loadtest_smoke.py` | exists |
| DASH-03/04 | New sections/diagrams/Q&A present, self-contained | static | `pytest tests/docs -x` | exists, extend + `PHASE3_TRACE` map |

### Sampling Rate
- **Per task commit:** the matching quick command for the touched area (< 30 s: pure unit/integration with `mock://` registry and eager Celery).
- **Per wave merge:** `.venv/bin/pytest tests -m "not e2e" -q`.
- **Phase gate:** full non-e2e suite + `pytest tests/e2e -m e2e` green before `/gsd-verify-work`.

### Real-stack e2e cost (measured in the spike)
Connect cold start about 20-30 s to healthy; connector RUNNING within about 12 s of the PUT; steady-state outbox -> Kafka delivery was seen within seconds, about 10 s right after a Connect restart. Budget about 60-90 s of stack warm-up per e2e session, then individual tests of 5-30 s. Reminder e2e uses a configurable short lead (for example a slot a few minutes ahead with `lead_minutes=1`), while the timer logic itself is proven with the time-skipping environment. Celery retry tests set a 1 s backoff base through settings.

### Wave 0 Gaps
- [ ] `tests/events/` (schemas, outbox helper, consumer base, trace chain), `tests/notification/`, `tests/analytics/`, `tests/infra/`, `tests/e2e/test_phase3_*.py`
- [ ] Mock registry fixture (`mock://`), Kafka test fixtures (unique topic/group per test against the Compose broker), eager-mode Celery fixture, `tests/workflows/histories/reminder_v1.json`
- [ ] Rebuild images, `alembic upgrade head` on live DBs, `wal_level=logical`, Connect up
- [ ] Update `tests/docs/test_dashboard.py` and add `PHASE3_TRACE` to the traceability test; rewrite sweeper tests (`tests/visit/test_downstream.py`, `tests/workflows/test_visit_schedules.py`, `tests/visit/test_visit_reconciler.py` docstrings)

**Manual-only:** viewing one booking trace in the Jaeger UI, the Grafana dashboards, and the dashboard HTML (visual quality), consistent with the Phase 2 manual checks.

## Security Domain

Config: `security_enforcement` enabled, `security_asvs_level: 1`, `security_block_on: high`.

### Applicable ASVS Categories
| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes (analytics API, service-to-service) | Existing RS256/JWKS verification via `libs/auth`; consumers/workers use service credentials only for infrastructure (DB, broker), no user tokens |
| V3 Session Management | no | - |
| V4 Access Control | yes | Analytics API `require_role`: admin all clinics, front desk own clinic; gateway route table gets `/api/analytics` only; Connect REST not published |
| V5 Input Validation | yes | Avro schema validation on serialize and deserialize; Pydantic for the analytics API; invalid events go to the DLQ, never crash the loop |
| V6 Cryptography | partial | No new crypto; secrets (Debezium DB password, Rabbit credentials) via env/Compose secrets like existing DB passwords; Celery `accept_content=["json"]` (no pickle) |
| V7 Error Handling/Logging | yes | structlog, ids only; DLQ headers carry error TYPE, never message text |
| V8 Data Protection | yes | PHI-free events and schemas (tests), PHI-free Celery args, Kafka/DLQ/Jaeger payloads |
| V14 Configuration | yes | Pinned images/packages; Connect/Kafka ports not published beyond need; Grafana datasource uses a read-only DB role |

### Known Threat Patterns
| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Unauthenticated Kafka Connect REST creating connectors that read any DB | Elevation / Info disclosure | Do not publish 8083; Compose-network only; Debezium DB role limited to REPLICATION + SELECT on `outbox` |
| Forged/duplicated events | Spoofing / Tampering | Registry-validated Avro, deterministic ids, idempotent consumers; local broker has no ACLs (accepted for dev, note for production: Kafka ACLs/TLS) |
| Poison message stalls a consumer | DoS | Non-retryable -> DLQ and commit; bounded in-process retries |
| Stalled slot fills the disk | DoS | `max_slot_wal_keep_size`, heartbeat, ops note |
| PHI leakage into events/traces/logs/DLQ | Information disclosure | IDs-only schemas, schema scan test, no payload logging; DLQ keeps the original bytes (same PHI-free content) |
| Unsafe deserialization in Celery | Tampering / RCE | JSON serializer only |
| Replay of old messages causing duplicate notifications | Repudiation / Tampering | Dedupe key with unique index |
| Metrics endpoints exposing internals | Information disclosure | Internal network only (the gateway `:9100` pattern), no host ports for new `/metrics` |

## Sources

### Primary (HIGH confidence)
- Spike evidence (isolated Compose project `p3spike`, since torn down): Debezium 3.7.1.Final on `quay.io/debezium/connect` vs `cp-kafka:7.6.13` + `cp-schema-registry:7.6.13`; Avro round trip with `EventRouter`; Connect stop/start and `docker kill` delivery behavior; wal_level restart preserving data; failed-task behavior; Celery 5.6.3 + RabbitMQ 4.3 + OTel; Temporal reminder workflow under `start_time_skipping` (temporalio 1.34.0); `mock://` registry client; manual traceparent extraction.
- In-repo files read this session (cited inline with line ranges): `infra/docker-compose.yml`, `infra/prometheus.yml`, `infra/postgres-init/create-service-databases.sh`, `libs/temporal.py`, `libs/observability/*.py`, `libs/db/postgres.py`, `services/scheduling/{repository/appointment_repo.py,activities/booking.py,workflows/booking.py,domain/contracts.py,domain/enums.py,app.py}`, `services/visit/{downstream.py,worker.py,schedules.py,workflows/visit_completed.py,repository/*.py,api/visits.py,domain/states.py}`, `services/gateway/routing.py`, `pyproject.toml`, tests conftests and fixtures.
- PyPI JSON API (versions, dependency metadata, wheels, first-release dates); installed package sources (`confluent_kafka.schema_registry` `_default_conf`, `opentelemetry.instrumentation.confluent_kafka`, `...celery`).
- https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html (options, binary payload, headers, delete handling)
- https://debezium.io/releases/ (3.7.1.Final / 3.6.4.Final, Java 17+, Connect 3.1+, PostgreSQL 14-18)
- https://docs.celeryq.dev/en/stable/userguide/tasks.html (acks_late, Reject, retry/backoff settings)
- https://www.rabbitmq.com/docs/quorum-queues (delivery-limit default 20, poison handling, global QoS)
- https://grafana.com/docs/grafana/latest/administration/provisioning/ (datasource and dashboard providers)

### Secondary (MEDIUM confidence)
- https://docs.celeryq.dev/en/v5.6.3/getting-started/backends-and-brokers/rabbitmq.html (quorum queue notes; does not mention the RabbitMQ 4.0 transient-queue issue, found empirically)

### Tertiary (LOW confidence)
- None relied on without verification.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH, every pin installed and exercised; seam flags (too-new/unknown-downloads) need the human checkpoint but not a replacement.
- Architecture: HIGH for CDC/Avro/consumers/Celery/Temporal (spikes); MEDIUM for analytics SQL and Grafana JSON (not built).
- Pitfalls: HIGH for 1-3, 6, 7, 11 (observed); MEDIUM for the rest (documented or inferred).

**Research date:** 2026-11-10
**Valid until:** about 2026-12-10 (Debezium, Celery 5.7 and confluent-kafka 2.16/4.x are moving; re-verify pins if planning slips)
