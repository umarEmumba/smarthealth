---
phase: 03-event-driven-system-observability
plan: 04
type: execute
wave: 2
depends_on: ["03-01"]
files_modified:
  - libs/events/consumer.py
  - libs/events/tracing.py
  - libs/events/idempotency.py
  - tests/events/conftest.py
  - tests/events/test_consumer_base.py
  - tests/events/test_consumer_dlq.py
  - tests/events/test_consumer_idempotency.py
autonomous: true
requirements: [EVENT-03, EVENT-05, OBS-02]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "One shared consumer base runs a synchronous confluent-kafka poll loop on one persistent asyncio loop, commits an offset only after its handler's effect, and never auto-commits (research Pattern 3)"
    - "The processing span of every consumed event is a CHILD of the producer's span taken from the traceparent header, so a booking stays one trace (D-11, Pitfall 6)"
    - "A poison message (undecodable bytes, a handler's non-retryable error, or retries exhausted) is copied to the group's DLQ topic with error-type headers and the partition moves on; an infrastructure outage (TransientEventError) is retried without dead-lettering (EVENT-05)"
    - "A database-effect consumer can claim an event id in the same transaction as its effect, so a redelivered event changes nothing (D-08 refined, EVENT-03)"
    - "Every consumer exposes lag, processing time, end-to-end lag and DLQ counters on an internal metrics port and touches a heartbeat file (OBS-01 groundwork)"
  artifacts:
    - path: libs/events/consumer.py
      provides: "EventConsumer, EventEnvelope, NonRetryableEventError, TransientEventError, metrics"
      contains: "enable.auto.commit"
    - path: libs/events/tracing.py
      provides: "extract_context(headers) tolerant of None header values"
    - path: libs/events/idempotency.py
      provides: "claim_event, create_processed_events_table, processed_events_table_ddl"
      contains: "ON CONFLICT DO NOTHING"
  key_links:
    - from: libs/events/consumer.py
      to: libs/events/tracing.py
      via: "start_as_current_span(..., context=extract_context(msg.headers()), kind=CONSUMER)"
      pattern: "extract_context"
    - from: libs/events/consumer.py
      to: libs/events/registry.py
      via: "deserializer() decodes the value by its writer schema id"
      pattern: "deserializer"
---

<objective>
Build the one consumer base all three consumer roles use (visit-downstream, notification-trigger, analytics-projection): decode, trace as a child span, run the handler on a persistent loop, commit after the effect, dead-letter poison messages, keep retrying through infrastructure outages, and emit metrics plus a heartbeat. Add the same-transaction processed-event helper for database effects.

Purpose: EVENT-03 (idempotent, duplicate-safe consumers) and EVENT-05 (dead-letter path on the Kafka side) are implemented once here; OBS-02 needs the consumer span to be a child of the outbox traceparent.

Output: `libs/events/consumer.py`, `libs/events/tracing.py`, `libs/events/idempotency.py`, Kafka test fixtures and tests against the live broker (unique topics per test).

Decisions honoured: D-08 and D-08 refined (one group per role, a DLQ topic per group, processed-event table only for database effects), D-11 (manual traceparent extraction, no `opentelemetry-instrumentation-confluent-kafka`), research Pattern 3 and Pitfalls 6, 9, 16. Claude's discretion: three in-process attempts with 0.5 s exponential backoff before DLQ for unexpected errors; `TransientEventError` retried with capped backoff (max 30 s) by re-seeking, so an outage never dead-letters valid events and `max.poll.interval.ms` is never exceeded.

Live stack note: tests use the live Kafka on `localhost:9092` with a unique topic and group per test (created and deleted by fixtures) and the `mock://` registry, so nothing reaches the real event topics. Wave 2 coupling: plans 03-02 and 03-03 run alongside and neither recreates a container (the first stack rebuild is plan 03-05 in wave 3). Plan 03-03 adds catalogue entries during this wave, each only after its schema file exists, and `mock_registry` registers every entry, so these tests see a consistent catalogue at any moment; they only produce `appointment.booked` samples.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@libs/events/registry.py
@libs/events/catalog.py
@libs/events/outbox.py
@libs/db/postgres.py
@libs/observability/otel.py
@libs/observability/logging.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `EventEnvelope` | dataclass | `event_id: uuid.UUID`, `event_type`, `aggregate_id`, `occurred_at: datetime`, `schema_version`, `payload: dict`, `topic`, `partition`, `offset` |
| `EventConsumer` | class | `EventConsumer(*, group_id, topics, handlers, dlq_topic, bootstrap_servers=None, registry_client=None, max_attempts=3, backoff_seconds=0.5, heartbeat_path=None, metrics_port=None, poll_timeout=1.0, consumer_overrides=None)`; `poll_once() -> Outcome | None`; `run(stop: threading.Event | None = None)`; SIGTERM/SIGINT stop |
| `Outcome` | enum | `PROCESSED`, `IGNORED`, `DEAD_LETTERED`, `RETRY_LATER` |
| `NonRetryableEventError`, `TransientEventError` | exceptions | handler signals: dead-letter now / retry without dead-lettering |
| metrics | Prometheus | `events_consumed_total{group,event_type,outcome}`, `event_processing_seconds{group,event_type}`, `event_end_to_end_lag_seconds{group,event_type}`, `dlq_events_total{group,error_type}`, `kafka_consumer_lag{group,topic,partition}` |
| DLQ headers | Kafka | original key, value and headers plus `dlq.error_type`, `dlq.consumer_group`, `dlq.original_topic`, `dlq.original_partition`, `dlq.original_offset`, `dlq.attempts` (no error message text) |
| `extract_context(headers)` | function | `libs/events/tracing.py` |
| `claim_event(db, *, event_id, consumer) -> bool`, `processed_events_table_ddl()`, `create_processed_events_table(op)` | functions | `libs/events/idempotency.py`; table `processed_events(event_id uuid, consumer text, processed_at timestamptz default now(), PRIMARY KEY (event_id, consumer))` |
| fixtures | tests | `kafka_admin`, `unique_topic`, `produce_event()`, `in_memory_spans` in `tests/events/conftest.py` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — an Avro event with a traceparent header is consumed as a child span, handled once, and committed</name>
  <files>libs/events/consumer.py, libs/events/tracing.py, tests/events/conftest.py, tests/events/test_consumer_base.py</files>
  <behavior>
    - An AppointmentBooked message produced inside span P to a unique topic is passed to the `appointment.booked` handler as an `EventEnvelope` whose fields equal the produced record
    - The handler runs inside a span whose trace id equals P's trace id and whose parent span id equals P's span id; span kind CONSUMER; attributes `messaging.system=kafka`, `messaging.destination.name`, `messaging.kafka.consumer.group`, `smarthealth.event_type`, `smarthealth.event_id`
    - After the handler returns, the offset is committed synchronously: a new consumer in the same group, started afterwards, receives nothing
    - Handlers are async and all run on ONE event loop for the consumer's lifetime (the loop identity seen by two handler calls is the same object)
    - A message whose type has no handler is committed and counted as `IGNORED`
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 3, Code Examples "Consumer span as a child", Pitfalls 6, 9, 16)
    - libs/events/registry.py (deserializer, mock registry handling) and tests/events/conftest.py (mock_registry from 03-01)
    - libs/db/postgres.py (engine cache per running loop: why one persistent loop)
    - tests/workflows/test_trace.py (InMemorySpanExporter on the global provider)
  </read_first>
  <action>
    tracing.py: `extract_context(headers)` turns a confluent-kafka header list into a carrier dict, skipping `None` values (Debezium emits a null header when the column is NULL) and decoding bytes as UTF-8 with errors ignored, then returns `opentelemetry.propagate.extract(carrier)`.

    consumer.py: `EventConsumer` wraps `confluent_kafka.Consumer` with `group.id`, `bootstrap.servers` (default `shared_settings.kafka_bootstrap_servers`), `enable.auto.commit=False`, `auto.offset.reset=earliest`, `max.poll.interval.ms=300000`, plus overrides. It owns one `asyncio.Runner` created at construction and used for every handler call (never `asyncio.run` per message). `poll_once()` polls with `poll_timeout`; on a message it decodes the value with `deserializer()` against `SerializationContext(topic, VALUE)`, builds the envelope (event type from the decoded `event_type`, falling back to the `eventType` header), opens `tracer.start_as_current_span(f"{topic} process", context=extract_context(msg.headers()), kind=SpanKind.CONSUMER, attributes=...)`, runs the handler through the runner, then `commit(message=msg, asynchronous=False)` and returns `PROCESSED`. Unknown type: commit, return `IGNORED`. Log one structlog line per message (`event.consumed`, with group, event_type, event_id, outcome; never the payload). `run(stop)` loops `poll_once()` until `stop` is set or SIGTERM/SIGINT arrives, then closes the consumer (commits nothing extra) and the runner.

    Fixtures (tests/events/conftest.py, keep `mock_registry`): `kafka_admin` (AdminClient on `localhost:9092`), `unique_topic` (creates `p3test.<uuid>` with 1 partition, deletes it after), `produce_event(topic, event_type, payload, headers)` (serializes with `serializer_for` on the mock registry, produces, flushes), `in_memory_spans` (attaches an InMemorySpanExporter via SimpleSpanProcessor to the global provider configured by `configure_tracer_provider`, clears between tests).

    Tests (marker `integration`): the five behaviors above against the live broker.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_consumer_base.py -x -q</automated>
    <fails_when>pytest exits non-zero, the consumer span's parent id differs from the producer span id, or a second consumer in the group receives the already-committed message.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_consumer_base.py -q` exits 0 with at least 5 passed
    - `grep -c "enable.auto.commit" libs/events/consumer.py` is at least 1
    - `.venv/bin/python -m pip show opentelemetry-instrumentation-confluent-kafka` exits non-zero (package not installed)
  </acceptance_criteria>
  <done>The consumer base delivers one event to one handler as a child span on a persistent loop and commits only after the effect.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Poison messages, retries, outages, metrics, heartbeat and shutdown</name>
  <files>libs/events/consumer.py, tests/events/test_consumer_dlq.py</files>
  <behavior>
    - Bytes that are not registry-framed Avro are produced to the DLQ topic with the original key, value and headers plus `dlq.error_type=DeserializationError`, `dlq.consumer_group`, `dlq.original_topic`, `dlq.original_partition`, `dlq.original_offset`, `dlq.attempts`; the source offset is committed and the next message on the partition is processed
    - A handler raising `NonRetryableEventError` is dead-lettered after one attempt; a handler raising `ValueError` three times is dead-lettered with `dlq.attempts=3` and `dlq.error_type=ValueError`; a handler failing twice then succeeding is processed once with no DLQ record
    - A handler raising `TransientEventError` is never dead-lettered: the offset is not committed, the consumer re-seeks to the same offset and retries with capped backoff, and once the handler succeeds the event is processed exactly once
    - If producing to the DLQ fails, the source offset is NOT committed and `poll_once` raises (the process restarts and the message is redelivered; nothing is lost)
    - DLQ headers never contain the exception message text
    - A message with a NULL traceparent header is processed (root span, no crash)
    - After processing, `events_consumed_total`, `event_processing_seconds`, `event_end_to_end_lag_seconds` (now minus `occurred_at`) and `dlq_events_total` have samples with the expected labels; `kafka_consumer_lag` is set from the high watermark minus the position; when `metrics_port` is given, `GET http://127.0.0.1:<port>/metrics` returns them
    - The heartbeat file's mtime advances at least once per `poll_timeout` while idle
  </behavior>
  <read_first>
    - libs/events/consumer.py (Task 1)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 3 DLQ rules, Security Domain "DLQ headers carry error TYPE")
    - infra/docker-compose.yml (worker heartbeat-file healthcheck pattern to mirror)
  </read_first>
  <action>
    Extend `poll_once`: deserialization failure or `NonRetryableEventError` -> DLQ immediately; any other exception -> retry in-process up to `max_attempts` with `backoff_seconds * 2**n` sleeps, then DLQ; `TransientEventError` -> do not commit, `seek` the consumer back to the message's partition and offset, sleep `min(30, backoff * 2**n)` and return `RETRY_LATER` (the next poll returns the same message). The DLQ write uses a `confluent_kafka.Producer` with `acks=all` and `enable.idempotence=True`, copies key, value and headers and adds the `dlq.*` headers listed above, then `flush(10)`; when the flush leaves the message undelivered, raise instead of committing. Log `event.dead_lettered` with group, topic, partition, offset, event type and `error_type` only.

    Metrics with `prometheus_client` in the default registry (names in the artifacts table); `kafka_consumer_lag` refreshed at most every 15 s from `get_watermark_offsets` and `position`. When `metrics_port` is set, call `prometheus_client.start_http_server(metrics_port)` once per process (bind `0.0.0.0`; Compose publishes no host port for it). When `heartbeat_path` is set, touch it on every loop iteration, including idle polls and transient retries.

    Tests (marker `integration`) for every behavior above, using unique topics plus a unique DLQ topic per test and a fake handler that records calls; force a DLQ producer failure by pointing the DLQ producer at an unreachable bootstrap address in that one test.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_consumer_dlq.py -x -q</automated>
    <fails_when>pytest exits non-zero, a poison message blocks the partition, a transient error is dead-lettered, a DLQ header carries exception text, or the offset is committed after a failed DLQ write.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_consumer_dlq.py -q` exits 0 with at least 9 passed
    - `grep -c "dlq.error_type" libs/events/consumer.py` is at least 1
    - `grep -c "start_http_server" libs/events/consumer.py` is at least 1
  </acceptance_criteria>
  <done>Poison messages are dead-lettered without stalling the partition, outages are retried without loss, and every consumer reports metrics and liveness.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Same-transaction idempotency for database effects, proven against a forced redelivery</name>
  <files>libs/events/idempotency.py, tests/events/test_consumer_idempotency.py</files>
  <behavior>
    - `claim_event(db, event_id=E, consumer="c")` returns True the first time and False on a second call in a later transaction; different consumers claim the same event id independently
    - A handler that claims the event and inserts an effect row in ONE transaction, run twice for the same message (the second time after rewinding the group to the earlier offset with `consumer.seek` / a fresh consumer on the same group with the offset reset), leaves exactly one effect row
    - If the effect transaction rolls back, the claim rolls back too: the next delivery applies the effect
    - Two concurrent handlers for the same event id (two sessions) produce one effect row and no exception escapes (the loser sees False)
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 4 "Three idempotency flavors")
    - libs/events/outbox.py (`outbox_table_ddl(temporary=...)` style to mirror for the DDL helper)
    - libs/db/postgres.py
  </read_first>
  <action>
    idempotency.py: `processed_events_table_ddl(temporary=False)` (`event_id uuid NOT NULL`, `consumer text NOT NULL`, `processed_at timestamptz NOT NULL DEFAULT now()`, `PRIMARY KEY (event_id, consumer)`), `create_processed_events_table(op)` for Alembic, and `async claim_event(db, *, event_id, consumer) -> bool` running `INSERT INTO processed_events (event_id, consumer) VALUES (:e, :c) ON CONFLICT DO NOTHING RETURNING event_id` on the caller's session (never commits). Module docstring states the rule from D-08 refined: this table is for database effects only; Temporal effects use deterministic workflow ids and Celery effects use the send-side dedupe key.

    Tests (marker `integration`): use the live scheduling_db as a scratch database with TEMP tables (`processed_events` and a one-column effect table) so no real schema changes; the redelivery case drives the real `EventConsumer` with a handler that does claim + insert in one transaction, then rewinds the group (seek to the first offset on a new consumer instance with the same group) and processes again.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_consumer_idempotency.py -x -q && .venv/bin/python -m pytest tests/events -q</automated>
    <fails_when>Either pytest run exits non-zero or the redelivered event produces a second effect row.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_consumer_idempotency.py -q` exits 0 with at least 4 passed
    - `.venv/bin/python -m pytest tests/events -q` exits 0
    - `grep -c "ON CONFLICT DO NOTHING" libs/events/idempotency.py` is at least 1
  </acceptance_criteria>
  <done>Database-effect consumers can make a redelivered event a no-op inside the effect's own transaction.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka topic -> consumer process | message bytes and headers are untrusted input (any producer on the dev broker can write) |
| consumer -> DLQ topic | failed messages are retained for inspection and replay |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-11 | Denial of Service | poison message on a partition | high | mitigate | Non-retryable and exhausted-retry messages go to the group's DLQ and the offset moves on; bounded in-process retries; tests prove the next message is processed |
| T-3-12 | Tampering | forged or replayed events | medium | mitigate | Values must decode against registry schemas (else DLQ); effects are idempotent by event id (`claim_event`), deterministic workflow ids or the send dedupe key; broker ACLs are a documented production item |
| T-3-13 | Information Disclosure | DLQ records and consumer logs | medium | mitigate | DLQ keeps the original PHI-free bytes plus error TYPE headers only (no exception text); logs carry ids and types, never payloads; metrics listen on an internal port with no Compose host mapping |
| T-3-14 | Repudiation | lost events after a failed DLQ write | medium | mitigate | Offset is committed only after the effect or a confirmed DLQ write; a failed DLQ flush raises so the message is redelivered |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan; the linking-only `opentelemetry-instrumentation-confluent-kafka` is deliberately absent (acceptance check) |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/events -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto libs/events`; record status in the SUMMARY
</verification>

<success_criteria>
- One consumer base with child-span tracing, commit-after-effect, DLQ, outage retry, metrics and heartbeat
- A redelivered event produces one database effect
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-04-SUMMARY.md` when done
</output>
