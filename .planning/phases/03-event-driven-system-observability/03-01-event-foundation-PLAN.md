---
phase: 03-event-driven-system-observability
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - pyproject.toml
  - libs/config.py
  - libs/events/__init__.py
  - libs/events/catalog.py
  - libs/events/registry.py
  - libs/events/outbox.py
  - libs/events/schemas/AppointmentBooked.avsc
  - tests/events/__init__.py
  - tests/events/conftest.py
  - tests/events/test_outbox_roundtrip.py
autonomous: false
requirements: [EVENT-01, EVENT-02, OBS-02]

estimate:
  tokens: 65000
  raw_tokens: 65000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "The six Phase 3 packages are installed at their exact pins, and the Debezium Connect image is named for approval, only after a human approved them (repo convention 01.1-04 / 02-06)"
    - "The event contract (Avro + Schema Registry, BACKWARD, one subject per event type named by record, one topic per aggregate, uuid5 event ids, PHI-free fields) was confirmed by the user before any schema file was written (D-05 one-way gate), together with an answer on every item of the user-veto list (Connect image deviation, EVENT-04 projection deviation, S3 deferral, no provider-event consumer, insert-only outbox, backfill)"
    - "A service can call one async helper inside its own transaction to write an outbox row whose payload is registry-framed Avro and whose traceparent is the active span; the helper never commits and a double call writes one row (D-06, EVENT-02)"
    - "Service serializers never register schemas (auto.register.schemas False); the catalogue starts with appointment.booked and plan 03-03 adds the other nine types together with their schema files and the deploy-time registration CLI (D-05)"
  artifacts:
    - path: libs/events/outbox.py
      provides: "enqueue_event, event_id_for, current_traceparent, outbox_table_ddl, outbox_insert_seconds histogram"
      contains: "ON CONFLICT (id) DO NOTHING"
    - path: libs/events/registry.py
      provides: "strict AvroSerializer per event type (auto.register.schemas False, record_subject_name_strategy), warm_up, register_all(client, event_types=None)"
      contains: "auto.register.schemas"
    - path: libs/events/catalog.py
      provides: "EVENT_CATALOG (appointment.booked; extended by plan 03-03), topic names, DLQ topic names, probe topic"
      contains: "smarthealth.appointment.events"
  key_links:
    - from: libs/events/outbox.py
      to: libs/events/registry.py
      via: "serializer_for(event_type) used inside enqueue_event"
      pattern: "serializer_for"
    - from: libs/events/registry.py
      to: libs/events/schemas
      via: "schema files loaded by EVENT_CATALOG record name"
      pattern: "avsc"
---

<objective>
Lay the shared event foundation every later Phase 3 plan builds on: the human-approved package pins (and the named Connect image), the user-confirmed event contract (D-05 is one-way) with the user-veto list answered, then the thinnest real slice of the event library: the catalogue, the strict registry client, the outbox write helper (D-06) and the first schema (`appointment.booked`), proven by a round trip through a live Postgres transaction.

Purpose: EVENT-01 (registered schema) and EVENT-02 (no dual write) start here. Every producer plan only calls `enqueue_event`; every consumer plan uses the same catalogue and registry. Getting the human gates done first keeps waves 2+ unblocked. Plan 03-03 (wave 2) adds the other nine schemas, the deploy-time registration CLI and the infrastructure secret files.

Output: pinned dependencies, `libs/events/` (catalog, registry, outbox, `AppointmentBooked.avsc`), the mock-registry fixture, the round-trip test.

Decisions honoured: D-05 (Avro + Schema Registry, BACKWARD, subjects registered in code, never at runtime), D-05 detail (service-side serialization into a `bytea` payload), D-06 revised (one topic per aggregate, key = aggregate id, uuid event id, traceparent column), D-07 (event set, PHI-free), D-10 revised (events carry check-in, visit-start and slot-start timestamps; `patient.deactivated` added), D-11 (traceparent captured from the producing span). Claude's discretion recorded here (and put to the user in Task 2's veto list): the S3 audit-via-outbox follow-up is NOT moved into the outbox in this phase (it would need an audit consumer and widen scope; the booking accept path stays untouched); provider schedule-change consumption by Scheduling stays deferred (publish only, the poller stays paused); Kafka Connect runs on `quay.io/debezium/connect:3.7.1.Final` (research Open Question 5); analytics projections run on a Kafka consumer rather than Celery (EVENT-04 deviation, see Task 2).
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/ROADMAP.md
@.planning/STATE.md
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@pyproject.toml
@libs/config.py
@libs/db/postgres.py
@libs/observability/otel.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `pyproject.toml` pins | deps | `confluent-kafka[avro,schemaregistry]==2.15.0`, `fastavro==1.13.1`, `celery==5.6.3`, `kombu==5.6.2`, `opentelemetry-instrumentation-celery==0.65b0`, `prometheus-client==0.26.0`; `[tool.setuptools.package-data]` `libs.events = ["schemas/*.avsc"]` |
| `SharedSettings.kafka_bootstrap_servers`, `.schema_registry_url`, `.analytics_service_url` | settings | env `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`), `ANALYTICS_SERVICE_URL` (default `http://analytics:8000`) |
| `libs/events/catalog.py` | module | `EventSpec`, `EVENT_CATALOG` (only `appointment.booked` here; plan 03-03 adds nine entries), `EventType` constants, `AGGREGATE_TOPICS` (appointment, visit, provider, patient, cdcprobe), `topic_for()`, `DLQ_TOPICS`, `CDC_PROBE_TOPIC = "smarthealth.cdcprobe.events"` |
| `libs/events/registry.py` | module | `get_registry_client()`, `load_schema_str()`, `serializer_for()`, `deserializer()`, `register_all(client, event_types=None)`, `warm_up()`, `reset_registry_for_tests()` |
| `libs/events/outbox.py` | module | `EVENT_ID_NAMESPACE`, `event_id_for()`, `current_traceparent()`, `outbox_table_ddl()`, `enqueue_event()`, histogram `outbox_insert_seconds` |
| `libs/events/schemas/AppointmentBooked.avsc` | Avro | `com.smarthealth.events.AppointmentBooked` (the other nine records come in plan 03-03) |
| `tests/events/conftest.py` | fixture | `mock_registry` (`mock://` client with every catalogue entry registered, so later entries are covered automatically) |
| SUMMARY sections | record | "Package approval", "D-05 contract decision", "User-veto list" (one line per item: accepted or vetoed, verbatim reply) |

<tasks>

<task type="checkpoint:human-verify" gate="blocking-human">
  <name>Task 1: Verify the six Phase 3 packages and the Connect image before install (package legitimacy gate: all SUS)</name>
  <what-built>Nothing installed yet. 03-RESEARCH.md "Package Legitimacy Audit" ran the legitimacy seam: every package came back SUS only for `too-new` (newest release of the project is days old) and `unknown-downloads` (no statistic available). Each pin below was installed and exercised in a scratch environment during research. Exact pins (do not float; Celery 5.7.0 is five days old and is NOT used):
    - `confluent-kafka[avro,schemaregistry]==2.15.0` (CLAUDE.md-named; first release 2016; github.com/confluentinc/confluent-kafka-python)
    - `fastavro==1.13.1` (first release 2012; github.com/fastavro/fastavro; fallback 1.12.2)
    - `celery==5.6.3` (CLAUDE.md-named; released 2026-03-26; github.com/celery/celery)
    - `kombu==5.6.2` (CLAUDE.md-named; released 2025-12-29; github.com/celery/kombu)
    - `opentelemetry-instrumentation-celery==0.65b0` (same 0.65b0 line as the approved FastAPI/httpx instrumentation; github.com/open-telemetry/opentelemetry-python-contrib)
    - `prometheus-client==0.26.0` (already installed transitively; promoted to a direct pin; github.com/prometheus/client_python)
    - Transitive via the confluent-kafka extras: `avro 1.12.2`, `authlib 1.9.5`, `cachetools 8.0.0`, `attrs 26.1.0`, `requests 2.34.2`, `certifi`.
    - Not installed on purpose: `opentelemetry-instrumentation-confluent-kafka` (it links instead of parenting the consumer span, research Pitfall 6).
    - Docker image to approve alongside the pins (not a package-manager install, pulled by plan 03-05): `quay.io/debezium/connect:3.7.1.Final` (Debezium project, Kafka Connect 4.3.1, Java 25, arm64; pulled and run against `cp-kafka:7.6.13` during research). It hosts Kafka Connect only. It deviates from the CLAUDE.md "same Confluent Platform line" rule because `cp-kafka-connect:7.6.13` runs Java 11 and cannot load Debezium 3.x (`UnsupportedClassVersionError`, research F1); the broker and Schema Registry stay on `confluentinc` 7.6.13. Fallback tag `3.6.4.Final`. Task 2 also lists this deviation on the user-veto list (research Open Question 5).</what-built>
  <how-to-verify>
    1. Open https://pypi.org/project/confluent-kafka/2.15.0/, https://pypi.org/project/fastavro/1.13.1/, https://pypi.org/project/celery/5.6.3/, https://pypi.org/project/kombu/5.6.2/, https://pypi.org/project/opentelemetry-instrumentation-celery/0.65b0/ and https://pypi.org/project/prometheus-client/0.26.0/; confirm each maintainer and source repository matches the list above.
    2. Open https://quay.io/repository/debezium/connect?tab=tags and confirm the `3.7.1.Final` tag is published under the `debezium` organisation (source: github.com/debezium/container-images).
    3. Optionally run `.venv/bin/python -m pip download --no-deps -d /tmp/p3-pkg-check 'confluent-kafka==2.15.0' fastavro==1.13.1 celery==5.6.3 kombu==5.6.2 opentelemetry-instrumentation-celery==0.65b0 prometheus-client==0.26.0` and confirm the wheel names carry those versions.
    4. Decide: approve all (six pins and the image), or name what to replace (for example "use fastavro 1.12.2" or "use connect 3.6.4.Final").
  </how-to-verify>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Package Legitimacy Audit" and "Standard Stack")
    - pyproject.toml (existing human-vetted pin comments)
  </read_first>
  <resume-signal>Reply "approved" (all six pins and the Connect image), or list what to change with the replacement pin or tag</resume-signal>
  <verify>
    <automated>.venv/bin/python -m pip --version</automated>
    <fails_when>The command exits non-zero, or its output line does not contain both `.venv` and `(python 3.12)` (at planning time it printed pip from `.venv` with python 3.12, so the install target is the project venv); approval itself is the human signal above.</fails_when>
  </verify>
  <acceptance_criteria>
    - The user's reply is recorded verbatim in the plan SUMMARY under "Package approval", including the answer on the Connect image
    - No `pip install` of any listed package ran before the reply
  </acceptance_criteria>
  <done>The user approved the pins and the Connect image (or named replacements, which Task 3 and plan 03-05 then use).</done>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task 2: Confirm the event contract before schemas are written (D-05 is one-way)</name>
  <decision>Freeze the event wire contract that Schema Registry subjects and every consumer will depend on.</decision>
  <context>D-05 is an `[auto-default]` rated one-way: once a subject is registered on the live registry (plan 03-05's schema-init) and consumers depend on it, changing format or naming needs a migration. The research refinements in 03-CONTEXT.md already adjust D-05..D-10; this checkpoint confirms them as one package. Contract proposed:
    - Format: Avro, serialized by the services (strict `AvroSerializer`, `auto.register.schemas=False`), stored as registry-framed bytes in an `outbox.payload bytea` column; Debezium forwards the bytes with `BinaryDataConverter`.
    - Governance: one subject per event type named by the record (`com.smarthealth.events.AppointmentBooked`, `record_subject_name_strategy`), compatibility BACKWARD per subject, registration only by `python -m libs.events.register` at deploy time.
    - Topics: one per aggregate, `smarthealth.appointment.events`, `smarthealth.visit.events`, `smarthealth.provider.events`, `smarthealth.patient.events`, 3 partitions, unlimited retention (replay source), key = aggregate id; DLQ topics `smarthealth.dlq.{visit-downstream,notification-trigger,analytics-projection}`; a test-only `smarthealth.cdcprobe.events` topic that no consumer reads.
    - Event ids: uuid5 of `{aggregate_id}:{event_type}:{discriminator}` (appointment version, visit transition, schedule-change row id, patient deactivation time), so a duplicate emission is impossible at the table's primary key.
    - Event set (D-07 plus refinements): appointment.booked, appointment.cancelled, appointment.rescheduled, appointment.checked_in, appointment.completed, appointment.no_show, visit.completed, provider.schedule_changed, patient.registered, patient.deactivated. Payloads carry ids, types and timestamps only.
    - Average wait time (D-10 revised): check-in to visit start, from `visit.completed` (`checked_in_at`, `started_at`); appointment events also carry `slot_start` so the definition can change by replay.
    - User-veto list (planner choices and deviations; answer each with "accept" or "veto"; a veto stops this plan for replanning of the named plans):
      1. Kafka Connect image deviation (research Open Question 5): Connect runs on `quay.io/debezium/connect:3.7.1.Final`, not a `confluentinc` 7.6.13 image, because `cp-kafka-connect:7.6.13` (Java 11) cannot load Debezium 3.x; the broker and Schema Registry stay on 7.6.13, so the CLAUDE.md broker/registry matching rule still holds. Veto affects plan 03-05.
      2. EVENT-04 projection deviation: REQUIREMENTS.md EVENT-04 lists projections among the background tasks on Celery + RabbitMQ, but D-10 runs the analytics projections on a Kafka consumer (group `analytics-projection`, plans 03-10 and 03-13). Reason: projections are stateful database upserts that need their idempotency claim in the same transaction as the effect, which a Kafka consumer with a processed-event table gives and a Celery task does not (a Celery task would add a second hop with its own redelivery and no shared transaction). Celery stays for notifications and follow-up sends (plans 03-02, 03-06, 03-14). The planner does not edit REQUIREMENTS.md: if you accept, you decide whether to reword EVENT-04; the deviation is recorded in the docs (plan 03-18) and in the PHASE3_TRACE notes (plan 03-20). Veto affects plans 03-10 and 03-13.
      3. S3 audit-via-outbox stays deferred (research Open Question 6); audits after commit stay best effort and the docs say so.
      4. Scheduling does not consume `provider.schedule_changed` in this phase (publish only, the `scheduling-schedule-change-poller` stays paused; research Open Question 3).
      5. The outbox table is insert-only (no pruning; it is the replay source; a prune statement is documented; research Open Question 4).
      6. Completed-but-unstarted visits at cutover are backfilled by a script (plan 03-12; research Open Question 7).</context>
  <options>
    <option id="option-a">
      <name>Approve the contract as listed</name>
      <pros>Spike-verified end to end; per-aggregate ordering holds; one subject per event type as D-05 asks</pros>
      <cons>Schema Registry becomes a cached dependency of the write path (warm-up at startup)</cons>
    </option>
    <option id="option-b">
      <name>One topic per event type</name>
      <pros>Topic name tells the event type</pros>
      <cons>Booked and cancelled for one appointment become unordered relative to each other (research F3); reminders and projections would need reordering logic</cons>
    </option>
    <option id="option-c">
      <name>JSON Schema instead of Avro</name>
      <pros>Human-readable payloads on the topic</pros>
      <cons>Not what the stack docs point at; different serializer classes; no spike evidence</cons>
    </option>
  </options>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-CONTEXT.md (D-05..D-10 and "Research refinements")
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Findings F2, F3, F4, F7; Open Questions 1-7)
  </read_first>
  <resume-signal>Reply "option-a", "option-b", "option-c" (or list edits to the contract, for example a renamed event or a different wait-time definition), plus "accept" or "veto" for each of the six user-veto items</resume-signal>
  <verify>
    <automated>test -f .planning/phases/03-event-driven-system-observability/03-CONTEXT.md</automated>
    <fails_when>The file is missing (exit 1), meaning the decision record to update is not present.</fails_when>
  </verify>
  <acceptance_criteria>
    - The chosen option and any edits are recorded in the plan SUMMARY under "D-05 contract decision"
    - Each of the six user-veto items has a recorded answer (accept or veto, verbatim reply) in the SUMMARY under "User-veto list"; a missing answer counts as not yet decided and the plan does not continue to Task 3
    - If any item is vetoed, the executor stops after recording it and returns the vetoed item and the affected plans for replanning
    - If the user changes the contract, Task 3 here and plan 03-03 use the changed names (record the mapping in the SUMMARY)
  </acceptance_criteria>
  <done>The event contract and every user-veto item are answered and recorded before any `.avsc` file exists.</done>
</task>

<task type="tracer" tdd="true">
  <name>Task 3: Tracer — install the pins, then one appointment.booked event round-trips through an outbox row (mock registry, live Postgres)</name>
  <reversibility rating="costly">The outbox column set and the uuid5 event-id scheme are read by Debezium and by every consumer's idempotency key; changing them later means a migration and re-keyed consumers (D-06).</reversibility>
  <files>pyproject.toml, libs/config.py, libs/events/__init__.py, libs/events/catalog.py, libs/events/registry.py, libs/events/outbox.py, libs/events/schemas/AppointmentBooked.avsc, tests/events/__init__.py, tests/events/conftest.py, tests/events/test_outbox_roundtrip.py</files>
  <behavior>
    - Inside an active span, `enqueue_event(db, event_type="appointment.booked", aggregate_id=..., payload=..., discriminator="1")` inserts exactly one `outbox` row; `aggregatetype` is `appointment`, `aggregateid` is the appointment id, `type` is `appointment.booked`, `schema_version` is 1
    - The row's `payload` decodes with an `AvroDeserializer` on the same registry to the envelope plus payload fields that were passed in
    - The row's `traceparent` carries the active span's trace id (W3C format `00-<32 hex>-<16 hex>-<2 hex>`); with no active span the column is NULL
    - A second call with the same aggregate id, type and discriminator leaves one row and returns the same event id
    - The helper never commits: a second session does not see the row until the caller commits, and a rollback leaves no row
    - A payload missing a required field raises before any INSERT (strict serializer)
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 1 "Outbox write with service-side Avro", the outbox DDL, Code Examples "Producer side" and "Strict serializer test", Pitfalls 8 and 16)
    - pyproject.toml (existing pins and the human-vetted comment style)
    - libs/config.py (SharedSettings alias rules: alias-only binding)
    - libs/db/postgres.py (get_session_factory; per-loop engine cache)
    - libs/observability/otel.py (configure_tracer_provider, get_tracer)
    - tests/workflows/test_trace.py (how an InMemorySpanExporter is attached to the global provider)
    - services/scheduling/config.py (how a host test gets the scheduling_db URL)
  </read_first>
  <action>
    Install: add the six approved pins from Task 1 to `pyproject.toml` `dependencies` with a comment "Human-vetted in plan 03-01 Task 1 (package legitimacy gate)", add `[tool.setuptools.package-data]` with `"libs.events" = ["schemas/*.avsc"]`, then run `.venv/bin/python -m pip install 'confluent-kafka[avro,schemaregistry]==2.15.0' fastavro==1.13.1 celery==5.6.3 kombu==5.6.2 opentelemetry-instrumentation-celery==0.65b0 prometheus-client==0.26.0` and `.venv/bin/python -m pip check`.

    Settings (libs/config.py): add `kafka_bootstrap_servers` (default `localhost:9092`), `schema_registry_url` (default `http://localhost:8081`) and `analytics_service_url` (default `http://analytics:8000`) to `SharedSettings`, following the existing comment-block style.

    Catalogue (libs/events/catalog.py): a frozen `EventSpec(event_type, aggregate_type, record_name, schema_file)` and `EVENT_CATALOG` keyed by event type. In this plan it holds only `appointment.booked` (aggregate `appointment`); plan 03-03 adds the other nine types confirmed in Task 2 at the same time as their schema files, so the catalogue never names a missing file (appointment.* map to aggregate `appointment`, visit.completed to `visit`, provider.schedule_changed to `provider`, patient.* to `patient`). Also define now, for all aggregates: `AGGREGATE_TOPICS` mapping `appointment`, `visit`, `provider`, `patient` and `cdcprobe` to `smarthealth.<aggregate>.events`; `topic_for(event_type)`; `DLQ_TOPICS` for the groups `visit-downstream`, `notification-trigger`, `analytics-projection` as `smarthealth.dlq.<group>`; `CDC_PROBE_TOPIC`. Record names are `com.smarthealth.events.<PascalName>`.

    Registry (libs/events/registry.py): `get_registry_client()` returns one process-wide `SchemaRegistryClient.new_client({"url": shared_settings.schema_registry_url})` (a `mock://` URL yields the mock client); `load_schema_str(event_type)` reads the `.avsc` via `importlib.resources`; `serializer_for(event_type)` caches one `AvroSerializer` per type with conf `auto.register.schemas=False` and `subject.name.strategy=record_subject_name_strategy` (D-05: services never register); `deserializer()` returns one generic `AvroDeserializer(client)`; `register_all(client, event_types=None)` registers every catalogue schema (or the given subset) under its record-name subject and returns `{subject: schema_id}` (used by tests with `mock://` and by plan 03-03's deploy-time CLI); `async warm_up(event_types, attempts=10, delay_seconds=2.0)` resolves each serializer's schema id once by serializing a sample in `asyncio.to_thread`, retrying, and raises after the last attempt; `reset_registry_for_tests()` drops the caches.

    Outbox (libs/events/outbox.py): constant `EVENT_ID_NAMESPACE` (one fixed UUID literal), `event_id_for(aggregate_id, event_type, discriminator)` = uuid5 of `f"{aggregate_id}:{event_type}:{discriminator}"`; `current_traceparent()` via `opentelemetry.propagate.inject` into a dict (None when no valid span); `outbox_table_ddl(temporary=False)` returning the research DDL (`id uuid PK`, `aggregatetype`, `aggregateid`, `type`, `payload bytea`, `traceparent`, `schema_version integer default 1`, `occurred_at timestamptz default now()`), as `CREATE TEMP TABLE` when temporary; `async enqueue_event(db, *, event_type, aggregate_id, payload, discriminator, occurred_at=None) -> uuid.UUID` builds the envelope (`event_id`, `event_type`, `schema_version`, `occurred_at`, `aggregate_id`) merged with `payload`, serializes with `serializer_for` against `SerializationContext(topic_for(event_type), MessageField.VALUE)` (if that type is not warmed yet, run the serialization in `asyncio.to_thread` so no blocking registry call ever happens on the event loop), then executes one `INSERT INTO outbox (...) VALUES (...) ON CONFLICT (id) DO NOTHING` through the caller's `AsyncSession` (no flush-commit, never `commit`), observing a `prometheus_client` Histogram `outbox_insert_seconds` labelled `event_type`. `libs/events/__init__.py` re-exports `enqueue_event` and `EVENT_CATALOG`.

    Schema: `libs/events/schemas/AppointmentBooked.avsc`, record `com.smarthealth.events.AppointmentBooked` with envelope fields first (`event_id` string with logicalType uuid, `event_type` string, `schema_version` int, `occurred_at` long timestamp-millis, `aggregate_id` string) then `appointment_id`, `clinic_id`, `patient_id`, `provider_id`, `slot_id` (uuid strings), `slot_start`, `slot_end`, `booked_at` (timestamp-millis). No names, contact data or coverage references.

    Tests: `tests/events/conftest.py` fixture `mock_registry` sets `SCHEMA_REGISTRY_URL=mock://p3-tests` for the test, resets the registry caches, runs `register_all` over every catalogue entry (so plan 03-03's additions are covered without changing the fixture), yields the client and resets again. `tests/events/test_outbox_roundtrip.py` (marker `integration`) opens a session on the live scheduling_db (scheduling_svc URL from `services.scheduling.config`), creates the outbox as a TEMP table via `outbox_table_ddl(temporary=True)` (so nothing reaches the real table or CDC), and covers the behaviors above with an InMemorySpanExporter attached to the global provider.
  </action>
  <verify>
    <automated>.venv/bin/python -m pip check && .venv/bin/python -m pytest tests/events/test_outbox_roundtrip.py -x -q</automated>
    <fails_when>pip check reports a conflict, pytest exits non-zero, `ModuleNotFoundError: confluent_kafka` appears, or the traceparent trace id differs from the span's.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -c "import confluent_kafka, fastavro, celery, kombu, prometheus_client; print(confluent_kafka.version()[0], fastavro.__version__, celery.__version__, kombu.__version__)"` prints `2.15.0 1.13.1 5.6.3 5.6.2` (or the replacement pins the user named)
    - `grep -c "celery==5.6.3" pyproject.toml` is 1 and `grep -c "libs.events" pyproject.toml` is at least 1
    - `.venv/bin/python -m pytest tests/events/test_outbox_roundtrip.py -q` exits 0 with at least 6 passed
    - `grep -c "auto.register.schemas" libs/events/registry.py` is at least 1
    - `grep -c "ON CONFLICT (id) DO NOTHING" libs/events/outbox.py` is 1
  </acceptance_criteria>
  <done>The pins are installed and one event round-trips from an active span through the outbox SQL and the strict serializer.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| service process -> outbox table | payload bytes and traceparent become a durable, replayable record read by every consumer |
| service process -> Schema Registry | services only read schema ids; they must never write subjects |
| human approver -> package index and image registry | the pins and the Connect image are trusted only after approval |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-01 | Information Disclosure | `AppointmentBooked.avsc` and outbox payloads | high | mitigate | Ids, types and timestamps only (no names, contact data or coverage references); strict serializer rejects fields not in the schema; no payload is ever logged; the PHI field-name scan over every schema is added in plan 03-03 (T-3-56) |
| T-3-57 | Tampering | runtime schema registration by a service | medium | mitigate | `auto.register.schemas=False` and `record_subject_name_strategy` in every `serializer_for` serializer (acceptance grep); services resolve ids only; plan 03-03's deploy-time CLI is the only writer (T-3-02) |
| T-3-04 | Denial of Service | Schema Registry as a write-path dependency | medium | mitigate | `warm_up` resolves ids at startup with retries; an unwarmed serialization runs in a thread so the event loop never blocks; `outbox_insert_seconds` histogram measures the cost; residual risk (cold start during a registry outage cannot become ready) documented in plan 03-18 |
| T-3-SC | Tampering | npm/pip/cargo installs | high | mitigate | Package-legitimacy gate: blocking human checkpoint (Task 1) for the six SUS packages and the Connect image before any install or pull; exact pins and an exact image tag; the linking-only OTel Kafka instrumentation is excluded |
</threat_model>

<verification>
- `.venv/bin/python -m pip check`
- `.venv/bin/python -m pytest tests/events -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (do not set OTEL_SDK_DISABLED) still passes: the new settings and packages must not break existing suites
- Semgrep: `semgrep --config auto libs/events`; record the status in the SUMMARY
</verification>

<success_criteria>
- Approved pins installed and the Connect image approved; contract confirmed and every user-veto item answered and recorded
- `enqueue_event` writes one registry-framed Avro row per (aggregate, type, discriminator) inside the caller's transaction, with the producing span's traceparent
- The catalogue, strict registry client and first schema are ready for plan 03-03 to extend
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md` when done
</output>
