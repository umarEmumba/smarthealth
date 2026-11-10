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
  - libs/events/register.py
  - libs/events/schemas/AppointmentBooked.avsc
  - libs/events/schemas/AppointmentCancelled.avsc
  - libs/events/schemas/AppointmentRescheduled.avsc
  - libs/events/schemas/AppointmentCheckedIn.avsc
  - libs/events/schemas/AppointmentCompleted.avsc
  - libs/events/schemas/AppointmentNoShow.avsc
  - libs/events/schemas/VisitCompleted.avsc
  - libs/events/schemas/ProviderScheduleChanged.avsc
  - libs/events/schemas/PatientRegistered.avsc
  - libs/events/schemas/PatientDeactivated.avsc
  - scripts/generate_infra_secrets.py
  - tests/events/__init__.py
  - tests/events/conftest.py
  - tests/events/test_outbox_roundtrip.py
  - tests/events/test_schemas.py
  - tests/tools/test_generate_infra_secrets.py
autonomous: false
requirements: [EVENT-01, EVENT-02, OBS-02]

estimate:
  tokens: 85000
  raw_tokens: 85000
  tasks: 4
  confidence: low

must_haves:
  truths:
    - "The six Phase 3 packages are installed at their exact pins only after a human approved them (repo convention 01.1-04 / 02-06)"
    - "The event contract (Avro + Schema Registry, BACKWARD, one subject per event type named by record, one topic per aggregate, uuid5 event ids, PHI-free fields) was confirmed by the user before any schema file was written (D-05 one-way gate)"
    - "A service can call one async helper inside its own transaction to write an outbox row whose payload is registry-framed Avro and whose traceparent is the active span; the helper never commits and a double call writes one row (D-06, EVENT-02)"
    - "Every event type of D-07 (plus patient.deactivated from the research refinements) has a schema file with ids, types and timestamps only, enforced by a test that scans for PHI field names (D-07)"
    - "Services never register schemas at runtime; a registration module run at deploy time is the only writer (D-05)"
    - "The Debezium, Grafana read-only and notification broker credentials exist only as gitignored files written with mode 0600"
  artifacts:
    - path: libs/events/outbox.py
      provides: "enqueue_event, event_id_for, current_traceparent, outbox_table_ddl, outbox_insert_seconds histogram"
      contains: "ON CONFLICT (id) DO NOTHING"
    - path: libs/events/registry.py
      provides: "strict AvroSerializer per event type (auto.register.schemas False, record_subject_name_strategy), warm_up, register_all"
      contains: "auto.register.schemas"
    - path: libs/events/catalog.py
      provides: "EVENT_CATALOG, topic names, DLQ topic names"
      contains: "smarthealth.appointment.events"
    - path: libs/events/register.py
      provides: "python -m libs.events.register: BACKWARD compatibility per subject, idempotent registration"
    - path: scripts/generate_infra_secrets.py
      provides: "infra/secrets/infra/{debezium.properties,grafana_ro.secret,notification_broker.secret}"
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
Lay the shared event foundation every later Phase 3 plan builds on: the human-approved package pins, the user-confirmed event contract (D-05 is one-way), the event catalogue and Avro schemas, the strict registry client, the outbox write helper (D-06) and the gitignored infrastructure secrets.

Purpose: EVENT-01 (registered schema) and EVENT-02 (no dual write) start here. Every producer plan only calls `enqueue_event`; every consumer plan uses the same catalogue and registry. Getting the human gates done first keeps waves 2+ unblocked.

Output: pinned dependencies, `libs/events/` (catalog, registry, outbox, register module, ten `.avsc` files), `scripts/generate_infra_secrets.py`, tests.

Decisions honoured: D-05 (Avro + Schema Registry, BACKWARD, subjects registered in code, never at runtime), D-05 detail (service-side serialization into a `bytea` payload), D-06 revised (one topic per aggregate, key = aggregate id, uuid event id, traceparent column), D-07 (event set, PHI-free), D-10 revised (events carry check-in, visit-start and slot-start timestamps; `patient.deactivated` added), D-11 (traceparent captured from the producing span). Claude's discretion recorded here: the S3 audit-via-outbox follow-up is NOT moved into the outbox in this phase (it would need an audit consumer and widen scope; the booking accept path stays untouched); provider schedule-change consumption by Scheduling stays deferred (publish only, the poller stays paused).
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
@scripts/generate_service_secrets.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `pyproject.toml` pins | deps | `confluent-kafka[avro,schemaregistry]==2.15.0`, `fastavro==1.13.1`, `celery==5.6.3`, `kombu==5.6.2`, `opentelemetry-instrumentation-celery==0.65b0`, `prometheus-client==0.26.0`; `[tool.setuptools.package-data]` `libs.events = ["schemas/*.avsc"]` |
| `SharedSettings.kafka_bootstrap_servers`, `.schema_registry_url`, `.analytics_service_url` | settings | env `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`), `ANALYTICS_SERVICE_URL` (default `http://analytics:8000`) |
| `libs/events/catalog.py` | module | `EventSpec`, `EVENT_CATALOG`, `EventType` constants, `AGGREGATE_TOPICS`, `topic_for()`, `DLQ_TOPICS`, `CDC_PROBE_TOPIC = "smarthealth.cdcprobe.events"` |
| `libs/events/registry.py` | module | `get_registry_client()`, `load_schema_str()`, `serializer_for()`, `deserializer()`, `register_all()`, `warm_up()`, `reset_registry_for_tests()` |
| `libs/events/outbox.py` | module | `EVENT_ID_NAMESPACE`, `event_id_for()`, `current_traceparent()`, `outbox_table_ddl()`, `enqueue_event()`, histogram `outbox_insert_seconds` |
| `libs/events/register.py` | CLI module | `python -m libs.events.register [--url] [--check]` |
| `libs/events/schemas/*.avsc` | Avro | `com.smarthealth.events.{AppointmentBooked, AppointmentCancelled, AppointmentRescheduled, AppointmentCheckedIn, AppointmentCompleted, AppointmentNoShow, VisitCompleted, ProviderScheduleChanged, PatientRegistered, PatientDeactivated}` |
| `scripts/generate_infra_secrets.py` | script | writes `infra/secrets/infra/debezium.properties` (`password=...`), `grafana_ro.secret`, `notification_broker.secret` |
| `tests/events/conftest.py` | fixture | `mock_registry` (`mock://` client with every schema registered) |

<tasks>

<task type="checkpoint:human-verify" gate="blocking-human">
  <name>Task 1: Verify the six Phase 3 packages before install (package legitimacy gate: all SUS)</name>
  <what-built>Nothing installed yet. 03-RESEARCH.md "Package Legitimacy Audit" ran the legitimacy seam: every package came back SUS only for `too-new` (newest release of the project is days old) and `unknown-downloads` (no statistic available). Each pin below was installed and exercised in a scratch environment during research. Exact pins (do not float; Celery 5.7.0 is five days old and is NOT used):
    - `confluent-kafka[avro,schemaregistry]==2.15.0` (CLAUDE.md-named; first release 2016; github.com/confluentinc/confluent-kafka-python)
    - `fastavro==1.13.1` (first release 2012; github.com/fastavro/fastavro; fallback 1.12.2)
    - `celery==5.6.3` (CLAUDE.md-named; released 2026-03-26; github.com/celery/celery)
    - `kombu==5.6.2` (CLAUDE.md-named; released 2025-12-29; github.com/celery/kombu)
    - `opentelemetry-instrumentation-celery==0.65b0` (same 0.65b0 line as the approved FastAPI/httpx instrumentation; github.com/open-telemetry/opentelemetry-python-contrib)
    - `prometheus-client==0.26.0` (already installed transitively; promoted to a direct pin; github.com/prometheus/client_python)
    - Transitive via the confluent-kafka extras: `avro 1.12.2`, `authlib 1.9.5`, `cachetools 8.0.0`, `attrs 26.1.0`, `requests 2.34.2`, `certifi`.
    - Not installed on purpose: `opentelemetry-instrumentation-confluent-kafka` (it links instead of parenting the consumer span, research Pitfall 6).
    - For awareness (Docker image, not a package manager install): `quay.io/debezium/connect:3.7.1.Final` hosts Kafka Connect only; `cp-kafka-connect:7.6.13` cannot load Debezium 3.x (Java 11). Broker and Schema Registry stay on 7.6.13 (D-01 detail).</what-built>
  <how-to-verify>
    1. Open https://pypi.org/project/confluent-kafka/2.15.0/, https://pypi.org/project/fastavro/1.13.1/, https://pypi.org/project/celery/5.6.3/, https://pypi.org/project/kombu/5.6.2/, https://pypi.org/project/opentelemetry-instrumentation-celery/0.65b0/ and https://pypi.org/project/prometheus-client/0.26.0/; confirm each maintainer and source repository matches the list above.
    2. Optionally run `.venv/bin/python -m pip download --no-deps -d /tmp/p3-pkg-check 'confluent-kafka==2.15.0' fastavro==1.13.1 celery==5.6.3 kombu==5.6.2 opentelemetry-instrumentation-celery==0.65b0 prometheus-client==0.26.0` and confirm the wheel names carry those versions.
    3. Decide: approve all, or name the packages to replace (for example "use fastavro 1.12.2").
  </how-to-verify>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Package Legitimacy Audit" and "Standard Stack")
    - pyproject.toml (existing human-vetted pin comments)
  </read_first>
  <resume-signal>Reply "approved" (all six pins), or list the packages to change with the replacement pin</resume-signal>
  <verify>
    <automated>.venv/bin/python -m pip --version</automated>
    <fails_when>The command exits non-zero (the venv is broken and no install can happen); approval itself is the human signal above.</fails_when>
  </verify>
  <acceptance_criteria>
    - The user's reply is recorded verbatim in the plan SUMMARY under "Package approval"
    - No `pip install` of any listed package ran before the reply
  </acceptance_criteria>
  <done>The user approved the pins (or named replacements, which Task 3 then uses).</done>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task 2: Confirm the event contract before schemas are written (D-05 is one-way)</name>
  <decision>Freeze the event wire contract that Schema Registry subjects and every consumer will depend on.</decision>
  <context>D-05 is an `[auto-default]` rated one-way: once a subject is registered on the live registry (plan 03-02's schema-init) and consumers depend on it, changing format or naming needs a migration. The research refinements in 03-CONTEXT.md already adjust D-05..D-10; this checkpoint confirms them as one package. Contract proposed:
    - Format: Avro, serialized by the services (strict `AvroSerializer`, `auto.register.schemas=False`), stored as registry-framed bytes in an `outbox.payload bytea` column; Debezium forwards the bytes with `BinaryDataConverter`.
    - Governance: one subject per event type named by the record (`com.smarthealth.events.AppointmentBooked`, `record_subject_name_strategy`), compatibility BACKWARD per subject, registration only by `python -m libs.events.register` at deploy time.
    - Topics: one per aggregate, `smarthealth.appointment.events`, `smarthealth.visit.events`, `smarthealth.provider.events`, `smarthealth.patient.events`, 3 partitions, unlimited retention (replay source), key = aggregate id; DLQ topics `smarthealth.dlq.{visit-downstream,notification-trigger,analytics-projection}`; a test-only `smarthealth.cdcprobe.events` topic that no consumer reads.
    - Event ids: uuid5 of `{aggregate_id}:{event_type}:{discriminator}` (appointment version, visit transition, schedule-change row id, patient deactivation time), so a duplicate emission is impossible at the table's primary key.
    - Event set (D-07 plus refinements): appointment.booked, appointment.cancelled, appointment.rescheduled, appointment.checked_in, appointment.completed, appointment.no_show, visit.completed, provider.schedule_changed, patient.registered, patient.deactivated. Payloads carry ids, types and timestamps only.
    - Average wait time (D-10 revised): check-in to visit start, from `visit.completed` (`checked_in_at`, `started_at`); appointment events also carry `slot_start` so the definition can change by replay.
    - Planner discretion choices recorded for your veto: S3 audit-via-outbox stays deferred; Scheduling does not consume `provider.schedule_changed` in this phase (publish only, poller stays paused); the outbox table is insert-only (no pruning, documented prune SQL); completed-but-unstarted visits at cutover are backfilled by a script (plan 03-10).</context>
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
  <resume-signal>Reply "option-a", "option-b", "option-c", or list edits to the contract (for example a renamed event or a different wait-time definition)</resume-signal>
  <verify>
    <automated>test -f .planning/phases/03-event-driven-system-observability/03-CONTEXT.md</automated>
    <fails_when>The file is missing (exit 1), meaning the decision record to update is not present.</fails_when>
  </verify>
  <acceptance_criteria>
    - The chosen option and any edits are recorded in the plan SUMMARY under "D-05 contract decision"
    - If the user changes the contract, Tasks 3 and 4 use the changed names (record the mapping in the SUMMARY)
  </acceptance_criteria>
  <done>The event contract is confirmed and recorded before any `.avsc` file exists.</done>
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

    Catalogue (libs/events/catalog.py): a frozen `EventSpec(event_type, aggregate_type, record_name, schema_file)` and `EVENT_CATALOG` keyed by event type for all ten types confirmed in Task 2 (appointment.* map to aggregate `appointment`, visit.completed to `visit`, provider.schedule_changed to `provider`, patient.* to `patient`); `AGGREGATE_TOPICS` mapping each aggregate to `smarthealth.<aggregate>.events`; `topic_for(event_type)`; `DLQ_TOPICS` for the groups `visit-downstream`, `notification-trigger`, `analytics-projection` as `smarthealth.dlq.<group>`; `CDC_PROBE_TOPIC`. Record names are `com.smarthealth.events.<PascalName>`.

    Registry (libs/events/registry.py): `get_registry_client()` returns one process-wide `SchemaRegistryClient.new_client({"url": shared_settings.schema_registry_url})` (a `mock://` URL yields the mock client); `load_schema_str(event_type)` reads the `.avsc` via `importlib.resources`; `serializer_for(event_type)` caches one `AvroSerializer` per type with conf `auto.register.schemas=False` and `subject.name.strategy=record_subject_name_strategy` (D-05: services never register); `deserializer()` returns one generic `AvroDeserializer(client)`; `register_all(client)` registers every catalogue schema under its record-name subject and returns `{subject: schema_id}` (used by tests with `mock://` and by Task 4's CLI); `async warm_up(event_types, attempts=10, delay_seconds=2.0)` resolves each serializer's schema id once by serializing a sample in `asyncio.to_thread`, retrying, and raises after the last attempt; `reset_registry_for_tests()` drops the caches.

    Outbox (libs/events/outbox.py): constant `EVENT_ID_NAMESPACE` (one fixed UUID literal), `event_id_for(aggregate_id, event_type, discriminator)` = uuid5 of `f"{aggregate_id}:{event_type}:{discriminator}"`; `current_traceparent()` via `opentelemetry.propagate.inject` into a dict (None when no valid span); `outbox_table_ddl(temporary=False)` returning the research DDL (`id uuid PK`, `aggregatetype`, `aggregateid`, `type`, `payload bytea`, `traceparent`, `schema_version integer default 1`, `occurred_at timestamptz default now()`), as `CREATE TEMP TABLE` when temporary; `async enqueue_event(db, *, event_type, aggregate_id, payload, discriminator, occurred_at=None) -> uuid.UUID` builds the envelope (`event_id`, `event_type`, `schema_version`, `occurred_at`, `aggregate_id`) merged with `payload`, serializes with `serializer_for` against `SerializationContext(topic_for(event_type), MessageField.VALUE)` (if that type is not warmed yet, run the serialization in `asyncio.to_thread` so no blocking registry call ever happens on the event loop), then executes one `INSERT INTO outbox (...) VALUES (...) ON CONFLICT (id) DO NOTHING` through the caller's `AsyncSession` (no flush-commit, never `commit`), observing a `prometheus_client` Histogram `outbox_insert_seconds` labelled `event_type`. `libs/events/__init__.py` re-exports `enqueue_event` and `EVENT_CATALOG`.

    Schema: `libs/events/schemas/AppointmentBooked.avsc`, record `com.smarthealth.events.AppointmentBooked` with envelope fields first (`event_id` string with logicalType uuid, `event_type` string, `schema_version` int, `occurred_at` long timestamp-millis, `aggregate_id` string) then `appointment_id`, `clinic_id`, `patient_id`, `provider_id`, `slot_id` (uuid strings), `slot_start`, `slot_end`, `booked_at` (timestamp-millis). No names, contact data or coverage references.

    Tests: `tests/events/conftest.py` fixture `mock_registry` sets `SCHEMA_REGISTRY_URL=mock://p3-tests` for the test, resets the registry caches, runs `register_all`, yields the client and resets again. `tests/events/test_outbox_roundtrip.py` (marker `integration`) opens a session on the live scheduling_db (scheduling_svc URL from `services.scheduling.config`), creates the outbox as a TEMP table via `outbox_table_ddl(temporary=True)` (so nothing reaches the real table or CDC), and covers the behaviors above with an InMemorySpanExporter attached to the global provider.
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

<task type="auto" tdd="true">
  <name>Task 4: The full schema set, the registration module, the PHI scan, and the infrastructure secret files</name>
  <files>libs/events/schemas/AppointmentCancelled.avsc, libs/events/schemas/AppointmentRescheduled.avsc, libs/events/schemas/AppointmentCheckedIn.avsc, libs/events/schemas/AppointmentCompleted.avsc, libs/events/schemas/AppointmentNoShow.avsc, libs/events/schemas/VisitCompleted.avsc, libs/events/schemas/ProviderScheduleChanged.avsc, libs/events/schemas/PatientRegistered.avsc, libs/events/schemas/PatientDeactivated.avsc, libs/events/register.py, scripts/generate_infra_secrets.py, tests/events/test_schemas.py, tests/tools/test_generate_infra_secrets.py</files>
  <behavior>
    - Every `EVENT_CATALOG` entry has exactly one `.avsc`, and every `.avsc` belongs to a catalogue entry; each parses with `fastavro.parse_schema`; namespace is `com.smarthealth.events`; the five envelope fields come first
    - No schema contains a field whose name contains any of: name, birth, dob, phone, address, email, coverage, ssn, insurance, diagnosis, note
    - Each event type serializes a sample through `serializer_for` and decodes back with `deserializer()` on the mock registry
    - `register.main(["--url", "mock://p3-register"])` registers all ten subjects (record full names), sets BACKWARD on each subject, prints one `subject -> id` line per subject and nothing else, and a second run returns the same ids
    - `register.main([...,"--check"])` exits 1 and names the subject when a schema is incompatible (driven through a fake client whose compatibility check returns False)
    - `generate_infra_secrets.main(["--output-dir", tmp])` writes `debezium.properties` (single line `password=<value>`), `grafana_ro.secret` and `notification_broker.secret` with mode 0600 in a 0700 directory, prints paths only, refuses to overwrite without `--force` (exit 1, nothing written)
  </behavior>
  <read_first>
    - libs/events/catalog.py, libs/events/registry.py, libs/events/schemas/AppointmentBooked.avsc (from Task 3; same envelope and style)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 9 projection columns, Pattern 7 reminder inputs, Pitfall 15)
    - scripts/generate_service_secrets.py (safety properties and output style to mirror)
    - services/scheduling/repository/models.py (Appointment columns available to events), services/visit/repository/models.py (Visit: appointment_id, clinic_id, patient_id, provider_id, checked_in_at, started_at, completed_at), services/provider/repository/models.py (ScheduleChangeEvent: id, provider_id, change_type, template_id), services/patient/repository/models.py (patient id, clinic_id, deactivated_at)
  </read_first>
  <action>
    Schemas (same envelope as AppointmentBooked; optional fields are `["null", type]` with default null so later additions stay BACKWARD-compatible):
    AppointmentCancelled (appointment, clinic, patient, provider, slot ids, `slot_start`, `cancelled_at`); AppointmentRescheduled (appointment, clinic, patient, provider ids, `old_slot_id`, `new_slot_id`, `old_slot_start`, `new_slot_start`, `new_slot_end`, `rescheduled_at`); AppointmentCheckedIn (appointment, clinic, patient, provider ids, `slot_start`, `checked_in_at`); AppointmentCompleted (appointment, clinic, patient, provider ids, `slot_start`, `completed_at`); AppointmentNoShow (appointment, clinic, patient, provider ids, `slot_start`, `marked_at`); VisitCompleted (`visit_id`, `appointment_id`, `clinic_id`, `patient_id`, `provider_id`, `checked_in_at`, optional `started_at`, `completed_at`); ProviderScheduleChanged (`provider_id`, `change_event_id`, `change_type` string, optional `template_id`, `changed_at`); PatientRegistered (`patient_id`, `clinic_id`, `registered_at`); PatientDeactivated (`patient_id`, `clinic_id`, `deactivated_at`). Ids are uuid-logical strings; timestamps are long timestamp-millis.

    Registration module (libs/events/register.py, runnable as `python -m libs.events.register`): arguments `--url` (default `shared_settings.schema_registry_url`) and `--check` (compatibility check only, registers nothing). For each catalogue entry: if the subject exists, test compatibility of the local schema against the latest version and stop with exit 1 naming the subject when incompatible; set the subject's compatibility to BACKWARD; register. Print `subject -> schema id` per subject. Never print schema bodies, URLs with credentials or tracebacks; exit 1 on any registry error with the exception class name only. This is the only writer of the registry (D-05); plan 03-02 runs it as the Compose `schema-init` job.

    Secrets script (scripts/generate_infra_secrets.py): a sibling of `generate_service_secrets.py` with the same safety properties (0700 directory, 0600 files via `os.open` with O_EXCL unless `--force`, refuse to overwrite, print paths only, `secrets.token_urlsafe(32)`), default output `infra/secrets/infra`, files `debezium.properties` (content `password=<value>` plus newline, the format Kafka Connect's FileConfigProvider reads), `grafana_ro.secret`, `notification_broker.secret`. Document in the module docstring which Compose service reads which file (db-init and connect: debezium; db-init and grafana: grafana_ro; notification processes and rabbitmq-init: notification_broker). After the tests pass, run `.venv/bin/python scripts/generate_infra_secrets.py` once so the files exist for plans 03-02 and 03-04, and confirm with `git check-ignore infra/secrets/infra/debezium.properties` that they are ignored (never `cat` them).

    Tests: `tests/events/test_schemas.py` (catalogue/file parity, parse, envelope order, PHI field-name scan, round trip per type, register idempotency and BACKWARD calls via a spy on the mock client, `--check` failure path via a fake client) and `tests/tools/test_generate_infra_secrets.py` (modes, content format, refusal without `--force`, stdout contains paths and no secret value).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events tests/tools/test_generate_infra_secrets.py -x -q && .venv/bin/python -m libs.events.register --help</automated>
    <fails_when>pytest exits non-zero, a PHI field name is found, the catalogue and schema files disagree, or `--help` exits non-zero.</fails_when>
  </verify>
  <acceptance_criteria>
    - `ls libs/events/schemas/*.avsc | wc -l` prints 10
    - `.venv/bin/python -m pytest tests/events/test_schemas.py -q` exits 0 with at least 12 passed
    - `.venv/bin/python -m pytest tests/tools/test_generate_infra_secrets.py -q` exits 0
    - `git check-ignore -q infra/secrets/infra/debezium.properties && git check-ignore -q infra/secrets/infra/grafana_ro.secret && git check-ignore -q infra/secrets/infra/notification_broker.secret` exits 0
    - `stat -f '%Lp' infra/secrets/infra/debezium.properties` prints 600
  </acceptance_criteria>
  <done>All ten PHI-free schemas exist and round-trip, registration is a deploy-time CLI with BACKWARD per subject, and the three infrastructure secrets exist as ignored 0600 files.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| service process -> outbox table | payload bytes and traceparent become a durable, replayable record read by every consumer |
| deploy job -> Schema Registry | only the registration module may write subjects |
| developer host -> secret files | generated credentials for Debezium, Grafana and the broker |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-01 | Information Disclosure | `.avsc` schemas and outbox payloads | high | mitigate | Ids, types and timestamps only; test scans every schema for PHI field names; strict serializer rejects fields not in the schema; no payload is ever logged |
| T-3-02 | Tampering | Schema Registry subjects | medium | mitigate | `auto.register.schemas=False` in every service serializer; registration only through `python -m libs.events.register`; BACKWARD set per subject and checked before registering |
| T-3-03 | Information Disclosure | `infra/secrets/infra/*` | high | mitigate | 0600 files in a 0700 gitignored directory, O_EXCL creation, refusal to overwrite, script prints paths only; files reach containers only as Compose secrets (plan 03-02, 03-11, 03-12) |
| T-3-04 | Denial of Service | Schema Registry as a write-path dependency | medium | mitigate | `warm_up` resolves ids at startup with retries; an unwarmed serialization runs in a thread so the event loop never blocks; `outbox_insert_seconds` histogram measures the cost; residual risk (cold start during a registry outage cannot become ready) documented in plan 03-14 |
| T-3-SC | Tampering | npm/pip/cargo installs | high | mitigate | Package-legitimacy gate: blocking human checkpoint (Task 1) for the six SUS packages before install; exact pins; the linking-only OTel Kafka instrumentation is excluded |
</threat_model>

<verification>
- `.venv/bin/python -m pip check`
- `.venv/bin/python -m pytest tests/events tests/tools/test_generate_infra_secrets.py -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (do not set OTEL_SDK_DISABLED) still passes: the new settings and packages must not break existing suites
- Semgrep: `semgrep --config auto libs/events scripts/generate_infra_secrets.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- Approved pins installed; contract confirmed and recorded
- `enqueue_event` writes one registry-framed Avro row per (aggregate, type, discriminator) inside the caller's transaction, with the producing span's traceparent
- Ten PHI-free schemas, a deploy-time registration CLI with BACKWARD per subject, and the infrastructure secret files exist
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md` when done
</output>
