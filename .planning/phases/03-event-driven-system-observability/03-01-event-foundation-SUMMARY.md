---
phase: 03-event-driven-system-observability
plan: 01
subsystem: events
tags: [kafka, avro, schema-registry, transactional-outbox, opentelemetry, prometheus]
requires:
  - phase: 02-scheduling-service-workflow
    provides: scheduling_db, AsyncSession repository contract (caller owns commit), OTel tracer setup
provides:
  - six human-approved Phase 3 package pins installed
  - libs/events catalogue, strict registry client, enqueue_event outbox helper
  - AppointmentBooked Avro schema
  - mock-registry test fixture
affects: [03-02, 03-03, 03-04, 03-05, 03-06, 03-10, 03-13]
tech-stack:
  added: [confluent-kafka 2.15.0, fastavro 1.13.1, celery 5.6.3, kombu 5.6.2, opentelemetry-instrumentation-celery 0.65b0, prometheus-client 0.26.0 (now a direct pin)]
  patterns:
    - "Outbox write inside the caller's AsyncSession, never commits, uuid5 event id + ON CONFLICT (id) DO NOTHING"
    - "Strict AvroSerializer per event type (auto.register.schemas False, record_subject_name_strategy); registration only via register_all"
    - "Unwarmed serialization runs in asyncio.to_thread so the registry never blocks the event loop"
key-files:
  created:
    - libs/events/__init__.py
    - libs/events/catalog.py
    - libs/events/registry.py
    - libs/events/outbox.py
    - libs/events/schemas/AppointmentBooked.avsc
    - tests/events/__init__.py
    - tests/events/conftest.py
    - tests/events/test_outbox_roundtrip.py
  modified:
    - pyproject.toml
    - libs/config.py
key-decisions:
  - "Contract D-05 confirmed unchanged: Avro, one subject per event type by record name, one topic per aggregate"
  - "load_schema_str returns the stripped .avsc text so the registered and looked-up schema strings are byte-identical"
  - "Round-trip tests use a local TracerProvider rather than the global one, so they do not depend on the harness having configured the SDK provider"
requirements-completed: [EVENT-01, EVENT-02, OBS-02]
status: complete
duration: 21 min
completed: 2026-11-10
actuals:
  tokens: 8300
  tasks: 3
  commits: 1
plan_head_before: 0770e86d74052d13cb0ec3c42e6fc904c1f0f667
plan_head_after: 559e17a3b90971c7d2aba1e0f084e684b73caf04
---

# Phase 3 Plan 01: Event Foundation Summary

**Registry-framed Avro outbox write helper (`enqueue_event`) with a strict, never-registering serializer, deterministic uuid5 event ids and the producing span's traceparent, proven by 11 passing tests against live Postgres; the six approved pins are installed and `pip check` is clean.**

Tasks 1 and 2 were human gates that the user had already answered in this session; the executor did not stop at them. Their answers are recorded verbatim below.

## Package approval

Task 1 reply: "Approved (Recommended)" = all six pins AND the Debezium Connect image:

- `confluent-kafka[avro,schemaregistry]==2.15.0`
- `fastavro==1.13.1`
- `celery==5.6.3`
- `kombu==5.6.2`
- `opentelemetry-instrumentation-celery==0.65b0`
- `prometheus-client==0.26.0`
- Docker image `quay.io/debezium/connect:3.7.1.Final` (approved; pulled by plan 03-05, not by this plan)

No `pip install` of a listed package ran before this approval was relayed to the executor. The install used only the approved pins (transitives pulled by the extras: avro 1.12.2, authlib 1.9.5, cachetools 8.0.0, attrs 26.1.0, amqp 5.4.2, billiard 4.3.2, vine 5.1.0 and the Celery/click helpers).

## D-05 contract decision

Task 2 reply: "option-a: Avro, topic per aggregate (Recommended)" with no edits to the contract. Task 3 and plan 03-03 therefore use the names exactly as proposed: subjects `com.smarthealth.events.<PascalName>` (record_subject_name_strategy), compatibility BACKWARD, topics `smarthealth.{appointment,visit,provider,patient}.events` plus the test-only `smarthealth.cdcprobe.events`, DLQ topics `smarthealth.dlq.{visit-downstream,notification-trigger,analytics-projection}`, uuid5 event ids, the ten-event set, wait time defined as check-in to visit start. No name mapping is needed.

Also recorded: CONTEXT D-13 (user-confirmed earlier, 2026-11-10): analytics start at cutover; events are emitted only for new activity, so the five metrics count from Phase 3 go-live and earlier patients, appointments and visits are not backfilled (the only backfill is 03-12's completed-visit downstream backfill). The phase-goal phrase "every state change" is scoped to the D-07 event set.

## User-veto list

Reply: "Accept all six (Recommended)". Every item is ACCEPTED, none vetoed:

1. Kafka Connect image deviation (`quay.io/debezium/connect:3.7.1.Final`, broker and Schema Registry stay on 7.6.13): ACCEPTED.
2. EVENT-04 projections on a Kafka consumer instead of Celery: ACCEPTED. REQUIREMENTS.md was NOT edited (the planner and executor leave that to the user; the deviation is to be recorded in plan 03-18 docs and plan 03-20 PHASE3_TRACE notes).
3. S3 audit-via-outbox stays deferred: ACCEPTED.
4. Scheduling does not consume `provider.schedule_changed` (publish only; the poller stays paused): ACCEPTED.
5. Insert-only outbox (no pruning): ACCEPTED.
6. Completed-but-unstarted visits backfilled by a script (plan 03-12): ACCEPTED.

## What was built

- `pyproject.toml`: the six pins under a "Human-vetted in plan 03-01 Task 1" comment, plus `[tool.setuptools.package-data]` `"libs.events" = ["schemas/*.avsc"]`.
- `libs/config.py`: `SharedSettings.kafka_bootstrap_servers` (default `localhost:9092`), `.schema_registry_url` (`http://localhost:8081`), `.analytics_service_url` (`http://analytics:8000`).
- `libs/events/catalog.py`: `EventSpec`, `EVENT_CATALOG` (only `appointment.booked` here), `EventType`, `AGGREGATE_TOPICS`, `topic_for`, `DLQ_TOPICS`, `CDC_PROBE_TOPIC`.
- `libs/events/registry.py`: `get_registry_client`, `load_schema_str`, `serializer_for` (strict), `deserializer`, `register_all(client, event_types=None)`, async `warm_up`, `is_warm`/`mark_warm`, `reset_registry_for_tests`.
- `libs/events/outbox.py`: `EVENT_ID_NAMESPACE`, `event_id_for`, `current_traceparent`, `outbox_table_ddl(temporary=False)`, `enqueue_event`, histogram `outbox_insert_seconds{event_type}`.
- `libs/events/schemas/AppointmentBooked.avsc` (`com.smarthealth.events.AppointmentBooked`, envelope fields first, then ids and timestamps only).
- `tests/events/conftest.py` (`mock_registry`) and `tests/events/test_outbox_roundtrip.py` (11 tests).

## Verification

- `.venv/bin/python -m pip check`: "No broken requirements found."
- Version check: `confluent_kafka.__version__` 2.15.0, fastavro 1.13.1, celery 5.6.3, kombu 5.6.2 (see deviation 2).
- `.venv/bin/python -m pytest tests/events -q`: 11 passed (the plan required at least 6).
- Acceptance greps: `celery==5.6.3` 1, `libs.events` in pyproject 1, `auto.register.schemas` in registry.py 2 (comment plus conf key), `ON CONFLICT (id) DO NOTHING` in outbox.py 1.
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (OTEL_SDK_DISABLED not set): **1366 passed, 9 skipped, 9 deselected, 1 failed** in 17m46s. The one failure is `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway`, a specialty-ordering assertion that also fails in isolation; it is unrelated to this plan (no provider code touched, `libs.events` not imported by Provider). Logged in `deferred-items.md`; not fixed.
- Semgrep: installed at `/Library/Frameworks/Python.framework/Versions/3.14/bin/semgrep`. `--config auto` is refused with `--metrics=off`, so the scan ran as `semgrep --config p/python --metrics=off libs/events`: 151 rules, 4 files, **0 findings**. (No Semgrep rule pack was run against `tests/events`.)

## Task Commits

1. Task 1 (package approval): no commit; human gate answered by the user.
2. Task 2 (contract decision): no commit; human gate answered by the user.
3. Task 3 (tracer: pins, settings, catalogue, registry, outbox, schema, tests): `559e17a` (feat)

The type-tracer feedback gate: the task's `<verify>` (`pip check` plus the round-trip tests) was re-run end to end after the commit and passes, so expansion (plan 03-03 and later) is unblocked.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Registered and serializer-looked-up schema text differed by a trailing newline**
- **Found during:** Task 3 (first test run)
- **Issue:** confluent-kafka's `AvroSerializer` strips the schema string it is given, while `register_all` registered the file text verbatim. With the strict serializer (`auto.register.schemas=False`) the mock registry's `lookup_schema` then returned 404 "Schema Not Found".
- **Fix:** `load_schema_str` returns the stripped text, so registered and looked-up strings are identical.
- **Files modified:** libs/events/registry.py
- **Commit:** 559e17a

### Plan wording differences (no behaviour change)

**2. Acceptance command printing the package version.** The plan's `confluent_kafka.version()[0]` returns the string `2.15.0` indexed to `2`, so the command prints `2 1.13.1 5.6.3 5.6.2` instead of `2.15.0 ...`. The installed version is 2.15.0 (`confluent_kafka.__version__`, and pip).

**3. Tracing in the round-trip tests.** The plan said to attach an InMemorySpanExporter to the global provider. The tests instead start the producer span from a local `TracerProvider` (a valid current span is all `propagate.inject` needs) and compare the stored traceparent trace id with that span's id. This keeps the tests independent of whether the harness has installed the SDK provider; the behaviour covered is the same.

**4. "A second session does not see the row until the caller commits."** A TEMP outbox table is private to one connection, so a second session cannot see it at all. "Never commits" is instead proven on one `AsyncConnection`: after `enqueue_event`, the session is still in a transaction and a `rollback()` leaves no row (a committed row would survive); a caller `commit()` then makes the row persist.

**5. Added beyond the six required behaviours:** payload may not override envelope fields (raises `ValueError` before any INSERT); `warm_up` marks types warm and raises after the last failed attempt; a strict-serializer test proving a registry missing the subject fails and the registry stays empty (no runtime registration); catalogue test that every entry has a schema file and registered subject.

**Live stack:** not restarted, no Compose change, no container touched. The tests only opened a connection to the existing scheduling_db and created a TEMP table (nothing persisted, nothing reaches the real `outbox` table or CDC).

**6. Requirements not ticked.** The plan frontmatter lists EVENT-01, EVENT-02 and OBS-02, and `requirements.mark-complete` was run, but the result was reverted in REQUIREMENTS.md: this plan delivers only the library slice (no event is yet published to Kafka, no trace crosses Kafka or Celery), so marking the three requirements complete would overstate it. They stay Pending until the producer, CDC and tracing plans finish; the orchestrator or verifier can tick them then.

## Known Stubs

None.

## Threat Flags

None. No new network endpoint, auth path or trust-boundary schema change; the new library only writes to a caller-supplied session. Threat register items: T-3-01 (PHI-free schema; no payload logged) and T-3-57 (strict serializer, acceptance grep) mitigated here; T-3-04 mitigated by `warm_up`, thread offload and the `outbox_insert_seconds` histogram (residual cold-start-during-registry-outage risk to be documented in plan 03-18); T-3-SC handled by the human gate.

## Issues / Notes for later plans

- Importing `confluent_kafka.schema_registry` emits an `AuthlibDeprecationWarning` ("httpx module is deprecated; use httpx2") from the transitive `authlib 1.9.5`; harmless noise in test output.
- The outbox table itself (Alembic migration, publication, `GRANT SELECT TO debezium`) is not created here; only `outbox_table_ddl()` exists. Producer plans and plan 03-04/03-05 own the real migrations.
- `warm_up` is not yet called by any service startup; the producer plans must call it with the event types they publish.

## Self-Check: PASSED

- FOUND: libs/events/{__init__,catalog,registry,outbox}.py, libs/events/schemas/AppointmentBooked.avsc, tests/events/{__init__,conftest,test_outbox_roundtrip}.py
- FOUND commit: 559e17a
