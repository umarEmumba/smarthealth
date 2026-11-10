---
phase: 03-event-driven-system-observability
plan: 17
type: execute
wave: 10
depends_on: ["03-16"]
files_modified:
  - tests/events/test_trace_chain.py
  - tests/e2e/test_phase3_trace.py
  - tests/e2e/test_phase3_redelivery.py
  - tests/e2e/test_phase3_logs.py
  - tests/libs/test_observability.py
  - libs/events/consumer.py
  - libs/observability/logging.py
  - services/notification/consumer.py
  - services/notification/tasks.py
  - services/provider/api/availability.py
  - services/billing/api/internal.py
autonomous: true
requirements: [OBS-02, OBS-03, EVENT-03, EVENT-02]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "A single booking request is ONE connected trace in Jaeger spanning the gateway, Scheduling API, the Temporal workflow and activities, the Kafka consumer processing spans and the Celery task, with the consumer span a child (not only a link) of a span in that trace (OBS-02, roadmap success criterion 4)"
    - "Forcing every consumer group to re-read the test's events (offset rewind) and hard-killing Connect after delivery changes no effect: projections, sends and workflow starts each stay at exactly one (roadmap success criterion 1, effect-level exactly-once, D-08)"
    - "Structured JSON logs with trace_id exist for booking, availability sync, billing, notifications and background workers, contain no PHI, and the booking's trace id can be found in the logs of each process it touched (OBS-03)"
  artifacts:
    - path: tests/e2e/test_phase3_trace.py
      provides: "Jaeger API assertion of one trace across HTTP, Temporal, Kafka and Celery"
      contains: "16686"
    - path: tests/e2e/test_phase3_redelivery.py
      provides: "offset-rewind and Connect hard-kill redelivery with one-effect assertions for all three consumer groups"
      contains: "reset-offsets"
    - path: tests/events/test_trace_chain.py
      provides: "in-memory exporter proof of the producer -> outbox -> consumer -> Celery parent chain"
    - path: tests/e2e/test_phase3_logs.py
      provides: "the booking trace id appears in JSON log lines of every process on the path"
  key_links:
    - from: tests/e2e/test_phase3_trace.py
      to: libs/events/consumer.py
      via: "consumer span references the outbox traceparent as CHILD_OF"
      pattern: "CHILD_OF"
---

<objective>
Prove the cross-cutting guarantees on the finished stack: one connected trace per booking, effect-level exactly-once under forced redelivery, and structured logs for every critical flow. Fix any gap these tests expose in the shared code.

Purpose: roadmap success criteria 1 (consumers tolerate redelivery without duplicate side effects) and 4 (one connected trace; structured logs), and OBS-02/OBS-03. Earlier plans proved each piece locally; this plan proves them together.

Output: an in-memory trace-chain test, Jaeger-based and log-based e2e tests, a forced-redelivery e2e test, extended observability unit tests, and minimal fixes in the listed files only where a test fails.

Decisions honoured: D-11 (traceparent through outbox, Kafka headers and Celery headers; structlog JSON with trace ids; PHI-free), D-08 refined (three idempotency flavors), research F4 and Pitfall 3 (honest at-least-once on the wire; the effect is what must be single), Pattern 8 (trace chain). Reminder timers fire days later inside the same trace; the docs (plan 03-18) label that long-lived trace.

Live stack rules: tests stop, start and kill only `smarthealth` Compose services (`connect`, `visit-consumer`, `analytics-consumer`, `notification-consumer`) through `docker compose -f infra/docker-compose.yml ...`, always restoring them in `finally` and waiting for health; consumer offsets are rewound with `docker compose -f infra/docker-compose.yml exec -T kafka kafka-consumer-groups ... --reset-offsets --to-datetime <test start> --execute` while the group is stopped. No image rebuild unless a fix is needed (then rebuild only the affected service with `--no-deps`). Never touch `tjo-*` containers (record status before and after). This plan runs alone in wave 10, after the smoke measurement in plan 03-16, so its kills and offset rewinds disturb no other plan's tests.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@libs/events/consumer.py
@libs/events/outbox.py
@libs/observability/logging.py
@services/notification/consumer.py
@services/notification/celery_app.py
@tests/e2e/phase2_stack.py
@tests/e2e/phase3_stack.py
@tests/e2e/test_trace_propagation.py
@tests/libs/test_observability.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `tests/events/test_trace_chain.py` | test | in-memory: producer span -> `enqueue_event` (TEMP outbox) -> headers built from the row -> `EventConsumer` span -> Celery publish (eager with `CeleryInstrumentor`) -> task span |
| `tests/e2e/test_phase3_trace.py` | test | `GET http://localhost:16686/api/traces/{trace_id}`; required services `smarthealth-gateway`, `smarthealth-scheduling`, `smarthealth-scheduling-worker`, `smarthealth-notification-consumer`, `smarthealth-notification-celery`, `smarthealth-analytics-consumer` |
| `tests/e2e/test_phase3_redelivery.py` | test | offset rewind for groups `visit-downstream`, `analytics-projection`, `notification-trigger`; Connect hard kill; one-effect assertions |
| `tests/e2e/test_phase3_logs.py` | test | `docker compose logs --since` per service, JSON lines carrying the booking trace id |
| `tests/libs/test_observability.py` additions | test | JSON log lines with `trace_id` and no PHI for booking, availability sync, billing, notification and consumer flows |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — one booking is one trace in Jaeger across HTTP, Temporal, Kafka and Celery</name>
  <files>tests/events/test_trace_chain.py, tests/e2e/test_phase3_trace.py, libs/events/consumer.py, services/notification/consumer.py, services/notification/tasks.py</files>
  <behavior>
    - In memory: the trace ids of the producer span, the consumer processing span and the Celery task span are equal; the consumer span's parent is the producer span; the Celery publish span's parent is the consumer span; the task span's parent is the publish span
    - Live: a booking sent to the gateway with `traceparent: 00-<T>-<span>-01` is visible in Jaeger as trace T within 90 s, containing spans from all six required services
    - Live: the `smarthealth.appointment.events process` span in `smarthealth-notification-consumer` has a `CHILD_OF` reference whose span id belongs to trace T (a `FOLLOWS_FROM` link alone fails the test)
    - Live: the Celery task span for `notifications.send` has a parent in trace T
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 8, Pitfall 6, Code Examples)
    - libs/events/consumer.py (span creation), libs/events/outbox.py (`current_traceparent`), services/notification/celery_app.py (`instrument_publisher`), services/notification/consumer.py
    - tests/e2e/test_trace_propagation.py (existing Jaeger query helper and polling style)
    - tests/workflows/test_trace.py (in-memory exporter setup)
  </read_first>
  <action>
    Write the in-memory chain test (TEMP outbox table on scheduling_db as in plan 03-01; build the Kafka-style header list from the row's columns as Debezium does; run the consumer base's processing path on a fake message object, or a unique live topic; Celery in eager mode with `task_always_eager` on an app built by `create_celery_app` plus `CeleryInstrumentor`). Write the live test with a polling Jaeger query (reuse the helper style of `tests/e2e/test_trace_propagation.py`), asserting the service set and the reference types. If a link appears where a parent is required, or the Celery publish is not under the consumer span, fix it in the listed files (for example make sure the publish happens inside the consumer span and the publishing process called `instrument_publisher()`), rebuild only the affected service with `--no-deps`, and record the fix in the SUMMARY.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_trace_chain.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_trace.py tests/e2e/test_trace_propagation.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a required service is missing from the trace, or the consumer span is only linked to the producer trace.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_trace_chain.py -q` exits 0
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_trace.py -m e2e -q` exits 0 and the test names all six services
    - the SUMMARY records the trace id used and the span count per service (for the human Jaeger check in plan 03-20)
  </acceptance_criteria>
  <done>One booking is one connected trace across every async boundary.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Forced redelivery leaves every effect at exactly one</name>
  <files>tests/e2e/test_phase3_redelivery.py</files>
  <behavior>
    - Setup in a fresh clinic: two patients registered, two bookings confirmed, one cancelled, one visit checked in, started and completed; wait until all effects exist: analytics summary for the clinic, deliveries for each booked/cancelled dedupe key, `reminder-{id}` for the remaining booking, `visit-completed-{visit_id}` and `followup-{visit_id}`
    - Rewind: for each consumer service and group (`visit-consumer`/`visit-downstream`, `analytics-consumer`/`analytics-projection`, `notification-consumer`/`notification-trigger`), stop the service, reset the group's offsets on its topics to the test start time, start it, wait until the group's lag is 0; evidence of reprocessing: the service's log since the restart contains the test's event ids
    - After the rewind: analytics summary unchanged; each dedupe key has one `sent` delivery and one provider call; exactly one execution each of `visit-completed-{visit_id}` and `followup-{visit_id}`; the reminder workflow has one run
    - Hard kill: a new booking; as soon as its booked event is on Kafka, `docker compose kill connect`, start it, wait for RUNNING tasks; count copies of the event id on the topic (logged, may be 1 or more); the booking still has one delivery, one reminder run and the analytics count rose by exactly one
  </behavior>
  <read_first>
    - tests/e2e/phase3_stack.py (helpers), tests/e2e/test_phase3_notifications.py, tests/e2e/test_phase3_analytics.py, tests/e2e/test_phase3_visit_downstream.py (how each effect is read)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Kill-between-commit-and-publish tests (shape)" item 3, Pitfalls 3 and 12)
  </read_first>
  <action>
    Implement the scenario with fixed-argv subprocess calls through the Compose helpers; the offset reset uses `kafka-consumer-groups --bootstrap-server kafka:29092 --group <g> --topic <t> [--topic <t>...] --reset-offsets --to-datetime <ISO-8601 with milliseconds, UTC> --execute` inside the `kafka` container. Every stopped or killed service is restarted in `finally` and its health awaited; Connect also waits for all four tasks RUNNING. Scope every assertion to the test's clinic and ids (Pitfall 12). Record in the SUMMARY the number of events each group re-processed and the duplicate count on the wire after the hard kill: this is the evidence quoted by the docs for "no loss, effect-level exactly-once".
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_redelivery.py -m e2e -x -q</automated>
    <fails_when>pytest exits non-zero, any effect count changes after the rewind or the hard kill, or a consumer shows no reprocessing evidence (the rewind did not happen).</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_redelivery.py -m e2e -q` exits 0 with at least 2 passed
    - after the run all consumer containers and `connect` are healthy and the four connector tasks are RUNNING
    - the SUMMARY records reprocessed-event counts per group and the on-the-wire duplicate count
  </acceptance_criteria>
  <done>Redelivery is forced for every consumer group and no effect is duplicated.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Structured logs for booking, availability sync, billing, notifications and workers</name>
  <files>tests/libs/test_observability.py, tests/e2e/test_phase3_logs.py, libs/observability/logging.py, services/provider/api/availability.py, services/billing/api/internal.py, services/notification/tasks.py</files>
  <behavior>
    - Unit (in-process harness, stdout captured): a booking request, an availability template update (schedule change recorded), a billing pre-check, a Celery send in eager mode and a consumer processing call each emit at least one JSON log line with `trace_id` and `span_id` of the active span; none of those lines contains the seeded patient's full name, date of birth, phone or address
    - Live: for the trace id T of a booking sent through the gateway, `docker compose logs --since <start>` of `scheduling`, `scheduling-worker`, `billing`, `notification-consumer` and `notification-celery` each contain a JSON line with `"trace_id": "<T>"`
  </behavior>
  <read_first>
    - libs/observability/logging.py (`add_trace_context`, `configure_logging`)
    - tests/libs/test_observability.py (existing tests)
    - services/provider/api/availability.py, services/billing/api/internal.py, services/scheduling/api/appointments.py, services/scheduling/activities/booking.py (existing log calls; find the flows that log nothing)
    - tests/conftest.py (harness mesh, seeding helpers)
  </read_first>
  <action>
    Write the unit tests against the in-process harness, parsing captured stdout lines as JSON. Where a flow has no log line inside its span (expected candidates: availability sync and billing pre-check), add one structlog call with ids and outcome only (for example `provider.availability_changed` with provider id, template id and change type; `billing.precheck` with appointment id and decision), in the files listed, and rebuild the affected services with `--no-deps`. Write the live log test with the Compose logs helper. Do not log payloads, request bodies or exception messages that could carry user input.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_observability.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_logs.py -m e2e -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a flow emits no JSON line with trace_id, a PHI value appears in a captured line, or a process on the booking path has no line with the booking's trace id.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/libs/test_observability.py -q` exits 0 with at least 7 passed
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_logs.py -m e2e -q` exits 0
    - `.venv/bin/python -m pytest tests -m "not e2e" -q` exits 0
  </acceptance_criteria>
  <done>Every critical flow logs PHI-free JSON correlated to its trace.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| test harness -> live consumer groups | tests rewind offsets and kill infrastructure on the shared dev stack |
| processes -> Jaeger and logs | spans and log lines leave the process |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-49 | Information Disclosure | spans and log lines | high | mitigate | Tests assert no seeded PHI in captured logs; span attributes are ids and types; new log lines carry ids and outcomes only |
| T-3-50 | Denial of Service | tests leaving the dev stack degraded | medium | mitigate | Every stop/kill is restored in `finally` with health and connector-task waits; only `smarthealth` services are touched; tjo-* checked before and after |
| T-3-51 | Repudiation | claiming exactly-once without proof | medium | mitigate | Redelivery is forced deterministically (offset rewind) and evidenced (reprocessing log lines), and the wire duplicates are counted and reported honestly |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/events tests/libs/test_observability.py -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q` (whole e2e suite, Phase 2 and Phase 3)
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
</verification>

<success_criteria>
- One connected trace per booking in Jaeger across HTTP, Temporal, Kafka and Celery
- Forced redelivery and a Connect hard kill leave every effect at one
- Structured, PHI-free logs with trace ids for all five named flows
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-17-SUMMARY.md` when done
</output>
