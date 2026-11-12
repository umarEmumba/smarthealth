---
phase: 03-event-driven-system-observability
plan: 04
subsystem: events
tags: [kafka, consumer, dlq, idempotency, opentelemetry, prometheus]
requires:
  - phase: 03-event-driven-system-observability
    provides: libs/events catalogue, registry (deserializer), mock_registry fixture (plan 03-01); full catalogue and schemas (plan 03-03)
provides:
  - EventConsumer, EventEnvelope, Outcome, NonRetryableEventError, TransientEventError and consumer metrics (libs/events/consumer.py)
  - extract_context for Kafka headers, tolerant of NULL values (libs/events/tracing.py)
  - claim_event / processed_events DDL helpers for database effects (libs/events/idempotency.py)
  - live-Kafka test fixtures (topic_factory, unique_topic, dlq_topic, unique_group, produce_event, in_memory_spans)
affects: [03-09, 03-10, 03-14, 03-15, 03-16, 03-17]
tech-stack:
  added: []
  patterns:
    - "Synchronous confluent-kafka poll loop with one persistent asyncio.Runner; every handler call gets copy_context() so it sees the consumer span"
    - "Offset committed only after the effect, an IGNORE decision or a confirmed DLQ write; DLQ headers carry error TYPE only"
    - "TransientEventError (and registry outage during decode) = seek back + capped backoff, never dead-lettered"
    - "Database effects claim the event id in the effect's own transaction (INSERT ... ON CONFLICT DO NOTHING RETURNING)"
key-files:
  created:
    - libs/events/consumer.py
    - libs/events/tracing.py
    - libs/events/idempotency.py
    - tests/events/test_consumer_base.py
    - tests/events/test_consumer_dlq.py
    - tests/events/test_consumer_idempotency.py
  modified:
    - tests/events/conftest.py
key-decisions:
  - "EventEnvelope.payload is the full decoded Avro record (envelope fields included), so handlers read payload['visit_id'] as plans 03-09/03-10 assume"
  - "Registry unreachable while decoding is transient (retry, no DLQ); SerializationError, 4xx registry answers (unknown schema id) and corrupt Avro bodies are poison"
  - "A failed DLQ write purges and drops the DLQ producer and raises DeadLetterWriteError, so a retry in the same process cannot create a duplicate DLQ record"
requirements-completed: []
status: complete
duration: not measured
completed: 2026-11-11
actuals:
  tokens: 16000
  tasks: 3
  commits: 4
plan_head_before: d92b6baf056c6f5de379268cb3d251a1611f12d1
plan_head_after: 537ca01b84d67000f36dc59235cf71d8eb935406
---

# Phase 3 Plan 04: Consumer Base Summary

**One shared Kafka consumer base (decode, child-span tracing, async handlers on one persistent loop, commit-after-effect, DLQ for poison messages, outage retry without dead-lettering, metrics and heartbeat) plus a same-transaction `claim_event` helper, proven against the live broker with 23 passing tests.**

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | afd23a6 | `EventConsumer`, `extract_context`, Kafka fixtures, `test_consumer_base.py` (6 tests). The DLQ, retry, metrics and heartbeat code was written in the same file and committed here; Task 2 only adds tests for it |
| 2 | 9f7d4b7 | `test_consumer_dlq.py` (10 tests) |
| 3 | ce87930 | `libs/events/idempotency.py` and `test_consumer_idempotency.py` (7 tests) |
| follow-up | 537ca01 | removed an unneeded `nosemgrep` comment (Semgrep reports 0 findings without it) |

## Verification (measured)

- `.venv/bin/python -m pytest tests/events/test_consumer_base.py -q`: 6 passed (plan: at least 5).
- `.venv/bin/python -m pytest tests/events/test_consumer_dlq.py -q`: 10 passed (plan: at least 9), about 65 s.
- `.venv/bin/python -m pytest tests/events/test_consumer_idempotency.py -q`: 7 passed (plan: at least 4).
- `.venv/bin/python -m pytest tests/events tests/libs/test_service_isolation.py -q`: **123 passed**, 0 failed, 101 s, `OTEL_SDK_DISABLED` unset. After the last comment-only edit, `test_consumer_base.py` was re-run: 6 passed.
- Acceptance greps: `enable.auto.commit` 2, `dlq.error_type` 1, `start_http_server` 2 (import plus call), `ON CONFLICT DO NOTHING` 1. `pip show opentelemetry-instrumentation-confluent-kafka` exits 1 (not installed); a test asserts its module is absent.
- Semgrep: `semgrep --config p/python --metrics=off libs/events`: 151 rules, 8 files, **0 findings**. Narrower than the plan's `--config auto` (refused with `--metrics=off`, as in 03-01 and 03-03). `tests/events` was not scanned.
- Not run: the plan's full `pytest tests -m "not e2e"` regression; the orchestrator said it is not required for this plan.
- Not touched: `ruff` is not installed in `.venv`, so lint/format were not run.

## What the tests prove

- A produced `appointment.booked` reaches the handler as an `EventEnvelope` equal to the record; the handler runs inside a CONSUMER span whose trace id and parent span id equal the producing span's (the handler's own current span is that span); attributes as specified; the offset is committed after the effect (committed offset 1) and a second consumer in the group receives nothing.
- Two handler calls see the same event-loop object; an unhandled type is committed and counted `ignored`; a NULL `traceparent` header yields a root span, not a crash.
- Undecodable bytes are copied to the DLQ with original key, value, headers and all six `dlq.*` headers, the offset moves on, and the next message is processed. `NonRetryableEventError` dead-letters after one attempt; `ValueError` three times gives `dlq.attempts=3`, `dlq.error_type=ValueError`; fail-twice-then-succeed is processed once with no DLQ record. No header contains exception text (the handler messages in the tests include fake name-like strings that must not appear).
- `TransientEventError` is never dead-lettered: offset uncommitted during the outage, same message redelivered via seek, processed once after recovery.
- A DLQ write to an unreachable broker raises `DeadLetterWriteError` and leaves the offset uncommitted; a healthy consumer in the same group then dead-letters the redelivered message.
- Metrics samples (`events_consumed_total`, `event_processing_seconds`, `event_end_to_end_lag_seconds`, `dlq_events_total`, `kafka_consumer_lag` = high watermark minus position) exist with the expected labels and are served on `/metrics` of the given port; the heartbeat mtime advances at least 3 times in 3 s while idle; `run(stop)` returns and closes cleanly.
- `claim_event`: true once per (event, consumer) across transactions; a rollback of the effect rolls back the claim; two concurrent sessions give one effect row and no exception (the loser blocks, then sees False; if the winner rolls back the loser applies it); a real consumer re-delivered the same message after a group rewind produces one effect row; an effect that fails after the claim is applied on the in-process retry.

## Deviations from Plan

**1. [Rule 3 - Blocking] Tests are synchronous functions, not `async def`.** `EventConsumer` owns an `asyncio.Runner`, and `Runner.close()` raises "Cannot run the event loop while another loop is running" when called from inside pytest-asyncio's running loop. The consumer is a sync loop by design (a process entrypoint), so its tests are plain `def` tests; idempotency tests that need the session helpers are `async def` and only use the DB.

**2. [Rule 3 - Blocking] Idempotency tests use a throw-away schema instead of TEMP tables for the two-session cases.** TEMP tables are private to one connection, so a concurrency test and a real consumer running on its own loop cannot share one. Those tests create `p3test_<hex>` schemas on scheduling_db, select them with `SET LOCAL search_path`, and drop them afterwards (verified by `DROP SCHEMA ... CASCADE` in `finally`). The `temporary=True` DDL path is still tested on one connection. No real table was touched.

**3. [Rule 2 - Missing critical functionality] Registry outage during decode is transient, not poison.** The plan only specified "undecodable bytes -> DLQ". Dead-lettering valid events because Schema Registry was briefly down would violate the plan's own rule that an outage never dead-letters valid events, so `SchemaRegistryError` with a non-4xx status and httpx/transport errors retry (seek and backoff), while `SerializationError`, 4xx answers and corrupt Avro bodies are poison. This path is implemented but has no automated test (no safe way to take the registry down on the live stack).

**4. [Rule 2] Handler context propagation.** `asyncio.Runner.run` reuses the contextvars copied when the Runner was created, so the handler would not have seen the consumer span (Celery/Temporal calls inside handlers would then break the trace). Each call passes `context=contextvars.copy_context()`; a test checks the handler's current span is the consumer span.

**5. Interface additions beyond the plan's signature (backward compatible, all optional):** `dlq_producer_overrides` and `dlq_flush_timeout` (needed to force a DLQ producer failure and keep that test fast), `refresh_lag(force=False)`, `stop()`, `close()`, `DeadLetterWriteError`. `poll_once` also returns None on non-fatal broker errors and raises `KafkaException` on fatal ones.

**6. Task 1 commit contains the Task 2 code.** `consumer.py` was written whole in the tracer commit (it was one coherent file); Task 2's commit is its tests only. Behaviour was verified by the Task 2 tests after the fact, not red-first.

**7. Semgrep ruleset** was `p/python --metrics=off`, not `--config auto` (same as plans 03-01 and 03-03).

**8. Requirements not ticked** (per the orchestrator): EVENT-03, EVENT-05, OBS-02 stay for the verifier. `requirements-completed` is empty.

## Live stack

Not restarted, no Compose change, no container touched. Tests created and deleted `p3test.*` topics on the live broker (unique per test, removed by fixture teardown), used the `mock://` registry, and created and dropped `p3test_*` schemas on scheduling_db. Nothing reached the real event topics. One interrupted background pytest run (output lost, apparently a laptop-sleep hang in the metrics test) was discarded; it was killed and the file re-run in full: 10 passed, then in the combined run.

## Known Stubs

None.

## Deferred Issues

- `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` is a known unrelated failure (fails on the pre-Phase-3 baseline). Not run here, not touched.
- The decode-time registry-outage branch (deviation 3) has no automated test.
- The metrics server binds 0.0.0.0 as the plan says; plan 03-15 must keep Compose from publishing that port to the host.

## Threat Flags

None beyond the plan's register. T-3-11 (poison messages do not stall the partition), T-3-13 (DLQ type-only headers, no payload in logs, internal metrics port) and T-3-14 (offset only after effect or confirmed DLQ write) are implemented and covered by tests; T-3-12 (idempotency) is covered by `claim_event` plus the redelivery tests. The new `/metrics` listener is only started when `metrics_port` is passed.

## Self-Check: PASSED

- FOUND: libs/events/consumer.py, libs/events/tracing.py, libs/events/idempotency.py, tests/events/test_consumer_base.py, test_consumer_dlq.py, test_consumer_idempotency.py, tests/events/conftest.py
- FOUND commits: afd23a6, 9f7d4b7, ce87930, 537ca01 (`git rev-list --count d92b6ba..HEAD` = 4)
