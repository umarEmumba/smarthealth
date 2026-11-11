---
phase: 03-event-driven-system-observability
plan: 21
subsystem: events
tags: [kafka, consumer, failure-classification, dlq, asyncpg, sqlstate, ast-guard, gap-closure]
requires:
  - phase: 03-event-driven-system-observability
    provides: "plans 03-04 (consumer base), 03-09 (visit consumer), 03-10 (analytics consumer), 03-14 (notification triggers), 03-20 (phase gate)"
provides:
  - "libs/events/classify.py: the one failure classifier (sqlstate_of, is_transient_db, is_transient_temporal, is_transient_broker, is_transient_network, is_transient_registry, is_transient)"
  - "visit-downstream, analytics-projection and notification-trigger classify through it; no private copy remains"
  - "consumer base: transient cap (TransientExhausted), registry 408/429/5xx retried, bounded commit retry, event_commit_failures_total{group}"
  - "AST guard test_no_transient_classifier_outside_classify with structural detectors and positive controls"
  - "tests/events/standins.py: FakeMessage and a recording Kafka-consumer stand-in for driving the real _handle_message without a broker"
affects: [03-22-visit-trigger-recovery, 03-26-gap-closeout]
tech-stack:
  added: []
  patterns:
    - "classification by SQLSTATE class first, SQLAlchemy class second"
    - "bounded transient retry: attempt floor AND time cap, then a visible dead letter"
    - "AST guard with a positive control per detector, misnamed offenders included"
key-files:
  created:
    - libs/events/classify.py
    - tests/events/test_classify.py
    - tests/events/test_consumer_reliability.py
    - tests/events/standins.py
    - tests/analytics/test_consumer_classification.py
  modified:
    - libs/events/consumer.py
    - services/visit/consumer.py
    - services/analytics/consumer.py
    - services/notification/triggers.py
    - tests/visit/test_visit_consumer.py
    - tests/notification/test_triggers.py
key-decisions:
  - "W3: WR-03 is closed by bounding, not by narrowing. The seven Temporal statuses (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN, INTERNAL), the SDK's closed-client RuntimeError and OSError stay transient; every transient failure is bounded by the 10-attempt / 900 s cap and ends as TransientExhausted"
  - "TRANSIENT_RPC_STATUSES holds status NAMES (strings), so the analytics consumer does not import the Temporal SDK"
  - "The registry decode classification (408/429/5xx/no-status transient, other 4xx poison) lives in classify.is_transient_registry, not in the consumer base, so the guard can forbid transient classification outside classify.py"
status: complete
requirements-completed: []
completed: 2026-11-11
actuals:
  tokens: 18000
  tasks: 3
  commits: 3
plan_head_before: ba894d15d0bfbeffb09449f55e38e5e0249b8685
plan_head_after: 8dab269374a852de00a271c12cc74ae9e8b113c8
---

# Phase 3 Plan 21: Consumer Reliability Summary

One shared classifier now decides transient versus poison for all three Kafka consumers, an asyncpg 53300 / 57P03 / 40P01 no longer dead-letters a healthy event, every transient retry is bounded by a visible `TransientExhausted` dead letter, Schema Registry back-pressure is retried and a failed offset commit no longer ends the consumer process. The three consumer containers were rebuilt, are healthy and their e2e tracers pass. The phase is NOT complete (gap plans 03-22..03-26 follow); REQUIREMENTS.md was not edited and no requirement was ticked.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | c23d899 | `libs/events/classify.py`, visit consumer uses it, unit tests, asyncpg handler cases and a real-`_handle_message` RETRY_LATER proof, `tests/events/standins.py` |
| 2 | cdbba08 | analytics and notification consumers use it, the AST guard with positive and negative controls, registry-decode classification moved into `classify.py` |
| 3 | 8dab269 | consumer base: transient cap with `TransientExhausted`, bounded commit retry, `event_commit_failures_total`, cap / Temporal-outage / commit / live-broker tests |

## What changed

**Classifier (`libs/events/classify.py`).** `sqlstate_of` reads `sqlstate` / `pgcode` from the exception, its `orig`, then the `__cause__` / `__context__` chain (5 hops, cycle-safe). `is_transient_db` decides by SQLSTATE class (08, 53, 57, 40) whenever a code is found, so a unique violation (23505) or undefined table (42P01) is never transient whichever SQLAlchemy class wraps it (even an `OperationalError` wrapping 23505); without a code it keeps `OperationalError`, `InterfaceError`, `connection_invalidated`, adds the pool-checkout `sqlalchemy.exc.TimeoutError`, and adds asyncpg's raw client-side `InterfaceError` (closed connection, no SQLSTATE). `is_transient_temporal`, `is_transient_broker` (kombu, amqp, pymongo, imported lazily), `is_transient_network` and `is_transient_registry` complete the set; `is_transient(exc, db=True, temporal=False, broker=False)` is the composite. The classifier reads classes and codes only, never an exception message.

**Consumers.** `services/visit/consumer.py` calls `classify.is_transient(exc, temporal=True)`; `services/analytics/consumer.py` calls `classify.is_transient(exc)`; `services/notification/triggers.py` calls `classify.is_transient(exc, db=False, temporal=True, broker=True)`. Each private helper, status set and now-unused import (`sqlalchemy.exc`, `RPCError`, `RPCStatusCode`, `kombu`, `amqp`, `pymongo.errors`) is gone; the three docstrings describe the new classification and the cap.

**Consumer base (`libs/events/consumer.py`).** `EventConsumer(..., max_transient_attempts=10, max_transient_seconds=900.0, commit_attempts=3)`. `_retry_later` keeps, per (topic, partition, offset), the attempt count and the time of the first transient failure; once BOTH thresholds are reached it logs `event.transient_exhausted` (ids only) and dead-letters through the existing `_dead_letter` with `TRANSIENT_EXHAUSTED`. The state is cleared on success, dead letter and exhaustion. `_decode` delegates to `classify.is_transient_registry`. `_commit` retries a non-fatal `KafkaException` with `_sleep(backoff * 2**attempt)`, counts every failed attempt in `event_commit_failures_total{group}`, logs `event.commit_failed` with the error NAME only, logs `event.commit_abandoned` after the last try and returns; a fatal `KafkaError` re-raises.

## Decision W3 (WR-03): bound, do not narrow

The Temporal status set stays at the seven statuses both consumers already treated as outages, plus the SDK's closed-client `RuntimeError` and `OSError`. Dropping INTERNAL / UNKNOWN to the poison path would turn a Temporal frontend restart into a dead letter after three quick attempts, which is the CR-01 failure mode again. What changed is that nothing can retry for ever: after at least 10 attempts AND at least 900 s the record is dead-lettered as `TransientExhausted` (`dlq_events_total{error_type="TransientExhausted"}`, original key, value and headers on the DLQ topic). Test `test_a_temporal_outage_ends_in_transient_exhausted_not_in_a_quick_dead_letter` (INTERNAL, UNKNOWN, closed-client `RuntimeError`) holds the event for 14 attempts, far beyond the base's 3 handler-error attempts, then dead-letters it as `TransientExhausted`. Recovery of such records (reconciler pass, DLQ replay script) is plan 03-22.

## Proof level (I6): stated honestly

- CR-01 / WR-02 classification is proven at the REAL `_handle_message` path at unit level, with real asyncpg exception classes (`TooManyConnectionsError` 53300, `CannotConnectNowError` 57P03, `DeadlockDetectedError` 40P01, raw and wrapped in a `DBAPIError` whose `orig` carries `sqlstate` like the dialect's) and a recording stand-in for the Kafka consumer: RETRY_LATER, one seek to the message's offset, no commit, no DLQ producer created. This runs for both the visit-downstream and the analytics-projection consumer.
- NO 53300 was injected into the running stack: saturating the shared dev Postgres would disturb every service and the unrelated `tjo-*` containers on this host. The live recovery path is plan 03-22's lost-trigger e2e.
- The transient cap is proven both with a fake clock at the real `_handle_message` and on the live broker (`test_live_broker_a_capped_message_is_dead_lettered_and_the_next_one_is_processed`: a unique topic/DLQ/group, DLQ record with original key, `dlq.error_type=TransientExhausted`, `dlq.attempts=2`, committed offset moved past both messages).
- Commit failures are proven with a stand-in whose `commit` raises `KafkaException(KafkaError(REQUEST_TIMED_OUT, fatal=False))`; no broker fault was injected live.

## The single-classifier AST guard (orchestrator directive: not name-based only)

`test_no_transient_classifier_outside_classify` scans every `.py` under `services/` and `libs/events/` except `classify.py`. Planned detectors: (a) a function named `^_*(is_)?(transient|retryable)` with a missing or `bool` return annotation, (b) a literal collection holding an `RPCStatusCode.<member>`, (c) a literal collection holding two or more SQLSTATE classes or codes. Added structural detectors that do not depend on a name:

1. `isinstance(x, <transient class(es)>)` for OperationalError, InterfaceError, DBAPIError, TimeoutError, ConnectionError, OSError, RPCError, KombuError, AMQPError, PyMongoError, asyncpg errors, TransportError, ... (also in tuples and as attributes);
2. an `except <transient class(es)>` handler whose body names a `Transient...Error` or `NonRetryable...Error` (a verdict for the consumer base, including a private `_TransientDecodeError`);
3. any read of `sqlstate` / `pgcode` (attribute, `getattr` string);
4. a transient Temporal status (`RPCStatusCode.UNAVAILABLE`, ...) used in any expression;
5. a transient class NAME compared as a string (`type(exc).__name__ == "OperationalError"`, or in a literal collection).

Allow rule (documented in the test module): ordinary try/except handling that is not classification passes, for example `except OSError:` that logs or returns a default, `except RPCError` that compares a status to `NOT_FOUND`, `except (ConnectionError, TimeoutError)` that raises an HTTP error, and `isinstance(cause, TimeoutError)` when `TimeoutError` is imported from `temporalio.exceptions` (the workflow activity-timeout class; resolved from the module's imports, so no file allow-list). Only a handler that gives a transient / non-retryable verdict counts, and a call to the shared classifier is of course allowed.

Positive controls (each must report exactly its shape on a synthetic offender whose function name is innocuous): the original function + status set + SQLSTATE tuple reports exactly 3 findings; 11 more offenders (plus the misnamed classifier below) cover each structural detector (isinstance with names and attributes, three except-verdict variants, sqlstate and pgcode reads, status in a comparison and in a membership test, class name as string and in a set); a misnamed `keep_for_later` classifier is found; 7 negative controls (ordinary handling) report nothing. The scan found real offenders before the fix (analytics and notification helpers, and two except handlers in `libs/events/consumer.py:_decode`), which is why the registry classification moved into `classify.py`; after the fix the repository scan reports nothing.

## Test results

| Command | Result |
|---|---|
| `tests/events/test_classify.py` (final) | 83 passed incl. the repository scan, 13 positive controls and 7 negative controls (the Task 1 version of the file, without the guard, passed with the visit tests: 82 in total) |
| `tests/events` (live broker) | 213 passed, 128 s |
| `tests/events/test_consumer_reliability.py` | 20 passed (acceptance needs at least 10) |
| `tests/analytics` + `tests/visit/test_visit_consumer.py` (visit-consumer stopped around the run, restarted healthy) | 64 passed, 73 s |
| `tests/notification` + `tests/libs/test_service_isolation.py` (notification-consumer left running) | 91 passed, 197 s |
| Task 1 verify: `tests/events/test_classify.py tests/visit/test_visit_consumer.py` (visit-consumer stopped) | 82 passed |
| e2e tracers `test_phase3_visit_downstream.py`, `test_phase3_analytics.py`, `test_phase3_notifications.py` (`-k tracer`, rebuilt consumers) | 3 passed, 14 s (visit tracer alone after the first rebuild: 1 passed) |

No run spanned a laptop sleep (durations consistent, no wake-time `E` marks). No full non-e2e regression was run (not required). The known unrelated failures (`test_admin_registers_provider_through_gateway`, `tests/identity/test_email_migration.py`) were neither run nor touched.

## Live stack actions

- Rebuilt with `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-consumer` (Task 1) and `... visit-consumer analytics-consumer notification-consumer` (Task 3). All three report `healthy` and import the new module (`TransientExhausted`, `event_commit_failures_total` present in each container).
- `visit-consumer` was stopped around the `tests/visit` runs and started again with `start --wait --wait-timeout 180` each time.
- Gateway: found at the defaults (20 auth / 300 default per minute); recreated with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` for the e2e tracers. IT IS STILL RAISED (1000 / 100000); restoring the defaults is left to plan 03-26, as instructed.
- No `down -v`, no volume removed, `tjo-api` (Up 34 hours, unhealthy) and `tjo-postgres` (Up 34 hours, healthy) identical before and after. The retired `visit-downstream-sweeper` Schedule was not re-introduced; no workflow or activity name or shape changed; no package installed.
- End state: `scripts/compose_healthcheck.sh` prints OK; replication slots `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all active.

## Deviations from Plan

1. **[Rule 3 - Blocking / orchestrator directive] The AST guard is stronger than planned.** Added the structural detectors, the import-resolved allow rule and their positive controls described above; the plan's three-finding control is kept exactly.
2. **[Rule 3 - Blocking] The registry-decode classification moved into `libs/events/classify.py` (`is_transient_registry`) in the Task 2 commit, not into `_decode` in Task 3.** The new guard (correctly) flagged the two `except` clauses in `_decode` that mapped `SchemaRegistryError` / transport errors to `_TransientDecodeError`; leaving them would have broken the single-classifier rule. Behavior: 408, 429, 5xx and a missing / non-integer / -1 status are transient, other 4xx are poison, httpx transport and network errors are transient (OSError is now covered where the old clause listed only `ConnectionError`, `TimeoutError` and `httpx.TransportError`; a generic corrupt-Avro body stays poison). The decode tests landed with it in `tests/events/test_consumer_reliability.py`.
3. **[Rule 2 - Missing critical functionality] `classify.is_transient_db` also treats asyncpg's raw client-side `InterfaceError` as transient.** It carries no SQLSTATE and is raised raw when a connection cannot be used, so it would otherwise be dead-lettered like the CR-01 errors.
4. **[Design] `TRANSIENT_RPC_STATUSES` is a set of status NAMES (strings), not `RPCStatusCode` members,** so importing the classifier in the analytics consumer does not load the Temporal SDK.
5. **[Design] The cap's clock is a module seam `libs.events.consumer._monotonic`** (tests monkeypatch it) instead of patching `time.monotonic` globally, which would also freeze `_sleep`'s deadline loop.
6. **[Process] The AST-guard section was held back from the Task 1 commit** (it would fail the repository scan while the analytics and notification copies still existed) and added in Task 2, as the plan intends; the Task 1 commit therefore contains only the classifier unit tests.

No auth gates, no checkpoints, no deferred fix-attempt limits reached.

## Follow-ups (open, out of scope per decision D-14)

WR-01 (reminder reschedule-signal race), WR-04 (debezium role privileges, Connect REST auth, FileConfigProvider allowed.paths), WR-06 (backfill / replay sends follow-ups for old visits) and IN-02..IN-06 stay open. IN-01 is CLOSED here. WR-02, WR-03 and WR-07 are closed by this plan. Still to do in later gap plans: the recovery route for a dead-lettered `visit.completed` (03-22), the connection budget that makes 53300 less likely (03-23), and the docs: docs/architecture.md section 10.2 still says an infrastructure outage "is never dead-lettered" and the three module docstrings point to its "recovery routes", which 03-22 / 03-26 must write. Plan 03-26 must also restore the gateway rate limits.

## Verification gates

- Semgrep (`semgrep --config p/python --metrics=off ...`): NOT RUN. `semgrep` is not installed in this environment (not on PATH, not in `.venv`) and installing packages is forbidden for this plan; the phase gate (03-20) recorded the project's Semgrep triage and the touched files add no new network, auth or file surface. Record this as an unrun verify.
- Ruff / mypy are not installed either; the code was written to the project's style by hand.

## Known Stubs

None.

## Threat Flags

None. T-3-63 (cap), T-3-64 (error type / KafkaError name only: a test asserts the handler's message text appears in no DLQ header and no commit log), T-3-65 (bounded commit retry) and T-3-66 (SQLSTATE class first; 23505 and 42P01 tested non-transient) are mitigated as planned.

## Self-Check: PASSED

- Created files exist: `libs/events/classify.py`, `tests/events/test_classify.py`, `tests/events/test_consumer_reliability.py`, `tests/events/standins.py`, `tests/analytics/test_consumer_classification.py`.
- Commits found in `git log`: c23d899, cdbba08, 8dab269.
- Acceptance greps: no `def _is_transient` in the three consumers; no `RPCStatusCode` in `services/visit/consumer.py` or `services/notification/triggers.py`; `classify` referenced in all three; `TransientExhausted` x4 and `event_commit_failures_total` x5 in `libs/events/consumer.py`; all three consumer containers healthy.
