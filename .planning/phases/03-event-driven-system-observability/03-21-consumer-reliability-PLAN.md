---
phase: 03-event-driven-system-observability
plan: 21
type: execute
wave: 14
depends_on: ["03-20"]
files_modified:
  - libs/events/classify.py
  - libs/events/consumer.py
  - services/visit/consumer.py
  - services/analytics/consumer.py
  - services/notification/triggers.py
  - tests/events/test_classify.py
  - tests/events/test_consumer_reliability.py
  - tests/visit/test_visit_consumer.py
  - tests/analytics/test_consumer_classification.py
  - tests/notification/test_triggers.py
autonomous: true
gap_closure: true
requirements: [EVENT-03, EVENT-05, NOTIF-01, ANLY-06]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 (CR-01, WR-02): an asyncpg TooManyConnectionsError (53300), CannotConnectNowError (57P03) or DeadlockDetectedError (40P01), raised raw while a connection opens or wrapped by SQLAlchemy as a DBAPIError, makes the visit-downstream and analytics-projection handlers raise TransientEventError, and the consumer base returns RETRY_LATER: the offset is not committed and nothing is written to the DLQ"
    - "IN-01: one shared classifier, libs/events/classify.py, decides transient versus non-retryable for all three consumers (SQLSTATE classes 08, 53, 57 and 40, SQLAlchemy connection errors and pool timeouts, Temporal UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED, broker, Mongo and network errors); no consumer module keeps its own copy"
    - "WR-03: Temporal UNKNOWN, INTERNAL, ABORTED and CANCELLED are no longer treated as outages, and a message that keeps failing transiently is dead-lettered with dlq.error_type=TransientExhausted only after BOTH the attempt floor and the time cap are reached (defaults 10 attempts and 900 s), so a stuck partition moves on and the record is visible in dlq_events_total{error_type=\"TransientExhausted\"} and on the DLQ topic with its original key, value and headers"
    - "WR-02: a Schema Registry 408, 429 or 5xx answer while decoding is retried (RETRY_LATER); other 4xx answers, corrupt Avro and SerializationError are dead-lettered as before"
    - "WR-07: a failed offset commit with a non-fatal KafkaError is retried with bounded backoff, counted in event_commit_failures_total{group} and logged with the error name only, and never ends the consumer process; only a fatal KafkaError still raises"
    - "The rebuilt visit-consumer, analytics-consumer and notification-consumer containers run this code, are healthy, and their e2e tracers pass"
  artifacts:
    - path: libs/events/classify.py
      provides: "sqlstate_of, is_transient_db, is_transient_temporal, is_transient_broker, is_transient_network, is_transient"
      contains: "TRANSIENT_SQLSTATE_CLASSES"
    - path: libs/events/consumer.py
      provides: "transient cap with TransientExhausted, registry 408/429/5xx transient, bounded commit retry, event_commit_failures_total"
      contains: "TransientExhausted"
    - path: tests/events/test_consumer_reliability.py
      provides: "cap, decode-status and commit-failure tests"
    - path: tests/analytics/test_consumer_classification.py
      provides: "asyncpg errors -> TransientEventError -> RETRY_LATER for analytics-projection"
  key_links:
    - from: services/visit/consumer.py
      to: libs/events/classify.py
      via: "handle_visit_completed maps a non-poison failure through classify.is_transient(exc, temporal=True)"
      pattern: "classify"
    - from: services/analytics/consumer.py
      to: libs/events/classify.py
      via: "handle_event maps a database failure through classify.is_transient(exc)"
      pattern: "classify"
    - from: services/notification/triggers.py
      to: libs/events/classify.py
      via: "_guarded maps broker, Mongo, Temporal and network failures through classify.is_transient"
      pattern: "classify"
    - from: libs/events/consumer.py
      to: "the group's DLQ topic"
      via: "_retry_later dead-letters with TRANSIENT_EXHAUSTED once both thresholds are reached"
      pattern: "TRANSIENT_EXHAUSTED"
---

<objective>
Close the classification half of verification gap 1 (CR-01) together with WR-02, IN-01, WR-03 and WR-07: a database connection or resource error must never dead-letter a healthy event, one shared classifier replaces the three drifted copies, transient retries are capped so a partition cannot block for ever (and the capped record is visibly recoverable), Schema Registry back-pressure answers are retried, and a failed offset commit no longer crashes the consumer.

Purpose: the phase goal says "no silent loss". The verifier proved that asyncpg's TooManyConnectionsError, CannotConnectNowError and DeadlockDetectedError reach the consumers as plain DBAPIError (or raw asyncpg exceptions) and are dead-lettered after three quick attempts; the 300-connection ceiling was actually hit at the phase gate, so the trigger is real. D-08 (idempotent consumers) is what makes retrying safe, and D-03 (sweeper retired) is why a dead-lettered `visit.completed` matters: plan 03-22 adds the recovery route; this plan stops healthy events from being dead-lettered in the first place.

Output: `libs/events/classify.py`; the three consumers importing it; the consumer base with the transient cap, the registry status fix and the commit retry; unit and live-broker tests; the three consumer containers rebuilt.

Scope guard (user decision for this gap closure): WR-01 (reminder reschedule-signal race), WR-04, WR-06 and IN-02..IN-06 are out of scope; record them as follow-ups in the SUMMARY. No workflow changes, no package installs, no REQUIREMENTS.md edits.

Live stack rules (put in front of every live step):
- Rebuild only with `docker compose -f infra/docker-compose.yml up -d --build --no-deps <names>`; the names in this plan are `visit-consumer`, `analytics-consumer`, `notification-consumer`. Never `down -v`, never remove a volume, never touch the unrelated `tjo-*` containers (record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before and after).
- `tests/visit` writes real visit rows that Debezium relays: stop the live consumer around those runs with `docker compose -f infra/docker-compose.yml stop visit-consumer` and start it again afterwards (`start visit-consumer`, then wait until `docker compose -f infra/docker-compose.yml ps visit-consumer --format '{{.Health}}'` prints `healthy`).
- Gateway budget rule: e2e runs need the raised gateway budget. Before the first e2e run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` and say so in the SUMMARY. Plan 03-25 restores the defaults (20 and 300) as its last step; do not restore them here.
- The dev Postgres has `max_connections=300`: run one pytest process at a time, never two suites in parallel.
- No `.env*` or `infra/secrets/*` reads, no package installs, PHI never in logs, metric labels or DLQ headers (error type only).
- The laptop sleeps: run anything longer than about 5 minutes in the background, poll it, and treat a run that spans a sleep (a wall-clock gap above 60 s in a 20 s heartbeat loop) as invalid and rerun it.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-VERIFICATION.md
@.planning/phases/03-event-driven-system-observability/03-REVIEW.md
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-04-consumer-base-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-09-visit-outbox-downstream-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-10-analytics-projections-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-14-notification-trigger-SUMMARY.md
@libs/events/consumer.py
@services/visit/consumer.py
@services/analytics/consumer.py
@services/notification/triggers.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `libs/events/classify.py` | module | `TRANSIENT_SQLSTATE_CLASSES = ("08", "53", "57", "40")`, `TRANSIENT_RPC_STATUSES` (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED) |
| `sqlstate_of(exc) -> str \| None` | function | first 5-character `sqlstate` (or `pgcode`) found on `exc`, `exc.orig`, then the `__cause__` / `__context__` chain (depth-bounded, cycle-safe) |
| `is_transient_db(exc) -> bool` | function | SQLSTATE class decides when present; else SQLAlchemy `OperationalError`, `InterfaceError`, `connection_invalidated`, `sqlalchemy.exc.TimeoutError` (pool checkout) |
| `is_transient_temporal(exc) -> bool` | function | `RPCError` with a status in `TRANSIENT_RPC_STATUSES`; `RuntimeError` (the SDK's closed-client signal) |
| `is_transient_broker(exc) -> bool` | function | kombu `KombuError`, amqp `AMQPError`, pymongo `PyMongoError` (imported lazily) |
| `is_transient_network(exc) -> bool` | function | builtin `TimeoutError`, `ConnectionError`, `OSError` |
| `is_transient(exc, *, db=True, temporal=False, broker=False) -> bool` | function | composite used by the three consumers |
| `TRANSIENT_EXHAUSTED = "TransientExhausted"` | constant | DLQ `dlq.error_type` after the transient cap |
| `EventConsumer(..., max_transient_attempts=10, max_transient_seconds=900.0, commit_attempts=3)` | constructor kwargs | cap needs both thresholds; commit retried with backoff |
| `event_commit_failures_total{group}` | Counter | one increment per failed commit attempt |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a raw asyncpg TooManyConnectionsError in the visit-downstream handler is classified by the shared classifier and held by the consumer base (RETRY_LATER), and the rebuilt visit-consumer still starts downstream work</name>
  <reversibility rating="reversible">The classifier is an internal helper behind the existing NonRetryable/Transient contract; swapping it back is a one-module change.</reversibility>
  <files>libs/events/classify.py, services/visit/consumer.py, tests/events/test_classify.py, tests/visit/test_visit_consumer.py</files>
  <behavior>
    - `sqlstate_of` returns "53300" for a raw `asyncpg.exceptions.TooManyConnectionsError`, for a `sqlalchemy.exc.DBAPIError` whose `.orig` carries `sqlstate="53300"` (the asyncpg dialect sets `sqlstate` and `pgcode` on its translated error), and for an exception whose `__cause__` is the asyncpg error; None for a plain `ValueError`
    - `is_transient_db` is True for TooManyConnectionsError (53300), CannotConnectNowError (57P03), DeadlockDetectedError (40P01), asyncpg SerializationError (40001), AdminShutdownError (57P01), ConnectionDoesNotExistError (08003), SQLAlchemy OperationalError / InterfaceError / an invalidated connection, and `sqlalchemy.exc.TimeoutError`; False for a DBAPIError carrying 23505 or 42P01 and for `KeyError`
    - `is_transient_temporal` is True for `RPCError` UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED and for `RuntimeError`; False for UNKNOWN, INTERNAL, ABORTED, CANCELLED, INVALID_ARGUMENT and NOT_FOUND
    - `handle_visit_completed` with a session factory that raises a raw TooManyConnectionsError (and, separately, a DBAPIError wrapping 53300, a CannotConnectNowError and a DeadlockDetectedError) raises `TransientEventError`
    - `build_consumer(handlers=..., metrics_port=None, heartbeat_path=None, backoff_seconds=0.01)` handling one fake `visit.completed` message whose handler uses that raising factory returns `Outcome.RETRY_LATER`; a recording stand-in for the Kafka consumer shows one seek to the message's offset and no commit; no DLQ producer was created
    - Every existing test in tests/visit/test_visit_consumer.py keeps passing unchanged (the closed-client RuntimeError is still transient, INVALID_ARGUMENT still is not, the OperationalError database outage is still transient)
  </behavior>
  <read_first>
    - services/visit/consumer.py (the module docstring's classification bullets; the local Temporal status set and the local transient helper at lines 76-109; `handle_visit_completed`; `build_consumer` overrides)
    - libs/events/consumer.py (`_handle_message`, `_retry_later`, `_dead_letter`, `Outcome`; the consumer object is `self._consumer`, the decoder is `self._deserializer`)
    - .venv/lib/python3.12/site-packages/sqlalchemy/dialects/postgresql/asyncpg.py lines 780-800 (translated error gets `pgcode` and `sqlstate`) and line 1010 (`_asyncpg_error_translate` maps only a few asyncpg classes)
    - tests/visit/test_visit_consumer.py (`_envelope`, `_BrokenSession`, `test_a_database_outage_is_transient`, `test_a_request_error_is_not_classified_as_an_outage`)
    - tests/visit/conftest.py (`_DownTemporal`)
    - .planning/phases/03-event-driven-system-observability/03-REVIEW.md (CR-01 fix sketch, IN-01, WR-03)
  </read_first>
  <action>
    Create `libs/events/classify.py` with the symbols in the artifacts table. `sqlstate_of` walks `exc`, then `getattr(exc, "orig", None)`, then `__cause__` and `__context__` (at most 5 hops, a seen-set against cycles) and returns the first string attribute `sqlstate` or `pgcode` of length 5. `is_transient_db` decides by `code[:2] in TRANSIENT_SQLSTATE_CLASSES` whenever a code is found (so a unique violation wrapped in any SQLAlchemy class is NOT transient); without a code it keeps the old SQLAlchemy test (OperationalError, InterfaceError, `connection_invalidated`) and adds `sqlalchemy.exc.TimeoutError` (a pool checkout timeout is resource pressure, not a poison event). `is_transient_temporal` uses the narrowed status set (WR-03) plus `RuntimeError`. `is_transient_broker` imports kombu, amqp and pymongo inside the function so the visit and analytics consumers do not load them. `is_transient` composes: db (default on), temporal and broker (off by default), network always. The module docstring names the SQLSTATE classes (08 connection, 53 insufficient resources, 57 operator intervention, 40 transaction rollback), says why UNKNOWN/INTERNAL/ABORTED/CANCELLED are not outages, and states that the classifier never reads or logs exception messages.

    In `services/visit/consumer.py` delete the module's own transient helper function and its Temporal status set (and the now unused `sqlalchemy.exc`, `RPCError` and status-code imports), and call `classify.is_transient(exc, temporal=True)` in `handle_visit_completed`. Rewrite the docstring's classification bullets: SQLSTATE classes and connection errors are transient; a transient failure is retried by the base until the cap of plan 03-21 Task 3, after which it is dead-lettered as TransientExhausted and recovered by the visit reconciler (plan 03-22).

    Tests: `tests/events/test_classify.py` (pure unit, no stack) covers the behaviors above with real asyncpg exception classes (construct them with a message string; they carry their class `sqlstate`) and a `sqlalchemy.exc.DBAPIError("SELECT 1", {}, orig)` whose `orig` has `sqlstate` set like the dialect does. In `tests/visit/test_visit_consumer.py` add the asyncpg cases next to `test_a_database_outage_is_transient` (same `_BrokenSession` pattern raising the asyncpg error from `__aenter__`) and one consumer-base test: build the consumer with the `handlers` override bound to the raising factory, close the real confluent `Consumer` object it created, swap in a small recording stand-in (seek, commit, close, assignment returning an empty list), replace `_deserializer` with a stub that returns a full `visit.completed` record dict (event_type, event_id, aggregate_id, occurred_at, schema_version, visit_id), feed a fake message object (topic, partition, offset, key, value, headers, error methods) to `_handle_message`, assert RETRY_LATER, one seek, zero commits and `consumer._dlq_producer is None`, and close the consumer in `finally`.

    Deploy and prove the tracer: stop `visit-consumer`, run the two test files, then `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-consumer` (this starts it again), wait for `healthy`, apply the gateway budget rule, and run the e2e tracer.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events/test_classify.py tests/visit/test_visit_consumer.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a 53300 / 57P03 / 40P01 error yields anything but TransientEventError and RETRY_LATER, the consumer base commits or dead-letters on that path, or the rebuilt visit-consumer is not healthy or does not start the downstream workflow within the tracer's 15 s.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_classify.py -q` exits 0 with at least 12 passed
    - `grep -c "def _is_transient" services/visit/consumer.py` prints 0
    - `grep -c "RPCStatusCode" services/visit/consumer.py` prints 0
    - `grep -v '^\s*#' services/visit/consumer.py | grep -c "classify"` prints at least 1
    - `docker compose -f infra/docker-compose.yml ps visit-consumer --format '{{.Health}}'` prints `healthy`
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>A database connection or resource error in the visit-downstream handler holds the event (RETRY_LATER) instead of dead-lettering it, through one shared classifier, and the deployed consumer still triggers downstream work.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The analytics and notification consumers use the shared classifier; the Temporal outage set is narrowed</name>
  <files>services/analytics/consumer.py, services/notification/triggers.py, tests/analytics/test_consumer_classification.py, tests/notification/test_triggers.py</files>
  <behavior>
    - analytics `handle_event` with a session factory that raises a raw TooManyConnectionsError, a CannotConnectNowError, a DeadlockDetectedError or a DBAPIError carrying 53300 raises `TransientEventError`; a DBAPIError carrying 23505 is re-raised unchanged (bounded retries, then DLQ); a payload `KeyError` is still `NonRetryableEventError("InvalidEvent")`
    - analytics `build_consumer(...)` handling one fake `patient.registered` message with the raising factory returns `Outcome.RETRY_LATER` with one seek, no commit and no DLQ producer (same stand-in technique as Task 1)
    - notification triggers: an `RPCError` with INTERNAL or UNKNOWN from the reminder helpers is no longer turned into `TransientEventError` (it propagates to the base's bounded retries); UNAVAILABLE still is; the existing parametrized broker, Mongo, `ConnectionError`, `OSError` and closed-client `RuntimeError` cases stay transient
    - No service module defines its own transient predicate or Temporal status set
  </behavior>
  <read_first>
    - services/analytics/consumer.py (docstring classification bullets, the local helper at lines 81-87, `handle_event`, `build_consumer`)
    - services/notification/triggers.py (docstring lines 19-25, the local status set at lines 68-79, the local helper at lines 97-104, `_guarded`)
    - tests/notification/test_triggers.py (the parametrized transient test near line 380 and `test_a_lost_temporal_connection_runtime_error_is_transient` near line 402, the fake helpers' `fail_with`)
    - tests/analytics/conftest.py and tests/analytics/test_projections.py (envelope factory, the malformed-event test)
    - libs/events/classify.py (Task 1)
  </read_first>
  <action>
    In `services/analytics/consumer.py` delete the module's own transient helper and the `sqlalchemy.exc` import, call `classify.is_transient(exc)` (db and network only), and update the docstring bullets (SQLSTATE classes, capped retries, then TransientExhausted; recovery is `services.analytics.rebuild`, which replays retained events idempotently).

    In `services/notification/triggers.py` delete the module's own transient helper and its Temporal status set and call `classify.is_transient(exc, db=False, temporal=True, broker=True)` in `_guarded`; keep the `amqp`, `kombu` and `pymongo` imports only if something else in the module still uses them. Update the docstring bullet: outages are retried until the cap; Temporal UNKNOWN and INTERNAL are treated as handler errors (bounded retries, then DLQ), per WR-03.

    Tests: create `tests/analytics/test_consumer_classification.py` (unit level, no live database: the factory raises before any query) for the analytics behaviors, and add to `tests/notification/test_triggers.py` a parametrized case asserting INTERNAL and UNKNOWN propagate as `RPCError` and UNAVAILABLE becomes `TransientEventError`. Do not weaken or delete any existing assertion. Run `tests/notification` with the live `notification-consumer` left running (its harness uses throw-away vhosts and unique groups, plan 03-14).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_consumer_classification.py tests/analytics/test_projections.py tests/notification/test_triggers.py tests/events/test_classify.py -x -q</automated>
    <fails_when>pytest exits non-zero, an asyncpg resource or connection error in the analytics handler is not transient, a non-transient SQLSTATE becomes transient, or Temporal INTERNAL / UNKNOWN is still mapped to TransientEventError.</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -c "def _is_transient" services/analytics/consumer.py services/notification/triggers.py` prints 0 for both files
    - `grep -c "RPCStatusCode" services/notification/triggers.py` prints 0
    - `grep -v '^\s*#' services/analytics/consumer.py | grep -c "classify"` and the same for `services/notification/triggers.py` each print at least 1
    - `.venv/bin/python -m pytest tests/notification -q` exits 0
  </acceptance_criteria>
  <done>All three consumers classify failures through one module; the analytics projection holds events on database resource errors; Temporal request errors are no longer retried for ever.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Consumer base — transient cap with TransientExhausted, Schema Registry 408/429/5xx retried, commit failures survive; rebuild the three consumers</name>
  <reversibility rating="reversible">Behavioural knobs on the shared base (cap thresholds, commit attempts) with defaults; changing them is a constructor argument.</reversibility>
  <files>libs/events/consumer.py, tests/events/test_consumer_reliability.py</files>
  <behavior>
    - Live broker (unique topic, DLQ topic and group from tests/events/conftest.py, mock registry): with `max_transient_attempts=2, max_transient_seconds=0.0`, a handler that always raises `TransientEventError` gets RETRY_LATER on the first delivery and DEAD_LETTERED on a later one; the DLQ record has the original key and value, `dlq.error_type=TransientExhausted`, `dlq.attempts` equal to the attempts made, no exception text in any header; the committed offset moves past it and the next message is processed
    - Both thresholds are required: with a large time cap and an attempt count already above the floor, the outcome stays RETRY_LATER (drive the clock through a monkeypatched `time.monotonic` in the consumer module); with the defaults, three transient failures in a row are never dead-lettered (the existing `test_transient_error_is_never_dead_lettered_and_retries_until_it_succeeds` passes unchanged)
    - `_decode` (fake message, stubbed `_deserializer`): `SchemaRegistryError` with 408, 429, 500 and 503 gives RETRY_LATER and no DLQ write; 404 and 422 give DEAD_LETTERED with `DeserializationError`; status -1 gives RETRY_LATER; `SerializationError` gives DEAD_LETTERED
    - Commit (stand-in consumer whose commit raises `KafkaException(KafkaError(KafkaError.REQUEST_TIMED_OUT, ..., fatal=False, retriable=True))` twice, then succeeds): the outcome is PROCESSED, `event_commit_failures_total{group}` rose by 2, nothing raised; a commit that always fails non-fatally: PROCESSED after `commit_attempts` tries, counter rose by that many, `event.commit_failed` logged with the error name only; a fatal `KafkaError` raises `KafkaException`
    - The transient count is cleared after a success, a dead-letter or an exhaustion (no unbounded growth of the per-offset map)
  </behavior>
  <read_first>
    - libs/events/consumer.py (module docstring lines 19-24 "Poison messages ... TransientEventError ... is NEVER dead-lettered"; `_handle_message`, `_decode` lines 341-378, `_commit` line 389, `_retry_later` lines 392-402, `_dead_letter`, the metric declarations)
    - tests/events/test_consumer_dlq.py (`_committed`, the DLQ read helpers, `test_transient_error_is_never_dead_lettered_and_retries_until_it_succeeds`, `test_failed_dlq_write_does_not_commit_and_the_message_is_redelivered`)
    - tests/events/conftest.py (`unique_topic`, `dlq_topic`, `unique_group`, `produce_event`, `poll_until`)
    - .planning/phases/03-event-driven-system-observability/03-REVIEW.md (WR-02 second half, WR-03, WR-07)
  </read_first>
  <action>
    Add the constructor keywords `max_transient_attempts: int = 10`, `max_transient_seconds: float = 900.0` and `commit_attempts: int = 3`. Keep, per (topic, partition, offset), the attempt count and the monotonic time of the first transient failure. In `_retry_later`, once the count is at least `max_transient_attempts` AND the elapsed time is at least `max_transient_seconds`, log `event.transient_exhausted` (group, topic, partition, offset, event type, attempts; nothing else) and dead-letter through the existing `_dead_letter` with the new constant `TRANSIENT_EXHAUSTED`, returning DEAD_LETTERED; otherwise seek and back off as today. The cap applies equally to registry-outage decode retries. Clear the map entry on success, dead-letter and exhaustion.

    In `_decode`, a `SchemaRegistryError` whose status is 408 or 429 or at least 500 raises the internal transient decode error; any other status from 400 to 499 is poison; a non-integer or -1 status stays transient.

    Replace `_commit` with a bounded loop: on `KafkaException`, read the `KafkaError` from `exc.args[0]`; a fatal error re-raises; otherwise increment the new Counter `event_commit_failures_total` (label `group`), log `event.commit_failed` with the error name, back off with `_sleep(self._backoff * 2**attempt)` and retry up to `commit_attempts`; after the last failure log `event.commit_abandoned` and return normally (the next successful commit covers the offset, and redelivery is absorbed by the idempotent handlers, D-08). Export `TRANSIENT_EXHAUSTED` and `event_commit_failures_total` in `__all__`.

    Rewrite the module docstring bullets: an outage is retried with capped backoff and, after both thresholds, dead-lettered as TransientExhausted (visible in `dlq_events_total{error_type="TransientExhausted"}`; recovery per group: visit reconciler, analytics rebuild, notification DLQ record re-produced to `dlq.original_topic` by an operator); a failed commit is retried and never ends the process unless fatal.

    Write `tests/events/test_consumer_reliability.py` for the behaviors (the cap test on the live broker; the decode and commit tests with the stand-in technique of Task 1). Then run all of `tests/events` (live broker, about 2 minutes) and rebuild the three consumers: `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-consumer analytics-consumer notification-consumer`, wait until all three are healthy, apply the gateway budget rule, and run the three e2e tracers.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a capped message is dead-lettered with any error type other than TransientExhausted or before both thresholds, a 408 / 429 / 5xx registry answer dead-letters, a non-fatal commit failure raises out of poll_once, or one of the three rebuilt consumers is not healthy.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_consumer_reliability.py -q` exits 0 with at least 8 passed
    - `grep -c "TransientExhausted" libs/events/consumer.py` prints at least 1 and `grep -c "event_commit_failures_total" libs/events/consumer.py` prints at least 2
    - `docker compose -f infra/docker-compose.yml ps visit-consumer analytics-consumer notification-consumer --format '{{.Service}} {{.Health}}'` shows `healthy` for all three
    - the SUMMARY lists WR-01, WR-04, WR-06 and IN-02..IN-06 as open follow-ups (IN-01 closed here)
  </acceptance_criteria>
  <done>No partition can block for ever, the capped record is visible and recoverable, back-pressure answers from the registry are retried, a broker blip during a commit no longer crash-loops the consumers, and all three deployed consumers run the new base.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka topic -> consumer handler | any producer on the unauthenticated dev network can put bytes on a topic; the classifier decides whether they are retried or dead-lettered |
| consumer -> DLQ topic / logs / metrics | failure information leaves the process |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-63 | Denial of Service | `libs/events/consumer.py` `_retry_later` (a crafted or deterministic failure classified transient blocks a partition) | medium | mitigate | Transient cap with both thresholds (10 attempts, 900 s), then dead-letter as TransientExhausted; Temporal UNKNOWN / INTERNAL / ABORTED / CANCELLED no longer transient; tests cover cap and narrowing |
| T-3-64 | Information Disclosure | DLQ headers, `event.transient_exhausted` / `event.commit_failed` logs, `event_commit_failures_total` labels | medium | mitigate | Error TYPE / KafkaError name only, never exception text or payload; labels are the group only; a test asserts no header contains the handler's message text |
| T-3-65 | Denial of Service | `_commit` (broker blip turns into a crash loop of all three consumers) | low | mitigate | Bounded retry with backoff, log and continue for non-fatal errors; only fatal errors raise; idempotent handlers absorb the redelivery |
| T-3-66 | Tampering | classification of SQLSTATE codes (a non-transient error such as a unique violation wrongly retried) | low | mitigate | Classification by SQLSTATE class first; 23505 and 42P01 are tested as non-transient |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/events tests/analytics tests/notification -q` (with `visit-consumer` running; these suites use unique topics, groups and vhosts)
- `.venv/bin/python -m pytest tests/visit/test_visit_consumer.py -q` with `visit-consumer` stopped, then started again
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py tests/e2e/test_phase3_notifications.py -m e2e -k tracer -q`
- Semgrep: `semgrep --config p/python --metrics=off libs/events services/visit/consumer.py services/analytics/consumer.py services/notification/triggers.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- Healthy events are never dead-lettered because of a database connection or resource error (CR-01 classification, WR-02)
- One classifier (IN-01); capped transient retries with a visible, recoverable TransientExhausted record (WR-03); registry 408/429/5xx retried (WR-02); commit failures survive (WR-07)
- The three consumer containers run the new code and their e2e tracers pass
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-SUMMARY.md` when done
</output>
