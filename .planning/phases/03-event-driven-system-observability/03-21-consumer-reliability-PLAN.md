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
  tokens: 95000
  raw_tokens: 95000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Gap 1 (CR-01, WR-02): an asyncpg TooManyConnectionsError (53300), CannotConnectNowError (57P03) or DeadlockDetectedError (40P01), raised raw while a connection opens or wrapped by SQLAlchemy as a DBAPIError, makes the visit-downstream and analytics-projection handlers raise TransientEventError, and the consumer base returns RETRY_LATER: the offset is not committed and nothing is written to the DLQ"
    - "IN-01: one shared classifier, libs/events/classify.py, decides transient versus non-retryable for all three consumers (SQLSTATE classes 08, 53, 57 and 40, SQLAlchemy connection errors and pool timeouts, the seven Temporal outage statuses, broker, Mongo and network errors); no consumer module keeps its own copy, and an AST guard test (with a positive control proving it can fail) rejects any transient-classification function or status / SQLSTATE set under services/ or libs/events/ outside classify.py"
    - "WR-03 is closed by the cap, not by dropping statuses (decision recorded in the SUMMARY): Temporal UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN and INTERNAL, the SDK's closed-client RuntimeError and OSError stay transient, and every transient failure is bounded: a message that keeps failing transiently is dead-lettered with dlq.error_type=TransientExhausted only after BOTH the attempt floor and the time cap are reached (defaults 10 attempts and 900 s), so a Temporal outage longer than the cap yields a visible, recoverable TransientExhausted record (dlq_events_total{error_type=\"TransientExhausted\"}, original key, value and headers on the DLQ topic) instead of a quick dead-letter after 3 attempts or a partition blocked for ever"
    - "WR-02: a Schema Registry 408, 429 or 5xx answer while decoding is retried (RETRY_LATER); other 4xx answers, corrupt Avro and SerializationError are dead-lettered as before"
    - "WR-07: a failed offset commit with a non-fatal KafkaError is retried with bounded backoff, counted in event_commit_failures_total{group} and logged with the error name only, and never ends the consumer process; only a fatal KafkaError still raises"
    - "The rebuilt visit-consumer, analytics-consumer and notification-consumer containers run this code, are healthy, and their e2e tracers pass"
  artifacts:
    - path: libs/events/classify.py
      provides: "sqlstate_of, is_transient_db, is_transient_temporal, is_transient_broker, is_transient_network, is_transient"
      contains: "TRANSIENT_SQLSTATE_CLASSES"
    - path: tests/events/test_classify.py
      provides: "classifier unit tests and test_no_transient_classifier_outside_classify (AST guard with a positive control)"
      contains: "test_no_transient_classifier_outside_classify"
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

Decision (W3, record it in the SUMMARY): WR-03 is closed by bounding, not by narrowing. The Temporal status set stays at the seven statuses the consumers treat as outages today (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN, INTERNAL), because a Temporal frontend restart or overload surfaces as several of them; the SDK's closed-client `RuntimeError` and `OSError` also stay transient. What changes is that no transient failure can retry for ever: after both cap thresholds the record is dead-lettered as `TransientExhausted`, which is visible (metric, DLQ topic) and recoverable (plan 03-22 adds the reconciler pass and a DLQ replay script). Dropping INTERNAL/UNKNOWN to the poison path instead would turn a Temporal outage into a dead-letter after three quick attempts, which is the CR-01 failure mode again.

Proof level (I6, say it in the SUMMARY; plan 03-26 says it in the docs): the CR-01 classification is proven at the real `_handle_message` path at unit level with real asyncpg exception classes (Task 1) plus, for recovery, the live lost-trigger e2e in plan 03-22. No 53300 is injected into the running stack (saturating the shared dev Postgres would disturb every service and the unrelated containers on this host).

Scope guard (user decision D-14): WR-01 (reminder reschedule-signal race), WR-04, WR-06 and IN-02..IN-06 are out of scope; record them as follow-ups in the SUMMARY. No workflow changes, no package installs, no REQUIREMENTS.md edits.

Live stack rules (put in front of every live step):
- Rebuild only with `docker compose -f infra/docker-compose.yml up -d --build --no-deps <names>`; the names in this plan are `visit-consumer`, `analytics-consumer`, `notification-consumer`. Never `down -v`, never remove a volume, never touch the unrelated `tjo-*` containers (record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before and after).
- `tests/visit` writes real visit rows that Debezium relays: stop the live consumer around those runs with `docker compose -f infra/docker-compose.yml stop visit-consumer` and start it again afterwards with `docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer` (returns once it is healthy). Every `<verify>` below that runs a `tests/visit` file does this itself.
- Gateway budget rule: e2e runs need the raised gateway budget. Before the first e2e run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` and say so in the SUMMARY. Plan 03-26 restores the defaults (20 and 300) as its last automated step; do not restore them here.
- The dev Postgres has `max_connections=300`: run one pytest process at a time, never two suites in parallel.
- No direct reads of `.env*` or `infra/secrets/*` (settings modules loading `.env` implicitly is fine), no package installs, PHI never in logs, metric labels or DLQ headers (error type only).
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
| `libs/events/classify.py` | module | `TRANSIENT_SQLSTATE_CLASSES = ("08", "53", "57", "40")`, `TRANSIENT_RPC_STATUSES` (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN, INTERNAL: the set both consumers use today, now in one place and bounded by the cap) |
| `sqlstate_of(exc) -> str \| None` | function | first 5-character `sqlstate` (or `pgcode`) found on `exc`, `exc.orig`, then the `__cause__` / `__context__` chain (depth-bounded, cycle-safe) |
| `is_transient_db(exc) -> bool` | function | SQLSTATE class decides when present; else SQLAlchemy `OperationalError`, `InterfaceError`, `connection_invalidated`, `sqlalchemy.exc.TimeoutError` (pool checkout) |
| `is_transient_temporal(exc) -> bool` | function | `RPCError` with a status in `TRANSIENT_RPC_STATUSES`; `RuntimeError` (the SDK's closed-client signal; kept transient, bounded by the cap) |
| `test_no_transient_classifier_outside_classify` | test in `tests/events/test_classify.py` | AST scan of `services/` and `libs/events/` (classify.py excluded); a positive control proves it flags each shape |
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
    - `is_transient_temporal` is True for `RPCError` UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN and INTERNAL and for `RuntimeError`; False for INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, FAILED_PRECONDITION and PERMISSION_DENIED
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
    Create `libs/events/classify.py` with the symbols in the artifacts table. `sqlstate_of` walks `exc`, then `getattr(exc, "orig", None)`, then `__cause__` and `__context__` (at most 5 hops, a seen-set against cycles) and returns the first string attribute `sqlstate` or `pgcode` of length 5. `is_transient_db` decides by `code[:2] in TRANSIENT_SQLSTATE_CLASSES` whenever a code is found (so a unique violation wrapped in any SQLAlchemy class is NOT transient); without a code it keeps the old SQLAlchemy test (OperationalError, InterfaceError, `connection_invalidated`) and adds `sqlalchemy.exc.TimeoutError` (a pool checkout timeout is resource pressure, not a poison event). `is_transient_temporal` uses the seven-status set from the artifacts table (unchanged from today's consumers, per the W3 decision in the objective) plus `RuntimeError`. `is_transient_broker` imports kombu, amqp and pymongo inside the function so the visit and analytics consumers do not load them. `is_transient` composes: db (default on), temporal and broker (off by default), network always. The module docstring names the SQLSTATE classes (08 connection, 53 insufficient resources, 57 operator intervention, 40 transaction rollback), says why UNKNOWN, INTERNAL, ABORTED, CANCELLED and the closed-client RuntimeError stay transient (a Temporal outage surfaces as them) and that the consumer base's cap bounds them (TransientExhausted), and states that the classifier never reads or logs exception messages.

    In `services/visit/consumer.py` delete the module's own transient helper function and its Temporal status set (and the now unused `sqlalchemy.exc`, `RPCError` and status-code imports), and call `classify.is_transient(exc, temporal=True)` in `handle_visit_completed`. Rewrite the docstring's classification bullets: SQLSTATE classes, connection errors and Temporal outage statuses are transient; a transient failure is retried by the consumer base until its cap, after which it is dead-lettered as TransientExhausted; recovery routes per consumer group are described in docs/architecture.md section 10.2 (do not name plan 03-22's code here: it does not exist yet when this task runs).

    Tests: `tests/events/test_classify.py` (pure unit, no stack) covers the behaviors above with real asyncpg exception classes (construct them with a message string; they carry their class `sqlstate`) and a `sqlalchemy.exc.DBAPIError("SELECT 1", {}, orig)` whose `orig` has `sqlstate` set like the dialect does. In `tests/visit/test_visit_consumer.py` add the asyncpg cases next to `test_a_database_outage_is_transient` (same `_BrokenSession` pattern raising the asyncpg error from `__aenter__`) and one consumer-base test: build the consumer with the `handlers` override bound to the raising factory, close the real confluent `Consumer` object it created, swap in a small recording stand-in (seek, commit, close, assignment returning an empty list), replace `_deserializer` with a stub that returns a full `visit.completed` record dict (event_type, event_id, aggregate_id, occurred_at, schema_version, visit_id), feed a fake message object (topic, partition, offset, key, value, headers, error methods) to `_handle_message`, assert RETRY_LATER, one seek, zero commits and `consumer._dlq_producer is None`, and close the consumer in `finally`.

    Deploy and prove the tracer: stop `visit-consumer`, run the two test files, then `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-consumer` (this starts it again), wait for `healthy`, apply the gateway budget rule, and run the e2e tracer. The `<verify>` below stops and restarts the consumer around the `tests/visit` file itself.
  </action>
  <verify>
    <automated>docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/events/test_classify.py tests/visit/test_visit_consumer.py -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; } && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q</automated>
    <fails_when>The consumer cannot be stopped or does not come back healthy within 180 s, either pytest run exits non-zero, a 53300 / 57P03 / 40P01 error yields anything but TransientEventError and RETRY_LATER, the consumer base commits or dead-letters on that path, or the rebuilt visit-consumer does not start the downstream workflow within the tracer's 15 s.</fails_when>
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
  <name>Task 2: The analytics and notification consumers use the shared classifier, and an AST guard keeps it the only one</name>
  <files>services/analytics/consumer.py, services/notification/triggers.py, tests/analytics/test_consumer_classification.py, tests/notification/test_triggers.py, tests/events/test_classify.py</files>
  <behavior>
    - analytics `handle_event` with a session factory that raises a raw TooManyConnectionsError, a CannotConnectNowError, a DeadlockDetectedError or a DBAPIError carrying 53300 raises `TransientEventError`; a DBAPIError carrying 23505 is re-raised unchanged (bounded retries, then DLQ); a payload `KeyError` is still `NonRetryableEventError("InvalidEvent")`
    - analytics `build_consumer(...)` handling one fake `patient.registered` message with the raising factory returns `Outcome.RETRY_LATER` with one seek, no commit and no DLQ producer (same stand-in technique as Task 1)
    - notification triggers: an `RPCError` with UNAVAILABLE, INTERNAL or UNKNOWN from the reminder helpers becomes `TransientEventError` (kept transient, bounded by the cap of Task 3, per the W3 decision); INVALID_ARGUMENT propagates unchanged as `RPCError` (bounded retries, then DLQ); the existing parametrized broker, Mongo, `ConnectionError`, `OSError` and closed-client `RuntimeError` cases stay transient
    - `test_no_transient_classifier_outside_classify` (in tests/events/test_classify.py) parses every `.py` file under `services/` and `libs/events/` except `libs/events/classify.py` with `ast` and reports: (a) a function or async function whose name matches `^_*(is_)?(transient|retryable)` and whose return annotation is missing or `bool` (a classifier shape; `services/notification/tasks.py`'s Celery retry helper returns an exception and is not one); (b) an assignment whose value is a set, frozenset(...), tuple or list literal containing an `RPCStatusCode.<member>` attribute; (c) an assignment whose value is a literal collection holding two or more of the strings "08", "53", "57", "40"; it asserts the repository scan reports nothing, naming file, line and shape on failure
    - Positive control: the same scanner run on an inline source string that contains one function, one status set and one SQLSTATE tuple of the forbidden shapes reports exactly three findings (so the guard is proven able to fail)
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

    In `services/notification/triggers.py` delete the module's own transient helper and its Temporal status set and call `classify.is_transient(exc, db=False, temporal=True, broker=True)` in `_guarded`; keep the `amqp`, `kombu` and `pymongo` imports only if something else in the module still uses them. Update the docstring bullet: broker, Mongo, network and Temporal outages (including UNKNOWN and INTERNAL) are transient and retried until the consumer base's cap, then dead-lettered as TransientExhausted (W3 decision: bounded, not narrowed).

    Tests: create `tests/analytics/test_consumer_classification.py` (unit level, no live database: the factory raises before any query) for the analytics behaviors, and add to `tests/notification/test_triggers.py` a parametrized case asserting UNAVAILABLE, INTERNAL and UNKNOWN become `TransientEventError` and INVALID_ARGUMENT propagates as `RPCError`. Add the AST guard and its positive control to `tests/events/test_classify.py` now (it would fail in Task 1, while the analytics and notification copies still existed); write the scanner as a small module-level function of the test file that takes source text and a path and returns findings, so the positive control and the repository scan share it. Do not weaken or delete any existing assertion. Run `tests/notification` with the live `notification-consumer` left running (its harness uses throw-away vhosts and unique groups, plan 03-14).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/analytics/test_consumer_classification.py tests/analytics/test_projections.py tests/notification/test_triggers.py tests/events/test_classify.py -x -q</automated>
    <fails_when>pytest exits non-zero: an asyncpg resource or connection error in the analytics handler is not transient, a non-transient SQLSTATE becomes transient, Temporal UNAVAILABLE / INTERNAL / UNKNOWN is not mapped to TransientEventError or INVALID_ARGUMENT is, the AST guard finds a classifier function or status / SQLSTATE set outside libs/events/classify.py, or its positive control reports fewer than three findings.</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -c "def _is_transient" services/analytics/consumer.py services/notification/triggers.py` prints 0 for both files
    - `grep -c "RPCStatusCode" services/notification/triggers.py` prints 0
    - `grep -v '^\s*#' services/analytics/consumer.py | grep -c "classify"` and the same for `services/notification/triggers.py` each print at least 1
    - `.venv/bin/python -m pytest tests/notification -q` exits 0
    - `.venv/bin/python -m pytest tests/events/test_classify.py -q -k "outside_classify"` exits 0 with at least 2 passed (the repository scan and the positive control)
  </acceptance_criteria>
  <done>All three consumers classify failures through one module, a guard test fails if a second copy appears, and the analytics projection holds events on database resource errors.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Consumer base — transient cap with TransientExhausted, Schema Registry 408/429/5xx retried, commit failures survive; rebuild the three consumers</name>
  <reversibility rating="reversible">Behavioural knobs on the shared base (cap thresholds, commit attempts) with defaults; changing them is a constructor argument.</reversibility>
  <files>libs/events/consumer.py, tests/events/test_consumer_reliability.py</files>
  <behavior>
    - Live broker (unique topic, DLQ topic and group from tests/events/conftest.py, mock registry): with `max_transient_attempts=2, max_transient_seconds=0.0`, a handler that always raises `TransientEventError` gets RETRY_LATER on the first delivery and DEAD_LETTERED on a later one; the DLQ record has the original key and value, `dlq.error_type=TransientExhausted`, `dlq.attempts` equal to the attempts made, no exception text in any header; the committed offset moves past it and the next message is processed
    - Both thresholds are required: with a large time cap and an attempt count already above the floor, the outcome stays RETRY_LATER (drive the clock through a monkeypatched `time.monotonic` in the consumer module); with the defaults, three transient failures in a row are never dead-lettered (the existing `test_transient_error_is_never_dead_lettered_and_retries_until_it_succeeds` passes unchanged)
    - A Temporal outage ends in TransientExhausted, not in a quick dead-letter (W3): a handler that maps an `RPCError` with INTERNAL (and, parametrized, UNKNOWN and a closed-client `RuntimeError`) through `classify.is_transient(exc, temporal=True)` as the visit and notification handlers do, run through `_handle_message` with the stand-in technique and the clock driven by the monkeypatched `time.monotonic`: RETRY_LATER for every attempt before both thresholds (well past the base's 3 handler-error attempts), then DEAD_LETTERED with `dlq.error_type=TransientExhausted` (DLQ write captured by a recording stub of `_write_dlq`)
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

    Rewrite the module docstring bullets: an outage is retried with capped backoff and, after both thresholds, dead-lettered as TransientExhausted (visible in `dlq_events_total{error_type="TransientExhausted"}`, original key, value and headers kept on the DLQ topic); the per-group recovery routes are described in docs/architecture.md section 10.2 (name no tool that does not exist yet); a failed commit is retried and never ends the process unless fatal.

    Write `tests/events/test_consumer_reliability.py` for the behaviors (the cap test on the live broker; the decode and commit tests with the stand-in technique of Task 1). Then run all of `tests/events` (live broker, about 2 minutes) and rebuild the three consumers: `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit-consumer analytics-consumer notification-consumer`, wait until all three are healthy, apply the gateway budget rule, and run the three e2e tracers.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/events -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a capped message is dead-lettered with any error type other than TransientExhausted or before both thresholds, a 408 / 429 / 5xx registry answer dead-letters, a non-fatal commit failure raises out of poll_once, or one of the three rebuilt consumers is not healthy.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/events/test_consumer_reliability.py -q` exits 0 with at least 10 passed
    - `grep -c "TransientExhausted" libs/events/consumer.py` prints at least 1 and `grep -c "event_commit_failures_total" libs/events/consumer.py` prints at least 2
    - `docker compose -f infra/docker-compose.yml ps visit-consumer analytics-consumer notification-consumer --format '{{.Service}} {{.Health}}'` shows `healthy` for all three
    - the SUMMARY lists WR-01, WR-04, WR-06 and IN-02..IN-06 as open follow-ups (IN-01 closed here), records the W3 decision (RuntimeError, OSError and the seven Temporal statuses stay transient, bounded by the 10-attempt / 900 s cap) and the I6 proof level (unit-level `_handle_message` with real asyncpg classes; no live 53300 injection)
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
| T-3-63 | Denial of Service | `libs/events/consumer.py` `_retry_later` (a crafted or deterministic failure classified transient blocks a partition) | medium | mitigate | Transient cap with both thresholds (10 attempts, 900 s), then dead-letter as TransientExhausted; every transient class (including Temporal UNKNOWN / INTERNAL and RuntimeError) is bounded by it; tests cover the cap generically and for a Temporal INTERNAL handler; the AST guard prevents an unbounded private classifier from reappearing |
| T-3-64 | Information Disclosure | DLQ headers, `event.transient_exhausted` / `event.commit_failed` logs, `event_commit_failures_total` labels | medium | mitigate | Error TYPE / KafkaError name only, never exception text or payload; labels are the group only; a test asserts no header contains the handler's message text |
| T-3-65 | Denial of Service | `_commit` (broker blip turns into a crash loop of all three consumers) | low | mitigate | Bounded retry with backoff, log and continue for non-fatal errors; only fatal errors raise; idempotent handlers absorb the redelivery |
| T-3-66 | Tampering | classification of SQLSTATE codes (a non-transient error such as a unique violation wrongly retried) | low | mitigate | Classification by SQLSTATE class first; 23505 and 42P01 are tested as non-transient |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/events tests/analytics tests/notification -q` (with `visit-consumer` running; these suites use unique topics, groups and vhosts)
- `docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit/test_visit_consumer.py -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; }`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py tests/e2e/test_phase3_notifications.py -m e2e -k tracer -q`
- Semgrep: `semgrep --config p/python --metrics=off libs/events services/visit/consumer.py services/analytics/consumer.py services/notification/triggers.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- Healthy events are never dead-lettered because of a database connection or resource error (CR-01 classification, WR-02)
- One classifier, guarded by an AST test that can fail (IN-01); every transient class, including a Temporal outage, bounded by the cap with a visible TransientExhausted record (WR-03, recovered by plan 03-22's tools); registry 408/429/5xx retried (WR-02); commit failures survive (WR-07)
- The three consumer containers run the new code and their e2e tracers pass
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-SUMMARY.md` when done
</output>
