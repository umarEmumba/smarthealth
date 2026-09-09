# Pitfalls Research

**Domain:** Distributed, event-driven healthcare-operations backend (appointment scheduling, visit workflows, billing pre-checks) — FastAPI + PostgreSQL + MongoDB + Redis + Temporal + Kafka/Schema Registry + Celery/RabbitMQ + Prometheus/Grafana/Jaeger/OpenTelemetry
**Researched:** 2026-09-09
**Confidence:** MEDIUM-HIGH (cross-checked across multiple independent sources per topic; no official case study specific to this exact 8-technology combination, so composition-level risks are reasoned from component-level evidence)

This document catalogues the concrete, stack-specific mistakes teams make when building systems like SmartHealth Part A — where Temporal orchestrates the booking saga, Kafka carries the event backbone, Celery/RabbitMQ runs background jobs, PostgreSQL is the system of record, MongoDB holds documents (audit trail, visit/service records), and Redis provides hot-read caching plus coordination. Every pitfall below maps to a specific week in the 3-week execution plan (Week 1 Foundation, Week 2 Scheduling, Week 3 Events/Observability).

---

## Critical Pitfalls

### Pitfall 1: Double-booking via check-then-insert without row locking

**What goes wrong:**
Two concurrent booking requests for the same provider/slot both run "check availability → insert appointment" as separate statements. Both read "slot free" before either commits its insert, so both proceed and the same slot gets double-booked. This is the single most common scheduling-system bug and it is exactly the failure mode the Requirements doc calls out ("Schedule conflicts should be prevented").

**Why it happens:**
Application code treats availability check and reservation as two independent queries instead of one atomic operation. It's easy to write and passes all single-user testing — it only fails under concurrent load, which most local dev/testing never exercises.

**How to avoid:**
- Use `SELECT ... FOR UPDATE` inside a single PostgreSQL transaction to lock the candidate slot row(s) before checking availability and inserting the appointment — lock, verify, write, commit, in that order, in one transaction.
- Prefer a **PostgreSQL exclusion constraint** (`EXCLUDE USING gist`) on `(provider_id, tsrange(start_time, end_time)) WHERE overlaps` as a database-level guarantee that is enforced even if application logic has a bug — this is the belt-and-suspenders answer graders will look for.
- If using Temporal to orchestrate the reserve-slot activity, the activity itself must do the locked check+insert against Postgres — Temporal retries do not make a racy SQL pattern safe; they just retry the race.
- Do not rely solely on Redis distributed locks as the sole double-booking guard (see Pitfall 7) — Redis lock + Postgres constraint together, constraint is the source of truth.

**Warning signs:** Booking service passes single-request manual tests but a k6/locust load test with 50 concurrent requests against the same slot produces more than one "confirmed" appointment; appointment counts don't match reserved-slot counts.

**Phase to address:** Week 2 (Scheduling) — this is the core deliverable of the scheduling workflow. Write the concurrency test (parallel booking attempts on one slot) as part of the Week 2 Definition of Done, not as an afterthought.

---

### Pitfall 2: Temporal workflow code contains non-deterministic operations

**What goes wrong:**
Workflow code (not activity code) calls `datetime.now()`, `random`, makes a direct DB/HTTP call, or iterates a Python dict/set without deterministic ordering. Temporal replays workflow history by re-executing workflow code from the start on worker recovery/upgrade. If the replay produces a different sequence of commands than what's in history, the SDK throws a `NonDeterminismError` and the workflow gets stuck — this typically surfaces days after deploy, during a worker restart, not at write time.

**Why it happens:**
Temporal's execution model (workflow code = pure orchestration logic that must be deterministic; activities = the only place with side effects) is unintuitive to developers coming from typical request/response or Celery-style task backgrounds where "just write a function" is the norm. Reading the current time or calling an external service directly inside `@workflow.defn` feels natural but breaks replay.

**How to avoid:**
- All I/O, randomness, and wall-clock reads happen only inside `@activity.defn` functions — the reserve-slot check, billing pre-check call, notification dispatch, DB writes are all activities, never inline workflow logic.
- Use `workflow.now()` (not `datetime.now()`) and `workflow.uuid4()` inside workflow code when a value is needed there.
- Any change to a workflow's code path (added/removed/reordered activity calls, changed branching) that will run against **already-in-flight** workflow executions must be gated with the SDK's versioning API (`workflow.get_version` / patch markers), not just deployed directly.
- Adopt **replay testing** early: capture real event histories from dev/staging and replay them in CI against the current workflow code before every deploy — this is the only reliable way to catch determinism breaks before they hit an in-flight booking workflow.

**Warning signs:** Worker logs show `NonDeterminismError` after a deploy; workflows that were "stuck" or "failed to complete" correlate with a recent code push, not a specific business input.

**Phase to address:** Week 2 (Scheduling) when the booking saga workflow is first built — establish the workflow/activity boundary convention immediately, before any orchestration logic accumulates on the wrong side of it. Revisit in Week 3 if workflow logic changes as events are added.

---

### Pitfall 3: Non-idempotent Temporal activities and Kafka/Celery consumers cause duplicate side effects on retry

**What goes wrong:**
Temporal automatically retries failed activities. Kafka consumers redeliver on rebalance/crash-before-commit. Celery redelivers when `acks_late=True` and a worker dies mid-task. In every one of these cases, the same logical operation runs twice. If the activity/consumer/task isn't idempotent, retries cause double notifications sent, double billing pre-check charges initiated, double analytics counters incremented, or duplicate audit-trail entries — directly violating the Requirements' "duplicate prevention" and "idempotent retries" requirements for the visit/service workflow.

**Why it happens:**
Idempotency isn't the default in any of these systems — it must be designed in explicitly. Developers test the happy path once, see it work, and don't test what happens when the exact same message/activity input is replayed.

**How to avoid:**
- Give every event/command a stable **idempotency key** (e.g., `appointment_id + event_type + attempt_correlation_id` or a client-supplied request ID) and store it in a dedup table/unique index (Postgres) or a Redis `SETNX` with TTL before executing side effects — check-and-mark before doing the work, but only mark "done" *after* the side effect succeeds (marking before risks silently dropping real failures, marking only after guarantees at-least-once + dedup = effectively-once).
- Temporal activities: use natural idempotency (upserts, `INSERT ... ON CONFLICT DO NOTHING`) rather than blind inserts; where an external call (e.g., billing pre-check) doesn't support idempotency keys itself, gate it with a local idempotency record.
- Kafka consumers: never commit offsets before business logic completes; combine `enable.auto.commit=false` with manual commit after processing, and layer a persisted dedup key check for operations with real-world side effects (sending a notification, writing billing state) since offset management alone only prevents re-reading, not re-processing after a partial failure.
- Celery: use `task_acks_late=True` + `task_reject_on_worker_lost=True` so a crashed worker's task is redelivered, and make the task body idempotent (`get_or_create`/`update_or_create` patterns, unique constraints) — acks_late without idempotent task bodies just converts "lost work" into "duplicated work."

**Warning signs:** Patients report receiving duplicate reminder SMS/emails; analytics counters (e.g., total appointments booked) drift upward faster than real booking volume; audit trail has multiple identical entries with different timestamps for one logical action.

**Phase to address:** Week 2 for Temporal activity idempotency (visit/service workflow retries); Week 3 for Kafka consumer and Celery task idempotency as the event backbone and background workers come online. Add an explicit "replay this exact message/activity twice, assert no duplicate side effect" test to each consumer/activity's Definition of Done.

---

### Pitfall 4: Dual-write inconsistency between PostgreSQL and Kafka (or MongoDB)

**What goes wrong:**
The booking service writes the appointment to PostgreSQL, then separately publishes a Kafka event (or writes an audit/visit record to MongoDB) as a second, unrelated operation. If the process crashes, the DB connection drops, or the Kafka broker is briefly unavailable between the two writes, one write succeeds and the other doesn't — Postgres says "confirmed" but no downstream event fires (no notification scheduled, no analytics update), or an event fires for a DB write that later rolled back. This is the classic **dual-write problem** and it directly threatens the project's core value proposition ("partial failures never corrupt scheduling state").

**Why it happens:**
Postgres and Kafka (and MongoDB) are separate systems with no shared transaction coordinator. Application code that does `db.commit(); kafka_producer.send(event)` looks correct and works in every demo — it only fails under the exact crash/network-blip timing window, which is rare enough to pass code review but common enough to occur in production within weeks.

**How to avoid:**
- Implement the **transactional outbox pattern**: write the domain change (appointment row) and an outbox row (serialized event payload) to PostgreSQL in the *same* transaction. A separate relay process (polling publisher, or Debezium CDC off the Postgres WAL) reads committed outbox rows and publishes them to Kafka, then marks them published. This guarantees the event is emitted if and only if the DB transaction committed.
- For the MongoDB side (audit trail, visit/service documents that are secondary/derived data, not the transactional source of truth), treat Mongo writes as **downstream consumers of Kafka events**, not as a second synchronous write in the same request path — the booking API writes Postgres + outbox; a consumer service later projects into MongoDB. This also resolves "which DB is authoritative" ambiguity (Postgres is authoritative for appointments; Mongo is a queryable projection).
- Accept that the outbox relay introduces **eventual consistency and at-least-once delivery** — downstream consumers (including the Mongo projector) must be idempotent (see Pitfall 3) since the outbox pattern guarantees delivery, not exactly-once.
- Do not attempt distributed 2PC/XA transactions across Postgres and Kafka — Kafka doesn't support XA, and even where 2PC is technically possible (e.g., with some brokers), it's operationally fragile and rarely used in production for this reason.

**Warning signs:** Appointments exist in Postgres with no corresponding Kafka event ever observed in the topic (detectable via a periodic reconciliation job comparing Postgres row counts/IDs against consumed event IDs); analytics dashboards under-count relative to actual bookings.

**Phase to address:** Week 3 (Event-Driven System) when Kafka integration is built — but the **outbox table schema** should be designed in Week 1/2 alongside the appointments schema so it's not bolted on later. Flag this phase for deeper research: outbox relay implementation (Debezium CDC vs. custom polling publisher) is a real design decision worth its own spike.

---

### Pitfall 5: Kafka consumer ordering assumptions broken by partitioning and rebalancing

**What goes wrong:**
Kafka guarantees order **within a partition only**. If appointment events for the same appointment_id are producer-keyed inconsistently (or not keyed at all, defaulting to round-robin), a "cancelled" event can land on a different partition than the "booked" event and be processed first by a different consumer instance — the system then processes a cancellation for an appointment that "doesn't exist yet" from its point of view. Separately, consumer group rebalances (a pod restart, scaling event, or slow consumer triggering a session timeout) can pause partition consumption or, with the default eager rebalance protocol, briefly halt the entire group's consumption ("stop-the-world").

**Why it happens:**
Developers assume Kafka is a globally ordered log because that's how they mentally model "the event stream." The partition-level ordering caveat is easy to miss until multi-partition topics are used at scale, which is exactly when message volume makes it matter most (peak booking windows — precisely this project's stated scalability target).

**How to avoid:**
- **Always key events by the entity they pertain to** — use `appointment_id` (or `provider_id` for schedule-change events where per-provider ordering matters) as the Kafka message key so all events for that entity land on the same partition and are processed in order by one consumer.
- Design consumers to be resilient to out-of-order delivery anyway (defense in depth): include an event sequence number or timestamp in the payload and have consumers reject/reorder-buffer stale events (e.g., ignore a "booked" event for an appointment already marked "cancelled" with a later sequence number) rather than assuming order is guaranteed.
- Use the **cooperative-sticky** partition assignment strategy (`partition.assignment.strategy=cooperative-sticky`) instead of the eager default to avoid full stop-the-world rebalances when scaling consumers up/down.
- Size consumer `max.poll.interval.ms` and processing time so slow processing (e.g., a consumer that calls a slow billing service) doesn't trigger spurious rebalances — offload slow work to Celery/RabbitMQ instead of doing it inline in the Kafka consumer (this is exactly why the stack has both Kafka and Celery — keep Kafka consumers fast/thin, dispatch slow work to Celery tasks).

**Warning signs:** Intermittent "cancel appointment failed: appointment not found" errors under load; event-processing-order bugs that only reproduce with multiple partitions/consumers, never in single-partition dev setups.

**Phase to address:** Week 3 (Event-Driven System) — decide partition keying strategy and consumer group topology explicitly as part of the Kafka + Schema Registry setup, not implicitly via defaults.

---

### Pitfall 6: Celery/RabbitMQ tasks lost or duplicated due to ack timing misconfiguration

**What goes wrong:**
Two opposite failure modes, both common: (1) default early-ack behavior acknowledges a task to RabbitMQ *before* it runs — if the worker crashes mid-task, the task is gone forever (lost reminder notifications, lost follow-up scheduling); (2) `acks_late=True` is turned on but tasks aren't idempotent — a worker crash mid-task causes RabbitMQ to redeliver, and the task runs again, causing duplicate notifications/side effects (converting Pitfall 3 into a guarantee rather than a possibility).

**Why it happens:**
Celery's default (`task_acks_late=False`) optimizes for "don't double-run tasks" at the cost of "tasks can be silently lost on crash" — the opposite tradeoff of what a reliability-graded healthcare scheduling system needs. Teams often don't realize this default exists until a worker OOM-kill or deploy-restart loses reminders in production.

**How to avoid:**
- Set `task_acks_late=True` and `task_reject_on_worker_lost=True` globally for any task with real-world side effects (notifications, billing pre-check triggers, follow-up scheduling) — accept redelivery risk and handle it via idempotency (Pitfall 3), since losing a healthcare reminder is worse than occasionally re-sending one (which idempotency prevents anyway).
- Configure RabbitMQ **durable queues + persistent messages** (`delivery_mode=2`) so broker restarts don't lose queued tasks outright.
- Set explicit `visibility`/`retry` policies with capped `max_retries` and exponential backoff (`retry_backoff=True`) — unbounded retries against a permanently-failing downstream (e.g., SMS provider down) will otherwise flood queues and starve other workers.
- Use a **dead-letter queue** for tasks that exhaust retries so failures are visible and reprocessable rather than silently dropped — critical for the "diagnose failures in reminder notifications/background workers" observability requirement.

**Warning signs:** Reminder/notification delivery rate is measurably lower than booking rate with no corresponding error logs (silent loss); or duplicate reminder complaints correlate with recent worker restarts/deploys (redelivery without idempotency).

**Phase to address:** Week 3 (Event-Driven System) when Celery + RabbitMQ workers are built for notifications/reminders/follow-ups — configure ack/retry/DLQ policy as part of the initial worker setup, not as a later hardening pass.

---

### Pitfall 7: Over-reliance on Redis locks for correctness-critical slot reservation

**What goes wrong:**
Teams use a Redis-based distributed lock (`SET key val NX PX ttl` or Redlock across multiple Redis instances) as the *sole* mechanism preventing double-booking. Redis locks have a fundamental gap: if the lock holder is paused (GC pause, slow I/O, network partition) longer than the TTL, the lock expires and a second process acquires it — both processes can now believe they hold exclusive access simultaneously, and both proceed to write. Redlock specifically has known, actively-debated safety issues around clock behavior across independent Redis nodes.

**Why it happens:**
Redis locking is fast to implement and works in almost all manual testing — the failure window (a process pausing exactly across the TTL boundary) is rare but not exotic under real container/GC/network conditions at the "tens of thousands of requests" scale this project targets.

**How to avoid:**
- Use Redis locks/coordination for **performance** (reducing contention, fast-failing obviously-taken slots before hitting Postgres) but never as the sole correctness guarantee — the PostgreSQL exclusion constraint or `SELECT FOR UPDATE` transaction (Pitfall 1) is the actual source of truth for "is this slot booked."
- If a distributed lock is used, always store a unique owner token and release via a Lua script that checks ownership before deleting (prevents releasing someone else's lock after your own TTL expired) — never a blind `DEL`.
- Keep TTLs short relative to expected operation time and make the guarded operation itself safe to run twice (idempotent), so an expired-lock double-execution is a no-op rather than a corruption.

**Warning signs:** Rare, hard-to-reproduce double-bookings that only show up under sustained load or when workers experience GC/CPU pressure (e.g., during a deploy or noisy-neighbor event) — these look like "flaky" bugs precisely because the failure window is a timing race.

**Phase to address:** Week 2 (Scheduling) — define Redis's role explicitly as "fast-path availability hint + rate limiting," with Postgres constraints as the enforced invariant, before Redis locking code is written.

---

### Pitfall 8: Missing trace context propagation across async boundaries (Kafka, Celery, Temporal)

**What goes wrong:**
A request enters via FastAPI, gets traced by OpenTelemetry auto-instrumentation, then triggers a Celery task or publishes a Kafka event or starts a Temporal workflow. Each of these is a *new* execution context on a different process/worker. Without explicit propagation, the trace context (trace ID / span ID) does not travel with the message — the async work shows up in Jaeger as a disconnected, orphaned trace with no parent, making it impossible to follow one appointment booking from HTTP request through slot reservation, event publication, notification dispatch, and billing pre-check as a single trace. This directly undermines the "diagnose failures in booking workflows / background workers" observability requirement.

**Why it happens:**
OpenTelemetry's auto-instrumentation handles the synchronous, same-process, HTTP-header-based propagation case well out of the box, but every message-passing boundary (Kafka message, Celery task headers, Temporal workflow/activity boundary) requires explicit inject/extract calls that aren't automatic by default and are easy to skip under deadline pressure — the app "works" and traces "exist," just not end-to-end, so the gap isn't obvious until someone tries to debug a real cross-service failure.

**How to avoid:**
- **Kafka:** Inject the W3C Trace Context (`traceparent` header) into Kafka message headers at publish time (ideally inside the same outbox-relay/producer code path from Pitfall 4); extract and set as the active parent context at the start of consumer processing before creating any new spans.
- **Celery:** Inject trace context into task headers when calling `apply_async` (capture context while the original request's context is still active — injecting inside the task itself is too late since the original context is gone); extract at the very start of task execution before instrumentation creates the task's span, so the task span attaches as a child of the originating request span. Community Celery OTel instrumentation packages support this pattern directly, but it must be configured, not assumed.
- **Temporal:** Propagate trace context via Temporal's supported context propagation interceptors — Temporal has first-class support for this specifically because workflow/activity boundaries are also async and cross-process; use it rather than hand-rolling. Confirm the tracing interceptor is registered on both workflow and activity workers.
- Instrument early (Week 1 foundation, when FastAPI/OTel/Jaeger are first wired up) and validate with a manual "one trace, five hops" smoke test (HTTP → Temporal workflow → activity → Kafka event → Celery task) before declaring observability "done" for any phase — a trace that stops at the first async hop is a common false-positive for "tracing is set up."

**Warning signs:** Jaeger UI shows many short, single-span traces for background work instead of long multi-service traces; on-call debugging of a "booking got stuck" incident requires manually correlating logs by appointment_id across services because traces don't connect the dots.

**Phase to address:** Week 1 (Foundation) — set up OTel SDK + Jaeger exporter + propagation conventions as part of initial service scaffolding, so every service built afterward inherits correct propagation by default. Re-verify explicitly in Week 3 when Kafka/Celery/Temporal integrations are added, since each is a new propagation boundary.

---

### Pitfall 9: Saga compensating actions modeled as database rollbacks instead of business-level undo operations

**What goes wrong:**
When the booking saga fails partway (e.g., slot reserved successfully but billing pre-check fails), the "compensation" is implemented as literally deleting the appointment row or undoing state as if the earlier step never happened. This breaks down the moment any external, already-observed side effect occurred (e.g., a confirmation notification was already sent, or a downstream analytics event was already consumed) — you can't "un-send" a notification, and deleting the row loses the audit trail the Requirements explicitly demand.

**Why it happens:**
"Rollback" is the mental model from single-database ACID transactions; developers unconsciously carry it into multi-step, multi-system saga design where each step already committed durably and independently.

**How to avoid:**
- Model every saga step's compensation as a **new forward business transaction**, not a rollback: "release the slot" (a new state transition to `released`, not row deletion), "reverse billing pre-check" (a cancellation/void record, not a deleted charge), "send cancellation notice" (a new notification, not un-sending the old one).
- Preserve the full state-transition history (appointment goes `pending → reserved → failed → compensated`, never disappears) — this satisfies both correctness and the audit-trail requirement simultaneously.
- In Temporal specifically, structure the workflow so each activity that has a compensating action registers/tracks it (e.g., a compensation stack), and run compensations in reverse order on failure — this is a well-established Temporal saga pattern, not a custom invention.
- Avoid synchronous HTTP calls between saga steps for anything that can instead go through Temporal activities or Kafka events — a synchronous call that times out drops saga state with no automatic path to compensate, whereas Temporal retries/compensates within its durable execution model.

**Warning signs:** Failed bookings leave no trace in the audit trail (row was hard-deleted); support/mentor review can't reconstruct "what happened" for a failed booking attempt from the data alone.

**Phase to address:** Week 2 (Scheduling) when the booking saga's failure/compensation paths are designed — this should be designed alongside the happy path, not added after the happy path works.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|--------------------|-----------------|------------------|
| Skip the outbox pattern; publish Kafka events directly after Postgres commit in application code | Faster to build, fewer moving parts | Dual-write inconsistency (Pitfall 4) surfaces under crashes/network blips, silently corrupting the "every event has a corresponding DB state" invariant | Never for booking/cancellation/billing events; acceptable only for non-critical, best-effort events (e.g., a UI "activity feed" ping) where occasional loss is truly harmless |
| Use Redis lock alone for slot reservation, skip Postgres exclusion constraint | Simple, fast, one less migration | Rare but real double-booking under GC pause / TTL race (Pitfall 7) | Never for the core booking path; fine for lower-stakes advisory locks (e.g., "someone is viewing this record" UI hints) |
| Leave Celery `task_acks_late=False` (default) | Slightly simpler mental model, no redelivery to handle | Silent task loss on worker crash — lost reminders/notifications with no error surfaced | Never for notification/billing/analytics tasks; only acceptable for cheap, frequently-repeating, non-critical tasks (e.g., a metrics heartbeat) |
| Instrument tracing only on the FastAPI HTTP layer, defer Celery/Kafka/Temporal propagation to "later" | Faster initial observability setup, dashboards look populated | Traces silently stop at first async hop; debugging real incidents becomes log-grepping across services (Pitfall 8) | Acceptable only for the very first days of Week 1 scaffolding, must be closed out before Week 3 event/observability phase is called done |
| Write MongoDB audit/visit documents synchronously in the same request as the Postgres write (not via Kafka projection) | One less moving part, immediate consistency-looking behavior | Reintroduces the dual-write problem in a second place (Pitfall 4); MongoDB write failure now also fails the booking request unnecessarily | Acceptable only if the Mongo write can be made truly optional/best-effort (fire-and-forget, not blocking the booking response) — but then it's not durable, so prefer Kafka-projected pattern instead |
| Use `datetime.now()` / random module inside Temporal workflow code "just this once" for a quick timestamp | Saves a few minutes vs. wiring up `workflow.now()` | Non-determinism error on next replay (Pitfall 2), can strand in-flight workflows | Never |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|-----------------|--------------------|
| Kafka + Schema Registry | Deploying a producer schema change (adding/removing/renaming a required field) without checking compatibility mode, breaking existing consumers on deserialization | Set the registry's compatibility mode explicitly (`BACKWARD` is the safe default for this project — new schema can read data written by old schema), and treat schema changes as a reviewed step, not an incidental side effect of a code change |
| Temporal + Postgres/Mongo (activities) | Activity code assumes it runs exactly once and skips its own idempotency handling because "Temporal handles retries" | Temporal retries the *activity*, not the side effect it already caused — activity code must itself be idempotent (Pitfall 3); Temporal's retry guarantee is "the activity will eventually run to completion," not "it will run exactly once" |
| Celery + RabbitMQ | Treating Celery/RabbitMQ and Kafka as interchangeable "message queues" and duplicating booking/cancellation *events* onto Celery instead of Kafka | Keep the split intentional: Kafka = durable, replayable, multi-consumer event log (booking/cancellation/analytics events other services need to react to); Celery+RabbitMQ = fire-and-forget discrete background jobs (send this one notification, run this one reminder) — don't blur the line or you lose Kafka's replayability for events that matter |
| Redis (cache + lock) | Using the same Redis instance/eviction policy for both the hot-read cache (evictable) and coordination locks (must never be evicted mid-hold) | Separate Redis logical databases or key namespaces/TTL policies for cache vs. lock keys, and ensure eviction policy (`maxmemory-policy`) doesn't apply to lock keys — an evicted lock key is indistinguishable from an expired one and silently breaks mutual exclusion |
| OpenTelemetry + Jaeger | Assuming OTel auto-instrumentation covers Kafka/Celery/Temporal boundaries "because it's installed" | Explicitly verify and wire propagation for each async boundary (Pitfall 8) — auto-instrumentation covers HTTP/DB drivers well, not custom message-passing boundaries by default |
| PostgreSQL + MongoDB (polyglot) | No clear rule for which store owns which entity, leading to the same fact (e.g., appointment status) being independently writable in both, then drifting | Decide ownership explicitly per entity up front: Postgres owns all transactional/relational entities (patients, providers, appointments, billing); MongoDB owns only derived/document-shaped data (audit log entries, visit/service records, notification logs) populated as a projection from Postgres-sourced events — never let Mongo be a second writable source of truth for the same fact |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|-----------------|
| Long-held Postgres row locks (`SELECT FOR UPDATE`) across an entire multi-step booking transaction, including waiting on external calls (billing pre-check) inside the same DB transaction | Booking requests queue up, latency spikes during peak booking windows, lock wait timeouts | Keep the DB transaction that holds the slot lock as short as possible — lock, check, insert, commit immediately; do billing pre-check as a separate Temporal activity/step outside the row lock, using appointment status (`pending`) to hold the reservation instead of an open transaction | Breaks first under "peak booking windows" — exactly this project's stated scale target — well before "tens of thousands of requests" if lock hold time includes any network call |
| Eager Kafka consumer rebalance strategy at multi-consumer scale | Processing pauses/latency spikes whenever a consumer pod restarts or scales, worse as consumer group grows | Use `cooperative-sticky` assignment strategy; keep consumer processing fast by offloading slow work to Celery (Pitfall 5) | Becomes noticeable once consumer group has more than a handful of instances or restarts are frequent (rolling deploys, autoscaling) |
| Redis used as both cache and lock coordinator without capacity planning | Lock acquisition failures or evicted cache entries under memory pressure during traffic spikes | Size Redis memory with headroom, separate key namespaces/eviction policies for cache vs. coordination (see Integration Gotchas) | Surfaces during exactly the "peak booking windows" this project must handle |
| Temporal workflow history growing unbounded from long-running or high-frequency-activity workflows (e.g., a single workflow per provider that never completes) | Workflow history size limits hit, replay becomes slow, worker CPU spikes on replay | Model one workflow per appointment/booking-saga (short-lived, bounded activity count), not one long-lived workflow per provider/day; use Continue-As-New for any genuinely long-running workflow | Becomes a problem once workflows accumulate hundreds of events — unlikely at this project's scope if workflow-per-appointment is used correctly, but a real risk if workflow scope is designed too broadly |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Logging full request/event payloads (including patient name, DOB, appointment reason) at INFO/DEBUG level for tracing/debugging convenience | PHI leakage into logs/traces that flow to Grafana/Jaeger, which may have broader access than the application itself | Scrub/redact PHI fields before logging; log identifiers (appointment_id, patient_id as opaque UUID) not PHI content; apply this rule to OpenTelemetry span attributes too, not just application logs |
| Embedding patient-identifying data directly in Kafka message keys or as plaintext in event payloads consumed broadly across services | Widens PHI exposure surface to every consumer of the topic, including analytics/observability tooling that doesn't need patient identity | Key events by opaque IDs; keep PHI fields minimal in event payloads, fetch full patient details from the system of record (Postgres) only in the service that actually needs them |
| No audit trail entry for automated/system-initiated state changes (e.g., a Temporal compensation or a Celery-driven auto-cancellation), only for user-initiated actions | Incomplete audit trail — the Requirements explicitly demand an audit trail for all operational changes, not just user-clicked ones | Every state transition, whether user-initiated or system/saga-initiated, writes an audit entry with actor = system/workflow_id where relevant |

## "Looks Done But Isn't" Checklist

- [ ] **Slot reservation:** Passes manual single-request testing — verify with a concurrent load test (parallel requests against the same slot) that a DB-level constraint (not just app logic) prevents double-booking.
- [ ] **Idempotent retries:** Temporal activity/Kafka consumer/Celery task "handles retries" — verify by manually replaying the exact same input twice and confirming no duplicate side effect (not just "no error thrown").
- [ ] **Event-driven backbone:** Kafka events are published — verify a full crash-mid-request test (kill the process between DB commit and event publish) confirms no dual-write inconsistency; check that the outbox/relay mechanism is actually the publish path, not a direct producer call in the request handler.
- [ ] **Observability / tracing:** Jaeger shows traces — verify a single trace spans HTTP request through Temporal workflow/activity, Kafka event, and Celery task as one connected trace, not four disconnected ones.
- [ ] **Audit trail:** Present for user actions — verify system/saga-initiated compensating actions (auto-release, auto-cancel) also produce audit entries.
- [ ] **Cancellation/reschedule:** Slot release "works" — verify the released slot correctly reappears as bookable (waitlist movement, availability cache invalidation in Redis) and doesn't leave a stale hold.
- [ ] **Schema Registry:** Kafka producers/consumers "work" — verify a schema evolution (add optional field) doesn't break existing consumers under the configured compatibility mode; test this explicitly, don't assume it.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|-----------------|------------------|
| Double-booking discovered in production data | MEDIUM | Add the exclusion constraint retroactively (will fail migration if existing violations exist — must reconcile duplicates manually first); backfill a reconciliation script to flag/resolve conflicting appointments; add the concurrency load test to CI going forward |
| Dual-write inconsistency (orphaned Postgres rows with no Kafka event, or vice versa) | MEDIUM-HIGH | Build a reconciliation job comparing Postgres appointment IDs against consumed/observed Kafka event IDs; backfill missing events from Postgres state; migrate to outbox pattern going forward so this class of bug can't recur |
| Non-deterministic Temporal workflow stuck mid-execution | LOW-MEDIUM | Use Temporal's replay tooling to diagnose the exact divergence point; for isolated stuck workflows, terminate and manually replay the intended business outcome; for a systemic version mismatch, roll back the offending deploy and re-deploy with proper `GetVersion` gating |
| Duplicate notifications sent to patients | LOW | Add/backfill an idempotency key check in the notification-sending task; no data corruption, just a support/goodwill issue — communicate and move on |
| Missing trace propagation discovered late (Week 3) | LOW-MEDIUM | Retrofitting propagation into existing Celery/Kafka/Temporal call sites is straightforward (inject at dispatch, extract at consumption) but requires touching every call site — budget a dedicated cleanup pass rather than doing it ad hoc across unrelated PRs |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|--------------------|----------------|
| Double-booking (check-then-insert race) | Week 2 (Scheduling) | Concurrent load test against one slot yields exactly one confirmed appointment; DB has exclusion constraint present in migration |
| Temporal workflow non-determinism | Week 2 (Scheduling), re-verify Week 3 | Replay test suite runs existing event histories against current workflow code in CI; no I/O/randomness/wall-clock calls found in workflow code via code review checklist |
| Non-idempotent activities/consumers/tasks | Week 2 (activities) + Week 3 (Kafka/Celery) | Manually replay same message/activity input twice; confirm no duplicate DB row, notification, or counter increment |
| Dual-write inconsistency (Postgres/Kafka/Mongo) | Week 3 (Event-Driven System), schema designed Week 1-2 | Kill process between DB commit and event publish; confirm outbox/relay still delivers the event; reconciliation job shows zero drift |
| Kafka ordering/rebalancing | Week 3 (Event-Driven System) | Multi-partition, multi-consumer load test confirms same-entity events process in order; rebalance during load doesn't cause processing errors |
| Celery/RabbitMQ ack/retry misconfiguration | Week 3 (Event-Driven System) | Kill a worker mid-task; confirm task is redelivered (not lost) and redelivery doesn't cause a duplicate side effect |
| Redis lock over-reliance | Week 2 (Scheduling) | Code review confirms Postgres constraint is the enforced invariant; Redis lock failure/expiry doesn't allow a double-write to succeed |
| Missing trace context propagation | Week 1 (Foundation), re-verify Week 3 | One "five-hop" smoke test trace (HTTP → Temporal → activity → Kafka → Celery) appears as a single connected trace in Jaeger |
| Saga compensation modeled as rollback | Week 2 (Scheduling) | Audit trail contains entries for a deliberately-failed/compensated booking attempt; no hard deletes in compensation code paths |

## Sources

- [Temporal — Spooky Stories: Chilling Temporal anti-patterns (part 1)](https://temporal.io/blog/spooky-stories-chilling-temporal-anti-patterns-part-1)
- [Temporal Docs — Side Effects (Go/Java SDK)](https://docs.temporal.io/develop/go/workflows/side-effects)
- [Temporal Docs — Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments)
- [Temporal Docs — Versioning (Go SDK)](https://docs.temporal.io/develop/go/workflows/versioning)
- [Understanding Non-Determinism in Temporal.io (Medium)](https://medium.com/@sanhdoan/understanding-non-determinism-in-temporal-io-why-it-matters-how-to-avoid-it-3d397d8a5793)
- [Temporal Replay Bugs Hide in Plain Sight (Medium)](https://medium.com/@Nexumo_/temporal-replay-bugs-hide-in-plain-sight-7e436f4b3ea4)
- [Temporal — Mastering Saga Patterns for Distributed Transactions in Microservices](https://temporal.io/blog/mastering-saga-patterns-for-distributed-transactions-in-microservices)
- [Confluent — Understanding the Dual-Write Problem and Its Solutions](https://www.confluent.io/blog/dual-write-problem/)
- [Conduktor — Transactional Outbox: Database-Kafka Consistency](https://www.conduktor.io/blog/transactional-outbox-pattern-database-kafka)
- [Exactly-Once Processing Across Kafka and Databases: Using the Outbox Pattern (Medium)](https://medium.com/threadsafe/exactly-once-processing-across-kafka-and-databases-using-the-outbox-pattern-f08fd640f683)
- [Confluent — Exactly-once Semantics is Possible: Here's How Apache Kafka Does It](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Idempotent Processing with Kafka (Nejc Korasa)](https://nejckorasa.github.io/posts/idempotent-kafka-procesing/)
- [How to Guarantee Message Order in Kafka (OneUptime)](https://oneuptime.com/blog/post/2026-01-26-kafka-message-ordering/view)
- [Confluent — Cooperative Rebalancing in the Kafka Consumer, Streams & ksqlDB](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/)
- [Redpanda — Kafka Rebalancing: Triggers, Effects, and Mitigation](https://www.redpanda.com/guides/kafka-performance-kafka-rebalancing)
- [Celery Docs — Frequently Asked Questions (acks_late)](https://docs.celeryq.dev/en/stable/faq.html)
- [Celery Task Resilience: Advanced Strategies for Reliable Workflows (GitGuardian)](https://blog.gitguardian.com/celery-tasks-retries-errors/)
- [Celery gotchas (Danny's blog)](https://blog.danwald.me/celery-gotchas)
- [How to Solve the Double Booking Problem with PostgreSQL](https://jsupskills.dev/how-to-solve-the-double-booking-problem/)
- [PostgreSQL's GiST Exclusion Constraint: The Database-Level Answer to Double Bookings](https://amitavroy.com/articles/postgresql-gist-exclusion-constraintthe-database-evel-answer-to-double-bookings)
- [Stormatics — SELECT FOR UPDATE in PostgreSQL](https://stormatics.tech/blogs/select-for-update-in-postgresql)
- [Redis Distributed Locking — Commands, Patterns, and Production Pitfalls (Medium)](https://codefarm0.medium.com/redis-distributed-locking-commands-patterns-and-production-pitfalls-397e12fe441b)
- [Redis Docs — Distributed Locks with Redis](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)
- [The Redlock Algorithm - Redis Patterns (antirez)](https://redis.antirez.com/fundamental/redlock.html)
- [How to Propagate OpenTelemetry Trace Context Through Celery Message Headers (OneUptime)](https://oneuptime.com/blog/post/2026-02-06-propagate-opentelemetry-trace-context-celery-headers/view)
- [OpenTelemetry Celery Instrumentation Guide (Uptrace)](https://uptrace.dev/guides/opentelemetry-celery)
- [OpenTelemetry Context Propagation Explained (W3C Trace Context Guide)](https://openobserve.ai/blog/opentelemetry-context-propagation/)
- [SigNoz — Jaeger vs Prometheus 2026: Key Differences](https://signoz.io/blog/jaeger-vs-prometheus/)
- [Saga Pattern: Ensuring Data Consistency in Microservices (Medium)](https://medium.com/@dperez_/saga-pattern-ensuring-data-consistency-in-microservices-d01ca9c15ada)
- [Database per Service: Patterns, Challenges, and Implementation Strategies (dasroot.net)](https://dasroot.net/posts/2026/03/database-per-service-patterns-challenges-implementation/)

---
*Pitfalls research for: Distributed event-driven healthcare-operations backend (SmartHealth Part A)*
*Researched: 2026-09-09*
