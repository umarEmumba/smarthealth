# Architecture Research

**Domain:** Distributed, event-driven healthcare-operations backend (appointment scheduling, visit lifecycle, notifications, analytics)
**Researched:** 2026-09-09
**Confidence:** MEDIUM

> Confidence note: Vendor-authoritative sources (Temporal's own docs/blog, Debezium's own blog, Stripe's own idempotency writeup) corroborate the community sources found. No Context7/official-docs MCP was available in this session, so everything below is sourced via web search and cross-checked across 3+ independent sources per claim rather than a single vendor doc lookup — hence MEDIUM, not HIGH, but every load-bearing claim (Temporal saga mechanics, outbox pattern, idempotency keys, Kafka Schema Registry compatibility modes) is triangulated. Treat this as ready-to-build guidance; verify Temporal SDK API specifics against `docs.temporal.io` during Week 2 implementation.

## Standard Architecture

### System Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          CLIENT LAYER                                     │
│   Patient app · Front-desk console · Provider console · Admin console     │
└───────────────────────────────┬───────────────────────────────────────────┘
                                 │ HTTPS/REST (+ idempotency-key header)
┌───────────────────────────────▼───────────────────────────────────────────┐
│                       API LAYER (FastAPI services)                        │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐              │
│  │ Identity & │ │ Scheduling │ │   Visit    │ │ Analytics  │              │
│  │ Directory  │ │    API     │ │   API      │ │   API      │              │
│  │ (patients, │ │ (booking,  │ │ (check-in, │ │ (read-only │              │
│  │ providers, │ │ cancel,    │ │ in-progress│ │ metrics)   │              │
│  │ depts,     │ │ reschedule)│ │ complete)  │ │            │              │
│  │ auth/RBAC) │ │            │ │            │ │            │              │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘              │
└────────┼──────────────┼──────────────┼──────────────┼─────────────────────┘
         │              │ starts/signals│              │ reads
         │              ▼              │              │
         │    ┌──────────────────────────────┐         │
         │    │   TEMPORAL (orchestration)    │         │
         │    │  BookingWorkflow              │         │
         │    │  CancelRescheduleWorkflow     │         │
         │    │  VisitLifecycleWorkflow       │         │
         │    │  (durable state, retries,     │         │
         │    │   saga compensation, timers)  │         │
         │    └───────┬───────────────┬───────┘         │
         │      activities invoke      │ emits outbox rows
         │            │                │                │
┌────────▼────────────▼────────────────▼────────────────▼──────────────────┐
│                        DATA + COORDINATION LAYER                         │
│ ┌──────────┐ ┌───────────┐ ┌──────────┐  ┌─────────────────────────────┐ │
│ │PostgreSQL│ │ MongoDB   │ │  Redis   │  │ Outbox table (in Postgres)  │ │
│ │(system of│ │(documents:│ │(cache +  │  │ → Debezium CDC → Kafka      │ │
│ │ record:  │ │ audit log,│ │ locks +  │  │                             │ │
│ │ patients,│ │ visit/    │ │ rate     │  └──────────────┬──────────────┘ │
│ │providers,│ │ service   │ │ limit +  │                 │                │
│ │appts,    │ │ records,  │ │ idempot- │                 ▼                │
│ │billing   │ │ notif.    │ │ ency     │  ┌─────────────────────────────┐ │
│ │pre-check)│ │ logs)     │ │ cache)   │  │  KAFKA + SCHEMA REGISTRY    │ │
│ └──────────┘ └───────────┘ └──────────┘  │  topics: appointment.*,     │ │
│                                            │  visit.*, billing.*,        │ │
│                                            │  provider.schedule.*        │ │
│                                            └──────────────┬──────────────┘ │
└───────────────────────────────────────────────────────────┼───────────────┘
                                                              │ consumed by
┌─────────────────────────────────────────────────────────────▼─────────────┐
│                    ASYNC WORKER LAYER                                     │
│  ┌───────────────────────────┐   ┌────────────────────────────────────┐  │
│  │ Celery workers + RabbitMQ │   │ Kafka consumer groups (analytics,  │  │
│  │ (notifications, reminders,│   │  audit-log writer, cross-service   │  │
│  │  follow-ups — discrete,   │   │  projections into MongoDB)         │  │
│  │  short-lived jobs, often  │   │                                    │  │
│  │  triggered BY a Kafka     │   │                                    │  │
│  │  consumer or a Temporal   │   │                                    │  │
│  │  activity)                │   │                                    │  │
│  └───────────────────────────┘   └────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────┐
│         OBSERVABILITY (cross-cutting, every layer instrumented)          │
│   OpenTelemetry SDK (traces+metrics+logs) → Jaeger (traces) ·             │
│   Prometheus (metrics) → Grafana (dashboards)                             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|-------------------------|
| Identity & Directory service | Patients, providers, staff, departments, clinics, RBAC, audit trail of profile changes | FastAPI + PostgreSQL; writes audit entries to MongoDB |
| Scheduling API | Thin HTTP layer over Temporal: accepts booking/cancel/reschedule requests, validates idempotency key, starts/signals workflows, returns workflow status | FastAPI; talks to Temporal client SDK, not to DB directly for scheduling state |
| Temporal (workflow engine) | Owns the multi-step business process: validate → reserve slot → prevent conflicts → billing pre-check → schedule notifications → confirm; owns saga compensation on failure | Temporal Server + Python SDK workers; workflow history is the durable source of truth for "where is this booking in its lifecycle" |
| Visit API + VisitLifecycleWorkflow | Check-in → in-progress → complete state machine; idempotent transitions; triggers billing + follow-up + analytics on completion | FastAPI + Temporal workflow (shorter-lived than booking workflow, still benefits from durable retries) |
| PostgreSQL | System of record for anything requiring strong consistency and relational integrity: patients, providers, appointments, provider schedules/slots, billing pre-check state, outbox table | Managed via migrations; row-level locking / `SELECT ... FOR UPDATE` for slot reservation inside a transaction |
| MongoDB | Operational/document data: audit trail entries, visit/service records (semi-structured, evolving fields), notification delivery logs, analytics event projections | Schema-flexible collections; not the source of truth for anything Temporal needs to make a scheduling decision |
| Redis | Hot-path cache (provider availability snapshots) + short-lived coordination (distributed lock/lease during slot reservation race, idempotency-key response cache, rate limiting on booking endpoint) | Redis with TTL keys; Lua scripts or `SET NX PX` for atomic check-and-set; not a durable store |
| Kafka + Schema Registry | Durable, replayable, ordered-per-key event backbone for cross-service facts that already happened: appointment.booked, appointment.cancelled, visit.completed, billing.status_changed, provider.schedule_changed | Avro/Protobuf schemas registered centrally, FULL compatibility mode; outbox-table + CDC (Debezium) as the producer path from Postgres |
| Celery + RabbitMQ | Discrete, short-lived background jobs that don't need multi-step durable orchestration: send this reminder, send this notification, run this single follow-up job | Celery workers behind RabbitMQ; tasks triggered either by a Temporal activity (schedule-a-reminder) or by a Kafka consumer (react-to-event) |
| OpenTelemetry / Jaeger / Prometheus / Grafana | Cross-cutting tracing, metrics, dashboards for every layer above | OTel SDK auto-instrumentation in FastAPI + Temporal workers + Celery workers; trace context propagated through Kafka headers and Temporal workflow metadata |

## Recommended Project Structure

```
smarthealth/
├── services/
│   ├── identity/              # Patient/provider/staff/dept/clinic + RBAC + audit
│   │   ├── api/                # FastAPI routers
│   │   ├── domain/              # Pydantic models, business rules
│   │   ├── repository/          # Postgres + Mongo access
│   │   └── tests/
│   ├── scheduling/             # Booking/cancel/reschedule HTTP surface
│   │   ├── api/
│   │   ├── workflows/           # Temporal workflow definitions (BookingWorkflow, CancelRescheduleWorkflow)
│   │   ├── activities/          # Temporal activities (reserve_slot, billing_precheck, schedule_notification, publish_outbox_event)
│   │   ├── worker.py            # Temporal worker entrypoint
│   │   └── tests/
│   ├── visits/                 # Visit/service lifecycle
│   │   ├── api/
│   │   ├── workflows/           # VisitLifecycleWorkflow
│   │   ├── activities/
│   │   └── tests/
│   ├── notifications/          # Celery tasks (reminders, follow-ups, delivery)
│   │   ├── tasks/
│   │   ├── consumers/           # Kafka consumers that enqueue Celery tasks
│   │   └── tests/
│   └── analytics/              # Read-side metrics API + Kafka consumer projections into Mongo
│       ├── api/
│       ├── consumers/
│       └── tests/
├── libs/
│   ├── events/                  # Shared Avro/Protobuf schemas + Schema Registry client wrapper
│   ├── idempotency/              # Shared idempotency-key middleware (Redis-backed)
│   ├── outbox/                   # Shared outbox-table writer helper (used inside DB transactions)
│   └── observability/            # Shared OTel setup, trace-context propagation helpers
├── infra/
│   ├── docker-compose.yml        # Postgres, Mongo, Redis, Kafka+SR, RabbitMQ, Temporal, Jaeger, Prometheus, Grafana
│   ├── migrations/                # Alembic migrations (Postgres)
│   └── dashboards/                # Grafana JSON, self-contained architecture-decision HTML dashboard
└── docs/
    ├── architecture-diagram.*
    └── decisions/                 # ADRs — one per Key Decision in PROJECT.md
```

### Structure Rationale

- **services/ split by bounded context, not by tech layer:** each service owns its API + workflows + activities + repository together, so "scheduling" is a vertical slice, not scattered across a global `controllers/` + `models/` split. This matches the domain boundaries the requirements describe (patient/provider mgmt, scheduling, visit lifecycle, notifications, analytics are distinct bounded contexts with distinct data ownership).
- **libs/events as a shared contract layer:** Kafka schemas must be shared/versioned centrally so producers and consumers don't drift — this is exactly what Schema Registry is for, and centralizing the client wrapper avoids every service reinventing Avro (de)serialization.
- **libs/idempotency and libs/outbox as shared cross-cutting concerns:** idempotency-key handling and outbox-table writes are needed by every write-path service (scheduling, visits, billing), so they belong in a shared library rather than duplicated per service.
- **worker.py per service, not one global worker:** Temporal workers are typically deployed per task-queue/domain so scheduling workflow failures don't compete for worker capacity with visit-lifecycle workflows.

## Architectural Patterns

### Pattern 1: Temporal Saga (Orchestration) for Multi-Step Scheduling

**What:** BookingWorkflow is a single Temporal workflow function that calls activities in sequence (validate patient/eligibility → reserve provider slot → billing pre-check → schedule notification) and accumulates a compensation for each successful step. If any activity fails after retries are exhausted, the workflow runs accumulated compensations in reverse order (release slot, reverse billing pre-check, cancel scheduled notification) and marks the appointment as failed/rejected rather than confirmed.
**When to use:** Any multi-step business process where partial completion must never be visible as "done" — this is explicitly the core reliability requirement of Part A ("partial failures must not corrupt scheduling state").
**Trade-offs:** Temporal gives you durable execution, automatic retry/backoff, and full history/replay for debugging almost for free, but every activity call must itself be idempotent (Temporal guarantees the workflow logic runs exactly once from the workflow's perspective, but an activity can still be retried and re-executed against external systems). Requires running a Temporal server + workers, which is added infra to operate but is mandated by the assignment.

**Example:**
```python
@workflow.defn
class BookingWorkflow:
    @workflow.run
    async def run(self, request: BookingRequest) -> BookingResult:
        compensations: list[Callable[[], Awaitable[None]]] = []
        try:
            await workflow.execute_activity(validate_patient, request, ...)

            slot = await workflow.execute_activity(reserve_slot, request, ...)
            compensations.append(lambda: workflow.execute_activity(release_slot, slot, ...))

            billing = await workflow.execute_activity(billing_precheck, request, ...)
            compensations.append(lambda: workflow.execute_activity(reverse_billing_precheck, billing, ...))

            await workflow.execute_activity(schedule_notification, request, ...)
            await workflow.execute_activity(publish_outbox_event, "appointment.confirmed", request, ...)
            return BookingResult(status="CONFIRMED", slot=slot)
        except ActivityError:
            for compensate in reversed(compensations):
                await compensate()
            await workflow.execute_activity(publish_outbox_event, "appointment.rejected", request, ...)
            return BookingResult(status="REJECTED")
```

### Pattern 2: Transactional Outbox for Postgres → Kafka

**What:** Instead of the Scheduling API or a Temporal activity writing to Postgres AND publishing to Kafka as two separate operations (the classic dual-write bug), the activity writes the business row (e.g., appointment status change) and an event row into an `outbox` table in the *same* Postgres transaction. A CDC connector (Debezium) tails the Postgres WAL, reads new outbox rows, and publishes them to the correct Kafka topic — decoupled from application-level publish calls entirely.
**When to use:** Every place where a Postgres state change must reliably become a Kafka event (appointment confirmed/cancelled, visit completed, billing status changed). This is the correct default for this project given Postgres is already the system of record.
**Trade-offs:** Removes the dual-write failure mode completely and requires no distributed transaction/2PC. Costs: operational complexity of running Debezium + Kafka Connect, need to enable logical replication on Postgres, and delivery is at-least-once (so Kafka consumers must be idempotent, e.g., dedupe on event ID). For a 3-week assignment, a simpler variant (application-level "write outbox row in same DB transaction, then a lightweight polling publisher process reads and publishes, marking rows sent") is an acceptable, easier-to-explain substitute if Debezium setup time is a concern — document this as an explicit, defensible tradeoff for the mentor review either way.

**Example:**
```python
async def reserve_slot_activity(request: BookingRequest) -> Slot:
    async with postgres.transaction() as tx:
        slot = await tx.execute(reserve_slot_sql, request.provider_id, request.time)
        await tx.execute(
            insert_outbox_sql,
            aggregate_type="appointment",
            aggregate_id=request.appointment_id,
            event_type="appointment.slot_reserved",
            payload=slot.to_avro(),
        )
    return slot
```

### Pattern 3: Idempotency Key + Redis-Backed Dedup on Booking Endpoints

**What:** Every state-changing endpoint that can be retried by a client or by Temporal (book, cancel, reschedule, check-in, complete-visit) accepts an `Idempotency-Key` header. The API stores `(key → in-flight | result)` in Redis with a TTL (e.g., 24h). A second request with the same key while the first is in flight either blocks briefly or returns a 409/425; after completion it returns the cached result verbatim instead of re-executing.
**When to use:** All patient/front-desk-facing write endpoints, and internally at the Temporal-activity level for any activity calling an external or non-Temporal-managed system (this is separate from, and in addition to, Temporal's own workflow-level exactly-once guarantees).
**Trade-offs:** Cheap to add (Redis SET NX + TTL) and directly satisfies the "no duplicate processing" requirement. Not literally exactly-once — it provides at-most-once *effects* per key; if the server crashes after executing but before caching the response, a retry could still double-execute unless the underlying operation itself is idempotent (e.g., unique constraint on `(patient_id, slot_id)` in Postgres as the last line of defense).

## Data Flow

### Booking Flow (happy path)

```
Patient/Front-desk → POST /appointments (Idempotency-Key: X)
    ↓
Scheduling API: check Redis for key X → miss → start BookingWorkflow(request)
    ↓
Temporal BookingWorkflow:
    validate_patient (Postgres read)
      ↓
    reserve_slot (Postgres tx: UPDATE slot WHERE available; + outbox row "slot_reserved")
      ↓ (Debezium tails WAL) → Kafka topic appointment.events
      ↓
    billing_precheck (Postgres read/write billing_precheck table)
      ↓
    schedule_notification (Temporal activity enqueues Celery task: send_confirmation_reminder)
      ↓
    mark appointment CONFIRMED (Postgres tx + outbox row "appointment.confirmed")
      ↓ → Kafka → [Analytics consumer updates Mongo projection] [Audit consumer writes Mongo audit log]
    ↓
Scheduling API returns 201 + result; Redis caches (key X → result)
```

### Cancel / Reschedule Flow

```
Patient/Front-desk → POST /appointments/{id}/cancel (Idempotency-Key)
    ↓
Scheduling API signals or starts CancelRescheduleWorkflow
    ↓
Temporal workflow:
    release_slot (Postgres tx + outbox "slot_released")
      ↓
    move_waitlist (Postgres: promote next waitlisted patient; triggers a nested/child booking attempt or notification)
      ↓
    reverse_billing (Postgres billing_precheck update + outbox "billing.reversed")
      ↓
    notify_patient (Celery task via activity)
      ↓
    outbox "appointment.cancelled" → Kafka → Analytics/Audit consumers
```

### Visit Lifecycle Flow

```
Front-desk → POST /visits/{appointment_id}/check-in (Idempotency-Key)
    ↓
VisitLifecycleWorkflow: check_in (Mongo write: visit record; Postgres: appointment status → IN_PROGRESS)
    ↓ outbox "visit.checked_in" → Kafka
[... in-progress updates, idempotent by visit_id + status transition guard ...]
    ↓
complete_visit (Mongo: finalize visit record; Postgres: appointment status → COMPLETED)
    ↓ outbox "visit.completed" → Kafka
    ↓
    → Celery task: schedule_followup_reminder
    → Analytics consumer: increment completed_visits, compute wait_time (check-in ts vs booked ts)
    → Billing consumer/activity: trigger billing workflow if applicable
```

### Notifications Flow

```
Kafka event (appointment.confirmed | appointment.cancelled | visit.completed | provider.schedule_changed)
    ↓
notifications-service Kafka consumer (consumer group, offset-committed only after successful enqueue)
    ↓
enqueue Celery task (send_notification) with dedup key = event_id
    ↓
Celery worker (RabbitMQ-backed): sends email/SMS/push, writes delivery log to MongoDB
    ↓
retries on transient failure (Celery retry/backoff); dead-letter queue on exhaustion
```

### Analytics Flow

```
Kafka topics (appointment.*, visit.*, billing.*) 
    ↓
analytics-service consumer group (separate from notifications' consumer group — independent offsets)
    ↓
projections written to MongoDB (denormalized read models: appointments_over_time, cancellation_rate, avg_wait_time, completed_visits, total_patients)
    ↓
Analytics API serves read-only queries directly from MongoDB projections (never recomputes from Postgres live — keeps analytics reads off the hot transactional path)
```

## Temporal vs Kafka vs Celery — Responsibility Split (key evaluation question)

This is the single most scrutinized design decision in this project. Get the mental model right:

| Question | Answer |
|----------|--------|
| "Is this a multi-step business process with an order, retries, and a need to compensate on partial failure?" | **Temporal.** Booking (validate→reserve→billing precheck→notify→confirm), cancel/reschedule (release→waitlist→refund→notify), visit lifecycle (check-in→in-progress→complete) are all Temporal workflows. |
| "Did something already happen elsewhere that other services need to know about, replay, or audit?" | **Kafka.** appointment.booked/cancelled/rescheduled, visit.completed, billing.status_changed, provider.schedule_changed are facts published once and consumed by N independent services (analytics, audit, notifications) at their own pace. Kafka does not know or care whether the consumer "succeeded" — that's the consumer's job. |
| "Is this a single, short-lived, fire-and-forget unit of work with no multi-step dependency?" | **Celery + RabbitMQ.** Sending one email, sending one SMS reminder, running one follow-up job. Celery has no concept of "workflow state" — every task is independent. |
| "Should a Temporal activity ever BE a Celery task, or vice versa?" | Yes for the "trigger" direction: a Temporal activity or a Kafka consumer can *enqueue* a Celery task as the actual unit of work (e.g., `schedule_notification` activity → enqueues `send_reminder` Celery task). Don't have Celery orchestrate multi-step business logic, and don't have Temporal directly do low-level fire-and-forget I/O that doesn't need durability guarantees (that's wasted overhead). |
| "Can Kafka replace Temporal?" | No — Kafka is stateless; it has no concept of "this workflow is 2 of 5 steps in and needs to retry step 3 in 30 seconds with backoff, then compensate step 1 and 2 if step 3 permanently fails." That is precisely the gap Temporal fills. Treating them as interchangeable is the most common architecture mistake in this space. |
| "Can Celery replace Temporal?" | No, not safely for this project's core requirement. Celery gives you retries per-task but no durable cross-task state machine, no built-in saga/compensation, no exactly-once-per-workflow semantics, no timers-that-survive-restarts. Attempting to hand-roll a state machine across multiple Celery tasks reinvents (poorly) what Temporal already solves — this is the most likely source of "corrupted scheduling state on partial failure" if chosen instead of Temporal. |

**Practical wiring for this project:**
- **Temporal workflows** own: BookingWorkflow, CancelRescheduleWorkflow, VisitLifecycleWorkflow.
- **Temporal activities** call: Postgres writes (within transactions, writing outbox rows), Redis lock acquisition, and *enqueue* Celery tasks for notification delivery (activity itself returns once the task is enqueued — it does not block on email delivery).
- **Kafka** carries: cross-service facts derived from outbox rows (via Debezium or a lightweight outbox poller), consumed independently by analytics-service and notifications-service and any future audit/reporting consumers.
- **Celery + RabbitMQ** executes: the actual notification/reminder/follow-up send jobs, either triggered by a Temporal activity (synchronous scheduling context) or by a Kafka consumer (asynchronous reactive context, e.g., "provider.schedule_changed" triggers a batch of affected-patient notifications).

## Consistency, Idempotency, and Failure-Handling Patterns

### Idempotency (three layers, defense in depth)
1. **API layer:** `Idempotency-Key` header + Redis-backed dedup cache on all booking/cancel/check-in/complete endpoints (Pattern 3 above).
2. **Temporal layer:** Workflow IDs derived deterministically from business keys (e.g., `booking-{appointment_id}`) so starting "the same" workflow twice is a no-op/attaches to the existing execution; activities use deterministic idempotency keys (e.g., `activity:{workflow_id}:{activity_name}:{attempt}` or better, a business key like `slot_id`) so retried activities don't double-reserve.
3. **Database layer (last line of defense):** unique constraints (e.g., unique index on `(provider_id, slot_start_time)` where status != CANCELLED) so even a bug in the above layers cannot physically create a double-booked slot.

### Transactional Outbox (Postgres → Kafka)
Every Postgres write that must produce a Kafka event writes to an `outbox` table in the same transaction (Pattern 2). Use either Debezium CDC (production-grade, more setup) or a simple polling publisher (`SELECT * FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT N FOR UPDATE SKIP LOCKED`, publish, mark published) as a pragmatic 3-week-timeline substitute — document the tradeoff explicitly in the architecture decision dashboard.

### Saga / Compensation
Implemented entirely inside Temporal workflows (Pattern 1). Do not implement saga/compensation logic via Kafka choreography for this project — orchestration is simpler to reason about, test, and demo to a mentor than choreography's distributed event-listening logic, and Temporal is mandated specifically to provide this.

### Exactly-Once — What You Actually Get
True exactly-once delivery across independent systems is not achievable; the honest target is **effectively-once processing** via idempotent consumers + at-least-once delivery everywhere:
- Kafka: at-least-once delivery by default (consumer commits offset after successful processing, not before); consumers dedupe on event ID.
- Celery: at-least-once execution (acks_late); tasks must be idempotent (celery-singleton pattern or DB-level unique constraint/dedup table).
- Temporal: workflow *logic* executes exactly-once from the workflow's perspective (history-based replay), but individual *activities* can still retry and must be idempotent themselves.
- Outbox/CDC: at-least-once publish; downstream consumers dedupe.

### Redis for Coordination (not durability)
Redis holds: (a) idempotency-key cache with TTL, (b) short-lived distributed locks / atomic check-and-set for slot-reservation races (belt-and-suspenders alongside the Postgres unique constraint — Redis lock reduces contention/wasted work, Postgres constraint guarantees correctness even if the lock fails), (c) hot-read cache of provider availability to reduce Postgres load during peak booking windows, (d) rate limiting on booking endpoints. Redis is never the source of truth for booking state — a Redis outage should degrade performance, not correctness (Postgres constraints still hold).

## PostgreSQL vs MongoDB — Data Ownership Split

| Data | Owner | Why |
|------|-------|-----|
| Patients, providers, staff, users, roles | PostgreSQL | Relational integrity (foreign keys to appointments/departments), strong consistency for identity/authz |
| Departments, clinics | PostgreSQL | Relational, low-churn reference data |
| Appointments (booking state, status, provider/slot FK) | PostgreSQL | This is the entity requiring the strongest consistency guarantee in the whole system — the core reliability requirement hinges on it; needs transactions + unique constraints |
| Provider schedules / slots | PostgreSQL | Conflict prevention requires transactional row-locking (`SELECT ... FOR UPDATE`) — this is a relational concurrency problem, not a document problem |
| Billing pre-check / status | PostgreSQL | Financial-adjacent state; wants ACID + auditability + joins to appointments |
| Outbox table | PostgreSQL | Must live in the same transactional boundary as the state changes it announces |
| Audit trail (profile & operational changes) | MongoDB | High write volume, append-mostly, schema naturally varies by what changed (patient field vs provider field vs appointment field) — a flexible document shape avoids constant migrations |
| Visit / service records | MongoDB | Semi-structured, evolves per visit type/service, not relationally joined in ways that need SQL joins; queried mostly by visit_id/patient_id, not by complex cross-entity joins |
| Notification delivery logs | MongoDB | High volume, write-heavy, short/medium retention, simple key-based lookups |
| Analytics projections (read models) | MongoDB | Denormalized, rebuilt from Kafka events, optimized for read patterns (time-series-ish aggregates), not for transactional writes |

**Rule of thumb applied:** if a mentor would ask "what happens if this write fails halfway," and the answer must be "the whole thing rolls back and nothing is half-true" — it's Postgres. If the answer is "it's fine, we just have a slightly incomplete document we can patch later" — it's Mongo. Never let MongoDB own anything Temporal's saga logic depends on for a compensation decision.

## Scaling Considerations

| Concern | At mentor-review scale (local Docker Compose, low load) | At tens of thousands of requests (assignment's stated target) | Beyond (production-grade, out of scope for Part A but worth naming) |
|---------|------|------|------|
| Slot reservation contention | Single Postgres instance, row-level locks sufficient | Add Redis-based pre-check/lock to shed contention before hitting Postgres; index `(provider_id, slot_start_time)` | Partition by provider/clinic; read replicas for availability reads |
| Kafka throughput | Single-broker Kafka (Docker Compose default) fine for demo | Increase partitions per topic keyed by `appointment_id`/`provider_id` for ordering + parallelism | Multi-broker cluster, tiered storage |
| Temporal worker capacity | Single worker process per task queue | Scale worker replicas horizontally (stateless workers, state lives in Temporal server) | Multiple Temporal namespaces per tenant/clinic |
| Celery throughput | Single worker, single queue | Multiple queues by priority (reminders vs urgent notifications), scale worker count | Autoscaled worker pools, priority queues per notification channel |
| Observability overhead | OTel console/basic exporters | Full Jaeger + Prometheus + Grafana stack, sampling at 100% (low volume) | Trace sampling strategy, metric cardinality control |

### Scaling Priorities
1. **First bottleneck:** Postgres row contention on slot reservation during peak booking windows — mitigate with Redis pre-check locks + tight transaction scope (reserve, don't do unrelated work inside the same DB transaction).
2. **Second bottleneck:** Kafka consumer lag on analytics/notification consumers if a single partition is over-subscribed — mitigate with partition count aligned to expected parallelism and consumer-group scaling.

## Anti-Patterns

### Anti-Pattern 1: Using Kafka as a Task Queue / Using Celery as a Workflow Engine

**What people do:** Publish "please do X" command messages to Kafka and treat consumer processing as the workflow engine, or chain multiple Celery tasks together with manual state stored in Redis to fake a saga.
**Why it's wrong:** Kafka has no per-message retry/backoff/timeout semantics tailored to a single logical operation, and hand-rolled Celery chains have no durable history, no built-in compensation, and are extremely hard to debug when they partially fail — exactly the "corrupted scheduling state" failure mode this project must avoid.
**Instead:** Multi-step orchestration → Temporal. Facts that already happened → Kafka. Single discrete jobs → Celery.

### Anti-Pattern 2: Dual Writes (App Writes to Postgres AND Publishes to Kafka as Two Separate Calls)

**What people do:** `db.commit(); kafka.publish(event)` as two sequential, non-atomic operations.
**Why it's wrong:** If the process crashes between the two calls, or the Kafka publish fails after the DB commit succeeds, the event is silently lost — but the state changed. Downstream consumers (analytics, notifications) never find out. This is a direct path to "inconsistent operational data" that the requirements explicitly call out as a current pain point.
**Instead:** Transactional outbox pattern (Pattern 2) — write the event in the same DB transaction as the state change, publish asynchronously via CDC or a poller.

### Anti-Pattern 3: Letting MongoDB Own Booking/Slot State

**What people do:** Store appointment or slot-availability documents in MongoDB "because it's flexible" and try to prevent double-booking with application-level checks.
**Why it's wrong:** MongoDB's default consistency model and lack of easy multi-document transactional row-locking make it a poor fit for the exact invariant this project is graded on (“partial failures must not corrupt scheduling state”); you'd be reimplementing what Postgres gives you for free (unique constraints + `SELECT FOR UPDATE`).
**Instead:** Postgres owns appointment/slot state; MongoDB owns everything that doesn't need that invariant (audit logs, visit records, notification logs).

### Anti-Pattern 4: Treating Idempotency Keys as Sufficient On Their Own

**What people do:** Add an idempotency-key cache in Redis and consider "duplicate prevention" solved.
**Why it's wrong:** Redis cache can expire, be evicted, or simply miss a race window between two near-simultaneous requests before the first has written its "in-flight" marker; relying on Redis alone leaves a real (if narrow) double-booking window.
**Instead:** Defense in depth — idempotency key cache (fast path) + Postgres unique constraint (correctness backstop) + Temporal deterministic workflow ID (workflow-level dedup).

## Integration Points

### External Services (mocked/simulated for Part A per Out of Scope)

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Billing / payment gateway (simulated) | Temporal activity calls an internal "billing pre-check" service/stub | Real payment processors explicitly out of scope; model status transitions only |
| Notification channels (email/SMS/push, likely simulated or using a real provider like a transactional email API) | Celery task performs the actual send | Wrap the provider call in a Celery task with retry/backoff; log delivery result to MongoDB |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Scheduling API ↔ Temporal | Temporal client SDK (start workflow / signal / query) | API layer never talks to Postgres directly for appointment status — always through the workflow, so Temporal's history is the single source of truth for in-flight booking state |
| Temporal activity ↔ Postgres | Direct DB connection within activity, transactional | Activity writes business row + outbox row in one transaction |
| Temporal activity ↔ Celery | Activity enqueues a Celery task and returns (does not block on task completion) for notification-type side effects | Keeps notification latency off the critical workflow path |
| Postgres outbox ↔ Kafka | Debezium CDC (or polling publisher) | Decoupled — Postgres has no direct dependency on Kafka being up to commit its own transactions |
| Kafka ↔ analytics-service / notifications-service | Independent consumer groups per service | Each service's consumer lag/failure is isolated from the others |
| All services ↔ Redis | Direct client connection, used only for cache/lock/rate-limit, never durable state | Redis outage degrades performance/latency, must not corrupt correctness |
| All services ↔ Observability stack | OTel SDK auto/manual instrumentation, trace context propagated via Kafka message headers and Temporal workflow/activity metadata | Enables tracing a single booking request end-to-end through API → Temporal → Postgres → Kafka → Celery |

## Suggested Build Order (aligned to the 3-week plan)

**Week 1 — Foundation + Core Services** (matches Execution Guidelines Week 1 scope):
1. Docker Compose skeleton: Postgres, MongoDB, Redis first (cheapest to stand up, unblocks everything else). Add Kafka+Schema Registry, RabbitMQ, Temporal server, and the observability stack (Jaeger, Prometheus, Grafana) to the same compose file even if not wired into code yet — de-risk infra setup early rather than in Week 3.
2. Identity & Directory service: patients, providers, staff, roles, departments, clinics, RBAC, audit-trail writes to MongoDB. This has no Temporal/Kafka dependency and validates the Postgres/MongoDB split early.
3. Appointment schema design in Postgres (including the outbox table and the unique constraint on provider/slot) — designed now even though Temporal wiring comes in Week 2.

**Week 2 — Scheduling + Service Workflow** (matches Week 2 scope):
4. Temporal integration: BookingWorkflow with saga/compensation (Pattern 1), backed by activities hitting Postgres.
5. Slot reservation logic with Redis lock as a fast-path guard in front of the Postgres transactional reservation.
6. VisitLifecycleWorkflow (check-in → in-progress → complete) with idempotent transitions.
7. CancelRescheduleWorkflow (release slot → waitlist → billing reversal → notify).

**Week 3 — Event-Driven System + Observability** (matches Week 3 scope):
8. Outbox → Kafka wiring (Debezium or polling publisher) for appointment/visit/billing events; register Avro schemas in Schema Registry.
9. Celery + RabbitMQ notification workers, triggered both from Temporal activities (booking confirmations) and Kafka consumers (reactive notifications like provider schedule changes).
10. Analytics consumer service projecting Kafka events into MongoDB read models; Analytics API serving the five required metrics.
11. OpenTelemetry instrumentation across all services, Jaeger trace verification end-to-end, Prometheus scraping, Grafana dashboards for the critical flows (booking, visit, billing, notifications, workers).
12. Architecture & Decision dashboard (self-contained HTML) — populate incrementally per phase rather than all at once at the end, since it's an explicit graded deliverable.

**Rationale for this order:** Postgres/Mongo/Redis and the Identity service have zero dependency on the event backbone or Temporal, so they can be built and demoed in Week 1 with no risk. Temporal is the highest-complexity, highest-risk-of-getting-wrong component and is mandated to solve the project's core reliability requirement, so it gets the full second week rather than being squeezed in alongside Kafka. Kafka/Celery/observability come last because they are additive (they react to state changes that already flow correctly through Temporal+Postgres) rather than foundational — building them before the saga/consistency logic is solid would mean re-plumbing events once the workflow design inevitably shifts during Week 2.

## Sources

- [Saga Pattern in Microservices: A Mastery Guide | Temporal](https://temporal.io/blog/mastering-saga-patterns-for-distributed-transactions-in-microservices)
- [Saga Pattern | Temporal Documentation](https://docs.temporal.io/design-patterns/saga-pattern)
- [Implementing the Saga Pattern with Temporal: Compensation Without the Complexity](https://hosseinnejati.medium.com/implementing-the-saga-pattern-with-temporal-compensation-without-the-complexity-2000edbf07c5)
- [Transactions in Microservices: Part 3 - SAGA Pattern with Orchestration and Temporal.io](https://dev.to/federico_bevione/transactions-in-microservices-part-3-saga-pattern-with-orchestration-and-temporalio-3e17)
- [Kafka vs Celery | Svix Resources](https://www.svix.com/resources/faq/kafka-vs-celery/)
- [Temporal vs Kafka: Workflow Orchestration Guide - Xgrid](https://www.xgrid.co/resources/temporal-vs-kafka-workflow-orchestration/)
- [The Rise of the Durable Execution Engine (Temporal, Restate) in an Event-driven Architecture (Apache Kafka) - Kai Waehner](https://www.kai-waehner.de/blog/2025/06/05/the-rise-of-the-durable-execution-engine-temporal-restate-in-an-event-driven-architecture-apache-kafka/)
- [What Kafka Temporal actually does and when to use it](https://hoop.dev/blog/what-kafka-temporal-actually-does-and-when-to-use-it)
- [Reliable Microservices Data Exchange With the Outbox Pattern - Debezium](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/)
- [Debezium Outbox Pattern: Reliable Event Streaming for Microservices | RisingWave](https://risingwave.com/blog/debezium-outbox-pattern-microservices/)
- [Transactional Outbox Pattern: Guaranteeing Consistency Between PostgreSQL and Kafka with Debezium - Florian Courouge](https://floriancourouge.com/en/blog/transactional-outbox-pattern-postgres-kafka-debezium)
- [Designing robust and predictable APIs with idempotency - Stripe](https://stripe.com/blog/idempotency)
- [Implementing Idempotency Keys in REST APIs - Zuplo](https://zuplo.com/learning-center/implementing-idempotency-keys-in-rest-apis-a-complete-guide)
- [How to Handle Idempotency in Microservices](https://oneuptime.com/blog/post/2026-01-24-idempotency-in-microservices/view)
- [Polyglot Persistence in Microservices: Managing Data Diversity in Distributed (arXiv)](https://arxiv.org/pdf/2509.08014)
- [Polyglot Persistence in Microservices: Let the Domain Choose the Database](https://dev.to/dancodingbr/polyglot-persistence-in-microservices-let-the-domain-choose-the-database-3o7f)
- [Building a Scalable Slot Booking System with Redis Distributed Locks](https://dev.to/abhivyaktii/building-a-scalable-slot-booking-system-with-redis-distributed-locks-4cf8)
- [How to Implement Distributed Locks with Redis (Redlock)](https://oneuptime.com/blog/post/2026-01-21-redis-distributed-locks/view)
- [How to Model Booking/Reservation Systems in Redis](https://oneuptime.com/blog/post/2026-03-31-redis-how-to-model-bookingreservation-systems-in-redis/view)
- [Schema Evolution: 8 Kafka Best Practices | Conduktor](https://www.conduktor.io/glossary/schema-evolution-best-practices)
- [Kafka Schema Registry with Avro for Seamless Schema Evolution](https://medium.com/@muhammadfurqan17/kafka-schema-registry-with-avro-for-seamless-schema-evolution-921cca5cffa4)
- [FastAPI + Celery Work Queues: Idempotent Tasks and Retries That Don't Duplicate](https://medium.com/@hjparmar1944/fastapi-celery-work-queues-idempotent-tasks-and-retries-that-dont-duplicate-d05e820c904b)
- [Celery Task Resilience: Advanced Strategies for Reliable Workflows](https://blog.gitguardian.com/celery-tasks-retries-errors/)
- [GitHub - steinitzu/celery-singleton](https://github.com/steinitzu/celery-singleton)

---
*Architecture research for: Distributed, event-driven healthcare-operations backend (SmartHealth Part A)*
*Researched: 2026-09-09*
