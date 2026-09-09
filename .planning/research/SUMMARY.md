# Project Research Summary

**Project:** SmartHealth Part A — distributed, event-driven healthcare-operations backend
**Domain:** Healthcare operations & appointment-scheduling platform (mentor-reviewed learning build; Part B GenAI explicitly deferred)
**Researched:** 2026-09-09
**Confidence:** MEDIUM

## Executive Summary

SmartHealth Part A is a mandated-stack distributed backend for clinic operations: patient/provider/staff management, appointment scheduling with conflict prevention, visit lifecycle, billing pre-check, notifications, and operational analytics — built on FastAPI, PostgreSQL, MongoDB, Redis, Temporal, Kafka+Schema Registry, Celery+RabbitMQ, and a full Prometheus/Grafana/Jaeger/OpenTelemetry observability stack. This is not a feature-differentiation problem; it is an architecture-defensibility problem. The grading criteria (architecture clarity, tradeoff reasoning, correct stack use, reliability, observability) mean the roadmap must sequence around getting four load-bearing decisions right, not around shipping the most features fastest: (1) Temporal owns the multi-step booking/visit sagas, Kafka carries durable replayable facts, Celery/RabbitMQ executes discrete fire-and-forget jobs — three engines, three distinct jobs, never substituted for one another; (2) the transactional outbox pattern (Postgres row + outbox row in one transaction, relayed to Kafka via Debezium CDC or a polling publisher) is the only acceptable way to get data from Postgres into Kafka — direct dual writes are a named anti-pattern; (3) idempotency is layered in defense-in-depth (Redis idempotency-key cache → Temporal deterministic workflow IDs → Postgres unique/exclusion constraints as the ultimate backstop), because no single layer is sufficient on its own; (4) PostgreSQL owns every entity requiring transactional/relational integrity (patients, providers, appointments, slots, billing pre-check, the outbox table itself), while MongoDB owns only derived, append-heavy, schema-flexible documents (audit trail, visit/service records, notification logs, analytics projections) — MongoDB must never become a second writable source of truth for anything Postgres already owns.

The recommended approach is to build in dependency order across the mandated 3-week plan: foundation entities and the Postgres/Mongo/Redis split first (zero risk, no Temporal/Kafka dependency), then the highest-risk component — Temporal-orchestrated booking/visit sagas with real concurrency-safe slot reservation — in week two, and finally the additive event backbone (Kafka, outbox relay, Celery workers, full observability) in week three once the core state machine is proven correct. This order exists specifically to avoid re-plumbing events after the saga design inevitably shifts, and to give the highest-complexity, most-scrutinized component (Temporal) a full week rather than squeezing it in alongside Kafka setup.

The primary risks are well-documented, stack-specific failure modes rather than novel ones: double-booking via check-then-insert races (mitigated by Postgres exclusion constraints, not just app logic or Redis locks), non-deterministic Temporal workflow code causing replay failures, non-idempotent activities/consumers/tasks causing duplicate side effects on retry, dual-write inconsistency between Postgres and Kafka/Mongo, and missing distributed trace propagation across Kafka/Celery/Temporal async boundaries. Every one of these has a concrete, well-established prevention pattern; the risk is not knowing the pattern but skipping it under time pressure, since each failure mode "looks done" in single-request manual testing and only surfaces under concurrent load or process-crash timing windows — exactly the conditions a 3-week assignment timeline tempts teams to under-test.

## Key Findings

### Recommended Stack

The stack is mandated (not chosen); the research therefore identifies which library within each component and how they wire together. Use SQLAlchemy 2.0 async + asyncpg for Postgres (never sync psycopg2 in async routes), PyMongo's native async API (`AsyncMongoClient`) instead of the now-deprecated Motor, redis-py's built-in `redis.asyncio` (not the unmaintained standalone `aioredis`), confluent-kafka (built on librdkafka, bundled Schema Registry client) over aiokafka for production throughput, and OTLP-native exporters into Jaeger (not the legacy Jaeger-native exporter). Four independent process types come from one codebase: `api` (FastAPI, never runs Celery tasks or Temporal activities in-process), `temporal-worker` (Workflow + Activity definitions), `celery-worker` (RabbitMQ-backed discrete jobs), and `kafka-consumer` (dedicated, synchronous, non-asyncio processes — confluent-kafka's consumer API is blocking and must never run inside the API's event loop).

**Core technologies:**
- FastAPI + Pydantic v2 + SQLAlchemy 2.0 async/asyncpg — async-native API layer and system-of-record ORM, required for the concurrent-load target (tens of thousands of requests)
- Temporal (temporalio SDK) — durable saga orchestration for booking/visit lifecycle; the mandated answer to "partial failures must not corrupt scheduling state"
- Kafka + Schema Registry (confluent-kafka) — durable, replayable, multi-consumer event backbone for facts that already happened (booked/cancelled/completed)
- Celery + RabbitMQ — discrete, single-step, fire-and-forget background jobs (notifications, reminders), triggered by Temporal activities or Kafka consumers, never the saga engine itself
- Redis (redis-py asyncio) — idempotency-key cache, fast-path locks, availability cache, rate limiting — coordination/performance only, never the source of correctness
- PostgreSQL / MongoDB — split system-of-record vs. document store (see Architecture below)
- OpenTelemetry + Jaeger + Prometheus + Grafana — cross-cutting tracing/metrics, explicitly graded

### Expected Features

Part A scope is an operations/scheduling backend, not a clinical or GenAI system. The differentiator for this milestone is engineering depth on table-stakes features, not additional patient-facing features.

**Must have (table stakes):**
- Patient/provider/staff registration with RBAC (patient, provider, front-desk, admin) and department/clinic management
- Real-time slot booking with conflict prevention (the single most-cited "must have" across scheduling platforms)
- Reschedule/cancel with slot release and waitlist movement
- Visit lifecycle (check-in → in-progress → complete) with idempotent transitions
- Billing pre-check (modeled state transitions only, no live gateway)
- Notifications (booking/cancellation/reminders/follow-ups) via background workers
- Event-driven backbone (Kafka+Schema Registry) for booking/visit/analytics events
- Core operational analytics (5 metrics: total patients, appointments over time, completed visits, cancellation rate, avg wait time)
- Audit trail for profile & operational changes
- Full observability wired into every critical flow
- Architecture & Decision dashboard (self-contained HTML) — explicit graded deliverable

**Should have (differentiators, still within Part A):**
- Capacity-aware scheduling (reserved urgent/same-day slot blocks) — cheap, high signal for "we thought about real operations"
- Intelligent overbooking via a simple statistical/heuristic no-show signal (NOT ML/LLM — must remain rule-based to stay out of Part B's GenAI territory)

**Defer (explicitly out of scope for Part A):**
- GenAI assistant, RAG, embeddings, AI evals (Part B)
- Live payment gateway integration
- Full HIPAA compliance certification (security fundamentals only: RBAC, audit trail, no PHI in logs)
- Clinical documentation/e-prescribing/lab orders, telemedicine video infra, third-party calendar federation (Google/Outlook), patient-facing native mobile app/UI polish

### Architecture Approach

Bounded-context services (identity, scheduling, visits, notifications, analytics) each own their API + workflows/activities + repository as a vertical slice, sharing cross-cutting libs (`events`, `idempotency`, `outbox`, `observability`). The system is organized around a clear responsibility split: Temporal orchestrates durable multi-step sagas; Kafka carries facts that already happened for independent downstream consumption; Celery executes single fire-and-forget jobs. PostgreSQL and MongoDB are split by consistency need, not by convenience.

**Major components:**
1. Identity & Directory service (FastAPI + Postgres + Mongo audit writes) — patients, providers, staff, departments, clinics, RBAC — zero dependency on Temporal/Kafka, buildable first
2. Scheduling API + Temporal BookingWorkflow/CancelRescheduleWorkflow — thin HTTP layer that starts/signals workflows; owns the saga (validate → reserve slot → billing pre-check → schedule notification → confirm) with compensation on failure
3. Visit API + VisitLifecycleWorkflow — check-in → in-progress → complete state machine, idempotent transitions
4. Postgres outbox table → Debezium CDC (or polling publisher) → Kafka+Schema Registry — the only sanctioned path from transactional state changes to durable events
5. Celery + RabbitMQ notification workers — triggered by Temporal activities or Kafka consumers, never orchestrating multi-step logic themselves
6. Analytics consumer service — projects Kafka events into MongoDB read models, served read-only, never recomputed live from Postgres
7. Observability layer (OTel/Jaeger/Prometheus/Grafana) — cross-cutting, with explicit trace-context propagation required at every async boundary (Kafka headers, Celery task headers, Temporal interceptors)

### Critical Pitfalls

1. **Double-booking via check-then-insert race** — passes single-request testing, fails under concurrent load. Avoid with a Postgres `EXCLUDE USING gist` constraint on `(provider_id, tsrange(start,end))` as the enforced invariant, plus `SELECT...FOR UPDATE`; never rely on Redis locks alone.
2. **Temporal workflow non-determinism** — calling `datetime.now()`/random/I-O directly inside `@workflow.defn` breaks replay and strands in-flight workflows days after deploy. Keep all I/O and wall-clock/randomness inside `@activity.defn`; use `workflow.now()`/`workflow.uuid4()`; adopt replay testing in CI.
3. **Non-idempotent activities/consumers/tasks duplicate side effects on retry** — Temporal retries, Kafka redelivery, and Celery `acks_late` redelivery are all expected; without idempotency keys/unique constraints, retries cause duplicate notifications, billing charges, or audit entries.
4. **Dual-write inconsistency between Postgres and Kafka/Mongo** — `db.commit(); kafka.publish()` as two separate calls silently loses events on crash/network blip. Solved only by the transactional outbox pattern; MongoDB writes should be downstream Kafka projections, not a second synchronous write in the request path.
5. **Missing trace context propagation across Kafka/Celery/Temporal boundaries** — OTel auto-instrumentation covers HTTP/DB but not custom async boundaries; without explicit inject/extract at each hop, Jaeger shows disconnected single-span traces instead of one end-to-end trace, undermining the core observability grading criterion.

## Implications for Roadmap

Based on research, suggested phase structure (aligned to the mandated 3-week execution plan):

### Phase 1: Foundation — Infra Skeleton + Identity/Directory
**Rationale:** Postgres/Mongo/Redis and the Identity service have zero dependency on Temporal or Kafka, so this phase is buildable and demoable with no architectural risk. It also validates the Postgres/Mongo ownership split early, before anything more complex depends on it.
**Delivers:** Full Docker Compose skeleton (all 8+ services, including Kafka/Temporal/observability stood up even if unwired), patient/provider/staff/department/clinic entities with RBAC, audit-trail writes to MongoDB, appointment schema design including the outbox table and provider/slot unique constraint.
**Addresses:** Patient/provider/staff registration, RBAC, department & clinic management, audit trail (table stakes from FEATURES.md).
**Avoids:** Sets up correct Postgres/Mongo ownership split before any drift can occur (Pitfall: polyglot persistence ambiguity); establishes OTel/Jaeger scaffolding early so trace propagation isn't retrofitted later (Pitfall 8).

### Phase 2: Scheduling & Visit Sagas (Temporal Core)
**Rationale:** Temporal is the highest-complexity, most heavily-scrutinized component and directly implements the project's core reliability requirement ("partial failures must not corrupt scheduling state"). It must be solid before the event backbone is built on top of it, or events will need re-plumbing once the saga design shifts.
**Delivers:** BookingWorkflow with saga/compensation, Redis fast-path lock + Postgres exclusion-constraint slot reservation, VisitLifecycleWorkflow (check-in→in-progress→complete), CancelRescheduleWorkflow (release→waitlist→billing reversal→notify), billing pre-check modeled as internal state transitions.
**Uses:** temporalio SDK, SQLAlchemy 2.0 async + asyncpg, redis-py distributed locks.
**Implements:** Temporal Saga pattern (Pattern 1), three-layer idempotency (API/Redis → Temporal workflow ID → Postgres constraint), waitlist management.
**Avoids:** Double-booking race (Pitfall 1), non-deterministic workflow code (Pitfall 2), over-reliance on Redis locks alone (Pitfall 7), saga compensation modeled as rollback instead of forward business transaction (Pitfall 9).

### Phase 3: Event Backbone, Background Workers, Analytics & Observability Closeout
**Rationale:** Kafka/Celery/full observability are additive — they react to state changes that must already flow correctly through Temporal+Postgres. Building them before the saga/consistency logic is solid risks re-plumbing. This phase also closes out the graded observability and architecture-dashboard deliverables.
**Delivers:** Transactional outbox → Kafka wiring (Debezium CDC or polling publisher) with Avro schemas in Schema Registry (BACKWARD compatibility mode), Celery+RabbitMQ notification workers (acks_late + DLQ + idempotent task bodies) triggered by both Temporal activities and Kafka consumers, analytics consumer projecting events into MongoDB read models serving the 5 required metrics, full OTel/Jaeger/Prometheus/Grafana wiring with verified end-to-end trace propagation, the self-contained Architecture & Decision dashboard populated incrementally.
**Uses:** confluent-kafka + Schema Registry, Celery 5.6 + RabbitMQ, OpenTelemetry SDK + OTLP exporters.
**Implements:** Outbox pattern (Pattern 2), Kafka partition-keying by `appointment_id`/`provider_id`, cooperative-sticky rebalance strategy, effectively-once consumer processing.
**Avoids:** Dual-write inconsistency (Pitfall 4), Kafka ordering/rebalance breakage (Pitfall 5), Celery ack/retry misconfiguration (Pitfall 6), missing trace propagation (Pitfall 8 — re-verified here since each new integration is a new propagation boundary).

### Phase Ordering Rationale

- Dependency-driven: scheduling requires patient/provider/clinic entities to exist; waitlist requires scheduling; notifications require scheduling events; analytics requires both scheduling and visit-lifecycle events as sources — this forces Foundation → Saga Core → Event/Analytics/Observability as the only defensible order.
- Risk-driven: Temporal is named repeatedly across all four research documents as the highest-scrutiny, highest-complexity, most failure-prone-if-rushed component — it gets a dedicated phase with nothing else competing for attention, rather than being interleaved with Kafka setup.
- Pitfall-driven: several critical pitfalls (double-booking, non-determinism, saga-as-rollback) are explicitly tied to Week 2/Scheduling in PITFALLS.md's phase mapping, while dual-write and Kafka-ordering pitfalls are tied to Week 3/Events — the phase boundaries mirror the pitfall document's own phase mapping almost exactly, which is a strong signal this is the correct sequencing.
- Additive-last: Kafka/Celery/observability react to already-correct state; sequencing them last avoids rebuilding event wiring after saga design changes.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2 (Scheduling & Visit Sagas):** Temporal saga/compensation design and exact activity idempotency-key strategy warrant a focused planning-time research pass — STACK.md and ARCHITECTURE.md both flag Temporal SDK↔server version compatibility as an open gap needing re-verification against `docs.temporal.io` at implementation time.
- **Phase 3 (Event Backbone):** Outbox relay implementation choice (Debezium CDC vs. custom polling publisher) is called out in PITFALLS.md as "worth its own spike" — this is a real architectural tradeoff to document explicitly in the decision dashboard, not a default to assume.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Foundation):** CRUD/RBAC/entity modeling and Docker Compose wiring are well-documented, low-risk patterns with no open questions in the research.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | MEDIUM | Verified against live PyPI/DockerHub registry APIs (HIGH-confidence primary sources) for exact versions, but no official-docs MCP/Context7 was available, so integration guidance (async Kafka patterns, Temporal/server SDK versioning) is cross-checked web search, not vendor-authoritative in all cases |
| Features | MEDIUM | Derived from commercial healthcare-scheduling industry sources (NexHealth, Zocdoc, Phreesia, etc.) plus the project's own PROJECT.md/Requirements docs — solid for table-stakes/anti-feature scoping, softer for the "intelligent overbooking" differentiator's exact uplift numbers |
| Architecture | MEDIUM | Vendor-authoritative sources (Temporal's own blog/docs, Debezium's own blog, Stripe's idempotency writeup) corroborate community sources; every load-bearing claim (saga mechanics, outbox pattern, idempotency layering, Schema Registry compatibility modes) triangulated across 3+ sources |
| Pitfalls | MEDIUM-HIGH | Cross-checked across multiple independent sources per topic including Temporal's own "anti-patterns" blog series; no single case study covers this exact 8-technology combination, so composition-level risk (e.g., Temporal+Kafka+Celery interacting) is reasoned from component-level evidence rather than an end-to-end reference implementation |

**Overall confidence:** MEDIUM

### Gaps to Address

- **Temporal SDK ↔ server version lockstep:** No official strict compatibility matrix was found; STACK.md and ARCHITECTURE.md both recommend re-verifying "recent SDK + recent server line" against `docs.temporal.io/develop/python/versioning` before locking a submission version — do this at Phase 2 planning time, not deferred to execution.
- **Outbox relay mechanism (Debezium CDC vs. polling publisher):** Research presents both as valid for a 3-week timeline but doesn't resolve which is better for this specific grading context; treat as a Phase 3 planning decision to make explicitly and document as a tradeoff in the Architecture & Decision dashboard (mentors are expected to probe this exact question).
- **Auto-setup vs. production-shaped Temporal deployment:** `temporalio/auto-setup` is fine for local dev per research but is explicitly not production-shaped — decide during Phase 1 whether to invest in the production-shaped `temporalio/server` + schema-setup job given "maintainable production-grade architecture" is a listed evaluation outcome.
- **Exact notification channel implementation (email/SMS provider):** Treated as an implementation detail across all four docs, not resolved to a specific vendor/stub choice — fine to decide ad hoc during Phase 3 planning.

## Sources

### Primary (HIGH confidence)
- pypi.org JSON API (direct query) — exact current versions for all core Python dependencies
- hub.docker.com v2 API (direct query) — image tags for all Compose services
- github.com/temporalio/temporal releases API — current Temporal server version

### Secondary (MEDIUM confidence)
- Temporal official blog/docs (saga pattern, anti-patterns, versioning, side-effects, safe deployments)
- Debezium official blog (outbox pattern)
- Stripe official blog (idempotency key design)
- MongoDB official docs (Motor deprecation timeline)
- Confluent official blog (dual-write problem, exactly-once semantics, cooperative rebalancing)
- Industry healthcare-scheduling sources (NexHealth, Zocdoc, Phreesia, PracticeSuite, SimplePractice, Pabau) for feature-landscape scoping
- Project source docs: PROJECT.md, Smart Health Requirements.md

### Tertiary (LOW confidence)
- Individual technical blog posts (Medium, dev.to, OneUptime, RisingWave, etc.) cross-referenced 2-3 per claim for Kafka async patterns, Redis locking pitfalls, Celery gotchas, OTel propagation — treated as corroborating detail, not sole basis for any recommendation

---
*Research completed: 2026-09-09*
*Ready for roadmap: yes*
