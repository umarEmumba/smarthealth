# SmartHealth — Intelligent Healthcare Operations Platform (Part A)

## What This Is

SmartHealth is MediNova's foundational healthcare-operations backend — a production-grade, distributed platform for patient/provider management, appointment scheduling, visit workflows, billing pre-checks, notifications, and operational analytics. This milestone builds **Part A (the core platform)**: scalable, consistent, reliable backend services for hospitals, clinics, diagnostic centers, and telemedicine providers. It is delivered as a mentor-reviewed engineering assignment, so **defensible architecture reasoning is a graded deliverable alongside working code**.

## Core Value

Booking, updating, and completing an appointment must be **reliable and consistent under load** — partial failures never corrupt scheduling state, and every operational data model stays accurate, durable, and queryable.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Patient, provider, and staff management with role-based access (patient, provider, front-desk, admin) and audit trail
- [ ] Department & clinic management
- [ ] Reliable appointment scheduling orchestrated with Temporal (validate → reserve slot → prevent conflicts → schedule notifications → billing pre-check → confirm)
- [ ] Cancel / reschedule with slot release, waitlist movement, and billing/refund updates
- [ ] Visit & service lifecycle (check-in → in-progress → complete) with idempotent retries and duplicate prevention
- [ ] Event-driven backbone (Kafka + Schema Registry) for booking/cancellation/reschedule/visit/analytics events
- [ ] Background processing via Celery workers + RabbitMQ (notifications, reminders, follow-ups)
- [ ] Operational analytics (total patients, appointments over time, completed visits, cancellation rate, average wait time)
- [ ] Observability: Prometheus + Grafana, Jaeger tracing, OpenTelemetry; logging + dashboards for all critical flows
- [ ] Architecture & Decision dashboard (self-contained HTML) — living record of the architecture diagram and the reasoning behind each technology/design choice, used during mentor evaluations

### Out of Scope

- **Part B — GenAI layer** (data parsing/ingestion, embeddings + vector search, RAG retrieval, patient AI assistant with streaming, AI evals) — deferred to the next milestone; Part A must be solid first.
- **Production PHI/HIPAA hardening beyond assignment scope** — this is a learning build; security fundamentals (AuthN/AuthZ, audit trail) are in scope, full compliance certification is not.
- **Real payment-processor integration** — billing pre-check and status workflows are modeled; live payment gateways are out of scope for Part A.

## Context

- **Nature:** Structured learning assignment executed with Claude; mentor reviews focus on system thinking, design clarity, tool understanding, and implementation quality.
- **Execution cadence:** Part A is a 3-week plan — Week 1 Foundation + Core Services, Week 2 Scheduling + Service Workflow, Week 3 Event-Driven System + Observability.
- **Evaluation criteria:** architecture clarity, code quality/maintainability, correct use of the mandated tech stack, reliability & failure handling, observability maturity, depth of engineering understanding.
- **Source docs:** `Smart Health Requirements.md` (functional/non-functional + stack), `SmartHealth — Execution Guidelines.md` (weekly plan + evaluation approach).

## Constraints

- **Tech stack (mandated):** Python, FastAPI, PostgreSQL, a NoSQL DB, Redis, Celery + RabbitMQ, Kafka + Schema Registry, Temporal — Docker + Docker Compose for local orchestration. Observability: Prometheus + Grafana, Jaeger, OpenTelemetry. Deviations must be justified.
- **Reliability:** partial failures must not corrupt scheduling state; workflows must be idempotent, recoverable, and prevent double-processing.
- **Scalability:** must handle tens of thousands of appointment requests, concurrent provider schedule updates, peak booking windows, and multi-clinic operations.
- **Deliverable:** clean repo structure, meaningful commits, README (overview, architecture diagram, setup, API overview, stack), and technical docs (service breakdown, data/event flows, key design decisions, assumptions/tradeoffs).

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| NoSQL store → MongoDB | Document model fits audit trails, visit/service records, notification logs; flexible schema for evolving operational data | — Pending (confirm in Phase 1) |
| Temporal for scheduling orchestration | Durable, resumable multi-step workflows with retries/compensation; prevents corrupt scheduling state on partial failure | — Pending |
| Kafka (events) + RabbitMQ/Celery (tasks) | Kafka = durable, replayable cross-service event log; RabbitMQ/Celery = discrete background job execution — different jobs, both needed | — Pending |
| PostgreSQL as system of record | Strong consistency for transactional entities (patients, providers, appointments, billing) | — Pending |
| Redis for hot reads + coordination | Cache provider availability; locks/rate control during peak booking windows | — Pending |
| Eval-helper UI = self-contained HTML dashboard | Opens anywhere during reviews, no server; living record of decisions grows per phase | — Pending |
| Six-service microservice cut, database per service (Phase 1.1) | Week-1 mentor review asked for true microservices; clear bounded contexts, PHI confined to Patient, independent deploy/scale. Costs: network hops, availability coupling, no distributed transactions | Implemented (Phase 1.1) |
| RS256 + JWKS, local verification in every service (Phase 1.1) | Only Identity holds the signing key; services verify from a cached JWKS without calling Identity, so a compromised service cannot mint tokens | Implemented (Phase 1.1) |
| Synchronous reference checks now, event-fed copies in Phase 3 (Phase 1.1) | No Kafka/outbox until Phase 3; fail closed (422 missing/inactive, 503 owner down), no cascade; event-fed copies later remove the availability coupling at the price of eventual consistency | Implemented (Phase 1.1) |
| Thin FastAPI gateway with Redis rate limiting (Phase 1.1) | Most reviewable edge; per-IP limits deliberately fail open; no JWT check or aggregation at the edge (every service verifies for itself) | Implemented (Phase 1.1) |
| Audit after a committed step (Phase 2.1, S3) | Non-fatal: log `audit.write_failed`, keep the outcome; transactional outbox in Phase 3. The transition is already durable in `appointment_status_history`; failing the request or the workflow after the commit would corrupt the caller's view. Cost: an audit entry can be lost if Mongo is down at that instant | Implemented (Phase 2.1) |
| Schedule-change delivery (Phase 2.1, M2) | Provider outbox table pulled by a polling Schedule, not pushed. A push is lost if Scheduling or Temporal is down when the change happens; the pending-until-acknowledged table has no such window and Phase 3 can publish the same rows to Kafka. Cost: up to one poll interval of delay | Implemented (Phase 2.1) |
| Stale-slot detection (Phase 2.1, M2) | Set difference against the re-expanded current templates, not tracking which template made which slot. Idempotent, needs no slot-to-template link and reuses the tested generator, so the candidate set cannot drift from what generation produces | Implemented (Phase 2.1) |
| Stuck-state recovery (Phase 2.1, S4) | Temporal Schedule per service running one idempotent reconciler workflow (overlap SKIP, batch cap, conditional updates). Same pattern as the hold reaper and the starter, observable in the Temporal UI; re-derives outcomes from the database instead of guessing. Cost: polling, and no fairness beyond oldest first | Implemented (Phase 2.1) |
| Check-in window (Phase 2.1, M1) | Opens 60 minutes before the slot, closes at slot end; no-show grace 30 minutes; all by the database clock and configurable. Judging with `now()` in the locking `SELECT` removes clock skew between services and makes the decision atomic with the transition | Implemented (Phase 2.1) |
| Refresh tokens (Phase 2.1, C1) | Single-use rotation with an atomic Redis claim and an `rjti` claim on the access token. One winner under concurrent reuse; logout revokes both tokens without client cooperation. Cost: clients must store the rotated token; a Redis outage blocks refresh (fail closed) | Implemented (Phase 2.1) |
| bcrypt placement (Phase 2.1, C2) | Bounded named thread pool via `run_in_executor`. bcrypt releases the GIL, so a small pool runs in parallel without stalling the event loop (proven by a loop-lag test) | Implemented (Phase 2.1) |
| Case-insensitive email (Phase 2.1, C3) | `lower(email)` unique expression index and a normalizer, without rewriting existing rows. Cheap and reversible: the migration refuses over duplicates (count only) and the downgrade just drops the index. Cost: legacy rows keep mixed casing | Implemented (Phase 2.1) |
| Pagination convention (Phase 2.1, C5) | Plain array body, `X-Total-Count`, `limit` 1..200 default 50, `offset` >= 0, order `(created_at, id)`, `422` outside bounds. Backward compatible with existing clients and Visit's list; the id tiebreak makes paging stable | Implemented (Phase 2.1) |
| Load test and NFRs (Phase 2.1, D1, D2) | k6 from a pinned Docker image; targets single-sourced in `loadtest/nfr.json`, checked against the PRD table and `docs/nfr.md` by a test. No package added to the project; a changed number cannot drift between the documents and the thresholds. Cost: the targets are not yet measured (Phase 3) | Implemented (Phase 2.1) |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid (esp. Part B promotion)?
4. Update Context with current state

---
*Last updated: 2026-09-09 after initialization*
