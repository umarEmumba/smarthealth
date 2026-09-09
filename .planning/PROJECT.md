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
