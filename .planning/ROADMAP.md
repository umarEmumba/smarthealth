# Roadmap: SmartHealth — Intelligent Healthcare Operations Platform (Part A)

## Overview

Part A delivers a reliable, distributed healthcare-operations backend across three weeks, each week a phase, in strict dependency order. Week 1 lays the foundation — infra skeleton, identity/RBAC, patient/provider/clinic management, audit trail, and the first cut of the Architecture & Decision dashboard — with zero dependency on Temporal or Kafka, so it is buildable and demoable with no architectural risk. Week 2 builds the highest-scrutiny, highest-risk component: Temporal-orchestrated booking, cancel/reschedule, and visit-lifecycle sagas with real concurrency-safe slot reservation, compensation, and a modeled billing pre-check — the direct implementation of the project's core reliability requirement ("partial failures never corrupt scheduling state"). Week 3 is additive: it wires the event-driven backbone (Kafka + Schema Registry via transactional outbox), background workers (Celery + RabbitMQ with DLQ), event-derived analytics, full observability (Prometheus/Grafana/Jaeger/OTel with end-to-end trace propagation), and closes out the Architecture & Decision dashboard with data/event flows and the evaluation Q&A. This order exists specifically so events never need re-plumbing after the saga design shifts, and so Temporal — the component every research document flags as the single most scrutinized design decision — gets a full week with nothing else competing for attention.

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work, one per execution week
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED) — none currently planned

- [ ] **Phase 1: Foundation + Core Services** - Infra skeleton, identity/RBAC, patient/provider/clinic/department management, audit trail, tracing conventions, and dashboard v1
- [ ] **Phase 2: Scheduling + Service Workflow** - Temporal-orchestrated booking/cancel/reschedule/visit sagas with conflict prevention, idempotency, waitlist, and modeled billing pre-check
- [ ] **Phase 3: Event-Driven System + Observability** - Kafka event backbone, Celery background workers, event-derived analytics, full observability, and dashboard closeout

## Phase Details

### Phase 1: Foundation + Core Services

**Goal**: Operators can authenticate with role-scoped access, register and manage patients/providers/clinics/departments with consistent state, every mutating action is captured in an audit trail with no PHI in logs, the full stack runs locally with health checks, and the Architecture & Decision dashboard exists showing the system diagram and initial tech-choice rationale.
**Depends on**: Nothing (first phase)
**Requirements**: IAM-01, IAM-02, IAM-03, IAM-04, PPL-01, PPL-02, PPL-03, PPL-04, ORG-01, ORG-02, ORG-03, OBS-04, DASH-01, DASH-02
**Success Criteria** (what must be TRUE):

  1. A user can register and log in as patient, provider, front-desk, or admin, receiving a session/token validated on every subsequent protected request.
  2. Role-based access control is enforced end-to-end — e.g., a front-desk account can manage appointments but is rejected (403) attempting an admin-only operation.
  3. Front-desk/admin can register and retrieve a patient profile, and admin can register a provider with one or more specialties and configure availability windows, with reads reflecting the latest committed state.
  4. Admin can create a clinic and departments within it, and providers/slots are visibly scoped to the correct clinic/department.
  5. Every mutating profile/operational action produces an audit-trail entry (who, what, when) inspectable in MongoDB, with no PHI content present in application logs.
  6. `docker compose up` brings up the full stack (Postgres, MongoDB, Redis, plus Kafka/Temporal/RabbitMQ/observability scaffolding even if not yet wired into code) with passing health checks, and the self-contained HTML dashboard opens with an architecture diagram and documented technology tradeoffs (including NoSQL choice, Temporal, Kafka+RabbitMQ split, Postgres system-of-record, Redis role).

**Plans**: 5/6 plans executed

Plans:
**Wave 1**

- [x] 01-01-scaffold-compose-pytest-PLAN.md — Project scaffolding, full-stack Docker Compose with health checks (OBS-04), pytest infrastructure

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-data-model-migrations-PLAN.md — SQLAlchemy 2.0 async data model + Alembic migrations (all entities, empty provider_slots with GiST exclusion constraint) + Mongo audit indexes

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-03-tracer-auth-rbac-audit-PLAN.md — TRACER: end-to-end register→login→RBAC-scoped request→audit→PHI-safe log/trace (IAM-01/02/03/04)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-04-clinic-department-crud-PLAN.md — Clinic + department CRUD, admin-only, clinic-scoped, audited (ORG-01/02/03)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 01-05-patient-provider-availability-crud-PLAN.md — Patient + provider (with specialties) + availability-template CRUD, PHI-safe (PPL-01/02/03/04)

**Wave 6** *(blocked on Wave 5 completion)*

- [ ] 01-06-dashboard-reconciliation-PLAN.md — Reconcile the self-contained Architecture & Decision dashboard to the actual build (DASH-01/02)

### Phase 2: Scheduling + Service Workflow

**Goal**: Booking, rescheduling, cancelling, and completing a visit are reliable and consistent under concurrent load — every multi-step transition is Temporal-orchestrated, double-booking is impossible at the data layer, retries never duplicate effects, partial failures compensate cleanly with no corrupted state, and the dashboard is extended with workflow/state diagrams and the "why Temporal" rationale.
**Depends on**: Phase 1
**Requirements**: SCHED-01, SCHED-02, SCHED-03, SCHED-04, SCHED-05, SCHED-06, SCHED-07, SCHED-08, VISIT-01, VISIT-02, VISIT-03, VISIT-04, VISIT-05, BILL-01, BILL-02
**Success Criteria** (what must be TRUE):

  1. A patient/front-desk user can book an appointment against an available slot, and the appointment is marked confirmed only after the full Temporal saga (validate → reserve slot → prevent conflicts → schedule notification → billing pre-check → confirm) completes successfully.
  2. Under a concurrent load test, parallel booking attempts against the same provider slot never produce more than one confirmed appointment — enforced by a Postgres-level constraint, not application logic alone.
  3. Retried/duplicate booking requests (same idempotency key) never create a second appointment, and visit-state transitions (checked-in → in-progress → complete) are safely repeatable without duplicate side effects.
  4. A patient/front-desk user can reschedule (old slot released and new slot reserved atomically) or cancel (slot released, billing/refund state updated) an appointment, and a freed slot correctly advances the waitlist to the next matching patient.
  5. A deliberately-failed booking (e.g., billing pre-check failure) leaves no corrupted or half-true state — compensating actions run as forward transactions (release, reverse, notify) preserving full audit history, never a hard delete or silent rollback.
  6. Front-desk can check in a confirmed appointment and advance it through visit states in real time queryable by ops staff, and completing a visit triggers downstream billing/follow-up/analytics work without blocking the transition; the dashboard now shows booking/visit workflow diagrams and the Temporal rationale.

**Plans**: TBD

### Phase 3: Event-Driven System + Observability

**Goal**: Every state change reliably becomes a durable, replayable event with no dual-write gaps, background jobs run independently of user-facing flows with guaranteed at-least-once delivery and no silent loss, operators can see the five core operational metrics derived from the event stream, and every critical flow is traceable end-to-end and observable via metrics/logs/dashboards — with the Architecture & Decision dashboard fully closed out.
**Depends on**: Phase 2
**Requirements**: EVENT-01, EVENT-02, EVENT-03, EVENT-04, EVENT-05, NOTIF-01, NOTIF-02, NOTIF-03, NOTIF-04, ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06, OBS-01, OBS-02, OBS-03, DASH-03, DASH-04
**Success Criteria** (what must be TRUE):

  1. Booking/cancellation/reschedule/visit-completion/provider-schedule-change/analytics events are published to Kafka with a registered schema via a transactional outbox — killing the process between DB commit and event publish never loses or duplicates the event, and consumers tolerate at-least-once redelivery without duplicate side effects.
  2. Booking, cancellation, and reschedule each trigger an asynchronous patient notification that never blocks the booking transaction; appointment reminders and post-visit follow-ups are scheduled and delivered by Celery/RabbitMQ workers, which retry on failure without double-sending and route poison messages to a dead-letter queue.
  3. An operator can view all five operational metrics (total patients, appointments over time, completed visits, cancellation rate, average wait time), each computed from the Kafka event stream/projections rather than by polling the transactional database on the hot path.
  4. Every service exposes Prometheus metrics visible in Grafana dashboards for critical flows, and a single booking request can be traced end-to-end in Jaeger across HTTP → Temporal workflow/activity → Kafka → Celery as one connected trace (not disconnected fragments), with structured logs available for booking, availability sync, billing, notifications, and background workers.
  5. The Architecture & Decision dashboard shows the key data/event flows (booking, cancel/reschedule, visit lifecycle, notifications, analytics) and includes a prepared evaluation Q&A section, complete and current as of this phase's close.

**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation + Core Services | 5/6 | In Progress|  |
| 2. Scheduling + Service Workflow | 0/TBD | Not started | - |
| 3. Event-Driven System + Observability | 0/TBD | Not started | - |
