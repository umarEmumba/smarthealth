# Roadmap: SmartHealth — Intelligent Healthcare Operations Platform (Part A)

## Overview

Part A delivers a reliable, distributed healthcare-operations backend across three weeks, each week a phase, in strict dependency order. Week 1 lays the foundation — infra skeleton, identity/RBAC, patient/provider/clinic management, audit trail, and the first cut of the Architecture & Decision dashboard — with zero dependency on Temporal or Kafka, so it is buildable and demoable with no architectural risk. Week 2 builds the highest-scrutiny, highest-risk component: Temporal-orchestrated booking, cancel/reschedule, and visit-lifecycle sagas with real concurrency-safe slot reservation, compensation, and a modeled billing pre-check — the direct implementation of the project's core reliability requirement ("partial failures never corrupt scheduling state"). Week 3 is additive: it wires the event-driven backbone (Kafka + Schema Registry via transactional outbox), background workers (Celery + RabbitMQ with DLQ), event-derived analytics, full observability (Prometheus/Grafana/Jaeger/OTel with end-to-end trace propagation), and closes out the Architecture & Decision dashboard with data/event flows and the evaluation Q&A. This order exists specifically so events never need re-plumbing after the saga design shifts, and so Temporal — the component every research document flags as the single most scrutinized design decision — gets a full week with nothing else competing for attention.

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work, one per execution week
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

- [x] **Phase 1: Foundation + Core Services** - Infra skeleton, identity/RBAC, patient/provider/clinic/department management, audit trail, tracing conventions, and dashboard v1 (completed 2026-09-09)
- [x] **Phase 1.1: Microservice Re-architecture** (INSERTED) - Split the Phase 1 monolith into independently deployable services with their own databases, an API gateway, and RS256/JWKS token verification (week-1 mentor feedback) (completed 2026-10-12)
- [ ] **Phase 2: Scheduling + Service Workflow** - Temporal-orchestrated booking/cancel/reschedule/visit sagas with conflict prevention, idempotency, waitlist, and modeled billing pre-check
- [ ] **Phase 2.1: Week 1-2 mentor review remediation** (INSERTED) - Appointment state machine and check-in window, provider schedule-change propagation, post-commit saga safety, reconcilers, Week 1 carry-overs, quantified NFRs and a load test (week 1-2 mentor feedback)
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

**Plans**: 7/7 plans executed

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

- [x] 01-06-dashboard-reconciliation-PLAN.md — Reconcile the self-contained Architecture & Decision dashboard to the actual build (DASH-01/02)

**Wave 7** *(gap-closure, blocked on Wave 4/5 completion)*

- [x] 01-07-crud-update-softdelete-PLAN.md — PATCH + soft-delete (deactivate) for clinics/departments/patients/providers/availability, closing the PPL-01/ORG-01/ORG-02 "manage" gap surfaced in Phase 1 verification (no hard deletes)

### Phase 01.1: Microservice Re-architecture (INSERTED)

**Goal**: The Phase 1 monolith (`services/identity`) is re-delivered as a true microservice architecture, following week-1 mentor review feedback. Each bounded context runs as an independently deployable service that owns its own database. There are no cross-service foreign keys or shared-table reads, and traffic enters through an API gateway. All Phase 1 behaviour (IAM, RBAC, patient/provider/clinic/department CRUD, audit, PHI-safe logging) keeps working end-to-end. The Architecture & Decision dashboard records the monolith→microservices decision and the service map.
**Depends on**: Phase 1
**Requirements**: Re-delivers IAM-01..04, PPL-01..04, ORG-01..03, OBS-04, DASH-01/02 under the new architecture (no new functional requirements)
**Success Criteria** (what must be TRUE):

  1. The Phase 1 domain is split into separately deployable services (locked in 01.1-CONTEXT.md: API Gateway, Identity, Organization, Provider, Patient, plus a Scheduling skeleton that owns `provider_slots`; audit remains a shared library until Phase 3). Each has its own codebase directory, Dockerfile, Alembic migrations, and database. No service connects to another service's database.
  2. Cross-service foreign keys (users→clinics, patients→users/clinics, providers→users/departments, availability_templates→departments, provider_slots→providers) are replaced by ID references. Integrity is validated through the owning service's API or through local copies of other services' data kept up to date by events.
  3. Identity is the only service that issues tokens (RS256 signing plus a JWKS endpoint). Every other service verifies tokens locally with the public key and enforces RBAC itself. The gateway routes all external traffic and applies rate limiting.
  4. Shared `libs/` is limited to infrastructure code (token verification, audit client, OTel/logging) and never shares domain models. Audit entries stay PHI-safe, and every service's traces carry a shared trace context across service-to-service calls.
  5. `docker compose up` starts every service plus the infrastructure with passing health checks. The existing Phase 1 test scenarios pass through the gateway, and the dashboard shows the updated service-map diagram and the rationale for the microservices move.

**Plans:** 10/10 plans complete (sequential waves — shared working tree and shared live test infrastructure)

**Wave 1**

- [x] 01.1-01-PLAN.md — TRACER: Identity on its own database with RS256 signing + JWKS, infrastructure-only shared libs, idempotent database-per-service bootstrap, multi-app test harness (IAM-01..04, OBS-04)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01.1-02-PLAN.md — Identity parity: account lifecycle, duplicate-registration race, relocated identity/audit/bootstrap tests (IAM-01..04)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01.1-03-PLAN.md — API gateway core + Organization service (clinics, departments, internal reference lookups); every test now travels through the gateway (ORG-01..03)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01.1-04-PLAN.md — Connected cross-service traces, package-legitimacy checkpoint (tenacity, opentelemetry-instrumentation-httpx), gateway rate limiting (IAM-03/04)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 01.1-05-PLAN.md — D-05 reference-check client, users.clinic_id validation, service tokens, Identity /internal/users (IAM-01/03/04, ORG-03)

**Wave 6** *(blocked on Wave 5 completion)*

- [x] 01.1-06-PLAN.md — Patient service (only PHI holder) with clinic/user reference checks (PPL-01, PPL-04)

**Wave 7** *(blocked on Wave 6 completion)*

- [x] 01.1-07-PLAN.md — Provider service (providers, specialties, availability) with department/user checks, no-cascade deactivation, /internal/providers (PPL-02..04, ORG-03)

**Wave 8** *(blocked on Wave 7 completion)*

- [x] 01.1-08-PLAN.md — Scheduling skeleton (provider_slots + exclusion constraint), per-service Dockerfiles, Compose wiring with the gateway as sole ingress, real-stack smoke test (OBS-04, ORG-03)

**Wave 9** *(blocked on Wave 8 completion)*

- [x] 01.1-09-PLAN.md — Monolith retirement + isolation invariant suite (OBS-04, PPL-01, ORG-03)

**Wave 10** *(blocked on Wave 9 completion)*

- [x] 01.1-10-PLAN.md — Architecture & Decision dashboard service map + decision records, README, architecture/ERD docs (DASH-01, DASH-02)

### Phase 2: Scheduling + Service Workflow

**Goal**: Booking, rescheduling, cancelling, and completing a visit are reliable and consistent under concurrent load — every multi-step transition is Temporal-orchestrated, double-booking is impossible at the data layer, retries never duplicate effects, partial failures compensate cleanly with no corrupted state, and the dashboard is extended with workflow/state diagrams and the "why Temporal" rationale.
**Depends on**: Phase 1.1 (microservice re-architecture — scheduling/visit/billing are built as their own services)
**Requirements**: SCHED-01, SCHED-02, SCHED-03, SCHED-04, SCHED-05, SCHED-06, SCHED-07, SCHED-08, VISIT-01, VISIT-02, VISIT-03, VISIT-04, VISIT-05, BILL-01, BILL-02
**Success Criteria** (what must be TRUE):

  1. A patient/front-desk user can book an appointment against an available slot, and the appointment is marked confirmed only after the full Temporal saga (validate → reserve slot → prevent conflicts → schedule notification → billing pre-check → confirm) completes successfully.
  2. Under a concurrent load test, parallel booking attempts against the same provider slot never produce more than one confirmed appointment — enforced by a Postgres-level constraint, not application logic alone.
  3. Retried/duplicate booking requests (same idempotency key) never create a second appointment, and visit-state transitions (checked-in → in-progress → complete) are safely repeatable without duplicate side effects.
  4. A patient/front-desk user can reschedule (old slot released and new slot reserved atomically) or cancel (slot released, billing/refund state updated) an appointment, and a freed slot correctly advances the waitlist to the next matching patient.
  5. A deliberately-failed booking (e.g., billing pre-check failure) leaves no corrupted or half-true state — compensating actions run as forward transactions (release, reverse, notify) preserving full audit history, never a hard delete or silent rollback.
  6. Front-desk can check in a confirmed appointment and advance it through visit states in real time queryable by ops staff, and completing a visit triggers downstream billing/follow-up/analytics work without blocking the transition; the dashboard now shows booking/visit workflow diagrams and the Temporal rationale.

**Plans:** 12 plans (sequential waves — shared working tree and shared live test infrastructure)

**Execution status (2026-10-13):** all 12 plans executed (each has a SUMMARY). Phase NOT yet complete: phase verification — the full non-e2e regression, the deferred tests listed in the 02-1x SUMMARY files and the 02-12 human dashboard check — is pending by user request.

**Wave 1**

- [x] 02-01-PLAN.md — Internal auth foundations: Identity client-credentials service-token mint, per-audience `aud` + required `exp`, allowed-caller checks, write-capable internal client, idempotent audit writer (SCHED-01/02/08)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 02-02-PLAN.md — Scheduling data layer: appointments/idempotency/history tables, active-slot unique index, patient-overlap EXCLUDE, one-transaction accept, retry-safe state steps, 1-of-50 concurrency proof (SCHED-03/04/08)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 02-03-PLAN.md — Internal reference lookups: Patient internal router, Provider availability + by-user, clinic IANA timezone (SCHED-01, VISIT-02)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 02-04-PLAN.md — Slot supply: DST-safe on-demand generation over a 28-day horizon, search, withdrawal, Scheduling gateway routes, shared seed helper (SCHED-01)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 02-05-PLAN.md — Billing service: deterministic pre-check and refund/finalize state machine, visit_db/billing_db provisioning, gateway row (BILL-01/02)

**Wave 6** *(blocked on Wave 5 completion)*

- [x] 02-06-PLAN.md — Temporal booking saga: temporalio checkpoint + pin, async 202 booking API, BookingWorkflow with reverse-order compensation, scheduling worker, contract/trace tests (SCHED-01/02/04, BILL-01)

**Wave 7** *(blocked on Wave 6 completion)*

- [x] 02-07-PLAN.md — Failure hardening: forced-billing-failure compensation, hold reaper + pending-starter Schedules, HTTP single-winner proof, history replay, worker-crash e2e (SCHED-03/04/08)

**Wave 8** *(blocked on Wave 7 completion)*

- [x] 02-08-PLAN.md — Cancel and reschedule workflows: cutoff refund policy, 409 while pending, atomic reserve-then-swap with billing re-link (SCHED-05/06, BILL-02)

**Wave 9** *(blocked on Wave 8 completion)*

- [x] 02-09-PLAN.md — Waitlist: FIFO SKIP LOCKED claim, auto-book via child BookingWorkflow, failure skip and lost-race put-back, cancel/reschedule advance behind `workflow.patched` with replay proof (SCHED-07)

**Wave 10** *(blocked on Wave 9 completion)*

- [x] 02-10-PLAN.md — Visit service: check-in against Scheduling's internal appointment lookup, duplicate-safe state machine, provider ownership, clinic-scoped visit list (VISIT-01/02/03/05)

**Wave 11** *(blocked on Wave 10 completion)*

- [x] 02-11-PLAN.md — Non-blocking visit completion: VisitCompletedWorkflow (billing finalize, follow-up/analytics intents), visit worker, sweeper Schedule + run-on-start (VISIT-04, BILL-02)

**Wave 12** *(blocked on Wave 11 completion)*

- [x] 02-12-PLAN.md — Compose wiring for Billing/Visit APIs and both workers, real-stack e2e flows, one-command concurrent-booking demo, docs + dashboard v3, requirement-coverage gate, human dashboard check (all 15)

### Phase 02.1: Week 1-2 mentor review remediation (INSERTED)

**Goal:** Close every gap raised in the mentor's Week 1 + Week 2 review before Phase 3 starts, without losing the praised design (database-enforced races, disciplined saga, layered idempotency, `workflow.patched` plus replay tests): the appointment state machine is complete (checked_in, completed, no_show, check-in window, no cancel or reschedule after check-in); provider schedule changes withdraw future slots and flag affected bookings; failures after commit never undo a committed booking; stuck states are reconciled and billing keeps retrying; the Week 1 carry-overs are fixed (token rotation and logout, bcrypt off the event loop, case-insensitive email, self-views, pagination); and the PRD carries quantified NFRs with a runnable load test.
**Requirements**: VISIT-01, VISIT-02, VISIT-03, VISIT-04, SCHED-01, SCHED-02, SCHED-05, SCHED-06, SCHED-07, SCHED-08, BILL-02, IAM-01, IAM-02, IAM-03, PPL-03, PPL-04, ORG-01, DASH-02, DASH-04 (re-hardened), plus mentor-review items M1, M2, S3, S4, S5, S6, S7, C1, C2, C3, C4, C5, D1, D2; this phase also defines NFR-01..NFR-08 in REQUIREMENTS.md (targets only; measuring them is Phase 3)
**Depends on:** Phase 2
**Success Criteria** (what must be TRUE):

  1. An appointment moves confirmed -> checked_in (inside a time window around the slot, decided by the database clock) -> completed, and a no-show becomes no_show; once checked in, cancel and reschedule are refused at the API and in the workflows, and the slot stays reserved (M1).
  2. Editing or deactivating an availability template, or deactivating a provider, withdraws the affected future slots and flags already-booked appointments through a durable, replay-recorded schedule-change workflow (M2).
  3. A compensation never undoes a confirmed booking, an audit failure after commit is non-fatal, the booking notification is requested only after confirm, past slots cannot be booked, and a freed slot is offered to the waitlist again after a direct booking fails (S3, S5, S6, S7).
  4. Reconcilers settle pending appointments whose workflow died, waitlist entries stuck in matched, orphaned billing and unsettled visits, and visit billing keeps retrying instead of being skipped (S4).
  5. Refresh tokens are single-use and logout kills them, bcrypt runs off the event loop, emails are case-insensitive, patients and providers can read their own data, and the three list endpoints are paginated (C1..C5).
  6. The PRD has quantified NFRs (throughput, latency, availability, durability, contention) with a single numeric source, and a k6 load test and seed script exist (D1, D2).

**Plans:** 5/13 plans executed (waves ordered by file ownership; plans in one wave touch disjoint files and databases)

**Wave 1**

- [x] 02.1-01-appointment-state-machine-PLAN.md — TRACER: checked_in/completed/no_show states, widened slot constraints, DB-clock check-in window, internal check-in/complete routes, cancel/reschedule refusal (M1)
- [x] 02.1-02-identity-carryovers-PLAN.md — single-use rotated refresh tokens and logout revocation, bcrypt off the event loop, case-insensitive email with a safe migration (C1, C2, C3)
- [x] 02.1-03-list-pagination-PLAN.md — limit/offset, stable order and X-Total-Count on patient, provider and clinic lists (C5)
- [x] 02.1-04-nfr-and-load-test-PLAN.md — NFR-01..08 in the PRD, docs/nfr.md, single-source nfr.json, load-test seed script and k6 profiles (D1, D2)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 02.1-05-provider-schedule-change-feed-PLAN.md — Provider schedule-change event table written atomically with each change, plus the internal feed and acknowledgement (M2)
- [ ] 02.1-06-visit-checkin-integration-PLAN.md — Visit check-in moves the appointment to checked_in first, window refusals, retry convergence (M1)
- [ ] 02.1-07-booking-saga-reliability-PLAN.md — patched BookingWorkflow (notify after confirm, waitlist re-offer), state-guarded compensations, non-fatal post-commit audits, past-slot rejection (S3, S5, S6, S7)

**Wave 3** *(blocked on Wave 2 completion)*

- [ ] 02.1-08-schedule-change-workflow-PLAN.md — poller Schedule and ScheduleChangeWorkflow withdraw stale slots, flag booked appointments, second pass for held slots (M2)
- [ ] 02.1-09-visit-completion-sync-PLAN.md — patched fourth branch completes the appointment, settlement markers in visit_db (M1)

**Wave 4** *(blocked on Wave 3 completion)*

- [ ] 02.1-10-visit-billing-reconciler-PLAN.md — open-ended visit billing retry behind workflow.patched and a visit reconciler Schedule (S4)
- [ ] 02.1-11-scheduling-reconciler-PLAN.md — reconciler for dead pending bookings, stuck waitlist entries, orphaned billing and no-shows; reaper offers freed slots to the waitlist (S4, S5, M1)
- [ ] 02.1-12-self-service-views-PLAN.md — GET /api/patients/me and provider read access to own appointments (C4)

**Wave 5** *(blocked on Wave 4 completion)*

- [ ] 02.1-13-docs-closeout-PLAN.md — README, architecture doc, ERD, dashboard and decision log updated; review-item traceability test; full non-e2e regression (all items)

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
| 1. Foundation + Core Services | 7/7 | Complete    | 2026-09-10 |
| 2. Scheduling + Service Workflow | 12/12 | Executed — verification pending | - |
| 2.1. Week 1-2 mentor review remediation | 5/13 | In Progress|  |
| 3. Event-Driven System + Observability | 0/TBD | Not started | - |
