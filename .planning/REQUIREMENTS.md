# Requirements: SmartHealth (Part A)

**Defined:** 2026-09-09
**Core Value:** Booking, updating, and completing an appointment is reliable and consistent under load — partial failures never corrupt scheduling state, and every operational data model stays accurate, durable, and queryable.

## v1 Requirements

Requirements for the Part A milestone (core operational platform). Each maps to a roadmap phase.

### Identity & Access (IAM)

- [x] **IAM-01**: User can register and authenticate with a role of patient, provider, front-desk staff, or admin
- [x] **IAM-02**: System enforces role-based access control so each role can only perform its permitted actions (e.g., front-desk manages appointments but cannot access admin-only operations)
- [x] **IAM-03**: User sessions/tokens are issued on login and validated on every protected request
- [x] **IAM-04**: Every mutating profile and operational action is recorded in an audit trail (who, what, when) with no PHI in logs

### Patient & Provider Management (PPL)

- [x] **PPL-01**: Front-desk/admin can register a patient and manage their profile
- [x] **PPL-02**: Admin can register a provider with one or more specialties
- [x] **PPL-03**: Provider (or admin) can configure provider availability windows and working hours
- [x] **PPL-04**: Users can retrieve patient and provider records with consistent, up-to-date state

### Clinic & Department Management (ORG)

- [x] **ORG-01**: Admin can create and manage clinics
- [x] **ORG-02**: Admin can create and manage departments within a clinic
- [x] **ORG-03**: Providers and their schedulable slots are scoped to a clinic/department

### Appointment Scheduling (SCHED)

- [ ] **SCHED-01**: Patient/front-desk can book an appointment against an available provider slot
- [ ] **SCHED-02**: Booking runs as a durable Temporal workflow (validate eligibility → reserve slot → prevent conflicts → schedule notifications → billing pre-check → confirm); appointment is marked confirmed only after successful processing
- [ ] **SCHED-03**: Concurrent booking attempts for the same slot cannot double-book (enforced at the data layer, not only in application code)
- [ ] **SCHED-04**: Booking is idempotent — a retried/duplicate booking request does not create a second appointment
- [x] **SCHED-05**: Patient/front-desk can reschedule an appointment, releasing the old slot and reserving the new one atomically
- [ ] **SCHED-06**: Patient/front-desk can cancel an appointment, which releases the slot and triggers billing/refund updates
- [ ] **SCHED-07**: On a freed slot, the waitlist advances and matches the next appropriate patient
- [ ] **SCHED-08**: A partial failure mid-booking leaves no corrupted state (compensation/rollback of forward steps)

### Visit & Service Lifecycle (VISIT)

- [ ] **VISIT-01**: Front-desk can record patient check-in for a confirmed appointment
- [ ] **VISIT-02**: Provider/front-desk can advance a visit through states (checked-in → in-progress → complete)
- [ ] **VISIT-03**: Visit state transitions are idempotent and duplicate-safe
- [ ] **VISIT-04**: Completing a visit triggers downstream workflows (billing, follow-up scheduling, analytics) without blocking the transition
- [ ] **VISIT-05**: Current visit status is queryable in real time for ops staff

### Billing Pre-Check (BILL)

- [ ] **BILL-01**: Booking initiates a billing pre-check that records an eligibility/status result (pending → verified/failed) — modeled internally, no live payment gateway
- [ ] **BILL-02**: Cancellation/reschedule updates the associated billing/refund state

### Notifications (NOTIF)

- [ ] **NOTIF-01**: Booking, cancellation, and reschedule each trigger a patient notification, delivered asynchronously (never blocking the booking transaction)
- [ ] **NOTIF-02**: Appointment reminders are scheduled and delivered ahead of the appointment
- [ ] **NOTIF-03**: Visit completion can schedule a follow-up reminder
- [ ] **NOTIF-04**: Notification delivery is retried on failure and does not double-send on retry

### Event-Driven Backbone & Background Processing (EVENT)

- [ ] **EVENT-01**: Domain events (appointment booked/cancelled/rescheduled, provider schedule change, visit completed, analytics) are published to Kafka with a registered schema
- [ ] **EVENT-02**: Events reach Kafka via a transactional outbox so a database commit and its event are never out of sync (no dual-write)
- [ ] **EVENT-03**: Event consumers are idempotent and safe against duplicate/at-least-once delivery
- [ ] **EVENT-04**: Background tasks (notifications, follow-ups, projections) run on Celery + RabbitMQ, independently from user-facing request flows
- [ ] **EVENT-05**: Failed background tasks are retried with a dead-letter path for poison messages

### Analytics (ANLY)

- [ ] **ANLY-01**: System reports total patients
- [ ] **ANLY-02**: System reports appointments booked over time
- [ ] **ANLY-03**: System reports completed visits
- [ ] **ANLY-04**: System reports cancellation rate
- [ ] **ANLY-05**: System reports average wait time
- [ ] **ANLY-06**: Analytics are derived from the event stream (not by polling the transactional database on the hot path)

### Observability & Reliability (OBS)

- [ ] **OBS-01**: All services expose Prometheus metrics surfaced in Grafana dashboards for critical flows
- [ ] **OBS-02**: Requests and workflows are traced end-to-end with OpenTelemetry, viewable in Jaeger, including trace-context propagation across async boundaries (Kafka, Celery, Temporal)
- [ ] **OBS-03**: Structured logs exist for booking, availability sync, billing, notifications, and background workers, enabling failure diagnosis
- [x] **OBS-04**: The full stack runs locally via Docker Compose with health checks

### Architecture & Decision Dashboard (DASH)

- [x] **DASH-01**: A self-contained HTML dashboard presents the system architecture diagram
- [x] **DASH-02**: The dashboard documents each major technology/design decision with alternatives considered and the reasoning for the choice
- [ ] **DASH-03**: The dashboard shows the key data and event flows (booking, cancel/reschedule, visit lifecycle, notifications, analytics)
- [ ] **DASH-04**: The dashboard includes an anticipated evaluation Q&A section with prepared answers, and is updated at each phase boundary

## v2 Requirements

Deferred within Part A — add once the P1 flows are solid.

### Scheduling Enhancements

- **SCHEDX-01**: Capacity-aware scheduling — reserved urgent/same-day slot blocks on provider schedules
- **SCHEDX-02**: Intelligent overbooking driven by a simple statistical no-show heuristic (no ML/LLM), using historical analytics data

## Out of Scope

Explicitly excluded to prevent scope creep.

| Feature | Reason |
|---------|--------|
| GenAI assistant, RAG, embeddings/vector search, AI evals (Part B) | Deferred to the next milestone; Part A must be solid first. Design data/event model so Part B can layer on later. |
| Live payment gateway (Stripe/etc.) integration | Adds PCI scope + vendor dependency irrelevant to Part A's reliability/architecture goals; billing is modeled as internal state only. |
| Full HIPAA compliance certification | Learning build — security fundamentals (AuthN/AuthZ, audit trail, no PHI in logs) are in scope; formal certification is not. |
| Clinical documentation / e-prescribing / lab orders | This is an operations platform, not an EHR clinical module. |
| Telemedicine video/streaming infrastructure | No requirement for live video; telemedicine is modeled only as a visit type. |
| External calendar federation (Google/Outlook OAuth sync) | "Calendar synchronization" means internal provider-availability sync, not third-party integration. |
| Patient-facing mobile/native app UI | Part A is graded on backend architecture; clean APIs + the eval dashboard suffice. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| IAM-01 | Phase 1 | Complete |
| IAM-02 | Phase 1 | Complete |
| IAM-03 | Phase 1 | Complete |
| IAM-04 | Phase 1 | Complete |
| PPL-01 | Phase 1 | Complete |
| PPL-02 | Phase 1 | Complete |
| PPL-03 | Phase 1 | Complete |
| PPL-04 | Phase 1 | Complete |
| ORG-01 | Phase 1 | Complete |
| ORG-02 | Phase 1 | Complete |
| ORG-03 | Phase 1 | Complete |
| OBS-04 | Phase 1 | Complete |
| DASH-01 | Phase 1 | Complete |
| DASH-02 | Phase 1 | Complete |
| SCHED-01 | Phase 2 | Pending |
| SCHED-02 | Phase 2 | Pending |
| SCHED-03 | Phase 2 | Pending |
| SCHED-04 | Phase 2 | Pending |
| SCHED-05 | Phase 2 | Complete |
| SCHED-06 | Phase 2 | Pending |
| SCHED-07 | Phase 2 | Pending |
| SCHED-08 | Phase 2 | Pending |
| VISIT-01 | Phase 2 | Pending |
| VISIT-02 | Phase 2 | Pending |
| VISIT-03 | Phase 2 | Pending |
| VISIT-04 | Phase 2 | Pending |
| VISIT-05 | Phase 2 | Pending |
| BILL-01 | Phase 2 | Pending |
| BILL-02 | Phase 2 | Pending |
| EVENT-01 | Phase 3 | Pending |
| EVENT-02 | Phase 3 | Pending |
| EVENT-03 | Phase 3 | Pending |
| EVENT-04 | Phase 3 | Pending |
| EVENT-05 | Phase 3 | Pending |
| NOTIF-01 | Phase 3 | Pending |
| NOTIF-02 | Phase 3 | Pending |
| NOTIF-03 | Phase 3 | Pending |
| NOTIF-04 | Phase 3 | Pending |
| ANLY-01 | Phase 3 | Pending |
| ANLY-02 | Phase 3 | Pending |
| ANLY-03 | Phase 3 | Pending |
| ANLY-04 | Phase 3 | Pending |
| ANLY-05 | Phase 3 | Pending |
| ANLY-06 | Phase 3 | Pending |
| OBS-01 | Phase 3 | Pending |
| OBS-02 | Phase 3 | Pending |
| OBS-03 | Phase 3 | Pending |
| DASH-03 | Phase 3 | Pending |
| DASH-04 | Phase 3 | Pending |

**Coverage:**

- v1 requirements: 49 total
- Mapped to phases: 49/49
- Unmapped: 0

**v2 (deferred, not mapped to Part A phases):** SCHEDX-01, SCHEDX-02

---
*Requirements defined: 2026-09-09*
*Last updated: 2026-09-09 after roadmap creation — 49/49 v1 requirements mapped across Phase 1 (14), Phase 2 (15), Phase 3 (20); corrected v1 total from stale count of 45 to actual 49 (v2 items SCHEDX-01/02 counted separately, not in v1 total)*
