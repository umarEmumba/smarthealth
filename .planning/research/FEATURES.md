# Feature Research

**Domain:** Healthcare operations & appointment-scheduling platform (Part A — operational backend, no GenAI)
**Researched:** 2026-09-09
**Confidence:** MEDIUM

Scope note: This document covers Part A only — patient/provider/staff management, scheduling, visit lifecycle, billing pre-check, notifications, and operational analytics. Part B (GenAI: parsing/ingestion, embeddings, RAG, AI assistant, evals) is explicitly out of scope and excluded from every table below.

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist in any credible healthcare scheduling/ops platform. Missing these makes the platform feel broken or unsafe to operate.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Patient registration & profile management | Baseline entity every downstream workflow (scheduling, visits, billing) depends on | LOW | CRUD + validation; PostgreSQL system-of-record per PROJECT.md |
| Provider registration & specialty/schedule management | Providers are the supply side of scheduling; specialty drives eligibility matching | LOW-MEDIUM | Includes availability windows, working hours, specialties |
| Role-based access (patient, provider, front-desk, admin) | Non-negotiable in any multi-actor healthcare system; wrong-role access is a core failure mode, not a nice-to-have | MEDIUM | RBAC maps role → permitted actions (e.g., front-desk creates encounters/verifies eligibility but has no clinical-note access); this is industry standard, not a differentiator |
| Audit trail for profile & operational changes | Expected for any system touching operational/patient data even without full HIPAA certification (per PROJECT.md, "security fundamentals... in scope") | MEDIUM | Who/what/when on every mutating action; feeds the observability story too |
| Department & clinic management | Multi-clinic is explicit in scope; scheduling/analytics need a clinic/department dimension to be queryable | LOW-MEDIUM | Hierarchical: clinic → department → provider |
| Real-time slot booking with conflict prevention | The single most-cited "must have" across scheduling platforms — real-time conflict detection blocks booking into an already-occupied slot | HIGH | This is the core reliability challenge in PROJECT.md; requires slot reservation + locking (Redis) and orchestration (Temporal) to avoid double-booking under concurrency |
| Reschedule & cancel with slot release | Table stakes alongside booking — a scheduler that can create but not safely mutate/cancel appointments is incomplete | MEDIUM-HIGH | Must trigger slot release, waitlist movement, and notification — same reliability bar as booking |
| Waitlist management | Waitlist tooling automatically matches the next appropriate patient to a newly-opened slot rather than requiring manual staff work through a list | MEDIUM | Explicit in requirements; ties into cancellation/reschedule event flow |
| Appointment reminders / notifications (booking, cancellation, follow-up) | Multi-channel reminders sent at strategic intervals are consistently cited as the top no-show-reduction lever | MEDIUM | Background/async (Celery+RabbitMQ per stack); do not block the booking transaction on notification delivery |
| Check-in → in-progress → complete visit lifecycle | Table stakes for any system claiming to manage "visits," not just bookings — front-desk and providers need real status visibility | MEDIUM | State machine with idempotent transitions; explicit in requirements |
| Billing pre-check (eligibility/copay awareness) | Practice-management systems universally check eligibility and copay/deductible info at or near booking time | MEDIUM | Modeled workflow only — no live payment gateway (explicit anti-feature, see below) |
| Basic operational analytics (patients, appointments over time, completed visits, cancellation rate, avg wait time) | These five metrics are the industry-standard KPI set for clinic operations (no-show rate, wait time, volume trends) — practically universal on any clinic ops dashboard | MEDIUM | Explicit list in requirements; wait time benchmark context: <10 days new-patient non-urgent, 3–5 days existing patient (useful only as a modeling reference, not a target to hit) |
| Idempotent retries / duplicate prevention on workflows | Any system processing background jobs and distributed events must guard against duplicate processing — treated as baseline, not advanced, in modern distributed systems | HIGH | Core to the "no corrupted scheduling state" value prop in PROJECT.md |
| Observability (logs, metrics, tracing) for critical flows | Table stakes for any "production-grade" claim, and explicitly graded per PROJECT.md | MEDIUM-HIGH | Prometheus/Grafana + Jaeger + OpenTelemetry mandated by stack |

### Differentiators (Competitive Advantage — within Part A's operational scope)

These aren't required for a workable platform, but this project is explicitly graded on architecture/reliability depth, so the differentiation for Part A is in **how robustly** table-stakes features are engineered — not in additional patient-facing bells and whistles.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Temporal-orchestrated booking saga (validate → reserve → prevent conflicts → schedule notifications → billing pre-check → confirm) | Durable, resumable, compensable multi-step workflow — most competing systems handle this with fragile synchronous code or ad-hoc queues, not a durable workflow engine | HIGH | This is the single biggest architectural differentiator called out in PROJECT.md; showcases mentor-graded "reliability & failure handling" |
| Event-driven backbone (Kafka + Schema Registry) decoupling booking/visit/analytics from user-facing flow | Enables replay, cross-service consistency, and analytics-without-blocking-the-critical-path — most small/mid scheduling tools are monolithic and don't have this | HIGH | Differentiator specifically because Part A intentionally over-engineers for the learning/scale narrative (tens of thousands of requests, multi-clinic) |
| Intelligent overbooking guided by no-show-probability signal | Practices using probability-informed double-booking see materially higher slot utilization (~18%) without raising patient wait times | MEDIUM-HIGH | Legitimate differentiator vs naive overbooking; requires historical no-show data — feasible with Part A's own analytics data, no GenAI needed (simple heuristic/statistical scoring, not ML/LLM) |
| Architecture & Decision dashboard (self-contained HTML) | Not a market feature at all — it's a differentiator for *this specific graded assignment*: a living, portable record of design reasoning for mentor review | LOW-MEDIUM | Explicit requirement in PROJECT.md; unusual for a "product" but core deliverable here |
| Real-time status visibility into visit/service progress | Front-desk and ops staff seeing check-in → in-progress → complete update live (not on refresh) raises perceived operational maturity | MEDIUM | Achievable via the event backbone already in scope; don't build a separate real-time transport if the event bus + polling suffices |
| Capacity-aware scheduling (reserved same-day/urgent slot blocks) | Reserving a portion of daily capacity for urgent/same-day appointments is called out repeatedly as a best practice that most basic schedulers skip | LOW-MEDIUM | Cheap to model as a slot-type/tag on provider schedule config; strong "we thought about real operations" signal for mentor review |

### Anti-Features (Explicitly Out of Scope for Part A)

Features that seem good, are common in mature commercial products, or are tempting scope-creep — but must NOT be built in this milestone.

| Feature | Why Requested / Tempting | Why Problematic for Part A | Alternative |
|---------|---------------------------|------------------------------|-------------|
| GenAI patient assistant, RAG retrieval, embeddings/vector search, AI evals | Modern healthcare platforms increasingly market conversational AI; feels like the "interesting" part | Explicitly deferred to Part B per PROJECT.md; building it now dilutes focus from the graded architecture/reliability deliverable and introduces unrelated infra (vector DB, LLM ops) | Design Part A's event/data model so Part B can layer on top later (e.g., don't foreclose future embedding pipelines), but implement none of it now |
| Live payment gateway integration (Stripe/etc.) | "Billing" sounds incomplete without real payment processing | Explicit anti-feature in PROJECT.md; adds PCI-scope, external vendor dependency, and compliance surface irrelevant to the reliability/architecture learning goals | Model billing pre-check and status transitions (pending/verified/failed) as internal state only; stub/mock the payment step |
| Full HIPAA compliance certification program | Healthcare data naturally raises "we must be fully compliant" instinct | PROJECT.md explicitly scopes this as a learning build: security fundamentals (authn/authz, audit trail) yes, full compliance certification no | Implement RBAC + audit trail + no PHI in logs (per shared HIPAA_ENGINEERING_RULES/PHI_HANDLING_RULES baseline) without pursuing formal certification artifacts |
| Deep EHR/clinical documentation (SOAP notes, diagnosis coding, e-prescribing, lab orders) | Real EHR/PM systems bundle clinical documentation with scheduling — feels natural to add "just a bit" of clinical charting | Explodes scope far beyond "operations & scheduling backbone"; introduces clinical-data complexity (ICD/CPT coding, e-prescribing regulations) with zero mention in requirements | Keep the data model scoped to visit *lifecycle status* (checked-in/in-progress/complete) and service metadata only — not clinical content |
| Telemedicine video/streaming infrastructure | Requirements mention "telemedicine providers" as a customer segment, tempting to build video calling | No functional requirement calls for live video; it's a different engineering problem (media servers, WebRTC) unrelated to the scheduling/reliability focus | Model telemedicine visits as just another visit *type* (in-person vs virtual) in the data model — no actual video transport |
| Patient-facing mobile app / native UI polish | "Patients should be able to book..." reads like a demand for a full consumer app | Requirements describe *capabilities*, not a mobile product; Part A is graded on backend architecture, not UX/frontend craft | Expose clean APIs; a minimal reference UI or API-driven demo (e.g., via the eval dashboard or simple test client) is enough to prove the flows work |
| Real-time two-way third-party calendar sync (Google/Outlook) | Common expectation in general-purpose scheduling tools; "calendar synchronization" is even named in requirements as an internal trigger | Requirements' "calendar synchronization" refers to internal provider-availability sync across SmartHealth's own services, not external calendar federation — building OAuth calendar integrations is unrelated third-party-API scope | Treat "calendar sync" as internal: provider schedule state propagating consistently across SmartHealth's own read models/caches |
| ML/LLM-based predictive no-show scoring | Tempting once you have historical booking data, and adjacent to the "intelligent overbooking" differentiator above | Any ML/LLM modeling belongs to Part B's GenAI layer per explicit out-of-scope boundary; even "simple ML" risks scope creep into AI territory this milestone forbids | If pursued at all, keep it a simple rule-based/statistical heuristic (e.g., historical no-show rate by patient/slot-type) computed in the analytics pipeline — not a model-serving component |

## Feature Dependencies

```
Patient/Provider/Staff Management (RBAC + entities)
    └──requires──> none (foundational)

Department & Clinic Management
    └──requires──> none (foundational, parallel to above)

Appointment Scheduling (book/reschedule/cancel)
    └──requires──> Patient/Provider Management (need patient + provider records)
    └──requires──> Department & Clinic Management (slots belong to a provider within a clinic/department)
    └──requires──> Slot Reservation & Conflict Prevention (Redis locks + Temporal orchestration)

Waitlist Management
    └──requires──> Appointment Scheduling (waitlist exists to fill slots freed by cancel/reschedule)

Billing Pre-Check
    └──requires──> Appointment Scheduling (pre-check is a step inside the booking saga)

Notifications (reminders/cancellations/follow-ups)
    └──requires──> Appointment Scheduling (notification triggers are booking/cancel/reschedule events)
    └──requires──> Event-Driven Backbone (Kafka) OR Background Workers (Celery/RabbitMQ) for async delivery

Visit & Service Lifecycle (check-in → in-progress → complete)
    └──requires──> Appointment Scheduling (a visit originates from a confirmed appointment)

Operational Analytics (patients, appointments over time, completed visits, cancellation rate, avg wait time)
    └──requires──> Appointment Scheduling (source of booking/cancellation events)
    └──requires──> Visit & Service Lifecycle (source of completion/wait-time events)
    └──requires──> Event-Driven Backbone (analytics consumes events, doesn't poll transactional DB directly)

Observability (Prometheus/Grafana/Jaeger/OTel)
    └──enhances──> all workflows above (cross-cutting, not a dependency gate)

Capacity-aware scheduling (reserved urgent/same-day slots) ──enhances──> Appointment Scheduling
Intelligent overbooking (no-show-probability heuristic) ──enhances──> Appointment Scheduling
    └──requires──> Operational Analytics (needs historical no-show data to compute the heuristic)

GenAI features (Part B) ──conflicts──> Part A milestone scope (explicitly deferred, do not build)
Live payment gateway ──conflicts──> Part A milestone scope (explicitly deferred, do not build)
```

### Dependency Notes

- **Appointment Scheduling requires Patient/Provider Management and Department & Clinic Management:** you cannot reserve a slot for an entity (provider, clinic) that doesn't exist yet — these must land in an earlier phase.
- **Appointment Scheduling requires Slot Reservation & Conflict Prevention as a first-class sub-capability:** this is not a "feature" bolted on later, it's the reliability core of the booking saga (validate → reserve → prevent conflicts → notify → billing pre-check → confirm) called out in both PROJECT.md and requirements. Building booking without this is building the wrong thing.
- **Waitlist Management requires Appointment Scheduling:** waitlist movement is triggered by cancel/reschedule events from the scheduling engine; it cannot be built or tested standalone.
- **Notifications requires either the event backbone or background workers:** reminders/cancellations/follow-ups must be async and non-blocking relative to the booking transaction — this is both a feature dependency and an architectural constraint from PROJECT.md ("partial failures must not corrupt scheduling state").
- **Operational Analytics requires both Scheduling and Visit Lifecycle as event sources:** cancellation rate needs scheduling events, average wait time needs visit check-in/complete timestamps — analytics cannot be meaningfully built until both upstream flows emit events.
- **Intelligent overbooking (differentiator) requires Operational Analytics:** the no-show-probability signal is derived from historical analytics data; do not attempt this before the analytics pipeline exists, and do not conflate it with GenAI/ML — this must remain a simple statistical/heuristic feature, per the anti-feature note above.
- **GenAI and live payment gateway conflict with Part A scope entirely:** any pull toward implementing these mid-milestone should be treated as scope creep and rejected — they are explicitly deferred/out-of-scope in PROJECT.md.

## MVP Definition

### Launch With (v1 — Part A must-have)

- [ ] Patient/provider/staff registration with RBAC (patient, provider, front-desk, admin) — foundational, everything depends on it
- [ ] Department & clinic management — needed to scope providers/slots/analytics
- [ ] Appointment booking with slot reservation, conflict prevention, and orchestrated multi-step confirmation (Temporal) — this is the core value proposition in PROJECT.md
- [ ] Reschedule/cancel with slot release and waitlist movement — booking without safe mutation is incomplete
- [ ] Visit lifecycle (check-in → in-progress → complete) with idempotent, duplicate-safe transitions — explicit requirement, also the second reliability showcase
- [ ] Billing pre-check workflow (modeled, no live gateway) — explicit requirement, keep scope to state transitions only
- [ ] Notifications for booking/cancellation/reminders/follow-ups via background workers — explicit requirement, essential for perceived completeness
- [ ] Event-driven backbone (Kafka+Schema Registry) for booking/cancellation/reschedule/visit/analytics events — mandated stack, also unlocks analytics and future extensibility
- [ ] Core analytics (5 metrics: total patients, appointments over time, completed visits, cancellation rate, avg wait time) — explicit requirement
- [ ] Audit trail for profile & operational changes — expected baseline, also feeds observability/compliance story
- [ ] Observability stack wired into all critical flows (Prometheus/Grafana, Jaeger, OTel) — explicitly graded evaluation criterion
- [ ] Architecture & Decision dashboard (self-contained HTML) — explicit deliverable, living document updated per phase

### Add After Core Is Working (v1.x, still within Part A)

- [ ] Capacity-aware scheduling (reserved urgent/same-day slot blocks) — cheap enhancement once basic slot model exists
- [ ] Intelligent overbooking heuristic based on historical no-show data — needs analytics pipeline in place first, keep it simple/statistical

### Future Consideration (Part B+ — explicitly deferred)

- [ ] GenAI data ingestion/parsing, embeddings + vector search, RAG retrieval, patient AI assistant, AI evals — Part B, entire milestone deferred
- [ ] Live payment gateway integration — explicitly out of scope per PROJECT.md, revisit only if a future milestone requires real transactions
- [ ] Third-party calendar federation (Google/Outlook OAuth sync) — not requested in requirements; only internal availability sync is in scope
- [ ] Clinical documentation / e-prescribing / lab orders — out of scope, this is an operations platform, not an EHR clinical module

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|----------------------|----------|
| RBAC + patient/provider/staff management | HIGH | MEDIUM | P1 |
| Department & clinic management | MEDIUM | LOW | P1 |
| Appointment booking (Temporal saga, conflict prevention) | HIGH | HIGH | P1 |
| Reschedule/cancel + waitlist | HIGH | MEDIUM-HIGH | P1 |
| Visit lifecycle (check-in/in-progress/complete) | HIGH | MEDIUM | P1 |
| Billing pre-check (modeled) | MEDIUM | MEDIUM | P1 |
| Notifications (reminders/cancellations/follow-ups) | HIGH | MEDIUM | P1 |
| Event-driven backbone (Kafka) | HIGH (architectural) | HIGH | P1 |
| Operational analytics (5 metrics) | MEDIUM-HIGH | MEDIUM | P1 |
| Audit trail | MEDIUM (HIGH for grading) | MEDIUM | P1 |
| Observability (Prometheus/Grafana/Jaeger/OTel) | HIGH (graded criterion) | MEDIUM-HIGH | P1 |
| Architecture & Decision dashboard | HIGH (graded deliverable) | LOW | P1 |
| Capacity-aware scheduling (urgent/same-day reserve) | MEDIUM | LOW | P2 |
| Intelligent overbooking (no-show heuristic) | MEDIUM | MEDIUM | P2 |
| GenAI assistant / RAG / embeddings | HIGH (future) | HIGH | P3 (Part B) |
| Live payment gateway | LOW (for this milestone) | MEDIUM | P3 (deferred) |
| Calendar federation (external) | LOW | MEDIUM | P3 (not requested) |
| Clinical documentation module | LOW (out of scope) | HIGH | P3 (reject) |

**Priority key:**
- P1: Must have for Part A milestone
- P2: Should have, add once P1 flows are solid (still within Part A)
- P3: Deferred to a future milestone or explicitly rejected

## Competitor Feature Analysis

Note: "competitors" here means mature commercial healthcare scheduling/PM platforms used as a feature-completeness reference, not direct products to clone.

| Feature | Typical Commercial EHR/PM Platform (e.g., NexHealth, Practice EHR class) | Consumer Scheduling Tools (e.g., Zocdoc, Acuity class) | Our Approach (Part A) |
|---------|----------------------------------------------------------------------|--------------------------------------------------------|------------------------|
| Slot booking & conflict prevention | Real-time conflict detection, calendar sync tied to clinical schedule | Real-time booking with buffer times, resource checks | Same functional bar, but implemented via durable Temporal saga + Redis locks for correctness under concurrency — architecturally deeper than typical PM tools |
| Waitlist | Manual or semi-automated waitlist matching | Automated waitlist with instant slot-fill on cancellation | Automated waitlist movement triggered by cancellation/reschedule events on the event backbone |
| Notifications/reminders | Multi-channel (SMS/email/voice) reminders bundled | Multi-channel reminders, patient self-service cancel | Async, event/queue-driven reminders and follow-ups (Celery+RabbitMQ); channel choice (email/SMS) is an implementation detail, not the differentiator |
| Billing | Full eligibility verification + claims + payment processing | Often none (booking-only tools) or basic payment collection | Billing *pre-check* and status modeling only — explicitly no live payment processing |
| Analytics | Deep clinical + financial dashboards (revenue cycle, coding) | Minimal (mostly booking volume) | Focused 5-metric operational dashboard (patients, appointments over time, completed visits, cancellation rate, avg wait time) — scoped intentionally narrower than full PM analytics |
| RBAC/audit | Standard, HIPAA-driven, mature | Often weaker (consumer-grade auth) | Full RBAC (patient/provider/front-desk/admin) + audit trail, security fundamentals without full compliance certification |
| AI/GenAI features | Increasingly present (ambient scribing, AI scheduling assistants) in 2026-era products | Some AI-based smart scheduling optimization | Explicitly none in Part A — deferred to Part B by design, not a gap to fill now |

## Sources

- [11 Best Patient Appointment Scheduling Software Solutions | PracticeSuite](https://practicesuite.com/resources/patient-appointment-scheduling-software/)
- [Medical Practice Scheduling Software | Practice EHR](https://www.practiceehr.com/features/scheduling)
- [EHR Scheduling: Appointment Scheduling For Your Patients | NexHealth Resources](https://www.nexhealth.com/resources/ehr-appointment-scheduling)
- [EHR Scheduling Software Features and Benefits - SimplePractice](https://www.simplepractice.com/blog/ehr-scheduling-software/)
- [Patient scheduling: 10 best practices for peak efficiency — Phreesia](https://www.phreesia.com/blog-patient-scheduling-10-best-practices-for-peak-efficiency/)
- [Patient Waitlist Management: Fill Cancellations Fast — Zocdoc](https://www.zocdoc.com/resources/blog/article/how-to-improve-patient-waitlist-management-and-fill-cancellations-faster/)
- [5 Patient scheduling best practices to cut no-shows | Pabau](https://pabau.com/blog/patient-scheduling-and-appointment-management/)
- [Double Booking Scheduling: Strategy Guide for Businesses – Wavetec](https://www.wavetec.com/blog/double-booking-scheduling/)
- [Double booking scheduling: Why strategic booking practices matter | Waitwhile](https://waitwhile.com/blog/double-booking-scheduling/)
- [Overlapping or Double-Booked Appointments: A Hidden Threat to Clinical Efficiency — Staffingly](https://staffingly.com/overlapping-or-double-booked-appointments-a-hidden-threat-to-clinical-efficiency/)
- [Clinic KPI Benchmarks for 2026: Key Metrics to Track — OmniMD](https://omnimd.com/blog/clinic-kpi-benchmarks/)
- [Real-Time Analytics and AI for Managing No-Show Appointments in Primary Health Care (UAE study) — NCBI](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC11729783/)
- [Large-Scale No-Show Patterns and Distributions for Clinic Operational Research — NCBI](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4934549/)
- [Role Based Access Controls — DocVilla](https://www.docvilla.com/roles-and-permission-model/)
- [Role-Based Access Control (RBAC) in Healthcare: Benefits, Examples, and Best Practices — AccountableHQ](https://www.accountablehq.com/post/role-based-access-control-rbac-in-healthcare-benefits-examples-and-best-practices)
- [How Role-Based Controls Protect Patient Data — Censinet](https://censinet.com/perspectives/how-role-based-controls-protect-patient-data)
- Project source docs: `/Users/emumba/smart Health/.planning/PROJECT.md`, `/Users/emumba/smart Health/Smart Health Requirements.md`

---
*Feature research for: Healthcare operations & appointment-scheduling platform (Part A)*
*Researched: 2026-09-09*
