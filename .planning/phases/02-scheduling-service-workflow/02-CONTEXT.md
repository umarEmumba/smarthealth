# Phase 2: Scheduling + Service Workflow - Context

**Gathered:** 2026-10-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Deliver the core reliability requirement: booking, cancelling, rescheduling and completing an appointment are Temporal-orchestrated, safe under concurrent load, idempotent, and leave no corrupted state on partial failure. Scope is SCHED-01..08, VISIT-01..05 and BILL-01..02 from `.planning/REQUIREMENTS.md`, plus extending the Architecture & Decision dashboard with the booking/visit workflow diagrams and the "why Temporal" rationale.

**In scope:** slot generation from availability templates, the booking/cancel/reschedule/waitlist-advance workflows, the visit lifecycle, a modeled billing pre-check and refund state, idempotency, compensation, and the dashboard update.

**Out of scope (Phase 3):** Kafka events, the transactional outbox, Celery/RabbitMQ workers and real notification delivery, event-derived analytics, and full observability dashboards. Capacity-aware scheduling and overbooking (SCHEDX-01/02) are v2. No live payment gateway.

</domain>

<decisions>
## Implementation Decisions

> **How these were chosen:** the user did not select options and delegated ("do what is best practice"). Every decision below is a **recommended default chosen by Claude** and is tagged `[auto-default]`. None is a user-confirmed preference. The user can overrule any of them before or after planning by editing this file and re-running `/gsd-plan-phase 2`. Decisions D-01..D-08 of Phase 1.1 (service cut, database-per-service, sync REST reference checks, IDs-only reads, no Kafka/outbox yet) carry forward unchanged.

### Slot supply and holds
- **D-01 `[auto-default]`:** Scheduling **materializes slots on demand over a rolling horizon** (default 28 days), generated from Provider's availability templates fetched through the existing internal REST client (GET only). Slot length is the template's `slot_duration_minutes`. Template times are interpreted in the clinic's timezone (from Organization) and stored as UTC `timestamptz`. Generation is idempotent (re-running over the same range creates no duplicates and never touches booked or held slots). It is triggered by an admin/front-desk endpoint and by an on-demand extension when a search asks beyond the generated range; no scheduler runs until Celery exists in Phase 3. — **Reversibility:** reversible — only the trigger and horizon change.
- **D-02 `[auto-default]`:** **Double-booking is closed by a partial unique index on active appointments** (`UNIQUE (slot_id) WHERE status IN ('pending','confirmed')`), not by application logic. The existing `ex_provider_slots_no_overlap` exclusion constraint keeps guarding against overlapping slot rows for one provider; the new index guards one slot against two appointments. Slot reservation additionally uses a single conditional `UPDATE ... SET status='held' WHERE id=? AND status='available'` so losers fail fast. The concurrency test must hit a real Postgres and show exactly one winner. — **Reversibility:** reversible — an index swap, no data migration.
- **D-03 `[auto-default]`:** A reserved slot gets a **short hold**: add `held_until` (default 10 minutes) and a `held_by_appointment_id` to `provider_slots`. The saga confirms the hold to `booked` or releases it. A hold that outlives its workflow (for example a worker crash past all retries) is released by a reaper activity driven by a Temporal Schedule or timer, not by cron. Slot statuses stay `available/held/booked/cancelled` as already constrained. — **Reversibility:** reversible — columns and one reaper.

### Service boundaries and Temporal topology
- **D-04 `[auto-default]`:** **Three services own this phase's data, each with its own database:**
  - **Scheduling** (existing skeleton): slots, appointments, waitlist entries, idempotency keys. Appointments live with slots so that "release old slot + book new slot + move the appointment" is one local ACID transaction (the reschedule-atomicity requirement), not a distributed one.
  - **Visit** (new): visits and their state history, in `visit_db`.
  - **Billing** (new): billing pre-checks and refund state, in `billing_db`.

  This matches the roadmap line "scheduling/visit/billing are built as their own services" and the 01.1 preference for finer boundaries. `db-init` and Compose gain `visit_db`/`billing_db`, a role per service, and a container per service. — **Reversibility:** costly — merging or splitting later means moving tables between databases and rewriting internal calls.
- **D-05 `[auto-default]`:** **A workflow lives with the service that owns its aggregate, with one Temporal task queue and one worker container per service** (`scheduling-worker`, `visit-worker`; Billing needs no workflow of its own). Booking, cancel, reschedule and waitlist-advance run on the Scheduling queue. The post-completion fan-out runs on the Visit queue. Activities call other services over the existing REST client; they never touch another service's database. Workers are separate processes from the API containers, so a worker restart does not drop HTTP traffic. — **Reversibility:** costly — task-queue names and workflow types are in running workflow histories.
- **D-06 `[auto-default]`:** **Activities authenticate with service tokens** (the `type=service` token from 01.1-05), because a workflow outlives the HTTP request and has no user context. The caller's JWT is never passed into Temporal. The internal endpoints that activities call must accept service tokens and re-apply clinic scope from explicit parameters. **Workflow inputs, outputs and event history carry IDs and enums only, never PHI.** Workflow IDs are deterministic (`booking-{appointment_id}`, `cancel-{appointment_id}-{n}`, `visit-completed-{visit_id}`) with duplicate-start rejection, so a repeated start is a no-op. Every activity is idempotent: downstream endpoints accept a deterministic idempotency key built from workflow ID and step. The deferred service-token `aud` gap from `01.1-09` should be closed here because Phase 2 widens service-token use. — **Reversibility:** costly — it sets the internal auth contract for all Phase 2 calls.

### Booking contract and failure behavior
- **D-07 `[auto-default]`:** **`POST /appointments` is asynchronous**: it requires an `Idempotency-Key` header, writes the appointment as `pending`, starts the workflow, and returns **202** with the appointment ID and a status URL. Clients poll `GET /appointments/{id}`; the appointment is `confirmed` only after the whole saga succeeds. No synchronous wait mode. — **Reversibility:** costly — a published API contract that clients will code against.
- **D-08 `[auto-default]`:** **Idempotency keys** are stored in Scheduling's Postgres (not Redis, so they survive a cache flush) as `(caller_user_id, key)` unique, with a SHA-256 of the canonicalized request body and the resulting `appointment_id`. The key insert and appointment insert happen in the same transaction, so concurrent duplicates are arbitrated by the unique constraint. Same key and same body returns the original response (no second appointment). Same key with a different body returns **422**. Keys expire after 24 hours (lazy cleanup). The workflow ID adds a second layer of dedupe. — **Reversibility:** costly — key scope and mismatch semantics are client-visible.
- **D-09 `[auto-default]`:** **Booking saga order follows SCHED-02**: validate (patient, provider, slot via REST; clinic scope; all must be active) → reserve slot (hold) → billing pre-check (Billing creates `pending`, then sets `verified` or `failed`) → schedule notification → confirm (slot `booked`, appointment `confirmed`). Notification scheduling is **best-effort**: it retries but its failure never fails or compensates the booking. **Compensation runs as forward transitions**, in reverse order, never deletes: release the hold, void the billing record, set the appointment to `failed` with a machine-readable reason, write an audit entry per step. A failed or compensated booking is fully reconstructable from audit plus the Temporal history. — **Reversibility:** costly — step order and state names are in workflow histories; changing them needs workflow versioning.
- **D-10 `[auto-default]`:** **The billing pre-check is a deterministic, rule-based simulation** with no external call. The booking request may carry an optional `coverage_ref`; Billing verifies it against a small rule set (self-pay verifies immediately; a reference with a configured failing prefix, set by an env var, fails). This makes the "deliberately failed booking leaves no half-true state" test reproducible without randomness. The result is stored as `pending → verified | failed`. — **Reversibility:** reversible — it is a stub behind a clean service boundary.
- **D-11 `[auto-default]`:** **Notifications are stubbed behind one function** in this phase: an activity that writes a PHI-free structured log line and an audit entry `notification.requested` (IDs and type only). Phase 3 swaps its internals for outbox + Celery without touching the workflows. Reminders and follow-ups are recorded as intents only. — **Reversibility:** reversible — one call site.

### Waitlist, cancel/reschedule and visit rules
- **D-12 `[auto-default]`:** **Waitlist matching**: an entry names a provider and an optional time window (default: any future slot for that provider). When a slot frees, matching is **FIFO by creation time among entries for the same provider whose window contains the slot**. The chosen entry is **auto-booked through the normal booking saga** (so idempotency, holds and compensation are reused), not an offer/accept flow, which avoids a second state machine with expiry timers. If the saga fails (for example billing), that entry is marked `failed` with a reason and the next entry is tried; there are no infinite retries. Department/specialty-wide matching and priority tiers are deferred. — **Reversibility:** reversible — matching is isolated in one activity.
- **D-13 `[auto-default]`:** **Cancel** is its own workflow. A cancel inside a configurable cutoff (default 24 hours before the slot) sets the billing state to `no_refund`; before the cutoff it goes `refund_pending → refunded`. Admin/front-desk may waive the cutoff. The slot is released, then the waitlist-advance workflow is started. Cancelling an already-cancelled appointment returns 200 as a no-op, consistent with Phase 1 soft-delete semantics. — **Reversibility:** reversible — policy values live in config.
- **D-14 `[auto-default]`:** **Reschedule is atomic reserve-then-swap**, never cancel-then-rebook: hold the new slot first; then, in one local Scheduling transaction, release the old slot, book the new one and repoint the appointment; then update billing. If the new slot cannot be reserved the original booking is untouched. The same cutoff and refund rules do not apply to a reschedule (billing is re-linked, not refunded). — **Reversibility:** reversible — one workflow.
- **D-15 `[auto-default]`:** **Visit state machine** in the Visit service: `checked_in → in_progress → completed`, one visit per appointment (`UNIQUE appointment_id`). Check-in requires a `confirmed` appointment (REST check to Scheduling) and is front-desk/admin only. Start and complete may be done by the visit's provider or front-desk/admin within the clinic. A repeat of the current or an already-passed target state returns 200 as a no-op; skipping a state returns 409. Transitions use a conditional `UPDATE ... WHERE status=<expected>` plus a state-history row, so concurrent duplicates cannot double-fire. — **Reversibility:** costly — state names and transition rules become API and history contracts.
- **D-16 `[auto-default]`:** **Completion triggers downstream work without blocking.** The transition commits first, with a `downstream_started_at IS NULL` marker in the same transaction. After the commit the service starts the `visit-completed` workflow (deterministic ID, so duplicates are rejected). A **sweeper** (on service start and via a Temporal Schedule) finds completed visits with the marker still null and starts their workflow, so a crash between commit and start cannot lose the trigger. The workflow finalizes billing, records a follow-up intent and an analytics intent (stubs for Phase 3). Phase 3's transactional outbox replaces this mechanism; the dashboard must say so. — **Reversibility:** reversible — contained in the Visit service.
- **D-17 `[auto-default]`:** **Real-time visit status (VISIT-05)** is a clinic-scoped `GET /visits` with status/provider/date filters for front-desk/admin, polled by clients. No WebSocket or SSE in this phase. — **Reversibility:** reversible.

### Cross-cutting rules (carried forward, restated because they bind every new service)
- **D-18 `[auto-default]`:** **RBAC and scope.** A patient may book, cancel and view only their own appointments (resolved through the Patient service). Front-desk and admin act within their own clinic. Every lookup stays clinic-scoped, and reads return IDs only (no cross-service enrichment, no PHI in these services). Every mutation calls `write_audit()` with field names only and a `trace_id`. Temporal activities propagate trace context so one booking is a connected trace.
- **D-19 `[auto-default]`:** **Verification bar.** The concurrency test fires parallel bookings at one slot against real Postgres and asserts exactly one confirmed appointment. Duplicate-request, forced-billing-failure, worker-crash-mid-saga and compensation-audit tests use the Temporal test environment for workflow logic plus the Compose Temporal for at least one end-to-end run. Workflow changes must go through Temporal versioning (`patched` or workflow-type versioning) once any workflow has run.

### Claude's Discretion
- Every decision above is a delegated default. Beyond them, the planner may choose: exact table/column names and migration order, the Temporal Python SDK structure (sandbox-safe workflow modules, activity grouping), retry/timeout policy values per activity, hold TTL and cutoff defaults, the sweeper/reaper schedule intervals, and API path shapes consistent with the existing services.
- The planner should call out in the plan any default it deviates from and why.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase scope and requirements
- `.planning/ROADMAP.md` § "Phase 2: Scheduling + Service Workflow" — goal and the six success criteria
- `.planning/REQUIREMENTS.md` — SCHED-01..08, VISIT-01..05, BILL-01..02
- `.planning/PROJECT.md` — mandated stack, constraints, Key Decisions table (to be extended)
- `.planning/STATE.md` § "Accumulated Context → Decisions" — locked decisions from Phases 1 and 1.1
- `.claude/CLAUDE.md` — Temporal vs Celery split (the booking saga is Temporal's job), Temporal server/SDK pins, `auto-setup` is not production, async-only, no `requests`

### Carried-forward context
- `.planning/phases/01.1-microservice-re-architecture/01.1-CONTEXT.md` — D-01..D-08 (service cut, sync REST reference checks, IDs-only reads, no Kafka yet) and the discretion defaults for internal auth, httpx, and libs layout
- `.planning/phases/01.1-microservice-re-architecture/deferred-items.md` — service-token `aud` gap and other deferred hardening that Phase 2 widens
- `.planning/phases/01-foundation-core-services/01-RESEARCH.md` — slot exclusion-constraint rationale and availability-template model

### Source assignment docs
- `Smart Health Requirements.md` — functional/non-functional requirements and mandated stack
- `SmartHealth — Execution Guidelines.md` — weekly plan and mentor evaluation approach

### Code to build on
- `services/scheduling/repository/models.py` — `ProviderSlot`, `ex_provider_slots_no_overlap`
- `services/scheduling/domain/enums.py` — `SlotStatus`
- `services/scheduling/app.py`, `services/scheduling/config.py`, `services/scheduling/db.py`, `services/scheduling/migrations/` — skeleton to extend
- `services/provider/repository/models.py` — `AvailabilityTemplate` (day_of_week, start/end time, `slot_duration_minutes`, effective window)
- `libs/service_client.py`, `libs/http.py`, `libs/auth/jwks_client.py`, `libs/audit`, `libs/observability`, `libs/health.py` — shared infrastructure to reuse
- `infra/docker-compose.yml` — existing `temporal`/`temporal-ui` services (image pin differs from CLAUDE.md; see STATE.md), `db-init` job
- `docs/architecture-dashboard.html` — dashboard to extend

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `services/scheduling`: service skeleton with health endpoint, JWKS verification wiring, Dockerfile, Alembic baseline (`scheduling_0001_provider_slots`) and the exclusion-constraint test. No `/api` routes yet.
- `libs/service_client.py` / `libs/http.py`: timeout-bounded internal client with bounded retries on idempotent GETs, trace propagation; the pattern for activity-side REST calls.
- Service-token support (`type=service`, 60s) and `require_internal_caller` from 01.1-05: the base for activity authentication.
- `AvailabilityTemplate.slot_duration_minutes`: slot length already modeled per template.
- Compose already runs Temporal (`auto-setup`) and Temporal UI, healthchecked, and Postgres `max_connections=300` (raised for Temporal).

### Established Patterns
- Fully async, `httpx` only, no sync I/O in routes; loop-aware DB singletons.
- One commit per request; routers call repositories, never inline ORM queries.
- Soft-delete/no-op idempotency (`200`), clinic-scoped lookups, one identical 404 for missing and out-of-scope IDs.
- 5xx/401/403 from an owning service is a 503, never a 422; only a clean 404 or `is_active=false` is an invalid reference (fail closed).
- Audit documents contain field names only, never PHI values.
- Each service declares its own `DeclarativeBase` and its own Alembic history; no cross-database foreign keys.

### Integration Points
- New services (Visit, Billing) and two worker containers added to `infra/docker-compose.yml`, `db-init`, and `infra/prometheus.yml`; the gateway route table gains `/appointments`, `/slots`, `/waitlist`, `/visits`, `/billing`.
- Temporal worker processes register workflows/activities and share OTel trace propagation with the API.
- Internal endpoints on Patient, Provider and Organization may need service-token access and clinic parameters for activity calls.

</code_context>

<specifics>
## Specific Ideas

- The mentor review weighs defensible architecture reasoning. The dashboard should show the booking and visit state machines, the compensation order, the "why Temporal, not Celery, for the saga" argument, and the interim mechanisms (sweeper, stub notifications, polling) with an explicit note that Phase 3 replaces them.
- Slot-booking correctness should be demonstrable live: a one-command concurrent-booking demo showing a single winner.

</specifics>

<deferred>
## Deferred Ideas

- Waitlist offer/accept flow with expiry, department/specialty-wide matching, and priority tiers (D-12 chose auto-book FIFO by provider).
- Synchronous "wait for result" booking mode or WebSocket/SSE visit status (D-07, D-17).
- Slot generation on a schedule (needs Celery/Temporal Schedules beyond the reaper; revisit in Phase 3).
- Replacing the visit sweeper and notification stubs with transactional outbox, Kafka and Celery (Phase 3).
- Capacity-aware scheduling and overbooking (SCHEDX-01/02, v2).
- Temporal production topology (`temporalio/server` plus schema job instead of `auto-setup`), noted in the dashboard only.

</deferred>

---

*Phase: 2-Scheduling + Service Workflow*
*Context gathered: 2026-10-12*

## Planning-Time Decisions (resolved with user at /gsd-plan-phase 2, 2026-10-12)

- **Saga order:** follow D-09 (billing pre-check before notification); document deviation from SCHED-02 and map "prevent conflicts" to the reserve step's DB constraints.
- **Service tokens:** add an Identity client-credentials endpoint issuing one token per audience (`aud`, required `exp`); update verifiers. Plan as an early wave (research assumption A1).
- **Patient overlap:** add a Postgres `EXCLUDE` constraint preventing overlapping active appointments for one patient.
- **Edge states:** cancel/reschedule while `pending` returns 409; no `completed` appointment status (completion lives on the visit).
