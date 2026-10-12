# Phase 2: Scheduling + Service Workflow - Research

**Researched:** 2026-10-12
**Domain:** Temporal-orchestrated saga workflows (Python SDK) over a Postgres data-layer concurrency guarantee, inside the existing FastAPI microservice mesh
**Confidence:** HIGH on Temporal/Postgres mechanics (probed live this session); MEDIUM on the internal-auth design (a design the user must confirm, see A1/A2)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

> **How these were chosen:** the user did not select options and delegated ("do what is best practice"). Every decision below is a **recommended default chosen by Claude** and is tagged `[auto-default]`. None is a user-confirmed preference. The user can overrule any of them before or after planning by editing this file and re-running `/gsd-plan-phase 2`. Decisions D-01..D-08 of Phase 1.1 (service cut, database-per-service, sync REST reference checks, IDs-only reads, no Kafka/outbox yet) carry forward unchanged.

#### Slot supply and holds
- **D-01 `[auto-default]`:** Scheduling **materializes slots on demand over a rolling horizon** (default 28 days), generated from Provider's availability templates fetched through the existing internal REST client (GET only). Slot length is the template's `slot_duration_minutes`. Template times are interpreted in the clinic's timezone (from Organization) and stored as UTC `timestamptz`. Generation is idempotent (re-running over the same range creates no duplicates and never touches booked or held slots). It is triggered by an admin/front-desk endpoint and by an on-demand extension when a search asks beyond the generated range; no scheduler runs until Celery exists in Phase 3. — **Reversibility:** reversible — only the trigger and horizon change.
- **D-02 `[auto-default]`:** **Double-booking is closed by a partial unique index on active appointments** (`UNIQUE (slot_id) WHERE status IN ('pending','confirmed')`), not by application logic. The existing `ex_provider_slots_no_overlap` exclusion constraint keeps guarding against overlapping slot rows for one provider; the new index guards one slot against two appointments. Slot reservation additionally uses a single conditional `UPDATE ... SET status='held' WHERE id=? AND status='available'` so losers fail fast. The concurrency test must hit a real Postgres and show exactly one winner. — **Reversibility:** reversible — an index swap, no data migration.
- **D-03 `[auto-default]`:** A reserved slot gets a **short hold**: add `held_until` (default 10 minutes) and a `held_by_appointment_id` to `provider_slots`. The saga confirms the hold to `booked` or releases it. A hold that outlives its workflow (for example a worker crash past all retries) is released by a reaper activity driven by a Temporal Schedule or timer, not by cron. Slot statuses stay `available/held/booked/cancelled` as already constrained. — **Reversibility:** reversible — columns and one reaper.

#### Service boundaries and Temporal topology
- **D-04 `[auto-default]`:** **Three services own this phase's data, each with its own database:**
  - **Scheduling** (existing skeleton): slots, appointments, waitlist entries, idempotency keys. Appointments live with slots so that "release old slot + book new slot + move the appointment" is one local ACID transaction (the reschedule-atomicity requirement), not a distributed one.
  - **Visit** (new): visits and their state history, in `visit_db`.
  - **Billing** (new): billing pre-checks and refund state, in `billing_db`.

  This matches the roadmap line "scheduling/visit/billing are built as their own services" and the 01.1 preference for finer boundaries. `db-init` and Compose gain `visit_db`/`billing_db`, a role per service, and a container per service. — **Reversibility:** costly — merging or splitting later means moving tables between databases and rewriting internal calls.
- **D-05 `[auto-default]`:** **A workflow lives with the service that owns its aggregate, with one Temporal task queue and one worker container per service** (`scheduling-worker`, `visit-worker`; Billing needs no workflow of its own). Booking, cancel, reschedule and waitlist-advance run on the Scheduling queue. The post-completion fan-out runs on the Visit queue. Activities call other services over the existing REST client; they never touch another service's database. Workers are separate processes from the API containers, so a worker restart does not drop HTTP traffic. — **Reversibility:** costly — task-queue names and workflow types are in running workflow histories.
- **D-06 `[auto-default]`:** **Activities authenticate with service tokens** (the `type=service` token from 01.1-05), because a workflow outlives the HTTP request and has no user context. The caller's JWT is never passed into Temporal. The internal endpoints that activities call must accept service tokens and re-apply clinic scope from explicit parameters. **Workflow inputs, outputs and event history carry IDs and enums only, never PHI.** Workflow IDs are deterministic (`booking-{appointment_id}`, `cancel-{appointment_id}-{n}`, `visit-completed-{visit_id}`) with duplicate-start rejection, so a repeated start is a no-op. Every activity is idempotent: downstream endpoints accept a deterministic idempotency key built from workflow ID and step. The deferred service-token `aud` gap from `01.1-09` should be closed here because Phase 2 widens service-token use. — **Reversibility:** costly — it sets the internal auth contract for all Phase 2 calls.

#### Booking contract and failure behavior
- **D-07 `[auto-default]`:** **`POST /appointments` is asynchronous**: it requires an `Idempotency-Key` header, writes the appointment as `pending`, starts the workflow, and returns **202** with the appointment ID and a status URL. Clients poll `GET /appointments/{id}`; the appointment is `confirmed` only after the whole saga succeeds. No synchronous wait mode. — **Reversibility:** costly — a published API contract that clients will code against.
- **D-08 `[auto-default]`:** **Idempotency keys** are stored in Scheduling's Postgres (not Redis, so they survive a cache flush) as `(caller_user_id, key)` unique, with a SHA-256 of the canonicalized request body and the resulting `appointment_id`. The key insert and appointment insert happen in the same transaction, so concurrent duplicates are arbitrated by the unique constraint. Same key and same body returns the original response (no second appointment). Same key with a different body returns **422**. Keys expire after 24 hours (lazy cleanup). The workflow ID adds a second layer of dedupe. — **Reversibility:** costly — key scope and mismatch semantics are client-visible.
- **D-09 `[auto-default]`:** **Booking saga order follows SCHED-02**: validate (patient, provider, slot via REST; clinic scope; all must be active) → reserve slot (hold) → billing pre-check (Billing creates `pending`, then sets `verified` or `failed`) → schedule notification → confirm (slot `booked`, appointment `confirmed`). Notification scheduling is **best-effort**: it retries but its failure never fails or compensates the booking. **Compensation runs as forward transitions**, in reverse order, never deletes: release the hold, void the billing record, set the appointment to `failed` with a machine-readable reason, write an audit entry per step. A failed or compensated booking is fully reconstructable from audit plus the Temporal history. — **Reversibility:** costly — step order and state names are in workflow histories; changing them needs workflow versioning.
- **D-10 `[auto-default]`:** **The billing pre-check is a deterministic, rule-based simulation** with no external call. The booking request may carry an optional `coverage_ref`; Billing verifies it against a small rule set (self-pay verifies immediately; a reference with a configured failing prefix, set by an env var, fails). This makes the "deliberately failed booking leaves no half-true state" test reproducible without randomness. The result is stored as `pending → verified | failed`. — **Reversibility:** reversible — it is a stub behind a clean service boundary.
- **D-11 `[auto-default]`:** **Notifications are stubbed behind one function** in this phase: an activity that writes a PHI-free structured log line and an audit entry `notification.requested` (IDs and type only). Phase 3 swaps its internals for outbox + Celery without touching the workflows. Reminders and follow-ups are recorded as intents only. — **Reversibility:** reversible — one call site.

#### Waitlist, cancel/reschedule and visit rules
- **D-12 `[auto-default]`:** **Waitlist matching**: an entry names a provider and an optional time window (default: any future slot for that provider). When a slot frees, matching is **FIFO by creation time among entries for the same provider whose window contains the slot**. The chosen entry is **auto-booked through the normal booking saga** (so idempotency, holds and compensation are reused), not an offer/accept flow, which avoids a second state machine with expiry timers. If the saga fails (for example billing), that entry is marked `failed` with a reason and the next entry is tried; there are no infinite retries. Department/specialty-wide matching and priority tiers are deferred. — **Reversibility:** reversible — matching is isolated in one activity.
- **D-13 `[auto-default]`:** **Cancel** is its own workflow. A cancel inside a configurable cutoff (default 24 hours before the slot) sets the billing state to `no_refund`; before the cutoff it goes `refund_pending → refunded`. Admin/front-desk may waive the cutoff. The slot is released, then the waitlist-advance workflow is started. Cancelling an already-cancelled appointment returns 200 as a no-op, consistent with Phase 1 soft-delete semantics. — **Reversibility:** reversible — policy values live in config.
- **D-14 `[auto-default]`:** **Reschedule is atomic reserve-then-swap**, never cancel-then-rebook: hold the new slot first; then, in one local Scheduling transaction, release the old slot, book the new one and repoint the appointment; then update billing. If the new slot cannot be reserved the original booking is untouched. The same cutoff and refund rules do not apply to a reschedule (billing is re-linked, not refunded). — **Reversibility:** reversible — one workflow.
- **D-15 `[auto-default]`:** **Visit state machine** in the Visit service: `checked_in → in_progress → completed`, one visit per appointment (`UNIQUE appointment_id`). Check-in requires a `confirmed` appointment (REST check to Scheduling) and is front-desk/admin only. Start and complete may be done by the visit's provider or front-desk/admin within the clinic. A repeat of the current or an already-passed target state returns 200 as a no-op; skipping a state returns 409. Transitions use a conditional `UPDATE ... WHERE status=<expected>` plus a state-history row, so concurrent duplicates cannot double-fire. — **Reversibility:** costly — state names and transition rules become API and history contracts.
- **D-16 `[auto-default]`:** **Completion triggers downstream work without blocking.** The transition commits first, with a `downstream_started_at IS NULL` marker in the same transaction. After the commit the service starts the `visit-completed` workflow (deterministic ID, so duplicates are rejected). A **sweeper** (on service start and via a Temporal Schedule) finds completed visits with the marker still null and starts their workflow, so a crash between commit and start cannot lose the trigger. The workflow finalizes billing, records a follow-up intent and an analytics intent (stubs for Phase 3). Phase 3's transactional outbox replaces this mechanism; the dashboard must say so. — **Reversibility:** reversible — contained in the Visit service.
- **D-17 `[auto-default]`:** **Real-time visit status (VISIT-05)** is a clinic-scoped `GET /visits` with status/provider/date filters for front-desk/admin, polled by clients. No WebSocket or SSE in this phase. — **Reversibility:** reversible.

#### Cross-cutting rules (carried forward, restated because they bind every new service)
- **D-18 `[auto-default]`:** **RBAC and scope.** A patient may book, cancel and view only their own appointments (resolved through the Patient service). Front-desk and admin act within their own clinic. Every lookup stays clinic-scoped, and reads return IDs only (no cross-service enrichment, no PHI in these services). Every mutation calls `write_audit()` with field names only and a `trace_id`. Temporal activities propagate trace context so one booking is a connected trace.
- **D-19 `[auto-default]`:** **Verification bar.** The concurrency test fires parallel bookings at one slot against real Postgres and asserts exactly one confirmed appointment. Duplicate-request, forced-billing-failure, worker-crash-mid-saga and compensation-audit tests use the Temporal test environment for workflow logic plus the Compose Temporal for at least one end-to-end run. Workflow changes must go through Temporal versioning (`patched` or workflow-type versioning) once any workflow has run.

### Claude's Discretion
- Every decision above is a delegated default. Beyond them, the planner may choose: exact table/column names and migration order, the Temporal Python SDK structure (sandbox-safe workflow modules, activity grouping), retry/timeout policy values per activity, hold TTL and cutoff defaults, the sweeper/reaper schedule intervals, and API path shapes consistent with the existing services.
- The planner should call out in the plan any default it deviates from and why.

### Deferred Ideas (OUT OF SCOPE)
- Waitlist offer/accept flow with expiry, department/specialty-wide matching, and priority tiers (D-12 chose auto-book FIFO by provider).
- Synchronous "wait for result" booking mode or WebSocket/SSE visit status (D-07, D-17).
- Slot generation on a schedule (needs Celery/Temporal Schedules beyond the reaper; revisit in Phase 3).
- Replacing the visit sweeper and notification stubs with transactional outbox, Kafka and Celery (Phase 3).
- Capacity-aware scheduling and overbooking (SCHEDX-01/02, v2).
- Temporal production topology (`temporalio/server` plus schema job instead of `auto-setup`), noted in the dashboard only.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCHED-01 | Patient/front-desk can book an appointment against an available provider slot | Accept-transaction pattern (Pattern 1), slot search/generation (Pattern 6), RBAC matrix, new Patient/Provider/Org internal lookups (Gap table G3) |
| SCHED-02 | Booking runs as a durable Temporal workflow (validate -> reserve -> prevent conflicts -> notify -> billing -> confirm); confirmed only after success | Saga skeleton with registered-before compensations (Pattern 3, run and verified), activity bounds (Pitfall 6), step-order discrepancy (Open Question 1) |
| SCHED-03 | Concurrent booking for the same slot cannot double-book, enforced at the data layer | Partial unique index + conditional UPDATE, probed live under 60-way concurrency (Pattern 1, Code Example 1), `CHECK (end_time > start_time)` (WR-07) |
| SCHED-04 | Booking is idempotent | `(caller_user_id, key)` arbitration in one transaction, probed live under 40-way duplicate keys (Pattern 1); workflow-ID second layer verified (Pattern 2) |
| SCHED-05 | Reschedule releases old slot and reserves new atomically | Reserve-then-swap in one local transaction (Pattern 5); unique-violation as a business outcome (Pitfall 9) |
| SCHED-06 | Cancel releases slot and triggers billing/refund updates | Cancel workflow + deterministic refund-policy input (Pattern 5), Billing state machine (Billing section) |
| SCHED-07 | Freed slot advances the waitlist | FIFO claim with `FOR UPDATE SKIP LOCKED`, child booking workflow, lost-race rule (Pattern 7) |
| SCHED-08 | Partial failure mid-booking leaves no corrupted state | Reverse-order forward compensations, reaper backstop, lost-trigger sweeper for appointments (Patterns 3 and 8, Pitfalls 4 and 5) |
| VISIT-01 | Front-desk can record check-in for a confirmed appointment | Visit service, Scheduling internal appointment lookup (Pattern 9) |
| VISIT-02 | Provider/front-desk can advance a visit through states | Conditional-UPDATE state machine (Pattern 9); provider-by-user lookup (G3) |
| VISIT-03 | Visit transitions are idempotent and duplicate-safe | Same pattern; repeat-or-past target returns 200 (D-15) |
| VISIT-04 | Completing a visit triggers downstream workflows without blocking | Commit-then-start with `downstream_started_at` marker and sweeper Schedule (Pattern 8) |
| VISIT-05 | Current visit status queryable in real time | Clinic-scoped `GET /visits` with filters and a partial index (Pattern 9) |
| BILL-01 | Booking initiates a billing pre-check (pending -> verified/failed), modeled internally | Billing service state machine, deterministic rule set (Billing section) |
| BILL-02 | Cancellation/reschedule updates billing/refund state | Billing target-state endpoints, idempotent by state (Billing section) |
</phase_requirements>

## Summary

Phase 2 is mostly a *correctness-at-the-data-layer* phase wrapped in a Temporal orchestration layer. The data-layer half is now proven: against the live Compose Postgres 15, a partial unique index on `appointments(slot_id) WHERE status IN ('pending','confirmed')` let exactly 1 of 60 parallel inserts win; the same-key arbitration (`INSERT ... ON CONFLICT DO NOTHING RETURNING` in the same transaction as the appointment insert) produced exactly 1 appointment from 40 parallel duplicate requests with all 39 replays pointing at the winner; a conditional `UPDATE ... WHERE status='available'` yielded 1 winner of 50. The orchestration half is also proven on the exact versions in play: `temporalio` 1.34.0 against the Compose Temporal server 1.29.7 (start policies, Schedules, worker-killed-mid-activity recovery), and a reverse-order compensation saga ran and produced the expected call order. This resolves the STATE.md blocker "Temporal SDK<->server compatibility has no strict matrix": there is none officially, but this exact pair was exercised end to end.

CONTEXT.md is internally consistent except for several things it assumed already exist and that do not. The planner must schedule work for each (Gap table below): (1) **no service other than Identity can mint a service token**, and Identity only mints one for its own outgoing calls, so D-06 has no implementation path yet; (2) adding `aud` to service tokens will make every existing verifier reject them, because python-jose raises "Invalid audience" when a token carries `aud` and the verifier passes none; (3) Patient has no internal endpoint, Provider has no availability-template or by-user lookup, and Organization's `ClinicRef` carries no `timezone`, which D-01 needs; (4) the shared reference client is GET-only by design but activities must POST; (5) `write_audit` is a plain `insert_one`, so an activity retry duplicates audit entries; (6) the Temporal default data converter silently corrupts `(str, Enum)` dataclass fields (the exact pattern `SlotStatus` uses); (7) D-07's "commit pending row, then start workflow" has the same lost-trigger window D-16 closes for visits, so appointments need the same marker-plus-sweeper.

Everything else follows the project's established patterns (database-per-service, per-service `DeclarativeBase`, async-only, IDs-only, 503-never-422 on upstream trouble). The Scheduling API process and the worker are two entrypoints of the same service image; workers must not run migrations.

**Primary recommendation:** Build the Postgres constraints and the accept transaction first and prove "1 winner of N" with a DB-level test before any workflow exists; then add the Temporal saga on top with string-named, class-based, DI-friendly activities and plain-`str` workflow I/O; close the service-token mint/`aud`/`exp` gap in the first wave because every later plan depends on it.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Double-booking prevention | Database (Scheduling Postgres) | API (maps unique-violation to 409) | Only a constraint is safe under concurrent writers; application checks have a race window (models.py docstring, probe) |
| Idempotent booking acceptance | API (Scheduling) + Database | Temporal (workflow-ID second layer) | Key and appointment must commit in one local transaction; Temporal ID uniqueness only lasts the namespace retention (24h, verified) |
| Multi-step booking/cancel/reschedule orchestration | Workflow worker (Temporal, `scheduling-worker`) | API (starts workflows) | Durable, replayable saga with compensation is Temporal's job, not Celery's (CLAUDE.md split) |
| Slot hold TTL expiry / lost-trigger recovery | Workflow worker (Temporal Schedule) | Database (partial indexes) | No cron exists until Phase 3; Schedules verified live |
| Reschedule atomic swap | Database (one local Scheduling transaction) | Workflow (orders the steps) | D-04 puts appointments with slots exactly so this is local ACID |
| Slot generation from templates | API (Scheduling) | Provider/Organization (data via REST) | Scheduling owns concrete slots; templates and timezone are other services' data |
| Billing pre-check and refund state | API/Backend (Billing) | Workflow (calls it as an activity) | Own aggregate, own DB; deterministic stub behind a clean boundary |
| Visit lifecycle state machine | API (Visit) + Database | Workflow worker (`visit-worker`, post-completion fan-out only) | Transitions are synchronous and duplicate-safe at the row; only the fan-out is a workflow |
| Service-to-service authentication | Identity (mints) + every service (verifies) | Libs (token provider, verification) | Only Identity holds the private key (test_service_isolation) |
| Workflow/state diagrams, "why Temporal" | Static dashboard (`docs/architecture-dashboard.html`) | README / docs | Self-contained HTML, no external assets |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| temporalio | 1.34.0 `[VERIFIED: PyPI JSON, 2026-09-30 upload; installed and run this session]` | Workflows, activities, worker, client, Schedules, test environment | Mandated stack. `requires_python >=3.10`; `requires_dist` has no hard pins that conflict: a `pip install --dry-run` of the project's pins plus `temporalio==1.34.0` resolved cleanly `[VERIFIED: pip dry-run this session]` |

`temporalio` `[WARNING: flagged as suspicious by the legitimacy gate (reasons: too-new, unknown-downloads) — verify before using.]` The flag is driven by the *latest release date* (2026-09-30) and missing download stats from the registry API, not by package youth: first upload 2022-03-18, 55 releases, repo `github.com/temporalio/sdk-python` (1218 stars), about 9.2M downloads/week per pypistats `[VERIFIED: PyPI JSON + pypistats + GitHub API this session]`. It is also the mandated library in CLAUDE.md. The protocol still requires a `checkpoint:human-verify` before install.

Everything else the phase needs is already pinned in `pyproject.toml` `[VERIFIED: pyproject.toml:8-32]`: FastAPI 0.141.1, SQLAlchemy 2.0.52 async, asyncpg 0.31.0, Alembic 1.19.2, pymongo 4.18.0, redis 8.1.0, httpx, tenacity 9.1.4, structlog 26.1.0, OpenTelemetry 1.44.0 / 0.65b0. `temporalio.testing`, `temporalio.contrib.opentelemetry` and `temporalio.contrib.pydantic` ship inside the `temporalio` wheel; the `[opentelemetry]` and `[pydantic]` extras only add `opentelemetry-api/sdk>=1.26,<2` and `pydantic>=2`, which are already satisfied, so **no extras are needed**.

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `zoneinfo` (stdlib) | Python 3.12 | Wall-clock to UTC for slot generation | Always; system tzdata is present in the `python:3.12-slim` service image (`ZoneInfo('Asia/Karachi')` succeeded inside the running scheduling container) `[VERIFIED: docker exec this session]`. Do not add the `tzdata` PyPI package (it also tripped the gate for the same "too-new" reason and is unnecessary) |
| `temporalio.contrib.opentelemetry.TracingInterceptor` | in 1.34.0 | One connected trace API -> workflow -> activity | Use the stable `TracingInterceptor`, not the `[experimental]` `OpenTelemetryPlugin` (README marks the plugin experimental `[CITED: temporalio/contrib/opentelemetry/README.md in 1.34.0 wheel]`) |
| `tenacity` (existing) | 9.1.4 | Retry for *API-process* GETs only | Do **not** stack it inside activities; Temporal owns activity retries |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| String-named activities + plain `str`/`int` dataclasses | `pydantic_data_converter` + Pydantic models | Pydantic converter round-trips `(str, Enum)`, UUID and tz-aware datetimes correctly (probed), at the cost of importing pydantic into the sandbox and wiring the converter on every client and worker. Plain dataclasses with `str` fields need nothing extra. Pick one and apply it everywhere |
| Stable `TracingInterceptor` | `OpenTelemetryPlugin` | Plugin gives accurate workflow-span durations but is flagged experimental and shows workflow spans only after completion |
| Temporal Schedule for sweepers | A long-lived "sweeper loop" workflow with `continue_as_new` | Schedule is simpler, verified live, and survives restarts; loop workflow needs history management |
| Identity client-credentials mint endpoint (recommended, A1) | Per-service signing keys published in JWKS | Per-service keys remove a network hop but change the "Identity alone holds a private key" invariant that `test_private_key_loading_only_in_identity` enforces |

**Installation:**
```bash
# pyproject.toml dependency (exact pin, as the project does for every other library)
"temporalio==1.34.0",
```

**Version verification:**
```bash
curl -s https://pypi.org/pypi/temporalio/json | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['info']['version'], d['info']['requires_python'])"
# -> 1.34.0 >=3.10   (this session; 1.32.0, the CLAUDE.md pin, also exists, uploaded 2026-08-24)
```
Deviation to call out in the plan: CLAUDE.md's version table says `temporalio` 1.32.0. 1.34.0 is recommended because every probe in this research ran on it and CLAUDE.md's own rule is "pin the exact version you test against". 1.32.0 is an acceptable fallback; the server image is unchanged either way (`temporalio/auto-setup:1.29.7`, compose line 201).

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| temporalio | PyPI | 4.6 yrs (first upload 2022-03-18; 55 releases) | ~9.2M/week (pypistats) | github.com/temporalio/sdk-python | SUS (too-new latest release, unknown-downloads from registry API) | Flagged — planner must add a `checkpoint:human-verify` before the install; evidence above supports approval |
| tzdata | PyPI | 10 yrs | n/a | github.com/python/tzdata | SUS (same reason) | NOT RECOMMENDED — not needed (system tzdata verified in image) |

**Packages removed due to [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** temporalio (approve after human check), tzdata (dropped from the plan)

No other new packages. Registry probes: `npm` is not relevant; PyPI was queried directly with `curl` (`pip` is not on PATH outside the venv). No postinstall concept applies to PyPI wheels.

## Architecture Patterns

### System Architecture Diagram

```
                       external clients
                              |
                       [ gateway :8000 ]  rate limit 300/min/IP default (compose:337)
                              |  route table: /api/appointments /api/slots /api/waitlist
                              |               /api/visits /api/billing  (NEW)
        +---------------------+------------------------+--------------------+
        v                                              v                    v
 [ scheduling API ]                              [ visit API ]        [ billing API ]
   POST /appointments (202)                        POST check-in        GET records (read)
   cancel / reschedule / waitlist / slots          PATCH start/complete /internal/* (mutate)
        |  (1) ONE local tx: idempotency row + appointment(pending)       |
        |      -> partial unique index arbitrates the slot                |
        |  (2) after commit: start workflow (deterministic id)            |
        v                                                                 |
 [ scheduling_db ]                                                        |
   provider_slots, appointments, idempotency_keys, waitlist_entries,      |
   appointment_status_history                                             |
        ^                                                                 |
        | activities (class-based, DI)                                    |
 [ scheduling-worker ] <---- Temporal server 1.29.7 (task queue scheduling-tq)
   BookingWorkflow  CancelWorkflow  RescheduleWorkflow  WaitlistAdvance  Reaper(Schedule)
        |
        | activity -> REST + service token (aud=<target>) + deterministic Idempotency-Key
        +--> patient   GET /internal/patients/{id}?clinic_id=        (NEW)
        +--> provider  GET /internal/providers/{id}                  (exists)
        |              GET /internal/providers/{id}/availability     (NEW)
        +--> organization GET /internal/departments/{id}, /clinics/{id} (+timezone NEW)
        +--> billing   POST /internal/billing/... (state-target, idempotent)
        +--> mongo     write_audit(dedupe_key=...)   (notification.requested = stub, D-11)

 [ visit API ] --commit visit(completed, downstream_started_at NULL)--> [ visit_db ]
        | after commit: start visit-completed-{visit_id}                   ^
        v                                                                  |
 [ visit-worker ] (task queue visit-tq)  VisitCompletedWorkflow            |
        +--> billing finalize, follow-up intent, analytics intent (stubs)  |
        +--> Sweeper (Schedule): completed AND downstream_started_at IS NULL --+

 Identity: POST /internal/service-tokens  (NEW)  <-- every worker/API process, cached per audience
 Jaeger/OTLP: TracingInterceptor on API-process client and on workers -> one trace
```

Decision points: duplicate key -> replay (200/202 same id) or 422 on body mismatch; unique violation on slot -> 409; any activity non-retryable failure -> compensation branch in reverse order -> appointment `failed`; billing `failed` is a non-retryable business outcome.

### Recommended Project Structure

```
libs/
├── temporal.py            # client factory (lazy=True, TracingInterceptor), task-queue constants, retry presets
├── internal_client.py     # POST/PATCH w/ service token + Idempotency-Key; NO internal retries (Temporal retries)
└── auth/service_tokens.py # ServiceTokenProvider: mint via Identity, cache per audience, refresh before expiry
services/scheduling/
├── api/            appointments.py slots.py waitlist.py internal.py
├── domain/         enums.py (extend) schemas.py contracts.py   # contracts.py = workflow I/O dataclasses + activity-name constants, PURE
├── repository/     models.py slot_repo.py appointment_repo.py idempotency_repo.py waitlist_repo.py
├── workflows/      booking.py cancel.py reschedule.py waitlist_advance.py reaper.py   # sandbox-safe: temporalio + stdlib + contracts only
├── activities/     booking.py cancel.py reschedule.py waitlist.py reaper.py           # class-based, constructor-injected deps
├── worker.py       # `python -m services.scheduling.worker`
└── migrations/versions/scheduling_0002_*.py
services/visit/     (same shape; workflows/visit_completed.py, sweeper)
services/billing/   (API + repository only; no workflow)
```

### Pattern 1: Accept transaction (SCHED-01, SCHED-03, SCHED-04)

**What:** `POST /api/appointments` does everything that must be atomic in ONE local transaction, then starts the workflow. Order matters: idempotency row first (arbiter for duplicates), appointment second (arbiter for the slot).

**When to use:** every booking entry, including the waitlist auto-book path (derive key `waitlist-{entry_id}`).

**Probed behavior (live Postgres 15, asyncpg, pool of 20-40, this session):**
- 60 parallel transactions with distinct keys on one slot -> `{'won': 1, 'lost': 59}`, losers failed on `zz_active_slot` (the partial unique index); only 1 idempotency row persisted (losers rolled back with their appointment insert).
- 40 parallel transactions with the SAME `(caller, key)` -> `{'won': 1, 'replay': 39}`; every replay read the winner's `appointment_id` after the winner committed. `ON CONFLICT DO NOTHING` waits for the in-flight conflicting transaction, so the replay `SELECT` always sees a committed row.
- 50 parallel conditional `UPDATE ... SET status='held' ... WHERE id=? AND status='available' RETURNING 1` -> 1 winner.
- Cancelling the active appointment (status leaves the index predicate) immediately allows a new `pending` row on the same `slot_id`.

```python
# Illustrative skeleton. SQL semantics VERIFIED by the probe above; table/column names are proposals.
async def accept_booking(db, *, caller_id, key, body_hash, appt_id, slot_id, ...):
    async with db.begin():
        got = await db.scalar(
            text("""INSERT INTO idempotency_keys (caller_user_id, key, request_hash, appointment_id, expires_at)
                    VALUES (:c, :k, :h, :a, now() + interval '24 hours')
                    ON CONFLICT (caller_user_id, key) DO NOTHING RETURNING appointment_id"""), ...)
        if got is None:                         # duplicate: arbitrate by stored hash
            row = (await db.execute(text("SELECT request_hash, appointment_id FROM idempotency_keys "
                                         "WHERE caller_user_id=:c AND key=:k"), ...)).one()
            if row.request_hash != body_hash:   raise IdempotencyKeyReused()   # -> 422
            return Replay(row.appointment_id)                                  # -> same 202 body
        try:
            await db.execute(insert_appointment_pending(...))                   # unique index fires here
        except IntegrityError as e:
            if "uq_appointments_active_slot" in str(e): raise SlotTaken()      # -> 409, whole tx rolls back
            raise
    # AFTER commit: start the workflow (Pattern 2). A crash here is covered by the pending-appointment sweeper (Pattern 8).
```

Rules: pre-generate the appointment UUID in the app so the key row can reference it; do the cheap `status='available'` read check *before* the transaction only as an early 409, never as the guard; `Idempotency-Key` is a header (`Header(alias="Idempotency-Key")`), required, bounded length; canonicalize the body (`json.dumps(sort_keys=True, separators=(",",":"))`) before SHA-256.

### Pattern 2: Start a workflow exactly once

**What:** deterministic workflow ID + `id_conflict_policy=USE_EXISTING` + `id_reuse_policy=REJECT_DUPLICATE`, and treat `WorkflowAlreadyStartedError` as success.

**Verified live against the Compose server (SDK 1.34.0 / server 1.29.7):**
- same ID started twice while running with `USE_EXISTING` -> same `result_run_id` (`True`);
- default policy while running -> `WorkflowAlreadyStartedError`;
- `REJECT_DUPLICATE` after the first run closed -> `WorkflowAlreadyStartedError`;
- `ALLOW_DUPLICATE_FAILED_ONLY` restarts only after a *failed* run and raises after a *successful* one.

```python
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

async def start_once(client, wf, arg, *, wf_id: str, task_queue: str) -> bool:
    try:
        await client.start_workflow(wf, arg, id=wf_id, task_queue=task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE)
        return True
    except WorkflowAlreadyStartedError:
        return False        # already ran or running: a repeated start is a no-op (D-06)
```

Caveat: ID uniqueness lasts only as long as the namespace retention, which is `24h0m0s` for `default` on the Compose server `[VERIFIED: tctl namespace describe this session]`. After that a closed ID can be reused. **The database arbiters (Pattern 1), not the workflow ID, are the real idempotency guarantee**; the workflow ID is a second layer (D-08 already says this).

For IDs that legitimately repeat per appointment, use a monotonically increasing counter stored on the appointment: `cancel-{appointment_id}-{n}` and `reschedule-{appointment_id}-{n}` where `n` is the appointment's `version`/attempt counter read inside the accept transaction (CONTEXT leaves `n` undefined). Terminal workflows (booking, visit-completed) need no counter.

### Pattern 3: Saga with compensations registered BEFORE the step (SCHED-02, SCHED-08)

Temporal's own guidance: register the compensation *before* running the step, make every compensation idempotent, tolerate "forward action never happened", and "log compensation failures but continue" `[CITED: docs.temporal.io/design-patterns/saga-pattern]`. The skeleton below was executed in the time-skipping test environment: happy path called `['reserve_slot','billing_precheck','confirm']`; with billing forced to a non-retryable failure it called `['reserve_slot','billing_precheck','void_billing','release_slot','mark_failed:BillingRejected']` (reverse order, forward transitions, one final failed-state write).

```python
# VERIFIED by execution (temporalio 1.34.0, time-skipping env). Activities referenced BY NAME so the
# workflow module imports nothing heavy into the sandbox.
@workflow.defn
class BookingWorkflow:
    @workflow.run
    async def run(self, inp: BookingInput) -> BookingResult:
        comps: list[str] = []
        try:
            await act("validate", inp)                         # no compensation needed
            comps.append("release_slot");  await act("reserve_slot", inp)
            comps.append("void_billing");  await act("billing_precheck", inp)   # non-retryable BillingRejected
            await act_best_effort("schedule_notification", inp)                 # never compensates (D-09)
            await act("confirm", inp)
            return BookingResult(outcome="confirmed")
        except ActivityError as err:
            reason = getattr(err.cause, "type", None) or "activity_failed"
            for name in reversed(comps):
                await act(name, inp)                            # compensation = forward transition
            await act("mark_failed", inp, reason)
            return BookingResult(outcome="failed", reason=reason)
```

Key properties to encode in the plan: the saga returns a normal result for a business failure (so the workflow closes *Completed*, and `REJECT_DUPLICATE` then blocks re-runs); only infrastructure problems leave it open and retrying.

### Pattern 4: Activity design (all workers)

- **Class-based activities with constructor injection** (db session factory, httpx client, service-token provider, mongo). Verified: a bound-method activity with an injected object ran, and the workflow invoked it by method name string (`"step_two"`). This is what lets the test harness inject the in-process `mesh` client.
- **Bound every activity**: `start_to_close_timeout` (required) plus `schedule_to_close_timeout` and `RetryPolicy(maximum_attempts=N, non_retryable_error_types=[...])`. The SDK default retry is unlimited attempts (`maximum_attempts: int = 0 ... no maximum` `[VERIFIED: temporalio/common.py RetryPolicy, 1.34.0]`), so an unbounded activity against a down Billing hangs the saga instead of compensating.
- **Business outcomes are non-retryable `ApplicationError(type="...", non_retryable=True)`**: `SlotUnavailable`, `BillingRejected`, `ReferenceInvalid`. Transport errors, 5xx, 401/403 and Redis/JWKS trouble are retryable. This mirrors the project rule "5xx/401/403 is a 503, never a 422".
- **No tenacity inside activities** (double retry multiplies attempts), and no PHI in `ApplicationError` messages (they land in history).
- Activities read what they need from their own DB by `appointment_id`; **`coverage_ref` and any other potentially identifying value is never passed through workflow input/output** (D-06). Billing's rule needs the value, so the activity loads it from the appointment row and sends it in the REST body.

### Pattern 5: Cancel and reschedule (SCHED-05, SCHED-06)

- **Cancel**: decide `refund_policy` in the API request (cutoff arithmetic, admin/front-desk waiver) and pass it as an enum string in the workflow input. Computing it in the workflow would require `workflow.now()`; computing it in the request keeps history deterministic. Steps: mark cancelled (conditional `WHERE status='confirmed'`, slot -> `available`, in one transaction) -> Billing `cancel` (`refund_pending -> refunded` or `no_refund`) -> notification stub -> start waitlist-advance as a child with `ParentClosePolicy.ABANDON` and ID `waitlist-advance-{appointment_id}-{n}`.
- **Do not confuse slot `cancelled` with appointment `cancelled`.** Slot `cancelled` means "administratively withdrawn" (the exclusion constraint ignores it so the window can be re-offered). A cancelled appointment returns its slot to `available`.
- **Reschedule**: (1) conditional hold of the new slot (`held_by_appointment_id = appointment_id`); (2) ONE local transaction: `UPDATE appointments SET slot_id=:new, version=version+1 WHERE id=:a AND slot_id=:old AND status IN ('pending','confirmed')` (probed: updated 1 row; the partial unique index re-checks the new slot), old slot -> `available`, new slot -> `booked`; (3) Billing re-link; (4) notification. If (2) fails with the unique violation or zero rows, release the new hold and leave the original untouched.
- Cancel while the booking saga is still `pending` is a race (the saga's `confirm` would then find `status<>'pending'` and compensate). Recommended rule: cancel/reschedule of a `pending` appointment returns **409 "booking in progress"** (Open Question 3).

### Pattern 6: Slot generation (D-01)

DST-safe wall-clock -> UTC conversion, executed this session (`America/New_York`, `Asia/Karachi`):

```python
def local_to_utc(day: date, t: time, tz: ZoneInfo) -> datetime | None:
    naive = datetime.combine(day, t)
    aware = naive.replace(tzinfo=tz)                       # fold=0 -> first occurrence when ambiguous
    if aware.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != naive:
        return None                                        # nonexistent wall time (spring-forward gap)
    return aware.astimezone(timezone.utc)
# gap 02:30 on 2026-03-08 -> None; 03:30 -> 07:30Z; fold 01:30 on 2026-11-01 -> 05:30Z (EDT);
# 01:00-05:00 with 60-min slots on spring-forward day -> 3 slots; 00:00-04:00 on fall-back day -> 5 slots.
# Slot k = start_utc + k*duration in elapsed (UTC) time, so a DST jump never overlaps or leaves gaps.
```

Insert with `INSERT ... ON CONFLICT DO NOTHING` and **no conflict target**: probed to skip both a unique violation and a GiST exclusion violation (3 candidate slots incl. one overlapping -> 2 inserted; re-run -> 0 inserted). For this to be idempotent *and* to never resurrect an admin-cancelled slot, add a plain (non-partial) `UNIQUE (provider_id, start_time)` (probed as `zz_gen_uq`): a cancelled slot still occupies its (provider, start) key, so re-generation skips it. Add `CHECK (end_time > start_time)` (closes review item WR-07: a zero-length `tstzrange` is empty and never overlaps; probed, rejected with the CHECK).

Bound the requested range (for example reject a search/generate range wider than 31-90 days with 422) because the on-demand extension turns a GET into a write; a client asking for year 2100 must not generate tens of thousands of rows. Defend against an invalid clinic timezone (`ZoneInfoNotFoundError`, review item WR-13) by returning a clear 409/422, and ideally validate IANA names in Organization's schema.

### Pattern 7: Waitlist advance (SCHED-07)

Claim one entry atomically so two concurrent frees for the same provider cannot pick the same entry: `SELECT ... WHERE provider_id=:p AND status='waiting' AND window contains slot ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1` (probed: `FOR UPDATE SKIP LOCKED` selects work), set `status='matched'`, commit, then run the booking as a **child workflow** (`execute_child_workflow(BookingWorkflow, ...)`, deterministic ID from the new appointment). Outcomes: booked -> `booked`; saga business failure (billing) -> `failed` with reason, try the next entry (cap the loop, no infinite retry); **lost the slot race (unique violation) -> put the entry back to `waiting` and stop for this slot**, because the slot is no longer free.

### Pattern 8: Sweepers and the reaper (D-03, D-16, plus appointments)

All run as Temporal Schedules registered idempotently at worker start: create, catch `ScheduleAlreadyRunningError` (verified: a second `create_schedule` with the same ID raises it), `SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP)` (verified live).

1. **Hold reaper** (Scheduling): slots `held AND held_until < now()` -> `available`, and the owning `pending` appointment -> `failed(reason=hold_expired)` in the same transaction, one status-history row and one audit entry. Use `FOR UPDATE SKIP LOCKED`.
2. **Pending-appointment starter** (Scheduling, NEW vs CONTEXT): D-07 commits the `pending` row and then starts the workflow. A crash between the two leaves a `pending` appointment holding the slot with no workflow. Mirror D-16: add `workflow_started_at timestamptz NULL`; set it after a successful start (conditional update); the sweeper starts workflows for `pending AND workflow_started_at IS NULL AND created_at < now() - interval '30 seconds'` using Pattern 2 (duplicate starts are no-ops).
3. **Visit sweeper** (Visit): completed visits with `downstream_started_at IS NULL` (D-16), also run once at service start.

Hold TTL (10 min) must comfortably exceed the saga's worst-case bounded duration (sum of `schedule_to_close_timeout`s, planned at about 1-2 minutes). If a reaper runs while a saga is still alive, the saga's `confirm` is a conditional update (`WHERE held_by_appointment_id=:a AND status='held'`) that returns zero rows and routes to compensation, so state stays consistent either way. Do **not** set a workflow `execution_timeout` on the booking saga: a timeout terminates the workflow *without running compensation*; bounded activities make the saga reach compensation on its own.

### Pattern 9: Visit state machine (VISIT-01..05)

Transition = one transaction: `UPDATE visits SET status=:to, ... WHERE id=:v AND status=:expected RETURNING id` plus an INSERT into `visit_state_history`; zero rows -> re-read: current is the target or beyond -> 200 no-op; otherwise 409 (skipped state). For `completed`, set the completion columns and leave `downstream_started_at NULL` in the same transaction, commit, then Pattern 2 start of `visit-completed-{visit_id}`, then conditional `UPDATE ... SET downstream_started_at=now() WHERE id=:v AND downstream_started_at IS NULL`. Partial index `ON visits (completed_at) WHERE status='completed' AND downstream_started_at IS NULL` serves the sweeper; a composite index on `(clinic_id, status, created_at)` serves `GET /visits`.
Visit calls Scheduling for the appointment (`GET /internal/appointments/{id}` forwarding the front-desk token, clinic scope re-applied by Scheduling) and Provider for "is this caller the visit's provider" (`by-user` lookup, G3).

### Pattern 10: Service-token mint, audience, expiry (D-06) — **new work, not in CONTEXT's inventory**

Verified facts: the only mint function is `create_service_token(service_name)` with `sub=service:<name>`, `type=service`, `jti`, `iat`, `exp` (60 s) and its only call site is Identity's own outgoing reference check; nothing in Scheduling/Visit/Billing can obtain one. python-jose 3.5.0: when a token has `aud` and `decode(..., audience=None)` it raises `JWTClaimsError("Invalid audience")`; when the token has **no** `aud` the check is skipped entirely (so `aud` can never be *required* by passing `audience=`); `require_exp` defaults to `False` `[VERIFIED: jose/jwt.py _validate_aud and decode options, python-jose 3.5.0 source read this session]`.

Recommended design (needs user confirmation, A1/A2):
1. Identity adds an internal `POST /internal/service-tokens` ("client credentials"): the caller proves identity with a per-service secret (Compose secret file, compared with `hmac.compare_digest`), names the target audience, and gets a 60 s token with `sub=service:<caller>`, `aud=<target>`. One token per target audience, cached in-process by `(caller, audience)` and refreshed ~15 s before expiry. If Identity is unreachable the activity fails *retryably* (fail closed, Temporal retries).
2. `decode_token(..., audience: str | None)` gains the parameter and passes `options={"require_exp": True}` (closes WR-03); `require_internal_caller` passes its own service name as `audience` and **additionally rejects a `type=service` token whose `aud` is missing or does not include this service**; add an allowed-callers set per internal route (closes WR-02: today any valid service token passes every internal route).
3. User access tokens carry no `aud`, so they keep working everywhere; every existing caller of `decode_token` (`libs/auth/current_user.py:81`, `libs/auth/internal.py:93`, `services/identity/api/auth.py:390,431`) keeps working because the new parameter defaults to `None`.

### Anti-Patterns to Avoid
- **Check-then-insert for slot availability** as the guard: the index/constraint is the guard; the pre-check is only an early 409.
- **Workflow code importing SQLAlchemy/httpx/`libs.config`** (sandbox re-imports the module; settings objects build at import time). Keep `workflows/*.py` to `temporalio` + stdlib + `domain/contracts.py`.
- **A workflow module with top-level side effects.** The sandbox re-imports the defining module; a probe script that called `asyncio.run(main())` at import time failed worker validation (`RuntimeError: Failed validating workflow Wf`, caused by `asyncio.run() cannot be called from a running event loop`) until guarded by `if __name__ == "__main__"`.
- **`(str, Enum)` fields in workflow I/O dataclasses** (see Pitfall 1).
- **Running Alembic in the worker container** (the API image CMD runs `alembic upgrade head` first; the worker overrides the command and must wait for the API's health).
- **Compensation as deletion.** Forward transitions only; history rows are append-only.
- **Passing the caller's JWT into Temporal** or any PHI/coverage identifier into inputs, outputs, signals, memos or `ApplicationError` text.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Durable multi-step orchestration, retries, timers | A DB-polling state machine or Celery chains | Temporal workflows/activities | Replay, retry policy, durable timers, history UI (mandated; dashboard rationale) |
| Slot double-booking guard | Redis locks / `SELECT ... FOR UPDATE` loops as the only guard | Partial unique index + existing GiST exclusion constraint | Probed: 1 winner of 60; app locks have failure windows (models.py:11-18 docstring) |
| Idempotency arbitration | In-memory dedupe or Redis `SETNX` | `INSERT ... ON CONFLICT DO NOTHING` in the same transaction | Survives cache flush, atomic with the business row, probed under 40-way duplicates |
| Wall-clock -> UTC with DST | Manual offset arithmetic | `zoneinfo` + round-trip check (Pattern 6) | Gap/fold handled; probed |
| Periodic sweepers | cron, `asyncio` background loops in the API | Temporal Schedule with `ScheduleOverlapPolicy.SKIP` | Survives restarts, visible in UI, verified live |
| Workflow versioning | Copy-paste `WorkflowV2` classes ad hoc | `workflow.patched` / `deprecate_patch` + `Replayer` replay test | Official mechanism `[CITED: docs.temporal.io/develop/python/workflows/versioning]` |
| Trace propagation through Temporal | Hand-built headers | `TracingInterceptor` on client and worker | Built in, stable |
| Service-token signing outside Identity | Per-service keys or shared secrets | Identity mint endpoint (Pattern 10) | Keeps the "only Identity loads a private key" invariant and its test |
| Audit dedupe on retry | Check-then-insert in Mongo | Upsert with a unique dedupe key (Pitfall 7) | Insert-only writer duplicates on every retry |

**Key insight:** every "exactly once" in this phase is really "at-least-once delivery plus an idempotent, state-targeted effect". Make each effect idempotent at the row (conditional UPDATE, unique key) and the orchestration layer becomes free to retry aggressively.

## Common Pitfalls

### Pitfall 1: Temporal's default converter corrupts `(str, Enum)` dataclass fields
**What goes wrong:** a dataclass field typed `class E(str, Enum)` round-trips as a list of characters (`outcome=['c','o','n','f','i','r','m','e','d']`). Plain `Enum` is not JSON-serializable at all. `enum.StrEnum`, `uuid.UUID` and tz-aware `datetime` round-trip fine. `[VERIFIED: executed this session, temporalio 1.34.0 default converter]`
**Why it happens:** the default JSON converter's `value_to_type` treats the str-subclass enum as a sequence. The project's existing `SlotStatus(str, Enum)` (`services/scheduling/domain/enums.py:12`) is exactly this shape.
**How to avoid:** workflow/activity I/O dataclasses use plain `str` for statuses/outcomes (compare against `.value`), or switch every client and worker to `pydantic_data_converter` (round-trip verified for `(str, Enum)`, `StrEnum`, UUID, datetime, including with plain dataclasses). One rule, enforced by a unit test that round-trips every contract dataclass.
**Warning signs:** `if result.outcome == Outcome.CONFIRMED` silently false; the Temporal UI shows a string but code sees a list.

### Pitfall 2: Adding `aud` breaks every verifier that does not pass `audience`
**What goes wrong:** python-jose raises `Invalid audience` for any token that carries `aud` when `audience=None`, so adding `aud` to service tokens before every verifier is updated turns all internal calls into 401s. `[VERIFIED: python-jose 3.5.0 source]`
**How to avoid:** ship the `decode_token(audience=...)` change and the `require_internal_caller` change in the same plan as the mint endpoint; keep user tokens `aud`-less; add a test that a service token with the wrong `aud`, a missing `aud`, and an expired token are all rejected.

### Pitfall 3: Temporal workflow-ID uniqueness is not forever
**What goes wrong:** relying on `REJECT_DUPLICATE` as the only duplicate guard fails after the namespace retention (24h on the Compose `default` namespace). **How to avoid:** DB arbiters first (Pattern 1); workflow ID second; document in the dashboard.

### Pitfall 4: Lost trigger between commit and `start_workflow` (appointments)
**What goes wrong:** the API commits `pending`, crashes (or Temporal is down), and a pending appointment holds the slot forever. **How to avoid:** `workflow_started_at` marker + Pattern 8 starter; `Client.connect(lazy=True)` (exists in 1.34.0) so the API still boots and serves reads when Temporal is down; return 202 anyway and let the sweeper start the workflow, rather than 503 after the commit.

### Pitfall 5: Reaper races an in-flight saga
**What goes wrong:** a hold expires while the saga is still running, the reaper frees the slot, and the saga then confirms a slot it no longer holds. **How to avoid:** conditional `confirm` (`WHERE status='held' AND held_by_appointment_id=:a`), zero rows -> compensate; TTL much larger than the bounded saga duration (Pattern 8).

### Pitfall 6: Unbounded activity retries hang instead of compensating
**What goes wrong:** the default activity retry policy never gives up, so a persistently down Billing stalls the saga forever. **How to avoid:** `schedule_to_close_timeout` + `maximum_attempts` on every forward activity; compensations may retry longer but must stay idempotent; an unrecoverable compensation leaves the workflow open and visible (acceptable, document it).

### Pitfall 7: `write_audit` duplicates on activity retry
**What goes wrong:** `libs/audit/writer.py:83` is `await collection.insert_one(document)`; a retried activity writes the audit entry twice, and the design says "an audit entry per step". **How to avoid:** extend `write_audit` with an optional `dedupe_key` (for example `f"{workflow_id}:{step}"`) written as `update_one({"dedupe_key": k}, {"$setOnInsert": doc}, upsert=True)` backed by a partial unique index added in `libs/audit/mongo_indexes.py`; catch `DuplicateKeyError` from concurrent upserts. Keep the parameter optional so Phase 1/1.1 call sites are untouched `[ASSUMED: standard MongoDB upsert/duplicate-key behavior, not exercised this session]`. Also note review item WR-04: audit is not atomic with the Postgres commit, so keep audit as the *last* step of each idempotent activity.

### Pitfall 8: Time-skipping test server does not skip database time
**What goes wrong:** tests that rely on `now()` in SQL (hold expiry, idempotency expiry) cannot be advanced by `WorkflowEnvironment.start_time_skipping()`. **How to avoid:** set `held_until`/`expires_at` explicitly in the past in the test, then invoke the reaper activity directly (`ActivityEnvironment`, verified to run an activity in isolation) or via the workflow.

### Pitfall 9: The unique index fires on paths other than the front door
**What goes wrong:** reschedule swap and waitlist auto-book can also hit `uq_appointments_active_slot`. **How to avoid:** every code path maps that violation to a business outcome (409 / compensate / put-back-to-waiting), never a 500 or an infinite activity retry. Add the constraint name to a small helper (`is_slot_taken(IntegrityError)`) so the check is not copy-pasted.

### Pitfall 10: Gateway rate limit throttles the load demo
**What goes wrong:** the default budget is `300` requests/minute/IP (`infra/docker-compose.yml:337`), so a "100 parallel bookings" demo through the gateway returns 429s that look like failures. The in-process test suite lifts the limits itself (`tests/conftest.py`). **How to avoid:** the demo script documents `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` for that run; the graded evidence is the pytest concurrency test, with the script as the live illustration.

### Pitfall 11: Postgres connection budget
**What goes wrong:** `max_connections=300` (compose line 50) is shared by Temporal (~40 idle, per the compose comment) and every process's SQLAlchemy pool (default 5 + 10 overflow). Two more APIs, two workers and 2 gunicorn workers each raise the theoretical peak. Idle usage measured this session: 35 connections. **How to avoid:** make `pool_size`/`max_overflow` configurable for the new services and keep the concurrency test's parallelism bounded (it tests the constraint, not pool exhaustion), or raise the limit; do not assume the existing headroom.

### Pitfall 12: Existing "GET-only" invariants
**What goes wrong:** tests assert internal calls are GET-only (`tests/patient/test_patient_references.py:327`, `tests/libs/test_service_client.py:226`, `tests/identity/test_service_token.py:237`). They scope to existing flows, so a *new* write-capable client does not break them, but the shared `libs/service_client.py` header states "the client has no write method at all" (lines 22-23). **How to avoid:** add a separate `libs/internal_client.py` for activity writes; do not retrofit write methods into `service_client.py`.

### Pitfall 13: Isolation tests enumerate services
**What goes wrong:** `tests/libs/test_service_isolation.py` hard-codes `OWNED_TABLES`, `BASES`, `SETTINGS` (lines 44-66), forbids cross-service imports and PHI column names (`PHI_COLUMNS = {"date_of_birth", "phone", "address"}`, line 71) outside Patient. New tables and the `visit`/`billing` packages must be added to those maps in the same plan, and no new table may have a column named `phone` or `address`. Workflow contracts shared between the API and worker are fine because they live in the same service; anything Visit and Scheduling both need must live in `libs/` (which may not import `services`).

### Pitfall 14: Provider/Organization data drift after slot generation
Materialized slots do not follow later template edits, provider deactivation, or a provider changing department (and therefore clinic). Booking re-validates provider/clinic at booking time (D-09 step 1), which is the guard; document the limitation and give admins a way to cancel generated slots. `clinic_id` on `provider_slots` is a denormalized copy taken at generation.

## Code Examples

> All SQL/Temporal API shapes below were executed this session unless marked. Table and column names are proposals (`[ASSUMED design]`, planner's discretion per CONTEXT).

### 1. Constraints migration (`scheduling_0002`) — SQL semantics VERIFIED by probe
```sql
ALTER TABLE provider_slots
  ADD COLUMN clinic_id uuid NOT NULL,                  -- table is empty in dev (SELECT count(*) = 0 verified)
  ADD COLUMN held_until timestamptz,
  ADD COLUMN held_by_appointment_id uuid,
  ADD CONSTRAINT ck_provider_slots_end_after_start CHECK (end_time > start_time),
  ADD CONSTRAINT uq_provider_slots_provider_start UNIQUE (provider_id, start_time);

CREATE TABLE appointments (
  id uuid PRIMARY KEY,                                  -- generated by the app (key row references it)
  clinic_id uuid NOT NULL, patient_id uuid NOT NULL, provider_id uuid NOT NULL,
  slot_id uuid NOT NULL REFERENCES provider_slots(id), -- intra-service FK is allowed (test_no_foreign_key_crosses_a_service)
  status varchar(16) NOT NULL CHECK (status IN ('pending','confirmed','cancelled','failed')),
  failure_reason text, version int NOT NULL DEFAULT 0,
  workflow_started_at timestamptz, created_by uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_appointments_active_slot ON appointments (slot_id)
  WHERE status IN ('pending','confirmed');               -- D-02 predicate, verbatim

CREATE TABLE idempotency_keys (
  caller_user_id uuid NOT NULL, key text NOT NULL, request_hash char(64) NOT NULL,
  appointment_id uuid NOT NULL, expires_at timestamptz NOT NULL,
  PRIMARY KEY (caller_user_id, key));
```
SQLAlchemy emits the partial unique index correctly: `Index("uq_appointments_active_slot", t.c.slot_id, unique=True, postgresql_where=text("status IN ('pending','confirmed')"))` compiled to `CREATE UNIQUE INDEX uq_appointments_active_slot ON appointments (slot_id) WHERE status IN ('pending','confirmed')` `[VERIFIED: compiled with the project's SQLAlchemy 2.0.52]`. Provide a downgrade for every revision (global rule: no destructive DB change without a rollback).

Optional, recommended (it gives the SCHED-02 "prevent conflicts" step real teeth): denormalize `slot_start`/`slot_end` onto `appointments` and add `EXCLUDE USING gist (patient_id WITH =, tstzrange(slot_start, slot_end) WITH &&) WHERE (status IN ('pending','confirmed'))` so one patient cannot hold two overlapping appointments. Same pattern as the existing provider constraint; `btree_gist` is already provisioned. Needs user confirmation (Open Question 2).

Existing values this builds on, quoted verbatim:
- `[VERIFIED: services/scheduling/domain/enums.py:19-22]` `AVAILABLE = "available"`, `HELD = "held"`, `BOOKED = "booked"`, `CANCELLED = "cancelled"`
- `[VERIFIED: services/scheduling/repository/models.py:66-76]` `"status IN ('available','held','booked','cancelled')"`, `name="ck_provider_slots_status_valid"`, `where=text("status <> 'cancelled'")`, `name="ex_provider_slots_no_overlap"`
- `[VERIFIED: services/scheduling/migrations/versions/scheduling_0001_provider_slots.py:34]` `revision: str = "scheduling_0001"` (so the new revision is `down_revision = "scheduling_0001"`)

### 2. Conditional, idempotent state steps — VERIFIED SQL semantics
```sql
-- reserve_slot activity: succeeds on first run AND on retry by the same appointment
UPDATE provider_slots
   SET status='held', held_by_appointment_id=:a, held_until=now() + make_interval(mins => :ttl)
 WHERE id=:s AND (status='available' OR (status='held' AND held_by_appointment_id=:a))
RETURNING 1;           -- 0 rows -> check whether it is already booked by :a (idempotent success) else SlotUnavailable (non-retryable)

-- confirm activity (single local tx)
UPDATE provider_slots SET status='booked', held_until=NULL
 WHERE id=:s AND status='held' AND held_by_appointment_id=:a RETURNING 1;
UPDATE appointments SET status='confirmed', version=version+1, updated_at=now()
 WHERE id=:a AND status='pending' RETURNING 1;           -- 0 rows -> not pending any more -> compensate
```

### 3. Worker entrypoint — API shapes VERIFIED (TracingInterceptor wiring not exercised)
```python
# services/scheduling/worker.py  (python -m services.scheduling.worker)
client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace,
                              interceptors=[TracingInterceptor()])          # data_converter only if Pydantic route chosen
acts = SchedulingActivities(session_factory=..., http=default_internal_client, tokens=ServiceTokenProvider(...), mongo=...)
await ensure_schedules(client)                                              # create_schedule + catch ScheduleAlreadyRunningError
async with Worker(client, task_queue=SCHEDULING_TQ,
                  workflows=[BookingWorkflow, CancelWorkflow, RescheduleWorkflow, WaitlistAdvanceWorkflow, ReaperWorkflow],
                  activities=[acts.validate, acts.reserve_slot, ...],
                  graceful_shutdown_timeout=timedelta(seconds=20)):
    await stop_event.wait()
```
Verified this session: `Worker(..., graceful_shutdown_timeout=...)`, class-method activities by name, and recovery when a worker is shut down mid-activity: a second worker finished the workflow (`result after restart: A1:done-by-worker2`, 2 `ActivityTaskScheduled` events) after the 5 s `start_to_close_timeout` elapsed against the live Compose server. This is the shape of the D-19 "worker-crash-mid-saga" test (about 6 s of real time, so it belongs in the Compose-Temporal tier, not the time-skipping tier).

### 4. Idempotent schedule registration — VERIFIED
```python
try:
    await client.create_schedule("scheduling-reaper", Schedule(
        action=ScheduleActionStartWorkflow("ReaperWorkflow", id="scheduling-reaper-run", task_queue=SCHEDULING_TQ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(seconds=60))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP)))
except ScheduleAlreadyRunningError:
    pass
```

### 5. Concurrency test shape (D-19) — pattern VERIFIED at the SQL level
Two tiers: (a) repository-level: 50-60 `asyncio.gather` calls of the accept transaction against real Postgres with distinct keys and distinct patient IDs, assert `count(*) WHERE slot_id=:s AND status IN ('pending','confirmed') == 1`, exactly one success and the rest `SlotTaken`; plus a raw-SQL variant that bypasses all application code (proves the constraint itself). (b) HTTP-level through the in-process mesh with a Temporal test worker running in the same loop: N parallel `POST /api/appointments`, assert one 202 and N-1 409, then await the workflow and assert exactly one `confirmed`. Use a front-desk token with N patient IDs (a patient token may only book for itself).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `temporalio` 1.32.0 pin (CLAUDE.md) | 1.34.0 current | 2026-09-30 | Recommended pin; probes ran on it |
| `TracingInterceptor` only | `OpenTelemetryPlugin` (experimental) | in 1.34.0 | Stay on the stable interceptor |
| Optional `id_conflict_policy` unknown | `USE_EXISTING` / `FAIL` / `TERMINATE_EXISTING` | present in 1.34.0 | Enables the safe "start once" helper (Pattern 2) |
| Cron for sweepers | Temporal Schedules | present in 1.34.0 | Verified on server 1.29.7 |
| Idempotency-Key as a vendor convention | IETF `draft-ietf-httpapi-idempotency-key-header-07` (Oct 2025) | draft | Matches D-08: same key + different payload -> 422; retry while the first is in flight -> 409 `[CITED: ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.html]` |

**Deprecated/outdated:**
- `temporalio/auto-setup`: fine for local dev; the highest tag that exists is `1.29.7` (STATE.md); label it non-production in the dashboard (deferred idea, notes only). `tctl` prints an end-of-support notice; use the `temporal` CLI in docs/runbooks where possible.
- Motor, `aioredis`, legacy Jaeger exporter: already excluded by CLAUDE.md.

## Gaps in existing code that Phase 2 must close (planner checklist)

| # | Gap (verified) | Evidence | Needed work |
|---|----------------|----------|-------------|
| G1 | No way for Scheduling/Visit/Billing to obtain a service token | `create_service_token` sole call site `services/identity/api/auth.py:140` `bearer_token=create_service_token("identity")`; mint fn `services/identity/security/signing.py:70` | Identity mint endpoint + per-service secrets + `ServiceTokenProvider` (Pattern 10) |
| G2 | Service tokens carry no `aud`; `exp` not required; any service token passes any internal route | `signing.py:80-86` claims: `sub`, `type`, `jti`, `iat`, `exp` only; review WR-02/WR-03 (deferred-items.md) | `audience` param, `require_exp`, caller allow-list, tests |
| G3 | Missing internal lookups | Patient has no `/internal` router (`services/patient/app.py:42-43` mounts only health + patients); `ProviderRef` = `id`, `department_id`, `is_active` only (`services/provider/api/internal.py:41-44`); `ClinicRef` = `id`, `is_active` only (`services/organization/api/internal.py:42-44`) while `Clinic.timezone` exists (`services/organization/repository/models.py:56`); `list_availability` exists (`provider_repo.py:208`) | Patient: `GET /internal/patients/{id}?clinic_id=` and `/by-user/{user_id}?clinic_id=` (the repo has no id-only lookup by design: `patient_repo.get_patient(db, patient_id, clinic_id)`); Provider: `/{id}/availability` and `/by-user/{user_id}`; Organization: add `timezone` to `ClinicRef`; Scheduling: `/internal/appointments/{id}`; Billing internal endpoints |
| G4 | Reference client is GET-only | `libs/service_client.py:22` "GET only (the client has no write method at all)" | New `libs/internal_client.py` for activity POSTs |
| G5 | Non-idempotent audit writer | `libs/audit/writer.py:83` `await collection.insert_one(document)` | `dedupe_key` upsert (Pitfall 7) |
| G6 | Default converter vs `(str, Enum)` | Pitfall 1 | Plain `str` I/O or Pydantic converter, plus round-trip unit test |
| G7 | Appointment lost-trigger window | D-07 vs D-16 | `workflow_started_at` + starter (Pattern 8) |
| G8 | Gateway route table and URL settings lack the new services | `services/gateway/routing.py:25-32` table has no appointments/slots/waitlist/visits/billing; `libs/config.py:53-57` has URL fields only for identity, organization, provider, patient, scheduling; `resolve_upstream` does `getattr(settings, f"{upstream_name}_service_url")` | Add `visit_service_url`, `billing_service_url`, five route-table rows (existing `/api/<x>` prefix style), gateway `depends_on`, routing tests |
| G9 | db-init and Compose lack visit/billing | `infra/postgres-init/create-service-databases.sh:20` `SERVICES=(identity organization provider patient scheduling)` | Add `visit billing`, `VISIT_DB_PASSWORD`/`BILLING_DB_PASSWORD` in db-init env, Dockerfiles, two APIs, two workers, healthchecks, `infra/prometheus.yml` jobs |
| G10 | Existing dashboard flow text is stale for this design | `docs/architecture-dashboard.html` `#flows` step list says reminder job to Celery, `AppointmentBooked` via outbox, and a different step order | Rewrite per Dashboard section below |
| G11 | WR-07 (zero-length slots) and WR-13 (timezone never validated) | deferred-items.md | CHECK constraint in `scheduling_0002`; defensive `ZoneInfo` handling (and ideally validation in Organization) |

## Billing model (BILL-01, BILL-02)

Recommended state set and idempotency (names are discretion): `billing_records(id, appointment_id UNIQUE, clinic_id, status, reason_code, created_at, updated_at)` with `status IN ('pending','verified','failed','voided','refund_pending','refunded','no_refund','finalized')` and append-only `billing_status_history(record_id, from_status, to_status, reason_code, idempotency_key, created_at, UNIQUE(record_id, idempotency_key))`. Endpoints are **state-targeted**, so a retry is naturally idempotent (`void` on an already voided record is a no-op returning the current state), and the `Idempotency-Key` header (D-06) is stored on the history row for traceability and de-duplication. Precheck: one call creates `pending`, evaluates the rule, and records `verified|failed` in the same transaction (two history rows) so there is no observable half-state. Do not persist the raw `coverage_ref` (only a coarse `coverage_kind` and `reason_code`). The configured failing prefix comes from an env var on Billing (D-10). Cancel before the cutoff: `refund_pending` then `refunded` (stub settlement, recorded as two history rows); inside the cutoff: `no_refund`. Reschedule: re-link only. Visit-completed: `finalized`. Expose a read-only, clinic-scoped, front-desk/admin `GET /api/billing/...` so BILL-01/02 are demonstrable through the gateway.

## Slot, appointment, waitlist and visit state vocabularies (recommended; planner may rename)

| Aggregate | States | Notes |
|-----------|--------|-------|
| Slot | `available`, `held`, `booked`, `cancelled` | Fixed by the existing CHECK; `cancelled` = administratively withdrawn |
| Appointment | `pending`, `confirmed`, `cancelled`, `failed` | Partial-index predicate covers `pending`,`confirmed` only (D-02). **Do not add a `completed` appointment status** unless the index predicate is extended, or a completed appointment would free its slot for rebooking (Open Question 4) |
| Waitlist entry | `waiting`, `matched`, `booked`, `failed`, `cancelled` | `matched` is the claim state that makes concurrent advances safe |
| Visit | `checked_in`, `in_progress`, `completed` | D-15 |
| Billing | see Billing model | |

## RBAC and scope matrix (D-18; recommended where CONTEXT is silent)

| Operation | patient | provider | front_desk | admin |
|-----------|---------|----------|-----------|-------|
| Book / cancel / reschedule / view appointment | own only (via Patient `by-user`) | read own (discretion) | own clinic | any clinic |
| Search slots | yes | yes | yes | yes |
| Generate slots, cancel a slot | no | no | own clinic | any |
| Waitlist add/cancel | own | no | own clinic | any |
| Visit check-in | no | no | own clinic | any |
| Visit start/complete | no | own visits | own clinic | any |
| Visit list | no | no | own clinic | any |
| Billing read | no | no | own clinic | any |

Roles are `patient`, `provider`, `front_desk`, `admin` `[VERIFIED: libs/auth/roles.py:15-18]` `PATIENT = "patient"`, `PROVIDER = "provider"`, `FRONT_DESK = "front_desk"`, `ADMIN = "admin"`. Existing guards to reuse: `require_role(...)` (public routes, access tokens only) and `require_internal_caller(...)` (internal routes; accepts access or service tokens; role allow-list applies to user callers only, `libs/auth/internal.py`). Out-of-scope and missing IDs return one identical 404 (established pattern).

## Dashboard update (DASH deliverable; self-contained HTML)

`docs/architecture-dashboard.html` is a single 70.9 KB file with CSS custom properties and ordered-list "steps" cards; sections `#overview #architecture #decisions #patterns #flows #qa #roadmap`; no external assets `[VERIFIED: file read this session]`. Update, without adding any external script or stylesheet (keep it self-contained, as DASH-01 requires): (1) replace the `#flows` booking-saga card with the implemented order and the compensation order, add cancel, reschedule, waitlist-advance and the visit state machine; draw state diagrams as **hand-written inline SVG** using the existing CSS variables; (2) add a "Why Temporal, not Celery, for the saga" decision card in `#decisions` and the layered-idempotency story (DB arbiters first, workflow ID second, retention caveat); (3) state the interim mechanisms and that Phase 3 replaces them: sweeper (-> outbox), notification stub (-> Celery), polling (-> events); (4) correct stale text: Celery reminder job, `AppointmentBooked` via outbox, "implemented in Phase 2" wording, the `meta-row` ("Dashboard v2 · Phase 1.1" -> v3 · Phase 2) and the footer; mark Phase 1.1 complete and Phase 2 active in `#roadmap`; (5) add Q&A entries (double-booking proof, idempotency, why not Celery, what if a worker dies, what is not yet verified). Also update `README.md`, `docs/architecture.md`, `docs/erd.md`/`erd.html` for the two new services and tables. The deferred-items interim risk statement still applies: do not describe logout, PHI log-safety or database isolation as fully hardened.

Order discrepancy the dashboard must address: SCHED-02 and roadmap success criterion 1 list `schedule notification` **before** `billing pre-check`; D-09's explicit list has billing first. See Open Question 1.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Identity gets a client-credentials style `POST /internal/service-tokens` with per-service secrets as the way non-Identity processes obtain service tokens | Pattern 10, G1 | If the user prefers another scheme (per-service keys in JWKS, mTLS), the whole internal-auth plan changes; costly because D-06 sets the contract for all Phase 2 calls |
| A2 | One token per target audience, 60 s, cached in-process | Pattern 10 | Low: tuning only |
| A3 | MongoDB upsert with a partial unique `dedupe_key` index is the right idempotent-audit mechanism and concurrent upserts surface `DuplicateKeyError` that can be ignored | Pitfall 7 | Audit duplicates or spurious failures on retry; not exercised this session |
| A4 | `TracingInterceptor` on the API-process client and the worker yields one connected Jaeger trace | Pattern 4, D-18 | Trace could fragment at the workflow boundary; needs the span-exporter test described in Validation, not run here |
| A5 | Cancel/reschedule of a `pending` appointment returns 409 "booking in progress" | Pattern 5 | If product wants cancel-during-booking, a signal-based design is needed |
| A6 | Patient-overlap `EXCLUDE` constraint on appointments is desired | Code Example 1 | Extra denormalized columns if not wanted; harmless to drop |
| A7 | Task queue names `scheduling-tq` / `visit-tq` | Architecture | Costly to change once histories exist (D-05); confirm before first run |
| A8 | The accept endpoint returns 202 (not 503) when Temporal is unreachable after the commit, relying on the starter sweeper | Pitfall 4 | Clients see `pending` longer during a Temporal outage |
| A9 | A replayed idempotent request returns the original appointment ID and status URL with the *current* status | Pattern 1 | CONTEXT says "returns the original response"; a strictly byte-identical replay would hide progress |
| A10 | Hold TTL 10 min, reaper every 60 s, starter grace 30 s, activity `schedule_to_close` about 60-120 s | Pattern 8 | Tuning; all in CONTEXT's discretion |
| A11 | Refund cutoff is decided in the API request and passed as an enum string | Pattern 5 | If decided inside the workflow it must use `workflow.now()` |
| A12 | New suite runtime stays acceptable: full suite baseline is 207 s | Validation | If Phase 2 doubles it, per-task commands must stay scoped |

## Open Questions

1. **Booking step order: SCHED-02 text vs D-09**
   - What we know: SCHED-02 and success criterion 1 say validate -> reserve -> prevent conflicts -> schedule notification -> billing pre-check -> confirm. D-09 says "follows SCHED-02" but lists billing before notification, and has no explicit "prevent conflicts" step.
   - What's unclear: which order to implement and document.
   - Recommendation: implement D-09's list (billing then notification) because the notification is best-effort and should fire only after the last fallible step, and map "prevent conflicts" to the reserve step's constraints (plus the optional patient-overlap constraint). State the deviation from the requirement text explicitly in the plan and the dashboard. The notification stub is only an intent, so either order is safe for correctness.
2. **Optional patient-overlap exclusion constraint** — recommended, low cost, gives "prevent conflicts" a data-layer story. Needs a yes/no (A6).
3. **Cancel/reschedule while `pending`** — recommend 409 (A5). Also: should a cancel be refused once the slot time has passed or a visit exists? Recommend: refuse (409) if the slot has started; do not cross-call Visit in Phase 2.
4. **Appointment `completed` status** — not required by any criterion; recommend not adding it (visit status lives in Visit). If wanted, extend the partial-index predicate in the same migration.
5. **Service-token scheme confirmation (A1)** — the single highest-risk assumption; confirm before planning the first wave.
6. **gateway body-size cap and shared-Redis items** deferred-items.md suggests "the first plan of Phase 2, before any public exposure" for the body-size cap, while the later decision (user, 2026-10-12) defers all review findings until after Phase 2. Follow the later decision; WR-02, WR-03 and WR-07 are pulled in only because Phase 2 directly depends on them (G2, G11).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker + Compose stack | integration/e2e tests, live probes | yes | Docker 29.1.5; 17 smarthealth containers up, services healthy | none needed |
| Temporal server (Compose) | e2e, crash test, Schedules | yes | server 1.29.7 (`temporal version 1.29.7`), default namespace retention 24h | none |
| Postgres 15 | all DB work | yes | `postgres:15-trixie`, `max_connections=300`, 35 idle connections | none |
| Mongo / Redis | audit, denylist | yes | `mongo:8.0`, `redis:8.2-bookworm` | none |
| Python | all | yes | `.venv` 3.12.15 (project pins `>=3.12,<3.13`) | none |
| Temporal time-skipping test server | workflow unit tests | yes | auto-downloaded and run this session (`RESULT hi x`); needs network on first run | `WorkflowEnvironment.from_client(await Client.connect("localhost:7233"))` against Compose (exists in 1.34.0) |
| `temporalio` in project venv | everything | **not yet installed** (`pip list` in `.venv` shows no temporalio) | target 1.34.0 | install after the human-verify checkpoint |
| Semgrep | global rule: document Semgrep status for BE changes | yes | `/Library/Frameworks/Python.framework/Versions/3.14/bin/semgrep` | none |
| Jaeger UI | trace evidence | yes (container up) | all-in-one 1.76.0 | in-process span exporter test |
| System tzdata | slot generation | yes | present in `python:3.12-slim` image | none |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** time-skipping binary needs network once (fallback above).

Side effects of this research session on the dev environment (all local, disposable): probe workflows/schedules with IDs `research-*` were created on the Compose Temporal and are closed or deleted (history expires at 24h); scratch tables `zz_probe_*` were created and dropped in `scheduling_db`; the baseline full-suite run `flushdb()`-ed the dev Redis, as test fixtures do by design (review item IN-04).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 + pytest-asyncio 1.4.0 (`asyncio_mode = "auto"`) `[VERIFIED: .venv pip list; pyproject.toml pytest config]` |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (markers `integration`, `e2e`) |
| Quick run command | `.venv/bin/pytest tests/scheduling -q` (9 passed in 1.85 s, baseline) |
| Full suite command | `.venv/bin/pytest tests -q -m "not e2e"` (baseline: 365 passed, 1 deselected, 207 s) |

New test directories follow the existing layout: `tests/scheduling/`, `tests/visit/`, `tests/billing/`, plus `tests/workflows/` for Temporal-environment tests. Add `visit` and `billing` to `HARNESS_SERVICES` (`tests/conftest.py:85-92`) and to the isolation maps (Pitfall 13).

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCHED-01 | Book against an available slot -> 202 then `confirmed` after saga | integration (mesh + Temporal env) | `.venv/bin/pytest tests/scheduling/test_booking_api.py -x` | Wave 0 gap |
| SCHED-02 | Saga order, confirm only at the end, notification failure does not fail booking | workflow (time-skipping) | `.venv/bin/pytest tests/workflows/test_booking_workflow.py -x` | Wave 0 gap |
| SCHED-03 | N parallel bookings, exactly one active appointment (repo-level, raw-SQL, and HTTP-level) | integration (real Postgres) | `.venv/bin/pytest tests/scheduling/test_concurrent_booking.py -x` | Wave 0 gap |
| SCHED-04 | Same key+body replays; same key+different body 422; 40-way duplicates yield one appointment | integration | `.venv/bin/pytest tests/scheduling/test_idempotency.py -x` | Wave 0 gap |
| SCHED-05 | Reschedule swap atomic; failed new-slot hold leaves original untouched; unique-violation path | integration + workflow | `.venv/bin/pytest tests/scheduling/test_reschedule.py -x` | Wave 0 gap |
| SCHED-06 | Cancel: slot back to `available`, refund vs no_refund by cutoff, cancel twice -> 200 no-op | integration + workflow | `.venv/bin/pytest tests/scheduling/test_cancel.py -x` | Wave 0 gap |
| SCHED-07 | Waitlist FIFO by provider/window, auto-book, failed entry skipped, lost race returns to waiting | integration + workflow | `.venv/bin/pytest tests/scheduling/test_waitlist.py -x` | Wave 0 gap |
| SCHED-08 | Forced billing failure: compensation order, `failed` reason, slot `available`, billing `voided`, audit entry per step, no hard delete | workflow + integration | `.venv/bin/pytest tests/workflows/test_booking_compensation.py -x` | Wave 0 gap |
| SCHED-08 | Worker killed mid-saga resumes on a new worker | e2e vs Compose Temporal (~6 s) | `.venv/bin/pytest tests/e2e/test_worker_crash.py -m e2e -x` | Wave 0 gap |
| VISIT-01..03 | Check-in needs `confirmed`; start/complete idempotent; skip -> 409; concurrent duplicates fire once | integration | `.venv/bin/pytest tests/visit -x` | Wave 0 gap |
| VISIT-04 | Completion commits first; workflow started; sweeper recovers a null marker | integration + workflow | `.venv/bin/pytest tests/visit/test_downstream.py -x` | Wave 0 gap |
| VISIT-05 | `GET /visits` clinic-scoped filters | integration | `.venv/bin/pytest tests/visit/test_visit_list.py -x` | Wave 0 gap |
| BILL-01/02 | pending -> verified/failed, deterministic failing prefix, cancel -> refund states, idempotent retries | integration | `.venv/bin/pytest tests/billing -x` | Wave 0 gap |
| (cross) | Service token: mint auth, `aud` match/mismatch/missing, `exp` required, caller allow-list | unit/integration | `.venv/bin/pytest tests/libs/test_service_tokens.py tests/identity/test_service_token.py -x` | partial (extend existing) |
| (cross) | Workflow contract round-trip (no `(str, Enum)`), no PHI in histories | unit | `.venv/bin/pytest tests/workflows/test_contracts.py -x` | Wave 0 gap |
| (cross) | Replay safety after any workflow edit | `Replayer` test | `.venv/bin/pytest tests/workflows/test_replay.py -x` | Wave 0 gap |
| (cross) | One connected trace across API -> workflow -> activity (A4) | integration (in-process span exporter, as `tests/e2e/test_trace_propagation.py` does for the mesh) | `.venv/bin/pytest tests/workflows/test_trace.py -x` | Wave 0 gap |
| DASH | Dashboard remains self-contained (no `http(s)://` script/link assets) and contains the new sections | static check | `.venv/bin/pytest tests/docs/test_dashboard.py -x` | Wave 0 gap |

### Sampling Rate
- **Per task commit:** the service's own directory (for example `.venv/bin/pytest tests/scheduling tests/workflows -q`), well under the 207 s full run.
- **Per wave merge:** `.venv/bin/pytest tests -q -m "not e2e"` (about 3.5 min today and growing).
- **Phase gate:** full suite green plus `-m e2e` against the up stack before `/gsd-verify-work`.

### Wave 0 Gaps
- [ ] `tests/workflows/conftest.py` - `WorkflowEnvironment.start_time_skipping()` fixture, a Worker factory that injects the in-process `mesh`, and a fixture that sets `app.state.temporal_client`
- [ ] `tests/scheduling/conftest.py` extensions - committed-data helpers (the current fixture rolls everything back, which cannot test concurrent transactions; concurrency tests need separate committed sessions and explicit cleanup)
- [ ] `tests/conftest.py` - add `visit`, `billing` to `HARNESS_SERVICES`
- [ ] Seed helpers for clinic -> department -> provider -> template -> patient -> generated slots
- [ ] Update `tests/libs/test_service_isolation.py` maps for the new services
- [ ] Framework install: `temporalio==1.34.0` (after the human-verify checkpoint)

## Security Domain

`security_enforcement` is enabled in `.planning/config.json` (`security_asvs_level: 1`, `security_block_on: high`).

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes | RS256/JWKS verification already shared; service-token mint authenticated by per-service secret with constant-time compare; short-lived, per-audience tokens |
| V3 Session Management | yes (service tokens) | 60 s expiry, `require_exp`, Redis `jti` denylist check already in `require_internal_caller`; fail closed (503) on Redis/JWKS trouble |
| V4 Access Control | yes | `require_role`/`require_internal_caller` allow-lists, clinic scope from explicit params, identical 404 for missing vs out-of-scope, patient-self rule via Patient lookup, per-route allowed-service set |
| V5 Input Validation | yes | Pydantic models with bounds on every body/param; bounded slot-generation range; canonical-body hashing; `Idempotency-Key` length/charset limit |
| V6 Cryptography | yes | Reuse existing signing; `hmac.compare_digest` for the mint secret; secrets via Compose secret files, never in env dumps or images (existing key-file pattern) |
| V7 Error Handling and Logging | yes | PHI-free structlog; IDs/enums only in workflow history, activity errors, spans and audit `changes`; `hide_parameters` concern (review CR-02) must not be reintroduced in new services |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Double-booking via race | Tampering | Partial unique index + exclusion constraint (data layer) |
| Replay / duplicate request creating a second appointment | Tampering/Repudiation | Key arbitrated in the accept transaction; hash mismatch 422 |
| Stolen/replayed service token against another service | Spoofing/Elevation | `aud` per target, required for service tokens, 60 s TTL, caller allow-list, denylist |
| Service-token mint abuse | Spoofing | Per-service secret, internal-only route (never in the gateway table), rate/lockout consideration |
| IDOR on appointments/visits/billing across clinics | Information disclosure | Clinic-scoped lookups, identical 404, patient-self check |
| PHI/identifier leakage into Temporal history or spans | Information disclosure | IDs and enums only; `coverage_ref` read inside the activity; no `ApplicationError` payloads with values; a unit test scans contract dataclasses and a history sample |
| Resource exhaustion via slot generation on a GET | DoS | Cap requested range; idempotent insert; per-request bound |
| Poisoned retry loops | DoS | Bounded retries, non-retryable business errors, `SKIP` overlap on schedules |
| Audit tampering or loss | Repudiation | Append-only history tables plus Mongo audit with dedupe key; note WR-04 (non-atomic) as a recorded limitation |

Organizational note: this repository is a learning/assignment build; the interim risk statement in `deferred-items.md` (local/dev only, no real patient data, do not describe logout/PHI log-safety/DB isolation as hardened) must carry into any README or dashboard text this phase writes. Whether any control is "in place" for Emumba production use is unverified and out of scope here; confirm with the control owner before any such claim.

## Project Constraints (from CLAUDE.md)

From `/Users/emumba/smart Health/.claude/CLAUDE.md` (treated as locked-decision authority):
- Stack is mandated (Python/FastAPI/Postgres/NoSQL/Redis/Celery+RabbitMQ/Kafka+Registry/Temporal, Docker Compose, Prometheus/Grafana/Jaeger/OTel); deviations must be justified.
- Booking saga is Temporal's job; Celery is for discrete single-step jobs and is Phase 3 here.
- Reliability: partial failures must not corrupt scheduling state; workflows idempotent, recoverable, no double-processing.
- Async only: SQLAlchemy 2.0 async + asyncpg; no sync psycopg2 in `async def` routes; no `requests`, use httpx; no `aioredis`; Motor is excluded (PyMongo `AsyncMongoClient`).
- Pydantic v2 models as the schema layer; do not pass raw Pydantic instances through Celery args.
- OpenTelemetry via OTLP only (no legacy Jaeger exporter); structlog JSON logs correlated with trace IDs.
- `temporalio/auto-setup` is not production; label it so in docs. SDK and server are versioned independently; re-verify before locking (done here: SDK 1.34.0 vs server 1.29.7).
- Compose: `healthcheck:` + `depends_on: condition: service_healthy`; one `.env` for app config and Compose.
- Deliverables: clean repo structure, meaningful commits, README (overview, architecture diagram, setup, API overview, stack), technical docs (service breakdown, data/event flows, key design decisions, assumptions/tradeoffs).
- GSD workflow enforcement: file changes go through a GSD command (this research is part of `/gsd-plan-phase`).
- CLAUDE.md's tag table is stale for `redis` and `temporalio/auto-setup` (STATE.md) and for the SDK version (this research).

From `~/.claude/CLAUDE.md` (global Atlas rules, carried into plans): never log PHI; never weaken authN/authZ; no destructive DB change without a rollback (every Alembic revision needs a working `downgrade`); never push to `main`; document Semgrep status for backend changes (Semgrep is installed, so the plan should include a run and a recorded status); every coding task ends with the standard final-response sections.

## Suggested build order (dependency waves, for the planner)

1. **Wave 1 - cross-cutting foundations:** service-token mint + `aud` + `require_exp` + caller allow-lists (G1, G2); `libs/temporal.py`, `libs/internal_client.py`, `libs/auth/service_tokens.py`; idempotent `write_audit` (G5); settings and gateway route-table scaffolding; add `temporalio` after the human-verify checkpoint; test fixtures (Wave 0 gaps).
2. **Wave 2 - data layer and services without workflows:** Scheduling migration `scheduling_0002` (constraints, appointments, keys, waitlist), slot generation and search, the accept transaction with the DB-level concurrency proof (SCHED-03/04) *before* any workflow; Patient/Provider/Organization internal lookups (G3); Billing service (BILL-01/02).
3. **Wave 3 - orchestration:** scheduling worker, `BookingWorkflow` with compensation, cancel, reschedule, waitlist-advance, reaper + pending starter Schedules; compensation, crash, replay tests.
4. **Wave 4 - Visit:** Visit service, state machine, `visit-worker`, `VisitCompletedWorkflow`, sweeper.
5. **Wave 5 - integration and documentation:** Compose (db-init, four new containers, healthchecks, Prometheus jobs), gateway routes and tests, e2e run on Compose Temporal, one-command concurrent-booking demo (`scripts/`; document the rate-limit override), README/docs/ERD, dashboard update.

Worker containers reuse the service image with a command override (`python -m services.scheduling.worker`), `depends_on` the API's health so migrations have run, and need their own healthcheck (no HTTP port; for example a heartbeat-file check) because the shared `x-app-healthcheck` hits `:8000/health`.

## Sources

### Primary (HIGH confidence)
- temporalio 1.34.0 wheel, installed in a scratch venv and read directly: `temporalio/common.py` (`WorkflowIDReusePolicy`, `WorkflowIDConflictPolicy`, `RetryPolicy` defaults), `client/_client.py` (`start_workflow` parameters, `Client.connect(lazy=...)`), `client/_schedule.py`, `client/_exceptions.py` (`ScheduleAlreadyRunningError`), `exceptions.py` (`WorkflowAlreadyStartedError`), `workflow/_context.py` (`patched`, `deprecate_patch`), `workflow/_workflow_ops.py` (`ParentClosePolicy`), `contrib/opentelemetry/README.md`, `contrib/pydantic.py`, `testing/`
- Live probes against the Compose stack (Temporal server 1.29.7, Postgres 15): start-policy matrix, Schedules, worker crash recovery, saga compensation order, converter round-trips, partial-unique-index / idempotency / conditional-UPDATE / generation-conflict behavior under concurrency, DST helper
- python-jose 3.5.0 source (`jose/jwt.py`: `_validate_aud`, `decode` options)
- In-repo files read this session (paths and line ranges cited inline): `services/scheduling/*`, `services/provider/api/internal.py`, `services/organization/api/internal.py`, `services/identity/api/internal.py`, `services/identity/security/signing.py`, `libs/*`, `infra/docker-compose.yml`, `infra/postgres-init/create-service-databases.sh`, `tests/conftest.py`, `tests/libs/test_service_isolation.py`, `docs/architecture-dashboard.html`
- PyPI JSON API, pypistats, GitHub API (package age/downloads/repo evidence)

### Secondary (MEDIUM confidence)
- docs.temporal.io Saga Pattern page (fetched): register compensation before the step, idempotency, log-and-continue on compensation failure
- docs.temporal.io Python versioning page (fetched): `patched`/`deprecate_patch`, replay behavior, `Replayer`
- IETF `draft-ietf-httpapi-idempotency-key-header-07` (search result summary): 422 on payload mismatch, 409 on in-flight retry
- community.temporal.io "Version compatibility between Server and SDK": no formal matrix, SDKs backward compatible (search result summary)

### Tertiary (LOW confidence)
- MongoDB upsert-with-unique-index duplicate-key behavior (training knowledge, A3)
- Trace connectivity across the Temporal boundary (A4, not exercised)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - one new package, version and legitimacy evidence checked, resolver dry-run clean, exercised against the live server
- Architecture: HIGH for the data layer and saga mechanics (executed); MEDIUM for service-token design (needs confirmation) and tracing (not exercised)
- Pitfalls: HIGH - most were reproduced (converter corruption, sandbox re-import, `aud` behavior, retention window, unbounded retry default)

**Research date:** 2026-10-12
**Valid until:** 2026-11-11 (30 days; Temporal SDK releases roughly every 2-3 weeks, so re-check the pin if planning slips)
