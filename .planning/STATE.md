---
gsd_state_version: "1.0"
current_phase: "02.1"
current_phase_name: Week 1-2 mentor review remediation
status: executing
stopped_at: Completed 02.1-03-PLAN.md
last_updated: "2026-10-25T04:56:38.278Z"
last_activity: 2026-10-24
last_activity_desc: Phase 02.1 execution started
state_head: 7fe98775d27e2864aa6a82912c7028ee030e2a93
progress:
  total_phases: 5
  completed_phases: 1
  total_plans: 42
  completed_plans: 32
  percent: 20
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-09-09)

**Core value:** Booking, updating, and completing an appointment is reliable and consistent under load — partial failures never corrupt scheduling state, and every operational data model stays accurate, durable, and queryable.
**Current focus:** Phase 02.1 — Week 1-2 mentor review remediation

## Current Position

Phase: 02.1 (Week 1-2 mentor review remediation) — EXECUTING
Plan: 4 of 13
Status: Ready to execute
(`pytest tests -m "not e2e"`, last full run was after 02-10: 755 passed, 3 skipped), the deferred
tests listed in the 02-0x SUMMARY files (UPDATE 2 scope cut from 02-11 on), and the 02-12 Task 5
human visual check of the dashboard/docs.
Last activity: 2026-10-24 — Phase 02.1 execution started
real-stack e2e 7 passed, demo 1 winner of 50, docs + dashboard v3

Progress: [██░░░░░░░░] 20%

## Performance Metrics

**Velocity:**

- Total plans completed: 29 (Phase 2 plans executed, phase verification pending)
- Average duration: - min
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01 | 7 | - | - |
| 1.1 | 10 | - | - |
| 02 | 12 (executed) | - | - |

**Recent Trend:**

- Last 5 plans: -
- Trend: -

*Updated after each plan completion*
**Per-Plan Metrics:**

| Plan | Duration | Tasks | Files |
|------|----------|-------|-------|
| Phase 01 P01 | 40min | 3 tasks | 17 files |
| Phase 01 P02 | 55min | 3 tasks | 12 files |
| Phase 01 P03 | 55min | 3 tasks | 20 files |
| Phase 01 P04 | 25min | 2 tasks | 6 files |
| Phase 01 P05 | 35min | 3 tasks | 9 files |
| Phase 01 P06 | 3min | 2 tasks | 1 files |
| Phase 01 P07 | 40min | 4 tasks | 15 files |
| Phase 01.1 P01 | 19min | 2 tasks | 59 files |
| Phase 01.1 P02 | 9 min | 2 tasks | 13 files |
| Phase 01.1 P03 | 17 min | 2 tasks | 33 files |
| Phase 01.1 P04 | 30 min | 3 tasks | 13 files |
| Phase 01.1 P05 | 14 min | 2 tasks | 12 files |
| Phase 01.1 P06 | 11 min | 2 tasks | 20 files |
| Phase 01.1 P07 | 20 min | 3 tasks | 27 files |
| Phase 01.1 P08 | 75min | 2 tasks | 30 files |
| Phase 01.1 P09 | 27 min | 2 tasks | 22 files |
| Phase 01.1 P10 | 66min | 2 tasks | 7 files |
| Phase 02.1 P01 | 19 min | 3 tasks | 10 files |
| Phase 02.1 P02 | 20min | 3 tasks | 14 files |
| Phase 02.1 P03 | 25 min | 3 tasks | 16 files |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Roadmap: Three phases locked to the mandated 3-week execution plan (Week 1 Foundation, Week 2 Scheduling, Week 3 Events/Observability) — horizontal-layer structure, not vertical slices, per PROJECT_MODE=standard and explicit user approval.
- Roadmap: DASH requirements split across phases (DASH-01/02 in Phase 1, DASH-03/04 in Phase 3) since the dashboard is a cross-cutting, incrementally-updated deliverable, not a standalone phase.
- Roadmap: v2 requirements (SCHEDX-01, SCHEDX-02 — capacity-aware scheduling, intelligent overbooking) intentionally left unmapped; deferred to a later iteration.
- [Phase 01]: Pinned structlog to 26.1.0 (verified live PyPI) — closes RESEARCH.md assumption A5
- [Phase 01]: redis:8.2-trixie and temporalio/auto-setup:1.31.2 do not exist on Docker Hub — substituted redis:8.2-bookworm and auto-setup:1.29.7 (verified live against the registry)
- [Phase 01]: Checkpoint resolved (01-02): separate users/patients tables, availability modeled as weekly templates, provider_slots created empty with a live-and-proven GiST exclusion constraint (btree_gist EXCLUDE USING gist provider_id=, tstzrange&& WHERE status<>'cancelled') as the Phase 2 double-booking backstop
- [Phase 01]: Checkpoint resolved (01-03): access JWT (~15min, sub/role/clinic_id/jti) + refresh JWT (~7days, sub/jti only), Redis jti-denylist (revoked:{jti}, TTL=remaining life) for revocation; refresh re-derives role/clinic from current DB state
- [Phase 01]: Checkpoint resolved (01-03): audit_log document shape locked - actor_id/actor_role/action/entity_type/entity_id/clinic_id/timestamp/trace_id/outcome(success|failure)/changes(field-names-only, never PHI values)
- [Phase 01]: [Phase 01] Fixed a latent cross-event-loop bug in smarthealth/db.py: AsyncMongoClient and the SQLAlchemy async engine are now loop-tracking singletons (get_engine()/get_mongo_client()), safe under both a single long-lived production event loop and pytest-asyncio's per-test-function loops
- [Phase 01]: [Phase 01] passlib 1.7.4 is incompatible with bcrypt 5.0.0 (CLAUDE.md pin) - passlib's internal bcrypt self-test raises ValueError on bcrypt's strict 72-byte limit; libs/auth/passwords.py calls bcrypt directly instead
- [Phase 01]: [Phase 01] (01-04) No uniqueness constraint on department name within a clinic - two same-named departments under the same clinic are both allowed with distinct ids (tested, deterministic outcome, not a schema gap)
- [Phase 01]: [Phase 01] (01-04) Clinic/department Pydantic schemas defined inline in their router files rather than added to services/identity/domain/schemas.py - keeps each router's request/response contract co-located; schemas.py stays scoped to auth-router payloads
- [Phase 01]: [Phase 01] (01-05) Availability is templates-only in Phase 1 (confirmed in code) — no concrete provider_slots rows generated; zero-count grep + integration test lock this in
- [Phase 01]: [Phase 01] (01-05) Async ORM relationships (Provider.specialties) must be eager-loaded via selectinload() in the SELECT itself — a lazy-load touched after commit raises MissingGreenlet under SQLAlchemy's async engine
- [Phase 01]: [Phase 01] (01-05) Zero-specialties provider creation is rejected 422 via Pydantic min_length=1 — deterministic outcome for the empty-list edge case
- [Phase 01]: [Phase 01] (01-06) Reconciled architecture-dashboard.html in place against actual Phase-1 build: diagram now distinguishes wired (Postgres/Mongo/Redis) vs scaffolding-only (Temporal/Kafka/RabbitMQ/observability); added auth-model and slot-model decision cards; Temporal card documents auto-setup non-production tradeoff
- [Phase 01]: [Phase 01] (01-06) Dashboard reconciliation confirmed no rebuild was needed — only #architecture and #decisions sections had drifted from delivered Phase-1 reality; #overview/#patterns/#flows/#qa/#roadmap were already accurate
- [Phase 01]: [Phase 01] (01-07 gap-closure) Deactivate (soft-delete) is idempotent as a no-op success (200, unchanged state) across all five resources, not 409 — matches HTTP idempotency convention for state-setting operations
- [Phase 01]: [Phase 01] (01-07 gap-closure) Provider `specialties` on PATCH REPLACES the full set (not append/merge) when supplied — consistent PATCH field-level semantics, reuses the create-path get-or-create-by-name logic
- [Phase 01]: [Phase 01] (01-07 gap-closure) Availability PATCH validates the RESULTING window (existing values merged with the partial update), not just the supplied fields in isolation — a single-field PATCH can never silently create an inverted start/end or effective_from>effective_to window
- [Phase 01]: [Phase 01] (01-07 gap-closure) No hard deletes anywhere in the codebase — every "delete" is a status flip (is_active=false + deactivated_at=now()); migration 0002 added these columns to all five Phase-1 resource tables with a verified upgrade/downgrade round-trip
- [Phase 01.1]: [Phase 01.1] (01.1-01) All services write to the ONE shared audit_log collection in the shared Mongo database (MONGO_DB_NAME); audit is cross-cutting infra owned by the future Audit service (D-02), not a per-service database
- [Phase 01.1]: [Phase 01.1] (01.1-01) SharedSettings binds aliased fields by alias only (validate_by_name=False): pydantic-settings 2.15.0 populate_by_name=True also reads the bare field-name env var, which leaked a stale DATABASE_URL into IdentitySettings.database_url
- [Phase 01.1]: [Phase 01.1] (01.1-01) JwksClient: miss-backoff applies to unknown-kid and stale-TTL refreshes alike, an empty/garbled JWKS is a failed refresh (cache kept), stale keys are served when Identity is down; get_current_user returns 401 for missing creds and fails closed with 503 if Redis/JWKS unavailable
- [Phase 01.1]: [Phase 01.1] (01.1-01) Database-per-service bootstrap is an idempotent db-init compose job (not initdb.d) so it runs on existing volumes; legacy single database left untouched. Stray empty users/alembic_version objects exist in legacy smarthealth DB from a pre-fix migration run awaiting user cleanup
- [Phase 01.1]: Duplicate-email registration race closed via users.email unique constraint + IntegrityError -> rollback -> 409 + failure audit (no app lock)
- [Phase 01.1]: HS256-with-public-key forgery test is hand-built (hmac) because python-jose refuses to HMAC with a PEM
- [Phase 01.1]: Gateway exposes no OpenAPI/docs of its own: every path outside the route table (including /openapi.json and /docs) is a plain 404
- [Phase 01.1]: Gateway proxy drops Connection-listed and client accept-encoding headers, appends response headers (Set-Cookie survives), and never retries an upstream call (a retried POST could double-apply)
- [Phase 01.1]: Internal lookups report inactive rows as is_active=false and return one identical 404 for missing and out-of-clinic-scope ids; require_internal_caller carries its own ACCESS_ONLY decode so plan 01.1-05 can widen to service tokens
- [Phase 01.1]: 01.1-04: Gateway rate limiter fails OPEN on Redis outage (visibly: warning log + gateway_ratelimit_fail_open_total); limiter keys on socket peer address only, X-Forwarded-For never trusted
- [Phase 01.1]: 01.1-04: tenacity==9.1.4 and opentelemetry-instrumentation-httpx==0.65b0 pinned after human approval ('Approved, both'); test harness mesh hops run in empty contextvars Context so traces connect only via traceparent headers
- [Phase 01.1]: 01.1-05: a 401/403/5xx or unreadable answer from an owning service is a 503, never a 422, so auth misconfiguration is not mistaken for a missing reference — Reference checks must fail closed (D-05); only a clean 404 or is_active=false means the reference is invalid
- [Phase 01.1]: 01.1-05: service tokens (type=service, sub=service:identity, 60s) are accepted only by require_internal_caller; public routes reject them — Anonymous registration has no user token to forward to Organization; the token is never returned to a client
- [Phase 01.1]: 01.1-05: Identity validates users.clinic_id before the duplicate-email check on both account-creation paths — A bad clinic_id must not be usable to learn whether an email is registered; nothing is written on refusal
- [Phase 01.1]: 01.1-06: a second patient profile for an already-linked account is a 409 (unique constraint arbiter, rollback, failure audit) instead of an unhandled unique violation
- [Phase 01.1]: 01.1-06: Patient validates clinic_id (Organization) then user_id (Identity) in one helper before the insert; internal calls are GET-only with ids in the URL, proven by the recording mesh
- [Phase 01.1]: 01.1-07: Missing or inactive department on provider create is 422 (D-05 supersedes the Phase 1 404); duplicate provider account link is 409 with providers.user_id UNIQUE as arbiter
- [Phase 01.1]: 01.1-07: Specialty get-or-create uses INSERT ON CONFLICT DO NOTHING plus SELECT (race-free, no mid-request commit); availability department reference validated at create only so a deactivated department never blocks PATCH/list/deactivate (D-06)
- [Phase 01.1]: 01.1-07: GET /internal/providers/{id} serves ADMIN and service tokens only, returns id/department_id/is_active, applies no clinic scope (Provider holds no clinic data)
- [Phase 01.1]: Plan 01.1-08: provider_slots and its exclusion constraint live in Scheduling's scheduling_db (no FK on provider_id); Phase 2 never moves the table
- [Phase 01.1]: Plan 01.1-08: application containers run as root (Phase 1 parity) because the bind-mounted key secret would be unreadable to a non-root user; harden later with explicit secret uid/mode
- [Phase 01.1]: Plan 01.1-08: Postgres max_connections raised to 300 in Compose (Temporal ~40 idle connections plus per-worker pools exhausted the default 100)
- [Phase 01.1]: Plan 01.1-08: get_internal_http_client is async def (sync dependencies run in a worker thread with no running loop)
- [Phase 01.1]: Plan 01.1-09: isolation invariants scan live code via ast (docstrings may mention HS256); role isolation is proven against live Postgres with the refusal reason pinned
- [Phase 01.1]: Plan 01.1-09: gateway /metrics is served only on an internal :9100 listener decided by the ASGI server scope, 404 on the published :8000; Prometheus scrapes gateway:9100
- [Phase 01.1]: Plan 01.1-09: gateway body-size cap, shared-Redis and service-token aud gap deferred to deferred-items.md; .env.example edit blocked by harness, variable table in the 01.1-09 SUMMARY
- [Phase 02]: 02-12: workers reuse their API image with a command override, no host port, heartbeat-file healthcheck, depend on their API (so Alembic ran); per-service client secrets only as Compose secrets (identity reads /run/secrets); Billing has no client secret and workers no Prometheus job
- [Phase 02]: 02-12: e2e/demo runs need the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000) as well as the default budget
- [Phase 02]: 02-12: booking saga order as built is validate -> reserve hold -> billing pre-check -> notification intent (best-effort) -> confirm (D-09 vs SCHED-02 order note recorded in docs and dashboard)
- [Phase 01.1]: 01.1-10: Documentation lists unverified items (real-socket outages, cold start, Jaeger UI view, key rotation, bootstrap creation branch) beside the claims they qualify; nothing is described as production-hardened
- [Phase 02.1]: 02.1-01: appointment states checked_in/completed/no_show hold their slot via widened uq_appointments_active_slot and ex_appointments_patient_no_overlap predicates (resolves the Phase 2 'completed frees the slot' question)
- [Phase 02.1]: 02.1-01: check-in window opens 60 min before slot_start and closes at slot_end (DB clock); no-show grace 30 min; no_show only via the 02.1-11 sweep; internal check-in/complete routes are Visit-service-token only
- [Phase 02.1]: Refresh tokens are single-use via atomic Redis SET NX EX claim; logout revokes paired refresh jti through access-token rjti claim
- [Phase 02.1]: bcrypt runs on a bounded dedicated thread pool (IDENTITY_BCRYPT_THREADS); prepared RSA signing key cached to keep the loop free
- [Phase 02.1]: Email identity is case-insensitive via normalizer, lower(email) lookups and uq_users_email_lower; identity_0002 refuses over duplicates reporting counts only and does not rewrite rows
- [Phase 02.1]: 02.1-03: list pagination uses one libs.pagination.page_params dependency (limit 1..200 default 50, offset >= 0, 422 outside), a plain-array body, an X-Total-Count header and a stable (created_at, id) order on patient, clinic and provider lists

### Pending Todos

- Phase 2 verification (deferred at the user's request): run the full regression
  `pytest tests -m "not e2e"` (background + poll, ~17+ min) and `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e -m e2e`
  with the stack up (gateway with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000`).
- Write the deferred tests listed under "Deferred tests" in 02-11-SUMMARY.md and 02-12-SUMMARY.md (and any earlier SUMMARY that lists them).
- 02-12 Task 5: human visual check of docs/architecture-dashboard.html (offline, narrow width), README demo run.
- Semgrep has not been run in the executor environment (not installed).

### Blockers/Concerns

- Phase 2: temporalio 1.34.0 SDK runs against the auto-setup 1.29.7 server in Compose (in-process suites, worker-crash e2e and the 02-12 real-stack flows all pass); there is still no official compatibility matrix.
- Phase 3 planning: Outbox relay mechanism (Debezium CDC vs. custom polling publisher) is an open architectural tradeoff per research — decide explicitly and document in the Architecture & Decision dashboard, not a default assumption.
- Phase 1 planning: Decide auto-setup vs. production-shaped Temporal deployment (temporalio/auto-setup vs. temporalio/server + schema-setup) even though Temporal isn't wired until Phase 2 — affects the Phase 1 Docker Compose skeleton.
- Deferred from 01.1-04: gateway request-body size cap (proxy.forward buffers whole body) and unauthenticated /metrics on public ingress (plan 01.1-09)

### Roadmap Evolution

- Phase 01.1 inserted after Phase 1: Microservice Re-architecture — week-1 mentor review required true microservices; Phase 1 monolith to be split (URGENT)
- Phase 02.1 inserted after Phase 2: Week 1-2 mentor review remediation (see phase FEEDBACK.md) (URGENT)

## Deferred Items

Items acknowledged and deferred at milestone close, most recent first:

| Category | Item | Status | Deferred At | Milestone |
|----------|------|--------|-------------|-----------|
| Scheduling enhancement | SCHEDX-01: Capacity-aware scheduling (reserved urgent/same-day slots) | Deferred (v2) | Roadmap creation | Part A |
| Scheduling enhancement | SCHEDX-02: Intelligent overbooking via no-show heuristic | Deferred (v2) | Roadmap creation | Part A |

## Session Continuity

Last session: 2026-10-25T04:56:38.217Z
Stopped at: Completed 02.1-03-PLAN.md
Resume file: None
