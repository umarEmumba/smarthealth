---
gsd_state_version: "1.0"
current_phase: "1.1"
current_phase_name: Microservice Re-architecture
status: executing
stopped_at: Completed 01.1-04-PLAN.md
last_updated: "2026-10-12T07:50:57.719Z"
last_activity: 2026-10-12
last_activity_desc: Phase 1.1 execution started
state_head: 07357cd28f7d1bbcd632b18036b228e0412ed0a9
progress:
  total_phases: 4
  completed_phases: 1
  total_plans: 17
  completed_plans: 11
  percent: 25
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-09-09)

**Core value:** Booking, updating, and completing an appointment is reliable and consistent under load — partial failures never corrupt scheduling state, and every operational data model stays accurate, durable, and queryable.
**Current focus:** Phase 1.1 — Microservice Re-architecture

## Current Position

Phase: 1.1 (Microservice Re-architecture) — EXECUTING
Plan: 5 of 10
Status: Ready to execute
Last activity: 2026-10-12 — Phase 1.1 execution started

Progress: [███░░░░░░░] 25%

## Performance Metrics

**Velocity:**

- Total plans completed: 7
- Average duration: - min
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01 | 7 | - | - |

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

### Pending Todos

None yet.

### Blockers/Concerns

- Phase 2 planning: Temporal SDK↔server version compatibility has no official strict matrix per research (ARCHITECTURE.md/SUMMARY.md) — re-verify against docs.temporal.io/develop/python/versioning before locking a submission version.
- Phase 3 planning: Outbox relay mechanism (Debezium CDC vs. custom polling publisher) is an open architectural tradeoff per research — decide explicitly and document in the Architecture & Decision dashboard, not a default assumption.
- Phase 1 planning: Decide auto-setup vs. production-shaped Temporal deployment (temporalio/auto-setup vs. temporalio/server + schema-setup) even though Temporal isn't wired until Phase 2 — affects the Phase 1 Docker Compose skeleton.
- Deferred from 01.1-04: gateway request-body size cap (proxy.forward buffers whole body) and unauthenticated /metrics on public ingress (plan 01.1-09)

### Roadmap Evolution

- Phase 01.1 inserted after Phase 1: Microservice Re-architecture — week-1 mentor review required true microservices; Phase 1 monolith to be split (URGENT)

## Deferred Items

Items acknowledged and deferred at milestone close, most recent first:

| Category | Item | Status | Deferred At | Milestone |
|----------|------|--------|-------------|-----------|
| Scheduling enhancement | SCHEDX-01: Capacity-aware scheduling (reserved urgent/same-day slots) | Deferred (v2) | Roadmap creation | Part A |
| Scheduling enhancement | SCHEDX-02: Intelligent overbooking via no-show heuristic | Deferred (v2) | Roadmap creation | Part A |

## Session Continuity

Last session: 2026-10-12T07:50:57.686Z
Stopped at: Completed 01.1-04-PLAN.md
Resume file: None
