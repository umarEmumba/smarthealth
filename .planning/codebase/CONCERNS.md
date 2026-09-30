---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# Codebase Concerns

**Analysis Date:** 2026-09-30

## Tech Debt

**Temporal SDK / Kafka / Celery Unimplemented (Phase 2-3):**
- Issue: Core distributed components (Temporal workflows, Kafka event backbone, Celery task workers) are scaffolded in `infra/docker-compose.yml` but not yet wired into application code.
- Files: `infra/docker-compose.yml` (services declared but unused), no `services/scheduling/`, no `services/events/`, no worker entry points
- Impact: Phase 1 ships as a single FastAPI service with no saga orchestration, event streaming, or background jobs — the entire concurrency-safety story (Pitfall 1: double-booking prevention, Pitfall 3: idempotent retries, Pitfall 4: dual-write consistency) depends on Phase 2-3 implementation.
- Fix approach: Execute Phase 2 (booking sagas with Temporal) and Phase 3 (event backbone with Kafka/Celery) per ROADMAP.md; critical review gate: every concern in `.planning/research/PITFALLS.md` must be addressed during Phase 2-3 design, not retroactively.

**Temporal Server Version Compatibility Matrix Undefined:**
- Issue: `temporalio/auto-setup:1.29.7` is pinned in `infra/docker-compose.yml`, but the Python SDK (`temporalio 1.32.0` in `pyproject.toml`) has no official strict version-locking matrix per `.planning/research/PITFALLS.md` and STATE.md concern log.
- Files: `infra/docker-compose.yml` line with `temporalio/auto-setup`, `pyproject.toml` dependency section
- Impact: During Phase 2 when the SDK actually connects, a version mismatch could cause startup failures, replay non-determinism errors, or wire-protocol incompatibilities mid-execution.
- Fix approach: Before Phase 2 execution, verify `docs.temporal.io/develop/python/versioning`; re-check if the current pins are compatible or require adjustment; document the compatibility check as part of Phase 2's acceptance criteria.

**Temporal Auto-Setup Image Used for Local Dev (Production-Readiness Risk):**
- Issue: `temporalio/auto-setup:1.29.7` is explicitly documented as non-production-ready by Temporal, yet is the bootstrapped image in `infra/docker-compose.yml`.
- Files: `infra/docker-compose.yml`, `docs/architecture-dashboard.html` (correctly flags this as a tradeoff)
- Impact: The codebase ships with a "development-only" image. Migrating to `temporalio/server` + schema pre-provisioning for a production-shaped deployment requires additional migration logic not yet designed.
- Fix approach: Acceptable for local dev/Phase 2 execution; before any production deployment claim, design and document the migration path to `temporalio/server` with schema tooling, and include that path in the final architecture documentation.

**Concrete Slot Generation Not Implemented (Deferred to Phase 2):**
- Issue: `services/identity/api/availability.py` creates only `AvailabilityTemplate` records (recurring weekly rules); Phase 2 is supposed to generate concrete `provider_slots` rows, but that work is unstarted.
- Files: `services/identity/api/availability.py` (line 1-28, explicit design note), `services/identity/repository/models.py` (ProviderSlot table exists but always empty post-init)
- Impact: Until Phase 2, patients cannot actually book appointments because no concrete slots exist — the booking saga has nowhere to reserve. This is a deliberate architectural boundary (templates in Phase 1, slot generation in Phase 2), but any Phase 2 delay extends this gap.
- Fix approach: Phase 2 Plan 02 must implement slot generation; see ROADMAP.md Phase 2 success criteria.

**Slot Reservation Assumes Postgres GiST Exclusion Constraint (Unverified at Scale):**
- Issue: The double-booking prevention strategy depends entirely on `ex_provider_slots_no_overlap` constraint (EXCLUDE USING gist) verified to exist in Phase 1 tests but never stress-tested under the "tens of thousands of concurrent requests" load target.
- Files: `infra/migrations/versions/0001_initial_schema.py` (constraint defined), `services/identity/repository/models.py` (ProviderSlot model), `.planning/research/PITFALLS.md` (Pitfall 1 documents the risk)
- Impact: A poorly-tuned GiST index or an unlucky query plan under real peak load could cause lock timeouts or unexpected constraint violations; the app will reject valid bookings or timeout rather than cleanly preventing double-booking.
- Fix approach: Phase 2 must include a concurrent load test (50+ parallel booking attempts on one slot) as part of Definition of Done; measure query plans and lock times under load; add explicit `SET LOCK TIMEOUT` or tune `max_locks_per_transaction` if needed.

**No Redis Eviction Policy Explicitly Documented for Lock/Cache Separation:**
- Issue: Redis is configured for both hot-read cache (patient availability, clinic details) and coordination locks (Redlock patterns during slot reservation, per PITFALLS.md Pitfall 7). The `infra/docker-compose.yml` Redis container uses default eviction (`maxmemory-policy=noeviction`), but there is no documented strategy for keeping locks alive vs. allowing cache eviction.
- Files: `infra/docker-compose.yml` (Redis container definition), no explicit `maxmemory` or `maxmemory-policy` documented
- Impact: Under memory pressure during peak booking, cache eviction could accidentally evict an active lock key, breaking mutual exclusion and allowing double-booking (same scenario as Pitfall 7).
- Fix approach: Phase 2 Redis setup: explicitly document maxmemory allocation and eviction policy; separate logical databases or key namespaces for cache (evictable) vs. locks (never evict); add a pre-execution check that confirms the configured policy matches the design assumption.

## Known Bugs

None identified in Phase 1 implementation. Phase 1 verification (`.planning/phases/01-foundation-core-services/01-VERIFICATION.md`) confirmed no anti-patterns, hardcoded stubs, or TODO/FIXME markers in delivered code.

## Security Considerations

**PHI Logging Risk (Mitigated but Requires Ongoing Vigilance):**
- Risk: OTel auto-instrumentation for FastAPI, if misconfigured, could capture request/response body payloads containing patient PII/PHI in trace spans or logs.
- Files: `libs/observability/otel.py` (line 47-48 explicitly disables body capture), `services/identity/app.py` (line 48 notes PHI safety)
- Current mitigation: `otel.py` sets `request_hook`/`response_hook` to strip all body payloads from spans; audit logging uses only identifiers (patient_id, actor_id as UUIDs), never names/DOB/appointment reasons.
- Recommendations: Phase 3 (when Kafka/Celery workers are added): re-verify OTel propagation doesn't inject PHI into Kafka message headers or Celery task headers; audit any new logging introduced in event consumers.

**JWT Algorithm Pinning Verified (Secure):**
- Risk: python-jose's default `jwt.decode()` behavior trusts the token header's own `alg` field, enabling algorithm-confusion attacks (STRIDE T-03-01 / alg:none forgery).
- Files: `libs/auth/jwt.py` (line 92-96 explicitly pins `algorithms=[settings.jwt_algorithm]`)
- Current mitigation: Every `decode_token()` call pins the algorithm allowlist; `test_decode_token_rejects_alg_none_forgery` verifies the fix.
- Recommendations: No further action; this is locked in correctly.

**Hardcoded JWT Secret in Development (By Design, Documented):**
- Risk: `smarthealth/config.py` line 17 defines `_INSECURE_DEFAULT_JWT_SECRET = "changeme-local-dev-only"` visible in source.
- Files: `smarthealth/config.py` (line 17 default, line 64-69 fail-closed check)
- Current mitigation: `Settings.model_post_init()` raises `RuntimeError` if `ENVIRONMENT != "development"` and the secret is still the default; Phase 1 verification confirms this check fires correctly outside dev.
- Recommendations: Production deployments must set `ENVIRONMENT=production` and `JWT_SECRET` via a secrets manager (not checked in); CI/CD linting should flag any attempt to remove or weaken the fail-closed check.

**Audit Trail Actor Identification (Minor Gap):**
- Risk: System-initiated actions (Temporal compensations in Phase 2, Celery tasks in Phase 3) write audit entries with `actor_id="system"` / `actor_role=null`, which is correct but could be improved by using opaque system-actor UUIDs and a registry.
- Files: `libs/audit/writer.py` (write_audit function), no centralized actor-ID registry exists
- Current mitigation: Audit entries distinguish user-initiated vs. system-initiated actions; Phase 1 verification confirmed entries are readable and traceable.
- Recommendations: Phase 2-3: consider adding a well-known system actor UUID and documenting the taxonomy of system-initiated action types (saga-compensation, task-retry, scheduled-reminder, etc.) in a central location for audit compliance.

## Performance Bottlenecks

**Provider Specialties Loaded Eagerly (Correct by Necessity):**
- Problem: `services/identity/repository/people_repo.py` uses `selectinload(Provider.specialties)` to eager-load the N-to-M relationship because SQLAlchemy 2.0 async sessions do not support lazy-loading after commit (raises `MissingGreenlet`).
- Files: `people_repo.py` (multiple `selectinload` calls), documented in STATE.md decision log
- Cause: Async-only SQLAlchemy design requires relationship loading to happen inside the session's context; lazy-loading across session boundaries is unsafe.
- Improvement path: This is already the correct solution; no change needed. Phase 2-3 must follow the same pattern for any new relationships.

**No Read Cache for Patient Availability or Clinic Details:**
- Problem: Every `GET /api/clinics/{id}` or `GET /api/patients/{id}` hits Postgres directly; no Redis cache layer exists.
- Files: `services/identity/api/clinics.py`, `services/identity/api/patients.py`, `services/identity/repository/org_repo.py`, `people_repo.py`
- Cause: Phase 1 scope (immediate consistency read-after-write is a success criterion per PPL-04); caching was deferred as a performance optimization, not a correctness requirement.
- Improvement path: Phase 2-3: measure actual read latency under load; if clinic/patient reads become a bottleneck during concurrent bookings, add a Redis cache with TTL and explicit cache invalidation on mutation (clinic update → invalidate clinic:{id} cache).

**Booking Saga Lock Hold Time (Risk, Not Yet Implementable):**
- Problem: Pitfall 1 and Performance Trap #1 (from PITFALLS.md) warn against long-held `SELECT FOR UPDATE` locks if the booking saga includes synchronous external calls (billing pre-check). The current design intends for billing to be a Temporal activity (async, outside the lock), but this is not yet implemented.
- Files: None yet (Phase 2 will implement), see `.planning/research/PITFALLS.md` lines 216-220
- Cause: Phase 1 did not implement booking; Phase 2 must keep the Postgres lock window short.
- Improvement path: Phase 2 Plan 02 (booking saga): measure lock hold time under load; ensure billing pre-check is a separate Temporal activity outside the `SELECT FOR UPDATE` transaction; add metrics for lock wait time and abort rate.

## Fragile Areas

**Availability Template Timezone Handling (Correct but Requires Phase 2 Attention):**
- Files: `services/identity/api/availability.py` (lines 54-80 model definition), documented in router docstring (line 12-18)
- Why fragile: `start_time`/`end_time` are naive TIME values that MUST resolve against `provider.department.clinic.timezone` at runtime. The Phase 1 template router does no resolution (by design), but Phase 2's slot-generation job must fetch the clinic timezone and apply it. If Phase 2 forgets to include the timezone join or hardcodes UTC, all generated slots will be off by the clinic's timezone offset.
- Safe modification: Phase 2 slot-generation job must: (1) load availability template, (2) join to provider → department → clinic to fetch timezone, (3) use Python's `zoneinfo` to resolve naive times, (4) generate concrete slot rows. Add a test that creates a template in one timezone and verifies slot generation in a different-timezone clinic produces different UTC instants.

**Clinic Scoping in Patient/Provider Reads (Correct, but No Explicit Validation):**
- Files: `services/identity/repository/people_repo.py` (list_patients function, line ~90-105), `services/identity/api/patients.py` (WR-02 fix documented in STATE.md)
- Why fragile: A front-desk user calling `GET /api/patients` should only see patients in their own clinic; the filter is applied via the query (`where(Patient.clinic_id == user_clinic_id)`), but there is no centralized "clinic scope guard" abstraction — each router must manually apply the filter. If Phase 2-3 adds new endpoints and forgets to apply this filter, users could query across clinics.
- Safe modification: Phase 2-3: before adding any new people/resource endpoint, add an explicit check to the Pydantic schema or repository layer that clinic_id is constrained; consider a `@Depends` middleware that injects clinic_id from the current user's token and rejects any request that tries to override it.

**Audit Trail Completeness (Correct so far, but not Comprehensive):**
- Files: `libs/audit/writer.py` (write_audit function), every mutating router calls it
- Why fragile: Phase 1 audits user-initiated mutations (register, create clinic, etc.). Phase 2 will add Temporal saga compensations (release slot, reverse billing); Phase 3 will add Celery task side effects (send notification, schedule reminder). If any of these skip audit entries, the audit trail will have gaps. The current write_audit signature accepts an `actor_id` parameter, but there is no enforcement that system-initiated actions MUST include actor_id="system" or a workflow_id.
- Safe modification: Phase 2-3: add an explicit rule: every Temporal activity that mutates state must call `write_audit`; every Celery task that sends a notification must call `write_audit`. Test this by querying the audit_log after a deliberate saga compensation and confirming the entry exists.

## Scaling Limits

**Single Temporal Server Instance (Non-Distributed):**
- Current capacity: `temporalio/auto-setup:1.29.7` runs a single Temporal server in-process; it is single-threaded for workflow execution and can handle ~100s of in-flight workflows on modern hardware.
- Limit: Where it breaks: once this project reaches >1000 concurrent in-flight booking sagas, a single Temporal server becomes the bottleneck; workflow history gets slow to replay, and the server's activity task queue backs up.
- Scaling path: Temporal server can be horizontally scaled behind a load balancer (multiple `temporalio/server` instances sharing a Postgres history store), but this requires architectural changes not yet designed. For Part A, a single-server deployment is acceptable; the CLAUDE.md and architecture docs should flag this as a future scaling lever.

**PostgreSQL Single Instance (Adequate for Pilot, Not for True Distributed Deployment):**
- Current capacity: One Postgres 15 container can handle ~1000 concurrent connections with reasonable latency; with proper indexing (which Phase 1 has for provider_slots: the GiST index), read and write throughput is not the bottleneck up to ~10K appointments/day.
- Limit: Where it breaks: once writes become contended (e.g., a spike of 1000+ booking requests hitting the same provider_slots rows simultaneously), lock contention on the GiST index can cause timeouts or cascading aborts. Read replicas help with reads, but writes must still serialize through the primary.
- Scaling path: A true distributed healthcare system would use Postgres sharding (by clinic_id or provider_id) or a distributed consensus DB (CockroachDB), but Part A does not require this. Document the limitation and the scaling path for future versions.

**Redis Single Instance (Memory-Constrained for Lock + Cache):**
- Current capacity: One Redis container (default 256MB if no explicit `maxmemory` set) can hold cache entries for ~100 clinics' worth of data (clinic details, patient lists) and concurrently coordinate ~100s of distributed locks (each lock key ~100 bytes).
- Limit: Where it breaks: under sustained peak booking load (100s of concurrent requests trying to acquire locks), Redis memory fills up; if the eviction policy is not carefully tuned, lock keys might get evicted (breaking mutual exclusion) or cache keys get evicted (causing cache misses and DB load spikes).
- Scaling path: Explicit `maxmemory` allocation and separate key namespaces/TTL policies for cache vs. locks; if lock contention becomes CPU-bound (not memory), use Redis Cluster for sharding; if cache misses dominate, add a local in-process cache layer in the app.

## Dependencies at Risk

**Temporal SDK / Python 3.12 Compatibility (Verified but Narrow):**
- Risk: `temporalio 1.32.0` on Python 3.12 is narrowly supported — the SDK's native extensions depend on CPython internals, and a future Python 3.13 release could introduce incompatibilities.
- Impact: If Python 3.13 ships incompatibly, the project would need to pin Python 3.12 or wait for a temporalio release; Temporal's release cadence historically lags Python's.
- Migration plan: Monitor `temporalio` releases and test against each new Python release in CI; if 3.13 incompatibility emerges, stay on 3.12 LTS until fixed.

**Confluent-Kafka Library Maintenance (Production-Grade but Vendor-Scoped):**
- Risk: `confluent-kafka 2.15.0` is maintained by Confluent Inc., a venture-backed company; if they shift priorities or the company's status changes, the library could be deprecated in favor of a Java-only ecosystem.
- Impact: Phase 3 depends critically on this library; switching to `aiokafka` mid-Phase-3 would require rewriting all Kafka integration code.
- Migration plan: No immediate risk; Confluent is actively developing Kafka. Alternatives exist (aiokafka, rdkafka via other bindings), but switching is a significant effort. Document this as a long-term dependency risk to monitor.

**PyMongo Async Client (Young, Rapidly Evolving):**
- Risk: PyMongo 4.18.0's native async API (`AsyncMongoClient`) is only a year old (introduced in PyMongo 4.9); MongoDB's Python ecosystem is still settling on the "correct" async pattern (Motor → PyMongo native → future TBD).
- Impact: A breaking change to PyMongo's async API could require code updates; the audit trail (MongoDB) is not the critical path, so this is low-severity but worth monitoring.
- Migration plan: Stay current with PyMongo patch releases; monitor MongoDB's Python driver docs for deprecation warnings; if async API breaks, the sync fallback exists (slower, but available).

## Missing Critical Features

**Booking Saga Not Implemented (Phase 2 Blocker):**
- Problem: No route to actually book an appointment; `POST /api/appointments` does not exist yet.
- Blocks: Patients cannot test the core workflow; load testing for concurrency bugs cannot begin; Temporal integration cannot be stress-tested.
- Phase: Phase 2 Plan 02.

**Waitlist and Rescheduling Not Implemented (Phase 2 Blocker):**
- Problem: When a patient cancels, there is no automatic waitlist queue to fill the slot.
- Blocks: Multi-step booking workflows (cancel → relist → waitlist advance) cannot be tested.
- Phase: Phase 2 Plan 03.

**Event-Driven Backbone Not Implemented (Phase 3 Blocker):**
- Problem: Kafka events are not published from any mutation; the transactional outbox pattern is not wired; event consumers do not exist.
- Blocks: Analytics, notifications, and observability all depend on event streaming; Phase 3 cannot start until Phase 2 is complete.
- Phase: Phase 3 Plan 01.

**End-to-End Trace Propagation Not Wired Across Async Boundaries (Phase 3 Blocker):**
- Problem: OTel trace context (trace ID / parent span ID) does not propagate from FastAPI → Temporal → Kafka → Celery tasks; each async boundary starts a new trace.
- Blocks: Debugging a booking failure requires manually grepping logs across services; Jaeger shows disconnected trace fragments instead of one end-to-end trace.
- Phase: Phase 3 Plan 05 (observability).

## Test Coverage Gaps

**Concurrent Load Testing for Double-Booking Prevention (Critical Gap):**
- What's not tested: Phase 1's test suite includes unit/integration tests for single requests, but no concurrent load test verifying that 50 parallel booking attempts against one slot produce exactly one confirmed appointment (not two, not zero).
- Files: `tests/test_availability.py` (exists, 542 lines, but tests availability TEMPLATES, not concrete slot booking; concrete slot booking tests don't exist yet)
- Risk: The GiST exclusion constraint is correct in theory, but if the query planner or lock contention behaves unexpectedly under load, this could surface as a silent double-booking bug in production.
- Priority: CRITICAL — must be added to Phase 2 Definition of Done before any booking saga is marked complete. Test: `pytest -m load -k "test_concurrent_booking_same_slot"` should run 50 concurrent requests and assert `SELECT COUNT(*) FROM appointments WHERE status='confirmed' AND slot_id=X` equals 1.

**Idempotency Verification for Temporal Activities (Gap Until Phase 2):**
- What's not tested: Replaying the same booking activity twice (same input, simulating a Temporal retry) and verifying no duplicate appointment is created.
- Files: None yet (Phase 2 must add this)
- Risk: Temporal's retry logic only guarantees "the activity will eventually succeed," not "it runs exactly once." If the activity is not idempotent (e.g., it blindly inserts an appointment row on every call), a retry creates a duplicate.
- Priority: HIGH — must be part of Phase 2 Definition of Done. Pattern: use an idempotency key (`appointment_booking_request_id`) unique per booking request; the activity checks if it already ran (idempotency key exists in a dedup table) before inserting; only mark done after the insert succeeds.

**Kafka Consumer Ordering Under Rebalance (Gap Until Phase 3):**
- What's not tested: Multi-partition, multi-consumer Kafka topic; trigger a consumer group rebalance during message processing; verify no out-of-order state transitions (e.g., "appointment.cancelled" processed before "appointment.booked").
- Files: None yet (Phase 3 must add this)
- Risk: Kafka guarantees per-partition order only; if messages are not keyed by appointment_id, a rebalance could cause a consumer to process events in the wrong order.
- Priority: MEDIUM — Phase 3 must verify consumer group design before going live. Test: keys all appointment events by appointment_id; run 100 events through a multi-partition topic; trigger rebalance mid-stream; assert all appointment state machines end in a valid final state.

**Distributed Lock Safety (Expires During Processing):**
- What's not tested: Acquire a Redis Redlock, simulate a 5-second GC pause, then try to proceed — verify the lock does NOT prevent a second process from acquiring it (demonstrating the race condition from Pitfall 7).
- Files: None yet (Phase 2 must add this if Redlock is used)
- Risk: This test documents the known limitation of Redis locking and ensures code doesn't over-rely on it.
- Priority: MEDIUM — add as a documented "limitations test" in Phase 2, with a comment explaining why Postgres constraints are the real source of truth.

## Deployment and DevOps Concerns

**No Pre-Commit Hooks or CI Linting for Common Pitfalls:**
- Issue: No automatic checks to prevent common Phase 2-3 mistakes (e.g., `datetime.now()` inside a Temporal workflow, blocking I/O in an activity, direct Kafka producer calls outside a transaction).
- Files: `.pre-commit-config.yaml` does not exist (or is empty); no CI pipeline file (`.github/workflows/`, `.gitlab-ci.yml`)
- Impact: Code review must catch these manually; under deadline pressure, pitfalls could slip through.
- Fix approach: Add a `.pre-commit-config.yaml` with ruff (Python linting) + custom rules for anti-patterns; add a CI pipeline that runs pytest + semgrep (if available) on every PR; document "golden rules" for Phase 2-3 (e.g., "all activity code must be idempotent").

**Docker Image Not Documented (Dockerfile Exists, No README for Deployment):**
- Issue: `Dockerfile` exists (scaffolded in Phase 1) but is not used in `infra/docker-compose.yml` — the API service runs Python directly via `python -m uvicorn`. No documentation on how to build/push/deploy the image.
- Files: `Dockerfile` (root), `infra/docker-compose.yml` (no `build:` directive for the api service)
- Impact: Deploying to a real cluster (Kubernetes, cloud) requires clear image-building instructions; if the Dockerfile is not tested in CI, it could be out of date when needed.
- Fix approach: (1) Add Dockerfile build to CI pipeline, (2) Test image builds and runs in CI, (3) Document in README how to build/tag/push the image, (4) Decide: use the image in local docker-compose as well, or keep `image:` mode for dev vs. `build:` mode for production.

---

*Concerns audit: 2026-09-30*
