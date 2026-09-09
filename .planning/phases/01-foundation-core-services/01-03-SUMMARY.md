---
phase: 01-foundation-core-services
plan: 03
subsystem: auth
tags: [fastapi, jwt, python-jose, bcrypt, redis, mongodb, rbac, otel, structlog, pytest]

# Dependency graph
requires:
  - phase: 01-01
    provides: "project scaffolding, pinned deps, Docker Compose stack, pytest fixtures (db_session/mongo_client/redis_client/client)"
  - phase: 01-02
    provides: "smarthealth.db Base/get_db, Role enum, User/Patient/Clinic/Department/Provider models, ensure_audit_indexes"
provides:
  - "libs/auth/{jwt,passwords,denylist}.py — access+refresh JWT issuance/validation (algorithms allowlist enforced), bcrypt hashing, Redis jti-denylist"
  - "libs/audit/writer.py — the single shared write_audit helper (locked audit_log document shape)"
  - "libs/observability/{otel,logging}.py — OTLP tracer export to Jaeger (body capture disabled), structlog JSON processor with trace_id/span_id injection"
  - "services/identity/security/{current_user,rbac}.py — get_current_user (decode+denylist), require_role(*roles) 403 guard"
  - "services/identity/api/{auth,admin_ping}.py — register/login/refresh/logout + admin-only RBAC probe route"
  - "services/identity/app.py + smarthealth/main.py — the FastAPI app factory and ASGI entrypoint every later plan's routers mount onto"
  - "smarthealth/db.py — event-loop-safe get_engine()/get_mongo_client() (fixes a cross-test-loop bug latent since Plan 01/02, only surfaced once real async DB+Mongo calls ran inside the same request path)"
affects: [01-04-clinic-department-crud, 01-05-patient-provider-availability-crud, 01-06-dashboard-reconciliation]

# Actuals (#2632)
actuals:
  tokens: 15440
  tasks: 3
  commits: 3
  plan_head_before: 8e7303e9aac87b52d55eab4fc3f2502a267b6e66

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Loop-bound singleton pattern for async DB/Mongo clients: smarthealth/db.py's get_engine()/get_mongo_client() track which asyncio event loop the cached client belongs to and transparently rebuild it if the running loop changed — required because pytest-asyncio gives each test function a fresh loop, while a real uvicorn/gunicorn worker has exactly one long-lived loop; both cases are now handled by the same function"
    - "Single shared write_audit(mongo, ...) helper called by every mutating route — no per-router Mongo insert copy"
    - "Server-side require_role(*roles) FastAPI dependency layered on get_current_user — RBAC never enforced client-side"
    - "PHI-safe logging/tracing convention: log/span calls carry identifiers only (user_id, patient_id, clinic_id, jti) — enforced by discipline + tests (test_no_phi_in_logs), not automated redaction"

key-files:
  created:
    - libs/auth/jwt.py
    - libs/auth/passwords.py
    - libs/auth/denylist.py
    - libs/audit/writer.py
    - libs/observability/otel.py
    - libs/observability/logging.py
    - services/identity/domain/schemas.py
    - services/identity/security/current_user.py
    - services/identity/security/rbac.py
    - services/identity/api/auth.py
    - services/identity/api/admin_ping.py
    - services/identity/app.py
    - smarthealth/main.py
    - tests/test_auth.py
    - tests/test_rbac.py
    - tests/test_audit.py
  modified:
    - smarthealth/db.py
    - tests/conftest.py

key-decisions:
  - "Checkpoint resolved (user-selected 'recommended' option): access JWT (~15min) carries sub/role/clinic_id/jti/type=access/exp/iat; refresh JWT (~7days) carries ONLY sub/jti/type=refresh/exp/iat. Redis jti-denylist keyed revoked:{jti} with TTL=remaining token life for revocation. A refresh exchange re-derives role/clinic from the CURRENT DB row, so a role change takes effect on next refresh, not just next login."
  - "audit_log document shape (locked contract): {actor_id, actor_role, action (dot-namespaced entity.verb), entity_type, entity_id, clinic_id, timestamp (server-set UTC), trace_id (from active OTel span), outcome (success|failure), changes: {fields: [name, ...]} | null}. changes stores field NAMES only, never PHI values."
  - "Pinned JWT algorithm: HS256 (settings.jwt_algorithm) — decode_token always passes an explicit algorithms=[...] allowlist to jose.jwt.decode, never trusting the token header's own alg claim (closes the alg:none/algorithm-confusion footgun, T-03-01)."
  - "Password hashing calls the bcrypt library directly rather than routing through passlib's CryptContext — see Deviations (passlib 1.7.4 + bcrypt 5.0.0 incompatibility). Still bcrypt, still CLAUDE.md-compliant; passwords are truncated to bcrypt's own 72-byte hard limit before hashing (standard practice, not a new limitation)."

patterns-established:
  - "Pattern: get_engine()/get_mongo_client() in smarthealth/db.py resolve to the client bound to the CURRENT running event loop, rebuilding if the loop changed — the correct way to share an async DB/Mongo client between a long-lived production process and a per-test-function pytest-asyncio loop without either a cross-loop RuntimeError or a real per-request client (defeating connection pooling)"
  - "Pattern: every mutating route in the auth router writes an audit entry on BOTH success and failure paths (duplicate-email 409, invalid-credentials 401) — audit coverage decided per-mutation, not opt-in per endpoint (RESEARCH Pitfall 2)"

requirements-completed: [IAM-01, IAM-02, IAM-03, IAM-04]

coverage:
  - id: D1
    description: "Register (POST /api/auth/register) then login (POST /api/auth/login) returns 200 with access_token + refresh_token; access token carries sub/role/clinic_id/jti/type=access, refresh token carries only sub/jti/type=refresh"
    requirement: "IAM-01, IAM-03"
    verification:
      - kind: integration
        ref: "tests/test_auth.py::test_register_then_login_returns_token_pair, ::test_access_token_claim_shape, ::test_refresh_token_claim_shape_is_minimal"
        status: pass
    human_judgment: false
  - id: D2
    description: "Every protected route validates the access token per request: signature+expiry via an explicit algorithms allowlist, then a Redis jti-denylist EXISTS check; logout revokes the token's jti and a subsequent request with that token returns 401"
    requirement: "IAM-03"
    verification:
      - kind: integration
        ref: "tests/test_auth.py::test_logout_revokes_access_token, ::test_missing_token_returns_401"
        status: pass
    human_judgment: false
  - id: D3
    description: "A front-desk-role and a patient-role token calling the admin-only route both return 403; an admin token returns 200; a missing token returns 401 (not 403)"
    requirement: "IAM-02"
    verification:
      - kind: integration
        ref: "tests/test_rbac.py (4/4 passed: test_forbidden_action_403, test_allowed_action_200, test_missing_token_401, test_patient_token_forbidden)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Every mutating action writes an audit_log document to MongoDB with actor_id/actor_role/action/entity_type/entity_id/clinic_id/timestamp/trace_id/outcome — including denied/failed mutations (outcome=failure), not only successes"
    requirement: "IAM-04"
    verification:
      - kind: integration
        ref: "tests/test_audit.py::test_mutation_creates_audit_entry, ::test_failed_mutation_audited"
        status: pass
    human_judgment: false
  - id: D5
    description: "structlog emits JSON logs carrying trace_id/span_id and no PHI field values; write_audit's changes field, when present, stores field names only, never PHI plaintext values"
    requirement: "IAM-04"
    verification:
      - kind: integration
        ref: "tests/test_audit.py::test_no_phi_in_logs, ::test_audit_changes_field_names_only"
        status: pass
    human_judgment: false
  - id: D6
    description: "decode_token rejects a token forged with alg:none (or any algorithm outside the pinned allowlist) — the V6 crypto / algorithm-confusion footgun this plan explicitly tests against"
    verification:
      - kind: integration
        ref: "tests/test_auth.py::test_decode_token_rejects_alg_none_forgery"
        status: pass
    human_judgment: false
  - id: D7
    description: "Registering the same email twice does not create two auth identities — the second attempt returns 409, and the original account can still log in exactly once afterward"
    verification:
      - kind: integration
        ref: "tests/test_auth.py::test_duplicate_email_registration_returns_409_and_no_second_user"
        status: pass
    human_judgment: false
  - id: D8
    description: "A refresh exchange re-derives role/clinic from the current DB state (not the old token's claims); an empty, garbage, or wrong-type (access-used-as-refresh) refresh token is rejected 401"
    verification:
      - kind: integration
        ref: "tests/test_auth.py::test_refresh_reissues_access_token, ::test_refresh_rejects_empty_and_garbage_token, ::test_refresh_rejects_access_token_used_as_refresh"
        status: pass
    human_judgment: false

duration: 55min
completed: 2026-09-09
status: complete
---

# Phase 1 Plan 3: End-to-End Auth/RBAC/Audit/Observability Tracer Summary

**One production-quality vertical slice proving the entire Phase-1 architecture wired together — register/login/refresh/logout issuing access+refresh JWTs (sub/role/clinic_id/jti claim shape, HS256, explicit algorithms allowlist), server-side RBAC 403 enforcement, Redis jti-denylist revocation, MongoDB audit trail on both success and failure paths, and PHI-safe structlog/OTel tracing — all 20 integration tests green against live Postgres/Mongo/Redis containers.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-09-09T16:28:00Z
- **Completed:** 2026-09-09T17:23:00Z
- **Tasks:** 3
- **Files modified:** 20 (18 created, 2 modified)

## Checkpoint Resolution

The plan opened with a `checkpoint:decision` (blocking-human) confirming the auth/session model and audit_log document shape before they became load-bearing contracts for Plans 04/05 and Phases 2/3. The user selected the **recommended** option (RESEARCH Decision 1):

- Access JWT (~15 min) carrying `sub`, `role`, `clinic_id`, `jti`, `type=access`, `exp`, `iat`.
- Refresh JWT (~7 days) carrying ONLY `sub`, `jti`, `type=refresh`, `exp`, `iat` — no role/clinic claims.
- Revocation via a Redis jti-denylist (`revoked:{jti}`, TTL = remaining token life, self-expiring).
- A refresh exchange re-derives role/clinic from the current DB row, so a role change takes effect on next refresh.
- python-jose for JWT, bcrypt for password hashing (direct library call — see Deviations), structlog + OTel for observability, never logging PHI.
- The `audit_log` MongoDB document shape: `actor_id`, `actor_role`, `action` (dot-namespaced), `entity_type`, `entity_id`, `clinic_id`, `timestamp`, `trace_id`, `outcome` (success|failure), `changes` (field names only, never PHI values).

This decision is recorded in `libs/auth/jwt.py`'s module docstring, `libs/audit/writer.py`'s module docstring, and here — not just STATE.md — since it is the load-bearing contract every later route in this phase and both later phases emit against.

## Accomplishments
- Full auth slice works end-to-end against live Postgres/Redis/MongoDB containers: register → login → RBAC-scoped request → audit written → PHI-safe log/trace, proven by 20 integration tests (0 skipped, 0 xfail)
- `decode_token` pins an explicit `algorithms=[jwt_algorithm]` allowlist on every call and rejects a manually-forged alg:none token (the classic JWT algorithm-confusion attack), proven by a test that builds the forged token byte-for-byte rather than relying on python-jose's encoder (which itself refuses to sign with `alg=none`)
- Logout revocation is live: `POST /api/auth/logout` writes the access token's `jti` to the Redis denylist with TTL = remaining token life; the SAME token immediately returns 401 on the next protected request
- RBAC is enforced server-side and proven at the HTTP boundary for two different non-admin roles (front-desk, patient) — both 403, admin 200, no-token 401
- Audit coverage extends to denied/failed mutations (duplicate-email 409), not only successes — a dedicated test proves an `outcome=failure` audit_log document exists for the rejected second registration attempt
- PHI-safe logging convention is enforced by a test, not just a docstring: a captured structlog JSON log line for an identifier-only call never contains a synthetic PHI value string
- OTel FastAPI auto-instrumentation is wired with body capture NOT enabled (verified by code inspection — `FastAPIInstrumentor.instrument_app(app)` is called with no body-capture hooks configured anywhere in this codebase)
- Found and fixed a latent cross-event-loop bug in `smarthealth/db.py` (Plan 01/02's SQLAlchemy engine + Mongo client were module-level singletons created once at import time) that had gone undetected because Plans 01/02's own tests each build their own throwaway engine/client per test rather than going through the shared app-level singleton — this plan's tests are the first to exercise the real FastAPI app's shared DB/Mongo dependencies across pytest-asyncio's per-test event loops, surfacing it

## Task Commits

Each task was committed atomically:

1. **Task 1 (tracer): End-to-end register→login→RBAC→audit→PHI-safe log/trace** - `cfbc236` (feat)
2. **Task 2: RBAC enforcement tests (403 end-to-end, IAM-02)** - `20663e7` (test)
3. **Task 3: Audit-trail + PHI-safe-logging tests (IAM-04)** - `2011a4e` (test)

**Plan metadata:** (this commit) — docs: complete plan

## Files Created/Modified
- `libs/observability/otel.py` - `setup_tracing(app)`, `configure_tracer_provider`, `current_trace_id()` — OTLP export to Jaeger, body capture disabled
- `libs/observability/logging.py` - `configure_logging()`, `add_trace_context` structlog processor, `get_logger()`
- `libs/auth/jwt.py` - `create_access_token`, `create_refresh_token`, `decode_token` (algorithms allowlist enforced)
- `libs/auth/passwords.py` - `hash_password`/`verify_password` via direct bcrypt calls (see Deviations)
- `libs/auth/denylist.py` - `revoke_token`/`is_revoked` against `redis.asyncio`, keyed `revoked:{jti}`
- `libs/audit/writer.py` - `write_audit(...)`, the single shared audit helper
- `services/identity/domain/schemas.py` - `RegisterRequest`, `LoginRequest`, `RefreshRequest`, `TokenPair`, `LoginResponse`, `UserOut`
- `services/identity/security/current_user.py` - `get_current_user`, `AuthenticatedUser`, `get_redis`
- `services/identity/security/rbac.py` - `require_role(*roles)`
- `services/identity/api/auth.py` - register/login/refresh/logout router
- `services/identity/api/admin_ping.py` - the minimal admin-only RBAC probe route
- `services/identity/app.py` - FastAPI app factory (routers + OTel + structlog + prometheus `/metrics`)
- `smarthealth/main.py` - ASGI entrypoint (`smarthealth.main:app`)
- `smarthealth/db.py` (modified) - added `get_mongo`/`get_mongo_client`/`get_engine`/`get_session_factory`, loop-safety fix for the pre-existing engine singleton, `__getattr__` back-compat shim for `engine`/`async_session`
- `tests/conftest.py` (modified) - `client` fixture now targets the real `smarthealth.main:app` (placeholder fallback removed)
- `tests/test_auth.py` - 12 integration tests (full auth slice, claim shapes, logout revocation, refresh, alg:none rejection, duplicate email, missing/invalid credentials)
- `tests/test_rbac.py` - 4 integration tests (403/200/401 at the HTTP boundary)
- `tests/test_audit.py` - 4 tests (audit on success/failure, PHI-safe logs, changes-field-names-only)

## Decisions Made
- Used `bcrypt` directly instead of `passlib.CryptContext(schemes=["bcrypt"])` — see Deviations below for why passlib 1.7.4 is incompatible with bcrypt 5.0.0 as pinned by CLAUDE.md. Still satisfies the CLAUDE.md mandate ("passlib[bcrypt] + bcrypt") in substance — bcrypt is still the hashing algorithm; only the thin passlib wrapper layer, whose own internal self-test is what breaks, was bypassed.
- Added `get_mongo_client()`/`get_engine()` loop-tracking helpers to `smarthealth/db.py` rather than switching to a per-request client — preserves connection-pooling behavior in the real (single-loop) production process while making the exact same code path safe under pytest-asyncio's per-test-function event loops
- Kept `smarthealth.db.engine`/`smarthealth.db.async_session` importable via a module `__getattr__` shim so `infra/migrations/env.py` (Plan 01-02's Alembic config) did not need to change — Alembic runs its own single `asyncio.run(...)` loop, so the loop-tracking logic is transparent to it
- `services/identity/domain/schemas.py`'s `LoginResponse` is kept as a distinct (currently identical) subclass of `TokenPair` rather than a type alias, anticipating a likely future divergence (e.g. adding `user_id` to the login response specifically) without breaking `TokenPair`'s use elsewhere
- Chose `HTTPBearer` (not `OAuth2PasswordBearer`) for the bearer-token dependency in `current_user.py` — this project's login endpoint accepts a JSON body (email/password), not OAuth2's form-encoded `username`/`password`, so `OAuth2PasswordBearer`'s Swagger-UI login-form affordance doesn't apply; `HTTPBearer` is the more accurate representation of "bearer token in the Authorization header," which is all this dependency needs to parse

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] passlib 1.7.4's bcrypt backend self-test is incompatible with bcrypt 5.0.0**
- **Found during:** Task 1, first `pytest tests/test_auth.py` run
- **Issue:** passlib's `CryptContext(schemes=["bcrypt"])` runs an internal `detect_wrap_bug` self-test on first use that probes with a 255-byte secret. bcrypt>=4.1 (pinned to 5.0.0 by CLAUDE.md) enforces its 72-byte hard limit strictly and raises `ValueError` instead of the old silent-truncation behavior pybcrypt/bcrypt<4.1 had. This is a documented upstream passlib/bcrypt version incompatibility with no passlib-1.7.4-side fix — every `hash_password` call failed with `ValueError: password cannot be longer than 72 bytes`.
- **Fix:** Call the `bcrypt` library directly (`bcrypt.hashpw`/`bcrypt.checkpw`) instead of routing through passlib's incompatible self-test. Passwords are truncated to bcrypt's own 72-byte hard limit before hashing (standard, industry-wide practice — bcrypt has never supported longer secrets).
- **Files modified:** libs/auth/passwords.py
- **Verification:** `hash_password`/`verify_password` round-trip correctly; all auth tests that create/verify passwords pass
- **Committed in:** cfbc236 (Task 1 commit)

**2. [Rule 1 - Bug] AsyncMongoClient and the SQLAlchemy async engine were module-level singletons bound to whichever event loop was active at import/first-use time**
- **Found during:** Task 1, second `pytest tests/test_auth.py` run (after fixing #1) — `RuntimeError: Cannot use AsyncMongoClient in different event loop` and asyncpg's `RuntimeError: ... attached to a different loop`
- **Issue:** `smarthealth/db.py` (Plan 01-02) created the SQLAlchemy `engine`/`async_session` and (this plan's addition) the Mongo client as plain module-level globals, built once. PyMongo's native async client and asyncpg's connection pool both bind their internal asyncio primitives to the event loop active when first created. pytest-asyncio gives each `async def test_...` function its OWN fresh event loop by default (no fixed loop scope configured in `pyproject.toml`), so any test after the first one to touch the shared app-level client hit a cross-loop error. This was latent since Plan 01/02 but undetected because those plans' own tests each build a throwaway engine/client per test rather than going through the shared singleton — this plan's tests are the first to exercise the real FastAPI app's shared dependencies across multiple tests in the same run.
- **Fix:** Added `get_engine()`/`get_mongo_client()` functions that track which event loop the cached client belongs to and transparently rebuild it whenever the running loop has changed. Correct and equivalent behavior in the real single-loop production process (build once, reuse forever) and in the multi-loop test suite (rebuild once per test). Preserved backward-compatible module-attribute access to `engine`/`async_session` via `__getattr__` so `infra/migrations/env.py` (Alembic, Plan 01-02) needed no changes.
- **Files modified:** smarthealth/db.py
- **Verification:** Full `pytest tests/` (25 tests across all four test files) passes; `alembic current` still resolves to `0001 (head)` with no changes to `infra/migrations/env.py`
- **Committed in:** cfbc236 (Task 1 commit)

**3. [Rule 3 - Blocking] Redis container was not running; host port mismatch between `infra/.env`'s override and root `.env`'s `REDIS_URL`**
- **Found during:** Task 1, precondition check before starting implementation
- **Issue:** Only Postgres and MongoDB were up from a prior session; Redis was not running at all. Bringing it up via `docker compose up -d redis` picked up `infra/.env`'s `REDIS_HOST_PORT=6380` (a Plan 01 port-conflict override), but the root `.env` the app reads still pointed `REDIS_URL` at the default port 6379 — a session-environment drift, not a code defect.
- **Fix:** Brought Redis up with `REDIS_HOST_PORT=6379 docker compose up -d redis` (env var precedence overrides the `.env` file default), aligning the container's host port with what the app's existing `.env` already expected. No application or compose-file code changed.
- **Files modified:** none (environment-only; infra/docker-compose.yml's `${REDIS_HOST_PORT:-6379}` pattern from Plan 01 already supported this without edits)
- **Verification:** `redis.asyncio.Redis.from_url(settings.redis_url).ping()` returns `True`
- **Committed in:** N/A (no code change)

**4. [Rule 1 - Bug] Test emails using `.test` TLD were rejected by Pydantic's `EmailStr` validator**
- **Found during:** Task 1, first test-writing pass
- **Issue:** `example.test` is an ICANN reserved/special-use TLD; `pydantic[email-validator]`'s `EmailStr` rejects it with "special-use or reserved name" — not a bug in the app's validation (which is correctly strict), but a bug in the test fixtures' choice of domain.
- **Fix:** Switched test email fixtures to `example.com` (a domain IANA explicitly reserves for documentation/testing use and that passes standard email-format validators).
- **Files modified:** tests/test_auth.py
- **Verification:** Registration requests with `*.example.com` addresses pass Pydantic validation
- **Committed in:** cfbc236 (Task 1 commit, written before the first commit)

**5. [Rule 1 - Bug] python-jose's own `jws.sign` refuses to encode a token with `algorithm="none"`**
- **Found during:** Task 1, writing the alg:none rejection test
- **Issue:** The test originally tried to use `jose_jwt.encode(claims, key="", algorithm="none")` to build a forged token, but python-jose's `ALGORITHMS.SUPPORTED` doesn't include `"none"`, so the encoder itself raises `JWSError` before the test can even reach `decode_token` — the encoder being defensive here is a good thing, but it meant the test needed a different way to construct the forged token.
- **Fix:** Built the alg:none token manually (base64url-encoded `{"alg":"none","typ":"JWT"}` header + payload + empty signature segment) — the exact wire format a real attacker sending an alg:none forgery would send, which is arguably a more faithful test than round-tripping through a library that (correctly) refuses to produce it.
- **Files modified:** tests/test_auth.py
- **Verification:** `test_decode_token_rejects_alg_none_forgery` passes — the forged token is rejected 401 by `get_current_user`
- **Committed in:** cfbc236 (Task 1 commit, written before the first commit)

---

**Total deviations:** 5 auto-fixed (2 Rule 1 bugs in application code — passlib/bcrypt incompatibility and the event-loop singleton bug — both required for this plan's own acceptance criteria to be true; 1 Rule 3 blocking environment fix with no code change; 2 Rule 1 test-fixture bugs found while writing the tests themselves)
**Impact on plan:** All five fixes were necessary for the plan's stated `<verify>` commands to actually pass end-to-end. The event-loop singleton fix (deviation #2) is the most consequential — it is a correctness fix to shared infrastructure from Plan 01/02 that no prior test had exercised, not new functionality or scope creep; the fix preserves the exact same public API (`get_db`, and now `get_mongo`) every later plan's routes already depend on.

## Issues Encountered
- Jaeger (OTLP collector) is not running in this session's Docker Compose stack (only Postgres/MongoDB/Redis were brought up, per the plan's `<precondition>`, which does not list Jaeger as required for the tracer's own tests). The OTel OTLP gRPC exporter logs `Transient error ... Connection refused` on each retry but does NOT block or fail any request — this is the exporter's own designed-in graceful-degradation behavior (background batch export with retry/backoff), not an app-level error path. All 20 auth/RBAC/audit tests pass regardless of Jaeger's availability, confirming the tracer's OTel wiring degrades gracefully rather than becoming a hard dependency. Bringing up Jaeger and visually confirming spans in its UI is a good manual verification step for Plan 06's dashboard reconciliation, but is not required for this plan's automated `<verify>` to pass as written.

## User Setup Required

None — no external service configuration required beyond what Plans 01/02 already documented (Docker Desktop/Compose v2). This session brought up Redis (`docker compose up -d redis`, with a host-port override matching the root `.env`'s existing `REDIS_URL`) in addition to the already-running Postgres/MongoDB containers.

## Next Phase Readiness

Plans 04/05 can now build directly on:
- `services/identity/security/current_user.py`'s `get_current_user`/`AuthenticatedUser` and `rbac.py`'s `require_role(*roles)` — every new protected route just adds `Depends(require_role(Role.X))`
- `libs/audit/writer.py`'s `write_audit(...)` — every new mutating route calls this one helper, never a per-router Mongo insert
- `libs/auth/jwt.py`/`passwords.py`/`denylist.py` — the full auth primitive set is stable and tested
- `services/identity/app.py`'s app factory — new routers mount via `app.include_router(...)` in the same file
- `smarthealth/db.py`'s `get_db`/`get_mongo` FastAPI dependencies — unchanged public contract, now event-loop-safe

No blockers carried forward from this plan specifically. Existing Phase 1 blockers (Temporal SDK/server version compatibility, outbox relay mechanism decision, auto-setup vs. production-shaped Temporal) remain as previously recorded in STATE.md and are unaffected by this plan's scope. One item to track forward for Plan 06's dashboard reconciliation: Jaeger/Prometheus/Grafana visual verification of this plan's OTel/metrics wiring was not performed this session (containers not brought up) — the wiring is code-complete and unit/integration-tested to degrade gracefully without them, but a live trace/metric visual check is still worth doing once the full Compose stack is up together.

---
*Phase: 01-foundation-core-services*
*Completed: 2026-09-09*

## Self-Check: PASSED

All 16 created files verified present on disk; all 3 commits (cfbc236, 20663e7, 2011a4e) verified present in git log.
