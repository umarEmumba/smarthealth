---
phase: 01-foundation-core-services
plan: 03
type: execute
wave: 3
depends_on: ["01-01", "01-02"]
files_modified:
  - libs/observability/otel.py
  - libs/observability/logging.py
  - libs/auth/jwt.py
  - libs/auth/passwords.py
  - libs/auth/denylist.py
  - libs/audit/writer.py
  - services/identity/domain/schemas.py
  - services/identity/security/rbac.py
  - services/identity/security/current_user.py
  - services/identity/api/auth.py
  - services/identity/api/admin_ping.py
  - services/identity/app.py
  - tests/test_auth.py
  - tests/test_rbac.py
  - tests/test_audit.py
autonomous: false
requirements: [IAM-01, IAM-02, IAM-03, IAM-04]

estimate:
  tokens: 74000
  raw_tokens: 47000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "A user can register (POST /api/auth/register) and log in (POST /api/auth/login) and receive both an access JWT (~15 min) and a refresh JWT (~7 days)"
    - "The access JWT carries sub, role, clinic_id, jti, type=access, exp, iat; the refresh JWT carries only sub, jti, type=refresh, exp"
    - "Every protected route validates the access token on each request: signature+expiry via python-jose with algorithms pinned to an allowlist, then a Redis jti-denylist EXISTS check (IAM-03)"
    - "POST /api/auth/logout writes the token jti to the Redis denylist with TTL = remaining token life; a subsequent request with that token returns 401"
    - "A front-desk-role token calling an admin-only route returns 403; an admin token returns 200 (IAM-02, end-to-end)"
    - "Every mutating action writes an audit_log document to MongoDB with actor_id, actor_role, action, entity_type, entity_id, clinic_id, timestamp, trace_id, outcome (IAM-04)"
    - "structlog emits JSON logs carrying trace_id/span_id and NO PHI field values (patient name/dob/contact never appear in a log event) (IAM-04)"
    - "jwt.decode is called with an explicit algorithms allowlist and rejects an alg:none token (V6 crypto)"
    - { statement: "Registering the same email twice does not create two auth identities — the second attempt is rejected (email uniqueness / idempotent registration)", verification: explicit }
    - { statement: "A failed mutation attempt (e.g., a 403) also produces an audit_log entry with outcome=failure — audit captures denied attempts, not only successes", verification: backstop }
    - { statement: "The audit_log document records which field changed but NOT its PHI value — the changes field stores field names, never patient name/dob/contact plaintext", verification: explicit }
    - { statement: "A refresh exchange (POST /api/auth/refresh) re-derives role/clinic from current DB state, so a role change takes effect on next refresh (empty/single/expired refresh token is rejected)", verification: backstop }
  artifacts:
    - path: libs/auth/jwt.py
      provides: "create_access_token, create_refresh_token, decode_token (algorithms allowlist), token claim shapes"
      contains: "algorithms"
    - path: libs/auth/denylist.py
      provides: "revoke_token(jti, ttl), is_revoked(jti) against redis.asyncio"
      contains: "is_revoked"
    - path: services/identity/security/rbac.py
      provides: "require_role(*roles) FastAPI dependency raising 403"
      contains: "403"
    - path: services/identity/security/current_user.py
      provides: "get_current_user dependency (decode + denylist check → AuthenticatedUser)"
    - path: libs/audit/writer.py
      provides: "write_audit(...) single shared helper used by every mutating route (no per-router copy)"
      contains: "audit_log"
    - path: libs/observability/logging.py
      provides: "structlog config with OTel trace-context processor + PHI-safe convention"
      contains: "trace_id"
    - path: libs/observability/otel.py
      provides: "OTel tracer/exporter setup (OTLP→Jaeger), FastAPI auto-instrumentation with request/response body capture disabled"
    - path: services/identity/api/auth.py
      provides: "register / login / refresh / logout endpoints"
      exports: ["router"]
    - path: services/identity/app.py
      provides: "FastAPI app factory wiring routers, OTel, structlog, prometheus /metrics"
      exports: ["app"]
  key_links:
    - from: services/identity/security/current_user.py
      to: libs/auth/denylist.py
      via: "get_current_user calls is_revoked(jti) on every protected request (IAM-03 revocation)"
      pattern: "is_revoked"
    - from: services/identity/api/admin_ping.py
      to: services/identity/security/rbac.py
      via: "admin-only route declares Depends(require_role(Role.ADMIN)) (IAM-02)"
      pattern: "require_role"
    - from: libs/audit/writer.py
      to: libs/observability/otel.py
      via: "write_audit reads the active OTel span's trace_id to correlate the audit entry to the request trace"
      pattern: "trace_id"
    - from: services/identity/app.py
      to: libs/observability/logging.py
      via: "app startup configures structlog + OTel so every request emits correlated PHI-safe logs/spans"
      pattern: "configure"
  prohibitions:
    - "jwt.decode MUST NOT accept the algorithm from the token header without an allowlist — pin algorithms=['HS256'] (or the chosen alg); accepting alg:none is a JWT footgun this plan explicitly tests against"
    - "RBAC MUST NOT be enforced only client-side or only at router registration — the 403 is a server-side dependency guard, and clinic scoping is a query-layer filter, not a UI check"
    - "Audit writes MUST NOT be copy-pasted per router — exactly one shared write_audit helper (a per-endpoint Mongo insert is how an endpoint silently ends up unaudited)"
    - "Log events and OTel span attributes MUST NOT contain PHI field values (patient name, dob, contact) — log only identifiers (patient_id, clinic_id) and jti/sub"
    - "Full JWT token contents MUST NOT be logged at any level — log only jti and sub if correlation is needed"
---

<objective>
Build ONE production-quality end-to-end vertical slice that proves the entire Phase-1 architecture wired together: register a user → log in → receive access+refresh JWTs → call an RBAC-scoped protected route → the request validates the token (signature + Redis denylist) → an audit_log entry is written to MongoDB → a PHI-safe structured log line carrying the trace_id is emitted → the OTel span is exported to Jaeger. This is the TRACER (Wave 3) — the thinnest path through every layer the phase touches, carrying real error handling and a real end-to-end verify. It is written for keeps: it becomes the skeleton (shared auth/RBAC/audit/observability libs) that Plans 04 and 05 expand CRUD breadth onto.

Purpose: Prove the architecture end-to-end on the executor's best early-context tokens BEFORE building out full CRUD. If the auth/RBAC/audit/observability wiring has an architectural dead-end, catch it here after one slice, not after ten CRUD endpoints. Satisfies IAM-01/02/03/04 completely. Stubs are allowed only where later-fillable without architectural change (e.g., the admin-ping route is a minimal RBAC probe, not full admin CRUD — that arrives in Plan 04).

Output: shared observability libs (otel, logging), shared auth libs (jwt, passwords, denylist), shared audit writer, RBAC + current_user security deps, auth router (register/login/refresh/logout), a minimal admin-only probe route, the FastAPI app factory, and integration tests proving the full slice.

**Flagged assumptions surfaced here (no CONTEXT.md locked them — following RESEARCH Decision 1):** access JWT ~15 min + refresh ~7 days; Redis jti denylist for revocation; access carries sub/role/clinic_id/jti, refresh carries sub/jti only. Single-clinic-scoped staff (clinic_id scalar claim, RESEARCH A1). department scoping is a query-time filter, not a token claim.
</objective>

<execution_context>
@~/.claude/gsd-core/workflows/execute-plan.md
@~/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.claude/CLAUDE.md
@.planning/phases/01-foundation-core-services/01-RESEARCH.md
@.planning/phases/01-foundation-core-services/01-01-SUMMARY.md
@.planning/phases/01-foundation-core-services/01-02-SUMMARY.md

<interfaces>
<!-- Consumes from Plan 01: -->
smarthealth.config.settings (jwt_secret, jwt_algorithm, access_token_expire_minutes, refresh_token_expire_days, redis_url, mongo_url, otlp_endpoint)
tests/conftest.py fixtures: db_session, mongo_client, redis_client, client
<!-- Consumes from Plan 02: -->
smarthealth.db: Base, get_db, async_session
services.identity.domain.enums: Role (PATIENT/PROVIDER/FRONT_DESK/ADMIN), SlotStatus
services.identity.repository.models: User (id, email unique, hashed_password, role, clinic_id)
libs.audit.mongo_indexes: ensure_audit_indexes(mongo_client)

<!-- This plan CREATES the shared security/audit/observability contract Plans 04/05 consume: -->
libs/auth/jwt.py: create_access_token(user_id, role, clinic_id)->str, create_refresh_token(user_id)->str, decode_token(token, expected_type)->claims (algorithms allowlist enforced)
libs/auth/passwords.py: hash_password(pw)->str, verify_password(pw, hashed)->bool (passlib/bcrypt)
libs/auth/denylist.py: revoke_token(jti, ttl_seconds, redis), is_revoked(jti, redis)->bool
services/identity/security/current_user.py: get_current_user (Depends) -> AuthenticatedUser(sub, role, clinic_id, jti)
services/identity/security/rbac.py: require_role(*allowed: Role) -> dependency (403 on mismatch)
libs/audit/writer.py: write_audit(mongo, *, actor_id, actor_role, action, entity_type, entity_id, clinic_id, trace_id, outcome, changes=None)
services/identity/app.py: app (FastAPI) — the ASGI app the conftest `client` fixture targets
</interfaces>
</context>

<artifacts_this_phase_produces>
- Endpoints: POST /api/auth/register, POST /api/auth/login, POST /api/auth/refresh, POST /api/auth/logout, GET /api/admin/ping (admin-only RBAC probe), GET /metrics (prometheus)
- `libs/observability/otel.py` → setup_tracing(app), tracer; `libs/observability/logging.py` → configure_logging(), add_trace_context processor
- `libs/auth/jwt.py` → create_access_token, create_refresh_token, decode_token; `libs/auth/passwords.py` → hash_password, verify_password; `libs/auth/denylist.py` → revoke_token, is_revoked
- `libs/audit/writer.py` → write_audit (the single shared audit helper)
- `services/identity/security/current_user.py` → get_current_user, AuthenticatedUser; `services/identity/security/rbac.py` → require_role
- `services/identity/domain/schemas.py` → RegisterRequest, LoginResponse, TokenPair Pydantic v2 models
- `services/identity/api/auth.py` → router; `services/identity/api/admin_ping.py` → router
- `services/identity/app.py` → app (FastAPI factory)
</artifacts_this_phase_produces>

<tasks>

<task type="checkpoint:decision" gate="blocking-human">
  <decision>Confirm the auth/session model (JWT claim shape + revocation strategy) and the audit_log document shape before they are written as contracts (no CONTEXT.md locked them)</decision>
  <context>This is a one-way door: the JWT claim shape (access carries sub/role/clinic_id/jti; refresh carries sub/jti only) and the audit_log document fields are contracts every later route in this phase AND both later phases emit against. Changing the claim set or audit fields after Plans 04/05 and Phase 2/3 write against them is a coordinated multi-site change plus re-issuing every live token. RESEARCH Decision 1 recommends the options below; this checkpoint confirms the door before the agent walks through it.</context>
  <options>
    <option id="recommended">
      <name>Access JWT (~15 min) + refresh JWT (~7 days), Redis jti-denylist for revocation; access carries sub/role/clinic_id/jti, refresh carries sub/jti only</name>
      <pros>Short access TTL bounds exposure; working prompt logout/revocation (IAM-03 implies it); TTL-matched denylist entries self-expire so Redis use is bounded; Redis is already mandated for Phase-2 slot locks so no new infra; role change takes effect on next refresh (RESEARCH Decision 1)</pros>
      <cons>One Redis round-trip per authenticated request; single-clinic-scoped-staff assumption (clinic_id scalar claim, RESEARCH A1) needs a claims redesign if multi-clinic staff ever appear</cons>
    </option>
    <option id="no-revocation">
      <name>Access + refresh JWT, no revocation list (client-side logout only)</name>
      <pros>Zero Redis dependency for auth; simpler</pros>
      <cons>A stolen/leaked access token stays valid until natural expiry regardless of logout — weaker for a build that explicitly grades security fundamentals (RESEARCH rejects)</cons>
    </option>
    <option id="access-only">
      <name>Access-only JWT (no refresh, no denylist)</name>
      <pros>Simplest possible</pros>
      <cons>Forces frequent re-login (poor UX for all-shift staff) or long-lived tokens with no clean revocation — neither satisfies IAM-03's implied working logout (RESEARCH rejects)</cons>
    </option>
  </options>
  <resume-signal>Select: recommended, no-revocation, or access-only (recommended is the RESEARCH-backed default)</resume-signal>
</task>

<task type="tracer">
  <name>End-to-end "register → login → RBAC-scoped request → audit → PHI-safe log/trace" — one path only</name>
  <read_first>.planning/phases/01-foundation-core-services/01-RESEARCH.md (Pattern 1 RBAC dependency, Pattern 2 JWT+Redis denylist, PHI-Safe Structured Logging, MongoDB Audit-Trail Schema, Code Examples: login route + audit-write helper), .claude/CLAUDE.md (python-jose/passlib/bcrypt/structlog/OTel mandate), smarthealth/config.py, services/identity/repository/models.py (User model), tests/conftest.py</read_first>
  <precondition>Postgres, MongoDB, and Redis containers from Plan 01 are reachable and Plan 02's migration has been applied (users table exists) — the tracer is a real end-to-end path, not a mock; assert reachability + `alembic current` shows head before running</precondition>
  <reversibility rating="one-way">The JWT claim shape (access carries sub/role/clinic_id/jti; refresh carries sub/jti) and the audit_log document shape are contracts every later route and both later phases emit against; changing the claim set or audit fields after Plans 04/05 and Phase 2/3 write against them is a coordinated multi-site change plus re-issuing all live tokens.</reversibility>
  <files>libs/observability/otel.py, libs/observability/logging.py, libs/auth/jwt.py, libs/auth/passwords.py, libs/auth/denylist.py, libs/audit/writer.py, services/identity/domain/schemas.py, services/identity/security/current_user.py, services/identity/security/rbac.py, services/identity/api/auth.py, services/identity/api/admin_ping.py, services/identity/app.py</files>
  <action>Wire ONE path through every layer, end-to-end, no extra call sites and no CRUD breadth. (1) libs/observability/otel.py: configure the OpenTelemetry tracer + OTLP exporter targeting settings.otlp_endpoint (Jaeger), instrument FastAPI via opentelemetry-instrumentation-fastapi with request/response body capture DISABLED (PHI safety). (2) libs/observability/logging.py: structlog processor chain with an add_trace_context processor injecting trace_id/span_id from the active OTel span, JSONRenderer, and the PHI-safe convention (log identifiers only). (3) libs/auth/passwords.py: hash_password/verify_password via passlib[bcrypt]. (4) libs/auth/jwt.py: create_access_token (claims sub, role, clinic_id, jti, type=access, exp, iat), create_refresh_token (sub, jti, type=refresh, exp), decode_token pinning algorithms=[settings.jwt_algorithm] and rejecting alg:none. (5) libs/auth/denylist.py: revoke_token/is_revoked against redis.asyncio keyed revoked:{jti} with TTL. (6) libs/audit/writer.py: the single write_audit helper inserting one audit_log doc (actor_id, actor_role, action, entity_type, entity_id, clinic_id, timestamp, trace_id from active span, outcome, changes=field-names-only). (7) security/current_user.py: get_current_user decodes the bearer token, checks is_revoked, returns AuthenticatedUser; security/rbac.py: require_role(*roles) raising HTTPException(403). (8) api/auth.py: register (hash pw, insert User, reject duplicate email, write_audit user.create), login (verify pw, issue token pair, write_audit auth.login), refresh (validate refresh token, re-derive role/clinic from DB, issue new access token), logout (revoke access jti, write_audit auth.logout). (9) api/admin_ping.py: GET /api/admin/ping with Depends(require_role(Role.ADMIN)) returning 200 — the minimal RBAC probe (real admin CRUD is Plan 04). (10) app.py: FastAPI factory mounting both routers under /api, calling configure_logging + setup_tracing + ensure_audit_indexes at startup, and prometheus-fastapi-instrumentator exposing /metrics. Real error handling on this one path (401 invalid creds, 401 revoked/expired, 403 wrong role, 409 duplicate email). No PHI in any log or span attribute.</action>
  <acceptance_criteria>
    - POST /api/auth/register then POST /api/auth/login returns 200 with access_token + refresh_token
    - a decoded access token contains claims: sub, role, clinic_id, jti, type=access
    - GET /api/admin/ping with an admin token returns 200; with a front-desk token returns 403
    - POST /api/auth/logout then re-using the same access token on /api/admin/ping returns 401 (denylist works)
    - after register+login, the audit_log collection contains user.create and auth.login entries carrying a trace_id
    - a captured log line for a register is valid JSON, contains trace_id, and does NOT contain the registered user's PHI values
    - decode_token rejects a token forged with alg:none
    - registering a duplicate email returns 409 and creates no second user
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_auth.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — register/login/refresh/logout, the token claim shape, or the alg:none rejection failed end-to-end</fails_when>
  </verify>
  <done>The single happy path (register→login→RBAC-scoped request→audit written→PHI-safe log/trace) works end-to-end and is committed. Logout revocation, alg:none rejection, and duplicate-email rejection all hold.</done>
</task>

<task type="auto">
  <name>Task 2: RBAC enforcement tests (403 end-to-end, IAM-02)</name>
  <read_first>services/identity/security/rbac.py + services/identity/api/admin_ping.py (from Task 1), tests/conftest.py (client fixture + JWT test helper), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Pattern 1 + Pitfall 1 clinic-scope)</read_first>
  <files>tests/test_rbac.py</files>
  <action>Create tests/test_rbac.py exercising the RBAC guard end-to-end through the real app: (a) test_forbidden_action_403 — a front-desk-role token GET /api/admin/ping returns 403; (b) test_allowed_action_200 — an admin token GET /api/admin/ping returns 200; (c) test_missing_token_401 — no Authorization header returns 401; (d) test_patient_token_forbidden — a patient token on the admin route returns 403. Use the conftest JWT-issuing helper to mint role-scoped tokens. These prove IAM-02's success-criterion-2 behavior (a front-desk account is rejected 403 on an admin-only operation) at the HTTP boundary, not just the dependency unit.</action>
  <acceptance_criteria>
    - `pytest tests/test_rbac.py -x` passes all four cases
    - the 403 originates from the server-side require_role dependency (assert the response status, not a client check)
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_rbac.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — a wrong-role token was NOT rejected with 403, or a correct-role token was rejected</fails_when>
  </verify>
  <done>RBAC is proven enforced at the HTTP boundary: front-desk and patient tokens get 403 on the admin route, admin gets 200, no token gets 401.</done>
</task>

<task type="auto">
  <name>Task 3: Audit-trail + PHI-safe-logging tests (IAM-04)</name>
  <read_first>libs/audit/writer.py + libs/observability/logging.py (from Task 1), tests/conftest.py (mongo_client fixture), .planning/phases/01-foundation-core-services/01-RESEARCH.md (MongoDB Audit-Trail Schema + PHI-Safe Structured Logging + Pitfall 2 audit coverage)</read_first>
  <files>tests/test_audit.py</files>
  <action>Create tests/test_audit.py: (a) test_mutation_creates_audit_entry — perform a mutating action (register) and assert an audit_log document exists with the expected actor/action/entity/clinic/trace_id/outcome=success fields; (b) test_failed_mutation_audited — trigger a denied/failed mutation (a 403 on the admin route, or a duplicate-email 409) and assert an outcome=failure audit entry is written (Pitfall 2 — denied attempts are audited); (c) test_no_phi_in_logs — capture structlog output for an action involving a PHI-bearing value and assert the emitted JSON contains identifiers (ids) but NOT the PHI value; (d) test_audit_changes_field_names_only — assert the audit document's changes field, when present, holds field names not PHI plaintext values. Keep PHI test fixtures synthetic (fake name/dob) so the test itself never handles real PHI.</action>
  <acceptance_criteria>
    - `pytest tests/test_audit.py -x` passes all four cases
    - the captured log JSON for a PHI-involving action does not contain the synthetic PHI value string
    - a failed/denied mutation produces an outcome=failure audit entry
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_audit.py -x</automated>
    <fails_when>pytest exits non-zero or reports "0 passed" — a mutation produced no audit entry, a PHI value leaked into a log line, or a denied mutation was not audited</fails_when>
  </verify>
  <done>IAM-04 proven: mutating (and denied) actions produce audit_log entries with trace correlation, and no PHI value appears in log events or audit change-values.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| client → API | Untrusted credentials and bearer tokens cross here; every token is validated (signature + denylist) per request |
| API → Redis (denylist) | Revocation state; consulted on every protected request |
| API → MongoDB (audit) | Audit documents; must carry no PHI values |
| API → stdout logs / Jaeger spans | Observability sinks; must carry no PHI values |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-03-01 | Spoofing | JWT algorithm confusion / alg:none acceptance | high | mitigate | decode_token pins algorithms=[jwt_algorithm]; a dedicated test forges an alg:none token and asserts rejection (V6) |
| T-03-02 | Elevation of Privilege | Token replay after logout | high | mitigate | Redis jti denylist checked on every protected request; logout revokes with TTL=remaining life; test re-uses a logged-out token and asserts 401 (Pattern 2) |
| T-03-03 | Elevation of Privilege | Broken access control (wrong role reaches admin op) | high | mitigate | Server-side require_role dependency; test_rbac.py asserts front-desk/patient → 403, admin → 200 at the HTTP boundary (IAM-02) |
| T-03-04 | Information Disclosure | PHI leakage via logs / OTel span attributes / audit values | high | mitigate | structlog PHI-safe processor (identifiers only); FastAPI auto-instrumentation body capture disabled; audit changes field stores field names not values; test_no_phi_in_logs asserts absence |
| T-03-05 | Information Disclosure | Credential/token disclosure via logs | high | mitigate | Passwords bcrypt-hashed (never stored/logged plaintext); full token contents never logged — only jti/sub |
| T-03-06 | Repudiation | Unaudited mutation / denied action | medium | mitigate | Single shared write_audit used by every mutating route; failed/denied mutations also audited (outcome=failure), Pitfall 2 |
| T-03-SC | Tampering | npm/pip/cargo installs | high | mitigate | Package legitimacy audit complete in RESEARCH (all approved, none [SLOP]); no new packages introduced by this plan beyond the pinned set installed in Plan 01 |
</threat_model>

<verification>
- Full auth slice green: register/login/refresh/logout, token claim shape, denylist revocation, alg:none rejection, duplicate-email 409 (`pytest tests/test_auth.py`).
- RBAC 403/200/401 at the HTTP boundary (`pytest tests/test_rbac.py`).
- Audit entries on success and failure, trace-correlated; no PHI in logs/spans/audit-values (`pytest tests/test_audit.py`).
- Full slice run: `pytest tests/test_auth.py tests/test_rbac.py tests/test_audit.py -x`.

**Tracer feedback gate:** this plan LEADS the phase with a `type="tracer"` task. Its end-to-end `<verify>` (`pytest tests/test_auth.py`) must pass before the expansion plans (04, 05) build CRUD breadth on the proven auth/RBAC/audit/observability skeleton.
</verification>

<success_criteria>
IAM-01/02/03/04 fully satisfied by a proven end-to-end slice: role-scoped auth with validated+revocable tokens, server-side RBAC 403 enforcement, MongoDB audit trail with trace correlation, and PHI-safe structured logging + tracing. The shared auth/RBAC/audit/observability libs are the skeleton Plans 04/05 expand.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-03-SUMMARY.md` when done. Record the exact JWT claim shape and audit_log document shape as delivered (contracts Plans 04/05 and Phases 2/3 depend on), and the pinned JWT algorithm.
</output>
