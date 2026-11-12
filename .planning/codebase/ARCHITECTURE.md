---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
<!-- refreshed: 2026-09-30 -->

# Architecture

**Analysis Date:** 2026-09-30

## System Overview

```text
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI HTTP Layer                        │
│                    `services/identity/api/`                      │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐  │
│  │  Auth Router │ Clinic Router │ Patient Router│Provider Router│  │
│  │ `auth.py`    │ `clinics.py`  │ `patients.py` │ `providers.py` │  │
│  └──────────────┴──────────────┴──────────────┴──────────────┘  │
│  Plus: departments, availability, admin_ping routers            │
└────┬────────────────────────────────────────────────────────────┘
     │
┌────▼────────────────────────────────────────────────────────────┐
│              Security & RBAC Middleware Layer                    │
│               `services/identity/security/`                      │
│  ┌──────────────────────┬──────────────────┐                    │
│  │ get_current_user()   │ require_role()   │                    │
│  │ JWT validation +     │ RBAC enforcement │                    │
│  │ denylist check       │ (4-role model)   │                    │
│  └──────────────────────┴──────────────────┘                    │
└────┬────────────────────────────────────────────────────────────┘
     │
┌────▼─────────────────────────────────────────────────────────────┐
│              Repository & Data Access Layer                       │
│              `services/identity/repository/`                      │
│  ┌──────────────────┬──────────────────┐                        │
│  │  org_repo.py     │  people_repo.py  │                        │
│  │ (Clinic, Dept,   │ (Patient, Provider│                       │
│  │  Availability)   │  Availability)   │                        │
│  └──────────────────┴──────────────────┘                        │
│  (SQLAlchemy 2.0 async ORM layer)                               │
└────┬────────────────────────────────────────────────────────────┘
     │
┌────┴────────────────────────┬──────────────────────┬─────────────┐
│                              │                      │             │
▼                              ▼                      ▼             ▼
PostgreSQL                  MongoDB                 Redis        Observability
(System of Record)          (Audit Trail)          (Cache,       (OTel + Logs)
`smarthealth` DB            `audit_log` coll       Locks,        Jaeger + Prometheus
                                                   Denylist)
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Auth Router | Register, login, refresh, logout, admin user creation | `services/identity/api/auth.py` |
| Clinic Router | Clinic CRUD (create, list, detail, update, deactivate) | `services/identity/api/clinics.py` |
| Department Router | Department CRUD under clinic scope | `services/identity/api/departments.py` |
| Patient Router | Patient CRUD under clinic scope with PHI | `services/identity/api/patients.py` |
| Provider Router | Provider CRUD and management | `services/identity/api/providers.py` |
| Availability Router | Provider availability templates | `services/identity/api/availability.py` |
| Security Layer | JWT validation, token revocation, RBAC enforcement | `services/identity/security/` |
| org_repo | Clinic, department, organization queries | `services/identity/repository/org_repo.py` |
| people_repo | Patient, provider, availability data access | `services/identity/repository/people_repo.py` |
| Models | SQLAlchemy ORM definitions (User, Clinic, Patient, etc.) | `services/identity/repository/models.py` |
| Auth Library | JWT encode/decode, password hashing, denylist | `libs/auth/` |
| Audit Library | MongoDB audit logging, indexes | `libs/audit/` |
| Observability | OTel instrumentation, structlog configuration | `libs/observability/` |
| Configuration | Settings from environment via pydantic-settings | `smarthealth/config.py` |
| Database Layer | Async SQLAlchemy engine, session factory, MongoDB client | `smarthealth/db.py` |

## Pattern Overview

**Overall:** Layered, single-process FastAPI monolith with strict separation between HTTP layer (routers), business logic (repositories), data access (ORM), and cross-cutting concerns (auth, audit, observability).

**Key Characteristics:**
- **Fully async-only**: No sync blocking code in any route; all database access via asyncpg + PyMongo AsyncMongoClient
- **Single ASGI process**: One FastAPI app (`smarthealth.main:app`) serving all routers; future services will mount additional routers onto this app
- **Event-loop aware singletons**: Database engines and Mongo clients transparently rebind to new event loops (critical for pytest-asyncio multi-loop testing)
- **Layered request flow**: HTTP request → FastAPI dependency injection (auth/RBAC) → Repository → ORM → Database
- **Audit-first design**: Every mutation writes to MongoDB audit log via shared `write_audit()` helper; PHI-safe (field names only, never values)
- **Multi-database**: PostgreSQL (transactional), MongoDB (audit/logs), Redis (cache/locks/denylist)

## Layers

**API/Router Layer:**
- Purpose: HTTP request/response handling, dependency injection of auth/RBAC guards, request validation via Pydantic schemas
- Location: `services/identity/api/`
- Contains: Router definitions, request/response models, error handling
- Depends on: Security (RBAC guards), Repository (data access), Domain (schemas), Observability (logging)
- Used by: FastAPI app, test client

**Security Layer:**
- Purpose: JWT token validation, Redis denylist revocation check, RBAC role enforcement
- Location: `services/identity/security/`
- Contains: `get_current_user()` (token validation + revocation), `require_role()` (RBAC guards), `AuthenticatedUser` dataclass
- Depends on: Auth library (JWT decode), Redis (denylist), Domain (Role enum)
- Used by: Every protected router endpoint as a FastAPI `Depends()`

**Repository Layer:**
- Purpose: Data access abstraction; encapsulates SQLAlchemy queries; returns domain models or None
- Location: `services/identity/repository/`
- Contains: `org_repo.py` (clinic/department/organizational queries), `people_repo.py` (patient/provider/availability queries)
- Depends on: Models (ORM definitions), Database (AsyncSession), Domain (enums)
- Used by: API routers, tests

**Domain Layer:**
- Purpose: Shared types: Pydantic request/response schemas, enums (Role, SlotStatus), type definitions
- Location: `services/identity/domain/`
- Contains: `schemas.py` (Pydantic models), `enums.py` (Role, SlotStatus)
- Depends on: Pydantic
- Used by: API routers, repositories, security layer

**ORM/Models Layer:**
- Purpose: SQLAlchemy 2.0 async declarative models for the relational schema
- Location: `services/identity/repository/models.py`
- Contains: User, Clinic, Department, Patient, Provider, Specialty, AvailabilityTemplate, ProviderSlot
- Depends on: SQLAlchemy, database config
- Used by: Repository layer, Alembic migrations

**Cross-Cutting Layers:**

**Auth Library** (`libs/auth/`):
- JWT encode/decode (`jwt.py`), password hashing (`passwords.py`), Redis denylist (`denylist.py`)
- Shared by: Security layer, routers (register/login)

**Audit Library** (`libs/audit/`):
- MongoDB audit logging (`writer.py`), index setup (`mongo_indexes.py`)
- Used by: Every mutating router via `write_audit()`

**Observability Library** (`libs/observability/`):
- OTel tracer setup (`otel.py`), structlog configuration (`logging.py`)
- Used by: App lifespan, routers (logging), audit writer (trace ID injection)

**Configuration & Database**:
- `smarthealth/config.py`: Pydantic Settings for environment-based config (DB URLs, JWT secret, OTLP endpoint)
- `smarthealth/db.py`: Async SQLAlchemy engine factory, session factory, MongoDB client factory; handles event-loop binding

## Data Flow

### Primary Request Path (e.g., Register a Patient)

1. **HTTP Request** → FastAPI router (`services/identity/api/auth.py:register()`)
2. **Dependencies Resolved**:
   - `Depends(get_db)` → AsyncSession (from `smarthealth/db.py`)
   - `Depends(get_mongo)` → AsyncMongoClient (from `smarthealth/db.py`)
3. **Validation** → Pydantic `RegisterRequest` schema validates email/password format
4. **Duplicate Check** → Repository query via SQLAlchemy ORM: `select(User).where(User.email == payload.email)`
5. **Create User** → ORM: `User(email=..., hashed_password=hash_password(...), role='patient')`; `db.add()` + `db.commit()`
6. **Audit Write** → `write_audit(mongo, actor_id='anonymous', action='user.create', outcome='success', ...)` → MongoDB `audit_log` collection
7. **Logging** → `log.info('auth.register.success', user_id=...)` via structlog
8. **Response** → FastAPI returns `UserOut` (Pydantic model), HTTP 201

### Token Refresh Path (Refresh Access Token)

1. **HTTP Request** with refresh token → `services/identity/api/auth.py:refresh()`
2. **Dependencies** → `Depends(get_current_user)` (via refresh token validation)
   - `decode_token(token, expected_type='refresh')` → validates signature/expiry
   - Check Redis denylist: `is_revoked(jti, redis)` → raise 401 if revoked
3. **Re-derive from DB** → `get_user(db, user_id)` → fetch User row to get CURRENT role/clinic (role changes take effect on next refresh)
4. **Issue new access token** → `create_access_token(user_id, role, clinic_id)` → JWT with 15-min expiry
5. **Response** → `AccessTokenResponse`

### Audit Data Flow (Cross-Cutting)

1. **Mutation endpoint** calls `write_audit(mongo_client, actor_id, action, entity_id, outcome, changed_fields=['field_name'], ...)`
2. **Inside `write_audit()`**:
   - Get current trace ID from active OTel span: `current_trace_id()`
   - Build document: `{actor_id, actor_role, action, entity_type, entity_id, clinic_id, timestamp(UTC), trace_id, outcome, changes: {fields: [...]}}`
   - Insert to MongoDB `smarthealth.audit_log` collection
3. **PHI Boundary**: `changed_fields` is field NAMES ONLY — never pass patient name/dob/phone/address values; callers enforce this discipline

### Observability Data Flow

1. **OTel Instrumentation**:
   - FastAPI auto-instrumentation captures: HTTP method, route, status code (NOT request/response bodies — PHI safe)
   - Every route is traced; spans include `trace_id`, `span_id`
2. **Structured Logging**:
   - `log.info("auth.register.success", user_id=user_id, email_domain=domain)` → structlog formats as JSON
   - OTel trace ID injected into logs via context
3. **Metrics**:
   - Prometheus `/metrics` endpoint exposes FastAPI request count/latency histograms
4. **Correlation**:
   - Audit log document's `trace_id` field allows audit records to be joined with Jaeger traces for full request replay

**State Management:**
- **Request-Scoped**: AsyncSession (from `Depends(get_db)`), per-request Redis client (from `get_redis()`)
- **Process-Scoped Singletons**: PostgreSQL AsyncEngine, MongoDB AsyncMongoClient, OTel TracerProvider
- **Event-Loop Aware**: Singletons rebind transparently if running event loop changes (e.g., pytest-asyncio's per-test-function loops)

## Key Abstractions

**AuthenticatedUser:**
- Purpose: Frozen dataclass representing the identity of the current request's authenticated user
- Examples: Returned by `get_current_user()`, holds sub (user ID), role, clinic_id, jti (token ID)
- Pattern: Passed as a dependency-injected parameter to protected routes; always non-None if route has `Depends(get_current_user)` or `Depends(require_role(...))`

**Role (Enum):**
- Purpose: 4-role RBAC model: PATIENT, PROVIDER, FRONT_DESK, ADMIN
- Examples: Stored in User.role column (string), in JWT role claim, in AuthenticatedUser.role
- Pattern: RBAC guards use `require_role(*allowed_roles)` to enforce role membership

**Repository Functions:**
- Purpose: Abstract data access; every repo function is async, takes AsyncSession, returns domain models or None/list
- Examples: `await org_repo.create_clinic(db, name=..., timezone=...)` → Clinic model, `await people_repo.get_patient(db, patient_id, clinic_id)` → Patient or None
- Pattern: No direct ORM imports in routers; routers call repository functions which handle the `select()`/`add()`/`commit()` logic

**Clinic-Scoped Lookups:**
- Purpose: Every patient/provider lookup requires explicit clinic_id to close enumeration attacks (RESEARCH Pitfall 1)
- Examples: `get_patient(db, patient_id, clinic_id)` — no patient-by-id-only function exists
- Pattern: Enforced at repository layer; clinic_id is a mandatory parameter, not optional

## Entry Points

**HTTP Entrypoint:**
- Location: `smarthealth/main.py:app`
- Triggers: Uvicorn/Gunicorn ASGI server starts `smarthealth.main:app` on port 8000 (or configured port)
- Responsibilities: Re-exports the FastAPI app created by `services/identity/app.py:create_app()`; later services will mount additional routers onto this same app

**App Factory (Lifespan):**
- Location: `services/identity/app.py:create_app()`
- Triggers: Called once at process startup
- Responsibilities:
  1. Configure structured logging via `configure_logging()`
  2. Set up MongoDB audit indexes via `ensure_audit_indexes(mongo_client)`
  3. Configure OTel tracer and auto-instrument FastAPI
  4. Mount all routers (auth, clinics, departments, patients, providers, availability)
  5. Expose Prometheus `/metrics` endpoint

**Test Entrypoint:**
- Location: `tests/conftest.py:client` fixture
- Triggers: Pytest creates an `AsyncClient` wired to the real `smarthealth.main:app`
- Responsibilities: Integration testing against live app, Postgres, Mongo, Redis

## Architectural Constraints

- **Threading:** Single-threaded event loop per process (uvicorn/gunicorn worker). No explicit threads spawned in Phase 1; all concurrency via asyncio. Celery/Temporal workers (Phase 2/3) will run in separate processes.
- **Global state:** AsyncEngine and AsyncMongoClient singletons in `smarthealth/db.py`, OTel TracerProvider singleton in `libs/observability/otel.py`. All transparently rebind to current event loop if it changes (critical for pytest-asyncio).
- **Circular imports:** None detected. Layering is respected: API → Security → Repository → Models.
- **One database commit per request:** Every repository function calls `db.commit()` after mutations. No long-running transactions left open at the end of a request.
- **No sync I/O in async routes:** All database calls via asyncpg + PyMongo AsyncMongoClient. No `requests` library (only httpx). No blocking operations inside `async def` endpoints.
- **Event-loop binding:** Postgres connections and Mongo connections bind to the event loop that created them and raise errors if reused from a different loop. `smarthealth/db.py` handles this by detecting loop changes and rebuilding engines/clients transparently.

## Anti-Patterns

### Mixing Sync and Async

**What happens:** A route calls `db.query()` (sync SQLAlchemy) or `requests.get()` (blocking HTTP client) inside an `async def` handler.
**Why it's wrong:** Blocks the single event-loop thread for every other concurrent request, defeating async concurrency. Under peak load (tens of thousands of requests), this serializes the work and causes response timeouts.
**Do this instead:** Use async equivalents throughout: SQLAlchemy 2.0 async engine with `AsyncSession`, `httpx.AsyncClient` for outbound calls. See `smarthealth/db.py` and `services/identity/api/` for the proven pattern.

### Direct ORM Imports in Routers

**What happens:** A router file imports `models.py` directly and writes raw `select(Patient)` or `Patient(...)` statements instead of calling a repository function.
**Why it's wrong:** Couples the router to ORM details; makes it hard to refactor queries or add caching later. Audit/validation logic gets duplicated across routers.
**Do this instead:** Define repository functions in `org_repo.py` or `people_repo.py`; routers call these functions. Repositories own all query logic. See `services/identity/api/patients.py` (calls `people_repo.get_patient()`) vs. what NOT to do (inline `select(Patient)`).

### Unscoped Patient/Provider Lookups

**What happens:** A route accepts a `patient_id` path parameter and does `await db.get(Patient, patient_id)` without checking clinic scope.
**Why it's wrong:** Allows enumeration: a provider in clinic A can fetch patient details from clinic B just by guessing UUIDs (RESEARCH Pitfall 1).
**Do this instead:** Every patient/provider lookup requires explicit `clinic_id` parameter: `await people_repo.get_patient(db, patient_id, clinic_id)`. No patient-by-id-only function exists in the codebase. See `services/identity/repository/people_repo.py` for the enforced pattern.

### Logging/Auditing PHI Values

**What happens:** A router calls `log.info("patient.update", full_name=patient.full_name)` or passes patient name to `write_audit()`.
**Why it's wrong:** Logs and audit trails become PHI-bearing; violates HIPAA PHI protection requirements and the "audit field names, not values" contract.
**Do this instead:** Only log identifiers (IDs, roles, domain names) and field NAMES. For audit changes, call `write_audit(..., changed_fields=['full_name', 'phone'])` — never the values themselves. See `services/identity/api/patients.py` and `libs/audit/writer.py` for the enforced PHI-safe pattern.

### Copy-Pasted Audit Writes

**What happens:** Each router writes its own `mongo.audit_log.insert_one({...})` instead of calling the shared `write_audit()` helper.
**Why it's wrong:** Makes audit coverage brittle; one endpoint silently ends up unaudited if the copy-paste is incomplete. Audit-log schema changes require updating every site.
**Do this instead:** Every mutating route must call the shared `libs.audit.writer.write_audit()` helper. Use it consistently. See `services/identity/api/auth.py` for the proven pattern.

### EventLoop Reuse Bugs

**What happens:** Code builds an AsyncEngine or AsyncMongoClient at module load time and stores it as a global, then pytest-asyncio runs multiple tests with different event loops.
**Why it's wrong:** Asyncpg/PyMongo connection pools bind to the loop they were created in. Reusing them from a different test's loop raises `RuntimeError: cannot schedule coroutine on a different loop`.
**Do this instead:** Let `smarthealth/db.py` handle event-loop awareness transparently. Use `get_engine()`, `get_session_factory()`, `get_mongo_client()` — these functions detect loop changes and rebuild on-the-fly. Never store raw engines/clients at module scope. See `smarthealth/db.py` for the working pattern.

---

*Architecture analysis: 2026-09-30*
