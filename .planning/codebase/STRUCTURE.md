---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# Codebase Structure

**Analysis Date:** 2026-09-30

## Directory Layout

```
smarthealth/                           # Core application module
├── main.py                            # ASGI entrypoint (re-exports app from identity service)
├── config.py                          # Pydantic Settings (environment-based configuration)
├── db.py                              # Async SQLAlchemy engine/session, MongoDB client, event-loop awareness
└── __init__.py

services/                              # Microservices (Phase 1 has identity; later phases add more)
└── identity/                          # Identity service (auth, users, org hierarchy, availability)
    ├── app.py                         # FastAPI app factory, lifespan setup, router mounting
    ├── __init__.py
    ├── api/                           # HTTP routers
    │   ├── __init__.py
    │   ├── auth.py                    # Register, login, refresh, logout, admin user creation
    │   ├── clinics.py                 # Clinic CRUD (create, list, detail, update, deactivate)
    │   ├── departments.py             # Department CRUD under clinic scope
    │   ├── patients.py                # Patient CRUD with full PHI (name, DOB, phone, address)
    │   ├── providers.py               # Provider CRUD and management
    │   ├── availability.py            # Provider availability templates (weekly schedules)
    │   └── admin_ping.py              # Admin-only diagnostic endpoint
    ├── domain/                        # Shared domain types
    │   ├── __init__.py
    │   ├── enums.py                   # Role (4-role RBAC), SlotStatus (appointment slot states)
    │   └── schemas.py                 # Pydantic request/response models (RegisterRequest, UserOut, etc.)
    ├── repository/                    # Data access layer (SQLAlchemy async ORM)
    │   ├── __init__.py
    │   ├── models.py                  # SQLAlchemy 2.0 declarative ORM models (User, Clinic, Patient, etc.)
    │   ├── org_repo.py                # Clinic, department, organizational data access
    │   └── people_repo.py             # Patient, provider, availability data access
    └── security/                      # Authentication & RBAC
        ├── __init__.py
        ├── current_user.py            # JWT validation, token revocation, AuthenticatedUser dataclass
        └── rbac.py                    # require_role() RBAC enforcement

libs/                                  # Shared libraries (used by all services)
├── __init__.py
├── auth/                              # Authentication & authorization primitives
│   ├── __init__.py
│   ├── jwt.py                         # JWT encode/decode, token factories
│   ├── passwords.py                   # Password hashing (bcrypt + passlib)
│   └── denylist.py                    # Redis token revocation denylist
├── audit/                             # Audit logging to MongoDB
│   ├── __init__.py
│   ├── writer.py                      # Shared write_audit() helper (PHI-safe: field names only)
│   └── mongo_indexes.py               # MongoDB audit_log collection index setup
└── observability/                     # Tracing, logging, metrics
    ├── __init__.py
    ├── otel.py                        # OpenTelemetry tracer setup, OTLP exporter (to Jaeger)
    └── logging.py                     # Structlog configuration, JSON log formatting

infra/                                 # Infrastructure & deployment
├── docker-compose.yml                 # Local dev stack (Postgres, Mongo, Redis, Kafka, etc.)
├── .env                               # Local dev environment variables (git-ignored)
├── .env.example                       # Example .env template
├── prometheus.yml                     # Prometheus scrape config (targets FastAPI /metrics)
└── migrations/                        # Alembic schema migrations
    ├── env.py                         # Alembic runtime config (async engine setup)
    ├── script.py.mako                 # Migration template
    └── versions/                      # Migration files (numbered, e.g., 0001_initial_schema.py)

scripts/                               # Utility scripts
└── bootstrap_admin.py                 # Create bootstrap admin user for local dev

tests/                                 # Pytest test suite
├── __init__.py
├── conftest.py                        # Shared pytest fixtures (db_session, mongo_client, redis_client, client)
├── test_auth.py                       # Auth flow tests (register, login, refresh, logout)
├── test_rbac.py                       # RBAC enforcement tests
├── test_clinics.py                    # Clinic CRUD tests
├── test_departments.py                # Department CRUD tests
├── test_patients.py                   # Patient CRUD tests
├── test_providers.py                  # Provider CRUD tests
├── test_availability.py               # Availability template tests
├── test_models.py                     # ORM model tests (constraints, exclusion indexes)
├── test_audit.py                      # Audit logging tests
├── test_passwords.py                  # Password hashing tests
├── test_bootstrap_admin.py            # Bootstrap script tests
└── test_smoke.py                      # Smoke tests (basic connectivity)

docs/                                  # Documentation
├── architecture-diagram.html          # Architecture diagram (Mermaid + HTML)
├── entity-relationship-diagram.html   # Entity-relationship diagram (Mermaid + HTML)
└── [other docs]

pyproject.toml                         # Python project metadata (dependencies, build config)
README.md                              # Project overview, setup, API guide
.env.example                           # Example environment variables
.gitignore                             # Git exclusions
```

## Directory Purposes

**smarthealth/:**
- Purpose: Core application module containing entry point, configuration, and database setup
- Contains: Main ASGI app, Settings object, async engine/session/mongo client factories
- Key files: `main.py` (entry point), `config.py` (env-based settings), `db.py` (database initialization)

**services/identity/:**
- Purpose: The Phase-1 identity microservice (auth, user management, organizational hierarchy)
- Contains: HTTP routers, domain types, ORM models, data access repositories, security/RBAC
- Key files: `app.py` (app factory), `api/` (routers), `repository/` (data access), `domain/` (schemas), `security/` (auth/RBAC)

**services/identity/api/:**
- Purpose: FastAPI routers exposing HTTP endpoints grouped by resource
- Contains: One router per resource (auth, clinics, departments, patients, providers, availability)
- Pattern: Each router imports its schemas, dependencies (auth/RBAC), and repository functions; never imports ORM models directly

**services/identity/repository/:**
- Purpose: SQLAlchemy ORM layer; data access abstraction
- Contains: Declarative ORM models (`models.py`), async repository functions (`org_repo.py`, `people_repo.py`)
- Key Pattern: All DB queries encapsulated in async functions; routers never write raw `select()` statements

**services/identity/security/:**
- Purpose: Authentication and authorization concerns
- Contains: JWT validation (`current_user.py`), RBAC role enforcement (`rbac.py`)
- Key Abstraction: `AuthenticatedUser` dataclass (frozen, immutable) returned by `get_current_user()`

**services/identity/domain/:**
- Purpose: Shared domain types (enums, Pydantic schemas)
- Contains: `Role` enum (4-role RBAC), `SlotStatus` enum, request/response schemas
- Usage: Routers use schemas for validation; repositories use enums; security layer uses Role

**libs/auth/:**
- Purpose: Authentication primitives reused across all services
- Contains: JWT encode/decode, password hashing, Redis denylist (token revocation)
- Used by: `services/identity/security/` (JWT validation), routers (login/register)

**libs/audit/:**
- Purpose: MongoDB audit logging for compliance and debugging
- Contains: `write_audit()` shared helper, index setup
- Usage: Every mutating endpoint calls `write_audit()` to record actor, action, outcome

**libs/observability/:**
- Purpose: Distributed tracing, logging, metrics infrastructure
- Contains: OpenTelemetry setup (tracer, OTLP exporter), structlog configuration
- Usage: App lifespan (configure on startup), routers (log), audit writer (trace ID injection)

**infra/:**
- Purpose: Infrastructure and deployment configuration
- Contains: Docker Compose (local dev stack), Alembic migrations, Prometheus config
- Key file: `docker-compose.yml` (Postgres, Mongo, Redis, Kafka, Temporal, Jaeger, Prometheus, Grafana)

**infra/migrations/:**
- Purpose: Alembic schema version control
- Contains: Migration scripts (numbered versions), Alembic runtime config
- Usage: `alembic upgrade head` applies all pending migrations to Postgres

**tests/:**
- Purpose: Pytest test suite (unit + integration)
- Contains: Shared fixtures (conftest.py), one test file per router/layer, integration test markers
- Key Pattern: Tests marked `@pytest.mark.integration` require live Postgres/Mongo/Redis containers; others run without

## Key File Locations

**Entry Points:**
- `smarthealth/main.py`: ASGI app (re-exports from `services/identity/app.py`)
- `services/identity/app.py`: App factory, router mounting, lifespan setup
- `infra/migrations/env.py`: Alembic runtime (runs on `alembic upgrade`/`downgrade`)

**Configuration:**
- `smarthealth/config.py`: Pydantic Settings (reads env vars, validates, defaults)
- `.env`: Local dev environment variables (git-ignored)
- `.env.example`: Template for `.env`
- `infra/docker-compose.yml`: Local dev container wiring
- `infra/prometheus.yml`: Prometheus scrape targets

**Core Logic:**
- `services/identity/api/`: HTTP routers (requests → responses)
- `services/identity/repository/`: ORM queries (models.py, org_repo.py, people_repo.py)
- `services/identity/domain/`: Shared schemas and enums
- `services/identity/security/`: JWT validation, RBAC enforcement

**Cross-Cutting:**
- `libs/auth/jwt.py`: JWT encode/decode
- `libs/audit/writer.py`: Audit logging helper
- `libs/observability/otel.py`: OpenTelemetry setup
- `libs/observability/logging.py`: Structlog configuration

**Testing:**
- `tests/conftest.py`: Shared fixtures (db_session, mongo_client, redis_client, client)
- `tests/test_auth.py`: Auth flow tests
- `tests/test_rbac.py`: RBAC enforcement
- `tests/test_models.py`: ORM model constraints
- `tests/test_audit.py`: Audit logging

## Naming Conventions

**Files:**
- Router files: Resource name + `.py` (e.g., `auth.py`, `clinics.py`, `patients.py`)
- Repository files: `*_repo.py` (e.g., `org_repo.py`, `people_repo.py`)
- Models: `models.py` (all ORM models in one file)
- Helpers: Descriptive name with purpose (e.g., `current_user.py`, `denylist.py`, `passwords.py`)
- Test files: `test_*.py` or `*_test.py` (pytest discovers these automatically)

**Directories:**
- Resource-based: `api/`, `repository/` (group by concern/layer)
- Feature-based: `auth/`, `audit/`, `observability/` (group by feature)
- Organizational: `services/`, `libs/`, `infra/` (top-level structure)

**Classes:**
- Pydantic models: PascalCase, suffixed with intent (e.g., `RegisterRequest`, `UserOut`, `ClinicCreate`, `ClinicUpdate`)
- SQLAlchemy models: PascalCase (e.g., `User`, `Clinic`, `Patient`, `Provider`)
- Enums: PascalCase (e.g., `Role`, `SlotStatus`)
- Dataclasses: PascalCase (e.g., `AuthenticatedUser`)

**Functions:**
- Async functions: lowercase_with_underscores (e.g., `get_current_user`, `create_patient`, `write_audit`)
- Router handlers: async, named for the action (e.g., `register`, `login`, `create_clinic`, `update_patient`)
- Repository functions: async, prefixed by action (e.g., `create_clinic`, `get_patient`, `list_patients`, `update_clinic`)

**Variables:**
- lowercase_with_underscores (e.g., `user_id`, `clinic_id`, `db_session`, `mongo_client`)

## Where to Add New Code

**New HTTP Endpoint (e.g., GET /api/patients/{patient_id}/appointments):**

1. **Router file**: Add a new route function to `services/identity/api/patients.py` or create a new router file (e.g., `appointments.py`) if the feature is significant
2. **Schema**: Add request/response Pydantic models to `services/identity/domain/schemas.py`
3. **Repository**: Add a data access function to `services/identity/repository/people_repo.py` (or create a new repo file for a new aggregate)
4. **Model**: Add ORM model to `services/identity/repository/models.py` if a new entity is needed
5. **Mount**: Include the router in `services/identity/app.py:create_app()` via `app.include_router(...)`
6. **Test**: Add tests to `tests/test_patients.py` (or new test file if a new router)

**New Shared Library (e.g., Email notifications):**

1. **Create** `libs/notifications/` directory with `__init__.py` and feature modules
2. **Implement**: Add functions (e.g., `send_email()`) and any required configuration in a module
3. **Export**: Add to `libs/notifications/__init__.py`
4. **Use**: Import and call from routers/services

**New Cross-Cutting Concern (e.g., Rate limiting middleware):**

1. **Create**: `libs/ratelimiting/` directory
2. **Implement**: Rate limiter logic, Redis key patterns
3. **Integrate**: Apply via FastAPI middleware or dependency in `services/identity/app.py`
4. **Test**: Add tests to `tests/test_ratelimiting.py`

**Database Migration (Schema change):**

1. **Auto-generate**: `alembic revision --autogenerate -m "Add user_preferences table"` (requires live Postgres)
2. **Review**: Check the generated file in `infra/migrations/versions/`
3. **Test**: Run `alembic upgrade head` against a test database
4. **Deploy**: Commit the migration; production deployment will run `alembic upgrade head`

**New Service (Phase 2, e.g., Appointments):**

1. **Create directory**: `services/appointments/`
2. **Structure**: Mirror identity service layout: `api/`, `repository/`, `domain/`, `security/`
3. **Create app factory**: `services/appointments/app.py:create_app()`
4. **Mount onto main app**: In `smarthealth/main.py` or have `services/identity/app.py` accept routers from other services
5. **Reuse shared libs**: Use `libs/auth/`, `libs/audit/`, `libs/observability/` without duplication

## Special Directories

**node_modules / .venv / .egg-info (Development artifacts — not committed):**
- Purpose: Virtual environment, installed dependencies, build artifacts
- Generated: Yes (by pip install, python -m build)
- Committed: No (excluded via .gitignore)

**.planning/ (GSD working directories):**
- Purpose: Orchestrator state, phase planning, phase history
- Generated: Yes (by GSD commands)
- Committed: Yes (tracks planning history)

**.pytest_cache / __pycache__ (Test/Python caches — not committed):**
- Purpose: pytest discovery/results caching, compiled Python bytecode
- Generated: Yes (by pytest, Python interpreter)
- Committed: No (excluded via .gitignore)

**infra/migrations/versions/ (Alembic history — committed):**
- Purpose: Versioned schema migrations
- Generated: Yes (by `alembic revision`)
- Committed: Yes (schema history is part of code)

---

*Structure analysis: 2026-09-30*
