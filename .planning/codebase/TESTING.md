---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# Testing Patterns

**Analysis Date:** 2026-09-30

## Test Framework

**Runner:**
- `pytest` (pinned in `pyproject.toml` under `[project.optional-dependencies] dev`)
- Config: `pyproject.toml` under `[tool.pytest.ini_options]`

**Async Support:**
- `pytest-asyncio` (auto mode enabled: `asyncio_mode = "auto"`)
- All test functions that touch async code are declared `async def` and use `await` naturally without any explicit loop management

**Assertion Library:**
- Built-in `assert` statements (pytest's default)
- `pytest.raises(ExceptionType)` context manager for exception testing

**Run Commands:**

```bash
pytest tests/ -x -k "not integration"              # Run unit tests only (exclude integration)
pytest tests/ -x                                    # Run all tests (requires Docker Compose stack)
pytest tests/ -x -v                                 # Verbose mode with test names
pytest tests/test_patients.py -x                   # Run single test file
pytest tests/ -x --tb=short                        # Short traceback format
```

## Test File Organization

**Location:**
- All tests in `tests/` directory (top-level)
- Co-located with main source (tests are not nested inside `src/` or `smarthealth/`)

**Naming:**
- Test files: `test_*.py` (e.g., `test_patients.py`, `test_auth.py`, `test_models.py`)
- Test functions: `test_<what_is_tested>` (e.g., `test_create_patient`, `test_forbidden_action_403`, `test_no_phi_in_patient_audit_or_logs`)

**Structure:**

```
tests/
├── conftest.py           # Shared fixtures (db_session, mongo_client, redis_client, client)
├── test_auth.py          # Auth/login/token tests
├── test_patients.py      # Patient CRUD tests
├── test_providers.py     # Provider CRUD tests
├── test_rbac.py          # RBAC enforcement tests
├── test_models.py        # ORM model constraints + database schema tests
├── test_audit.py         # Audit logging tests
├── test_clinics.py       # Clinic CRUD tests
├── test_departments.py   # Department CRUD tests
├── test_availability.py  # Availability template tests
└── test_passwords.py     # Password hashing tests
```

## Test Structure

**Suite Organization:**

Test functions are organized by feature/entity with helper functions defined at module level:

```python

# test_patients.py structure:

"""End-to-end patient CRUD tests — module docstring explains scope."""

from __future__ import annotations
import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient

# Test data / synthetic constants

SYNTHETIC_PHI_NAME = "Zzyzx Testpatient Synthetic"
SYNTHETIC_PHI_DOB = "1990-01-01"

# Helper functions for test setup

async def _register_and_login(client: AsyncClient, *, role: str) -> dict:
    """Helper: create user and return login tokens."""
    ...

def _auth_header(tokens: dict) -> dict:
    """Helper: format authorization header."""
    ...

async def _create_clinic(client: AsyncClient, tokens: dict, *, name: str) -> str:
    """Helper: create a clinic and return its ID."""
    ...

# Test functions

@pytest.mark.integration
async def test_create_patient(client: AsyncClient) -> None:
    """A front-desk can register a patient (PPL-01)."""
    ...

@pytest.mark.integration
async def test_read_after_write_consistency(client: AsyncClient) -> None:
    """Registering a patient then immediately reading it back..."""
    ...
```

**Key patterns:**
- Module docstring explains what is tested and why
- Synthetic test data constants defined at top (never real/prod data)
- Helper functions prefixed with `_` (private to the test module)
- Test functions use `@pytest.mark.integration` for tests requiring Docker containers
- Each test function has a comprehensive docstring explaining the scenario

## Test Structure — Detailed Example

```python
@pytest.mark.integration
async def test_front_desk_cannot_access_another_clinics_patients(
    client: AsyncClient
) -> None:
    """WR-02 regression: a FRONT_DESK token scoped to Clinic A must be
    rejected 403 when passing Clinic B's clinic_id to list/get/create —
    front-desk is scoped to their own clinic at the route layer, not just
    "any clinic the caller can guess a UUID for".
    """
    # Setup: create two clinics
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_a = await _create_clinic(client, admin_tokens, name="WR-02 Clinic A")
    clinic_b = await _create_clinic(client, admin_tokens, name="WR-02 Clinic B")

    # Setup: create front-desk user scoped to clinic A
    front_desk_a_tokens = await _register_and_login(
        client, role="front_desk", clinic_id=clinic_a
    )

    # Assertion: list should be rejected
    list_resp = await client.get(
        "/api/patients",
        params={"clinic_id": clinic_b},
        headers=_auth_header(front_desk_a_tokens),
    )
    assert list_resp.status_code == 403

    # Assertion: create should be rejected
    create_resp = await client.post(
        "/api/patients",
        json=_patient_payload(clinic_b),
        headers=_auth_header(front_desk_a_tokens),
    )
    assert create_resp.status_code == 403

    # Assertion: get should be rejected
    get_resp = await client.get(
        f"/api/patients/{uuid.uuid4()}",
        params={"clinic_id": clinic_b},
        headers=_auth_header(front_desk_a_tokens),
    )
    assert get_resp.status_code == 403

    # Assertion: admin is exempt from clinic-scoping
    admin_list_resp = await client.get(
        "/api/patients",
        params={"clinic_id": clinic_b},
        headers=_auth_header(admin_tokens),
    )
    assert admin_list_resp.status_code == 200
```

## Fixtures and Dependency Injection

**Location:** `tests/conftest.py` (pytest's auto-discovered fixture module)

**Core Fixtures:**

**`db_session` (AsyncIterator[AsyncSession]):**

```python
@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    """Async SQLAlchemy session, rolled back at teardown for test isolation.
    
    Requires a live Postgres container (integration-marked tests only).
    The outer transaction is never committed, so each test sees a clean
    slate regardless of what it writes.
    """
    # Creates a new engine, opens a transaction, yields a session
    # At teardown: closes session, rolls back transaction, closes connection
```

- Isolation: Transaction rollback ensures no test pollution
- Usage: `async def test_something(db_session: AsyncSession) -> None:`

**`mongo_client` (AsyncIterator[AsyncMongoClient]):**

```python
@pytest_asyncio.fixture
async def mongo_client() -> AsyncIterator[AsyncMongoClient]:
    """PyMongo native async client against a dedicated test database.
    
    Requires a live MongoDB container (integration-marked tests only).
    The test database is dropped at teardown so audit-trail tests never
    leak documents between runs.
    """
    # Connects to {mongo_db_name}_test (e.g., smarthealth_test)
    # At teardown: drops the test database
```

- Isolation: Test database dropped after each test
- Usage: Access audit collection: `collection = mongo_client[settings.mongo_db_name]["audit_log"]`

**`redis_client` (AsyncIterator[Redis]):**

```python
@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """redis.asyncio client against a dedicated test db index, flushed per test.
    
    Requires a live Redis container (integration-marked tests only).
    """
    # Connects to Redis, flushes the DB before the test
    # At teardown: flushes the DB again
```

- Isolation: Database flushed before and after test
- Usage: `await redis_client.set("key", "value")`

**`client` (AsyncIterator[AsyncClient]):**

```python
@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """httpx.AsyncClient wired to the real FastAPI app for integration tests.
    
    Runs the entire app startup (lifespan context manager, logging config,
    OTel tracing, Mongo audit indexes) without a real server.
    """
    # Creates an AsyncClient with ASGI transport (no network layer)
    # This allows testing the full request/response cycle
```

- No mock: Uses the real FastAPI app
- Transports: ASGI transport (in-process), not HTTP
- Usage: `resp = await client.post("/api/auth/login", json={...})`

**Helper Functions (conftest.py):**

**`unique_email(prefix: str) -> str`:**

```python
def unique_email(prefix: str = "user") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}@example.com"
```

- Generates `prefix-<random-uuid>@example.com` to avoid email collisions across test runs
- No cleanup: emails are never deleted (tests just keep generating new ones)

**`bootstrap_admin_and_login(client: AsyncClient) -> dict`:**

```python
async def bootstrap_admin_and_login(client: AsyncClient) -> dict:
    """Create a throwaway ADMIN account and return its login token pair.
    
    Used when a test needs an authenticated admin but doesn't need to test
    admin creation itself.
    """
    # Creates an admin user directly in DB (bypassing API)
    # Logs in and returns token pair
```

- Committed to separate short-lived engine (not the rolled-back test transaction)
- Hashes password using the real `hash_password()` function
- Returns: `{"access_token": "...", "refresh_token": "...", "token_type": "bearer"}`

## Mocking

**Framework:** Not explicitly used in this codebase

**Patterns observed:**
- **No mocks for repo layer:** Tests use real `db_session` fixture (Postgres transaction)
- **No mocks for external services:** Redis, Mongo, Postgres are real live containers
- **Integration-first testing:** Tests verify real end-to-end behavior, not isolated unit tests

**When to use mocks (not yet present but guidance from architecture):**
- Future external API calls (billing provider, notification service): mock via `httpx` library's mock capabilities or fixture patches
- Redis/Mongo failures (circuit-breaker testing): would mock via test-local fixture override

**What NOT to mock:**
- Database operations (test with real Postgres/Mongo)
- Auth/RBAC logic (test with real token generation)
- Audit logging (test with real Mongo audit collection)

## Fixtures and Factories

**Test Data:**

Module-level constants for synthetic data:

```python
SYNTHETIC_PHI_NAME = "Zzyzx Testpatient Synthetic"
SYNTHETIC_PHI_DOB = "1990-01-01"
SYNTHETIC_PHI_PHONE = "+1-555-010-9999"
```

Helper functions for payload construction:

```python
def _patient_payload(clinic_id: str) -> dict:
    return {
        "clinic_id": clinic_id,
        "full_name": SYNTHETIC_PHI_NAME,
        "date_of_birth": SYNTHETIC_PHI_DOB,
        "phone": SYNTHETIC_PHI_PHONE,
        "address": "123 Synthetic Ave",
    }
```

**Location:**
- Test-local helpers: module level in each `test_*.py` file
- Shared helpers: `tests/conftest.py`
- No separate `tests/factories.py` or `tests/fixtures/` directory

**Philosophy:**
- Synthetic data only (never real/prod patient names or details)
- Data generated on the fly, not static JSON files

## Coverage

**Requirements:** No explicit coverage % enforced in config

**Target observed:** High density (most code paths covered by integration tests)

**View Coverage:**

```bash

# Install coverage tool

pip install pytest-cov

# Run with coverage report

pytest tests/ --cov=smarthealth --cov=libs --cov=services --cov-report=html

# Open htmlcov/index.html in browser

```

**Coverage gaps identified:** None formal, but future phases should test:
- Temporal workflow error paths
- Celery task retry/backoff logic
- Kafka consumer error handling (dead-letter queue patterns)

## Test Types

**Unit Tests:**
- Scope: Single function/class in isolation
- Example: `test_availability_template_day_of_week_check_declared()` in `test_models.py` — pure unit test checking ORM constraint declaration (no live DB)
- Count: Minimal (most tests are integration)

**Integration Tests:**
- Scope: Multiple components working together (FastAPI app, Postgres, Mongo, Redis)
- Marked: `@pytest.mark.integration`
- Example: `test_create_patient(client)` — creates user, posts to `/api/patients`, queries audit log
- Count: Majority of test suite
- Run only with Docker Compose stack up: `docker compose -f infra/docker-compose.yml up -d`

**E2E Tests:**
- Definition: Same as integration tests in this project (full request/response cycle via `client` fixture)
- Framework: Not separate (no Selenium/Playwright; httpx ASGI transport serves as E2E test layer)

## Common Patterns

**Async Testing:**

```python
@pytest.mark.integration
async def test_something(client: AsyncClient) -> None:
    """Test that uses async fixtures and async operations."""
    # Fixtures are injected as normal (pytest-asyncio handles them)
    resp = await client.post("/api/endpoint", json={...})
    # Assertions on response
    assert resp.status_code == 201
```

**Parametrized Tests:**
- Not observed in existing tests (single-scenario tests preferred for clarity)
- If added: `@pytest.mark.parametrize("param", [value1, value2])`

**Error Testing:**

```python
@pytest.mark.integration
async def test_missing_token_401(client: AsyncClient) -> None:
    """No Authorization header at all returns 401, not 403."""
    resp = await client.get("/api/admin/ping")  # No Authorization header
    assert resp.status_code == 401

# Exception testing with pytest.raises

@pytest.mark.integration
async def test_exclusion_constraint_live(db_session: AsyncSession) -> None:
    """Overlapping slots raise IntegrityError."""
    # ... setup ...
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(overlapping_slot)
            await db_session.flush()
```

**Database Constraint Testing:**

```python
@pytest.mark.integration
async def test_audit_indexes_created(mongo_client: AsyncMongoClient) -> None:
    """ensure_audit_indexes creates the three query indexes idempotently."""
    collection = mongo_client[settings.mongo_db_name]["audit_log"]
    
    try:
        # First call creates the indexes
        names_first = await ensure_audit_indexes(mongo_client)
        assert set(names_first) == {
            "ix_audit_entity_type_entity_id_timestamp",
            "ix_audit_actor_id_timestamp",
            "ix_audit_clinic_id_timestamp",
        }
        
        # Second call must not raise (idempotent)
        names_second = await ensure_audit_indexes(mongo_client)
        assert set(names_second) == set(names_first)
    finally:
        await collection.drop()  # Cleanup after test
```

**Audit Trail Verification:**

```python
@pytest.mark.integration
async def test_no_phi_in_patient_audit_or_logs(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    """A patient mutation's audit `changes` field contains no PHI value."""
    # ... create patient ...
    collection = mongo_client[settings.mongo_db_name]["audit_log"]
    doc = await collection.find_one({"action": "patient.create", "entity_id": patient_id})
    
    assert doc is not None
    assert doc["outcome"] == "success"
    # PHI boundary: changes stores field NAMES only
    assert doc["changes"] == {
        "fields": ["full_name", "date_of_birth", "phone", "address"]
    }
    # Verify actual PHI values are NOT in the audit log
    assert SYNTHETIC_PHI_NAME not in str(doc)
```

**RBAC Testing:**

```python
@pytest.mark.integration
async def test_forbidden_action_403(client: AsyncClient) -> None:
    """A front-desk-role token calling the admin-only route is rejected 403."""
    tokens = await _register_and_login(client, role="front_desk")
    resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert resp.status_code == 403

@pytest.mark.integration
async def test_allowed_action_200(client: AsyncClient) -> None:
    """An admin-role token calling the admin-only route is allowed 200."""
    tokens = await _register_and_login(client, role="admin")
    resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
```

**Read-After-Write Consistency Testing:**

```python
@pytest.mark.integration
async def test_read_after_write_consistency(client: AsyncClient) -> None:
    """Registering a patient then immediately reading it back returns the
    just-committed record — PPL-04 read-after-write.
    """
    # Create patient
    create_resp = await client.post("/api/patients", json=_patient_payload(...))
    assert create_resp.status_code == 201
    patient_id = create_resp.json()["id"]
    
    # Read back immediately
    get_resp = await client.get(f"/api/patients/{patient_id}", params={...})
    assert get_resp.status_code == 200
    assert get_resp.json() == create_resp.json()  # Exact match
```

## Test Execution

**Local dev (no containers):**

```bash
pytest tests/ -x -k "not integration"
```

Runs only unit tests (those without `@pytest.mark.integration`)

**Full suite (with Docker Compose):**

```bash

# Terminal 1: start containers

docker compose -f infra/docker-compose.yml up -d
scripts/compose_healthcheck.sh  # Wait for stack ready

# Terminal 2: run tests

pytest tests/ -x
```

**CI/CD (assumed pattern):**
- Start Docker Compose stack in pipeline
- Run `pytest tests/ -x`
- If all pass, artifact test results and coverage report
- No automated flake8/pylint enforcement observed (discipline + code review)

---

*Testing analysis: 2026-09-30*
