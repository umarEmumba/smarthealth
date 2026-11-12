---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# Coding Conventions

**Analysis Date:** 2026-09-30

## Naming Patterns

**Files:**
- Module files: `snake_case.py` (e.g., `people_repo.py`, `jwt.py`, `logging.py`)
- Test files: `test_*.py` (e.g., `test_patients.py`, `test_auth.py`)
- Private helper modules: underscore prefix avoided for module names (functions/constants use it instead)

**Functions:**
- Public functions: `snake_case` (e.g., `create_patient`, `get_provider`, `decode_token`)
- Private functions: leading underscore `_snake_case` (e.g., `_assert_clinic_scope`, `_to_out`, `_reject_empty_body`)
- Async functions: no special prefix, just `async def snake_case(...)` (e.g., `async def create_patient(...)`)
- Test functions: `test_<what_is_tested>` with descriptive suffixes (e.g., `test_create_patient`, `test_forbidden_action_403`, `test_no_phi_in_patient_audit_or_logs`)

**Variables:**
- Public/module-level: `UPPER_SNAKE_CASE` for constants (e.g., `ACCESS_TOKEN_TYPE`, `AUDIT_COLLECTION_NAME`, `_PATIENT_PROFILE_FIELDS`)
- Local variables: `snake_case` (e.g., `patient_id`, `clinic_id`, `tokens`)
- Discriminator fields in unions: lowercase (e.g., `type="access"`, `outcome="success"`)

**Types:**
- Pydantic models: `PascalCase` (e.g., `PatientCreate`, `LoginResponse`, `TokenPair`)
- Enum classes: `PascalCase` (e.g., `Role`)
- TypeVars: `UPPER_CASE` if module-level
- Type aliases: `PascalCase` if semantic, `UPPER_CASE` if literal (e.g., `Outcome = Literal["success", "failure"]`)
- Exceptions: `PascalCase` ending in `Error` (e.g., `TokenError`)

## Code Style

**Formatting:**
- Python 3.12+ required (enforced via `pyproject.toml`)
- Line length: soft limit (no hard configuration found; observed code runs 80–100 chars comfortably)
- Indentation: 4 spaces
- Imports follow PEP 8: grouped into stdlib, third-party, local; sorted within groups
- Blank lines: one between module docstring and imports; two between top-level function/class definitions
- Future imports: `from __future__ import annotations` on line 1 of every module (enables postponed evaluation of type hints, required for circular reference safety)

**Linting:**
- No `.pylintrc`, `.flake8`, or `ruff.toml` found in repo root
- Enforcement method: discipline + code review (no automated linter presently enforcing rules)
- Observed style: clean, readable, and consistent with PEP 8

## Import Organization

**Order (enforced by discipline):**
1. Future imports: `from __future__ import annotations` (always first)
2. Standard library: `import uuid`, `from datetime import date`, etc.
3. Third-party: `from fastapi import APIRouter`, `from sqlalchemy import select`, etc.
4. Local (same project): `from libs.auth.jwt import decode_token`, `from services.identity.repository import people_repo`, etc.

**Path Aliases:**
- No `sys.path.insert()` or `PYTHONPATH` gymnastics observed
- Imports use absolute paths from repo root (e.g., `from libs.audit.writer import write_audit`)
- Setuptools `packages` in `pyproject.toml` declares the importable top-level modules: `["smarthealth", "libs", "services", "services.identity"]`

**Circular Imports:**
- Resolved via late imports (noqa comments where needed): e.g., `from services.identity.repository.org_repo import get_department  # noqa: PLC0415` inside a function body to break a circular dependency

**Barrel Files:**
- Used sparingly: `services/identity/__init__.py` and `libs/__init__.py` exist but are minimal
- API routers imported with alias for clarity: `from services.identity.api.auth import router as auth_router`

## Error Handling

**Patterns:**
- **Repository layer:** Returns `None` on not-found or validation failure (no exceptions raised for domain logic failures)
  - Example: `async def get_patient(...) -> Patient | None:` — returns `None` if patient does not exist
  - Caller maps `None` to appropriate HTTP status (404, 400, etc.)
- **API layer (FastAPI routers):** Raises `HTTPException` with explicit `status_code` for HTTP-translateable errors
  - Example: `raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="...")`
  - Import status codes from `fastapi.status` module (e.g., `status.HTTP_401_UNAUTHORIZED`)
- **Custom exceptions:** Defined for domain-specific errors (e.g., `TokenError` in `libs/auth/jwt.py`)
  - Raised in library functions; caught and mapped to `HTTPException` in API layer
  - Always include a descriptive message: `raise TokenError(f"expected token type {expected_type!r}, got {claims.get('type')!r}")`
- **Exception chaining:** Use `from exc` to preserve traceback context: `raise TokenError(...) from exc`

**No implicit silent failures:** If an operation fails, it either raises, returns `None`, or returns a typed Outcome (e.g., `Outcome = Literal["success", "failure"]`)

## Logging

**Framework:** `structlog` (version 26.1.0) configured for JSON output with OpenTelemetry trace correlation

**Patterns:**
- Initialize logger in module: `log = get_logger(__name__)`
- Structured event logging: `log.info("event_name", key1=value1, key2=value2)` (structured kwarg dict auto-converted to JSON)
- **PHI safety convention (enforced by discipline, not automation):** Never log PHI values (patient name, DOB, phone, address, full JWT token)
  - Safe to log: identifiers (`user_id`, `patient_id`, `clinic_id`, `jti`, `sub`, `trace_id`)
  - Comments in code remind developers: `# PHI note: never log these values directly`
- Trace correlation: `add_trace_context()` processor auto-injects `trace_id` and `span_id` from active OpenTelemetry span context into every log event
- Idempotent setup: `configure_logging()` uses a module-level `_LOGGING_CONFIGURED` flag to prevent re-configuration on test reruns

## Comments

**When to Comment:**
- **Design decisions:** Reference requirement IDs or research documents (e.g., `# PPL-01: no login required`, `# WR-02: clinic-scoped access check`)
- **Non-obvious implementation choices:** Explain *why*, not *what* (code shows what; comment explains why it's done that way)
  - Example: `# Re-select with selectinload() instead of lazy-loading post-commit — SQLAlchemy's async ORM raises MissingGreenlet if lazy-load is triggered outside the greenlet context`
- **Security/PHI boundaries:** Explicit warnings and explanations
  - Example: `# PHI-bearing request payload — never log/audit these field VALUES.`
- **Circular dependency resolution:** Note the noqa comment: `# noqa: PLC0415`
- **Gotchas and tradeoffs:** When a simpler approach was avoided for good reason
  - Example: `# Validation (...) happens in the router's Pydantic schema BEFORE this is called — this function trusts its caller and only persists`

**JSDoc/TSDoc:**
- Not used (Python project)
- Docstrings replace this role:
  - Module docstring: purpose, design context, PHI notes
  - Class docstring: what the class represents, business rules, constraints
  - Function/method docstring: what it does, parameter meanings, return value, edge cases, exceptions (if raised)
  - Docstring format: triple-quoted `"""..."""` on lines immediately following the `def`/`class` statement

## Function Design

**Size:** 
- Functions keep simple operations to 10–40 lines (observed average)
- Longer functions (50+ lines) break out sub-helpers (e.g., `_assert_clinic_scope` extracted from router handler)
- No hard rule enforced; readability drives decomposition

**Parameters:**
- Positional-only parameters: rare; not used in this codebase
- Positional-or-keyword: used for required parameters
- Keyword-only (after `*`): used for optional parameters, configuration, and any parameter whose meaning is unclear from position alone
  - Example: `async def create_patient(db: AsyncSession, *, clinic_id: uuid.UUID, ...)`
- Type hints required on all parameters (enforced by discipline)

**Return Values:**
- Explicit return type annotation on all functions (including `None` for no return)
- Union types use `|` operator (Python 3.10+ syntax, enabled by `from __future__ import annotations`)
  - Example: `-> Patient | None` (not `Optional[Patient]`)
- Tuple returns for multiple values: `tuple[str, str]`
  - Example: `def create_access_token(...) -> tuple[str, str]:` returns `(token, jti)`

## Module Design

**Exports:**
- Use `__all__` to declare public interface in modules with multiple exports
  - Example: `__all__ = ["app"]` in `smarthealth/main.py`
- Small modules (1–3 functions) may omit `__all__`

**Barrel Files:**
- Minimal use observed (prefer direct imports)
- Example: `from services.identity.repository import org_repo, people_repo` works because `services/identity/repository/__init__.py` exists (even if minimal)

**Single Responsibility:**
- Modules group by concern:
  - `libs/auth/jwt.py`: JWT issuance/validation only (not password hashing)
  - `libs/auth/passwords.py`: password hashing/verification only
  - `libs/audit/writer.py`: audit log writes only (shared across all routers)
  - `services/identity/api/*.py`: router definitions (one entity per file: `patients.py`, `providers.py`, etc.)
  - `services/identity/repository/`: data access layer (one entity per file: `people_repo.py`, `org_repo.py`)

## Async/Await Patterns

**Consistency:** All database queries, external calls, and I/O are `async` — no sync blocking calls inside `async def` endpoints
- Example: `async def create_patient(db: AsyncSession, ...) -> Patient:` (all SQLAlchemy calls use `await`)
- Never call sync drivers (e.g., psycopg2, sync Motor) inside `async def` without wrapping in `loop.run_in_executor()`

**Signatures:** Functions that are awaited are declared `async def`; callers invoke with `await`:

```python
async def create_patient(...): ...
patient = await create_patient(db, clinic_id=..., ...)
```

## Testing Conventions

**Coverage expectation:** No explicit coverage % requirement found in config, but observed test density is high (one test file per major entity/feature)

**Test file naming:** `tests/test_<entity>.py` (e.g., `tests/test_patients.py`)

**Helper function naming in tests:** Leading underscore `_helper_name` to distinguish from test functions

---

*Convention analysis: 2026-09-30*
