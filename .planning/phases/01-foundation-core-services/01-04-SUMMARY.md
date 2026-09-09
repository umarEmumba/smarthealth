---
phase: 01-foundation-core-services
plan: 04
subsystem: org-management
tags: [fastapi, sqlalchemy, rbac, audit, pytest]

# Dependency graph
requires:
  - phase: 01-02
    provides: "Clinic/Department SQLAlchemy models (clinic_id FK chain), smarthealth.db Base/get_db"
  - phase: 01-03
    provides: "require_role(*roles) RBAC guard, write_audit shared helper, get_current_user, FastAPI app factory (services.identity.app)"
provides:
  - "services/identity/repository/org_repo.py — clinic-scoped async repository (create_clinic, get_clinic, list_clinics, create_department, list_departments); every department method requires clinic_id"
  - "services/identity/api/clinics.py — admin-only, audited clinic CRUD router (POST/GET /api/clinics, GET /api/clinics/{id})"
  - "services/identity/api/departments.py — admin-only, audited department CRUD router nested under a clinic (POST/GET /api/clinics/{clinic_id}/departments)"
affects: [01-05-patient-provider-availability-crud, 01-06-dashboard-reconciliation]

# Actuals (#2632)
actuals:
  tokens: 4670
  tasks: 2
  commits: 2
  plan_head_before: b2ccee7

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Clinic-scoped repository methods: every department read/write in org_repo.py requires clinic_id as an explicit parameter — no id-only department fetch path exists (RESEARCH Pitfall 1, closes T-04-01 cross-clinic enumeration)"
    - "Application-level existence check before insert: create_department verifies the parent clinic exists and returns None (router maps to 404) rather than letting an IntegrityError surface from the FK constraint — a clean rejection path, with the FK constraint as backstop (T-04-04)"

key-files:
  created:
    - services/identity/repository/org_repo.py
    - services/identity/api/clinics.py
    - services/identity/api/departments.py
    - tests/test_clinics.py
    - tests/test_departments.py
  modified:
    - services/identity/app.py

key-decisions:
  - "No uniqueness constraint on department name within a clinic — two departments with the same name under the same clinic are both allowed to exist with distinct ids (verified by test_duplicate_department_name_defined_outcome). This matches the Phase-1 schema in models.py, which declares no such constraint; the must_have's 'deterministic outcome' bar is satisfied by asserting the allowed-with-distinct-ids behavior explicitly rather than leaving it untested."
  - "Clinic/department Pydantic schemas (ClinicCreate/ClinicOut/DepartmentCreate/DepartmentOut) were defined inline in each router file rather than added to services/identity/domain/schemas.py — keeps the plan's stated files_modified list accurate and each router's request/response contract co-located with its own routes; schemas.py continues to hold only the auth-router payloads it was created for."

patterns-established:
  - "Pattern: a repository function that must check parent-entity existence before a scoped-child insert returns None (not an exception) on a missing parent, leaving the HTTP-status decision (404) to the router — the repository stays HTTP-status-agnostic while the router owns the API contract."

requirements-completed: [ORG-01, ORG-02, ORG-03]

coverage:
  - id: D1
    description: "An admin can create a clinic (POST /api/clinics) with a required timezone and retrieve it (GET /api/clinics/{id}, GET /api/clinics) reflecting the latest committed state"
    requirement: "ORG-01"
    verification:
      - kind: integration
        ref: "tests/test_clinics.py::test_create_clinic, ::test_read_after_write"
        status: pass
    human_judgment: false
  - id: D2
    description: "An admin can create a department within a clinic and list departments for a clinic"
    requirement: "ORG-02"
    verification:
      - kind: integration
        ref: "tests/test_departments.py::test_create_department, ::test_list_departments_empty"
        status: pass
    human_judgment: false
  - id: D3
    description: "A non-admin token (front-desk) creating a clinic returns 403"
    verification:
      - kind: integration
        ref: "tests/test_clinics.py::test_non_admin_forbidden"
        status: pass
    human_judgment: false
  - id: D4
    description: "Every clinic/department mutation writes a shared write_audit entry (no PHI)"
    verification:
      - kind: integration
        ref: "tests/test_clinics.py::test_create_clinic_audited"
        status: pass
    human_judgment: false
  - id: D5
    description: "A department cannot be created under a non-existent clinic — rejected 404, no orphan row"
    requirement: "ORG-02"
    verification:
      - kind: integration
        ref: "tests/test_departments.py::test_department_under_missing_clinic_404"
        status: pass
    human_judgment: false
  - id: D6
    description: "Departments are visibly scoped to the correct clinic via the FK chain (ORG-03)"
    requirement: "ORG-03"
    verification:
      - kind: integration
        ref: "tests/test_departments.py::test_department_scoped_to_clinic"
        status: pass
    human_judgment: false
  - id: D7
    description: "Two departments with the same name under the same clinic have a deterministic, asserted outcome (allowed, distinct ids)"
    verification:
      - kind: integration
        ref: "tests/test_departments.py::test_duplicate_department_name_defined_outcome"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-09-09
status: complete
---

# Phase 1 Plan 4: Clinic + Department CRUD Summary

**Admin-only, audited clinic and department CRUD built directly on the Plan-03 tracer's auth/RBAC/audit skeleton — clinic-scoped repository methods, read-after-write consistency, and referential integrity, all proven by 9 new integration tests against live Postgres/Mongo/Redis.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-09-09T17:07:00Z
- **Completed:** 2026-09-09T17:32:00Z
- **Tasks:** 2
- **Files modified:** 6 (5 created, 1 modified)

## Accomplishments

- `org_repo.py`'s department methods (`create_department`, `list_departments`) take `clinic_id` as a required parameter — no id-only department lookup path exists anywhere in the repository, closing the RESEARCH Pitfall 1 / T-04-01 cross-clinic enumeration hole structurally rather than by convention
- Both routers (`clinics.py`, `departments.py`) guard every write route with `Depends(require_role(Role.ADMIN))` and call the shared `write_audit` helper on every mutation (success and failure paths) — reused, not reinvented, from Plan 03
- `create_department` verifies the parent clinic exists before issuing an INSERT and returns `None` on a missing clinic, which the router maps to 404 — no orphan department row is ever written; the FK constraint from Plan 02 is the backstop, not the primary rejection path
- Read-after-write consistency proven directly: create a clinic, `GET` it back in the same test, assert the exact committed row (single Postgres system of record, no cache in front of it)
- Empty-list edge case proven: a clinic with zero departments returns `[]` with a 200, not an error
- The duplicate-department-name case (must_haves backstop item) has a defined, tested outcome: the Phase-1 schema declares no uniqueness constraint on department name, so two same-named departments under the same clinic are both created with distinct ids — this is now an asserted behavior, not an untested gap
- Full test suite (34 tests, all four phase plans' tests combined) passes with zero regressions after this plan's changes

## Task Commits

Each task was committed atomically:

1. **Task 1: Clinic + Department repository and routers (admin-only, audited)** - `4bed906` (feat)
2. **Task 2: Clinic + Department integration tests (ORG-01/02/03)** - `56380ce` (test)

**Plan metadata:** (this commit) — docs: complete plan

## Files Created/Modified

- `services/identity/repository/org_repo.py` - `create_clinic`, `get_clinic`, `list_clinics`, `create_department`, `list_departments` (all clinic-scope-aware)
- `services/identity/api/clinics.py` - `ClinicCreate`/`ClinicOut` schemas + admin-guarded, audited router (POST/GET `/api/clinics`, GET `/api/clinics/{id}`)
- `services/identity/api/departments.py` - `DepartmentCreate`/`DepartmentOut` schemas + admin-guarded, audited router nested under `/api/clinics/{clinic_id}/departments`
- `services/identity/app.py` (modified) - mounted `clinics_router` and `departments_router`
- `tests/test_clinics.py` - 4 integration tests (create, read-after-write, non-admin 403, audit presence)
- `tests/test_departments.py` - 5 integration tests (create, missing-clinic 404, empty-list, clinic-scoping, duplicate-name outcome)

## Decisions Made

- Defined `ClinicCreate`/`ClinicOut`/`DepartmentCreate`/`DepartmentOut` inline in their respective router files rather than adding them to `services/identity/domain/schemas.py` — keeps this plan's file footprint matching its stated `files_modified` list exactly, and keeps each router's request/response contract co-located with the routes that use it. `schemas.py` remains scoped to the auth router's payloads it was created for in Plan 03.
- `create_department` signals a missing parent clinic by returning `None` rather than raising an application exception — keeps the repository layer HTTP-status-agnostic; the router is the one place that decides `None` means 404.
- Confirmed no department-name uniqueness constraint exists in the Phase-1 schema (`services/identity/repository/models.py`) and wrote a test asserting the resulting behavior (both departments created with distinct ids) rather than treating the must_haves backstop item as satisfied by inspection alone.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's exact `<verify>` command (`r.path` on `app.routes`) does not work against the pinned FastAPI 0.141.1**
- **Found during:** Task 1, running the plan's stated automated verification command
- **Issue:** `for r in app.routes: r.path` raised `AttributeError: '_IncludedRouter' object has no attribute 'path'`. FastAPI 0.141.1 (the CLAUDE.md-pinned version) represents an `include_router()`-mounted router as an internal `_IncludedRouter` wrapper in `app.routes` rather than exposing flattened `APIRoute` objects with a `.path` attribute directly at that list's top level — a version-specific internal representation change, not a defect in this plan's routers.
- **Fix:** Verified the equivalent fact (both routers' paths are live and mounted) via `app.openapi()["paths"]`, which correctly listed `/api/clinics`, `/api/clinics/{clinic_id}`, and `/api/clinics/{clinic_id}/departments` alongside the pre-existing auth/admin routes. No application code changed — this was a verification-method substitution, confirmed equivalent to the plan's intent (prove the routers are wired into the app).
- **Files modified:** none
- **Verification:** `python -c "from services.identity.app import app; assert '/api/clinics' in app.openapi()['paths']"` passes; the plan's grep-based acceptance criteria (`require_role`/`write_audit` counts) independently confirm both routers carry their guards
- **Committed in:** N/A (no code change; verification-only)

---

**Total deviations:** 1 (verification-method substitution only, no code defect)
**Impact on plan:** None on functionality — both routers are correctly mounted and pass every other stated acceptance criterion and the full pytest suite. The deviation is scoped entirely to how the "app imports and routes are mounted" fact was confirmed, not to what was built.

## Issues Encountered

- OTel OTLP exporter logs `Transient error ... Connection refused` to `localhost:4317` during every test run — Jaeger is not running in this session's container set (only Postgres/Mongo/Redis were up, matching this plan's `<precondition>`, which does not require Jaeger). This is the same graceful-degradation behavior documented in the 01-03 SUMMARY; it does not affect or fail any test in this plan.

## User Setup Required

None — Postgres, MongoDB, and Redis containers were already running from the prior session; no new external service configuration was needed for this plan.

## Next Phase Readiness

Plan 05 (patient/provider/availability CRUD) can build on the same pattern this plan established: a clinic-scoped repository module + an admin-guarded, audited router pair, mounted in `services/identity/app.py`. `org_repo.get_clinic`/`list_clinics` are also directly reusable if Plan 05's provider routes need to validate a clinic/department reference.

No new blockers introduced. Existing Phase 1 blockers (Temporal SDK/server version compatibility, outbox relay mechanism decision, auto-setup vs. production-shaped Temporal) remain as previously recorded in STATE.md and are unaffected by this plan's scope.

---
*Phase: 01-foundation-core-services*
*Completed: 2026-09-09*

## Self-Check: PASSED

All 5 created files verified present on disk; both commits (4bed906, 56380ce) verified present in git log.
