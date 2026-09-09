---
phase: 01-foundation-core-services
plan: 05
subsystem: patient-provider-availability
tags: [fastapi, sqlalchemy, rbac, audit, phi-safe, pytest]

# Dependency graph
requires:
  - phase: 01-02
    provides: "Patient/Provider/Specialty/provider_specialties/AvailabilityTemplate SQLAlchemy models, Clinic.timezone, empty provider_slots with live GiST exclusion constraint"
  - phase: 01-03
    provides: "require_role(*roles) RBAC guard, write_audit shared helper, get_current_user, FastAPI app factory (services.identity.app)"
  - phase: 01-04
    provides: "org_repo.py clinic/department repository pattern (id-only get_clinic, clinic-scoped list_departments), app.py mount point Plan 05 appends onto"
provides:
  - "services/identity/repository/people_repo.py — clinic-scoped patient CRUD, department-scoped provider CRUD (get-or-create specialty attachment via selectinload eager-loading), availability-template create/list"
  - "services/identity/api/patients.py — POST/GET /api/patients (FRONT_DESK/ADMIN), PHI-safe audited router"
  - "services/identity/api/providers.py — POST/GET /api/providers (ADMIN), specialties (>=1) attached at creation"
  - "services/identity/api/availability.py — POST/GET /api/providers/{provider_id}/availability (PROVIDER/ADMIN), Pydantic-validated weekly templates, no concrete slot generation"
  - "services/identity/repository/org_repo.py — added get_department (id-only lookup) to resolve a provider's department->clinic scope chain"
affects: [01-06-dashboard-reconciliation]

# Actuals (#2632)
actuals:
  tokens: 11965
  tasks: 3
  commits: 3
  plan_head_before: a557b62c8960016d12b7e9534ed877713f08cb8d

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Async ORM eager-loading via selectinload() in the SELECT itself, not by touching a lazy relationship attribute post-commit — accessing an unloaded relationship on an ORM object after control returns to the router raises MissingGreenlet under SQLAlchemy's async engine (the sync-to-async greenlet bridge is not active outside the query call). get_provider/list_providers/create_provider all eager-load Provider.specialties this way."
    - "Get-or-create specialty by name at provider-registration time (no separate specialty-management endpoint this plan) — keeps provider registration self-contained while still satisfying PPL-02's >=1-specialty requirement via a Pydantic min_length=1 constraint on the request schema."
    - "PHI-safe audit convention extended from Plan 03's identifier-only pattern: patient mutations pass changed_fields=[\"full_name\",\"date_of_birth\",\"phone\",\"address\"] (field NAMES) to write_audit — the PHI VALUES themselves never reach Mongo, logs, or spans anywhere in patients.py."

key-files:
  created:
    - services/identity/repository/people_repo.py
    - services/identity/api/patients.py
    - services/identity/api/providers.py
    - services/identity/api/availability.py
    - tests/test_patients.py
    - tests/test_providers.py
    - tests/test_availability.py
  modified:
    - services/identity/repository/org_repo.py
    - services/identity/app.py

key-decisions:
  - "Availability is templates-only in Phase 1 (RESEARCH Decision 2, confirmed in code): POST /api/providers/{provider_id}/availability persists AvailabilityTemplate rows only. Zero-count grep on 'provider_slots|ProviderSlot' in availability.py, and test_no_concrete_slots_generated directly queries provider_slots after template creation and asserts it stays empty. Phase 2's slot-generation job is the sole future consumer of these templates."
  - "Availability template times are naive TIME values that resolve against the owning provider's clinic's Clinic.timezone (RESEARCH Pitfall 3) — this plan does not perform that resolution (no concrete instant is computed here), it only documents the contract and validates internal window consistency. The clinic's timezone is reachable via provider.department.clinic.timezone for Phase 2's slot-generation job."
  - "users (auth identity) vs patients (operational profile) separation, confirmed at the API level: PatientCreate.user_id is optional and test_create_patient explicitly asserts a front-desk-created patient has user_id=None — no login is required for PPL-01 registration."
  - "Provider RBAC scope is path-param-driven (department_id in the request body, provider_id in the availability path), not JWT-clinic_id-driven — matching Plan 04's department-router pattern. An admin's token has no clinic_id claim requirement to operate across departments; scoping is enforced by requiring department_id/provider_id to resolve to a real row, not by matching against the token's clinic_id."
  - "Zero-specialties provider creation is rejected 422 by a Pydantic min_length=1 constraint on ProviderCreate.specialties (must_haves backstop item) — a deterministic, tested outcome rather than an unspecified one."

patterns-established:
  - "Pattern: an async ORM relationship that will be read back through a Pydantic response model must be eager-loaded (selectinload) in the same SELECT that fetches the parent row — never accessed as a bare attribute after the fetching call has returned, which is unsafe under SQLAlchemy's async engine (MissingGreenlet)."

requirements-completed: [PPL-01, PPL-02, PPL-03, PPL-04]

coverage:
  - id: D1
    description: "Front-desk or admin can register a patient (POST /api/patients) and manage the profile; a patient-role token cannot register a patient on someone else's behalf via this staff route (PPL-01)"
    requirement: "PPL-01"
    verification:
      - kind: integration
        ref: "tests/test_patients.py::test_create_patient, ::test_patient_role_forbidden"
        status: pass
    human_judgment: false
  - id: D2
    description: "Admin can register a provider (POST /api/providers) with one or more specialties (PPL-02)"
    requirement: "PPL-02"
    verification:
      - kind: integration
        ref: "tests/test_providers.py::test_create_provider_with_specialties, ::test_non_admin_forbidden"
        status: pass
    human_judgment: false
  - id: D3
    description: "Provider or admin can configure availability windows (POST /api/providers/{provider_id}/availability) creating availability_templates with day_of_week/start/end/slot_duration/effective dates (PPL-03)"
    requirement: "PPL-03"
    verification:
      - kind: integration
        ref: "tests/test_availability.py::test_create_availability_template, ::test_availability_forbidden_for_front_desk"
        status: pass
    human_judgment: false
  - id: D4
    description: "Registering a patient then immediately reading it back returns the just-committed record (PPL-04 read-after-write)"
    requirement: "PPL-04"
    verification:
      - kind: integration
        ref: "tests/test_patients.py::test_read_after_write_consistency"
        status: pass
    human_judgment: false
  - id: D5
    description: "A provider is created under a department, and its availability templates carry provider_id + department_id — providers/availability are clinic/department scoped (ORG-03 people half)"
    requirement: "ORG-03"
    verification:
      - kind: integration
        ref: "tests/test_providers.py::test_provider_scoped_to_department; tests/test_availability.py::test_create_availability_template (asserts body[\"department_id\"] == department_id)"
        status: pass
    human_judgment: false
  - id: D6
    description: "Every patient/provider/availability mutation writes a shared write_audit entry with NO PHI values in the audit changes field or logs (IAM-04 discipline extended to PHI-heavy patient data)"
    verification:
      - kind: integration
        ref: "tests/test_patients.py::test_no_phi_in_patient_audit_or_logs"
        status: pass
    human_judgment: false
  - id: D7
    description: "Creating a provider with zero specialties vs one vs many all have defined outcomes — empty specialties list is handled deterministically (rejected or allowed per a stated rule)"
    verification:
      - kind: integration
        ref: "tests/test_providers.py::test_provider_zero_specialties_defined_outcome (422, min_length=1)"
        status: pass
    human_judgment: false
  - id: D8
    description: "An availability template with end_time not after start_time, or day_of_week outside 0-6, is rejected (invalid-window edge)"
    verification:
      - kind: integration
        ref: "tests/test_availability.py::test_invalid_window_rejected"
        status: pass
    human_judgment: false
  - id: D9
    description: "A patient auth account (users, IAM-01) and a patient operational profile (patients, PPL-01) are distinct — a front-desk-created patient profile need not have a linked user_id"
    verification:
      - kind: integration
        ref: "tests/test_patients.py::test_create_patient (asserts body[\"user_id\"] is None)"
        status: pass
    human_judgment: false
  - id: D10
    description: "Creating a provider under a non-existent department id is rejected 404 with no orphan row"
    requirement: "ORG-03"
    verification:
      - kind: integration
        ref: "tests/test_providers.py::test_provider_under_missing_department_404"
        status: pass
    human_judgment: false
  - id: D11
    description: "After creating an availability template, provider_slots (the concrete-slot table) remains empty — RESEARCH Decision 2"
    verification:
      - kind: integration
        ref: "tests/test_availability.py::test_no_concrete_slots_generated"
        status: pass
    human_judgment: false

duration: 35min
completed: 2026-09-09
status: complete
---

# Phase 1 Plan 5: Patient + Provider (with Specialties) + Provider-Availability CRUD Summary

**The highest-PHI-density expansion of Phase 1 — patient management (front-desk/admin), provider registration with specialties (admin), and validated weekly availability templates (provider/admin) — all built on the proven auth/RBAC/audit/PHI-safe skeleton, with zero PHI values ever reaching audit records, logs, or spans, proven by 13 new integration tests (47/47 full suite, zero regressions).**

## Performance

- **Duration:** 35 min
- **Started:** 2026-09-09T17:34:00Z
- **Completed:** 2026-09-09T18:09:00Z
- **Tasks:** 3
- **Files modified:** 9 (7 created, 2 modified)

## Accomplishments

- `patients.py` guards POST/GET `/api/patients` to `FRONT_DESK`/`ADMIN` only; a patient-role token attempting to self-register a staff-managed profile via this route is rejected 403 (T-05-02), proven end-to-end
- Every patient mutation's `write_audit` call passes `changed_fields=["full_name","date_of_birth","phone","address"]` — field NAMES only. `test_no_phi_in_patient_audit_or_logs` asserts the synthetic PHI name, DOB, and phone strings never appear in the stored Mongo audit document or in a captured structlog JSON log line (T-05-01)
- `people_repo.py`'s patient methods require `clinic_id` explicitly (no id-only patient lookup path exists anywhere in the repository) — the same structural closure of cross-clinic enumeration Plan 04 established for departments (T-05-03)
- `providers.py` guards provider registration to `ADMIN` only (PPL-02); `ProviderCreate.specialties` carries a Pydantic `min_length=1` constraint, so the zero-specialties edge case has a deterministic, tested 422 outcome rather than an unspecified one
- Provider creation resolves specialties via get-or-create-by-name, attaching them through the `provider_specialties` association table established in Plan 02 — no new schema, reused as designed
- `availability.py`'s `AvailabilityTemplateCreate` Pydantic model rejects `day_of_week` outside 0-6, `end_time <= start_time`, non-positive `slot_duration_minutes`, and `effective_from > effective_to` — all with a clean 422, never a surfaced DB CHECK-constraint error
- Confirmed by a zero-count grep (`provider_slots|ProviderSlot`) against `availability.py` and by `test_no_concrete_slots_generated` (queries `provider_slots` directly after creating a template and asserts it is empty): this router NEVER writes to the concrete-slot table — RESEARCH Decision 2 held exactly as designed
- All three routers (`patients`, `providers`, `availability`) mounted in `services/identity/app.py` alongside Plan 04's `clinics`/`departments` routers — existing mounts preserved, confirmed via `app.openapi()["paths"]` listing all 13 routes
- Full test suite (47 tests across all five phase plans' test files) passes with zero regressions after this plan's changes

## Task Commits

Each task was committed atomically:

1. **Task 1: Patient + Provider repository and routers (specialties, RBAC, PHI-safe, audited)** - `b6336f7` (feat)
2. **Task 2: Availability-template router (weekly, timezone-aware, validated) + app mount** - `86bd031` (feat)
3. **Task 3: Patient/Provider/Availability integration tests (PPL-01/02/03/04)** - `fcc07b4` (test)

**Plan metadata:** (this commit) — docs: complete plan

## Files Created/Modified

- `services/identity/repository/people_repo.py` - `create_patient`/`get_patient`/`list_patients` (clinic-scoped), `create_provider`/`get_provider`/`list_providers` (department-scoped, specialties eager-loaded via `selectinload`), `get_or_create_specialty`, `create_availability_template`/`list_availability`
- `services/identity/api/patients.py` - `PatientCreate`/`PatientOut` schemas + `FRONT_DESK`/`ADMIN`-guarded, audited, PHI-safe router (POST/GET `/api/patients`, GET `/api/patients/{id}`)
- `services/identity/api/providers.py` - `ProviderCreate`/`ProviderOut`/`SpecialtyOut` schemas + `ADMIN`-guarded, audited router (POST/GET `/api/providers`, GET `/api/providers/{id}`)
- `services/identity/api/availability.py` - `AvailabilityTemplateCreate` (with `model_validator` window checks) / `AvailabilityTemplateOut` schemas + `PROVIDER`/`ADMIN`-guarded, audited router (POST/GET `/api/providers/{provider_id}/availability`)
- `services/identity/repository/org_repo.py` (modified) - added `get_department` (id-only lookup, mirrors `get_clinic`) so `people_repo.create_provider` can resolve a provider's department->clinic scope chain
- `services/identity/app.py` (modified) - mounted `patients_router`, `providers_router`, `availability_router` alongside Plan 04's clinic/department routers
- `tests/test_patients.py` - 4 integration tests (create, read-after-write, patient-role-forbidden, no-PHI-in-audit-or-logs)
- `tests/test_providers.py` - 5 integration tests (create-with-specialties, zero-specialties-422, department-scoping, missing-department-404, non-admin-403)
- `tests/test_availability.py` - 4 integration tests (create-valid-template, invalid-window-422, no-concrete-slots-generated, non-provider/admin-403)

## Decisions Made

- Fixed a Rule-1 bug (async ORM lazy-load outside the greenlet bridge) discovered on this plan's own first test run — see Deviations below; this became the "eager-load in the SELECT, never touch the bare relationship post-commit" pattern documented in `patterns-established` above, now the template every later async-ORM relationship access in this codebase should follow
- Kept `PatientCreate`/`ProviderCreate`/`AvailabilityTemplateCreate` schemas inline in their router files, matching Plan 04's established convention (schemas co-located with the routes that use them; `services/identity/domain/schemas.py` stays scoped to the auth router's payloads)
- Specialty attachment is get-or-create-by-name rather than requiring a pre-existing specialty id — there is no separate "manage specialties" admin endpoint in this plan's scope (PPL-02 only requires >=1 specialty on a provider, not a specialty-catalog CRUD surface)
- Provider/availability RBAC scope is path-param/body-driven (`department_id` in the create-provider body, `provider_id` in the availability path), not derived from the authenticated user's JWT `clinic_id` claim — mirrors Plan 04's department-router pattern exactly, so an admin's token needs no `clinic_id` claim to operate across departments/clinics

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Async ORM lazy-load of `Provider.specialties` outside the greenlet bridge raised `MissingGreenlet`**
- **Found during:** Task 3, first `pytest tests/test_providers.py` run (`test_provider_scoped_to_department`)
- **Issue:** `people_repo.get_provider`/`list_providers`/`create_provider` touched the bare `provider.specialties` attribute after the fetching/commit call had already returned, intending to force-load the relationship within the session. SQLAlchemy's async ORM bridges lazy-loads to its sync core via a greenlet context that is only active during an actual `await db.execute(...)`/`await db.refresh(...)` call — accessing an unloaded relationship attribute afterward (even on the same session, same request) raises `sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called`. This was latent in Task 1's code and only surfaced once a test exercised a route (`list_providers`, `get_provider`) that returns the relationship in its response body.
- **Fix:** Switched all three functions to eager-load `Provider.specialties` via `.options(selectinload(Provider.specialties))` directly in the `SELECT` statement — the correct async-ORM pattern (fetch related rows in a second batched query issued by SQLAlchemy itself, still inside the awaited `execute()` call) rather than a lazy attribute touch afterward. `create_provider` now re-fetches via `get_provider` after commit instead of `db.refresh(provider, attribute_names=["specialties"])`, since `refresh` on a many-to-many relationship has the same greenlet-timing risk.
- **Files modified:** services/identity/repository/people_repo.py
- **Verification:** `pytest tests/test_patients.py tests/test_providers.py tests/test_availability.py -x` — 13/13 pass; full suite `pytest tests/` — 47/47 pass, zero regressions
- **Committed in:** fcc07b4 (Task 3 commit, alongside the tests that surfaced it)

---

**Total deviations:** 1 auto-fixed (Rule 1 bug in this plan's own Task 1 code, found and fixed during Task 3's test-writing/first-run pass, before any commit of the buggy version had shipped standalone — Task 1's commit `b6336f7` predates this fix and technically carries the bug, but it was never exercised by a passing test until Task 3, and the fix landed in the very next commit of this same plan)
**Impact on plan:** Required for the plan's own stated acceptance criteria (`test_create_provider_with_specialties`, `test_provider_scoped_to_department`, `test_provider_under_missing_department_404` all read back `specialties` in their response-body assertions) to pass at all. No scope creep — same repository functions, same public contract (`Provider` objects with a populated `.specialties` list), just the internal loading strategy changed from lazy-touch to eager `selectinload`.

## Issues Encountered

- OTel OTLP exporter logs `Transient error ... Connection refused` to `localhost:4317` during every test run — Jaeger is not running in this session's container set (only Postgres/Mongo/Redis were up, matching this plan's `<precondition>`, which does not require Jaeger). Same graceful-degradation behavior documented in the 01-03/01-04 SUMMARYs; does not affect or fail any test in this plan.

## User Setup Required

None — Postgres, MongoDB, and Redis containers were already running (healthy) from the prior session; no new external service configuration was needed for this plan.

## Next Phase Readiness

Plan 06 (dashboard reconciliation) can now confirm the dashboard's claims against a Phase 1 that is functionally complete for its 14 requirements: IAM-01..04, ORG-01..03, PPL-01..04 (this plan), plus OBS-04/DASH-01/02 tracked separately. The full people-management surface — patients, providers with specialties, availability templates — is live, RBAC-scoped, PHI-safe, and audited, mounted on the same `services/identity/app.py` every prior plan's routers share.

The `availability_templates` API contract Phase 2's slot-generation job will read: `POST/GET /api/providers/{provider_id}/availability` returns `{id, provider_id, department_id, day_of_week (0-6), start_time, end_time, slot_duration_minutes, effective_from, effective_to}`. Times are naive and MUST be resolved against `provider.department.clinic.timezone` before being turned into concrete `tstzrange` values for `provider_slots` — this plan does not perform that resolution, it only persists the validated template and documents the contract (RESEARCH Pitfall 3).

No new blockers introduced. Existing Phase 1 blockers (Temporal SDK/server version compatibility, outbox relay mechanism decision, auto-setup vs. production-shaped Temporal deployment) remain as previously recorded in STATE.md and are unaffected by this plan's scope.

---
*Phase: 01-foundation-core-services*
*Completed: 2026-09-09*

## Self-Check: PASSED

All 7 created files (people_repo.py, patients.py, providers.py, availability.py, test_patients.py, test_providers.py, test_availability.py) verified present on disk; all 3 task commits (b6336f7, 86bd031, fcc07b4) plus the summary commit (64bba34) verified present in git log.
