---
phase: 01-foundation-core-services
fixed_at: 2026-09-09T19:00:00Z
review_path: .planning/phases/01-foundation-core-services/01-REVIEW.md
iteration: 1
findings_in_scope: 9
fixed: 9
skipped: 0
status: all_fixed
---

# Phase 01: Code Review Fix Report

**Fixed at:** 2026-09-09T19:00:00Z
**Source review:** .planning/phases/01-foundation-core-services/01-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope (Critical + Warning): 9
- Fixed: 9
- Skipped: 0

Note: CR-04 was retracted by the reviewer within REVIEW.md itself ("Re-classifying: no crash here... Fix: N/A — see WR-06 instead") and is not counted in the in-scope total — it documents a trace that concluded no code change was needed, with the real issue captured as WR-03 (timing side-channel). `fix_scope: critical_warning` therefore covers CR-01, CR-02, CR-03, WR-01 through WR-06 = 9 actionable findings.

All 60 tests pass (54 pre-existing + 6 new/adjusted regression tests net), and `alembic current`/`alembic history` were verified to run cleanly against the live Postgres container after the WR-06 fix (which required a companion fix to `infra/migrations/env.py` to avoid breaking every Alembic CLI invocation — see below).

## Fixed Issues

### CR-01: Unauthenticated caller can self-register as `admin` (privilege escalation)

**Files modified:** `services/identity/api/auth.py`, `services/identity/domain/schemas.py`, `tests/conftest.py`, `tests/test_auth.py`, `tests/test_rbac.py`, `tests/test_providers.py`, `tests/test_clinics.py`, `tests/test_departments.py`, `tests/test_patients.py`, `tests/test_availability.py`, `tests/test_audit.py`
**Commit:** `997d7de`
**Applied fix:** `RegisterRequest` no longer has a `role` field — public `/api/auth/register` always creates a `Role.PATIENT` account. Added `StaffRegisterRequest` and a new `POST /api/auth/admin/users` route (`require_role(Role.ADMIN)`) as the only path to create elevated-role accounts. Updated every test file's `_register_and_login`/`_register` helper (all 8 test files that previously abused `/register`'s caller-supplied `role` to mint arbitrary-role test accounts) to instead bootstrap a throwaway admin via a direct, committed DB insert (`tests/conftest.py::bootstrap_admin_and_login`) and use the new admin-only route. Added regression tests proving (1) a self-registered "admin" is rejected by admin-only routes and forced to `role="patient"`, and (2) the staff-creation route itself rejects non-admin callers.

### CR-02: Broken object-level authorization on provider availability templates (IDOR)

**Files modified:** `services/identity/api/availability.py`, `tests/test_availability.py`
**Commit:** `29b6f36`
**Applied fix:** Added an ownership check on top of the existing `require_role(Role.PROVIDER, Role.ADMIN)` guard in both `create_availability_template` and `list_availability`: when `user.role == Role.PROVIDER`, `provider.user_id` must equal `user_id_as_uuid(user)`, else 403. `Role.ADMIN` is exempt and may act on any `provider_id`. Added regression tests: a provider whose `Provider.user_id` matches the caller can manage their own availability; a different provider is rejected 403 on both create and list.

### CR-03: Insecure hardcoded JWT secret default has no fail-closed production guard

**Files modified:** `smarthealth/config.py`, `.env.example`, `tests/test_smoke.py`
**Commit:** `db6bd6e`
**Applied fix:** Added an `environment: str = "development"` setting and a `model_post_init` guard that raises `RuntimeError` at `Settings` construction time when `environment != "development"` and `jwt_secret` is still the well-known source-visible default — refusing to start rather than booting insecurely. Documented `ENVIRONMENT` in `.env.example`. Added unit tests: default construction (development) boots fine; non-development + default secret raises; non-development + a real secret boots fine.

### CR-04: Login failure branch — retracted by reviewer, no fix needed

**Files modified:** none
**Commit:** n/a
**Applied fix:** N/A. The reviewer's own re-trace within REVIEW.md concluded this was not a bug ("on closer trace this is NOT a bug as first suspected... Retracted") and pointed to the real issue as the timing side-channel documented as WR-03, which is fixed below. No code change applied for CR-04 itself.

### WR-01: `list_patients` does not verify the clinic exists

**Files modified:** `services/identity/api/patients.py`, `tests/test_patients.py`
**Commit:** `9af1413`
**Applied fix:** Added the same `org_repo.get_clinic` existence check `list_departments` already used, returning 404 for a non-existent `clinic_id` instead of silently returning `[]`. Added a regression test asserting 404 on a fabricated clinic UUID.

### WR-02: `get_patient`/`list_patients`/`create_patient` allow cross-clinic access for non-admin roles

**Files modified:** `services/identity/api/patients.py`, `tests/test_patients.py`
**Commit:** `9af1413` (same commit as WR-01 — both are the patients-router clinic-scoping fix mentioned together in the task instructions)
**Applied fix:** Added `_assert_clinic_scope(user, clinic_id)` — non-`ADMIN` callers must have `user.clinic_id == clinic_id`, else 403. Applied to `create_patient`, `list_patients`, and `get_patient`. Updated the existing test helper to link front-desk test accounts to their clinic via `StaffRegisterRequest.clinic_id` (previously test front-desk accounts had no `clinic_id`, which the new check would have rejected outright — fixed the test setup accordingly). Added a regression test proving a front-desk token scoped to Clinic A is rejected 403 against Clinic B on create/list/get, while an admin token remains unaffected.

### WR-03: Login timing side-channel (user-enumeration via bcrypt short-circuit)

**Files modified:** `libs/auth/passwords.py`, `services/identity/api/auth.py`, `tests/test_passwords.py`
**Commit:** `3ac0dd7`
**Applied fix:** Added a precomputed `DUMMY_PASSWORD_HASH` in `libs/auth/passwords.py`. `login()` now always calls `verify_password` — against the real user's hash when the user exists, or the dummy hash when it doesn't — so the same bcrypt cost is paid on every login attempt regardless of email existence. Added unit tests for the dummy hash (valid bcrypt hash, never accidentally verifies a real password).

### WR-04: `jti` discarded, losing audit/log correlation for login/refresh

**Files modified:** `services/identity/api/auth.py`, `tests/test_auth.py`
**Commit:** `c8f70e7`
**Applied fix:** Threaded `access_jti`/`refresh_jti` into the `auth.login.success` and `auth.refresh.success` structured-log events (the Mongo audit-log document schema is an explicit locked contract without a `jti` field, so correlation lives in the log line per the review's own suggested fix). Added an integration test that captures stdout via `capsys` and asserts both jtis are present in the JSON log lines for login and refresh.

### WR-05: Redundant conditional statement rebuild in `list_providers`

**Files modified:** `services/identity/repository/people_repo.py`
**Commit:** `abf25bd`
**Applied fix:** Build the base `select(Provider)...` statement once and conditionally apply `.where(Provider.department_id == department_id)` on top of it, instead of discarding and rebuilding the entire query chain in the `department_id is not None` branch. Behavior-preserving refactor; existing `test_providers.py` coverage already exercises both branches.

### WR-06: `asyncio.get_event_loop()` deprecated/unsafe usage

**Files modified:** `smarthealth/db.py`, `infra/migrations/env.py`
**Commit:** `b4404b0`
**Applied fix:** Switched `get_engine()` and `get_mongo_client()` to `asyncio.get_running_loop()` as the review suggested. This surfaced a real regression during verification: `infra/migrations/env.py` imported `smarthealth.db`'s lazy `engine` attribute at plain module-import time (before Alembic's `asyncio.run(...)` starts a loop) — `get_running_loop()` correctly raises `RuntimeError` there instead of the old deprecated fallback silently spinning up a throwaway loop, which would have completely broken every `alembic` CLI invocation (`alembic current`, `alembic upgrade`, etc.). Fixed `env.py` to build its own dedicated `create_async_engine(settings.database_url)` directly instead of reusing the app's request-scoped lazy singleton, and to source the offline-mode URL from `settings.database_url` rather than `engine.url`. Verified `alembic current` and `alembic history` run cleanly against the live Postgres container after the fix.

## Skipped Issues

None — all 9 in-scope findings were fixed.

---

_Fixed: 2026-09-09T19:00:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
