---
phase: 01-foundation-core-services
fixed_at: 2026-09-09T18:42:50Z
review_path: .planning/phases/01-foundation-core-services/01-REVIEW.md
iteration: 2
findings_in_scope: 2
fixed: 2
skipped: 0
status: all_fixed
---

# Phase 01: Code Review Fix Report

**Fixed at:** 2026-09-09T18:42:50Z
**Source review:** .planning/phases/01-foundation-core-services/01-REVIEW.md
**Iteration:** 2

**Summary:**
- Findings in scope: 2
- Fixed: 2
- Skipped: 0

## Fixed Issues

### WR-07: No production bootstrap path exists to create the first ADMIN account after the CR-01 fix

**Files modified:** `scripts/bootstrap_admin.py` (new), `tests/test_bootstrap_admin.py` (new), `README.md`
**Commit:** e0d05a6
**Applied fix:** Added `scripts/bootstrap_admin.py`, a one-shot, non-HTTP CLI script that reads `BOOTSTRAP_ADMIN_EMAIL`/`BOOTSTRAP_ADMIN_PASSWORD` from the environment and creates the first `admin`-role `User` row only if no admin exists yet anywhere in the `users` table. Idempotent by design: the existence check uses `.scalars().first()` (not `.scalar_one_or_none()`) so it tolerates more than one pre-existing admin without raising, and any existing admin makes the run a clean no-op — it never creates a second admin and never overwrites an existing user's password or any other field. Also guards the target email against a non-admin collision (raises a clear error rather than silently violating the `users.email` unique constraint). Password hashing goes through the same `libs.auth.passwords.hash_password` bcrypt path the app's own `/api/auth/register` and `/api/auth/admin/users` routes use. A successful creation writes an `audit_log` entry (`action="user.bootstrap"`, `actor_id="system"`, `outcome="success"`) via the same `write_audit` helper every other mutation in this codebase uses.

Deliberately implemented as a CLI script rather than an HTTP route (one of the three options the review offered) specifically so it can never be reached by an unauthenticated (or any) network caller — this does not reopen the CR-01 privilege-escalation hole that removing the role field from `RegisterRequest` and gating `/api/auth/admin/users` behind `require_role(Role.ADMIN)` closed.

Documented the one-time bootstrap procedure in a new "First-Admin Bootstrap" section in `README.md`, per the review's explicit ask (README setup section is a milestone deliverable).

Added `tests/test_bootstrap_admin.py` with two integration tests:
- `test_bootstrap_admin_is_idempotent_and_never_duplicates` — calls `bootstrap_admin()` twice with a fresh never-before-seen email/password and asserts the admin count in the `users` table is identical before/after the second call (handles both the "DB already has an admin from other tests" and "DB has zero admins" starting states, since this integration suite shares a dev database across test files).
- `test_bootstrap_admin_created_user_can_log_in` — when the creation branch actually fires (no pre-existing admin), asserts the new account can successfully authenticate via `POST /api/auth/login`, proving the bcrypt hash round-trips through the app's own login path.

Verification: ran the script manually against the live dev Postgres/Mongo containers (`docker compose -f infra/docker-compose.yml ps` confirmed healthy) — confirmed the no-op branch logs `bootstrap_admin.already_exists` and exits 0 without creating a row, and confirmed the missing-env-var path exits 1 with a clear stderr message. Full test suite (`pytest tests/`, 62 tests, includes both new tests) passes with no regressions.

### IN-06: `list_availability`'s 404 (provider-not-found) path is not audited, unlike its sibling `create_availability_template`

**Files modified:** `services/identity/api/availability.py`
**Commit:** bd0007a
**Applied fix:** Added a `mongo: AsyncMongoClient = Depends(get_mongo)` dependency to `list_availability` and, in its 404 (provider-not-found) branch, added a `write_audit(..., action="availability.list", entity_type="availability_template", entity_id="unknown", outcome="failure")` call plus a `log.info("availability.list.provider_not_found", provider_id=...)` line, directly mirroring the existing pattern in `create_availability_template`'s 404 branch and `list_availability`'s own pre-existing 403 (ownership) branch immediately below it. This closes the audit-coverage inconsistency the re-review flagged, consistent with the codebase's documented principle (`auth.py` module docstring) that denied/failed attempts are audited, not only successes.

Verification: full test suite (`pytest tests/`, 62 tests) passes with no regressions — `tests/test_availability.py`'s existing 404/403 assertions on this route continued to pass unchanged, confirming the new dependency injection and audit call did not alter response behavior.

## Skipped Issues

None — both in-scope findings were fixed.

---

_Fixed: 2026-09-09T18:42:50Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 2_
