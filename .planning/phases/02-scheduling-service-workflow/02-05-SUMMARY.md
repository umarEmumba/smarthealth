---
phase: 02-scheduling-service-workflow
plan: 05
subsystem: api
tags: [billing, fastapi, postgres, alembic, state-machine, idempotency, service-tokens]

# Dependency graph
requires:
  - phase: 02-01
    provides: "Service tokens (aud-scoped), require_internal_caller allowed_services, write_audit dedupe_key, harness service-client secrets"
  - phase: 02-04
    provides: "Gateway route table conventions and scheduling public routes"
provides:
  - "Billing service (services/billing) with its own billing_db, BillingBase and Alembic baseline billing_0001"
  - "POST /internal/billing/prechecks (scheduling): deterministic pending -> verified|failed in one transaction"
  - "POST /internal/billing/records/{appointment_id}/transitions (scheduling, visit): state-targeted, key-idempotent"
  - "POST /internal/billing/records/{appointment_id}/relink (scheduling): reschedule history row only"
  - "GET /api/billing/records?appointment_id= and /api/billing/records/{record_id} (front-desk/admin, clinic-scoped) via gateway"
  - "visit_db and billing_db provisioned by db-init; visit_service_url / billing_service_url in SharedSettings"
affects: [02-06, 02-08, 02-10, 02-11, 02-12]

actuals:
  tokens: 21895
  tasks: 3
  commits: 5

tech-stack:
  added: []
  patterns:
    - "State-targeted internal endpoint: conditional UPDATE ... WHERE status=:from + history row in a savepoint; NOOP/CONFLICT -> rollback"
    - "Idempotency by state AND by key: UNIQUE(record_id, idempotency_key) on append-only history"
    - "Overridable settings dependency (get_billing_settings) for config-driven behaviour in tests"

key-files:
  created:
    - services/billing/app.py
    - services/billing/config.py
    - services/billing/api/internal.py
    - services/billing/api/records.py
    - services/billing/domain/rules.py
    - services/billing/domain/states.py
    - services/billing/domain/schemas.py
    - services/billing/repository/models.py
    - services/billing/repository/billing_repo.py
    - services/billing/migrations/versions/billing_0001_billing_records.py
    - tests/billing/conftest.py
    - tests/billing/test_precheck.py
    - tests/billing/test_transitions.py
  modified:
    - infra/postgres-init/create-service-databases.sh
    - infra/docker-compose.yml
    - libs/config.py
    - services/gateway/routing.py
    - tests/conftest.py
    - tests/libs/test_service_isolation.py
    - tests/gateway/test_routing.py

key-decisions:
  - "Transition targets accept every billing state name; ALLOWED_TRANSITIONS (not the schema) refuses illegal moves with 409"
  - "Per-service least privilege on transitions: Visit may only target finalized; Scheduling may target anything except finalized; ADMIN user tokens any"
  - "Pre-check outcome history row stores Idempotency-Key + ':outcome' (UNIQUE(record_id, key) forbids two rows sharing a key); header capped at 190 chars"
  - "A repeated pre-check for an existing appointment with a DIFFERENT clinic_id is 409, not a replay"
  - "Relink is only legal on a verified record (409 otherwise)"
  - "GET /api/billing/records?appointment_id= returns a single record (appointment_id is UNIQUE), 404 when missing/out of scope"

patterns-established:
  - "Billing internal callers send Idempotency-Key on every mutating call (422 without it)"
  - "Refund settlement is simulated: the cancel activity requests refund_pending then refunded as two calls"

requirements-completed: [BILL-01, BILL-02]

coverage:
  - id: D1
    description: "billing_db provisioned by db-init; Billing migration round-trips (upgrade/downgrade/upgrade)"
    requirement: BILL-01
    verification:
      - kind: other
        ref: "docker compose up db-init && alembic -c services/billing/migrations/alembic.ini upgrade head / downgrade base / upgrade head"
        status: pass
    human_judgment: false
  - id: D2
    description: "Scheduling service token pre-check -> verified; front-desk reads record + two history rows via gateway"
    requirement: BILL-01
    verification:
      - kind: integration
        ref: "tests/billing/test_precheck.py#test_tracer_scheduling_precheck_verifies_and_front_desk_reads_it_via_gateway"
        status: pass
    human_judgment: false
  - id: D3
    description: "Deterministic rule set (DENY- fails, SELF-PAY/none self_pay, else insured), prefix override, no re-evaluation on replay, concurrent duplicates -> one record"
    requirement: BILL-01
    verification:
      - kind: integration
        ref: "tests/billing/test_precheck.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "Transition matrix (void, refund_pending->refunded, no_refund, finalized), same-target and same-key no-ops, 409 illegal moves, 404 unknown, relink history-only"
    requirement: BILL-02
    verification:
      - kind: integration
        ref: "tests/billing/test_transitions.py"
        status: pass
    human_judgment: false
  - id: D5
    description: "Billing covered by isolation invariants; /api/billing routed, /internal/billing/* 404 at gateway"
    verification:
      - kind: unit
        ref: "tests/libs/test_service_isolation.py tests/gateway"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 05: Billing Service Summary

**New Billing service on its own billing_db with a deterministic DENY-prefix pre-check (pending -> verified|failed in one transaction) and a state-targeted, key-idempotent void/refund/no_refund/finalize/relink state machine, readable by clinic staff via `/api/billing/records`**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-10-13T05:39:57Z
- **Completed:** 2026-10-13T06:00:00Z
- **Tasks:** 3
- **Files modified:** 30

## Accomplishments
- `services/billing/**` copied from the Patient skeleton: app (`smarthealth-billing` tracing, JWKS, metrics, audit indexes), config (`BILLING_DATABASE_URL`, `BILLING_PRECHECK_FAIL_PREFIX="DENY-"`), Dockerfile, async Alembic env and `billing_0001` (both tables, `ck_billing_records_status_valid`, `uq_billing_records_appointment_id`, `uq_billing_history_record_key`, `ix_billing_records_clinic_created`, full downgrade).
- `evaluate_precheck` is pure and never carries the raw reference; nothing named `coverage_ref` is stored, logged, audited or returned.
- Internal endpoints audit once per effective change after commit (`billing:{appointment_id}:precheck` / `billing:{appointment_id}:{key}`).
- db-init now provisions `visit_db` and `billing_db` (02-10 needs no db-init change); gateway routes `/api/billing`; harness mounts `billing`.

## Task Commits

1. **Task 1: Tracer — Billing service, pre-check, gateway read** - `038ede4` (feat)
2. **Task 2: Deterministic failure, transitions, relink, idempotency** - `324c904` (test, RED: 2 failing), `e38a2b4` (feat, GREEN)
3. **Task 3: Isolation invariants and gateway routing** - `ea434b1` (test)

**Plan metadata:** see the `docs(02-05): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter. The per-service transition allowlist (`SERVICE_TRANSITION_TARGETS`) is additional hardening under T-02-22; it matches what 02-06/02-08 (scheduling: voided, refund_pending, refunded, no_refund, relink) and 02-11 (visit: finalized) call.

## Deviations from Plan

### Auto-fixed Issues

**1. [Plan error] Both pre-check history rows cannot carry the identical Idempotency-Key**
- **Found during:** Task 1
- **Issue:** The plan says both history rows carry the key, but its own `uq_billing_history_record_key UNIQUE(record_id, idempotency_key)` forbids two rows of one record sharing a key.
- **Fix:** the `null -> pending` row carries the key as sent; the `pending -> verified|failed` row carries `<key>:outcome`. `Idempotency-Key` header limited to 1..190 chars so the suffix fits `varchar(200)`.
- **Files modified:** services/billing/repository/billing_repo.py, services/billing/api/internal.py
- **Committed in:** 038ede4

**2. [Plan ambiguity] `refunded -> verified` must be 409, so the target vocabulary covers all states**
- **Found during:** Task 2 (RED run: 422 from the Literal instead of 409)
- **Fix:** `TransitionRequest.target` is a Literal over all eight states; the state machine answers 409.
- **Committed in:** e38a2b4

**3. [Process] Most Task 2 endpoint code landed in Task 1**
- `internal.py` was written whole in Task 1 (transitions/relink included), so the Task 2 RED run showed only the 2 vocabulary failures; all other Task 2 tests passed on first run.

**4. [Test layout] Added `tests/billing/conftest.py`** (not in files list) for shared helpers/fixture `billing_world`.

**Total deviations:** 4 (1 plan error, 1 ambiguity, 2 process/layout)
**Impact on plan:** No scope creep; behaviour matches the plan's must-haves.

## Issues Encountered
- Between commits `038ede4` and `ea434b1` `tests/gateway/test_routing.py::test_route_table_maps_owners...` would fail (route row added in Task 1, test updated in Task 3 per plan file assignment). Green at HEAD.
- Semgrep: not run (not installed in executor environment).

## Verification
- Task 1 verify (db-init, alembic upgrade/downgrade/upgrade, tracer): pass.
- `.venv/bin/pytest tests/billing -x -q`: 38 passed.
- `.venv/bin/pytest tests/libs/test_service_isolation.py tests/gateway -x -q`: 85 passed.
- Full suite `-m "not e2e"`: 561 passed, 1 deselected (was 517).

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- 02-06 booking activity: POST `/internal/billing/prechecks` with body `{appointment_id, clinic_id, coverage_ref}` and header `Idempotency-Key` (<=190 chars); 201 new / 200 replay; `status == "failed"` -> BillingRejected. Void = transitions `{"target": "voided", "reason_code": "..."}`; 404 when no record -> treat as no-op.
- 02-08: cancel = `refund_pending` then `refunded` (two calls, distinct keys) or `no_refund`; relink has no body; relink on non-verified is 409.
- 02-11: visit token may only target `finalized`; 409 if not verified.
- Compose app/container for Billing still to come (02-12); no `BILLING_DATABASE_URL` in compose yet.

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
