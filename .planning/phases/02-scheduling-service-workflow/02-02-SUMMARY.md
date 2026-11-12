---
phase: 02-scheduling-service-workflow
plan: 02
subsystem: database
tags: [postgres, sqlalchemy, alembic, asyncpg, partial-unique-index, exclusion-constraint, btree_gist, idempotency, concurrency, skip-locked]

requires:
  - phase: 01.1-microservice-re-architecture
    provides: scheduling_db, SchedulingBase, provider_slots with ex_provider_slots_no_overlap (scheduling_0001)
  - phase: 02-scheduling-service-workflow (02-01)
    provides: service tokens / internal client / audit dedupe (not used directly here; consumed by 02-06 activities)
provides:
  - "Alembic scheduling_0002: provider_slots clinic_id/department_id/held_until/held_by_appointment_id, ck_provider_slots_end_after_start, uq_provider_slots_provider_start, ix_provider_slots_held_until"
  - "appointments table with uq_appointments_active_slot, ex_appointments_patient_no_overlap, ck_appointments_status_valid, ix_appointments_pending_unstarted, ix_appointments_clinic_status"
  - "idempotency_keys (PK caller_user_id, key) with lazy 24 h expiry; append-only appointment_status_history"
  - "ORM models Appointment, IdempotencyKey, AppointmentStatusHistory; ProviderSlot hold columns"
  - "AppointmentStatus / FailureReason plain-str constant classes; SlotTaken, PatientOverlap, IdempotencyKeyReused, SlotNotBookable"
  - "appointment_repo: accept_booking, AcceptResult, is_slot_taken, is_patient_overlap, append_status_history, confirm_booking, mark_appointment_failed"
  - "idempotency_repo: claim_idempotency_key, get_idempotency_record"
  - "slot_repo: hold_slot, release_hold, expire_holds"
  - "test fixtures committed_scheduling_engine, seed_slot, cleanup_scheduling_rows"
affects: [02-03, 02-04, 02-05, 02-06, 02-07, scheduling, booking-saga, reaper, slot-generation]

actuals:
  tokens: 19566
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Accept transaction: idempotency claim first (INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at < now() RETURNING), appointment insert second, caller owns commit"
    - "Constraint-name -> business exception mapping in one helper (is_slot_taken / is_patient_overlap)"
    - "State steps are conditional UPDATE ... WHERE <expected state> RETURNING; repeat by owner = True, lost precondition = False with no change; history only on real transitions; never DELETE"
    - "Reaper claims rows with FOR UPDATE SKIP LOCKED"
    - "Concurrency proofs use committed sessions on a bounded 20-connection engine, rows scoped by random provider_id and cleaned up per test"

key-files:
  created:
    - services/scheduling/migrations/versions/scheduling_0002_appointments.py
    - services/scheduling/domain/errors.py
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/repository/idempotency_repo.py
    - services/scheduling/repository/slot_repo.py
    - tests/scheduling/test_concurrent_booking.py
    - tests/scheduling/test_appointment_constraints.py
    - tests/scheduling/test_idempotency.py
    - tests/scheduling/test_slot_state_steps.py
  modified:
    - services/scheduling/domain/enums.py
    - services/scheduling/repository/models.py
    - tests/scheduling/conftest.py
    - tests/scheduling/test_slot_constraint.py
    - tests/scheduling/test_health.py
    - tests/libs/test_service_isolation.py

key-decisions:
  - "appointments.id keeps a gen_random_uuid() server default as a fallback, but accept_booking always supplies the app-generated id the idempotency row references"
  - "idempotency_keys has no FK to appointments (key row is written before the appointment); key column bounded to varchar(255)"
  - "confirm_booking locks the appointment row (SELECT ... FOR UPDATE) before the conditional slot UPDATE, so a False result never leaves a half-applied confirm in the caller's transaction"
  - "held_by_appointment_id stays set when a slot is booked, so a retried hold/confirm by the same appointment is recognised as done"
  - "release_hold returns True when no appointment holds the slot (repeat or reaped) and False when another appointment, or this one after confirm, holds/booked it"
  - "Added ix_appointments_patient_id (patient lookups) beyond the plan's index list"

patterns-established:
  - "Repository functions flush, never commit; the API route or Temporal activity owns the transaction"
  - "IntegrityError from accept_booking aborts the transaction (no SAVEPOINT); caller must roll back, which also discards the key claim"

requirements-completed: [SCHED-03, SCHED-04, SCHED-08]

coverage:
  - id: D1
    description: "scheduling_0002 migration round-trips (upgrade, downgrade scheduling_0001, upgrade) and matches the ORM (alembic check: no new operations)"
    requirement: SCHED-03
    verification:
      - kind: other
        ref: "alembic -c services/scheduling/migrations/alembic.ini upgrade head && downgrade scheduling_0001 && upgrade head; alembic check"
        status: pass
    human_judgment: false
  - id: D2
    description: "50 parallel committed accept_booking calls on one slot -> exactly one active appointment, 49 SlotTaken, one idempotency row"
    requirement: SCHED-03
    verification:
      - kind: integration
        ref: "tests/scheduling/test_concurrent_booking.py#test_fifty_parallel_accepts_produce_exactly_one_active_appointment"
        status: pass
    human_judgment: false
  - id: D3
    description: "Same guarantee with application code bypassed: 50 raw INSERTs -> one row; raw-SQL constraint proofs for active slot, patient overlap, slot shape, provider/start key, status vocabulary, slot FK"
    requirement: SCHED-03
    verification:
      - kind: integration
        ref: "tests/scheduling/test_concurrent_booking.py#test_fifty_parallel_raw_inserts_leave_exactly_one_active_row"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_appointment_constraints.py (10 tests)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Idempotency: 40-way same key -> one appointment and identical replays; body mismatch -> IdempotencyKeyReused; per-caller scope; lazy expiry overwrite without delete"
    requirement: SCHED-04
    verification:
      - kind: integration
        ref: "tests/scheduling/test_idempotency.py (4 tests)"
        status: pass
    human_judgment: false
  - id: D5
    description: "Retry-safe hold / confirm / release / mark-failed / expire-holds steps with history on real transitions only and no DELETE"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/scheduling/test_slot_state_steps.py (11 tests)"
        status: pass
    human_judgment: false

duration: 13min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 02: Scheduling Data Layer Summary

**Postgres now enforces the booking rules. A partial unique index on active appointments, a patient-overlap EXCLUDE constraint and a `(caller, key)` idempotency primary key decide the races, and live tests show one winner out of 50 parallel transactions with or without application code. The slot and appointment state steps are retry-safe conditional UPDATEs.**

## Performance

- **Duration:** about 13 min
- **Started:** 2026-10-13T02:20:30Z (approx., after the 02-01 summary commit)
- **Completed:** 2026-10-13T02:40:00Z
- **Tasks:** 3
- **Files modified:** 15

## Accomplishments
- Migration `scheduling_0002` adds the following, with a working downgrade:
  - slot hold columns and the clinic/department ids
  - the `ck_provider_slots_end_after_start` CHECK and the `uq_provider_slots_provider_start` UNIQUE constraint
  - the `appointments`, `idempotency_keys` and `appointment_status_history` tables, with every planned constraint and index
- `accept_booking` writes the idempotency claim first and the appointment second, in one transaction. A same-body duplicate is replayed and a different-body duplicate is rejected. Constraint violations become `SlotTaken` or `PatientOverlap`.
- Live-Postgres proofs:
  - 1 of 50 accept transactions wins (through `accept_booking`, and again with raw INSERTs).
  - 40 requests with the same key create one appointment.
  - A key whose `expires_at` has passed is overwritten by the next claim and never deleted.
  - Raw-SQL tests check every named constraint.
- `hold_slot`, `release_hold`, `confirm_booking`, `mark_appointment_failed` and `expire_holds` (which uses SKIP LOCKED) are safe to run twice. They append history only when a row actually changes, and a test fails if any of these repo files contains a DELETE.

## Task Commits

1. **Task 1: Tracer: migration, models, accept_booking, 1-of-50 proof** - `c5cbf83` (feat)
2. **Task 2: Constraint and idempotency proof suites** - `665375c` (test)
3. **Task 3: Retry-safe slot and appointment state steps** - `be01696` (feat; RED was confirmed first: the test module failed to import `confirm_booking`)

**Plan metadata:** see the `docs(02-02): complete plan summary` commit

Tracer gate: the Task 1 `<verify>` was re-run in full (upgrade, then downgrade, then upgrade, then pytest). It exited 0 and expansion went ahead.

## Files Created/Modified
- `services/scheduling/migrations/versions/scheduling_0002_appointments.py`: schema, constraints and full downgrade.
- `services/scheduling/repository/models.py`: the ProviderSlot hold columns and constraints, plus the Appointment, IdempotencyKey and AppointmentStatusHistory models.
- `services/scheduling/domain/enums.py`: `AppointmentStatus` and `FailureReason` as plain classes of `str` constants.
- `services/scheduling/domain/errors.py`: the business-outcome exceptions.
- `services/scheduling/repository/idempotency_repo.py`: key claim and lookup.
- `services/scheduling/repository/appointment_repo.py`: the accept step, constraint helpers, confirm, mark-failed and history.
- `services/scheduling/repository/slot_repo.py`: hold, release and the reaper.
- `tests/scheduling/conftest.py`: the committed-engine fixture, `seed_slot` and `cleanup_scheduling_rows`.
- `tests/scheduling/test_concurrent_booking.py`, `test_appointment_constraints.py`, `test_idempotency.py`, `test_slot_state_steps.py`: the proof suites.
- `tests/scheduling/test_slot_constraint.py`: slot inserts now supply `clinic_id` and `department_id`.
- `tests/libs/test_service_isolation.py`: `OWNED_TABLES["scheduling"]` lists the four tables.
- `tests/scheduling/test_health.py`: the table-ownership assertion is updated (see Deviations).

## Decisions Made
See `key-decisions` in the frontmatter. The main ones:
- `confirm_booking` locks the appointment row first, so the function never leaves the slot `booked` while the appointment is still not confirmed.
- `held_by_appointment_id` stays set on booked slots, so a retried hold or confirm by the same appointment is recognised as done.
- `idempotency_keys` has no foreign key to `appointments`, because the key row is written before the appointment row.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug, plan omission] Updated stale table-ownership assertion in tests/scheduling/test_health.py**
- **Found during:** Task 1
- **Issue:** `test_scheduling_owns_only_provider_slots_without_a_foreign_key` asserted that `SchedulingBase.metadata.tables == {"provider_slots"}`. The plan adds three tables, so this test would fail. The file was not in the plan's `files_modified`.
- **Fix:** The test is renamed `test_scheduling_owns_its_tables_without_a_cross_service_foreign_key`. It now expects the four tables and still checks that `provider_id` has no foreign key. It also checks that every foreign key points at a Scheduling table. The test is stricter than before, not weaker.
- **Files modified:** tests/scheduling/test_health.py
- **Verification:** `pytest tests/scheduling/test_health.py` passed 4 tests.
- **Committed in:** c5cbf83

**2. [Rule 2 - Correctness] confirm_booking locks the appointment before updating the slot**
- **Found during:** Task 3
- **Issue:** The plan's order was "update the slot, then the appointment". If the appointment was no longer `pending`, the slot would already be `booked` when the function returned False. A caller that commits after a False would then leave the slot booked for an appointment that was never confirmed.
- **Fix:** The function first runs `SELECT status ... FOR UPDATE` on the appointment, and returns early if it is already `confirmed` (idempotent) or in any non-`pending` state. Only then does it run the conditional slot UPDATE and the conditional appointment UPDATE. Every behaviour the plan specifies is unchanged.
- **Files modified:** services/scheduling/repository/appointment_repo.py
- **Verification:** tests/scheduling/test_slot_state_steps.py, confirm and lost-hold cases
- **Committed in:** be01696

**3. [Minor additions]**
- `ix_appointments_patient_id` index, a `FailureReason` constants class, and an optional `actor` keyword on `confirm_booking` (default `system:workflow`).
- Extra tests: per-caller key scope, a slot foreign-key violation, and a no-DELETE static gate.

---

**Total deviations:** 2 auto-fixed (1 stale test, 1 correctness), plus the minor additions above.
**Impact on plan:** None of these changes scope. Every planned symbol keeps the name and signature the plan gave it.

## Issues Encountered
- Semgrep: not run (not installed in the executor environment).
- A global `ruff` is installed, but the project has no ruff configuration. With ruff's default rules it reports UP017/I001/RUF100 findings on files that existed before this plan as well as new ones. No lint gate exists, so these were not changed; the code follows the repo's existing conventions (`timezone.utc`, `noqa: PLC0415`).

## User Setup Required
None. Run `/home/claude/devtools/migrate.sh` (or the scheduling `alembic upgrade head`) to apply `scheduling_0002`. The migration adds NOT NULL columns with no default, so it fails if `provider_slots` already has rows. That is intentional: the dev table is empty.

## Next Phase Readiness
- **Slot generation (02-03 and later):** `provider_slots` inserts must now supply `clinic_id` and `department_id`. Use `uq_provider_slots_provider_start` as the idempotency key, e.g. `ON CONFLICT (provider_id, start_time) DO NOTHING`.
- **API (POST /appointments):**
  - Call `accept_booking` inside `async with session.begin()`.
  - Map `SlotTaken` and `PatientOverlap` to 409 and `IdempotencyKeyReused` to 422. A replay returns the same 202 body.
  - Generate the appointment UUID in the app.
- **02-06 activities:**
  - Wrap `hold_slot`, `confirm_booking`, `release_hold`, `mark_appointment_failed` and `expire_holds`. Each activity owns its commit.
  - A False result is a business outcome, not a retryable failure.
  - Statuses are plain strings (`AppointmentStatus.*`), and failure reasons come from `FailureReason`.
- **Concurrency tests:** use `committed_scheduling_engine`, `seed_slot` and `cleanup_scheduling_rows` from `tests/scheduling/conftest.py`.
- **Full suite:** 446 passed, 1 deselected (e2e), against a baseline of 419 after 02-01.

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
