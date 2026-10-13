---
phase: 02-scheduling-service-workflow
plan: 09
subsystem: workflow
tags: [temporal, waitlist, child-workflow, skip-locked, workflow-patched, replay, rbac]

# Dependency graph
requires:
  - phase: 02-02
    provides: "accept_booking (one-transaction accept), uq_appointments_active_slot, ex_appointments_patient_no_overlap, committed_scheduling_engine/seed_slot/cleanup_scheduling_rows"
  - phase: 02-04
    provides: "references.get_provider_ref/get_department_ref/get_clinic_ref/get_patient_ref/get_patient_by_user, seed_bookable_world, gateway route /api/waitlist"
  - phase: 02-06
    provides: "BookingWorkflow saga, contracts rules, temporal_policies presets, shared Temporal fixtures"
  - phase: 02-07
    provides: "worker.WORKFLOWS, pending-booking starter (workflow_started_at), replay gate + recorder pattern"
  - phase: 02-08
    provides: "CancelWorkflow/RescheduleWorkflow, cancel_refund_v1.json/reschedule_v1.json, cancel/reschedule test helpers"
provides:
  - "scheduling_0003: waitlist_entries (uq_waitlist_active_patient_provider, ix_waitlist_fifo, ix_waitlist_clinic_status, status/window CHECKs, full downgrade)"
  - "WaitlistEntry model, WaitlistStatus constants, waitlist_repo (create/get/list/cancel, find_matched_entry, claim_next_entry FOR UPDATE SKIP LOCKED, fail_waiting_entry, resolve_entry)"
  - "accept_booking(..., workflow_started=True) sets workflow_started_at in the accept transaction"
  - "WaitlistAdvanceWorkflow (capped claim -> child BookingWorkflow -> resolve) and WaitlistActivities(session_factory, mongo)"
  - "CancelWorkflow / RescheduleWorkflow start the advance as an ABANDONed child behind workflow.patched(WAITLIST_PATCH_ID)"
  - "POST/GET /api/waitlist, GET /api/waitlist/{id}, POST /api/waitlist/{id}/cancel"
  - "Histories cancel_waitlist_v2.json, reschedule_waitlist_v2.json, waitlist_advance_booked_v1.json"
affects: [02-10, 02-11, 02-12]

actuals:
  tokens: 90000
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Work-queue claim: SELECT ... ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1 + `matched` claim state, committed together with the appointment insert"
    - "Parent workflow runs the normal saga as a child (execute_child_workflow by type name, ParentClosePolicy.ABANDON) instead of duplicating saga steps"
    - "Deterministic per-iteration ids from workflow.uuid4(); a retried claim finds its own match (matched_appointment_id) instead of inserting twice"
    - "Command-sequence change to recorded workflows behind workflow.patched(<id>), proven by replaying pre-patch histories with ALL workflows registered and by v2 histories that contain the marker"

key-files:
  created:
    - services/scheduling/migrations/versions/scheduling_0003_waitlist.py
    - services/scheduling/repository/waitlist_repo.py
    - services/scheduling/activities/waitlist.py
    - services/scheduling/workflows/waitlist_advance.py
    - services/scheduling/api/waitlist.py
    - tests/scheduling/test_waitlist.py
    - tests/scheduling/test_waitlist_concurrency.py
    - tests/workflows/test_waitlist_workflow.py
    - tests/workflows/histories/cancel_waitlist_v2.json
    - tests/workflows/histories/reschedule_waitlist_v2.json
    - tests/workflows/histories/waitlist_advance_booked_v1.json
  modified:
    - services/scheduling/repository/models.py
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/domain/enums.py
    - services/scheduling/domain/schemas.py
    - services/scheduling/domain/contracts.py
    - services/scheduling/workflows/cancel.py
    - services/scheduling/workflows/reschedule.py
    - services/scheduling/workflows/__init__.py
    - services/scheduling/activities/__init__.py
    - services/scheduling/app.py
    - services/scheduling/worker.py
    - tests/temporal_fixtures.py
    - tests/scheduling/conftest.py
    - tests/scheduling/test_health.py
    - tests/workflows/test_replay.py
    - tests/libs/test_service_isolation.py

key-decisions:
  - "Advance id waitlist-advance-{source_workflow_id} (cancel-{id}-{v} / reschedule-{id}-{v}); only cancel and a successful reschedule start an advance (compensation/reaper releases and joining while a slot is open do not auto-book)"
  - "Claim activity creates the pending appointment itself (key waitlist-{appointment_id}, caller/created_by = entry.created_by, coverage_ref copied DB-to-DB, workflow_started=True) so the 02-07 starter never races the child start"
  - "SlotTaken at claim -> rollback, entry stays waiting (attempts unchanged), advance ends slot_taken; PatientOverlap -> rollback then a second transaction marks the entry failed/PatientOverlap (attempt counted), advance moves on"
  - "Child results: confirmed -> entry booked; SlotUnavailable/ConfirmLostHold (LOST_RACE_REASONS) -> entry back to waiting, advance ends slot_gone; anything else -> entry failed with the saga's reason, next entry; cap WAITLIST_MAX_ATTEMPTS = 5"
  - "Claim exhausting its infra retries -> advance outcome claim_failed (no unbounded retry); resolve exhausting its compensation budget is logged and the booking outcome still decides"
  - "Patched branch in CancelWorkflow runs after the notification whenever step 1 did not raise (i.e. cancelled or already); the activity has no return value, so no outcome variable was needed"

patterns-established:
  - "Waitlist test helpers in tests/scheduling/test_waitlist.py: add_patient(client, world) -> (patient_id, tokens), book_for(client, env, world, slot_id, patient_id), join/joined, get_entry, result_of(env, wf_id, type), cancel_and_advance, run_advance(env, world, slot_id), appointments_on_slot(engine, slot_id)"
  - "cleanup_scheduling_rows also removes waitlist_entries by provider_id"
  - "Replay of a history whose child ids derive from workflow.info().workflow_id must use the recorded workflow id (test_replay._load_as_recorded)"

requirements-completed: [SCHED-07]

coverage:
  - id: D1
    description: "Tracer: patient joins with own token, another patient's cancellation auto-books them via child BookingWorkflow"
    requirement: SCHED-07
    verification:
      - kind: integration
        ref: "tests/scheduling/test_waitlist.py#test_patient_joins_waitlist_and_is_auto_booked_on_cancel"
        status: pass
    human_judgment: false
  - id: D2
    description: "FIFO, window, billing rejection -> next, patient overlap -> failed without appointment, lost race at claim, no match, reschedule frees old slot"
    requirement: SCHED-07
    verification:
      - kind: integration
        ref: "tests/scheduling/test_waitlist.py"
        status: pass
    human_judgment: false
  - id: D3
    description: "Advance loop in isolation: one claim/child/resolve, lost race stops, business failure moves on, entry_failed and saga failures capped, bounded claim failure"
    requirement: SCHED-07
    verification:
      - kind: integration
        ref: "tests/workflows/test_waitlist_workflow.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "Concurrent frees never share an entry (repo level and through the gateway)"
    requirement: SCHED-07
    verification:
      - kind: integration
        ref: "tests/scheduling/test_waitlist_concurrency.py"
        status: pass
    human_judgment: false
  - id: D5
    description: "Pre-patch histories replay against patched cancel/reschedule; v2 histories carry the marker + child and replay"
    requirement: SCHED-06, SCHED-05
    verification:
      - kind: integration
        ref: "tests/workflows/test_replay.py"
        status: pass
    human_judgment: false
---

# Phase 2 Plan 09: Waitlist Advance Summary

**A freed slot (cancel, or the old slot of a reschedule) now auto-books the oldest matching waitlist entry through the normal booking saga, run as a child workflow. The entry is claimed with `FOR UPDATE SKIP LOCKED`, so concurrent frees never share one. The change to the two recorded workflows sits behind `workflow.patched("waitlist-advance-v1")`.**

## Performance
- Tasks: 3 of 3
- Files: 11 created, 16 modified

## Accomplishments
- `waitlist_entries` (migration `scheduling_0003`, round-trips up, down to `scheduling_0002`, and up again) with a one-active-entry partial unique index, a FIFO partial index and status/window CHECKs.
- `/api/waitlist` endpoints:
  - POST: 201. A patient joins only for themselves (403 otherwise). Staff must name the patient (422 without one). The provider must resolve to the caller's clinic (the same 404 otherwise). A second active entry for the same patient and provider is 409. An inverted, past or naive window is 422.
  - GET list and GET by id: scoped by role.
  - POST cancel: waiting -> cancelled is 200, a repeat is a 200 no-op, a matched entry is 409 "match in progress", booked or failed is 409.
  - The provider role gets 403 everywhere. `coverage_ref` is never returned.
- `WaitlistAdvanceWorkflow` loops up to 5 times: claim, run `BookingWorkflow` as an ABANDONed child, then resolve. Its outcomes are `booked`, `no_match`, `slot_gone`, `slot_taken`, `exhausted` and `claim_failed`.
- CancelWorkflow and RescheduleWorkflow start the advance after their notification step. It runs only on new runs, behind `workflow.patched(WAITLIST_PATCH_ID)`. All four v1 histories still replay with every Scheduling workflow registered.

## Task Commits
1. **Task 1: Tracer** - `aae0931` (feat)
2. **Task 2: Behaviour, reschedule patch, RBAC** - `5af8ee7` (test, includes the reschedule patch)
3. **Task 3: Concurrency, replay, histories** - `0f83f15` (test)

**Plan metadata:** see the `docs(02-09): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter.

## Deviations from Plan

**1. [Plan assumption obsolete] Fixture move not needed.** `temporal_env`, `scheduling_worker` and `booking_world` were already shared from `tests/temporal_fixtures.py` through the root `pytest_plugins` (02-06). So `tests/workflows/fixtures.py` was not created, and neither `tests/conftest.py` nor `tests/workflows/conftest.py` changed. The worker registration went into `tests/temporal_fixtures.running_scheduling_worker` and `worker.WORKFLOWS` (the 02-07 convention), not into a `scheduling_worker` fixture body.

**2. [Real code] Cancel "outcome" condition.** `ACT_CANCEL_APPOINTMENT` returns nothing; refusals raise and return early. So the patched branch is just `if workflow.patched(WAITLIST_PATCH_ID):` after the notification step. It covers exactly the "cancelled or already" cases.

**3. [Additions to the contracts table]**
- New constants:
  - `WAITLIST_ADVANCE_WORKFLOW` and `WAITLIST_MAX_ATTEMPTS = 5`
  - claim outcomes `CLAIM_*` and advance outcomes `ADVANCE_*`
  - outcome `claim_failed`, for a claim whose infrastructure retries were exhausted
- New repository functions: `find_matched_entry` and `fail_waiting_entry` (used on PatientOverlap, since the claim was rolled back).
- New helpers `waitlist_request_hash` and `waitlist_idempotency_key` in `activities/waitlist.py`. These copy the API's canonical hash so the activity does not import FastAPI code.
- `resolve_entry` returns the resolution string.
- `WaitlistResolveInput.reason` defaults to None.

**4. [Extra fixture]** I also recorded `waitlist_advance_booked_v1.json` (the advance history itself: claim, child, resolve), so later changes to `WaitlistAdvanceWorkflow` have a replay gate. The recorder is `SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay.py -k record_waitlist`.

**5. [Test infra]**
- The v2 cancel/reschedule histories must be replayed under their RECORDED workflow id, because the advance child id is derived from `workflow.info().workflow_id`. `_load_as_recorded` recovers that id from the child-start event.
- Patch markers are `core_patch` markers whose id is base64 JSON. The test decodes them to assert the marker is present in v2 and absent in v1.
- `run_advance` uses `execute_workflow` without `time_skipping_unlocked()`. The 02-08 gotcha applied: wrapping it made the workflow "time out".

**6. [Test hygiene]**
- `cleanup_scheduling_rows` now deletes `waitlist_entries` by provider (test-data only).
- `test_health` now expects 5 Scheduling tables.
- `waitlist_entries` was added to `OWNED_TABLES` in Task 1 rather than Task 3, so every commit stays green.

**7. [Test realism]** The tracer uses `world.last_day` (at least 33 h ahead) instead of "3 days ahead". The lost-race case inserts the competing pending appointment with SQL, as the plan says.

**8. [Process]** The Task 2 tests were written first. Three failed before implementation: two because of the test-helper timeout above, and the reschedule test because the patch was not there yet. Then the reschedule patch was added in the same commit.

## Human checkpoints
- None in this plan.

## Verification
- Task 1 verify (alembic upgrade, downgrade `scheduling_0002`, upgrade, then `pytest tests/scheduling/test_waitlist.py -x`): exit 0
- Task 2 verify (`pytest tests/scheduling/test_waitlist.py tests/workflows/test_waitlist_workflow.py -x -q`): 24 passed
- Task 3 verify (`pytest tests/scheduling/test_waitlist_concurrency.py tests/workflows/test_replay.py tests/libs/test_service_isolation.py -x -q`): 37 passed, 3 skipped (recorders)
- Full suite `pytest tests -q -m "not e2e"`: **713 passed, 3 skipped** (was 659 + 2 skipped), 13 min 49 s
- Acceptance greps: all pass
  - `workflow.patched(WAITLIST_PATCH_ID)` appears once in cancel.py and once in reschedule.py
  - no `delete(` or `DELETE FROM` in waitlist_repo.py
  - no `coverage_ref` in contracts.py
  - no `coverage`, `Bearer` or `DENY-` in any history fixture
- Semgrep: not run (not installed in executor environment)

## Known limitations
- If the claim activity commits and then fails past all its retries (the database is unreachable for over a minute right after the commit), the entry stays `matched` and its `pending` appointment, marked `workflow_started_at`, is never started. That appointment blocks the slot through `uq_appointments_active_slot` until an operator fails it. The advance reports `claim_failed`.
- `CancelInput.slot_id` is the slot from the time of the request. If a reschedule committed between the request and the cancel, the advance looks at the old slot and ends `slot_gone`, and the slot that was actually freed is not offered.
- Joining a waitlist while a matching slot is already open does not auto-book. Slots freed by saga compensation or by the reaper do not advance the waitlist.

## Next Phase Readiness
- Any further change to the CancelWorkflow, RescheduleWorkflow or WaitlistAdvanceWorkflow command sequence needs a new patch id. Keep the v1 and v2 fixtures replaying.
