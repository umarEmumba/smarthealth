---
phase: 02-scheduling-service-workflow
plan: 08
subsystem: workflow
tags: [temporal, cancel, reschedule, refund, reserve-then-swap, billing, replay]

# Dependency graph
requires:
  - phase: 02-02
    provides: "slot_repo.hold_slot / release_hold (conditional, retry-safe), uq_appointments_active_slot, ex_appointments_patient_no_overlap"
  - phase: 02-05
    provides: "Billing state-targeted transitions (refund_pending/refunded/no_refund) and relink, idempotent per key"
  - phase: 02-06
    provides: "start_once, temporal_policies presets, contracts rules, shared Temporal fixtures"
  - phase: 02-07
    provides: "worker.WORKFLOWS list, replay gate + recorder pattern, time_skipping_unlocked gotcha"
provides:
  - "POST /api/appointments/{id}/cancel (202 / 200 no-op) and /reschedule (202), refund policy decided in the request"
  - "CancelWorkflow, RescheduleWorkflow (reserve-then-swap), AppointmentChangeActivities (8 change.* activities)"
  - "appointment_repo.cancel_appointment / swap_slot / record_history_note"
  - "Contracts: CancelInput, RescheduleInput, ChangeResult, NotifyInput, ChangeOutcomeInput, cancel_workflow_id, reschedule_workflow_id, ACT_* change names"
  - "SchedulingSettings.cancel_cutoff_hours (SCHEDULING_CANCEL_CUTOFF_HOURS, default 24)"
  - "Recorded histories cancel_refund_v1.json and reschedule_v1.json (replayed in test_replay.py) for 02-09's workflow.patched proof"
affects: [02-09, 02-10, 02-11, 02-12]

actuals:
  tokens: 60000
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Time-dependent policy (started? inside cutoff? waiver?) decided in the API request and passed as plain strings; workflows never read the clock"
    - "Forward-only change workflows: once the local transaction commits, a billing step that misses its budget is recorded (history note + audit) and reported as reason billing_pending, never undone"
    - "History notes (from_status == to_status) for outcomes that are not transitions, idempotent per (reason, actor=workflow:<wf_id>)"
    - "Change notification is its own activity (change.schedule_notification) so BookingWorkflow and its recorded histories are untouched"

key-files:
  created:
    - services/scheduling/workflows/cancel.py
    - services/scheduling/workflows/reschedule.py
    - services/scheduling/activities/changes.py
    - tests/scheduling/test_cancel.py
    - tests/scheduling/test_reschedule.py
    - tests/workflows/test_change_workflows.py
    - tests/workflows/histories/cancel_refund_v1.json
    - tests/workflows/histories/reschedule_v1.json
  modified:
    - services/scheduling/config.py
    - services/scheduling/domain/contracts.py
    - services/scheduling/domain/schemas.py
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/api/appointments.py
    - services/scheduling/worker.py
    - services/scheduling/workflows/__init__.py
    - services/scheduling/activities/__init__.py
    - tests/temporal_fixtures.py
    - tests/workflows/test_replay.py

key-decisions:
  - "Separate ACT_NOTIFY_CHANGE = change.schedule_notification activity with NotifyInput(appointment_id, clinic_id, notification_type) (the plan's own fallback); BookingWorkflow, ACT_NOTIFY and the 02-07 histories are unchanged, so no workflow.patched was needed"
  - "cancel_appointment releases the slot the appointment points at NOW (read under FOR UPDATE), not the slot in the request, so a reschedule that committed first never leaves its new slot booked by a cancelled appointment"
  - "A failed reschedule's history note bumps appointments.version, so the next attempt gets a fresh reschedule-{id}-{version} id (otherwise REJECT_DUPLICATE would silently swallow every later attempt)"
  - "Billing refusals in change steps raise non-retryable BillingConflict; billing not landing in the COMPENSATION budget -> outcome kept, reason billing_pending, note cancel_incomplete:billing_pending / reschedule_incomplete:billing_pending, audit billing.cancel_incomplete / billing.relink_incomplete"
  - "A cancel refused by the repo (NotCancellable) is recorded as note cancel_failed:NotCancellable"
  - "Cancel/reschedule start failures are 503 'workflow service unavailable' (nothing was committed, the client retries); unlike booking there is no sweeper"
  - "NON_RETRYABLE in libs/temporal_policies.py was NOT extended (it is part of BookingWorkflow's activity options); the new business errors are raised with non_retryable=True"

patterns-established:
  - "History-note actor = workflow:<workflow_id> (<= 64 chars) for idempotency; transition rows keep the requesting user (user:<id>) as actor"
  - "Shared change-test helpers live in tests/scheduling/test_cancel.py (day_slots, book_confirmed, change_result, billing_moves, insert_appointment, slot_status)"

requirements-completed: [SCHED-05, SCHED-06, BILL-02]

coverage:
  - id: D1
    description: "Tracer: confirmed appointment cancelled through the gateway (patient) -> workflow cancelled, appointment cancelled with one confirmed->cancelled row, slot available, billing verified -> refund_pending -> refunded"
    requirement: SCHED-06
    verification:
      - kind: integration
        ref: "tests/scheduling/test_cancel.py#test_tracer_cancel_frees_the_slot_and_refunds"
        status: pass
    human_judgment: false
  - id: D2
    description: "Cutoff and waiver: slot ~2h ahead -> no_refund; staff waive_cutoff -> refunded; patient waive_cutoff -> 403"
    requirement: BILL-02
    verification:
      - kind: integration
        ref: "tests/scheduling/test_cancel.py"
        status: pass
    human_judgment: false
  - id: D3
    description: "Repeat cancel -> 200 no-op, no second workflow, one refund_pending/refunded pair, one cancel history row; pending 409 'booking in progress'; failed 409; started 409; other patient 404 identical to missing"
    requirement: SCHED-06
    verification:
      - kind: integration
        ref: "tests/scheduling/test_cancel.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "Reschedule success: slot_id/start/end moved, version+1, old slot available, new booked, billing relink row (verified->verified, rescheduled), no refund state"
    requirement: SCHED-05
    verification:
      - kind: integration
        ref: "tests/scheduling/test_reschedule.py#test_reschedule_swaps_slots_atomically_and_relinks_billing"
        status: pass
    human_judgment: false
  - id: D5
    description: "New slot taken -> failed SlotUnavailable, original untouched, note reschedule_failed:SlotUnavailable, and a later attempt succeeds; patient overlap -> SwapRejected, new hold released, original untouched; swap repeat -> already, no extra row; API refusals (pending/cancelled/failed 409, other provider 422, other clinic/missing 404, past/withdrawn 409)"
    requirement: SCHED-05
    verification:
      - kind: integration
        ref: "tests/scheduling/test_reschedule.py"
        status: pass
    human_judgment: false
  - id: D6
    description: "Stub-activity step orders: cancel [cancel, billing, notify]; billing failing -> cancelled/billing_pending, never undone; NotCancellable recorded; reschedule [hold, swap, relink, notify]; swap rejected [hold, swap, release, record]; hold unavailable never swaps; relink failure keeps the swap"
    verification:
      - kind: unit
        ref: "tests/workflows/test_change_workflows.py"
        status: pass
    human_judgment: false
  - id: D7
    description: "cancel_refund_v1.json and reschedule_v1.json replay against current code (and the 02-07 booking histories still do); fixtures hold ids only"
    verification:
      - kind: unit
        ref: "tests/workflows/test_replay.py"
        status: pass
    human_judgment: false

duration: 50min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 08: Cancel and Reschedule Workflows Summary

**Cancel and reschedule now run as Temporal workflows on `scheduling-tq`. A cancel releases the slot in one local transaction, then moves billing to `refund_pending -> refunded` before the 24-hour cutoff or to `no_refund` inside it (staff can waive the cutoff). A reschedule holds the new slot first and then swaps in one transaction. Any failure leaves the original booking untouched and is written to the appointment's history. Both workflows have recorded histories that replay against the current code.**

## Performance

- **Duration:** ~50 min
- **Completed:** 2026-10-13
- **Tasks:** 3
- **Files modified:** 18

## Accomplishments
- `POST /api/appointments/{id}/cancel` and `/reschedule` use the same scope rules as GET. A patient can act only on their own appointments; front-desk staff only in their clinic. Out-of-scope requests get the same 404 as a missing appointment. A `pending` appointment returns 409 "booking in progress". A cancelled or failed appointment returns 409, and so does one whose slot has already started. Cancelling an already-cancelled appointment returns 200 as a no-op. The refund policy is decided in the request.
- `CancelWorkflow`: `cancel_appointment` -> `billing_cancel` -> notification, forward-only. If billing does not land within its long budget, the result is `cancelled/billing_pending` and the gap is recorded and audited for an operator.
- `RescheduleWorkflow`: `hold_new_slot` -> `swap_slot` (one transaction; the DB arbiters' IntegrityError becomes `SwapRejected`) -> `billing_relink` -> notification. If the hold or the swap fails, the new hold is released, a `reschedule_failed:<reason>` note is written and the result is `failed`.
- Every step is idempotent and writes its audit entry last with `{wf}:{step}`. Billing keys are `{wf}:refund_pending`, `{wf}:refunded`, `{wf}:no_refund` and `{wf}:relink`.
- Recorded histories `cancel_refund_v1.json` and `reschedule_v1.json` are committed, contain ids only, and replay cleanly. An opt-in recorder is `SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay.py -k record_change`.

## Task Commits

1. **Task 1: Cancel tracer** - `0c8b506` (feat; includes the reschedule implementation, see deviation 4)
2. **Task 2: Reschedule tests** - `7ff9be8` (test)
3. **Task 3: Cancel edge cases, step orders, histories** - `cb89d34` (test)

**Plan metadata:** see the `docs(02-08): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter.

## Deviations from Plan

**1. [Plan fallback taken] Separate change-notification activity**
- The plan's preferred design was a `NotifyInput` on `ACT_NOTIFY` shared with booking. That would change the input of BookingWorkflow's notify activity and so the 02-07 recorded histories. I used the plan's own fallback instead: `ACT_NOTIFY_CHANGE = "change.schedule_notification"`, as the plan-writer asked. `NotifyInput` also carries `clinic_id` for the audit entry.

**2. [Defect in plan design] Repeated reschedule attempts would be swallowed**
- **Issue:** `reschedule-{id}-{version}` combined with `REJECT_DUPLICATE` means that after a failed reschedule (version unchanged) every later attempt at the same version hits the closed run. `start_once` then returns False and the request would answer 202 with nothing happening.
- **Fix:** the failed-reschedule note bumps `appointments.version` (`record_history_note(..., bump_version=True)` through `ChangeOutcomeInput.bump_version=1`). The test retries and succeeds after a `SlotUnavailable` failure.

**3. [Defect guard] Cancel releases the appointment's current slot**
- `cancel_appointment(db, *, appointment_id, actor)` locks the row and releases the slot the appointment points at now. A cancel racing a reschedule therefore always frees the right slot. `CancelInput.slot_id` is kept for traceability only.

**4. [Process] TDD order for Task 2**
- The reschedule repo step, activities, workflow and endpoint were written together with the cancel plumbing in the Task 1 commit, because the worker registers both workflows. Task 2's tests were written afterwards and passed on their first run, so there was no observed red phase for Task 2.

**5. [Additions]** `ChangeOutcomeInput` (note, audit_action, bump_version) is a new contract that the plan's table does not list. Constants added: `CANCEL_WORKFLOW`, `RESCHEDULE_WORKFLOW`, `REFUND`, `NO_REFUND`, `OUTCOME_CANCELLED`, `OUTCOME_RESCHEDULED`, `BILLING_PENDING`, `NOTIFY_*`. `worker.build_activities()` now returns `(SchedulingActivities, AppointmentChangeActivities)`.

**6. [Test infra]** Wrapping `env.client.execute_workflow(...)` in `time_skipping_unlocked()` made the stub workflow "time out" immediately. `execute_workflow` on the test client already skips time, so the stub tests call it plainly. Waits on `get_workflow_handle(...).result()` still need the unlock (02-07 gotcha).

**7. [Test realism]** The tracer uses the seeded last day (at least 33 h ahead) rather than "3 days ahead". The "new slot taken between request and hold" case starts `RescheduleWorkflow` directly after taking the slot in the DB, because that race cannot be steered through the API. Pending, failed and started appointments are inserted directly (`insert_appointment`).

## Human checkpoints
- None in this plan.

## Verification
- `pytest tests/scheduling/test_cancel.py -x`: 9 passed
- `pytest tests/scheduling/test_reschedule.py -x`: 6 passed
- `pytest tests/scheduling/test_cancel.py tests/scheduling/test_reschedule.py tests/workflows -x -q`: 81 passed, 2 skipped (recorders)
- Full suite `pytest tests -q -m "not e2e"`: **659 passed, 2 skipped** (was 617 + 1 skipped), 12 min
- Acceptance greps: all pass (`delete(`/`DELETE FROM` in appointment_repo.py = 0)
- Semgrep: not run (not installed in executor environment)

## Known limitations
- A second reschedule request with a different `new_slot_id` while one is running for the same version attaches to the running workflow (202) and its slot is ignored. The client sees the real outcome on GET.
- A swap that commits but whose audit write keeps failing past the forward budget would be reported `failed` (the release is a no-op on a booked slot, so the data stays correct). The note would then be misleading. This is rare and depends on Mongo being down for over 60 s.

## Next Phase Readiness
- 02-09 (waitlist advance) adds the child workflow to `CancelWorkflow` behind `workflow.patched(...)` and replays `cancel_refund_v1.json` to prove it.
