---
phase: 02-scheduling-service-workflow
plan: 07
subsystem: workflow
tags: [temporal, saga, compensation, schedules, reaper, replay, concurrency, e2e]

# Dependency graph
requires:
  - phase: 02-02
    provides: "slot_repo.expire_holds (FOR UPDATE SKIP LOCKED), conditional confirm/release, mark_appointment_failed"
  - phase: 02-05
    provides: "Billing deterministic pre-check (DENY- prefix) and voided transition"
  - phase: 02-06
    provides: "BookingWorkflow, SchedulingActivities, start_once, workflow_started_at marker, shared Temporal fixtures"
provides:
  - "Proof that a deliberately rejected or unreachable Billing step compensates to a clean, audited, re-offerable state (SCHED-08)"
  - "billing_precheck and void_billing now write one audit entry per saga step"
  - "HoldReaperWorkflow + PendingBookingStarterWorkflow, MaintenanceActivities (expire_holds, start_pending_bookings)"
  - "appointment_repo.list_unstarted_pending; SchedulingSettings reaper/starter interval + grace settings"
  - "services/scheduling/schedules.ensure_schedules (overlap SKIP, idempotent), called by the worker before polling"
  - "HTTP-tier single-winner test (20 parallel gateway bookings), committed replay histories, Compose-Temporal worker-crash test"
affects: [02-08, 02-09, 02-10, 02-11, 02-12]

actuals:
  tokens: 32672
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Maintenance as Temporal Schedules: one idempotent activity per run, ScheduleOverlapPolicy.SKIP, registered at worker start (ScheduleAlreadyRunningError = already present)"
    - "Lost-trigger starter: scan pending AND workflow_started_at IS NULL past a grace window, start_once, then conditional marker"
    - "Replay gate: committed, scrubbed (no stackTrace) histories replayed with Replayer; an opt-in recorder test re-captures them"
    - "Await a get_workflow_handle() result inside env.time_skipping_unlocked(), or retry backoff runs in real time"

key-files:
  created:
    - services/scheduling/workflows/maintenance.py
    - services/scheduling/activities/maintenance.py
    - services/scheduling/schedules.py
    - tests/workflows/test_booking_compensation.py
    - tests/workflows/test_maintenance.py
    - tests/workflows/test_schedules.py
    - tests/workflows/test_replay.py
    - tests/workflows/histories/booking_confirmed_v1.json
    - tests/workflows/histories/booking_billing_rejected_v1.json
    - tests/e2e/test_worker_crash.py
  modified:
    - services/scheduling/activities/booking.py
    - services/scheduling/activities/__init__.py
    - services/scheduling/workflows/__init__.py
    - services/scheduling/domain/contracts.py
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/config.py
    - services/scheduling/worker.py
    - tests/temporal_fixtures.py
    - tests/workflows/conftest.py
    - tests/workflows/test_contracts.py
    - tests/scheduling/test_concurrent_booking.py

key-decisions:
  - "An infrastructure failure that exhausts the forward retries is stored as its error type (UpstreamServiceUnavailable), keeping the 02-06 rule and its existing test, instead of the plan's activity_failed/activity_timeout"
  - "billing_precheck audits its outcome (success/failure, never the reference); void_billing audits only an applied void"
  - "Reaper audit: action slot.hold_expired, actor system:reaper, dedupe reaper:{appointment_id}"
  - "Starter takes no row lock (it calls Temporal per row); duplicates are harmless via start_once + conditional marker; MaintenanceResult.processed counts NEW starts"
  - "ensure_schedules returns only the ids it created; an existing Schedule is never modified (interval changes need a one-off update/delete)"
  - "Worker exits 1 (worker.schedules_failed) if the Schedules cannot be registered, so the orchestrator restarts it"

patterns-established:
  - "New Scheduling workflows go into services.scheduling.worker.WORKFLOWS; the test fixture imports that list"
  - "Workflow contracts may use plain int for counts (str for everything else)"

requirements-completed: [SCHED-08, SCHED-03, SCHED-04]

coverage:
  - id: D1
    description: "Rejected pre-check (DENY- coverage) via gateway: failed/BillingRejected, slot available, billing pending->failed->voided, one audit per step, pending->failed history, nothing deleted, slot re-offered"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/workflows/test_booking_compensation.py"
        status: pass
    human_judgment: false
  - id: D2
    description: "Billing unreachable for every attempt: bounded 5 attempts then compensation (release, mark_failed), no billing record"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/workflows/test_booking_compensation.py#test_unreachable_billing_exhausts_bounded_retries_then_compensates"
        status: pass
    human_judgment: false
  - id: D3
    description: "Hold reaper releases expired holds once, leaves live holds; a saga whose hold was reaped never confirms"
    verification:
      - kind: integration
        ref: "tests/workflows/test_maintenance.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "Pending-booking starter honours the grace window, starts orphans once, duplicate start is a no-op (running and closed runs)"
    requirement: SCHED-04
    verification:
      - kind: integration
        ref: "tests/workflows/test_maintenance.py"
        status: pass
    human_judgment: false
  - id: D5
    description: "ensure_schedules creates both Schedules with overlap SKIP on the Compose Temporal, second call is a no-op"
    verification:
      - kind: integration
        ref: "tests/workflows/test_schedules.py"
        status: pass
      - kind: manual_procedural
        ref: "SCHEDULING_CLIENT_SECRET_PATH=<tmp> timeout -s INT 12 python -m services.scheduling.worker -> schedule.* + worker.started/stopped; schedules then deleted"
        status: pass
    human_judgment: false
  - id: D6
    description: "20 parallel gateway bookings for one slot: one 202, nineteen 409, one confirmed appointment, slot booked"
    requirement: SCHED-03
    verification:
      - kind: integration
        ref: "tests/scheduling/test_concurrent_booking.py#test_twenty_parallel_http_bookings_for_one_slot_yield_one_202_and_one_confirmed"
        status: pass
    human_judgment: false
  - id: D7
    description: "Committed histories replay against BookingWorkflow; an unversioned edit fails replay; fixtures hold no coverage/Bearer/DENY-"
    verification:
      - kind: unit
        ref: "tests/workflows/test_replay.py"
        status: pass
    human_judgment: false
  - id: D8
    description: "Worker hung mid-billing on Compose Temporal; second worker completes the saga (billing started on attempt 2)"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_worker_crash.py"
        status: pass
    human_judgment: false

duration: 45min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 07: Booking Saga Failure Paths and Recovery Summary

**A deliberately rejected or unreachable Billing step now provably compensates to a clean, audited, re-offerable state. Two Temporal Schedules recover what the saga cannot: expired holds (reaper) and bookings whose workflow never started (starter). One-winner booking is proven through the gateway, recorded histories replay against current code, and a worker that dies mid-saga does not lose the booking.**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-10-13T06:39:50Z
- **Completed:** 2026-10-13T07:25:00Z
- **Tasks:** 3
- **Files modified:** 21

## Accomplishments
- Compensation tracer through the gateway with real activities and audit: `DENY-0001` gives `failed/BillingRejected`, the slot `available` with no hold left, billing `pending -> failed -> voided`, exactly one audit document per step (`reserve_slot`, `billing_precheck`, `void_billing`, `release_slot`, `mark_failed`) and none for `confirm`, history `[null->pending, pending->failed]`, row counts unchanged. The slot books again afterwards. Re-running every compensation after the saga changes nothing.
- Billing unreachable (worker-only degraded mesh): `billing_precheck` stops at attempt 5 and the saga compensates. Nothing reaches Billing.
- `HoldReaperWorkflow` / `PendingBookingStarterWorkflow` + `MaintenanceActivities`, `list_unstarted_pending`, settings, and `ensure_schedules` wired into the worker (and into the shared test worker via `worker.WORKFLOWS`).
- Tests for the reaper/saga race (Pitfall 5): confirm finds the hold gone, so the appointment stays `failed/hold_expired` and the slot is free. Also covered: the starter's grace window, the duplicate start against a running run (attach) and against a closed run (refused), and Schedule registration on Compose Temporal.
- HTTP tier of SCHED-03: 20 parallel front-desk bookings for distinct patients give one 202 and nineteen 409s, and exactly one appointment is confirmed.
- Two scrubbed histories (no stack traces) are committed and replay cleanly. A reordered, unversioned workflow raises `NondeterminismError`.
- Worker-crash e2e on Compose Temporal in about 12 s: worker A hangs inside billing and stops polling, worker B (on a separate client) picks up attempt 2 after the 10 s `start_to_close`, and the saga confirms.

## Task Commits

1. **Task 1: Compensation tracer** - `9726a78` (test, includes the audit fix)
2. **Task 2: Reaper + starter Schedules** - `d543e15` (feat)
3. **Task 3: HTTP concurrency, replay, worker crash** - `ae21e53` (test)

**Plan metadata:** see the `docs(02-07): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Defect] Billing saga steps wrote no audit entry**
- **Found during:** Task 1. The `{wf}:billing_precheck` and `{wf}:void_billing` audit counts were 0.
- **Fix:** `billing_precheck` audits `billing.precheck` (outcome success/failure, field names only), and `void_billing` audits `billing.void` when a void was applied. `_audit` gained an `outcome` parameter.
- **Files:** services/scheduling/activities/booking.py. **Commit:** 9726a78

**2. [Plan conflict] The reason for exhausted infrastructure retries is `UpstreamServiceUnavailable`, not `activity_failed`/`activity_timeout`**
- **Issue:** 02-06 stores `ApplicationError.type` verbatim, and its existing test `test_exhausted_transient_failure_compensates_with_generic_reason` asserts that. Mapping the reason to `activity_failed` would have meant changing that test.
- **Resolution:** Kept the behavior. The Task 1 test asserts the exact reason `UpstreamServiceUnavailable`.

**3. [Test infra] `get_workflow_handle(...).result()` does not unlock time skipping**
- **Issue:** Retry backoff ran in real time, and the long-poll ended with "No completion event found".
- **Fix:** The new tests wrap waits in `temporal_env.time_skipping_unlocked()`.

**4. [Plan conflict] `MaintenanceResult.processed: int` vs the contract test's "plain str only" rule**
- **Fix:** `tests/workflows/test_contracts.py` now allows plain `int` as well as `str`. Round-trip type identity, the no-Enum check and the PHI-name checks are unchanged.

**5. [Test layout]** `scheduling_worker_degraded` (in tests/workflows/conftest.py) builds the worker's client from `_mesh_mounts(("billing",))` instead of `degraded_mesh(...)`, so the gateway can still read Billing and prove nothing was written. That conftest also re-exports `committed_scheduling_engine`.

**6. [Test realism] Worker crash**
- **Issue:** A plain `Worker.shutdown()` reports the cancelled activity as failed, so the retry would happen at once without exercising the timeout.
- **Fix:** Worker A's billing activity ignores cancellation (a hung, dead worker), so recovery really goes through `start_to_close`. Worker B uses a second client, because one client may not run two workers on one task queue. The assertion is one `ActivityTaskScheduled` with its `ActivityTaskStarted.attempt == 2`: retries never add scheduled events.

**7. [Process]** The Task 2 tests were written before the implementation, but the first run happened after it, so no red run was recorded. The Task 3 HTTP concurrency test passed on its first run with no production change.

**8. [Extra]** `test_replay.py` contains an opt-in recorder (`SMARTHEALTH_RECORD_HISTORIES=1`; skipped otherwise, which is the "1 skipped" in the suite) and a negative replay test.

**9. [Environment]** The plan's `docker compose ... up -d --wait temporal` was run with `--pull never`, because of the Docker Hub rate limit.

**Total deviations:** 9. All must-haves are met, with no scope creep.

## Issues Encountered
- Semgrep: not run (not installed in executor environment).
- The worker smoke run registered `scheduling-hold-reaper` / `scheduling-pending-starter` on Compose Temporal; I deleted both afterwards so the dev server stays clean. A real worker start recreates them.

## Verification
- Task 1: `pytest tests/workflows/test_booking_compensation.py -x` gives 4 passed. Greps: 4 tests, 1 `DENY-`.
- Task 2: compose temporal healthy; `pytest tests/workflows/test_maintenance.py tests/workflows/test_schedules.py -x` passes (21 with test_contracts). Greps: SKIP 3, ScheduleAlreadyRunningError 3, ensure_schedules in worker 3, workflow_started_at in repo 5.
- Task 3: `pytest tests/scheduling/test_concurrent_booking.py tests/workflows/test_replay.py -x -q` gives 8 passed, 1 skipped (recorder). `pytest tests/e2e/test_worker_crash.py -m e2e -x` gives 1 passed (~12 s). History grep gate is empty, and both files are non-empty.
- Full suite `-m "not e2e"`: **617 passed, 1 skipped, 2 deselected** (was 599), 10 min 48 s.

## Next Phase Readiness
- Add new Scheduling workflows to `services.scheduling.worker.WORKFLOWS`. Add activities to `SchedulingActivities.all()`, or to `MaintenanceActivities.all()` for system jobs.
- When waiting on a workflow obtained with `get_workflow_handle` in the time-skipping env, use `async with temporal_env.time_skipping_unlocked():`.
- Changing `BookingWorkflow` from here on: the replay test fails unless the change is guarded with `workflow.patched(...)`. Record new `_v2` histories and keep the v1 files.
- Running two workers on one task queue in one process needs two Temporal clients.
- 02-12: the worker container registers the Schedules at start. Visit's sweeper (D-16) can reuse the `schedules.py` and `MaintenanceActivities` shape.

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
