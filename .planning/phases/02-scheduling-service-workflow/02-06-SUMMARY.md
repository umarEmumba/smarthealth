---
phase: 02-scheduling-service-workflow
plan: 06
subsystem: api
tags: [temporal, saga, fastapi, idempotency, opentelemetry, scheduling, billing]

# Dependency graph
requires:
  - phase: 02-02
    provides: "accept_booking, hold_slot, confirm_booking, release_hold, mark_appointment_failed, idempotency_keys arbiter"
  - phase: 02-04
    provides: "references.* service-token reads, api/deps.get_service_tokens, seed_bookable_world, /api/slots"
  - phase: 02-05
    provides: "Billing /internal/billing/prechecks and /transitions (voided) contracts"
provides:
  - "temporalio==1.34.0 pinned (human-approved), libs/temporal.py (connect_client lazy + TracingInterceptor, start_once) and sandbox-safe libs/temporal_policies.py"
  - "services/scheduling/domain/contracts.py: BookingInput, BookingResult, MarkFailedInput, ACT_* names, booking_workflow_id"
  - "BookingWorkflow saga (validate -> reserve -> billing -> best-effort notify -> confirm; reverse-order compensation; mark_failed)"
  - "SchedulingActivities (validate, reserve_slot, billing_precheck, schedule_notification, confirm, release_slot, void_billing, mark_failed)"
  - "python -m services.scheduling.worker entrypoint (heartbeat /tmp/scheduling-worker.alive)"
  - "POST /api/appointments (202 + Location), GET /api/appointments/{id} (with history), GET /api/appointments (scoped filters)"
  - "Shared Temporal test fixtures temporal_env, scheduling_worker, booking_world, running_scheduling_worker() in tests/temporal_fixtures.py"
affects: [02-07, 02-08, 02-09, 02-10, 02-11, 02-12]

actuals:
  tokens: 27963
  tasks: 3
  commits: 3

tech-stack:
  added: ["temporalio==1.34.0"]
  patterns:
    - "Saga with compensation registered before each forward step; compensations are forward transitions run in reverse; business failure returns BookingResult('failed', reason) so the run closes Completed"
    - "Activities referenced by string name (ACT_*); workflow module imports only temporalio + passed-through pure contracts/policies"
    - "Class-based activities with constructor-injected session factory, httpx client, ServiceTokenProvider, Mongo; audit LAST with dedupe_key {workflow_id}:{step}"
    - "API accepts in one local tx, commits, then start_once; Temporal failure => 202 anyway + booking.workflow_start_deferred (sweeper recovers)"
    - "Shared pytest fixtures for sibling test folders via pytest_plugins in the root tests/conftest.py"

key-files:
  created:
    - libs/temporal.py
    - libs/temporal_policies.py
    - services/scheduling/domain/contracts.py
    - services/scheduling/workflows/booking.py
    - services/scheduling/activities/booking.py
    - services/scheduling/worker.py
    - services/scheduling/api/appointments.py
    - tests/temporal_fixtures.py
    - tests/workflows/conftest.py
    - tests/workflows/test_booking_workflow.py
    - tests/workflows/test_contracts.py
    - tests/workflows/test_trace.py
    - tests/scheduling/test_booking_api.py
  modified:
    - pyproject.toml
    - libs/config.py
    - services/scheduling/config.py
    - services/scheduling/app.py
    - services/scheduling/api/deps.py
    - services/scheduling/domain/schemas.py
    - services/scheduling/domain/enums.py
    - services/scheduling/repository/appointment_repo.py
    - services/scheduling/repository/idempotency_repo.py
    - tests/conftest.py

key-decisions:
  - "Task 1 (blocking human-verify): user approved temporalio 1.34.0 ('Approve 1.34.0'); pinned exactly, no extras"
  - "Saga order follows D-09 (billing before notification), deviating from SCHED-02 text; documented in the workflow docstring"
  - "No execution/run timeout when starting the saga; all activities bounded by FORWARD/NOTIFY/COMPENSATION presets"
  - "failure_reason stores the failing ApplicationError.type verbatim (FailureReason.CONFIRM_LOST_HOLD is now 'ConfirmLostHold'; added activity_timeout/activity_failed)"
  - "The request hash uses the RESOLVED patient_id, so a PATIENT's body with or without its own patient_id hashes the same"
  - "void_billing treats 404 (no record) and 409/422 (nothing left to void) as no-ops; billing_precheck maps 409/422/404 and any non-verified status to BillingRejected"
  - "GET routes allow PATIENT/FRONT_DESK/ADMIN only (PROVIDER 403); out-of-scope reads are the same 404 as missing"

patterns-established:
  - "Inject a Temporal client into a service via app.state.temporal_client (None = lazy connect_client on first use)"
  - "Workflow logic tests register stub activities under the real ACT_* names on a time-skipping env"

requirements-completed: [SCHED-01, SCHED-02, SCHED-04, BILL-01]

coverage:
  - id: D1
    description: "temporalio 1.34.0 pinned and importable after the human legitimacy checkpoint"
    verification:
      - kind: other
        ref: ".venv/bin/python -c \"import temporalio, importlib.metadata as m; print(m.version('temporalio'))\" -> 1.34.0"
        status: pass
    human_judgment: false
  - id: D2
    description: "Front-desk booking via gateway -> 202 -> saga confirmed -> GET confirmed, slot booked, billing verified"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_booking_api.py#test_tracer_front_desk_booking_is_confirmed_by_the_saga"
        status: pass
    human_judgment: false
  - id: D3
    description: "Saga order, best-effort notification, reverse-order compensation with mark_failed reasons"
    requirement: SCHED-02
    verification:
      - kind: unit
        ref: "tests/workflows/test_booking_workflow.py"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_booking_api.py#test_billing_rejection_fails_the_booking_and_frees_the_slot"
        status: pass
    human_judgment: false
  - id: D4
    description: "Idempotent HTTP contract: replay same id, 422 body mismatch, 409 taken/pending/withdrawn/overlap, 400 key, 403/404 scope, deferred start when Temporal is down"
    requirement: SCHED-04
    verification:
      - kind: integration
        ref: "tests/scheduling/test_booking_api.py"
        status: pass
    human_judgment: false
  - id: D5
    description: "Workflow contracts round-trip through the default converter; no Enum fields; no PHI/coverage/credential field names; pure module"
    verification:
      - kind: unit
        ref: "tests/workflows/test_contracts.py"
        status: pass
    human_judgment: false
  - id: D6
    description: "One trace across the API request, StartWorkflow, RunWorkflow and RunActivity spans"
    verification:
      - kind: integration
        ref: "tests/workflows/test_trace.py#test_booking_request_workflow_and_activities_share_one_trace"
        status: pass
    human_judgment: false
  - id: D7
    description: "Billing pre-check from the saga (coverage_ref read in the activity, Idempotency-Key {workflow_id}:billing_precheck)"
    requirement: BILL-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_booking_api.py#test_tracer_front_desk_booking_is_confirmed_by_the_saga"
        status: pass
    human_judgment: false
  - id: D8
    description: "Worker entrypoint starts against Compose Temporal and stops cleanly on SIGINT"
    verification:
      - kind: manual_procedural
        ref: "SCHEDULING_CLIENT_SECRET_PATH=<tmp> timeout -s INT 12 .venv/bin/python -m services.scheduling.worker (worker.started ... worker.stopped)"
        status: pass
    human_judgment: false

duration: 30min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 06: Temporal Booking Saga Summary

**`POST /api/appointments` accepts a booking in one local transaction and answers 202, then a durable `BookingWorkflow` on `scheduling-tq` validates, holds the slot, runs the Billing pre-check, records the notification intent and confirms, compensating in reverse order (void billing, release hold, mark failed) on any business failure. Built on temporalio 1.34.0 with ids-only contracts and one connected trace per booking.**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-10-13T05:58:00Z
- **Completed:** 2026-10-13T06:40:00Z
- **Tasks:** 3 (Task 1 = approved human checkpoint)
- **Files modified:** 26

## Accomplishments
- `libs/temporal_policies.py` (sandbox-safe: task queues, `NON_RETRYABLE`, FORWARD/NOTIFY/COMPENSATION presets) and `libs/temporal.py` (`connect_client(lazy=...)` with `TracingInterceptor`, `start_once` with USE_EXISTING + REJECT_DUPLICATE and a 5 s RPC timeout).
- `BookingWorkflow` per research Pattern 3; the module docstring covers the D-09 order, the SCHED-02 deviation, the no-run-timeout rule and the D-19 patching rule.
- `SchedulingActivities` (8 activities); every state change commits, then audits with `dedupe_key={workflow_id}:{step}`, `actor_id="service:scheduling"`.
- Worker: bounded connect retries (2 s / 60 s), audit indexes, SIGTERM/SIGINT stop, 20 s graceful shutdown, heartbeat file. It never runs Alembic.
- `/api/appointments` POST/GET/list are mounted in the Scheduling app. The app gets `state.temporal_client` (lazy connect on first use), and its lifespan now also ensures audit indexes.

## Task Commits

1. **Task 1: temporalio 1.34.0 legitimacy checkpoint** - approved by the user ("Approve 1.34.0") before execution; no commit of its own (the pin is in Task 2's commit).
2. **Task 2: Tracer** - `e2839e0` (feat)
3. **Task 3: Saga/contract/trace proofs + API edges** - `170a566` (test)

**Plan metadata:** see the `docs(02-06): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Test layout] Shared Temporal fixtures in `tests/temporal_fixtures.py`, registered via `pytest_plugins` in the root `tests/conftest.py`**
- **Issue:** The plan puts `temporal_env`/`scheduling_worker`/`booking_world` in `tests/workflows/conftest.py`, but `tests/scheduling/test_booking_api.py` needs them, and pytest does not share fixtures between sibling folders' conftests.
- **Fix:** New plugin module `tests/temporal_fixtures.py` (plus a `running_scheduling_worker(client, http, mongo)` context manager, which the trace test uses with a traced client). `tests/workflows/conftest.py` holds only a pointer docstring. `booking_world` cleans up with its own engine, because `committed_scheduling_engine` lives only in `tests/scheduling/conftest.py`.
- **Committed in:** e2839e0

**2. [Defect] An expired idempotency key could never be reused with a new body**
- **Issue:** The API's early replay check (needed so a replay is answered before any slot check) would have read an expired claim and answered 422 forever, because only `accept_booking` overwrites expired claims.
- **Fix:** `get_idempotency_record(..., live_only=False)` gained a `live_only` flag. The API passes `live_only=True`, and `accept_booking` is unchanged.
- **Files modified:** services/scheduling/repository/idempotency_repo.py
- **Committed in:** e2839e0

**3. [Consistency] `FailureReason.CONFIRM_LOST_HOLD` changed from `"confirm_lost_hold"` to `"ConfirmLostHold"`, and `ACTIVITY_TIMEOUT`/`ACTIVITY_FAILED` were added**
- **Issue:** The workflow stores `ApplicationError.type` as the reason, so the old constant would never match a stored value. Nothing referenced it yet. (enums.py is not in the plan's file list.)
- **Committed in:** e2839e0

**4. [Robustness] Worker exits 1 with `worker.client_secret_unavailable` instead of a traceback when its secret file is unreadable; the saga start is wrapped in `asyncio.wait_for(5 s)` so a request never outlives the gateway's 10 s upstream timeout.**

**5. [Process] TDD order for Task 3:** Task 2 implemented the workflow, activities and API in full, so the Task 3 tests passed on their first run. No production change was needed in Task 3.

**6. [Extra tests]** Besides the plan's behaviours, the tests cover: a patient booking for themselves, staff omitting patient_id (422), a pending-slot 409 from the database arbiter, `workflow_started_at` being set after a start, billing `DENY-` -> failed/voided with the slot freed, read scoping, `ReferenceInvalid` without compensation, and a transient error that exhausts its retries and still compensates.

**Total deviations:** 6 (1 test layout, 1 defect, 1 consistency, 1 robustness, 2 process/coverage)
**Impact on plan:** No scope creep; all must-haves met.

## Issues Encountered
- The Compose Temporal container had exited (it raced Postgres on an earlier restart). `docker start smarthealth-temporal-1` brought it back healthy.
- The worker smoke test needs `SCHEDULING_CLIENT_SECRET_PATH` because `infra/secrets/service-clients/` does not exist locally. Compose wiring for the secret is 02-12's job.
- Semgrep: not run (not installed in executor environment).
- Research A4 (trace): the HTTP request span, StartWorkflow, RunWorkflow and every RunActivity span share one trace id, and StartWorkflow descends from the Scheduling request span. No wiring fix was needed.

## Verification
- Task 2 verify: version prints `1.34.0`; `pytest tests/scheduling/test_booking_api.py -x`: pass. All acceptance greps pass (pin=1, scheduling-tq=1, forbidden workflow imports=0, execution_timeout=0/0, `(str, Enum)` in contracts=0, status_code=202 >=1).
- Task 3 verify: `pytest tests/workflows tests/scheduling/test_booking_api.py -x -q`: 38 passed. Test-function counts: 8 / 7 / 1 / 16.
- Full suite `-m "not e2e"`: **599 passed, 1 deselected** (was 561). It takes about 10 min now; run it in the background or split it.
- Manual: `python -m services.scheduling.worker` against Compose Temporal logs `worker.started`, and on SIGINT logs `worker.stopping` then `worker.stopped`.

## User Setup Required
None. The time-skipping test server binary downloads automatically the first time (cached afterwards).

## Next Phase Readiness
- 02-07 (sweepers/reaper): use `appointment_repo.mark_workflow_started` and the `ix_appointments_pending_unstarted` scan. To restart, call `start_once(client, BOOKING_WORKFLOW, BookingInput(...), workflow_id=booking_workflow_id(id), task_queue=SCHEDULING_TASK_QUEUE)`, then mark the appointment.
- Register new workflows/activities in `services/scheduling/worker.py` (`SchedulingActivities.all()` lists the activities) AND in `tests/temporal_fixtures.running_scheduling_worker`.
- Tests that book without a worker should set `harness_apps()["scheduling"].state.temporal_client` to a stand-in and restore it afterwards (see `_DownTemporal` in test_booking_api.py), or the API will lazily connect to `TEMPORAL_ADDRESS`.
- 02-12: the worker container needs `SCHEDULING_CLIENT_SECRET_PATH`, `TEMPORAL_ADDRESS=temporal:7233`, a heartbeat-file healthcheck on `/tmp/scheduling-worker.alive`, and no Alembic.

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
