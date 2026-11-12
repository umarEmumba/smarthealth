---
phase: 03-event-driven-system-observability
plan: 06
subsystem: notification
tags: [temporal, reminders, durable-timers, celery, replay-tests, signal-with-start]
requires:
  - phase: 03-event-driven-system-observability
    provides: enqueue_send / instrument_publisher and the notifications.send task (plan 03-02)
provides:
  - NOTIFICATION_TASK_QUEUE "notification-tq" and the pure workflow contracts
  - ReminderWorkflow (signals cancel, reschedule) and FollowUpReminderWorkflow
  - NotificationActivities.enqueue (activity name notification.enqueue) publishing the shared Celery send from a thread
  - client helpers start_reminder, reschedule_reminder, cancel_reminder, start_followup for plan 03-14
  - services.notification.temporal_worker (build_worker, main, heartbeat) for the Compose container added in 03-14
  - recorded histories reminder_v1.json and followup_v1.json with replay tests
affects: [03-14, 03-17, 03-18]
tech-stack:
  added: []
  patterns:
    - "Reminders are Temporal wait_condition timers (signal-interruptible); delivery stays on the one Celery send task"
    - "Dedupe key includes the slot epoch, so a rescheduled slot gets its own reminders: appointment.reminder:{appointment_id}:{slot_epoch}:{lead}"
    - "Signal-with-start (USE_EXISTING + ALLOW_DUPLICATE) for reschedule; USE_EXISTING + REJECT_DUPLICATE for start"
key-files:
  created:
    - services/notification/contracts.py
    - services/notification/workflows/__init__.py
    - services/notification/workflows/reminder.py
    - services/notification/activities.py
    - services/notification/reminders.py
    - services/notification/temporal_worker.py
    - tests/workflows/test_reminder_workflow.py
    - tests/workflows/test_replay_reminder.py
    - tests/workflows/histories/reminder_v1.json
    - tests/workflows/histories/followup_v1.json
  modified:
    - libs/temporal_policies.py
    - services/notification/config.py
    - tests/notification/conftest.py
key-decisions:
  - "A reminder whose enqueue activity ultimately fails is logged and the walk continues with the next lead (one lost lead must not cancel the others); the follow-up has one send, so its failure fails the workflow and stays visible in the Temporal UI"
  - "sent_leads in the result is for the latest slot: a reschedule clears the leads sent for the old slot"
  - "Lead settings are a comma string with a positive-integer validator and a reminder_leads property (pydantic-settings would otherwise require JSON for a list)"
requirements-completed: []
status: complete
duration: not measured
completed: 2026-11-11
actuals:
  tokens: 13400
  tasks: 3
  commits: 3
plan_head_before: ca7534fcc1ddaa0c10371d758a17647ddf996424
plan_head_after: 4468e279b7db99db3b8f18209990cb8f44bd966f
---

# Phase 3 Plan 06: Reminder Workflows Summary

**Appointment reminders and post-visit follow-ups are Temporal durable timers on `notification-tq`: `ReminderWorkflow` walks the configured leads largest-first with a signal-interruptible `wait_condition`, honours `cancel` and `reschedule`, skips leads already in the past, and each fired timer runs the `notification.enqueue` activity that publishes the same Celery `notifications.send` task the event consumer uses; both workflows have recorded, replayable histories and a worker entrypoint ready for Compose.**

REQUIREMENTS.md was not edited and no requirement was ticked (`requirements-completed` is empty by instruction). The plan addresses NOTIF-02, NOTIF-03 and EVENT-04.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | eb175f9 | `NOTIFICATION_TASK_QUEUE`, settings, pure contracts, both workflows, `notification.enqueue`, tracer test |
| 2 | c2c1567 | `reminders.py` client helpers, ten workflow tests, harness teardown-timeout fix |
| 3 | 4468e27 | `temporal_worker.py`, recorded histories, replay tests, worker tests |

The tracer feedback gate: Task 1's `<verify>` passed end to end (1 passed), so expansion proceeded. The workflow module in Task 1 already contained the cancel/reschedule logic (it is one class); Task 2 added the helpers and the tests that prove that logic.

## Verification (measured)

- Tracer: `pytest tests/workflows/test_reminder_workflow.py -k tracer` passed (about 18 s, of which about 10 s is the in-process Celery worker stopping).
- `pytest tests/workflows/test_reminder_workflow.py -q`: **12 passed** (plan: at least 8). Includes the real-activity tracer, ten fake-activity tests, the `build_worker` test and the `main()` test.
- `pytest tests/workflows/test_replay_reminder.py -q`: **7 passed, 2 skipped** (the two record tests, skipped without `SMARTHEALTH_RECORD_HISTORIES=1`); plan: at least 2 replays passed.
- Combined `test_replay_reminder.py` + `test_reminder_workflow.py`: 19 passed, 2 skipped, 22 s.
- `pytest tests/workflows tests/libs/test_service_isolation.py -q` (run in the background, one uninterrupted 181 s run, no sleep gap, no `E` marks): **290 passed, 11 skipped, 0 failed**. All existing replay tests still pass.
- Acceptance: `grep -c "notification-tq" libs/temporal_policies.py` = 1; `grep -c "notification.enqueue" services/notification/activities.py` = 2; `grep -c start_signal services/notification/reminders.py` = 2; both history files non-empty.
- Semgrep: `semgrep --config p/python --metrics=off services/notification libs/temporal_policies.py`: 151 rules, 13 files, **0 findings**. Narrower than the plan's `--config auto` (refused with `--metrics=off`, as in earlier Phase 3 plans). `tests/` not scanned.
- Not run: the plan's full `pytest tests -m "not e2e"` (the orchestrator said it is not required). `ruff` is not installed in `.venv`, so lint and format were not run.
- The throwaway RabbitMQ vhosts and `p3test*` Mongo databases the tests create were checked after the final runs: none remain.

## What the tests prove

- **Tracer** (real activity, throwaway vhost, in-process Celery worker, live Mongo): a slot three hours ahead with lead 120 completes `ReminderResult("completed", [120])`; the delivery with key `appointment.reminder:{appointment_id}:{slot_epoch}:120` ends `sent`, type `appointment.reminder`. The workflow runs in the default sandboxed runner (imports only `temporalio`, stdlib, `services.notification.contracts`, `libs.temporal_policies`).
- **Two leads** `[120, 1440]` given out of order, slot two days ahead: two activity calls, 1440 first, distinct keys.
- **Cancel** after the workflow reached its timer: `cancelled`, zero calls; `cancel_reminder` on the closed id and on an unknown id returns normally.
- **Reschedule** of a running reminder (three hours to eight hours): one call, key carries the NEW slot epoch, the old epoch appears nowhere.
- **Past leads**: slot 30 minutes ahead with lead 120 gives `skipped_past`, no call; slot five hours ahead with leads `[1440, 120]` skips 1440 and fires 120.
- **Idempotent starts**: a second `start_reminder` while running returns True and the run id is unchanged; after the run closed it returns False without raising. `start_followup` twice while sleeping is one run; after close, False.
- **Reschedule after close**: `reschedule_reminder` on a completed reminder starts a new run (different run id) that fires for the new slot (signal-with-start, `ALLOW_DUPLICATE`).
- **Follow-up**: one `visit.followup` call after the 4320-minute delay, key `visit.followup:{visit_id}`.
- **Replay**: `reminder_v1.json` (24 h and 2 h leads, a slot two days ahead, a reschedule signal to a slot five hours ahead, one fired lead after the signal) and `followup_v1.json` replay with no non-determinism; the history shape (signal before the single activity, one timer fired) is asserted; both files carry no coverage text, no `Bearer`/`DENY-` and no stack trace. An unpatched change (an extra timer) fails replay with `NondeterminismError`, so the gate bites.
- **Worker entrypoint**: `build_worker(client, celery_app)` runs one reminder and one `notifications.reminder` message waits in the queue; `main()` (connect patched to the time-skipping client) calls `instrument_publisher`, touches the heartbeat file and returns 0 after a real SIGTERM.

## Deviations from Plan

**1. [Rule 3 - Blocking, harness] In-process Celery worker teardown timeout raised from 10 s to 60 s** (`tests/notification/conftest.py`, `make_worker`). Plan 03-02's own tests take 8 to 10 s to stop an in-process worker, right at `celery.contrib.testing`'s 10 s default `shutdown_timeout`; my tracer failed in teardown (`Worker thread failed to exit`) in about 1 run in 3 until I raised it. This edits a 03-02 file; the fix is one argument and no behaviour of the code under test changes. After it the tracer passed in every later run (the 12-test file, the 19-test replay+workflow run and the final suite).

**2. [Test design] Result waits use `env.time_skipping_unlocked()`.** Handles obtained by `get_workflow_handle` (the helpers discard the start handle) do not unlock time skipping themselves; the first run of the tests hung 20 s on "No completion event found". A `_result` helper wraps the wait in the unlock. The helpers' production behaviour is unaffected.

**3. [Design addition] Enqueue failure handling.** The plan says only "using an existing forward activity preset". I use `FORWARD_ACTIVITY` and, in `ReminderWorkflow`, catch the `ActivityError` after its retries, log, and continue with the next lead. The follow-up lets the failure fail the workflow. Neither path has a test with an unreachable broker (not exercised).

**4. [Interface] Result of a rescheduled reminder.** `sent_leads` lists leads for the final slot only (cleared on reschedule). The plan did not specify this.

**5. [Plan wording] Record command.** The plan's `-k record` also matches replay test names containing "recorded", so those tests are named `committed_*`; `-k record` now selects only the two record tests (and the docstring command works as written).

**6. [Plan wording] `ReminderResult` for a follow-up** is `("completed", [delay_minutes])`; the plan does not define it.

### Issues seen and not explained

- Each in-process Celery worker takes about 10 s to stop (also in 03-02's tests). I did not look for the cause; it is why the tracer test spends about 10 s in teardown.
- `docker compose exec rabbitmq rabbitmqctl list_vhosts` lists only `/` after my runs; I did not inspect why the live `smarthealth` vhost is not listed (it is the live stack's, not created or touched by this plan; plan 03-14 owns broker provisioning).

## Stack changes

None. Nothing was installed (no pip or other package change); no container was started, recreated or restarted; no `tjo-*` container was touched. The time-skipping Temporal environment is an in-process test server (the SDK-managed test server; it is not the Compose Temporal). RabbitMQ and Mongo were used only through throwaway vhosts and databases, all removed. No `.env*` or `infra/secrets/*` contents were read by me; the existing settings machinery loads `.env` when the tests import it.

## Known Stubs

None.

## Threat Flags

None. `temporal_worker.py` opens no port. The reminder contracts carry ids, ISO timestamps and integers only (T-3-31); the recorded histories hold synthetic UUIDs. T-3-30 mitigations are in place: deterministic ids with `REJECT_DUPLICATE`, signal-with-start for reschedule, the slot-specific dedupe key, and the send task's closed-appointment guard from 03-02.

## Notes for downstream plans

- 03-14: the consumer should call `start_reminder(client, appointment_id=..., patient_id=..., slot_start_iso=..., lead_minutes=settings.reminder_leads)` on `appointment.booked`, `reschedule_reminder(...)` on `appointment.rescheduled`, `cancel_reminder(client, appointment_id)` on cancelled/no_show (also `mark_appointment_closed`), and `start_followup(..., delay_minutes=settings.followup_delay_minutes)` on `visit.completed`. The Compose container runs `python -m services.notification.temporal_worker` with the Temporal address and the broker password file; its healthcheck is the heartbeat file `/tmp/notification-temporal-worker.alive` (setting `NOTIFICATION_TEMPORAL_HEARTBEAT_PATH`).
- Reminder dedupe key is `appointment.reminder:{appointment_id}:{slot_epoch_seconds}:{lead_minutes}`; follow-up `visit.followup:{visit_id}`. Neither starts with `FAIL-`.
- A later change to either workflow's command sequence needs a `workflow.patched` id and a new recorded history; keep `reminder_v1.json` and `followup_v1.json`.
- Capacity note for 03-18 (T-3-32): one idle workflow per active appointment, timer-driven, no polling.

## Self-Check: PASSED

- Created files exist (ten files under `services/notification`, `tests/workflows`, `tests/workflows/histories`), checked with `ls` before commit.
- Commits eb175f9, c2c1567 and 4468e27 exist on `gsd/phase-02-scheduling-service-workflow`; `git rev-list --count ca7534f..HEAD` was 3 before this SUMMARY.
