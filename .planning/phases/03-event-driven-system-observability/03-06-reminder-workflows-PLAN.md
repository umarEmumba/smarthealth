---
phase: 03-event-driven-system-observability
plan: 06
type: execute
wave: 3
depends_on: ["03-02"]
files_modified:
  - libs/temporal_policies.py
  - services/notification/config.py
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
autonomous: true
requirements: [NOTIF-02, NOTIF-03, EVENT-04]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Appointment reminders are scheduled with Temporal durable timers, not Celery ETA or beat; when a timer fires, the workflow enqueues the same Celery send task used by event-driven notifications (D-02, D-09)"
    - "A reminder fires at each configured lead before the slot, a cancel signal stops it, a reschedule signal moves it to the new slot, and a lead already in the past is skipped (NOTIF-02)"
    - "A follow-up reminder workflow fires once after a fixed delay from visit completion (NOTIF-03)"
    - "Starts are idempotent: deterministic ids reminder-{appointment_id} / followup-{visit_id}; a duplicate start attaches or is refused; reschedule uses signal-with-start (research Pattern 7)"
    - "Recorded histories for both workflows replay cleanly; activity and workflow names are stable from day one"
    - "No existing workflow changes shape; the booking saga does not start reminders (the notification consumer does, plan 03-14)"
  artifacts:
    - path: services/notification/workflows/reminder.py
      provides: "ReminderWorkflow (signals cancel, reschedule), FollowUpReminderWorkflow"
      contains: "wait_condition"
    - path: services/notification/activities.py
      provides: "NotificationActivities.enqueue (activity name notification.enqueue) -> enqueue_send in a thread"
      contains: "notification.enqueue"
    - path: services/notification/reminders.py
      provides: "start_reminder, reschedule_reminder, cancel_reminder, start_followup client helpers"
    - path: tests/workflows/histories/reminder_v1.json
      provides: "recorded ReminderWorkflow history"
  key_links:
    - from: services/notification/activities.py
      to: services/notification/tasks.py
      via: "enqueue_send(app, ...) published from asyncio.to_thread"
      pattern: "enqueue_send"
    - from: services/notification/reminders.py
      to: services/notification/workflows/reminder.py
      via: "start_workflow with id reminder-{appointment_id}, REJECT_DUPLICATE / ALLOW_DUPLICATE with start_signal"
      pattern: "start_signal"
---

<objective>
Schedule appointment reminders and post-visit follow-ups with Temporal durable timers that hand the actual send to the Celery task from plan 03-02.

Purpose: NOTIF-02 (reminders scheduled and delivered ahead) and NOTIF-03 (visit completion schedules a follow-up) under D-02 (Temporal timers) and D-09 (Celery delivery, one send task). Plan 03-14's notification consumer starts, signals and cancels these workflows from events.

Output: task queue `notification-tq`, workflow contracts, `ReminderWorkflow`, `FollowUpReminderWorkflow`, the `notification.enqueue` activity, client helpers, the `notification-temporal-worker` entrypoint (its Compose container is added in plan 03-14), time-skipping tests and recorded histories.

Decisions honoured: D-02, D-09 revised (reminder dedupe key includes the slot so a rescheduled slot gets its own reminders; the send task refuses reminders for closed appointments), research Pattern 7 (signal-with-start, deterministic ids, spike-verified policies), Pitfall 11 (compute slot times from `env.get_current_time()` per test). Claude's discretion: lead times `1440,120` minutes and follow-up delay 4320 minutes as settings (assumption A7), passed as workflow inputs and never read inside the workflow. Reminder dedupe key `appointment.reminder:{appointment_id}:{slot_epoch_seconds}:{lead_minutes}`; follow-up `visit.followup:{visit_id}`.

Live stack note: tests use the Temporal time-skipping environment, the live RabbitMQ through the throwaway vhost helper from plan 03-02 (`tests/notification/broker.py`) and the live Mongo; no Compose change and no container restart in this plan. Wave 3 coupling: plan 03-05 runs alongside and recreates Postgres, rebuilds the application images and starts Kafka Connect; this plan uses none of those (the time-skipping environment is an in-process test server, not the Compose Temporal), and 03-05 does not change the `rabbitmq` or `mongo` service definitions. If RabbitMQ or Mongo is nevertheless briefly unreachable, re-run the failed test once after `scripts/compose_healthcheck.sh` passes.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-02-SUMMARY.md
@libs/temporal.py
@libs/temporal_policies.py
@services/scheduling/workflows/booking.py
@services/visit/worker.py
@services/notification/tasks.py
@tests/temporal_fixtures.py
@tests/workflows/test_replay_booking_patches.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `NOTIFICATION_TASK_QUEUE = "notification-tq"` | constant | `libs/temporal_policies.py` |
| `ReminderInput(appointment_id, patient_id, slot_start_iso, lead_minutes: list[int])`, `FollowUpInput(visit_id, appointment_id, patient_id, completed_at_iso, delay_minutes)`, `EnqueueRequest(notification_type, dedupe_key, patient_id, appointment_id, visit_id, requested_at_iso)`, `ReminderResult(status, sent_leads)` | dataclasses | `services/notification/contracts.py` (pure: no settings, no I/O) |
| `ReminderWorkflow` | workflow | name `ReminderWorkflow`; signals `cancel`, `reschedule(new_slot_iso)`; results `completed`, `cancelled`, `skipped_past` |
| `FollowUpReminderWorkflow` | workflow | name `FollowUpReminderWorkflow` |
| `NotificationActivities.enqueue` | activity | name `notification.enqueue` (stable forever); runs `enqueue_send` in `asyncio.to_thread` |
| `start_reminder`, `reschedule_reminder`, `cancel_reminder`, `start_followup` | functions | `services/notification/reminders.py`; ids `reminder-{appointment_id}`, `followup-{visit_id}` |
| `services/notification/temporal_worker.py` | process | `python -m services.notification.temporal_worker`; tracer `smarthealth-notification-temporal-worker`; heartbeat `/tmp/notification-temporal-worker.alive` |
| settings | config | `NOTIFICATION_REMINDER_LEADS_MINUTES` (`1440,120`), `NOTIFICATION_FOLLOWUP_DELAY_MINUTES` (4320), `NOTIFICATION_TEMPORAL_HEARTBEAT_PATH` |
| histories | tests | `tests/workflows/histories/reminder_v1.json`, `followup_v1.json` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — a reminder timer fires in the time-skipping environment and the real activity's Celery send is delivered and recorded</name>
  <files>libs/temporal_policies.py, services/notification/config.py, services/notification/contracts.py, services/notification/workflows/__init__.py, services/notification/workflows/reminder.py, services/notification/activities.py, tests/workflows/test_reminder_workflow.py</files>
  <behavior>
    - `ReminderWorkflow` started with a slot 3 hours ahead of `env.get_current_time()` and one 120-minute lead completes with status `completed` and `sent_leads == [120]` after the environment skips time
    - The activity published `notifications.send` with type `appointment.reminder` and dedupe key `appointment.reminder:{appointment_id}:{slot_epoch}:120`; an in-process Celery worker on a throwaway vhost delivered it and `notification_deliveries` shows `sent` for that key
    - The workflow module imports only `temporalio`, the standard library and `services.notification.contracts` / `libs.temporal_policies` (sandbox-safe; the test runs with the default sandboxed runner)
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 7, Pitfall 11)
    - services/scheduling/workflows/booking.py (sandbox import style: `with workflow.unsafe.imports_passed_through()`, activity option presets)
    - libs/temporal_policies.py (task queue constants and activity presets)
    - services/notification/tasks.py and celery_app.py (enqueue_send, QUEUE_FOR_TYPE)
    - tests/temporal_fixtures.py and tests/workflows/conftest.py (time-skipping environment and test worker fixtures)
    - tests/notification/broker.py and tests/notification/conftest.py (throwaway vhost, in-process worker)
  </read_first>
  <action>
    libs/temporal_policies.py: add `NOTIFICATION_TASK_QUEUE = "notification-tq"` next to the existing queue constants.

    contracts.py: the dataclasses in the artifacts table (plain strings and ints only).

    workflows/reminder.py: `ReminderWorkflow.run(inp)` keeps `self._cancelled` and `self._new_slot_iso`; for the current slot it walks `lead_minutes` from largest to smallest; for each, `fire_at = slot - lead`; if `fire_at <= workflow.now()` record it as skipped; else `await workflow.wait_condition(lambda: self._cancelled or self._new_slot_iso is not None, timeout=fire_at - workflow.now())` and treat the timeout as "fire": execute activity `notification.enqueue` with an `EnqueueRequest` (requested_at from `workflow.now()`), using an existing forward activity preset from `libs.temporal_policies`; after a signal, a cancel returns `ReminderResult("cancelled", sent)`, a reschedule replaces the slot and restarts the walk for the new slot. When every lead was skipped as past, return `skipped_past`; otherwise `completed`. `@workflow.signal(name="cancel")` and `@workflow.signal(name="reschedule")` set the flags. `FollowUpReminderWorkflow.run(inp)`: `fire_at = completed_at + delay`; sleep with `workflow.sleep` when in the future; then one `notification.enqueue` with type `visit.followup` and key `visit.followup:{visit_id}`.

    activities.py: `NotificationActivities(celery_app)` with `@activity.defn(name="notification.enqueue") async def enqueue(self, req)` -> `await asyncio.to_thread(enqueue_send, self._app, notification_type=..., dedupe_key=..., patient_id=..., appointment_id=..., visit_id=..., requested_at=...)`; it raises on broker errors so Temporal retries.

    config.py: the three settings from the artifacts table (leads parsed from a comma list, all positive).

    Test (marker `integration`): time-skipping environment, a Worker on `NOTIFICATION_TASK_QUEUE` with both workflows and real `NotificationActivities` whose Celery app points at a throwaway vhost, an in-process Celery worker on that vhost, assertions above. Use a slot computed from `env.get_current_time()`.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, the workflow fails sandbox validation, the result is not `completed`, or no `sent` delivery exists for the reminder key.</fails_when>
  </verify>
  <acceptance_criteria>
    - the tracer test passes
    - `grep -c "notification-tq" libs/temporal_policies.py` is 1
    - `grep -c "notification.enqueue" services/notification/activities.py` is at least 1
  </acceptance_criteria>
  <done>A Temporal timer fires a reminder that is delivered through the Celery send task.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Cancel, reschedule, past leads, follow-up and idempotent client helpers</name>
  <files>services/notification/workflows/reminder.py, services/notification/reminders.py, tests/workflows/test_reminder_workflow.py</files>
  <behavior>
    - Leads `[1440, 120]` with a slot 2 days ahead: two activity calls, largest lead first, keys differ by lead
    - `cancel` before the first lead fires: result `cancelled`, zero activity calls; `cancel_reminder` on an unknown or closed id returns normally
    - `reschedule(new_slot)` before the lead: the activity fires once with the NEW slot's epoch in the key and never for the old slot
    - Slot 30 minutes ahead with lead 120: result `skipped_past`, no activity call
    - `start_reminder` twice while running attaches to the same run (same run id); after the run closed, a second `start_reminder` is refused and reported as already started (False), never raising to the caller
    - `reschedule_reminder` on a closed reminder (all leads sent) starts a new run for the new slot through signal-with-start with ALLOW_DUPLICATE
    - `FollowUpReminderWorkflow` fires once after the delay; `start_followup` twice starts one run
  </behavior>
  <read_first>
    - services/notification/workflows/reminder.py (Task 1)
    - libs/temporal.py (`start_once` semantics, `START_RPC_TIMEOUT`)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 7 "Spike results")
  </read_first>
  <action>
    reminders.py: `start_reminder(client, *, appointment_id, patient_id, slot_start_iso, lead_minutes) -> bool` (id `reminder-{appointment_id}`, USE_EXISTING + REJECT_DUPLICATE, `WorkflowAlreadyStartedError` -> False, mirroring `start_once`); `reschedule_reminder(client, *, ..., new_slot_iso, lead_minutes)` (`start_workflow` with the new input, `start_signal="reschedule"`, `start_signal_args=[new_slot_iso]`, USE_EXISTING + ALLOW_DUPLICATE); `cancel_reminder(client, appointment_id)` (signal `cancel`; an RPC NOT_FOUND or a closed workflow is ignored and logged at info); `start_followup(client, *, visit_id, appointment_id, patient_id, completed_at_iso, delay_minutes) -> bool` (id `followup-{visit_id}`, REJECT_DUPLICATE). All pass `task_queue=NOTIFICATION_TASK_QUEUE` and `rpc_timeout=START_RPC_TIMEOUT`.

    Tests: the behaviors above in the time-skipping environment with a recording fake activity registered under the same name `notification.enqueue` (no broker needed), one environment per test or slot times from `env.get_current_time()` (Pitfall 11).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -x -q</automated>
    <fails_when>pytest exits non-zero, a cancelled reminder fires, a rescheduled reminder fires for the old slot, or a duplicate start raises to the caller.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -q` exits 0 with at least 8 passed
    - `grep -c "start_signal" services/notification/reminders.py` is at least 1
  </acceptance_criteria>
  <done>Reminders and follow-ups behave correctly under cancel, reschedule, past leads and duplicate starts.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Recorded histories, replay test, and the notification Temporal worker entrypoint</name>
  <files>services/notification/temporal_worker.py, tests/workflows/test_replay_reminder.py, tests/workflows/histories/reminder_v1.json, tests/workflows/histories/followup_v1.json</files>
  <behavior>
    - With `SMARTHEALTH_RECORD_HISTORIES=1`, the record tests write `reminder_v1.json` (a reminder with two leads, one reschedule signal, one fired lead after it) and `followup_v1.json`; without the flag they are skipped
    - `Replayer` replays both histories against the current workflow code with no non-determinism error
    - `services.notification.temporal_worker` builds a Worker on `notification-tq` with both workflows and the `notification.enqueue` activity (a test calls its `build_worker(client, celery_app)` against the time-skipping client and runs one reminder through it), touches its heartbeat file, and instruments Celery publishing so the enqueue joins the workflow's trace
  </behavior>
  <read_first>
    - tests/workflows/test_replay_booking_patches.py (record/replay convention, `SMARTHEALTH_RECORD_HISTORIES`, history file layout)
    - services/visit/worker.py (connect-with-retry, heartbeat task, signal handling, exit codes)
    - services/notification/celery_app.py (`instrument_publisher`)
  </read_first>
  <action>
    temporal_worker.py: `build_worker(client, celery_app) -> Worker` and `main()` mirroring `services/visit/worker.py` (configure logging, `configure_tracer_provider("smarthealth-notification-temporal-worker")`, `instrument_publisher()`, eager Temporal connection with bounded retries through `libs.temporal.connect_client(lazy=False)`, heartbeat every 10 s to `NOTIFICATION_TEMPORAL_HEARTBEAT_PATH`, clean shutdown on SIGTERM).

    test_replay_reminder.py: record tests gated by the env flag that write the two JSON histories from real time-skipping runs, and replay tests that load them with `WorkflowHistory.from_json` and run `Replayer(workflows=[ReminderWorkflow, FollowUpReminderWorkflow]).replay_workflow(...)`. Record once: `SMARTHEALTH_RECORD_HISTORIES=1 .venv/bin/python -m pytest tests/workflows/test_replay_reminder.py -k record -x -q`, then commit the JSON files. Add a module note: any later change to either workflow's command sequence needs a `workflow.patched` id and a new recorded history, as with the booking patches.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/workflows/test_replay_reminder.py tests/workflows/test_reminder_workflow.py -x -q && .venv/bin/python -m pytest tests/workflows -q</automated>
    <fails_when>Either pytest run exits non-zero, a replay raises a non-determinism error, or a history file is missing.</fails_when>
  </verify>
  <acceptance_criteria>
    - `test -s tests/workflows/histories/reminder_v1.json && test -s tests/workflows/histories/followup_v1.json` exits 0
    - `.venv/bin/python -m pytest tests/workflows/test_replay_reminder.py -q` exits 0 with at least 2 passed (replays) and the record tests skipped
    - `.venv/bin/python -m pytest tests/workflows -q` exits 0 (all existing replays still pass)
  </acceptance_criteria>
  <done>Both workflows have recorded, replayable histories and a worker entrypoint ready for Compose.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| notification consumer -> Temporal (start/signal) | event-driven control of long-lived timers |
| Temporal activity -> RabbitMQ | a fired timer publishes a send task |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-30 | Tampering | duplicate or late reminders | medium | mitigate | Deterministic ids with REJECT_DUPLICATE, signal-with-start for reschedule, slot-specific dedupe key, and the send task's closed-appointment guard (plan 03-02) |
| T-3-31 | Information Disclosure | workflow inputs and histories | medium | mitigate | Contracts carry ids, ISO timestamps and integers only; recorded histories are synthetic ids; no PHI in Temporal history |
| T-3-32 | Denial of Service | many long-lived reminder workflows | low | accept | One workflow per active appointment, idle on a timer (no polling); Temporal is built for this; documented capacity note in plan 03-18 |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/workflows -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/notification`; record status in the SUMMARY
</verification>

<success_criteria>
- Reminders and follow-ups are Temporal timers that enqueue the shared Celery send
- Cancel, reschedule, past leads and duplicate starts are correct and tested; histories replay
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-06-SUMMARY.md` when done
</output>
