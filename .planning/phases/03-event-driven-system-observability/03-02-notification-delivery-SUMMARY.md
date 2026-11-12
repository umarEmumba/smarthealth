---
phase: 03-event-driven-system-observability
plan: 02
subsystem: notification
tags: [celery, rabbitmq, dead-letter, idempotency, opentelemetry, prometheus]
requires:
  - phase: 03-event-driven-system-observability
    provides: Celery 5.6.3 / kombu 5.6.2 / opentelemetry-instrumentation-celery pins (plan 03-01); notification broker secret file (plan 03-03)
provides:
  - services/notification package: settings, Celery app on RabbitMQ 4.3, the notifications.send task, idempotent fake provider, Mongo delivery log
  - enqueue_send(app, **kwargs) and instrument_publisher() for the Temporal worker (03-06) and the event consumer (03-14)
  - real-subprocess worker-kill proof that a send killed between the provider call and mark_sent reaches the provider once
affects: [03-05, 03-06, 03-14, 03-15, 03-17, 03-18]
tech-stack:
  added: []
  patterns:
    - "Two-layer exactly-once: unique dedupe_key on the delivery log (status machine) AND on the provider's idempotency store (insert_one is the delivery)"
    - "Reject(requeue=False) with the exception class name as reason (never exception text) dead-letters through smarthealth.dlx"
    - "Module-level Celery app resolves the broker URL (password file) lazily at worker_init / enqueue time"
key-files:
  created:
    - services/notification/__init__.py
    - services/notification/config.py
    - services/notification/celery_app.py
    - services/notification/tasks.py
    - services/notification/provider.py
    - services/notification/deliveries.py
    - tests/notification/__init__.py
    - tests/notification/broker.py
    - tests/notification/conftest.py
    - tests/notification/test_tasks.py
    - tests/notification/test_dedupe.py
    - tests/notification/test_dlq.py
  modified: []
key-decisions:
  - "claim_delivery returns ClaimResult(state: Claim, attempts) instead of a bare Claim, because the provider call and the crash switch need the stored attempt number"
  - "declare_topology(app) runs in worker_init: a worker started with -Q declares only the three work queues, so without it the DLX binding and notifications.dlq would not exist and rejected messages would be dropped"
  - "Unexpected exceptions dead-letter (not silently acked) and a PyMongoError is treated as transient (retry), so a Mongo outage cannot lose a message"
requirements-completed: []
status: complete
duration: not measured
completed: 2026-11-11
actuals:
  tokens: 15200
  tasks: 3
  commits: 4
plan_head_before: 268fb3a54cdae7ab0e5312c8ca60139c750d5082
plan_head_after: 3c44daf04a6695b954daef1a8ff0f9cac6a60f28
---

# Phase 3 Plan 02: Notification Delivery Summary

**Celery 5.6.3 on RabbitMQ 4.3 (threads pool, acks_late, classic queues, DLX `smarthealth.dlx`) with one `notifications.send` task that reaches an idempotency-key-honouring fake provider once per dedupe key, including after a real worker subprocess is killed between the provider call and `mark_sent`, retries transient failures with exponential backoff, dead-letters poison messages to `notifications.dlq`, and skips reminders for closed appointments; 19 tests pass against the live RabbitMQ and Mongo.**

REQUIREMENTS.md was not edited and no requirement was ticked (`requirements-completed` is empty by instruction). The plan names EVENT-04, EVENT-05, NOTIF-04 and OBS-02 as the requirements this plan addresses.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 7658e26 | settings, Celery app, task happy path, fake provider, delivery log, throwaway-vhost harness, `test_tasks.py` (6 tests) |
| 2 | 958e440 | `test_dedupe.py` (8 tests), retry/skip/crash-injection logic in `tasks.py`. The dead-letter code was written in this same commit (as in plan 03-04); its tests are Task 3 |
| 3 | 41b80e2 | `test_dlq.py` (5 tests) and the sanitised `Reject` reason found by them |
| follow-up | 3c44daf | retry throwaway-vhost deletion in the test harness |

The tracer feedback gate: `<verify>` for Task 1 passed end to end after the commit (6 passed), so expansion proceeded.

## Verification (measured)

- `.venv/bin/python -m pytest tests/notification/test_tasks.py -q`: 6 passed (plan: at least 5), about 28 s.
- `.venv/bin/python -m pytest tests/notification/test_dedupe.py -q`: 8 passed (plan: at least 6), about 94 s.
- `.venv/bin/python -m pytest tests/notification/test_dlq.py -q`: 5 passed, about 88 s.
- `.venv/bin/python -m pytest tests/notification tests/libs/test_service_isolation.py -q`: **33 passed** (19 notification + the isolation suite), 0 failed, 199 s, `OTEL_SDK_DISABLED` unset. This run was started after the last change to `tasks.py`; the only later edit was the test-harness vhost-deletion retry (`broker.py`), after which `test_tasks.py` was re-run: 6 passed.
- `tests/notification` total: 19 passed (plan: at least 14).
- Acceptance greps: `requeue=False` in tasks.py 2; `fake_crash_enabled` in config.py 1; `worker_enable_remote_control|accept_content` in celery_app.py 2 lines (both settings present).
- No `p3test-` vhost or user remains on the live RabbitMQ and no `p3test*` Mongo database remains (checked after the final runs).
- Semgrep: `semgrep --config p/python --metrics=off services/notification`: 151 rules, 6 files, **0 findings**. Narrower than the plan's `--config auto` (refused with `--metrics=off`, as in 03-01/03-03/03-04). `tests/notification` was not scanned.
- Not run: the plan's full `pytest tests -m "not e2e"` (the orchestrator said it is not required). `ruff` is not installed in `.venv`, so lint and format were not run.

## What the tests prove

- **One trace** (`test_send_is_delivered_once_and_recorded_in_one_trace`): a send published inside span `test.publish` is consumed by a threads-pool worker; the worker span `run/notifications.send` has the publisher's trace id, its parent is `apply_async/notifications.send`, whose parent is `test.publish`. The delivery document is `sent`, `attempts=1`, ids only, with one provider record.
- **Queue routing**: a worker listening only on `notifications.booking` delivers the booking send while the reminder send stays in `notifications.reminder` (depth 1).
- **Real worker kill** (`test_worker_process_killed_between_provider_call_and_mark_sent_sends_once`): a `celery -A services.notification.celery_app worker -P threads -c 1 -Q notifications.booking --without-mingle --without-gossip --without-heartbeat` SUBPROCESS (throwaway vhost and user, 0600 temp password file, test Mongo database, a free metrics port, `NOTIFICATION_FAKE_CRASH_ENABLED=true`) took a `FAIL-CRASH-AFTER-PROVIDER-` send. The provider wrote its call record, the process exited with code **70** via `os._exit` before `mark_sent` and before the ack. Immediately after: one provider record, delivery `sending`, `attempts=1`. RabbitMQ returned the unacknowledged message; an in-process worker (crash switch off) took it: `claim_delivery` returned RETRY, the provider answered from its store (`replayed=True`), the delivery ended `sent` with `attempts=2`, the same `message_id` as the original provider record, `notification_provider_calls.count_documents == 1`, and `notifications_provider_replayed_total` rose by 1. This is a genuine process kill, not a simulation.
- **Deterministic window**: a delivery left `sending` plus an existing provider record ends `sent`, `attempts=2`, `replayed=True`, one provider record.
- **Duplicates**: the same key enqueued twice gives one `sent` delivery and one provider record. Eight enqueues of one key consumed by one subprocess worker plus one in-process worker, and eight threads racing `claim_delivery` plus `deliver` on one key, each give exactly one provider record (and exactly one non-replayed result in the thread race).
- **Backoff**: a `FAIL-TRANSIENT-` key is retried with recorded countdowns `[1.0, 2.0]` (base 1 s, `base * 2**retries`), takes at least 3 s, ends `sent` on attempt 3 with one provider record whose `attempt` is 3; `notifications_retried_total` rose by 2.
- **Closed-appointment guard**: a reminder for an appointment marked closed is `skipped_closed` with zero provider records and `notifications_skipped_total{reason="closed"}` +1; a booking send for the same appointment is delivered.
- **Dead letter**: a `FAIL-PERMANENT-` key (retries limited to 2 in the test) ends `failed`, `attempts=3`, message in `notifications.dlq` with its original kwargs, zero provider records, `notifications_dead_lettered_total{error_type="TransientDeliveryError"}` +1, `notifications_retried_total` +2. A `FAIL-INVALID-` key dead-letters on attempt 1 with no retry. A message with invalid kwargs dead-letters on attempt 1 (label `notification_type="unknown"`, `ValidationError`) and creates no delivery. An unexpected `RuntimeError` from the provider dead-letters instead of being acked and lost. `rabbitmqctl list_queues name arguments` shows `x-dead-letter-exchange=smarthealth.dlx` and routing key `notifications.dlq` on the three work queues and none on the DLQ.
- **Config**: json only, `acks_late`, `reject_on_worker_lost`, prefetch 1, remote control / task events / sent events off; `broker_url` is built lazily from the password file, URL-quotes the password, the password is not in `repr` or `model_dump`, and a missing file raises an error naming the setting without the path.

## Deviations from Plan

**1. [Interface] `claim_delivery` returns `ClaimResult(state, attempts)`** rather than a bare `Claim`. `Claim` is an enum with `NEW`, `RETRY`, `DONE`. Reason: the caller needs the stored attempt number for the provider call and the crash switch. Plans 03-06 and 03-14 only call `enqueue_send`, so they are unaffected.

**2. [Rule 2 - Missing critical functionality] Topology declared at worker start.** `declare_topology(app)` (exchanges, the three work queues, `smarthealth.dlx` and `notifications.dlq`) runs in the `worker_init` hook, and the test fixtures call it too. A worker started with `-Q` declares only the work queues, so the DLQ and its binding would otherwise not exist until plan 03-14's provisioning, and a rejected message would be dropped. Plan 03-14 may still own provisioning (and the non-admin broker user); the declaration is idempotent.

**3. [Rule 2 - Security, T-3-16] `Reject` reason is the exception class name only.** The first DLQ test run showed Celery logging the pydantic `ValidationError` text, which echoes the offending input value. A malformed message could therefore put payload content in the worker log. All dead-letter and retry raises now use `Reject(error_type, requeue=False)` and `raise ... from None`; the dead-letter log line carries type, dedupe key and error type only. The re-run's log was checked: no `input_value` text.

**4. [Rule 2] Failure handling beyond the plan.** A `PyMongoError` (delivery log unreachable) is retried like a transient provider error, and any other unexpected exception is dead-lettered with a metric. Without these Celery would ack the failed message under `acks_late` and the send would be lost.

**5. [Test design] The concurrency test uses one subprocess worker and one in-process worker, not two in-process workers.** Two `start_worker` workers in one process share Celery's global `state.should_terminate`; the first run (two in-process workers) passed its assertions but one worker failed to stop at teardown (`RuntimeError: Worker thread failed to exit`). The subprocess variant is a stronger test of cross-process concurrency; the same-key race is additionally covered directly with eight threads and a barrier.

**6. [Harness] Throwaway-vhost deletion retries.** After one errored run a `p3test-` vhost (holding 8 messages from the concurrency test) remained on the live broker. It was deleted by hand and `throwaway_broker` now retries the delete three times. No later run leaked.

**7. [Plan wording] Task 2/3 code split.** The dead-letter code in `tasks.py` was committed in Task 2 (the module is one function), with Task 3 adding its tests and the sanitisation fix.

### Issues seen and not explained

- In the second full run of `test_dedupe.py` one `FAIL-TRANSIENT-` test stalled: the delivery stayed `failed` at `attempts=2` and the pytest process did not finish for more than 8 minutes, so I interrupted it and cleaned up its vhost and Mongo database by hand (the run's results were discarded). I could not reproduce it: the same test passed on a later isolated run, in the next two full runs, and in four consecutive loops of the transient and deterministic-window tests. I did not find a root cause. A likely area is the ETA retry message and the prefetch of the in-process worker, but that is a guess, not a finding. If it recurs, `-o faulthandler_timeout` output would show where the threads are blocked.
- Each test's in-process worker shutdown can take several seconds, which makes the suite slow (about 3 min for the whole directory).

## Stack changes

None. Nothing was installed; no container was started, recreated or restarted; no `tjo-*` container was touched. Tests used throwaway RabbitMQ vhosts/users and throwaway Mongo databases, all removed. No `.env*` or `infra/secrets/*` contents were read (the tests use random test-only credentials and temp files; `config.py` reads the secret path at runtime only).

## Known Stubs

None. The fake provider is the intended provider for this phase (a real SMS/email provider is out of scope and the docs plan 03-18 must state that it needs an idempotency key for the same guarantee).

## Threat Flags

| Flag | File | Description |
|------|------|-------------|
| threat_flag: network-endpoint | services/notification/celery_app.py | `prometheus_client.start_http_server(settings.metrics_port)` serves `/metrics` (port 9100) from the worker; it exposes counters and labels (notification type, error type) with no ids. Plan 03-15 decides scrape wiring and network exposure |

## Notes for downstream plans

- 03-06 and 03-14 should publish with `enqueue_send(app, notification_type=..., dedupe_key=..., patient_id=..., appointment_id=..., event_id=..., requested_at=...)`; `visit_id` and `event_id` are optional. In publishing processes call `instrument_publisher()` first (idempotent). The module-level `app` resolves the broker URL from the password file on first use.
- Real keys must not start with `FAIL-` (the failure-injection prefix).
- 03-14 must assert that nothing in Compose sets `NOTIFICATION_FAKE_CRASH_ENABLED`, and should provision the broker user and decide whether it keeps configure permission (the worker declares its queues and the DLX topology at start).
- The send-time guard reads `notification_appointment_state`; 03-14's consumer must call `mark_appointment_closed(db, appointment_id, "cancelled" | "no_show")` on those events.

## Self-Check: PASSED

- Created files exist (all twelve under `services/notification` and `tests/notification`), checked with `ls`.
- Commits 7658e26, 958e440, 41b80e2 and 3c44daf exist on `gsd/phase-02-scheduling-service-workflow`; `git rev-list --count 268fb3a..HEAD` was 4 before this SUMMARY.
