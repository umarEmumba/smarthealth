---
phase: 03-event-driven-system-observability
reviewed: 2026-11-12T09:00:00Z
depth: standard
scope: gap-closure (git diff 1291b59..HEAD, plans 03-21..03-26); 03-REVIEW.md (original 20 plans) is untouched
files_reviewed: 28
files_reviewed_list:
  - libs/events/classify.py
  - libs/events/consumer.py
  - libs/db/postgres.py
  - libs/config.py
  - libs/observability/gunicorn_conf.py
  - libs/observability/http_metrics.py
  - services/visit/consumer.py
  - services/visit/downstream.py
  - services/visit/activities/downstream.py
  - services/visit/repository/visit_repo.py
  - services/visit/worker.py
  - services/visit/workflows/reconciler.py
  - services/analytics/consumer.py
  - services/notification/triggers.py
  - services/gateway/app.py
  - scripts/replay_dlq.py
  - scripts/backfill_visit_completed_events.py
  - services/gateway/Dockerfile
  - services/analytics/Dockerfile
  - services/billing/Dockerfile
  - services/identity/Dockerfile
  - services/organization/Dockerfile
  - services/patient/Dockerfile
  - services/provider/Dockerfile
  - services/scheduling/Dockerfile
  - services/visit/Dockerfile
  - infra/docker-compose.yml
  - libs/temporal_policies.py (read for the activity timeout only)
findings:
  critical: 0
  warning: 7
  info: 6
  total: 13
status: issues_found
---

# Phase 3 Gap Closure: Code Review Report

**Reviewed:** 2026-11-12
**Depth:** standard (with call-chain tracing through the consumer base, the visit worker and the Temporal policies)
**Status:** issues_found

## Summary

The gap closure does what it claims on the two blockers. A dead-lettered `visit.completed` is now recoverable three ways (reconciler lost-trigger pass, backfill `stale_queued`, DLQ replay), the shared classifier fixes the SQLSTATE hole of CR-01/WR-02, and the multiprocess wiring for CR-02 is correct as shipped: the variable is exported only inside the API `CMD`, after Alembic, so the Temporal workers and consumers that reuse the image never see it; `on_starting` empties the directory in the master before any fork; `child_exit` removes only live-gauge files; the gateway renders from a `MultiProcessCollector` on the 9100 listener only and still answers 404 on 8000; no dashboard or rule reads `process_*` / `python_*`, which the multiprocess exposition drops. I found no way to double-start a visit (deterministic id, `USE_EXISTING` + `REJECT_DUPLICATE`, conditional marker), the retired sweeper Schedule is not touched, and no workflow or activity name or shape changed.

I found no BLOCKER. The defects are in how the new safety nets behave under the same stress that makes them necessary: the lost-trigger pass is queued behind a pass that can run for minutes inside a 60 s activity; the replay tool's default selection replays exactly the records the summary says must not be replayed, and fans out to every consumer group; the transient retry sleeps on the single poll thread, so one bad message slows every partition; and the "worst case 241 of 297" connection budget is not a bound.

Verified sound (checked, no finding): classifier precedence (an explicit SQLSTATE is authoritative, 23505 / 42P01 are never transient, the fallback only runs without a code); SQLSTATE walk is depth-bounded and cycle-safe and never reads a message; the dry-run default and PHI-safe output of `replay_dlq.py` (`dlq.*` headers removed, key and value never printed, header values pattern-checked, topic allow-list enforced, `murmur2_random` matches the Java partitioner for non-null keys, nothing committed); `_retire` only ever touches entries whose loop `is_closed()`, never a live loop's engine, and cannot shut down a reused descriptor (a closed socket reports `fileno() == -1`, so `os.dup(-1)` raises and is suppressed); the commit retry re-raises a fatal `KafkaError` and abandons a non-fatal one without losing at-least-once.

## Warnings

### WR-01: The lost-trigger pass runs after an unbounded settle pass inside a 60 s activity, and at worker start-up before the worker heartbeat

**File:** `services/visit/activities/downstream.py:283-302`, `services/visit/worker.py:150-154`, `libs/temporal_policies.py:71-81`
**Issue:** `reconcile()` runs `reconcile_unsettled` first (up to 100 visits, two HTTP steps each, billing and scheduling calls) and only then `restart_lost_triggers`. The activity runs under `MAINTENANCE_ACTIVITY`: `start_to_close_timeout` 60 s, 5 attempts, and it does not heartbeat. The settle pass has no overall time budget, and nothing wraps it, so an exception from its initial `list_unsettled` query also skips the lost-trigger pass.
**Failure scenario:** Billing is down or slow, which is exactly when a `visit.completed` trigger was dead-lettered (`TransientExhausted`). Each of the up to 100 unsettled visits burns an HTTP timeout per step, the activity passes 60 s, Temporal times it out and retries, and every retry starts again with the same slow settle pass. `restart_lost_triggers` is never reached, so the lost trigger is not recovered until billing is healthy again. A timed-out async activity with no heartbeat is not cancelled in the worker, so the next attempt also runs concurrently with the previous one (idempotent, but doubled load on a degraded billing service). The same code runs at worker start before the heartbeat task and before `Worker` starts polling: the compose healthcheck (`start_period` 40 s, 6 x 10 s, heartbeat file younger than 30 s) turns unhealthy after about 100 s, and the 100 sequential failing `start_workflow` calls of a flapping Temporal frontend (5 s `START_RPC_TIMEOUT` each, no short-circuit on a systemic error) can alone take 500 s.
**Fix:** Run the cheap, RPC-bounded pass first and isolate the two passes from each other; give each a wall-clock budget and stop the pass on a systemic error.
```python
async def reconcile(self) -> int:
    restarted = 0
    if self._client is not None:
        try:
            restarted = await downstream.restart_lost_triggers(
                self._session_factory, self._client, grace_minutes=self._reconcile_grace)
        except Exception as exc:  # noqa: BLE001
            log.warning("visit.lost_trigger_pass_failed", error=type(exc).__name__)
    settled = await downstream.reconcile_unsettled(...)
    return settled + restarted
```
In `restart_lost_triggers` break out of the loop after N consecutive `RPCError`s (Temporal is down, the remaining visits will fail the same way), and in `worker.main` run the start-up pass as a background task after the heartbeat starts.

### WR-02: `replay_dlq.py` replays poison by default, fans the record out to every consumer group on the topic, and the "WR-06 unaffected" claim is wrong for events a group never handled

**File:** `scripts/replay_dlq.py:161-172` (`matches`), `:209` (limit applies to selected), `:257-294` (`replay`), `:307-328` (`run`), docstring `:18-19`
**Issue:** With only `--group visit-downstream --apply`, `matches()` selects every record whatever its `dlq.error_type`, oldest first, up to `--limit` 100. Nothing refuses error types that a replay cannot fix (`NonRetryableEventError`, `DeserializationError`). The 03-26 SUMMARY records that the live DLQ holds 100+ `NonRetryableEventError` records for deleted harness visits at the head of that topic; the default 100-record window is exactly those records, and the one legitimate `TransientExhausted` record behind them is never reached unless the operator already knows to add `--error-type`. The only protection is an operator note in a SUMMARY. Replay re-produces to the shared source topic (`smarthealth.visit.events`), which the analytics-projection and notification-trigger groups also read, not only the group whose DLQ it is. The docstring says a replay is harmless because "a replay keeps the original event id", but that only holds for a group that already recorded that id. `NotificationTriggers.on_visit_completed` has no staleness guard (WR-06 from the first review is still open) and starts `followup-{visit_id}` immediately when `completed_at + delay` is in the past, so a group that never handled the event (dead-lettered by it, or before its `latest` pin) sends a follow-up for an arbitrarily old visit.
**Failure scenario:** An operator runs `replay_dlq.py --group visit-downstream --apply` to recover one visit. It re-produces the 100 harness records: the visit consumer dead-letters them again (another 100 DLQ records, and the same 100 are selected on the next run, forever), analytics upserts 100 rows for deleted visits, notification-trigger attempts 100 follow-up workflows for deleted patients. Re-running `--apply` re-produces the same records again (no replayed marker, no dedupe by event id).
**Fix:** Make the safe selection the default: skip `NonRetryableEventError` and `DeserializationError` unless `--include-poison` is given, require `--error-type` or `--event-id` or `--key` for `--apply`, dedupe by `dlq.original_topic:partition:offset` within a run, and print the per-error-type counts of what was skipped. Add the staleness guard to `on_visit_completed` (WR-06) so a replay cannot message old patients; correct the docstring sentence.

### WR-03: The transient retry sleeps on the one poll thread, so one stuck message slows every partition of the consumer for up to the full cap

**File:** `libs/events/consumer.py:490-493` (`seek` + `_sleep`), `:495-502`
**Issue:** `_retry_later` seeks the partition back and sleeps `min(30, 0.5 * 2**count)` seconds on the single consumer thread before the next `poll`. The topics have 3 partitions (`infra/kafka/create-topics.sh:20`) and each group runs one consumer process, so every retry cycle also stops the messages of the other two partitions for up to 30 s. A message that the classifier wrongly treats as transient (WR-04) therefore throttles the whole consumer for the 900 s cap, not "one partition" as the module docstring says. The 03-26 summary documents the "one dead letter per 15 minutes per partition" effect but not this cross-partition one.
**Failure scenario:** One `appointment.booked` event raises a deterministic `RuntimeError` in the Temporal client. For 15 minutes the notification-trigger consumer alternates 30 s sleeps with single messages from the other partitions: reminder triggers for unrelated appointments are delayed by minutes, and `event_end_to_end_lag_seconds` and `kafka_consumer_lag` rise on partitions that have nothing wrong.
**Fix:** Pause only the failing partition and keep polling the others; resume when the backoff elapses.
```python
tp = TopicPartition(topic, partition, offset)
self._consumer.seek(tp)
self._consumer.pause([tp])
self._resume_at[(topic, partition)] = _monotonic() + delay   # checked at the top of poll_once
```

### WR-04: Blanket exception families are classified transient, so a deterministic failure costs a full 15 minute cap per message

**File:** `libs/events/classify.py:128-149`, `services/notification/triggers.py:175`
**Issue:** `is_transient_temporal` returns True for every `RuntimeError` (including `RecursionError` and `NotImplementedError`, which subclass it); `is_transient_broker` returns True for every `KombuError`, `AMQPError` and `PyMongoError`; `is_transient_network` for every `OSError` (`PermissionError`, `FileNotFoundError`). The cap bounds the damage (previously unbounded), but each such message now blocks for the whole of 10 attempts and 900 s before it is dead-lettered, and every following deterministic failure costs another 900 s because each message gets its own state.
**Failure scenario:** After a RabbitMQ topology change the declared queue arguments no longer match, so every publish raises `amqp.exceptions.PreconditionFailed` (a channel error, deterministic) or `kombu.exceptions.EncodeError` on a payload the serializer rejects. Ten affected `appointment.booked` events therefore stall the notification-trigger consumer for 150 minutes, each ending in a `TransientExhausted` record that suggests an outage which never existed.
**Fix:** Narrow the families to connection-level types and let the rest go through the bounded 3-attempt path: `kombu.exceptions.OperationalError`, `amqp.exceptions.ConnectionError` / `RecoverableConnectionError`, `pymongo.errors.AutoReconnect` / `NetworkTimeout` / `ServerSelectionTimeoutError`, `ConnectionError` / `TimeoutError` instead of `OSError`, and only the SDK's closed-client `RuntimeError` message class rather than all of `RuntimeError`.

### WR-05: Pool settings have no bounds, and the halved API pool is untested against the burst profile

**File:** `libs/config.py:74-75`, `libs/db/postgres.py:111-116`, `infra/docker-compose.yml:43-44`
**Issue:** `db_pool_size: int` and `db_max_overflow: int` accept any integer. In SQLAlchemy `pool_size=0` or `max_overflow=-1` mean "no limit", so a mistyped `DB_POOL_SIZE=0` silently removes the cap that the whole connection budget depends on. The static budget test reads the Compose strings, not the validated setting. Separately, the API pool dropped from 5+10 to 5+5 per worker (10 connections for each of 16 API workers) while `pool_timeout` stays at 30 s; the sustained, burst and contention load profiles are explicitly deferred, so nothing shows that 10 connections per worker survive the booking peak the phase is built for. A request that waits for a connection for 30 s ends as a 500 and, in the consumers, as `sqlalchemy.exc.TimeoutError`, which the classifier now treats as transient.
**Failure scenario:** Under the `burst` profile the scheduling API's two workers saturate their 10 connections, later requests queue for 30 s and fail; the p95/p99 NFR thresholds the plan set out to measure cannot be met by pool starvation rather than by the service.
**Fix:** `db_pool_size: int = Field(5, ge=1)`, `db_max_overflow: int = Field(10, ge=0)`, set an explicit short `pool_timeout` (for example 5 s, fail fast) and run the deferred `burst` profile against the 5+5 setting before declaring the budget closed.

### WR-06: "Worst case 241 of 297" is not a bound; the 53300 failure can still occur

**File:** `infra/docker-compose.yml:29-43`, `libs/db/postgres.py:84-96`
**Issue:** Three terms of the budget are not worst cases. Temporal (40) is an idle observation, no `SQL_MAX_CONNS` / `SQL_VIS_MAX_CONNS` is set in Compose, and the auto-setup single process runs frontend, history, matching and worker services, each with its own pool per store (the SUMMARY itself says the upper bound is "not established"). Debezium's 8 is assumed. Anything run from the host against the same Postgres is not in the sum at all: pytest, `services.analytics.rebuild`, the backfill and the e2e harness use the code defaults 5+10 per (URL, loop), and with eight service databases that is up to 120 connections on top. Also `_retire` drains only checked-in records; a connection that was checked out when its loop closed stays in neither queue and is not closed until garbage collection. The measured peaks (105, 108) show the suites passed once; they do not bound the next run.
**Failure scenario:** A booking burst raises Temporal's pools towards their 20-per-store limits while a developer runs the e2e suite from the host: 241 budgeted + roughly 80 more Temporal + 120 host connections exceeds 297, new connections fail with 53300, and the consumers handle it correctly (transient) but the APIs return 500.
**Fix:** Set `SQL_MAX_CONNS` and `SQL_VIS_MAX_CONNS` (and `SQL_MAX_IDLE_CONNS`) on the `temporal` service so the term is derivable, set `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` small by default for host processes (conftest and scripts), add the host-side term to the budget comment, and reword the comment from "worst case" to what it derives.

### WR-07: The lost-trigger pass has no age ceiling, no kill switch and a head-of-queue starvation mode

**File:** `services/visit/downstream.py:195-243`, `services/visit/repository/visit_repo.py:244-275`
**Issue:** `list_lost_triggers` selects every `completed AND downstream_started_at IS NULL AND completed_at < now() - 10 min`, oldest first, 100 per pass, for all clinics, every 60 s, with no maximum age and no setting to disable it. The backfill script's own CAUTION still tells operators not to start downstream work for throw-away visits, but the reconciler now does that automatically. A visit whose restart raises on every pass (the per-visit `except` only logs) stays at the head of the oldest-first order; 100 such visits permanently starve every newer lost trigger.
**Failure scenario:** The dev database is restored from a snapshot or holds the harness's completed visits with a NULL marker (the state the phase's own kill tests create). Within a minute of the next worker start, up to 100 billing finalizations and appointment completions are started for visits nobody asked to be processed. If `ensure_downstream_started` fails for the oldest 100 (for example a malformed visit row), no newer visit is ever restarted.
**Fix:** Add `VISIT_RECONCILE_LOST_TRIGGER_MAX_AGE_HOURS` (default bounded) and an enable flag, and rotate the window: order by `completed_at` but exclude ids that failed in this process within the last N passes (an in-memory failure set), or advance a cursor `completed_at > :last_seen` after a failure.

## Info

### IN-01: `sqlstate_of` follows `__context__`, so an implicit chain can reclassify an unrelated exception

**File:** `libs/events/classify.py:94-100`
**Issue:** The walk adds `__context__` (the implicit "during handling of" chain) next to `orig` and `__cause__`, and the first 5-character code in breadth-first order decides. An exception that is raised while a 40P01 or 53300 is being handled inherits that code, so a deterministic bug that happens to surface inside an `except DBAPIError` block is retried as an outage; the reverse is possible for a connection error raised inside an `except` for a 23505 (the connection error is then judged non-transient through the context's code).
**Fix:** Walk only `orig` and `__cause__` (explicit chaining); keep `__context__` out.

### IN-02: Per-message transient state is cleared on only some paths

**File:** `libs/events/consumer.py:227`, `:324-328`, `:366`
**Issue:** `_transient` entries are removed on success (line 366) and in `_dead_letter`, but not on the IGNORED path (lines 324-328). A message that first failed at decode (registry down) and then decodes to an event type the group has no handler for leaves its entry behind for the life of the process; entries for partitions that were revoked in a rebalance are never removed either, and the whole map is lost on restart, so a consumer that crashes or restarts more often than every 15 minutes never exhausts the cap. A registry outage of 15 minutes also dead-letters, as `TransientExhausted` with `event_type="unknown"`, a message this group would have ignored.
**Fix:** Pop the key on the IGNORED path, clear entries for a partition in an `on_revoke` callback, and ignore-check the `event_type` header before treating a decode failure as retriable.

### IN-03: The engine registry is mutated without a lock

**File:** `libs/db/postgres.py:104-107`
**Issue:** Two threads that each run a loop (the consumer thread and a test's loop, the case the module docstring names) can both see the same closed loop's entry in their `list(_registry.items())` snapshot; the second `del _registry[key]` raises `KeyError`, and `_retire` can run twice on one engine. Production has one loop per process, so this is a test-harness hazard.
**Fix:** `_registry.pop(key, None)` and retire only if it returned the entry; guard `_entry_for` with a `threading.Lock`.

### IN-04: The backfill can fail after partially applying, and one failing visit aborts the restart loop

**File:** `scripts/backfill_visit_completed_events.py:174-193`, `:195-226`
**Issue:** `_run` decides whether to connect to Temporal from a dry-run preview, then runs the real pass; a visit that crosses the stale threshold in between has no client and the loop raises `RuntimeError` after the outbox rows of the same run were already committed (exit 1, counts never printed). The restart loop has no per-visit `try`, so one Temporal error abandons the remaining stale visits.
**Fix:** Connect to Temporal whenever `--apply` is set (lazily, as `connect_client` already supports), and catch per visit like `restart_lost_triggers` does.

### IN-05: Smaller `replay_dlq.py` behaviours that mislead an operator

**File:** `scripts/replay_dlq.py:192-194`, `:289-294`, `:320-325`, `:371`
**Issue:** A mistyped `--dlq-topic` is reported as `scanned=0 ... replayed=0`, exit 0, indistinguishable from an empty DLQ. If a delivery fails after some records were confirmed, the exception message names only the error and the run prints no counts, so the operator cannot tell what was replayed. A refused record exits 0. The allow-list is global (every catalogue topic), so a visit DLQ record whose `dlq.original_topic` header names `smarthealth.appointment.events` is replayed there; `--allow-topic` accepts any topic including a DLQ topic itself.
**Fix:** Exit non-zero on a missing topic and on `refused > 0`, print the delivered count in the failure path, and allow only the topic a group actually consumes (`DLQ_TOPICS` to source topic map) unless `--allow-topic` is given.

### IN-06: Classification gaps at the edges of the transient set

**File:** `libs/events/classify.py:59`, `:119-125`
**Issue:** SQLSTATE `25006` (read-only transaction after a primary failover) and class `58` (system error, I/O) are not transient, while asyncpg's client-side `InterfaceError` is, including its deterministic variants (for example "number of query arguments cannot exceed 32767"), and `08P01` / `53400` are deterministic yet transient by class. All are bounded now, so the effect is a delayed dead letter or an early one, not a loss.
**Fix:** Add `25006` to the transient set; narrow the `InterfaceError` fallback to connection-state messages by class (`ConnectionDoesNotExistError`, `ConnectionFailureError`) rather than the base class.

---

_Reviewed: 2026-11-12_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
