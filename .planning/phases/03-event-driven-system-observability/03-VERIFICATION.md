---
phase: 03-event-driven-system-observability
verified: 2026-11-12T10:00:00Z
status: passed
score: 5/5 roadmap success criteria verified (SC2 VERIFIED with a recorded, user-scoped caveat; SC1, SC3, SC4, SC5 VERIFIED); 20/20 requirements accounted for (17 satisfied, 2 satisfied with caveat [NOTIF-02, NOTIF-03], 1 accepted deviation [EVENT-04], 0 partial, 0 orphaned)
covered_files:
  - ".planning/phases/03-event-driven-system-observability/03-01-event-foundation-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-01-event-foundation-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-02-notification-delivery-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-02-notification-delivery-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-03-event-schemas-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-03-event-schemas-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-04-consumer-base-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-04-consumer-base-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-05-cdc-pipeline-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-05-cdc-pipeline-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-06-reminder-workflows-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-06-reminder-workflows-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-07-scheduling-producer-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-07-scheduling-producer-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-08-patient-provider-producer-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-08-patient-provider-producer-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-09-visit-outbox-downstream-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-09-visit-outbox-downstream-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-10-analytics-projections-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-10-analytics-projections-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-11-commit-kill-proofs-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-11-commit-kill-proofs-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-12-visit-sweeper-retirement-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-12-visit-sweeper-retirement-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-13-analytics-api-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-13-analytics-api-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-14-notification-trigger-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-14-notification-trigger-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-15-metrics-scrape-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-15-metrics-scrape-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-16-dashboards-nfr-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-17-e2e-guarantees-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-17-e2e-guarantees-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-18-docs-closeout-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-18-docs-closeout-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-19-dashboard-closeout-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-19-dashboard-closeout-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-20-phase-gate-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-21-consumer-reliability-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-22-visit-trigger-recovery-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-22-visit-trigger-recovery-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-23-connection-budget-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-23-connection-budget-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-24-multiprocess-metrics-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-25-multiprocess-rollout-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-25-multiprocess-rollout-SUMMARY.md"
  - ".planning/phases/03-event-driven-system-observability/03-26-gap-closeout-PLAN.md"
  - ".planning/phases/03-event-driven-system-observability/03-26-gap-closeout-SUMMARY.md"
  - "libs/db/postgres.py"
  - "libs/events/classify.py"
  - "libs/events/consumer.py"
  - "libs/observability/gunicorn_conf.py"
  - "libs/observability/http_metrics.py"
  - "scripts/backfill_visit_completed_events.py"
  - "scripts/replay_dlq.py"
  - "services/visit/activities/downstream.py"
  - "services/visit/downstream.py"
  - "services/visit/repository/visit_repo.py"
  - "services/visit/worker.py"
covered_digest: "v2:sha256:eda27ddf6ac8c2d05a0f66c5b29cf143c1309e824f9a4f03a484e29344a0140c"
behavior_unverified: 0
overrides_applied: 1
overrides:
  - must_have: "SC2 / EVENT-05 / NOTIF-02 reminders are delivered with no silent loss: a reschedule signal arriving while the last lead's enqueue activity runs is not dropped (first review WR-01)"
    reason: "User decision D-14 (2026-11-11) put the reminder reschedule-signal race out of scope for gap closure and recorded it as a follow-up (needs workflow.patched plus a recorded-history replay test). The window is one activity execution at the final lead; every other reschedule path is covered. Recorded in 03-CONTEXT.md D-14 and in the 03-26 SUMMARY follow-ups. Orchestrator: confirm this override is acceptable, or downgrade SC2 to PARTIAL and status to human_needed."
    accepted_by: "user (D-14 in 03-CONTEXT.md; recorded by verifier, not newly asked)"
    accepted_at: "2026-11-11T00:00:00Z"
re_verification:
  previous_status: gaps_found
  previous_score: "3/5 roadmap success criteria (SC2, SC4 PARTIAL); 18/20 requirements outright"
  gaps_closed:
    - "CR-01: a dead-lettered or dropped visit.completed is recoverable and a transient DB/broker/Temporal/registry failure is retried, bounded, then dead-lettered as TransientExhausted"
    - "CR-02: HTTP request/latency series correct under gunicorn -w 2 (Prometheus multiprocess mode in all nine APIs)"
    - "WR-05 (connection budget; closed-loop engines retired; measured peak 105)"
    - "WR-02/WR-03/WR-07/IN-01 of the first review (shared classifier, transient cap, commit retry, single classifier)"
    - "Gap-review WR-01, WR-02, WR-07 (quick task 261112-8r6: lost-trigger pass first under its own budget, age ceiling plus kill switch plus per-visit backoff, replay --apply needs a selector and skips poison)"
  gaps_remaining: []
  regressions: []
---

# Phase 3: Event-Driven System + Observability Verification Report (re-verification after gap closure)

**Phase Goal:** Every state change reliably becomes a durable, replayable event with no dual-write gaps, background jobs run independently of user-facing flows with guaranteed at-least-once delivery and no silent loss, operators can see the five core operational metrics derived from the event stream, and every critical flow is traceable end-to-end and observable via metrics/logs/dashboards, with the Architecture & Decision dashboard fully closed out.
**Verified:** 2026-11-12
**Status:** passed (one verifier judgement call on SC2, flagged below)
**Re-verification:** Yes, after gap closure plans 03-21..03-26 and quick task 261112-8r6

## Previous verification (gaps_found, 2026-11-11)

The first verification found two blockers and scored 3/5 (SC2 and SC4 PARTIAL), 18/20 requirements outright (EVENT-05 and OBS-01 PARTIAL). What changed since:

| Previous gap | What changed | Re-verified how |
|---|---|---|
| CR-01 silent loss of a dead-lettered `visit.completed`; consumers dead-lettering on asyncpg 53300/57P03/40P01 | `libs/events/classify.py` (one classifier, SQLSTATE classes 08/53/57/40, shared by visit, analytics, notification consumers); transient cap -> `TransientExhausted` in `libs/events/consumer.py`; visit-reconciler lost-trigger pass (`services/visit/downstream.py` `restart_lost_triggers`); backfill `stale_queued`; `scripts/replay_dlq.py` | read the code, ran tests, live Temporal and docs (below) |
| CR-02 wrong HTTP metrics under gunicorn `-w 2` | Prometheus multiprocess mode, `libs/observability/gunicorn_conf.py`, `MultiProcessCollector` in `http_metrics.py`, gateway `/metrics` rebuilt from the multiprocess registry | live Prometheus `resets()` query, live container env |
| WR-05 connection budget | `libs/db/postgres.py` retires closed-loop engines; pool settings; `test_compose_connection_budget`; peak 304 -> 105 | test run, sample file |
| Docs / dashboard claimed an outage is never dead-lettered | docs 10.2/10.4/10.7/10.8/10.9 and three dashboard sentences (plus the fourth in 261112-8r6) rewritten, guard tests added | `tests/docs` run |

## Verdict

Both blockers are closed with code, test and live evidence, and I could not falsify either closure. The ROADMAP contract holds. The one judgement call is SC2: the first-review WR-01 reminder reschedule-signal race remains open by user decision D-14. I rated SC2 VERIFIED with a recorded caveat rather than PARTIAL, because (a) the user explicitly scoped it out and recorded it, (b) the loss window is a single activity execution at the final reminder lead, and (c) the SC2 text (asynchronous, never blocking, delivered by Celery/RabbitMQ, retried without double-send, poison to DLQ) is demonstrably met. The orchestrator may legitimately choose to treat it as PARTIAL; the frontmatter override records the basis and what to change.

## Closure checks by item

### 1. CR-01 closed (VERIFIED)

| Check | Evidence |
|---|---|
| Classifier covers asyncpg `TooManyConnectionsError` / `CannotConnectNowError` / `DeadlockDetectedError` and SQLSTATE classes 08/53/57/40 | `libs/events/classify.py`: `TRANSIENT_SQLSTATE_CLASSES = ("08","53","57","40")`, `sqlstate_of` walks `sqlstate`/`pgcode` on the exception, `orig`, `__cause__`/`__context__` (depth-bounded, cycle-safe); explicit SQLSTATE is authoritative (23505/42P01 stay non-transient); SQLAlchemy class only decides when no code exists |
| Used by all three consumers, no copies | `services/visit/consumer.py:123` `classify.is_transient(exc, temporal=True)`; `services/analytics/consumer.py:111` `classify.is_transient(exc)`; `services/notification/triggers.py:175` `classify.is_transient(exc, db=False, temporal=True, broker=True)`; AST guard in `tests/events/test_classify.py` |
| Cap yields `TransientExhausted` | `libs/events/consumer.py:76` `TRANSIENT_EXHAUSTED`, `_retry_later` (line 456) dead-letters after the attempt floor and time cap (10 / 900 s) |
| Lost trigger recoverable: reconciler | `services/visit/downstream.py` `restart_lost_triggers(max_age_minutes, backoff)` with `LostTriggerBackoff`, `VISIT_LOST_TRIGGER_RESTART_ENABLED` kill switch, `VISIT_LOST_TRIGGER_MAX_AGE_HOURS` ceiling (default 24), abort after 3 consecutive RPC errors; runs first under its own 20 s budget (`services/visit/activities/downstream.py`) |
| Lost trigger recoverable: backfill and replay | `scripts/backfill_visit_completed_events.py` `stale_queued` / `restarted` counts; `scripts/replay_dlq.py` (dry run default, `--apply` needs a selector, `POISON_ERROR_TYPES` skipped by default) |
| Live proof of recovery | `closeout-logs/gap-e2e.txt`: `PASSED ...test_lost_visit_trigger_is_recovered_by_the_reconciler` |
| Retired sweeper Schedule still absent | I listed live Temporal schedules from inside `visit-worker`: `scheduling-hold-reaper, scheduling-pending-starter, scheduling-reconciler, scheduling-schedule-change-poller, visit-reconciler`. No `visit-downstream-sweeper`. Gate e2e `test_sweeper_schedule_retired_on_live_server` PASSED |
| No workflow/activity name or shape changed | `git diff 1291b59..HEAD` on workflows, `schedules.py`, notification and scheduling workflows and REQUIREMENTS.md: only `services/visit/workflows/reconciler.py`, and that diff is docstring only; `tests/workflows/test_replay_reconciler.py`: 10 passed, 1 skipped (opt-in recorder) |
| D-03 condition for the dead-letter branch | Now holds: a dead-lettered `visit.completed` has three recovery routes, the Schedule stays retired, and the kill-between-commit-and-publish proof from 03-11/03-12 is unchanged |
| Tests (mine, this session) | `tests/events/test_classify.py`, `test_replay_dlq.py`, `tests/docs`, `tests/libs/test_postgres_registry.py`: 280 passed; `tests/visit/test_visit_consumer.py`, `test_visit_reconciler.py`, `tests/events/test_consumer_reliability.py`: 68 passed |

Proof level, stated honestly: the 53300/57P03/40P01 path is proven at the real `_handle_message` with real asyncpg error classes, not by a live 53300 injection (docs 10.9 says so). This is a behavioral test, so the truth is VERIFIED, not presence-only.

### 2. CR-02 closed (VERIFIED)

- Live Prometheus (queried now): `sum by (job)(resets(http_requests_total[30m]))` returned **0** for all nine API jobs (provider, patient, scheduling, identity, billing, visit, gateway, organization, analytics).
- Targets: 20 active targets, all `up` (17 job groups; the three Temporal workers expose two targets each).
- `PROMETHEUS_MULTIPROC_DIR` appears in exactly the nine API Dockerfile `CMD`s (grep: analytics, billing, gateway, identity, organization, patient, provider, scheduling, visit; notification Dockerfile: 0). Live pid 1 environment: 1 match in scheduling, gateway, visit, analytics API containers; 0 in scheduling-worker, visit-worker, visit-consumer, analytics-consumer, notification-celery, notification-consumer, notification-temporal-worker.
- Two workers kept: `-w ${WEB_CONCURRENCY:-2}` in the CMD (checked in `services/visit/Dockerfile`; D-14 honored, not collapsed to 1).
- `libs/observability/gunicorn_conf.py` has `on_starting` (empties the directory in the master) and `child_exit` (`mark_process_dead`); `http_metrics.py:54` builds `MultiProcessCollector`.
- Gate e2e: nine `test_http_request_counters_have_no_resets_and_never_decrease[...]` cases PASSED (count 9 in `gap-e2e.txt`); smoke resets file: 0 for each of the nine jobs.

### 3. WR-05 closed (VERIFIED)

`libs/db/postgres.py`: `_registry`, `_retire`, and `_entry_for` retire entries whose loop `is_closed()`; `test_compose_connection_budget` in `tests/libs/test_postgres_registry.py` passes (my run). Measured peak: I recomputed from `closeout-logs/gap-pg-samples.txt` (192 samples): max `total=` **105** (first gate: 304). Caveat that stays open (gap-review WR-05/WR-06): the "241 of 297" budget is derived for services but Temporal's 40 is observed, not derived, and host-side processes are not in the sum; pool settings are unbounded integers.

### Gate evidence cross-check (logs exist and match the 03-26 SUMMARY)

- `closeout-logs/gap-non-e2e.txt` last line: `2049 passed, 11 skipped, 98 deselected, 128 warnings in 1618.63s`. Match.
- `closeout-logs/gap-e2e.txt` last line: `97 passed, 5 deselected, 1 warning in 1280.55s`. Match.
- Both logs untracked (`git status`). I did not rerun them.

## ROADMAP Success Criteria

| # | Criterion | Verdict | Evidence |
|---|-----------|---------|----------|
| 1 | Events via transactional outbox with registered schema; kill between commit and publish never loses or duplicates; consumers tolerate redelivery | **VERIFIED** | Unchanged and not regressed: previous report's evidence (same-transaction `enqueue_event`, Debezium CDC, 10 Avro subjects) plus the gate e2e (97 passed) containing the outbox/CDC, redelivery and visit-downstream kill proofs. New: lost-event recovery now exists, so "never loses" also holds for the dead-letter branch |
| 2 | Booking/cancel/reschedule notify asynchronously without blocking; reminders and follow-ups by Celery/RabbitMQ; retry without double-send; poison to DLQ | **VERIFIED with recorded caveat** | Delivery side unchanged and verified (acks_late, dedupe keys at two layers, DLX to `notifications.dlq`, Temporal reminder workflows feed one Celery send). The "no silent loss" clause previously failing through CR-01 now holds for the notification-trigger consumer (shared classifier, cap, replay script). Caveat: first-review WR-01 (a `reschedule` signal arriving while the last lead's enqueue activity runs is dropped, so the new slot gets no reminders) is open by D-14. Why VERIFIED and not PARTIAL: user-scoped and recorded, one-activity window at the final lead, and SC2's literal text is met. Honest residual: it is a real silent-loss path for reminders in that window, and the fix needs `workflow.patched` plus a replay test |
| 3 | Five metrics from the event stream/projections, not by polling the OLTP database | **VERIFIED** | Unchanged: five views, analytics API, no OLTP access in `services/analytics`. Previous caveat (WR-02, projection event dead-lettered on a DB error) is closed by the shared classifier in `services/analytics/consumer.py:111` and recoverable by `scripts/replay_dlq.py` or `services.analytics.rebuild` |
| 4 | Prometheus metrics for every service visible in Grafana; one booking is one connected Jaeger trace; structured logs | **VERIFIED** | Metrics now correct (CR-02 closed, section 2); 20 targets up; Grafana dashboards provisioned (`tests/infra/test_grafana_provisioning.py` part of the earlier gate). Trace: see item 5 |
| 5 | Dashboard shows the flows, includes the evaluation Q&A, complete and current | **VERIFIED with a small accuracy debt** | `tests/docs` passes (my run, inside the 280). The three outage sentences were rewritten and approved by the user; the fourth ("Not verified") was updated in 261112-8r6. One stale sentence remains (item 6) |

## 5. Jaeger: one connected trace (VERIFIED by test, live fetch not possible now)

I tried to fetch a live booking trace. The Jaeger query API (`localhost:16686`) is up but holds no booking traces: it is in-memory and was restarted after the OOM kill, so the earlier trace `8132991350448d3158960a29412aa965` now returns 404 and the newest gateway traces are non-booking. I did not generate new traffic (no state mutation). Evidence instead: `closeout-logs/gap-e2e.txt` line 237 `PASSED tests/e2e/test_phase3_trace.py::test_one_booking_is_one_connected_trace_across_http_temporal_kafka_and_celery` and line 169 `test_the_booking_trace_id_is_in_the_json_logs_of_every_process_it_touched`, both in the finished-stack gate run after all the gap-closure code; the previous verification fetched the live 61-span, 12-service trace. The gap-closure changes did not touch tracing code (only consumer retry, reconciler, metrics, pool code).

## 6. Docs and dashboard honesty (one recorded follow-up)

- `docs/architecture.md` 10.2/10.4/10.7/10.8/10.9 and the dashboard now state: retried with capped backoff, `TransientExhausted` after the cap, recovery tools, head-of-line effect, multiprocess metrics, connection budget with measured peaks, what was and was not proven. Guard tests (`tests/docs`) fail if the false claim returns. They pass.
- **Remaining stale sentence** (recorded follow-up, not fixed): `docs/architecture-dashboard.html` line 1542, the **"Failure paths:"** sentence, still says "the consumer's decode-time Schema Registry outage path has no automated test". It is untrue in the safe direction (it understates: 03-21 added unit tests for 408/429/5xx). The same paragraph also says the smoke profile "passed in three runs" (four runs recorded) and that nobody has looked at the Jaeger trace in the UI (the user did). Understatements, not overclaims; not blocking.

## Observable Truths vs User Decisions

| Decision | Honored? | Evidence |
|---|---|---|
| D-01 CDC, no polling relay | Yes | four Debezium connectors RUNNING per the gate; services only insert outbox rows |
| D-02 Temporal reminders, Celery delivery | Yes | unchanged |
| D-03 retire the sweeper, prove replacement | Yes, now also for the dead-letter branch | live schedule list has no sweeper; lost-trigger e2e PASSED |
| D-04 NFRs measured after Phase 3 | Yes | smoke ran four times, thresholds passed (accept p95/p99 21.8/78.9, 29.3/170, 49.1/200.8, 41.85/245.5 ms, per docs); sustained/burst/contention deferred by the user |
| D-13 analytics start at cutover | Yes | stated in docs and dashboard |
| D-14 gap closure scope (multiprocess with two workers kept; WR-01/WR-04/WR-06 etc. out of scope) | Yes | two workers kept; no WR-01 change; REQUIREMENTS.md diff empty |
| EVENT-04 projections on Kafka consumer, not Celery | ACCEPTED, RECORDED DEVIATION (flagged, not failed) | user accepted at the 03-01 checkpoint; REQUIREMENTS.md wording untouched; `PHASE3_TRACE_NOTES["EVENT-04"]` |

## Requirements Coverage (20 of 20, no orphans)

All 20 IDs appear in the plans' `requirements:` frontmatter (union across 03-01..03-26; the gap plans cite EVENT-02/03/05, NOTIF-01, ANLY-06, OBS-01, DASH-03/04) and all 20 appear in REQUIREMENTS.md mapped to Phase 3 (checked by grep: each ID is present in at least one plan, and in REQUIREMENTS.md twice). I did not tick or edit any requirement; `git diff 1291b59..HEAD -- .planning/REQUIREMENTS.md` is empty (DASH-04 was already `[x]` before this phase's verification, and the traceability rows still read Pending; the orchestrator owns ticking).

| Requirement | Status | Note |
|---|---|---|
| EVENT-01 | SATISFIED | 10 subjects, strict serializer |
| EVENT-02 | SATISFIED | same-transaction outbox, CDC, kill proofs; lost-event recovery added |
| EVENT-03 | SATISFIED | idempotent consumers; the CR-01 loss path is closed |
| EVENT-04 | ACCEPTED DEVIATION (flagged) | notifications/reminders/follow-ups on Celery+RabbitMQ; projections are a Kafka consumer, user-accepted; wording untouched. Not a failure |
| EVENT-05 | SATISFIED | retry plus DLQ (`notifications.dlq`, Kafka DLQs); transient failures no longer dead-lettered early; `TransientExhausted` and replay. Caveats: `notifications.dlq` unbounded/undrained (IN-04 first review); head-of-line effect of the cap needs a replay step after a long outage |
| NOTIF-01 | SATISFIED | |
| NOTIF-02 | SATISFIED with caveat | first-review WR-01 reschedule race (D-14, recorded) |
| NOTIF-03 | SATISFIED with caveat | first-review WR-06: a replay or backfill can start follow-ups for old visits (no staleness guard); `replay_dlq.py` docstring now says so |
| NOTIF-04 | SATISFIED | fake-provider caveat documented |
| ANLY-01..05 | SATISFIED | five views, API, panels |
| ANLY-06 | SATISFIED | views only |
| OBS-01 | SATISFIED | CR-02 closed live |
| OBS-02 | SATISFIED | connected trace (test, previous live fetch) |
| OBS-03 | SATISFIED | `test_phase3_logs.py` passed in the gate |
| DASH-03 | SATISFIED | flows section |
| DASH-04 | SATISFIED | Q&A and roadmap; one stale "Failure paths" sentence (follow-up) |

## Anti-patterns and review state

- Debt markers (`TBD`/`FIXME`/`XXX`) in the 11 gap-closure source files: none found.
- Gap review (`03-REVIEW-GAP.md`): 0 critical, 7 warning, 6 info. WR-01, WR-02 (the default-poison and no-selector part) and WR-07 were fixed by quick task 261112-8r6 (I confirmed the code names: `POISON_ERROR_TYPES`, the `--apply needs a selector` refusal, `LostTriggerBackoff`, max age, kill switch; their tests pass). None of the remaining warnings is a blocker for the roadmap contract; none is mitigated by a user override except as listed in the follow-ups.
- Semgrep (recorded by 03-26): 24 findings identical to the 03-20 triage, all accepted or false positives (ten root-user Dockerfiles accepted by the user); 0 findings on the gap-closure code with `p/python` and `p/security-audit`. I did not rerun it.

## Accepted limits honored (not failed)

Ten root-user Dockerfiles; NFR-01..08 unmeasured (smoke only, four runs, thresholds passed; sustained/burst/contention deferred); the known baseline test (`tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway`) deselected by node id; macOS fork SIGTRAP class; Kafka and Jaeger OOM kills on the 7.65 GiB Docker VM; `scripts/compose_healthcheck.sh` not flagging a stopped container.

## Remaining follow-ups (explicit)

1. First-review WR-01: reminder reschedule-signal race (needs `workflow.patched` plus a replay test). This is the SC2 caveat.
2. First-review WR-04: `debezium` role can read other databases via `test_decoding`; Connect REST unauthenticated; `FileConfigProvider` unrestricted (PHI-relevant, local-dev scope).
3. First-review WR-06: no staleness guard in `on_visit_completed` (replay/backfill can message old patients).
4. Gap-review WR-02 remainder: no dedupe of repeated DLQ copies and no replayed marker in `scripts/replay_dlq.py` (a replay fans out to every group on the topic).
5. Gap-review WR-03: the transient retry sleeps on the single poll thread, so one stuck message slows all three partitions up to the cap (pause only the failing partition).
6. Gap-review WR-04: `RuntimeError`, `OSError`, `KombuError`/`AMQPError`/`PyMongoError` are blanket-transient, so a deterministic failure costs a 900 s cap per message.
7. Gap-review WR-05: `DB_POOL_SIZE`/`DB_MAX_OVERFLOW` unbounded (`Field(ge=...)`); the halved API pool (5+5) is untested against the deferred burst profile.
8. Gap-review WR-06: "241 of 297" is not a bound (Temporal's connection pools observed not derived, host-side processes not counted); set `SQL_MAX_CONNS`/`SQL_VIS_MAX_CONNS`.
9. Gap-review IN-01..IN-06 (e.g. `sqlstate_of` follows `__context__`, per-message transient state cleanup, engine registry lock, backfill partial-apply, replay edge cases, `25006` classification).
10. Dashboard line 1542 "Failure paths:" sentence (and the "three runs" / "nobody has looked at Jaeger" statements) is stale.
11. `scripts/compose_healthcheck.sh` does not flag a stopped required container; Kafka and Jaeger memory caps; dev `max_connections` not raised.
12. Deferred load profiles (`sustained`, `burst`, `contention`) and an A/B of multiprocess-metrics overhead on the accept path; provider specialty-ordering test; SIGTRAP class; identity migration test on the live DB.
13. The live `visit-downstream` DLQ holds 814 `NonRetryableEventError` records for deleted harness visits (replay skips poison by default).
14. Real SMS/email provider idempotency key (documented caveat); EVENT-04 REQUIREMENTS.md wording decision; ticking the 20 requirements in REQUIREMENTS.md (orchestrator's step).

## Human Verification

None outstanding. The user already approved Jaeger, the four dashboards, the architecture dashboard, and the gap-closure dashboard sentences (03-26 sign-off, 2026-11-12). The single judgement item (SC2 caveat) is recorded in the frontmatter override rather than as a human-verification request.

## Gaps Summary

No gaps. Both blockers from the previous report are closed with code, test and live evidence; no regression found in Phases 1, 1.1 or 2.1 (the retired sweeper Schedule stays absent, the other five Schedules are registered, reconciler workflow and activity names unchanged and replay-tested).

---

_Verified: 2026-11-12T10:00:00Z_
_Verifier: Claude (gsd-verifier)_

## User confirmation (2026-11-12)

The user confirmed the SC2 override (first-review WR-01 reminder reschedule-signal race accepted as a recorded follow-up, per D-14) with "Accept as recorded follow-up (Recommended)", and chose to tick EVENT-04 with the reworded note ("Tick it and reword (Recommended)").
