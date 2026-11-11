---
phase: 03-event-driven-system-observability
verified: 2026-11-11T14:00:00Z
status: gaps_found
score: 3/5 roadmap success criteria verified (SC1, SC3, SC5 VERIFIED; SC2, SC4 PARTIAL); 18/20 requirements satisfied outright, 2 partial (EVENT-05 on the consumer path, OBS-01)
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
  - "libs/events/consumer.py"
  - "libs/events/outbox.py"
  - "libs/observability/http_metrics.py"
  - "scripts/backfill_visit_completed_events.py"
  - "services/analytics/consumer.py"
  - "services/notification/workflows/reminder.py"
  - "services/visit/consumer.py"
  - "services/visit/repository/visit_repo.py"
covered_digest: "v2:sha256:34cba1fa075effd65028eb237955eb1b8f1086e0321a6e4534f784e41b2c1f49"
behavior_unverified: 0
overrides_applied: 0
re_verification: none (initial verification)
gaps:
  - truth: "No silent loss: a healthy visit.completed (and any analytics or notification event) is never dead-lettered because of a transient infrastructure failure, and a dead-lettered visit.completed can be recovered (SC2 / phase goal; EVENT-03, EVENT-05; D-03 replacement safety net)"
    status: failed
    reason: "CR-01 confirmed in code. _is_transient() classifies only sqlalchemy OperationalError/InterfaceError/connection_invalidated, TimeoutError, ConnectionError, OSError and Temporal RPC statuses. asyncpg TooManyConnectionsError (53300), CannotConnectNowError (57P03) and DeadlockDetectedError (40P01) are not in the SQLAlchemy asyncpg error-translate map (checked against SQLAlchemy 2.0.52 here) and are not RuntimeError/OSError, so they are re-raised as ordinary handler errors; the base consumer (libs/events/consumer.py 309-326) retries 3 times with 0.5 s backoff and dead-letters. The sweeper Schedule is deleted, list_unsettled requires downstream_started_at IS NOT NULL, scripts/backfill_visit_completed_events.py skips a visit whose outbox row already exists (already_queued), and no DLQ replay tool exists. A dead-lettered visit.completed therefore leaves the visit completed with downstream_started_at NULL for ever (billing never finalized, appointment never completed). The 300-connection ceiling was actually hit at the phase gate, so the trigger is demonstrated. docs/architecture.md 10.2 states 'An infrastructure outage (Postgres ...) is never dead-lettered', which is false for these error classes. No test covers it: tests/visit/test_visit_consumer.py test_a_database_outage_is_transient uses the SQLAlchemy OperationalError class only."
    artifacts:
      - path: "services/visit/consumer.py"
        issue: "_is_transient (lines 101-109) misses asyncpg PostgresError classes 08/53/57/40"
      - path: "services/analytics/consumer.py"
        issue: "same classification gap (lines ~81-87): projection events dead-lettered on DB errors (WR-02)"
      - path: "services/notification/triggers.py"
        issue: "third copy of _is_transient (lines ~97-104) has drifted (no SQLAlchemy branch) (IN-01)"
      - path: "libs/events/consumer.py"
        issue: "_decode dead-letters every 4xx Schema Registry status including 408/429 (line ~347)"
      - path: "services/visit/repository/visit_repo.py"
        issue: "list_unsettled excludes downstream_started_at IS NULL, so no reconciler recovers a lost trigger"
      - path: "scripts/backfill_visit_completed_events.py"
        issue: "treats an existing outbox row as already_queued (line 121), so it cannot repair a dead-lettered event; also no replay of a DLQ record"
    missing:
      - "One shared libs/events/classify.py that classifies by SQLSTATE class (08, 53, 57, 40) and asyncpg.PostgresError, plus Temporal and broker classification; import it from the three consumers; exempt 408/429/5xx Schema Registry statuses from the poison path"
      - "A recovery route for a dead-lettered visit.completed: either a reconciler query (status='completed' AND downstream_started_at IS NULL AND completed_at < now() - grace) that calls the idempotent ensure_downstream_started, or a DLQ replay script that re-produces a dead-lettered record to its source topic"
      - "A test that makes the handler raise asyncpg.exceptions.TooManyConnectionsError and asserts RETRY_LATER (not dead-letter) for the visit and analytics consumers, and a test for the recovery route"
      - "Correct docs/architecture.md 10.2 and the dashboard Q&A once fixed (or state the limitation until then)"
  - truth: "Every service exposes Prometheus metrics visible in Grafana dashboards for critical flows (SC4; OBS-01): the HTTP request-rate, error-ratio and latency panels read correct values"
    status: partial
    reason: "CR-02 confirmed in code and live. Every gunicorn-served API (gateway, billing, analytics, identity, patient, provider, organization, scheduling, visit) runs -w ${WEB_CONCURRENCY:-2} (services/*/Dockerfile, WEB_CONCURRENCY not set in infra/docker-compose.yml) and PROMETHEUS_MULTIPROC_DIR / MultiProcessCollector appear nowhere in libs, services, infra, scripts, README or docs. Each scrape of /metrics is answered by one of two worker registries. Live Prometheus query sum by (job)(resets(http_requests_total[6h])) returned 2758 (scheduling), 2008 (identity), 1690 (billing), 1632 (organization), 1536 (provider), 1375 (gateway), 1024 (patient), 999 (visit), 644 (analytics) counter resets in 6 h on a stack that was not restarted. rate() and histogram_quantile on the booking/NFR dashboard (smarthealth-booking-nfr.json) and the gateway 5xx ratio are therefore unreliable; the Phase 3 purpose of those histograms (measure NFR-03/NFR-04 after the phase) cannot be trusted. Single-process targets (Temporal workers, three consumers, Celery) are unaffected; all 17 Prometheus job groups are up and every dashboard is provisioned, so exposure and provisioning are real; the defect is value correctness."
    artifacts:
      - path: "services/gateway/Dockerfile"
        issue: "gunicorn -w 2, no multiprocess mode; gateway /metrics is a hand-written generate_latest() route on :9100"
      - path: "services/analytics/Dockerfile"
        issue: "same; applies to the eight other gunicorn Dockerfiles (billing, identity, patient, provider, organization, scheduling, visit)"
      - path: "libs/observability/http_metrics.py"
        issue: "Instrumentator uses the process registry; does not switch to MultiProcessCollector"
      - path: "infra/grafana/dashboards/smarthealth-booking-nfr.json"
        issue: "http_requests_total and http_request_duration_seconds panels read per-worker, reset-riddled series"
    missing:
      - "Either set PROMETHEUS_MULTIPROC_DIR (cleaned at start) plus a gunicorn_conf.py child_exit calling multiprocess.mark_process_dead, and build the gateway /metrics from a CollectorRegistry with MultiProcessCollector, or set WEB_CONCURRENCY=1 for the Prometheus-scraped APIs (also eases the connection budget, WR-05)"
      - "An e2e check that resets(http_requests_total[...]) is 0 after a short traffic burst, so the defect cannot return"
deferred: []
---

# Phase 3: Event-Driven System + Observability Verification Report

**Phase Goal:** Every state change reliably becomes a durable, replayable event with no dual-write gaps, background jobs run independently of user-facing flows with guaranteed at-least-once delivery and no silent loss, operators can see the five core operational metrics derived from the event stream, and every critical flow is traceable end-to-end and observable via metrics/logs/dashboards, with the Architecture & Decision dashboard fully closed out.
**Verified:** 2026-11-11
**Status:** gaps_found
**Re-verification:** No, initial verification

## Verdict

The core design is real and works: transactional outbox in every producer's own transaction, Debezium CDC, ten Avro subjects, idempotent consumers, Temporal reminders feeding one Celery send task with a dead-letter exchange, an event-fed analytics projection with five views behind a clinic-scoped API, one connected Jaeger trace, and a closed-out dashboard. I could not falsify any of that and found live evidence for most of it.

Two clauses of the phase goal are not met as written, both confirmed against the code and, for the second, against the live stack:

1. "no silent loss" (CR-01): a dead-lettered `visit.completed` is never recovered, and the consumer dead-letters on asyncpg connection/resource errors it should retry. This is the exact safety net D-03 retired.
2. "observable via metrics/dashboards" (CR-02): HTTP rate and latency series are wrong under gunicorn `-w 2` without multiprocess mode.

Neither is mitigated by an accepted deviation: the user accepted the test-environment limits, the root-user Dockerfiles, the EVENT-04 deviation and unmeasured NFRs, but nothing in the CONTEXT, the SUMMARYs or the docs records either of these two as accepted. The documented claim in `docs/architecture.md` 10.2 ("an infrastructure outage (Postgres ...) is never dead-lettered") is contradicted by the code, which makes it a documentation-accuracy defect as well.

## Success Criteria (ROADMAP contract)

| # | Criterion | Verdict | Evidence |
|---|-----------|---------|----------|
| 1 | Events published to Kafka with a registered schema via a transactional outbox; killing the process between commit and publish never loses or duplicates; consumers tolerate redelivery | **VERIFIED** | `libs/events/outbox.py` `enqueue_event` inserts through the caller's `AsyncSession`, never commits, deterministic uuid5 id with `ON CONFLICT (id) DO NOTHING`. Emit sites inside the state-change transactions: `appointment_repo.py` lines 256, 494, 615, 761, 812, 876 (booked, cancelled, rescheduled, checked_in, completed, no_show); `patient_repo.py` 60, 175; `provider_repo.py` 391; `visit_repo.py` 171. Live Schema Registry: 10 subjects (`com.smarthealth.events.*`), global compatibility BACKWARD. Live Connect (via `docker exec`): `scheduling-outbox`, `patient-outbox`, `provider-outbox`, `visit-outbox` connector and task both RUNNING. `wal_level=logical` in `infra/docker-compose.yml`. Kill/redelivery proofs exist (`tests/e2e/test_phase3_outbox_cdc.py`, `test_phase3_redelivery.py`, `test_phase3_visit_downstream.py`); e2e gate run 2 = 71 passed (recorded by 03-20; I did not rerun the suite). D-01 honored: no service-side relay loop (grep for relay/poll code found only comments and the replay tooling). |
| 2 | Booking/cancel/reschedule trigger an asynchronous notification that never blocks the booking; reminders and follow-ups scheduled and delivered by Celery/RabbitMQ workers, retried without double-send, poison to a DLQ | **PARTIAL** | Delivery side VERIFIED: `services/notification/celery_app.py` `task_acks_late`, `task_reject_on_worker_lost`, `x-dead-letter-exchange` to `notifications.dlq`; `tasks.py` unique `dedupe_key` at delivery-log and provider layers, exponential `task.retry`; Temporal `ReminderWorkflow`/`FollowUpReminderWorkflow` (D-02) enqueue the same send; the live Jaeger trace contains `apply_async/notifications.send` and `run/notifications.send`. Booking is never blocked (event is consumed after commit). PARTIAL because of the phase-goal clause "no silent loss": the Kafka-to-Celery and Kafka-to-Temporal trigger consumers share the CR-01 classification gap, so a transient failure can dead-letter a valid notification trigger or visit-downstream start with no recovery, and WR-01 (a `reschedule` signal arriving while the last lead's enqueue activity runs is dropped, so the new slot gets no reminders) is a real lost-reminder path. |
| 3 | Operator can view all five metrics, each computed from the event stream/projections, not by polling the transactional DB | **VERIFIED** | `services/analytics/repository/queries.py` reads only five views (`v_active_patients`, `v_appointments_booked_daily`, `v_completed_visits_daily`, `v_cancellation`, `v_visit_wait`) created in `analytics_0001_projections.py`; no OLTP database referenced under `services/analytics`. API `/api/analytics/summary` and `/timeseries` behind the gateway (`routing.py` line 41), role- and clinic-scoped; projections fed by the analytics consumer with claim and effect in one transaction; rebuild by replay (`rebuild.py`). Wait time defined check-in to visit start per the research revision; D-13 (start at cutover) stated in docs. Caveat: WR-02 (analytics projection can permanently drop an event on a DB connection error until a manual rebuild), which affects metric accuracy under load, not the existence or derivation of the metrics. |
| 4 | Every service exposes Prometheus metrics visible in Grafana for critical flows; one booking traced end-to-end in Jaeger as one connected trace; structured logs for booking, availability sync, billing, notifications, workers | **PARTIAL** | Tracing VERIFIED live: fetched trace `8132991350448d3158960a29412aa965` from Jaeger (61 spans, 12 services) containing gateway POST, scheduling `POST /api/appointments`, `StartWorkflow:BookingWorkflow`, `RunActivity:booking.*` (validate, reserve_slot, billing_precheck, confirm, schedule_notification), billing/identity/organization/patient/provider hops, `smarthealth.appointment.events process` in both the analytics and notification consumers, `StartWorkflow:ReminderWorkflow`/`RunWorkflow:ReminderWorkflow`, `apply_async/notifications.send` and `run/notifications.send`: HTTP to Temporal to Kafka to Celery in one trace. Live Prometheus: 17 job groups, all targets up. Four dashboards provisioned from `infra/grafana/dashboards/` (operations, events, notifications, booking-nfr) with the user's visual approval. Structured logs: `tests/e2e/test_phase3_logs.py`. PARTIAL because of CR-02: HTTP rate/latency/5xx panels show per-worker, reset-ridden data (thousands of counter resets per job in 6 h). Exposure is real; correctness of the headline HTTP/NFR series is not. |
| 5 | Architecture & Decision dashboard shows booking, cancel/reschedule, visit lifecycle, notifications, analytics flows, plus an evaluation Q&A, complete and current | **VERIFIED** | `docs/architecture-dashboard.html`: "Event flows (Phase 3)" section (line 1058) with cancel/reschedule, visit lifecycle, notification and analytics flows; Phase 3 decision cards (CDC vs poller, Debezium image deviation, EVENT-04 deviation, Avro, topic per aggregate, idempotency flavours, Temporal reminders, Celery on RabbitMQ 4.3, sweeper retired); evaluation Q&A section 06 (line 1326); roadmap section 07 updated; stale "scaffolding" language removed (the Event backbone and Background-queue cards say "Wired"). `pytest tests/docs`: 164 passed (run by me). The user reviewed it offline at full and narrow width. Minor accuracy debt: the sweeper-retired card and 10.2 text assert behaviour that CR-01 contradicts (see gap 1). |

## Observable Truths vs User Decisions

| Decision | Honored? | Evidence |
|---|---|---|
| D-01 CDC (Debezium), no polling relay | Yes | Four connectors RUNNING live; `wal_level=logical`; services only insert outbox rows |
| D-02 Temporal reminders, Celery delivery | Yes | `services/notification/workflows/reminder.py` uses `workflow.now()`, `wait_condition`, `execute_activity` only; trace shows RunWorkflow:ReminderWorkflow then Celery run |
| D-03 Retire the sweeper, prove replacement by kill test | Retired yes (`services/visit/schedules.py` `retire_sweeper_schedule` runs at each worker start; tombstone workflow kept); replacement proven for the kill-between-commit-and-publish path, NOT for the dead-letter branch (CR-01) | see gap 1 |
| D-04 NFRs measured after Phase 3; no extra sync work on accept; histograms added; smoke still passes | Yes, with the CR-02 caveat on the histograms' trustworthiness | Smoke passed three times (recorded); NFR-01..08 deliberately unmeasured (accepted) |
| D-13 Analytics start at cutover | Yes | Stated in docs/architecture.md 10.9 and the dashboard |
| EVENT-04 projections on a Kafka consumer, not Celery | ACCEPTED, RECORDED DEVIATION (flagged, not failed) | `PHASE3_TRACE_NOTES["EVENT-04"]` in `tests/docs/test_requirement_traceability.py`; dashboard card "Projections on a Kafka consumer, not Celery"; REQUIREMENTS.md wording intentionally untouched |

## Requirements Coverage

All 20 IDs appear in the plan frontmatter (union of plans 03-01..03-20; 03-20 lists all 20) and all 20 are mapped to Phase 3 in REQUIREMENTS.md (all still Pending; I did not tick any). No orphaned requirement.

| Requirement | Plans | Status | Evidence / note |
|---|---|---|---|
| EVENT-01 | 03-01,03,05,07,08,09,18 | SATISFIED | 10 registered Avro subjects (live), strict serializer, schemas under `libs/events/schemas/` |
| EVENT-02 | 03-01,05,07,08,09,11,12,17 | SATISFIED | Same-transaction outbox, CDC, kill proofs |
| EVENT-03 | 03-04,09,10,11,12,14,17 | SATISFIED with caveat | Idempotent via processed-event claims, deterministic workflow ids, dedupe keys; redelivery test; caveat: CR-01 path loses the effect for one visit event |
| EVENT-04 | 03-02,06,14 | SATISFIED (accepted deviation, flagged) | Notifications, reminders and follow-ups are Celery/RabbitMQ; projections are a Kafka consumer (user-accepted at the 03-01 checkpoint) |
| EVENT-05 | 03-02,04,14 | PARTIAL | Celery retry plus `notifications.dlq` and Kafka DLQs work for poison messages; but non-poison infra errors are dead-lettered and a dead-lettered visit event has no recovery (CR-01, WR-02); `notifications.dlq` unbounded and undrained (IN-04) |
| NOTIF-01 | 03-07,14 | SATISFIED | notification-trigger consumer, async, traced |
| NOTIF-02 | 03-06,14 | SATISFIED with caveat | Reminders via Temporal; WR-01 drops a reschedule signal in one window |
| NOTIF-03 | 03-06,14 | SATISFIED with caveat | FollowUpReminderWorkflow; WR-06 (no staleness guard on backfilled `visit.completed`) |
| NOTIF-04 | 03-02,14 | SATISFIED | acks_late, retry, dedupe key at two layers; fake-provider caveat documented |
| ANLY-01..05 | 03-08,07,09,10,13,16 | SATISFIED | five views, API, Grafana panels; WR-02 accuracy caveat under DB errors |
| ANLY-06 | 03-10,13 | SATISFIED | queries read views only; no OLTP access in `services/analytics` |
| OBS-01 | 03-15,16,18 | PARTIAL | targets up, dashboards provisioned; HTTP series wrong under 2 gunicorn workers (CR-02) |
| OBS-02 | 03-01,02,04,17 | SATISFIED | one connected Jaeger trace verified live |
| OBS-03 | 03-17 | SATISFIED | `tests/e2e/test_phase3_logs.py` |
| DASH-03 | 03-18,19 | SATISFIED | flows section present |
| DASH-04 | 03-18,19 | SATISFIED | Q&A and roadmap updated |

## Required Artifacts / Key Links (spot-verified)

| Link | Status |
|---|---|
| service transaction -> `outbox` row (`enqueue_event`) at all 10 emit sites | WIRED |
| outbox -> Debezium connector -> `smarthealth.*.events` topics | WIRED (4 connectors, tasks RUNNING) |
| `smarthealth.visit.events` -> visit-downstream consumer -> `ensure_downstream_started` | WIRED (but CR-01 failure branch) |
| `smarthealth.appointment.events` -> notification-trigger -> Celery `notifications.send` and Temporal reminder | WIRED (live trace) |
| appointment/patient/visit events -> analytics consumer -> projections -> views -> `/api/analytics` -> gateway | WIRED |
| `/metrics` on all processes -> Prometheus -> Grafana | WIRED; HTTP series incorrect (CR-02) |

## Anti-Patterns / Review Findings Bearing on the Goal

| Finding | Severity | Bearing |
|---|---|---|
| CR-01 (confirmed) | BLOCKER | Conflicts with "no silent loss" and with the D-03 condition |
| CR-02 (confirmed live) | BLOCKER | Conflicts with "observable via metrics/dashboards"; undermines the post-phase NFR measurement |
| WR-02 dead-lettering projection events on DB errors; 4xx 408/429 poison | WARNING | Same root cause as CR-01; fix together |
| WR-01 reminder drops late reschedule signal | WARNING | Needs `workflow.patched` plus recorded-history replay test |
| WR-03 over-broad transient set with no cap (partition can block for ever) | WARNING | Delay, not loss; add a cap or narrow |
| WR-04 `debezium` REPLICATION role can decode `patient_db` (PHI) via `test_decoding`; Connect REST unauthenticated; FileConfigProvider unrestricted | WARNING | Contradicts the 'outbox only' docstring in `libs/events/migration.py`; Atlas review returned only low findings, so this was either not raised or rated low; recommend `CONNECT_CONFIG_PROVIDERS_FILE_ALLOWED_PATHS`, `REVOKE CONNECT ... FROM PUBLIC`, and a docstring correction. Not blocking for a local-dev assignment but PHI-relevant |
| WR-05 connection budget above 300; `_entry_for` orphans engines without `dispose()` | WARNING | Most likely cause of the observed `TooManyConnectionsError` and the trigger of CR-01; the user accepted the test symptom, not the production-path consequence |
| WR-06 backfill/replay sends follow-ups for old visits | WARNING | Add staleness guard in `on_visit_completed` and a caution in the backfill script |
| WR-07 failed offset commit crashes the consumer | WARNING | Crash-loop on broker blip; at-least-once preserved |
| IN-01..IN-06 | INFO | Duplicate classification code (fold into CR-01 fix), duplicated notification code, unbounded `event_type` label cardinality, rabbitmq-init admin password in env, metrics-server flag, slot invalidation recovery undocumented |
| Debt markers (TBD/FIXME/XXX) | none blocking found in the reviewed files by the review; not rescanned |

## Test Evidence and Accepted Limits

- `tests/docs`: 164 passed (my run). I did not run the full suites; the recorded 03-20 numbers are: non-e2e run 1 = 2 failed / 1854 passed / 1 error; run 2 = 5 failed / 1850 passed / 1 error; e2e run 2 = 71 passed.
- The non-e2e gate never finished clean on any run. The user accepted this explicitly (provider specialty-ordering test, `TooManyConnectionsError` at the 300-connection ceiling, macOS fork SIGTRAP; each passed alone) as recorded test-environment limitations, plus ten root-user Dockerfiles, and the 03-20 follow-up to raise `max_connections`, cap Jaeger memory and dispose engines is not done. I honor these as accepted. Note that the connection-ceiling symptom is the same mechanism that, in production-shaped use, triggers CR-01, so the acceptance should not be read as covering CR-01.
- NFR-01..08 are unmeasured by the user's decision (smoke only). Not a gap.
- Regression check of prior phases: Phases 1, 1.1 and 2.1 VERIFICATION files were not changed by Phase 3 truths except the one the phase deliberately retired (the Phase 2.1 note that `visit-downstream-sweeper` runs; D-03). The remaining 2.1 reconcilers (scheduling, visit, hold reaper, pending starter) are still registered. The only regression exposure is the lost-trigger case in gap 1: the Phase 2.1 SC4 'unsettled visits' reconciler never covers a visit whose `downstream_started_at` is NULL. No Phase 1/1.1 regression found.

## Human Verification

None outstanding. The user already checked Jaeger (trace `8132991350448d3158960a29412aa965`, which I re-fetched and confirmed), the four Grafana dashboards and `docs/architecture-dashboard.html`.

## Gaps Summary and Suggested Gap-Closure Plans

Two focused plans close the phase.

**Plan A (CR-01 + WR-02 + IN-01 + WR-03, event reliability).**
1. Add `libs/events/classify.py` (SQLSTATE class 08/53/57/40 and `asyncpg.PostgresError`, Temporal statuses narrowed per WR-03, broker errors) and use it in `services/visit/consumer.py`, `services/analytics/consumer.py`, `services/notification/triggers.py`; cap consecutive transient retries and then dead-letter with `TransientExhausted`; exempt 408/429 in `libs/events/consumer.py` `_decode`.
2. Add a recovery route for a lost visit trigger: extend `services/visit/repository/visit_repo.py` (a `list_lost_trigger` query for `status='completed' AND downstream_started_at IS NULL AND completed_at < now() - grace`) wired into the visit reconciler activity that calls `ensure_downstream_started`; or ship a DLQ replay script under `scripts/`.
3. Tests: asyncpg `TooManyConnectionsError` -> `RETRY_LATER` for visit and analytics; dead-lettered `visit.completed` is recovered. Update `docs/architecture.md` 10.2 and the dashboard Q&A.
4. Optionally WR-05 (pool size setting; `dispose()` in `libs/db/postgres.py` `_entry_for`) to remove the trigger, WR-01 (patched reminder fix with replay test), WR-06 (staleness guard) and WR-07 (commit retry).

**Plan B (CR-02, metrics correctness).** Enable multiprocess mode in the nine gunicorn Dockerfiles (`services/{gateway,billing,analytics,identity,patient,provider,organization,scheduling,visit}/Dockerfile`) and the gateway `/metrics` route, or drop to `WEB_CONCURRENCY=1` for scraped APIs; add an e2e assertion that `resets(http_requests_total[5m])` is zero after traffic; rerun the smoke profile. Do this before the deferred `sustained`/`burst`/`contention` runs.

Security follow-up (not gating): WR-04 hardening and docstring correction, IN-04 DLQ cap and admin-password secret.

If the user prefers to accept either gap, record it as an override in this file's frontmatter (`overrides:` with reason, accepted_by, accepted_at) and add it to `docs/architecture.md` 10.9 with the operator recovery step; both look like real defects, not intentional deviations, so closure is recommended over override.

---

_Verified: 2026-11-11T14:00:00Z_
_Verifier: Claude (gsd-verifier)_
