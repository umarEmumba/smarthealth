---
phase: "3"
slug: "event-driven-system-observability"
status: draft
nyquist_compliant: false
wave_0_complete: false
created: "2026-11-10"
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Task ids are `<plan>-<task>` from the 20 PLAN files (revised 2026-11-10 after plan-check: plans split to stay under 15 files and 3 tasks, disruptive live-stack proofs moved to waves of their own, 13 waves); every task carries an automated command, and each test file is created by the task that needs it (no separate Wave 0 plan). The full non-e2e and e2e runs are the phase gate in plan 03-20 Task 2, whose own per-task check reads the recorded gate logs. Plans 03-21..03-26 (waves 14-19) are the gap closure from 03-VERIFICATION.md (CR-01, CR-02 and the related review warnings), revised 2026-11-11 after plan-check (the multiprocess rollout split into 03-24 and 03-25, closeout renumbered 03-26); their final full runs are in plan 03-26 Task 2, whose check fails on any failed or error test in the final logs. Every per-task command that runs a `tests/visit` file stops `visit-consumer` first and starts it again (`start --wait`) whatever the result.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 + pytest-asyncio 1.4.0 (`asyncio_mode = "auto"`) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (markers `integration`, `e2e`) |
| **Quick run command** | `.venv/bin/python -m pytest tests/events tests/notification tests/analytics tests/visit -x -q` |
| **Full suite command** | `.venv/bin/python -m pytest tests -m "not e2e" -q` (do NOT set `OTEL_SDK_DISABLED`; it breaks the trace tests) |
| **E2E command** | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q` with the Compose stack up |
| **Estimated runtime** | quick under 30 s; full about 19 min; e2e about 2 min plus 60 to 90 s stack warm-up |

---

## Sampling Rate

- **After every task commit:** run the matching quick command for the touched area
- **After every plan wave:** run the full non-e2e suite
- **Before `/gsd-verify-work`:** full non-e2e suite and `tests/e2e -m e2e` green
- **Max feedback latency:** 30 seconds for per-task checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 03-01 | 1 | (package gate) | T-3-SC | Six pins and the Connect image approved before any install or pull | checkpoint | `.venv/bin/python -m pip --version` | ❌ created in task | ⬜ pending |
| 03-01-02 | 03-01 | 1 | EVENT-01, EVENT-04 (deviation) | T-3-57 | D-05 contract and all six user-veto items (Connect image, EVENT-04 projections, S3, provider event, insert-only outbox, backfill) answered before any schema exists | checkpoint | `test -f .../03-CONTEXT.md` | ❌ created in task | ⬜ pending |
| 03-01-03 | 03-01 | 1 | EVENT-02, OBS-02 | T-3-01, T-3-04, T-3-57 | Pins installed; outbox row atomic, uuid5 no-op repeat, traceparent from active span, never commits | integration | `.venv/bin/python -m pip check && .venv/bin/python -m pytest tests/events/test_outbox_roundtrip.py -x -q` | ❌ created in task | ⬜ pending |
| 03-02-01 | 03-02 | 2 | EVENT-04, OBS-02 | T-3-15, T-3-16 | Send runs on threads worker over live RabbitMQ, same trace, json only | integration | `.venv/bin/python -m pytest tests/notification/test_tasks.py -x -q` | ❌ created in task | ⬜ pending |
| 03-02-02 | 03-02 | 2 | NOTIF-04 | T-3-18 | Duplicate, transient retry and a worker killed between the provider call and mark_sent -> exactly one provider record per dedupe key; closed reminder skipped | integration | `.venv/bin/python -m pytest tests/notification/test_dedupe.py -x -q` | ❌ created in task | ⬜ pending |
| 03-02-03 | 03-02 | 2 | EVENT-05 | T-3-17, T-3-19 | Exhausted/invalid -> Reject(requeue=False) -> notifications.dlq | integration | `.venv/bin/python -m pytest tests/notification -x -q` | ❌ created in task | ⬜ pending |
| 03-03-01 | 03-03 | 2 | EVENT-01 | T-3-02 | Deploy-time CLI registers BACKWARD subjects idempotently; --check refuses an incompatible schema | unit | `.venv/bin/python -m pytest tests/events/test_schemas.py -x -q && .venv/bin/python -m libs.events.register --help` | ❌ created in task | ⬜ pending |
| 03-03-02 | 03-03 | 2 | EVENT-01 | T-3-56 | Ten PHI-free schemas match the catalogue one to one and round-trip | unit | `.venv/bin/python -m pytest tests/events -x -q` | ❌ created in task | ⬜ pending |
| 03-03-03 | 03-03 | 2 | EVENT-01 (secrets) | T-3-03 | 0600 gitignored infra secrets, paths-only output | unit | `.venv/bin/python -m pytest tests/tools/test_generate_infra_secrets.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-01 | 03-04 | 2 | OBS-02 | T-3-12 | Consumer span is CHILD of producer span; commit after effect; one persistent loop | integration | `.venv/bin/python -m pytest tests/events/test_consumer_base.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-02 | 03-04 | 2 | EVENT-05 | T-3-11, T-3-13, T-3-14 | Poison -> DLQ with type-only headers; transient never dead-lettered; failed DLQ write never commits | integration | `.venv/bin/python -m pytest tests/events/test_consumer_dlq.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-03 | 03-04 | 2 | EVENT-03 | T-3-12 | Forced redelivery -> one DB effect (claim in same txn) | integration | `.venv/bin/python -m pytest tests/events/test_consumer_idempotency.py -x -q` | ❌ created in task | ⬜ pending |
| 03-05-01 | 03-05 | 3 | EVENT-01, EVENT-02 | T-3-05, T-3-06, T-3-10 | Probe row -> Debezium -> Kafka with key/headers, Avro decodes; no :8083 host port | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-05-02 | 03-05 | 3 | EVENT-02 | T-3-07, T-3-08 | Per-connector task RUNNING, slot active, debezium reads outbox only (parametrized over every connector file) | static + e2e | `.venv/bin/python -m pytest tests/infra/test_cdc_config.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "connector_tasks or privilege or slots" -x -q` | ❌ created in task | ⬜ pending |
| 03-05-03 | 03-05 | 3 | EVENT-02 | T-3-09 | Connect stop/start in order, hard kill no loss (duplicates counted), snapshot covers pre-start rows | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-06-01 | 03-06 | 3 | NOTIF-02, EVENT-04 | T-3-30 | Timer fires -> real activity -> Celery -> sent in Mongo | workflow (time-skipping) | `.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-06-02 | 03-06 | 3 | NOTIF-02, NOTIF-03 | T-3-30 | Cancel stops, reschedule moves, past skipped, duplicate starts safe, follow-up once | workflow (time-skipping) | `.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -x -q` | ❌ created in task | ⬜ pending |
| 03-06-03 | 03-06 | 3 | NOTIF-02, NOTIF-03 | T-3-31 | Recorded histories replay | replay | `.venv/bin/python -m pytest tests/workflows/test_replay_reminder.py -x -q` | ❌ created in task | ⬜ pending |
| 03-07-01 | 03-07 | 4 | EVENT-01, OBS-02 | T-3-20 | Gateway booking -> appointment.booked on Kafka with request trace id, no coverage_ref | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-07-02 | 03-07 | 4 | EVENT-02, ANLY-02, ANLY-04 | T-3-20, T-3-21 | Six transitions atomic; accept path writes nothing; PHI/coverage_ref absent | integration | `.venv/bin/python -m pytest tests/scheduling/test_outbox.py -x -q` | ❌ created in task | ⬜ pending |
| 03-07-03 | 03-07 | 4 | EVENT-02, NOTIF-01 | T-3-21 | Cancel and reschedule each publish one ordered event; no container stopped or killed | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-08-01 | 03-08 | 4 | EVENT-01, ANLY-01 | T-3-24, T-3-58 | patient-outbox task RUNNING, outbox-only scope; patient.registered on Kafka with no PHI bytes | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "patient-outbox" -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_registry_events.py -m e2e -k registered -x -q` | ❌ created in task | ⬜ pending |
| 03-08-02 | 03-08 | 4 | EVENT-02, ANLY-01 | T-3-24, T-3-25 | patient events atomic, repeat no-op, PHI absent at the table | integration | `.venv/bin/python -m pytest tests/patient -x -q` | ❌ created in task | ⬜ pending |
| 03-08-03 | 03-08 | 4 | EVENT-01 | T-3-26, T-3-58 | provider-outbox task RUNNING; provider.schedule_changed with each feed row; feed unchanged | integration + e2e | `.venv/bin/python -m pytest tests/provider -x -q` | ❌ created in task | ⬜ pending |
| 03-09-01 | 03-09 | 4 | EVENT-01, EVENT-02 | T-3-27, T-3-29 | visit-outbox task RUNNING; completion -> outbox -> visit-downstream consumer starts workflow within 15 s | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-09-02 | 03-09 | 4 | EVENT-03, ANLY-03, ANLY-05 | T-3-27, T-3-28 | Redelivery starts one run; unknown visit DLQ; Temporal outage retried | integration | `.venv/bin/python -m pytest tests/visit -x -q` | ❌ created in task | ⬜ pending |
| 03-10-01 | 03-10 | 4 | ANLY-01, ANLY-06 | T-3-34 | Patient events on Kafka -> real handler -> v_active_patients (order-tolerant) | integration | `.venv/bin/python -m pytest tests/analytics/test_projections.py -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-10-02 | 03-10 | 4 | ANLY-01..05, EVENT-03 | T-3-35 | Fixed sequence -> exact metrics through the views, order-tolerant, idempotent, clinic-isolated | integration | `.venv/bin/python -m pytest tests/analytics/test_projections.py -x -q` | ❌ created in task | ⬜ pending |
| 03-11-01 | 03-11 | 5 | EVENT-02 | T-3-60 | Scheduling worker killed after cancel commit / Connect down -> event still delivered | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k "worker_kill or connect_restart" -x -q` | ❌ created in task | ⬜ pending |
| 03-11-02 | 03-11 | 5 | EVENT-02, EVENT-03 (D-03 gate) | T-3-61 | Kill API after commit / consumer down / Connect down -> one downstream start | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-11-03 | 03-11 | 5 | EVENT-02 (D-04 smoke) | T-3-60 | k6 smoke passes on a quiet stack; gateway budget restored | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q` | ❌ created in task | ⬜ pending |
| 03-12-01 | 03-12 | 6 | EVENT-02 (D-03) | T-3-38 | Live sweeper Schedule deleted; reconciler kept; poller still paused | workflow + e2e | `.venv/bin/python -m pytest tests/workflows/test_visit_schedules.py -x -q` | ❌ created in task | ⬜ pending |
| 03-12-02 | 03-12 | 6 | EVENT-03 (D-03) | T-3-40 | Sweep path removed; tombstone keeps replay safe | integration | `.venv/bin/python -m pytest tests/visit tests/workflows -x -q` | ❌ created in task | ⬜ pending |
| 03-12-03 | 03-12 | 6 | EVENT-02 (D-03) | T-3-39 | Backfill writes one event per unstarted visit, idempotent | integration | `.venv/bin/python -m pytest tests/visit/test_backfill.py -x -q` | ❌ created in task | ⬜ pending |
| 03-13-01 | 03-13 | 6 | ANLY-01, ANLY-06 | T-3-33 | Patient registered -> analytics-consumer container -> /api/analytics total_patients | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-13-02 | 03-13 | 6 | ANLY-01..06 | T-3-33, T-3-36, T-3-37 | RBAC per role/clinic; analytics imports no other service, one DB URL; e2e metrics | integration + e2e | `.venv/bin/python -m pytest tests/analytics/test_api.py tests/libs/test_service_isolation.py tests/gateway/test_routing.py -x -q` | ❌ created in task | ⬜ pending |
| 03-13-03 | 03-13 | 6 | ANLY-06 (D-10 rebuild) | T-3-59 | Replay rebuild reproduces the views; refuses under a live member | integration | `.venv/bin/python -m pytest tests/analytics/test_replay_rebuild.py -x -q` | ❌ created in task | ⬜ pending |
| 03-14-01 | 03-14 | 7 | NOTIF-01, NOTIF-02, EVENT-04 | T-3-42 | Booking -> one sent delivery and one provider record by Compose Celery worker; reminder running; non-admin broker user | e2e | `.venv/bin/python -m pytest tests/notification/test_broker_setup.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-14-02 | 03-14 | 7 | NOTIF-01..04, EVENT-03 | T-3-41 | Each event -> intended sends/reminder ops; stale guard; outages transient | unit | `.venv/bin/python -m pytest tests/notification -x -q` | ❌ created in task | ⬜ pending |
| 03-14-03 | 03-14 | 7 | NOTIF-01, NOTIF-03, EVENT-05 | T-3-43, T-3-44 | Cancel/reschedule one send and one provider record each; follow-up; live Kafka and Celery DLQs | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-15-01 | 03-15 | 8 | OBS-01 | T-3-45 | booking_saga_seconds scraped; temporal_ series scraped; Grafana provisioning clean | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-15-02 | 03-15 | 8 | OBS-01 | T-3-45, T-3-48 | All targets up (three Kafka consumers, Celery worker, three Temporal workers); no host ports | static + e2e | `.venv/bin/python -m pytest tests/infra/test_prometheus_config.py -x -q` | ❌ created in task | ⬜ pending |
| 03-16-01 | 03-16 | 9 | ANLY-01..05, OBS-01 | T-3-46 | Operations panels return the five metrics as grafana_ro; grafana_ro cannot write | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "operations or grafana_ro_cannot or provisioning_clean" -x -q` | ❌ created in task | ⬜ pending |
| 03-16-02 | 03-16 | 9 | OBS-01 | T-3-47 | Dashboards reference real datasources/metrics; NFR thresholds = nfr.json; slot query as grafana_ro | static + e2e | `.venv/bin/python -m pytest tests/infra -x -q` | ❌ created in task | ⬜ pending |
| 03-16-03 | 03-16 | 9 | OBS-01 (D-04) | T-3-46 | NFR histograms present; k6 smoke passes on finished stack | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q` | ❌ created in task | ⬜ pending |
| 03-17-01 | 03-17 | 10 | OBS-02 | T-3-49 | One Jaeger trace across gateway, scheduling, Temporal, Kafka consumer, Celery (CHILD_OF) | integration + e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_trace.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-17-02 | 03-17 | 10 | EVENT-03, EVENT-02 | T-3-50, T-3-51 | Offset rewind + Connect hard kill -> every effect still one | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_redelivery.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-17-03 | 03-17 | 10 | OBS-03 | T-3-49 | JSON logs with trace_id for five flows, no PHI; booking trace id in each process's logs | unit + e2e | `.venv/bin/python -m pytest tests/libs/test_observability.py -x -q` | ❌ created in task | ⬜ pending |
| 03-18-01 | 03-18 | 11 | DASH-03 | T-3-53 | Booking event flow documented; doc names resolve to code; no wire exactly-once claim | static | `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -x -q` | ❌ created in task | ⬜ pending |
| 03-18-02 | 03-18 | 11 | DASH-03, DASH-04 | T-3-52, T-3-53 | All flows, decisions (incl. EVENT-04 deviation), limitations (incl. real-provider idempotency caveat) in architecture md/html, README, .env.example | static | `.venv/bin/python -m pytest tests/docs -x -q` | ❌ created in task | ⬜ pending |
| 03-18-03 | 03-18 | 11 | DASH-03 | T-3-52 | ERD, NFR doc, load-test README current; visit docstrings no longer describe the sweeper as live | static | `.venv/bin/python -m pytest tests/docs tests/loadtest -x -q` | ❌ created in task | ⬜ pending |
| 03-19-01 | 03-19 | 12 | DASH-03 | T-3-54 | Booking flow SVG on the dashboard; marker v4 · Phase 3; self-contained | static | `.venv/bin/python -m pytest tests/docs/test_dashboard.py -x -q` | ❌ created in task | ⬜ pending |
| 03-19-02 | 03-19 | 12 | DASH-03, DASH-04 | T-3-54 | Remaining flows, nine decision cards, Q&A, roadmap, PROJECT.md rows | static | `.venv/bin/python -m pytest tests/docs -x -q` | ❌ created in task | ⬜ pending |
| 03-20-01 | 03-20 | 13 | all 20 | T-3-55 | PHASE3_TRACE for 20 ids plus EVENT-04 and NOTIF-04 notes | static | `.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -x -q` | ❌ created in task | ⬜ pending |
| 03-20-02 | 03-20 | 13 | all 20 (phase gate) | T-3-55, T-3-62 | Full non-e2e and e2e green on a rebuilt stack, Semgrep triaged; per-task check reads the gate logs (about 30 s) | static (gate logs) | `grep -qE "[0-9]+ passed" .../closeout-logs/non-e2e.txt && ... && .venv/bin/python -m pytest tests/docs -q` | ❌ created in task | ⬜ pending |
| 03-20-03 | 03-20 | 13 | OBS-01, OBS-02, DASH-03/04 | T-3-62 | Human: Jaeger trace, Grafana dashboards, dashboard HTML, manual k6 smoke | manual | `.venv/bin/python -m pytest tests/docs -q` | ❌ created in task | ⬜ pending |
| 03-21-01 | 03-21 | 14 | EVENT-03, EVENT-05 (gap 1, CR-01) | T-3-63, T-3-66 | Raw or SQLAlchemy-wrapped asyncpg 53300 / 57P03 / 40P01 in the visit handler -> TransientEventError -> RETRY_LATER at the real `_handle_message` (no commit, no DLQ); rebuilt visit-consumer still triggers | unit + integration + e2e | `docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/events/test_classify.py tests/visit/test_visit_consumer.py -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; } && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-21-02 | 03-21 | 14 | EVENT-05, NOTIF-01, ANLY-06 (WR-02, IN-01) | T-3-63 | Analytics holds events on DB resource errors (RETRY_LATER); notification Temporal UNAVAILABLE / INTERNAL / UNKNOWN transient (bounded by the cap), INVALID_ARGUMENT not; AST guard with a positive control rejects any classifier copy outside libs/events/classify.py | unit + static (AST) | `.venv/bin/python -m pytest tests/analytics/test_consumer_classification.py tests/analytics/test_projections.py tests/notification/test_triggers.py tests/events/test_classify.py -x -q` | ❌ created in task | ⬜ pending |
| 03-21-03 | 03-21 | 14 | EVENT-05 (WR-02, WR-03, WR-07) | T-3-63, T-3-64, T-3-65 | Cap -> TransientExhausted DLQ record (type-only headers) after both thresholds, including a Temporal INTERNAL handler; registry 408/429/5xx retried; non-fatal commit failure never crashes; three consumers rebuilt | integration (live broker) + e2e | `.venv/bin/python -m pytest tests/events -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-22-01 | 03-22 | 15 | EVENT-03, EVENT-05 (gap 1, CR-01, D-03) | T-3-67, T-3-68, T-3-70 | Live: a consumed-and-dropped visit.completed is restarted by the visit-reconciler (one execution, marker set, billing finalized, appointment completed), not by the consumer; no sweeper | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k "lost_visit_trigger or tracer or sweeper_schedule_retired" -x -q` | ❌ created in task | ⬜ pending |
| 03-22-02 | 03-22 | 15 | EVENT-03 | T-3-67, T-3-71 | Lost-trigger pass selective, idempotent, every harness call clinic-scoped; reconcile() wiring proven with recorders; session guard removes and fails on surviving NULL-marker harness visits; replay suite green | integration + replay | `docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit tests/workflows -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; }` | ❌ created in task | ⬜ pending |
| 03-22-03 | 03-22 | 15 | EVENT-02, EVENT-05 (WR-03 recovery) | T-3-69, T-3-72, T-3-73 | Backfill reports stale_queued and restarts a queued-but-dead-lettered visit on apply; `scripts/replay_dlq.py` dry run by default, --apply re-produces original key, value and headers to a catalogue original topic, never commits group offsets, ids-only output | integration (live broker) | `docker compose -f infra/docker-compose.yml stop visit-consumer && { .venv/bin/python -m pytest tests/visit/test_backfill.py tests/events/test_replay_dlq.py -x -q; rc=$?; docker compose -f infra/docker-compose.yml start --wait --wait-timeout 180 visit-consumer && test $rc -eq 0; } && .venv/bin/python scripts/backfill_visit_completed_events.py && .venv/bin/python scripts/replay_dlq.py --group visit-downstream` | ❌ created in task | ⬜ pending |
| 03-23-01 | 03-23 | 16 | EVENT-05 (WR-05) | T-3-74, T-3-75 | Sequential loops leave at most one backend alive; live loops never torn down from another loop | integration | `.venv/bin/python -m pytest tests/libs/test_postgres_registry.py tests/libs/test_observability.py -x -q` | ❌ created in task | ⬜ pending |
| 03-23-02 | 03-23 | 16 | EVENT-05, OBS-01 (WR-05) | T-3-74 | Pool sizes as settings; Compose worst case derived from Compose, Dockerfile CMD worker defaults, connector files, Temporal env and Grafana provisioning, <= max_connections - 3 and equal to the Compose comment; four long-running processes healthy; sampled peak below 270 | static + e2e + live sample | `.venv/bin/python -m pytest tests/libs/test_postgres_registry.py tests/libs/test_shared_settings.py tests/libs/test_config.py -x -q -s && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q && (sample file: at least 12 samples, peak total below 270)` | ❌ created in task | ⬜ pending |
| 03-24-01 | 03-24 | 17 | OBS-01 (gap 2, CR-02) | T-3-77 | Scheduling scrape sums both workers: 40 driven requests fully counted, zero resets, monotonic (red before, green after); scheduling-worker stays single-process | unit + e2e | `.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/libs/test_http_metrics.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "no_resets and scheduling" -x -q` | ❌ created in task | ⬜ pending |
| 03-24-02 | 03-24 | 17 | OBS-01 (gap 2, CR-02) | T-3-76, T-3-77, T-3-78 | Gateway /metrics from the multiprocess registry, still internal-only; per-pid files summed; 40 driven gateway requests fully counted, zero resets (red before, green after) | unit + e2e | `.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py tests/libs/test_http_metrics.py tests/gateway -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k "no_resets and (gateway or scheduling)" -x -q` | ❌ created in task | ⬜ pending |
| 03-25-01 | 03-25 | 18 | OBS-01 (gap 2, CR-02) | T-3-79 | Seven more APIs multiprocess; for all nine jobs driven traffic fully counted, zero resets, monotonic (seven red before rebuild); no Compose service carries the directory or a worker-count variable | static + e2e | `.venv/bin/python -m pytest tests/libs/test_multiprocess_metrics.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k no_resets -x -q` | ❌ created in task | ⬜ pending |
| 03-25-02 | 03-25 | 18 | OBS-01 | T-3-79 | pid 1 of the nine APIs has the directory variable, the seven single-process roles do not; every target up; metrics suite and Phase 2 flows green | static + e2e | `.venv/bin/python -m pytest tests/infra -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_phase2_flows.py -m e2e -q` | ❌ created in task | ⬜ pending |
| 03-25-03 | 03-25 | 18 | OBS-01 (D-04 smoke) | T-3-80, T-3-81 | k6 smoke exit 0 and nine zero resets read from recorded logs; credential file and plan.json deleted; accept-path cost of multiprocess mode recorded (I10) | e2e (manual run, checked from logs) | `tail -n 1 .../closeout-logs/gap-smoke.txt` is `k6_exit=0` `&&` (resets file: exactly the nine API jobs, each 0) `&& test ! -e loadtest/data/plan.json && .venv/bin/python -m pytest tests/docs/test_phase3_docs.py tests/loadtest -q` | ❌ created in task | ⬜ pending |
| 03-26-01 | 03-26 | 19 | DASH-03, DASH-04, EVENT-05 | T-3-83 | Guard tests reject the false outage claim in docs and dashboard; docs name only recovery tools that exist, per group; exactly three dashboard sentences changed | static | `.venv/bin/python -m pytest tests/docs -q` | ❌ created in task | ⬜ pending |
| 03-26-02 | 03-26 | 19 | all gap items | T-3-82, T-3-84 | Full non-e2e (known provider tracer failure deselected by node id) and e2e on the final stack: last summary line reports no failed or error test; lost-trigger e2e and nine no-reset cases PASSED; connection peak below 270; gateway restored to 20 / 300 as the last automated step | static (gate logs) + live | `! tail -n 1 .../gap-non-e2e.txt \| grep -qE "[0-9]+ (failed\|error)"` and the same for `gap-e2e.txt`, both with `[0-9]+ passed`, plus the PASSED, peak, `printenv` 20 / 300 and `pytest tests/docs -q` checks | ❌ created in task | ⬜ pending |
| 03-26-03 | 03-26 | 19 | DASH-03, DASH-04 | T-3-83 | Human: re-check exactly the three changed dashboard sentences (D-14) | manual | `.venv/bin/python -m pytest tests/docs/test_dashboard.py tests/docs/test_phase3_docs.py -q` | ❌ created in task | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Covered inside the plans (each test file is written by the task that first needs it):

- [ ] `tests/events/` outbox helper and mock registry fixture: 03-01 Task 3; schemas and registration: 03-03 Tasks 1-2; consumer base, Kafka fixtures, DLQ and idempotency: 03-04; trace chain: 03-17 Task 1
- [ ] `tests/notification/` with the throwaway RabbitMQ vhost helper, in-process and subprocess Celery workers: 03-02; triggers and broker provisioning: 03-14
- [ ] `tests/analytics/`: 03-10 (projections) and 03-13 (API, rebuild); `tests/infra/`: 03-05 Task 2, 03-15 Task 2 and 03-16 Task 2; `tests/e2e/test_phase3_*.py` and `tests/e2e/phase3_stack.py`: 03-05 onward
- [ ] `tests/workflows/histories/reminder_v1.json` and `followup_v1.json`: 03-06 Task 3
- [ ] Rebuild images, `alembic upgrade head` on the existing volumes, `wal_level=logical`, Connect up: 03-05 Task 1; patient, provider and visit outboxes and connectors: 03-08 Tasks 1 and 3, 03-09 Task 1
- [ ] Sweeper tests rewritten (`tests/visit/test_downstream.py`, `tests/workflows/test_visit_schedules.py`, `tests/visit/test_visit_reconciler.py`): 03-09 Task 2 and 03-12 Tasks 1-2
- [ ] `tests/docs/test_dashboard.py` updated: 03-19 Tasks 1-2; `PHASE3_TRACE` added and the phase gate run: 03-20 Tasks 1-2
- [ ] Gap closure (03-21..03-26): `tests/events/test_classify.py` with the AST single-classifier guard, `tests/events/test_consumer_reliability.py`, `tests/analytics/test_consumer_classification.py` (03-21); lost-trigger e2e, clinic-scoped harness tests, the `tests/visit/conftest.py` leftover guard and `tests/events/test_replay_dlq.py` (03-22); `tests/libs/test_postgres_registry.py` with the derived Compose budget (03-23); `tests/libs/test_multiprocess_metrics.py` and the no-reset e2e with driven traffic (03-24, extended to nine jobs and pid-1 checks in 03-25); docs guard tests (03-26). Each is created by the task that needs it.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| One booking trace viewed in the Jaeger UI | OBS-02 | Visual | Book an appointment, open Jaeger, confirm one trace spans gateway, scheduling, Temporal, Kafka consumer and Celery |
| Grafana dashboards render | OBS-01 | Visual | Open the provisioned dashboards, confirm panels have data |
| Architecture dashboard visual quality | DASH-03/04 | Visual | Open `docs/architecture-dashboard.html` offline and at narrow width |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency under 30 s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
