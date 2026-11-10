---
phase: "3"
slug: "event-driven-system-observability"
status: draft
nyquist_compliant: false
wave_0_complete: false
created: "2026-11-10"
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Task ids are `<plan>-<task>` from the 15 PLAN files (filled by the planner 2026-11-10); every task carries an automated command, and each test file is created by the task that needs it (no separate Wave 0 plan).

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
| 03-01-01 | 03-01 | 1 | (package gate) | T-3-SC | Pins installed only after human approval | checkpoint | `.venv/bin/python -m pip --version` | ❌ created in task | ⬜ pending |
| 03-01-02 | 03-01 | 1 | EVENT-01 | T-3-02 | D-05 contract confirmed before any schema exists | checkpoint | `test -f .../03-CONTEXT.md` | ❌ created in task | ⬜ pending |
| 03-01-03 | 03-01 | 1 | EVENT-02, OBS-02 | T-3-01, T-3-04 | Outbox row atomic, uuid5 no-op repeat, traceparent from active span, never commits | integration | `.venv/bin/python -m pytest tests/events/test_outbox_roundtrip.py -x -q` | ❌ created in task | ⬜ pending |
| 03-01-04 | 03-01 | 1 | EVENT-01 | T-3-01, T-3-03 | 10 PHI-free schemas, BACKWARD registration CLI, 0600 gitignored secrets | unit | `.venv/bin/python -m pytest tests/events tests/tools/test_generate_infra_secrets.py -x -q` | ❌ created in task | ⬜ pending |
| 03-02-01 | 03-02 | 2 | EVENT-01, EVENT-02 | T-3-05, T-3-06, T-3-10 | Probe row -> Debezium -> Kafka with key/headers, Avro decodes; no :8083 host port | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-02-02 | 03-02 | 2 | EVENT-02 | T-3-07, T-3-08 | Four connector TASKS RUNNING; debezium role reads outbox only | static + e2e | `.venv/bin/python -m pytest tests/infra/test_cdc_config.py -x -q` | ❌ created in task | ⬜ pending |
| 03-02-03 | 03-02 | 2 | EVENT-02 | T-3-09 | Connect stop/start in order, hard kill no loss (duplicates counted), snapshot covers pre-start rows | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-03-01 | 03-03 | 2 | OBS-02 | T-3-12 | Consumer span is CHILD of producer span; commit after effect; one persistent loop | integration | `.venv/bin/python -m pytest tests/events/test_consumer_base.py -x -q` | ❌ created in task | ⬜ pending |
| 03-03-02 | 03-03 | 2 | EVENT-05 | T-3-11, T-3-13, T-3-14 | Poison -> DLQ with type-only headers; transient never dead-lettered; failed DLQ write never commits | integration | `.venv/bin/python -m pytest tests/events/test_consumer_dlq.py -x -q` | ❌ created in task | ⬜ pending |
| 03-03-03 | 03-03 | 2 | EVENT-03 | T-3-12 | Forced redelivery -> one DB effect (claim in same txn) | integration | `.venv/bin/python -m pytest tests/events/test_consumer_idempotency.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-01 | 03-04 | 2 | EVENT-04, OBS-02 | T-3-15, T-3-16 | Send runs on threads worker over live RabbitMQ, same trace, json only | integration | `.venv/bin/python -m pytest tests/notification/test_tasks.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-02 | 03-04 | 2 | NOTIF-04 | T-3-18 | Duplicate send -> one provider call; retry with backoff; closed reminder skipped | integration | `.venv/bin/python -m pytest tests/notification/test_dedupe.py -x -q` | ❌ created in task | ⬜ pending |
| 03-04-03 | 03-04 | 2 | EVENT-05 | T-3-17, T-3-19 | Exhausted/invalid -> Reject(requeue=False) -> notifications.dlq; least-privilege broker user | integration | `.venv/bin/python -m pytest tests/notification -x -q` | ❌ created in task | ⬜ pending |
| 03-05-01 | 03-05 | 3 | EVENT-01, OBS-02 | T-3-20 | Gateway booking -> appointment.booked on Kafka with request trace id, no coverage_ref | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-05-02 | 03-05 | 3 | EVENT-02, ANLY-02, ANLY-04 | T-3-20, T-3-21 | Six transitions atomic; accept path writes nothing; PHI/coverage_ref absent | integration | `.venv/bin/python -m pytest tests/scheduling/test_outbox.py -x -q` | ❌ created in task | ⬜ pending |
| 03-05-03 | 03-05 | 3 | EVENT-02, NOTIF-01 | T-3-22 | Kill committing worker / Connect down -> event still delivered; k6 smoke passes | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-06-01 | 03-06 | 3 | EVENT-01, ANLY-01 | T-3-24 | patient.registered on Kafka with no PHI bytes | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_registry_events.py -m e2e -k registered -x -q` | ❌ created in task | ⬜ pending |
| 03-06-02 | 03-06 | 3 | EVENT-02, ANLY-01 | T-3-24, T-3-25 | patient events atomic, repeat no-op, PHI absent at the table | integration | `.venv/bin/python -m pytest tests/patient -x -q` | ❌ created in task | ⬜ pending |
| 03-06-03 | 03-06 | 3 | EVENT-01 | T-3-26 | provider.schedule_changed with each feed row; feed unchanged | integration + e2e | `.venv/bin/python -m pytest tests/provider -x -q` | ❌ created in task | ⬜ pending |
| 03-07-01 | 03-07 | 3 | EVENT-01, EVENT-02 | T-3-27 | Visit completion -> outbox -> visit-downstream consumer starts workflow within 15 s | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-07-02 | 03-07 | 3 | EVENT-03, ANLY-03, ANLY-05 | T-3-27, T-3-28 | Redelivery starts one run; unknown visit DLQ; Temporal outage retried | integration | `.venv/bin/python -m pytest tests/visit -x -q` | ❌ created in task | ⬜ pending |
| 03-07-03 | 03-07 | 3 | EVENT-02 (D-03 gate) | T-3-28 | Kill API after commit / consumer down / Connect down -> one downstream start | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-08-01 | 03-08 | 3 | NOTIF-02, EVENT-04 | T-3-30 | Timer fires -> real activity -> Celery -> sent in Mongo | workflow (time-skipping) | `.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-08-02 | 03-08 | 3 | NOTIF-02, NOTIF-03 | T-3-30 | Cancel stops, reschedule moves, past skipped, duplicate starts safe, follow-up once | workflow (time-skipping) | `.venv/bin/python -m pytest tests/workflows/test_reminder_workflow.py -x -q` | ❌ created in task | ⬜ pending |
| 03-08-03 | 03-08 | 3 | NOTIF-02, NOTIF-03 | T-3-31 | Recorded histories replay | replay | `.venv/bin/python -m pytest tests/workflows/test_replay_reminder.py -x -q` | ❌ created in task | ⬜ pending |
| 03-09-01 | 03-09 | 4 | ANLY-01, ANLY-06 | T-3-36 | Patient registered -> projection -> /api/analytics total_patients | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-09-02 | 03-09 | 4 | ANLY-01..05, EVENT-03 | T-3-35 | Fixed sequence -> exact metrics, order-tolerant, idempotent; replay rebuild identical | integration | `.venv/bin/python -m pytest tests/analytics/test_projections.py tests/analytics/test_replay_rebuild.py -x -q` | ❌ created in task | ⬜ pending |
| 03-09-03 | 03-09 | 4 | ANLY-06 | T-3-33, T-3-34, T-3-36 | RBAC per role/clinic; analytics imports no other service, one DB URL | integration + e2e | `.venv/bin/python -m pytest tests/analytics tests/libs/test_service_isolation.py tests/gateway/test_routing.py -x -q` | ❌ created in task | ⬜ pending |
| 03-10-01 | 03-10 | 4 | EVENT-02 (D-03) | T-3-38 | Live sweeper Schedule deleted; reconciler kept; poller still paused | workflow + e2e | `.venv/bin/python -m pytest tests/workflows/test_visit_schedules.py -x -q` | ❌ created in task | ⬜ pending |
| 03-10-02 | 03-10 | 4 | EVENT-03 (D-03) | T-3-40 | Sweep path removed; tombstone keeps replay safe | integration | `.venv/bin/python -m pytest tests/visit tests/workflows -x -q` | ❌ created in task | ⬜ pending |
| 03-10-03 | 03-10 | 4 | EVENT-02 (D-03) | T-3-39 | Backfill writes one event per unstarted visit, idempotent | integration | `.venv/bin/python -m pytest tests/visit/test_backfill.py -x -q` | ❌ created in task | ⬜ pending |
| 03-11-01 | 03-11 | 5 | NOTIF-01, NOTIF-02, EVENT-04 | T-3-42 | Booking -> one sent delivery by Compose Celery worker; reminder running | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-11-02 | 03-11 | 5 | NOTIF-01..04, EVENT-03 | T-3-41 | Each event -> intended sends/reminder ops; stale guard; outages transient | unit | `.venv/bin/python -m pytest tests/notification -x -q` | ❌ created in task | ⬜ pending |
| 03-11-03 | 03-11 | 5 | NOTIF-01, NOTIF-03, EVENT-05 | T-3-43, T-3-44 | Cancel/reschedule one send each; follow-up; live Kafka and Celery DLQs | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_notifications.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-12-01 | 03-12 | 6 | OBS-01 | T-3-45 | booking_saga_seconds scraped; temporal_ series scraped; Grafana provisioning clean | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py -m e2e -k tracer -x -q` | ❌ created in task | ⬜ pending |
| 03-12-02 | 03-12 | 6 | OBS-01, ANLY-01..05 | T-3-46, T-3-47, T-3-48 | All targets up; dashboards reference real datasources/metrics; NFR thresholds = nfr.json | static + e2e | `.venv/bin/python -m pytest tests/infra -x -q` | ❌ created in task | ⬜ pending |
| 03-12-03 | 03-12 | 6 | OBS-01 (D-04) | T-3-45 | NFR histograms present; k6 smoke passes on finished stack | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_metrics.py tests/e2e/test_loadtest_smoke.py -m e2e -q` | ❌ created in task | ⬜ pending |
| 03-13-01 | 03-13 | 7 | OBS-02 | T-3-49 | One Jaeger trace across gateway, scheduling, Temporal, Kafka consumer, Celery (CHILD_OF) | integration + e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_trace.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-13-02 | 03-13 | 7 | EVENT-03, EVENT-02 | T-3-50, T-3-51 | Offset rewind + Connect hard kill -> every effect still one | e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_redelivery.py -m e2e -x -q` | ❌ created in task | ⬜ pending |
| 03-13-03 | 03-13 | 7 | OBS-03 | T-3-49 | JSON logs with trace_id for five flows, no PHI; booking trace id in each process's logs | unit + e2e | `.venv/bin/python -m pytest tests/libs/test_observability.py -x -q` | ❌ created in task | ⬜ pending |
| 03-14-01 | 03-14 | 8 | DASH-03 | T-3-53 | Booking event flow documented; doc names resolve to code; no wire exactly-once claim | static | `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -x -q` | ❌ created in task | ⬜ pending |
| 03-14-02 | 03-14 | 8 | DASH-03, DASH-04 | T-3-52, T-3-53 | All flows, decisions, limitations in architecture md/html and README | static | `.venv/bin/python -m pytest tests/docs -x -q` | ❌ created in task | ⬜ pending |
| 03-14-03 | 03-14 | 8 | DASH-03 | T-3-52 | ERD, NFR doc, load-test README current | static | `.venv/bin/python -m pytest tests/docs tests/loadtest -x -q` | ❌ created in task | ⬜ pending |
| 03-15-01 | 03-15 | 9 | DASH-03 | T-3-54 | Booking flow SVG on the dashboard; marker v4 · Phase 3; self-contained | static | `.venv/bin/python -m pytest tests/docs/test_dashboard.py -x -q` | ❌ created in task | ⬜ pending |
| 03-15-02 | 03-15 | 9 | DASH-03, DASH-04 | T-3-54 | Remaining flows, decisions, Q&A, roadmap, PROJECT.md rows | static | `.venv/bin/python -m pytest tests/docs -x -q` | ❌ created in task | ⬜ pending |
| 03-15-03 | 03-15 | 9 | all 20 | T-3-55 | PHASE3_TRACE; full non-e2e and e2e green | static + full | `.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -x -q` | ❌ created in task | ⬜ pending |
| 03-15-04 | 03-15 | 9 | OBS-01, OBS-02, DASH-03/04 | T-3-54 | Human: Jaeger trace, Grafana dashboards, dashboard HTML, manual k6 smoke | manual | `.venv/bin/python -m pytest tests/docs -q` | ❌ created in task | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Covered inside the plans (each test file is written by the task that first needs it):

- [ ] `tests/events/` schemas, outbox helper and mock registry fixture: 03-01 Tasks 3-4; consumer base, Kafka fixtures, DLQ and idempotency: 03-03; trace chain: 03-13 Task 1
- [ ] `tests/notification/` with the throwaway RabbitMQ vhost helper and in-process Celery worker: 03-04; triggers: 03-11
- [ ] `tests/analytics/`: 03-09; `tests/infra/`: 03-02 Task 2 and 03-12 Task 2; `tests/e2e/test_phase3_*.py` and `tests/e2e/phase3_stack.py`: 03-02 onward
- [ ] `tests/workflows/histories/reminder_v1.json` and `followup_v1.json`: 03-08 Task 3
- [ ] Rebuild images, `alembic upgrade head` on the existing volumes, `wal_level=logical`, Connect up: 03-02 Task 1
- [ ] Sweeper tests rewritten (`tests/visit/test_downstream.py`, `tests/workflows/test_visit_schedules.py`, `tests/visit/test_visit_reconciler.py`): 03-07 Task 2 and 03-10 Tasks 1-2
- [ ] `tests/docs/test_dashboard.py` updated and `PHASE3_TRACE` added: 03-15 Tasks 1-3

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
