---
phase: "3"
slug: "event-driven-system-observability"
status: draft
nyquist_compliant: false
wave_0_complete: false
created: "2026-11-10"
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Plan and wave columns are filled in once plans exist; the commands come from 03-RESEARCH.md "Validation Architecture".

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
| 03-TBD-01 | TBD | TBD | EVENT-01 | PHI-free events / no secrets | Every `.avsc` registers and is BACKWARD compatible; no PHI fields | unit/integration | `pytest tests/events/test_schemas.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-02 | TBD | TBD | EVENT-02 | PHI-free events / no secrets | Transition + outbox row commit atomically; rollback leaves no row; double call is a no-op (uuid5 PK) | integration | `pytest tests/scheduling/test_outbox.py tests/visit/test_outbox.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-03 | TBD | TBD | EVENT-02 | PHI-free events / no secrets | Commit then kill, Connect stop/start: delivered once in order | e2e | `pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e` | ❌ W0 | ⬜ pending |
| 03-TBD-04 | TBD | TBD | EVENT-03 | PHI-free events / no secrets | Forced redelivery yields one effect per consumer | integration + e2e | `pytest tests/events/test_consumer_idempotency.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-05 | TBD | TBD | EVENT-04 | PHI-free events / no secrets | Task runs on the worker via RabbitMQ (eager mode unit; real broker e2e) | unit + e2e | `pytest tests/notification/test_tasks.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-06 | TBD | TBD | EVENT-05 | PHI-free events / no secrets | Retry with backoff; poison -> `notifications.dlq`; Kafka poison -> DLQ topic | integration | `pytest tests/notification/test_dlq.py tests/events/test_consumer_dlq.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-07 | TBD | TBD | NOTIF-01 | PHI-free events / no secrets | booked/cancelled/rescheduled events produce one task each; booking transaction unaffected | integration | `pytest tests/notification/test_triggers.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-08 | TBD | TBD | NOTIF-02/03 | PHI-free events / no secrets | Reminder fires at lead, cancel stops it, reschedule moves it, replay of recorded history | workflow (time-skipping) | `pytest tests/workflows/test_reminder_workflow.py tests/workflows/test_replay_reminder.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-09 | TBD | TBD | NOTIF-04 | PHI-free events / no secrets | Same task twice sends once (Mongo unique index) | unit | `pytest tests/notification/test_dedupe.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-10 | TBD | TBD | ANLY-01..05 | PHI-free events / no secrets | Projection math for each metric from a fixed event sequence (incl. out-of-order, D-10 definition) | unit/integration | `pytest tests/analytics/test_projections.py tests/analytics/test_api.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-11 | TBD | TBD | ANLY-06 | PHI-free events / no secrets | Analytics process never imports/queries an OLTP DB; replay rebuilds identical tables | integration | `pytest tests/analytics/test_replay_rebuild.py tests/libs/test_service_isolation.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-12 | TBD | TBD | OBS-01 | PHI-free events / no secrets | Every scrape target answers `/metrics`; dashboards JSON parse; provisioning files reference existing UIDs | unit + e2e | `pytest tests/infra/test_grafana_provisioning.py tests/e2e/test_phase3_metrics.py` | ❌ W0 | ⬜ pending |
| 03-TBD-13 | TBD | TBD | OBS-02 | PHI-free events / no secrets | One trace id across HTTP -> Temporal -> outbox -> consumer -> Celery (in-memory exporter; real Jaeger query in e2e) | integration + e2e | `pytest tests/events/test_trace_chain.py tests/e2e/test_phase3_trace.py` | ❌ W0 | ⬜ pending |
| 03-TBD-14 | TBD | TBD | OBS-03 | PHI-free events / no secrets | Log lines for each flow carry `trace_id`, no PHI | unit | `pytest tests/libs/test_observability.py -x` | ❌ W0 | ⬜ pending |
| 03-TBD-15 | TBD | TBD | D-03 | PHI-free events / no secrets | Visit completion starts downstream via consumer; sweeper Schedule absent; backfill | integration + e2e | `pytest tests/visit/test_downstream.py tests/e2e/test_phase3_visit_downstream.py` | ❌ W0 | ⬜ pending |
| 03-TBD-16 | TBD | TBD | D-04 | PHI-free events / no secrets | `smoke` k6 profile still passes; histograms exist | e2e | `tests/e2e/test_loadtest_smoke.py` | ❌ W0 | ⬜ pending |
| 03-TBD-17 | TBD | TBD | DASH-03/04 | PHI-free events / no secrets | New sections/diagrams/Q&A present, self-contained | static | `pytest tests/docs -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/events/` (schemas, outbox helper, consumer base, trace chain), `tests/notification/`, `tests/analytics/`, `tests/infra/`, `tests/e2e/test_phase3_*.py`
- [ ] Mock Schema Registry fixture (`mock://`), Kafka fixtures (unique topic and group per test), eager-mode Celery fixture, `tests/workflows/histories/reminder_v1.json`
- [ ] Rebuild images, `alembic upgrade head` on live DBs, `wal_level=logical`, Connect up
- [ ] Update `tests/docs/test_dashboard.py`, add `PHASE3_TRACE` to the traceability test, rewrite the sweeper tests (`tests/visit/test_downstream.py`, `tests/workflows/test_visit_schedules.py`)

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
