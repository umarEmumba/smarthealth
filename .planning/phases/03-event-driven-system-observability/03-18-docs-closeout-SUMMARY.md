---
phase: 03-event-driven-system-observability
plan: 18
subsystem: docs
tags: [documentation, architecture, erd, nfr, docs-to-code-test, decision-record]
requires:
  - phase: 03-event-driven-system-observability
    provides: "plans 03-01..03-17: the event backbone, consumers, notification pipeline, analytics, observability and the measured evidence quoted here"
provides:
  - "docs/architecture.md section 10 (event backbone, delivery semantics, cancel/reschedule, visit lifecycle, notifications and reminders, analytics, observability, decisions, limitations) and corrected earlier sections"
  - "docs/architecture.html mirror, README (containers, /api/analytics, Phase 3 settings, prerequisites, limitations), .env.example ANALYTICS_DATABASE_URL"
  - "docs/erd.md and docs/erd.html: outbox tables, analytics_db, processed_events, Mongo notification collections, migration chain"
  - "docs/nfr.md and loadtest/README.md: metric and panel per NFR, dated Phase 3 smoke notes"
  - "seven visit docstrings that no longer describe the sweeper as live"
  - "tests/docs/test_phase3_docs.py: 73 tests tying document names to the code"
affects: [03-19, 03-20]
key-files:
  created:
    - tests/docs/test_phase3_docs.py
  modified:
    - docs/architecture.md
    - docs/architecture.html
    - docs/erd.md
    - docs/erd.html
    - docs/nfr.md
    - loadtest/README.md
    - README.md
    - .env.example
    - services/visit/app.py
    - services/visit/api/visits.py
    - services/visit/repository/models.py
    - services/visit/workflows/__init__.py
    - services/visit/workflows/reconciler.py
    - services/visit/workflows/visit_completed.py
    - services/visit/activities/__init__.py
key-decisions:
  - "Phase 3 lives in one new top-level section 10 of architecture.md (10.1 to 10.9); sections 1 to 9 keep their Phase 1.1 to 2.1 text and were corrected only where Phase 3 made a statement false, with pointers to section 10"
  - "The docs state the delivery guarantee as no loss, at-least-once on the wire, effect-level exactly-once, with the measured duplicate counts, and a test rejects any sentence that says exactly-once without saying effect or denying it"
  - "Every limitation the executions collected is stated (real-provider idempotency key, reminder's long-lived trace, histogram bucket caveat, no authentication on the dev network, containers as root, unmeasured NFRs, scope of 'every state change')"
requirements-completed: []
status: complete
duration: not measured
completed: 2026-11-11
actuals:
  tokens: 43200
  tasks: 3
  commits: 4
plan_head_before: b65dde4d18cada8494ad90b645c6517c2e645875
plan_head_after: 42b34c68c0bafa9bd738056a1f435f8e31ae2979
---

# Phase 3 Plan 18: Docs Closeout Summary

**The Phase 3 system is documented as built, with its decisions, deviations and limits stated plainly, and 73 tests keep every topic, group, Compose service, event type, queue, setting, migration, view, collection, dashboard and metric name the documents use tied to the code. REQUIREMENTS.md was not edited and no requirement was ticked (`requirements-completed` is empty by instruction).**

The plan addresses DASH-03, DASH-04, EVENT-01 and OBS-01.

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 8249306 | architecture.md 10.1 (mermaid booking flow gateway to Kafka to consumers, topics, event set, outbox, CDC relay, Compose services added in Phase 3) and `tests/docs/test_phase3_docs.py` (first 49 tests) |
| 2 | 247b0b6 | architecture.md 10.2 to 10.9 and corrected earlier sections, architecture.html, README, `.env.example`; docs tests for the README settings table, stale claims and metric names |
| 3 | b811e45 | erd.md, erd.html, nfr.md, loadtest/README.md, seven visit docstrings; tests for migrations, views, collections, dashboards and visit docstrings |
| follow-up | 42b34c6 | one sentence: which dev event ports are published (Kafka 9092 and Schema Registry 8081 are; the Connect REST API is not) |

The tracer gate: Task 1's `<verify>` passed after the commit (49 passed), so expansion proceeded.

## What the documents now say (all checked against the code or a plan SUMMARY, not the plan text)

- **Flows**: booking (mermaid sequence with the reminder start), cancel and reschedule, visit lifecycle, notifications and reminders, analytics, observability chain, each as built. Synchronous work added to booking is the outbox insert inside confirm only; the 202 path writes no outbox row.
- **Delivery semantics**: no loss, at-least-once on the wire, effect-level exactly-once. Measured evidence quoted from the SUMMARYs: 5 duplicates of 11 messages after a Connect SIGKILL (03-05), 2 copies of one event id with 1 distinct id (03-17), offset rewinds of all three groups with identical effects (03-17), kill-after-commit and outage runs (03-11).
- **Decisions**: CDC over a polling publisher with the cost accepted; the Connect image deviation with the user's answer ("Accept all six (Recommended)", plan 03-01); the EVENT-04 deviation (projections on the `analytics-projection` Kafka consumer, with the reason; the REQUIREMENTS.md wording is recorded as the user's decision and was not edited); Avro with strict service-side serialization; topic per aggregate; three idempotency flavours; Temporal reminders versus Celery ETA and beat; Celery threads pool and RabbitMQ 4.3 settings; classic versus quorum queues; dedicated broker user; sweeper retirement (D-03).
- **Remaining safety nets** (hold reaper, pending-booking starter, both reconcilers, the paused schedule-change poller, the notification stub activities, `auto-setup`), each with why events do not subsume it.
- **Start policy (D-13)**: `ANALYTICS_CONSUMER_START` and `NOTIFICATION_CONSUMER_START` are `latest` in Compose with pinned go-live offsets; metrics count from go-live; `total_patients` counts only patients registered after go-live; a rebuild replays everything still retained, including throwaway test events; the operator procedure (stop consumer, dry run, `--apply`, start consumer) is written out; `processed_events` has no retention.
- **Metric definitions**: UTC days; cancellation rate = cancelled / booked over appointments booked in the window; wait = check-in to visit start; an empty denominator is no data (`null`).
- **Operations notes**: replication slots retain WAL while Connect is down (`max_slot_wal_keep_size=2GB`, deleting a connector does not drop its slot, the drop command); the heartbeat action query from 03-17 with the before and after retained WAL (`dbz_provider` 83 to 87 MB frozen to 2.4 to 5 MB moving, then 1.4 MB idle; 517 MB was the orchestrator's earlier figure); the outbox is insert-only with a documented prune statement that is explicitly not exercised; Schema Registry as a cached write-path dependency; no authentication on the dev event network (production: SASL/TLS, ACLs, Connect REST auth); `temporalio/auto-setup` is non-production; containers run as root and the Semgrep missing-user findings are accepted for now by the user; consumers' and workers' metrics ports are internal only; `rabbitmq-init` provisions vhost `smarthealth` and the non-admin user `smarthealth_notification`; `NOTIFICATION_MAX_EVENT_AGE_MINUTES`; the backfill command (dry run found 0 candidates); the gateway rate budgets are a test-time setting.
- **Honesty items**: the real-provider idempotency-key caveat (stated in architecture.md, README and architecture.html); the reminder's long-lived trace (the later firing was not exercised); the one Jaeger trace (`e839d1a9ec7d48e435d09abb68881b00`, 57 spans, 12 services); the HTTP bucket caveat (an edge on every NFR threshold makes above or below reliable, not the exact quantile; only scheduling was rebuilt with the new buckets); `provider.schedule_changed` published but not consumed and the poller paused; S3 audit-via-outbox deferred; "every state change" scoped to the ten D-07 event types; event-fed local copies were NOT built (an older README and architecture claim that Phase 3 would remove the synchronous reference checks was corrected).
- **Smoke and NFRs**: three smoke runs recorded as dated notes (baseline 2026-11-10: accept p95 21.8 ms and p99 78.9 ms; after producers 29.3 and 170 ms; finished stack 49.1 and 200.8 ms; saga p95 about 0.52 s each time; every threshold passed; host load differed; accept latency crept up and is the number to watch). `sustained`, `burst` and `contention` are stated as not run (deferred by the user), so NFR-01..08 remain unmeasured targets. `docs/nfr.md` gained a table naming the metric and Grafana panel per NFR (no panel exists for NFR-01, 06, 07, 08, and it says so); `loadtest/README.md` points at `smarthealth-booking-nfr`. History was not rewritten: the 2026-11-10 notes are kept.

## Verification (measured)

- `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -q`: **73 passed** (plan: at least 4).
- `.venv/bin/python -m pytest tests/docs tests/loadtest -q`: **232 passed** (includes the NFR single-source consistency test and the Phase 2/2.1 docs tests).
- `.venv/bin/python -m pytest tests/workflows/test_visit_schedules.py -q`: 5 passed. `tests/workflows/test_replay_visit_completion.py` and `test_replay_visit_billing.py`: 19 passed, 2 skipped (the opt-in recording tests). `tests/libs/test_service_isolation.py`: 17 passed.
- `.venv/bin/python -c "import services.visit.app, services.visit.workflows, services.visit.workflows.reconciler, ..."` for all seven edited visit modules: imports fine.
- Acceptance greps: `effect-level exactly-once` in architecture.md 2; `at-least-once` in README.md 1; `max_slot_wal_keep_size` in architecture.md 2; `/api/analytics` in README.md 5; `hold reaper|pending-starter` in architecture.md 7; `idempotency key` in architecture.md 3 and README.md 2; `EVENT-04` in architecture.md 2; `ANALYTICS_DATABASE_URL` in `.env.example` 1 (the harness accepted the edit); the five migration names in erd.md 10; `booking_saga_seconds` in nfr.md 1; `notification_deliveries` in erd.md 3; `grep -c sequenceDiagram docs/architecture.md` 12 versus 9 at the base commit.
- Secret scan of the diff for key material and password literals: nothing found (T-3-52). Only placeholders (`changeme-...-local-dev-only`) and secret file paths appear.
- **Not run**: the plan's full `pytest tests -m "not e2e"` (the orchestrator scoped this plan to its named tests plus `tests/docs`). The known unrelated failures `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` and `tests/identity/test_email_migration.py` were not run or touched. No stack was started, stopped, rebuilt or restarted; no `.env*` or `infra/secrets/*` content was read (the docs test reads `.env.example`, the committed template, and skips every `secrets` directory).

## Deviations from Plan

1. **[Rule 1/2 - stale documentation] Visit docstrings in seven files, not three.** The plan's `files_modified` names `app.py`, `workflows/__init__.py` and `workflows/reconciler.py`. Its own success criterion ("no in-code docstring describes the sweeper as live") also needed `api/visits.py` ("the Phase 2 visit sweeper still runs as a safety net until plan 03-12"), `repository/models.py` ("plan 02-11's sweeper finds completed visits"), `workflows/visit_completed.py` ("Phase 3 replaces the trigger") and `activities/__init__.py` ("the lost-trigger sweep"). All edits are docstring text only (checked by reading the diff: every changed line sits inside a docstring; all modules import; the visit replay and schedule tests pass). The acceptance line "`git diff HEAD --stat -- services/visit` lists only the three docstring files" is therefore not literally met; the diff is docstring-only. The historical migration docstring (`visit_0001`) was left as history.
2. **[Rule 1 - doc correction] The plan 03-17 SUMMARY called the heartbeat action query a "read-only function call".** `pg_logical_emit_message` writes a small WAL message (that is why it advances the slot). The docs say it writes a tiny WAL message into the monitored database (no table, no migration, no event).
3. **[Scope, same intent] Earlier sections of architecture.md, README, architecture.html and the ERD that Phase 3 made false were corrected** (Kafka and Celery "scaffolding only", the sweeper as a live trigger, "workers expose no metrics", "Phase 3 replaces the starter", "event-fed local copies in Phase 3", Grafana "dashboards not built yet", the Phase 2.1 "Phase 3 observability measures them", the visit completion diagram in 4b). Without this the new section would have contradicted the old text; the tests now fail if those claims return. The Phase 2.1 docs tests were not weakened (the "not yet measured" wording stays).
4. **[Test design] Name checks:** the Compose services are checked through a table headed "Compose service" in section 10.1 (forward and reverse, so a new Phase 3 service must be listed); settings are ALL_CAPS tokens with an underscore in backticks, matched case-insensitively against the code (so a Pydantic field name counts); `CHILD_OF` (a Jaeger reference type) and three Temporal enum names are on a short allowlist with reasons. The exactly-once checker has its own unit test showing it rejects a wire-level claim.
5. **[Plan wording] The plan's 10-step list names "Phase 3 sections" by heading;** they are `## 10. Phase 3: event backbone, ...` to the end of the file (the closing "See ..." pointer was moved after them).
6. **Not touched by design:** `docs/architecture-dashboard.html` (plan 03-19), `.planning/REQUIREMENTS.md`, any service code, Compose, `loadtest/nfr.json`.

## Known Stubs

None. The documents contain no placeholder text; the fake notification provider is described as the phase's intended provider with its limits.

## Threat Flags

None. No endpoint, auth path or schema changed (docstrings and documents only). T-3-52: the diff was scanned for key material; secret files are referenced by path only. T-3-53: `test_phase3_docs.py` ties names to code and rejects a wire-level exactly-once claim; measured numbers are quoted from the SUMMARYs; the limitations list includes the real-provider idempotency-key caveat and the EVENT-04 deviation with the user's answer.

## Notes for plans 03-19 and 03-20

- The dashboard (03-19) can take its Phase 3 text from architecture.md section 10 (flows 10.1 to 10.7, decisions 10.8, limitations 10.9) and the evaluation answers from 10.2, 10.5 and 10.8. The trace id and span count for the human Jaeger check are in 10.7 and the 03-17 SUMMARY.
- The EVENT-04 wording in `REQUIREMENTS.md` still lists projections among Celery tasks; the docs record the deviation and leave the wording to the user, as agreed at plan 03-01.

## Self-Check: PASSED

- FOUND: tests/docs/test_phase3_docs.py; docs/architecture.md section `## 10. Phase 3: event backbone`; docs/erd.md `analytics_db` section; the edited visit modules import.
- FOUND commits 8249306, 247b0b6, b811e45 and 42b34c6 on gsd/phase-02-scheduling-service-workflow; `git rev-list --count b65dde4..HEAD` was 4 before this SUMMARY; the working tree has no uncommitted tracked changes.
