---
phase: 03-event-driven-system-observability
plan: 18
type: execute
wave: 11
depends_on: ["03-17"]
files_modified:
  - README.md
  - .env.example
  - docs/architecture.md
  - docs/architecture.html
  - docs/erd.md
  - docs/erd.html
  - docs/nfr.md
  - loadtest/README.md
  - services/visit/app.py
  - services/visit/workflows/__init__.py
  - services/visit/workflows/reconciler.py
  - tests/docs/test_phase3_docs.py
autonomous: true
requirements: [DASH-03, DASH-04, EVENT-01, OBS-01]

estimate:
  tokens: 95000
  raw_tokens: 95000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "The architecture doc describes the data and event flows as built: booking, cancel/reschedule, visit lifecycle, notifications and reminders, analytics, and the observability chain (D-12, DASH-03 source text)"
    - "The decision record explains why CDC over a polling publisher (no relay code, WAL ordering, no polling load) and the cost accepted (Connect plus logical replication to operate), why Temporal reminders, and the Debezium image deviation (D-01, D-02, specifics)"
    - "The docs state delivery semantics honestly: no loss, at-least-once on the wire, effect-level exactly-once through idempotent consumers, with the measured evidence from plans 03-05, 03-11 and 03-17 (research F4)"
    - "The notification guarantee is stated honestly: one send per dedupe key holds at the delivery log and at the fake provider's idempotency store, proven by a worker kill between the provider call and mark_sent (plan 03-02); a real SMS/email provider must accept an idempotency key for the same guarantee, otherwise one duplicate per crash at that exact point remains possible; the fake demonstrates the pattern"
    - "The EVENT-04 deviation is recorded with its reason and the user's answer from plan 03-01 Task 2: projections run on the analytics-projection Kafka consumer because they need the idempotency claim in the effect's own transaction; Celery covers notifications and follow-up sends; REQUIREMENTS.md wording is the user's call"
    - "Interim mechanisms are labelled: the sweeper is retired; the hold reaper, pending-starter and both reconcilers stay as safety nets, each with its reason (D-03); the notification stub activities are intent records (F5); in-code docstrings no longer describe the sweeper as a live trigger"
    - "Every topic, consumer group, Compose service and setting named in the docs exists in the code, enforced by a test"
  artifacts:
    - path: docs/architecture.md
      provides: "Phase 3 sections: event backbone, flows, notifications, analytics, observability, decisions, limitations"
      contains: "smarthealth.appointment.events"
    - path: tests/docs/test_phase3_docs.py
      provides: "docs-to-code name checks for topics, groups, services and settings"
    - path: docs/erd.md
      provides: "outbox tables, analytics_db, processed_events, Mongo notification collections"
      contains: "analytics_db"
  key_links:
    - from: tests/docs/test_phase3_docs.py
      to: libs/events/catalog.py
      via: "every topic and DLQ name in docs/architecture.md must be in AGGREGATE_TOPICS or DLQ_TOPICS"
      pattern: "DLQ_TOPICS"
---

<objective>
Bring README, `.env.example`, architecture doc (md and html), ERD (md and html), NFR doc, the load-test README and the three visit docstrings that still mention the sweeper in line with what plans 03-01..17 built, with honest limitations and a test that keeps doc names tied to code.

Purpose: the mentor grades defensible architecture reasoning; D-12 asks that docs state only what the code does and keep interim mechanisms labelled. These texts are also the source for the dashboard closeout in plan 03-19 (DASH-03/04) and the PHASE3_TRACE notes in plan 03-20.

Output: updated documents, docstrings and `tests/docs/test_phase3_docs.py`.

Decisions honoured: D-12, D-01..D-04 reasoning, D-03 (record why each remaining safety net stays), the CONTEXT "Specific Ideas" (why CDC and the cost accepted), research F1-F8, the user-veto answers recorded in the plan 03-01 SUMMARY (Connect image deviation, EVENT-04 projection deviation, S3 deferral, no provider-event consumer, insert-only outbox, backfill). Read every 03-NN-SUMMARY.md first and prefer what they report over this plan where they differ (measured numbers, fixes, renamed symbols). This plan does not edit `.planning/REQUIREMENTS.md`.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@README.md
@docs/architecture.md
@docs/architecture.html
@docs/erd.md
@docs/erd.html
@docs/nfr.md
@loadtest/README.md
@libs/events/catalog.py
@infra/docker-compose.yml
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| docs/architecture.md "Phase 3" sections | doc | event backbone (outbox, CDC, topics, schemas), flows (booking, cancel/reschedule, visit lifecycle, notifications and reminders, analytics), observability (metrics, dashboards, trace chain, logs), decision rows, limitations and operations notes |
| docs/architecture.html | doc | the same sections in the existing visual style |
| README.md | doc | new services and containers, `/api/analytics` endpoints, configuration rows for every new setting, Phase 3 test prerequisites, known limitations |
| `.env.example` | config doc | host `ANALYTICS_DATABASE_URL` line beside the other service URLs (moved here from the analytics plans) |
| visit docstrings | code docs | `services/visit/app.py`, `services/visit/workflows/__init__.py` (sweeper tombstone note), `services/visit/workflows/reconciler.py` (never-started visits are triggered by the outbox consumer) |
| docs/erd.md, docs/erd.html | doc | `outbox` in scheduling/visit/provider/patient DBs, analytics_db tables and views, `processed_events`, Mongo `notification_deliveries`, `notification_provider_calls`, `notification_appointment_state`; migration chain lines |
| docs/nfr.md, loadtest/README.md | doc | histogram per NFR, the Grafana NFR dashboard, the post-Phase 3 smoke result; sustained/burst/contention still unmeasured |
| `tests/docs/test_phase3_docs.py` | test | names in docs resolve to code |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the booking event flow documented end to end, with a test that every name in it exists in the code</name>
  <files>docs/architecture.md, tests/docs/test_phase3_docs.py</files>
  <behavior>
    - docs/architecture.md has a "Phase 3: event backbone" section with a mermaid sequence diagram of the booking flow: gateway -> Scheduling API (202) -> Temporal saga -> confirm activity commits the appointment, history and outbox row in one transaction -> WAL -> Debezium `scheduling-outbox` -> `smarthealth.appointment.events` -> `notification-trigger` (Celery `notifications.booking` -> fake provider) and `analytics-projection`, with the reminder workflow start
    - The test extracts every backticked `smarthealth.*` topic, `smarthealth.dlq.*` topic, consumer group name and Compose service name from the Phase 3 sections and asserts each exists (catalog constants, `DLQ_TOPICS`, the group ids in the consumer modules, `infra/docker-compose.yml` services)
    - The test fails if the Phase 3 section claims exactly-once delivery on the wire (searches for the phrase "exactly-once delivery" outside a sentence that also says "effect")
  </behavior>
  <read_first>
    - every `.planning/phases/03-event-driven-system-observability/03-NN-SUMMARY.md` (what was built; measured numbers)
    - docs/architecture.md (structure, numbering and tone; the existing workflow table and decision table)
    - libs/events/catalog.py, services/visit/consumer.py, services/analytics/consumer.py, services/notification/consumer.py, infra/docker-compose.yml
    - tests/docs/test_phase21_traceability.py (how docs sections are located by heading)
  </read_first>
  <action>
    Add the section with the diagram and a short narrative (what is synchronous: only the outbox insert inside the confirm step; the 202 path is unchanged). Write `tests/docs/test_phase3_docs.py` locating the Phase 3 sections by heading, with the name-resolution checks above and a small allowlist mechanism for names that are intentionally external (for example `quay.io/debezium/connect:3.7.1.Final`).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -x -q</automated>
    <fails_when>pytest exits non-zero: a documented topic, group or service does not exist in the code, or the doc claims wire-level exactly-once.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs/test_phase3_docs.py -q` exits 0 with at least 4 passed
    - `grep -c "sequenceDiagram" docs/architecture.md` increases by at least 1 relative to `git show HEAD:docs/architecture.md`
  </acceptance_criteria>
  <done>The booking event flow is documented and every name in it is verified against the code.</done>
</task>

<task type="auto">
  <name>Task 2: All flows, decisions, limitations and operations notes in architecture.md, architecture.html, README and .env.example</name>
  <files>docs/architecture.md, docs/architecture.html, README.md, .env.example</files>
  <read_first>
    - docs/architecture.md (Task 1 section), docs/architecture.html (existing markup patterns), README.md ("What exists today", "API overview", "Configuration", "Running tests", "Known limitations")
    - the 03-NN SUMMARY files (measured duplicate counts after Connect hard kill, reprocessed counts, smoke numbers, any deviations)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Findings F1-F8, Pitfalls 4, 8, Assumptions A1-A10)
  </read_first>
  <action>
    architecture.md: add flow subsections for cancel/reschedule (events, notifications, reminder cancel/reschedule via signal-with-start), visit lifecycle (check-in, completion, `visit.completed` -> visit-downstream consumer -> `VisitCompletedWorkflow`; follow-up workflow), notifications (event-driven sends, dedupe key, closed-appointment guard, retries, `notifications.dlq`, Kafka DLQs; the Phase 2 notification activities are intent records and do not send; one send per dedupe key at two layers, the delivery log and the fake provider's idempotency store, with the plan 03-02 worker-kill test as evidence, and the plain statement that a real SMS/email provider must accept an idempotency key for the same guarantee, otherwise each crash between the provider call and `mark_sent` can cause one duplicate; the fake demonstrates the pattern), analytics (projections on the `analytics-projection` Kafka consumer, metric definitions including wait time = check-in to visit start and cancellation rate denominator, replay rebuild procedure) and observability (scrape jobs, four dashboards, trace chain with the reminder's long-lived trace labelled, structured logs). Add decision rows: CDC vs polling publisher (with the cost accepted), Debezium Connect image deviation from the CP-line rule (with the user's answer from the plan 03-01 SUMMARY), projections on a Kafka consumer instead of Celery (the EVENT-04 deviation: requirement EVENT-04 lists projections among Celery + RabbitMQ background tasks; reason: stateful upserts need the idempotency claim in the effect's own transaction, which a Kafka consumer with a processed-event table gives and a Celery task does not; Celery covers notifications and follow-up sends; record the user's answer, and say that the REQUIREMENTS.md wording is the user's decision), service-side Avro with strict serializers, topic per aggregate, three idempotency flavors, Temporal reminders vs Celery ETA/beat, Celery threads pool and RabbitMQ 4.3 settings, classic vs quorum queues, sweeper retirement, dedicated broker user. Limitations and operations notes: at-least-once on the wire with the measured numbers; Schema Registry as a cached write-path dependency (warm-up; cold start during an outage cannot become ready); replication slots retain WAL while Connect is down (`max_slot_wal_keep_size=2GB`, deleting a connector does not drop its slot, the drop command); outbox is insert-only with a documented prune statement; Kafka and Connect have no authentication on the dev network (production: SASL/TLS, ACLs, Connect REST auth); `temporalio/auto-setup` is non-production; the remaining safety nets (hold reaper, pending-starter, scheduling reconciler, visit reconciler) stay and why each is not subsumed by events; S3 audit-via-outbox deferred; `provider.schedule_changed` is published but not consumed and the poller stays paused (user decision); k6 sustained/burst/contention not yet run.

    architecture.html: mirror the new sections in the existing style. README: list the new containers and what they do, `/api/analytics/summary` and `/timeseries` in the API overview, every new setting in the configuration table (names and defaults from the settings classes; secret files by path only), the Phase 3 e2e prerequisites (secrets script, rebuilt stack, `alembic upgrade head` for analytics), and the limitations above in one or two sentences each, including the real-provider idempotency-key caveat. `.env.example`: add the host `ANALYTICS_DATABASE_URL` line beside the other service URLs with the same placeholder convention (if the harness blocks that edit, list it in the SUMMARY as plan 01.1-09 did).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs -x -q</automated>
    <fails_when>pytest exits non-zero: a named topic, group, service or setting is missing from the code, a stale claim returns, or the Phase 2/2.1 docs tests fail.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs -q` exits 0
    - `grep -c "effect-level exactly-once" docs/architecture.md` is at least 1 and `grep -c "at-least-once" README.md` is at least 1
    - `grep -c "max_slot_wal_keep_size" docs/architecture.md` is at least 1
    - `grep -c "/api/analytics" README.md` is at least 1
    - `grep -cE "hold reaper|pending-starter" docs/architecture.md` is at least 2
    - `grep -c "idempotency key" docs/architecture.md` is at least 1 and `grep -c "idempotency key" README.md` is at least 1
    - `grep -c "EVENT-04" docs/architecture.md` is at least 1
    - `grep -c "ANALYTICS_DATABASE_URL" .env.example` is 1 (or the SUMMARY records the blocked edit)
  </acceptance_criteria>
  <done>The architecture doc, its HTML view and the README describe the Phase 3 system, its decisions (including the EVENT-04 deviation) and its honest limits (including the real-provider caveat).</done>
</task>

<task type="auto">
  <name>Task 3: ERD, NFR doc, load-test README and the stale visit docstrings</name>
  <files>docs/erd.md, docs/erd.html, docs/nfr.md, loadtest/README.md, services/visit/app.py, services/visit/workflows/__init__.py, services/visit/workflows/reconciler.py</files>
  <read_first>
    - docs/erd.md and docs/erd.html (per-service diagrams, notation conventions, migration chain table)
    - the four outbox migrations and `services/analytics/migrations/versions/analytics_0001_projections.py`; services/notification/deliveries.py (collection names and indexes)
    - docs/nfr.md, loadtest/README.md, the 03-11 and 03-16 SUMMARY files (smoke numbers)
    - services/visit/app.py, services/visit/workflows/__init__.py, services/visit/workflows/reconciler.py, services/visit/workflows/sweeper.py (current docstrings; the tombstone note written in plan 03-12)
  </read_first>
  <action>
    ERD (md and html): add `outbox` to the scheduling, visit, provider and patient diagrams with its columns and the `dbz_outbox` publication note; a new analytics_db diagram (`processed_events`, `patients`, `appointments`, `visits_completed`, the five views); Mongo `notification_deliveries` (unique `dedupe_key`), `notification_provider_calls`, `notification_appointment_state` (unique `appointment_id`); migration chain entries `scheduling_0006`, `visit_0003`, `provider_0003`, `patient_0002`, `analytics_0001`; keep the existing notation (dashed id-only links). nfr.md: for each NFR name the histogram or metric and the dashboard panel that will measure it, state that sustained/burst/contention are still unmeasured, and record the post-Phase 3 smoke result with its date. loadtest/README.md: point at the `smarthealth-booking-nfr` dashboard for reading runs and record the smoke result.

    Visit docstrings (documentation only; no behaviour change): `services/visit/app.py` (completion downstream work is triggered by the `visit.completed` outbox event, not by the API or a sweeper), `services/visit/workflows/__init__.py` (the sweeper workflow is a registered tombstone kept for in-flight runs and recorded histories; safe to remove once the `list_workflows` query recorded in `workflows/sweeper.py` is empty on every environment), `services/visit/workflows/reconciler.py` (never-started visits are triggered by the outbox consumer; the reconciler settles started-but-unsettled visits).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs tests/loadtest -x -q && .venv/bin/python -c "import services.visit.app, services.visit.workflows, services.visit.workflows.reconciler"</automated>
    <fails_when>pytest exits non-zero (including the NFR single-source consistency test), the docs name a table or migration that does not exist, or a visit module no longer imports.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs tests/loadtest -q` exits 0
    - `git diff HEAD --stat -- services/visit` lists only the three docstring files, and `git diff HEAD -- services/visit` changes docstrings and comments only
    - `.venv/bin/python -m pytest tests/workflows/test_visit_schedules.py -q` exits 0
    - `grep -c "scheduling_0006\|visit_0003\|provider_0003\|patient_0002\|analytics_0001" docs/erd.md` is at least 5
    - `grep -c "booking_saga_seconds" docs/nfr.md` is at least 1
    - `grep -c "notification_deliveries" docs/erd.md` is at least 1
  </acceptance_criteria>
  <done>Data model and NFR documents reflect Phase 3, with measurement status stated honestly.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| documentation -> readers (mentor, operators) | wrong or stale statements mislead security-relevant operation |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-52 | Information Disclosure | docs embedding secrets or real data | medium | mitigate | Secret files referenced by path only, placeholder defaults only, synthetic examples; reviewer greps the diff for key material before commit |
| T-3-53 | Repudiation | docs overstating guarantees | medium | mitigate | `test_phase3_docs.py` ties names to code and rejects a wire-level exactly-once claim; measured numbers quoted from SUMMARY files; limitations listed explicitly, including the real-provider idempotency-key caveat and the EVENT-04 deviation with the user's answer |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs tests/loadtest -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
</verification>

<success_criteria>
- Every Phase 3 flow, decision and limitation is documented as built, and doc names are test-tied to code
- The notification guarantee and the EVENT-04 deviation are stated honestly; no in-code docstring describes the sweeper as live
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-18-SUMMARY.md` when done
</output>
