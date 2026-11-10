---
phase: 03-event-driven-system-observability
plan: 14
type: execute
wave: 8
depends_on: ["03-13"]
files_modified:
  - README.md
  - docs/architecture.md
  - docs/architecture.html
  - docs/erd.md
  - docs/erd.html
  - docs/nfr.md
  - loadtest/README.md
  - tests/docs/test_phase3_docs.py
autonomous: true
requirements: [DASH-03, DASH-04, EVENT-01, OBS-01]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "The architecture doc describes the data and event flows as built: booking, cancel/reschedule, visit lifecycle, notifications and reminders, analytics, and the observability chain (D-12, DASH-03 source text)"
    - "The decision record explains why CDC over a polling publisher (no relay code, WAL ordering, no polling load) and the cost accepted (Connect plus logical replication to operate), why Temporal reminders, and the Debezium image deviation (D-01, D-02, specifics)"
    - "The docs state delivery semantics honestly: no loss, at-least-once on the wire, effect-level exactly-once through idempotent consumers, with the measured evidence from plans 03-02 and 03-13 (research F4)"
    - "Interim mechanisms are labelled: the sweeper is retired; the hold reaper, pending-starter and both reconcilers stay as safety nets, each with its reason (D-03); the notification stub activities are intent records (F5)"
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
Bring README, architecture doc (md and html), ERD (md and html), NFR doc and the load-test README in line with what plans 03-01..13 built, with honest limitations and a test that keeps doc names tied to code.

Purpose: the mentor grades defensible architecture reasoning; D-12 asks that docs state only what the code does and keep interim mechanisms labelled. These texts are also the source for the dashboard closeout in plan 03-15 (DASH-03/04).

Output: updated documents and `tests/docs/test_phase3_docs.py`.

Decisions honoured: D-12, D-01..D-04 reasoning, D-03 (record why each remaining safety net stays), the CONTEXT "Specific Ideas" (why CDC and the cost accepted), research F1-F8. Read every 03-NN-SUMMARY.md first and prefer what they report over this plan where they differ (measured numbers, fixes, renamed symbols).
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
  <name>Task 2: All flows, decisions, limitations and operations notes in architecture.md, architecture.html and README</name>
  <files>docs/architecture.md, docs/architecture.html, README.md</files>
  <read_first>
    - docs/architecture.md (Task 1 section), docs/architecture.html (existing markup patterns), README.md ("What exists today", "API overview", "Configuration", "Running tests", "Known limitations")
    - the 03-NN SUMMARY files (measured duplicate counts after Connect hard kill, reprocessed counts, smoke numbers, any deviations)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Findings F1-F8, Pitfalls 4, 8, Assumptions A1-A10)
  </read_first>
  <action>
    architecture.md: add flow subsections for cancel/reschedule (events, notifications, reminder cancel/reschedule via signal-with-start), visit lifecycle (check-in, completion, `visit.completed` -> visit-downstream consumer -> `VisitCompletedWorkflow`; follow-up workflow), notifications (event-driven sends, dedupe key, closed-appointment guard, retries, `notifications.dlq`, Kafka DLQs; the Phase 2 notification activities are intent records and do not send), analytics (projections, metric definitions including wait time = check-in to visit start and cancellation rate denominator, replay rebuild procedure) and observability (scrape jobs, four dashboards, trace chain with the reminder's long-lived trace labelled, structured logs). Add decision rows: CDC vs polling publisher (with the cost accepted), Debezium Connect image deviation from the CP-line rule, service-side Avro with strict serializers, topic per aggregate, three idempotency flavors, Temporal reminders vs Celery ETA/beat, Celery threads pool and RabbitMQ 4.3 settings, classic vs quorum queues, sweeper retirement, dedicated broker user. Limitations and operations notes: at-least-once on the wire with the measured numbers; Schema Registry as a cached write-path dependency (warm-up; cold start during an outage cannot become ready); replication slots retain WAL while Connect is down (`max_slot_wal_keep_size=2GB`, deleting a connector does not drop its slot, the drop command); outbox is insert-only with a documented prune statement; Kafka and Connect have no authentication on the dev network (production: SASL/TLS, ACLs, Connect REST auth); `temporalio/auto-setup` is non-production; the remaining safety nets (hold reaper, pending-starter, scheduling reconciler, visit reconciler) stay and why each is not subsumed by events; S3 audit-via-outbox deferred; `provider.schedule_changed` is published but not consumed and the poller stays paused (user decision); k6 sustained/burst/contention not yet run.

    architecture.html: mirror the new sections in the existing style. README: list the new containers and what they do, `/api/analytics/summary` and `/timeseries` in the API overview, every new setting in the configuration table (names and defaults from the settings classes; secret files by path only), the Phase 3 e2e prerequisites (secrets script, rebuilt stack, `alembic upgrade head` for analytics), and the limitations above in one or two sentences each.
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
  </acceptance_criteria>
  <done>The architecture doc, its HTML view and the README describe the Phase 3 system, its decisions and its honest limits.</done>
</task>

<task type="auto">
  <name>Task 3: ERD, NFR doc and load-test README</name>
  <files>docs/erd.md, docs/erd.html, docs/nfr.md, loadtest/README.md</files>
  <read_first>
    - docs/erd.md and docs/erd.html (per-service diagrams, notation conventions, migration chain table)
    - the four outbox migrations and `services/analytics/migrations/versions/analytics_0001_projections.py`; services/notification/deliveries.py (collection names and indexes)
    - docs/nfr.md, loadtest/README.md, the 03-05 and 03-12 SUMMARY files (smoke numbers)
  </read_first>
  <action>
    ERD (md and html): add `outbox` to the scheduling, visit, provider and patient diagrams with its columns and the `dbz_outbox` publication note; a new analytics_db diagram (`processed_events`, `patients`, `appointments`, `visits_completed`, the five views); Mongo `notification_deliveries` (unique `dedupe_key`), `notification_provider_calls`, `notification_appointment_state` (unique `appointment_id`); migration chain entries `scheduling_0006`, `visit_0003`, `provider_0003`, `patient_0002`, `analytics_0001`; keep the existing notation (dashed id-only links). nfr.md: for each NFR name the histogram or metric and the dashboard panel that will measure it, state that sustained/burst/contention are still unmeasured, and record the post-Phase 3 smoke result with its date. loadtest/README.md: point at the `smarthealth-booking-nfr` dashboard for reading runs and record the smoke result.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs tests/loadtest -x -q</automated>
    <fails_when>pytest exits non-zero (including the NFR single-source consistency test) or the docs name a table or migration that does not exist.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs tests/loadtest -q` exits 0
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
| T-3-53 | Repudiation | docs overstating guarantees | medium | mitigate | `test_phase3_docs.py` ties names to code and rejects a wire-level exactly-once claim; measured numbers quoted from SUMMARY files; limitations listed explicitly |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs tests/loadtest -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
</verification>

<success_criteria>
- Every Phase 3 flow, decision and limitation is documented as built, and doc names are test-tied to code
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-14-SUMMARY.md` when done
</output>
