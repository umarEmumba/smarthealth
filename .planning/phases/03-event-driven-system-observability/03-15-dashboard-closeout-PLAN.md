---
phase: 03-event-driven-system-observability
plan: 15
type: execute
wave: 9
depends_on: ["03-14"]
files_modified:
  - docs/architecture-dashboard.html
  - tests/docs/test_dashboard.py
  - tests/docs/test_requirement_traceability.py
  - .planning/PROJECT.md
autonomous: false
requirements: [DASH-03, DASH-04, EVENT-01, EVENT-02, EVENT-03, EVENT-04, EVENT-05, NOTIF-01, NOTIF-02, NOTIF-03, NOTIF-04, ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06, OBS-01, OBS-02, OBS-03]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 4
  confidence: low

must_haves:
  truths:
    - "The Architecture & Decision dashboard shows the key data and event flows: booking, cancel/reschedule, visit lifecycle, notifications and analytics (DASH-03, roadmap success criterion 5)"
    - "The dashboard's evaluation Q&A is updated for Phase 3 (why CDC, why Temporal reminders, at-least-once honesty, PHI-free events, outage behaviour, analytics rebuild, trace across Kafka and Celery, NFR status) and the page stays self-contained (DASH-04, D-12)"
    - "Every Phase 3 requirement maps to existing test files through PHASE3_TRACE, and a test fails if a mapping or a file disappears"
    - "The full non-e2e suite and the full e2e suite pass on the finished stack"
    - "A human has looked at one booking trace in Jaeger, the four Grafana dashboards with data, the dashboard HTML, and a manual k6 smoke run with their own admin credential"
  artifacts:
    - path: docs/architecture-dashboard.html
      provides: "Phase 3 flow diagrams, decision cards, Q&A, roadmap block, version marker v4 · Phase 3"
      contains: "v4 · Phase 3"
    - path: tests/docs/test_requirement_traceability.py
      provides: "PHASE3_TRACE for the 20 Phase 3 requirements"
      contains: "PHASE3_TRACE"
    - path: tests/docs/test_dashboard.py
      provides: "Phase 3 needles, SVG accessibility, self-containment, marker and roadmap checks"
  key_links:
    - from: tests/docs/test_requirement_traceability.py
      to: tests/e2e/test_phase3_redelivery.py
      via: "EVENT-03 and NOTIF-04 map to the forced-redelivery test"
      pattern: "test_phase3_redelivery"
---

<objective>
Close out the Architecture & Decision dashboard for Phase 3, add the Phase 3 requirement traceability map, run the full regression, and hand the visual and manual checks to the human at the end of the phase.

Purpose: roadmap success criterion 5 (DASH-03 flows, DASH-04 Q&A current at the phase close) and a regression-proof trail from all 20 Phase 3 requirements to tests.

Output: updated `docs/architecture-dashboard.html`, `tests/docs/test_dashboard.py`, `PHASE3_TRACE` in `tests/docs/test_requirement_traceability.py`, Phase 3 rows in `.planning/PROJECT.md` Key Decisions, regression results, and a human sign-off.

Decisions honoured: D-12 (event-flow diagrams for booking, cancel/reschedule, visit lifecycle, notifications and analytics; "why CDC outbox" and "why Temporal reminders"; updated Q&A; docs state only what the code does; interim mechanisms labelled). Wording comes from docs/architecture.md as updated in plan 03-14; keep them consistent. Planner discretion: the version marker becomes `v4 · Phase 3`; the roadmap marks Phase 3 as the current block.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-14-SUMMARY.md
@docs/architecture.md
@docs/architecture-dashboard.html
@tests/docs/test_dashboard.py
@tests/docs/test_requirement_traceability.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| dashboard `flows` additions | dashboard | five inline SVG flows (booking event, cancel/reschedule, visit lifecycle, notifications and reminders, analytics), each `role="img"` with `<title>` and `<desc>` |
| dashboard `decisions` additions | dashboard | cards: CDC outbox vs polling publisher (cost accepted), Debezium Connect image deviation, Avro + registry with strict serializers, topic per aggregate, idempotency flavors, Temporal reminders vs Celery ETA, Celery on RabbitMQ 4.3, sweeper retirement |
| dashboard `qa` additions | dashboard | Phase 3 answers, keeping "not yet verified" where measurement is pending (k6 sustained/burst/contention) |
| dashboard `roadmap` and marker | dashboard | `Phase 3 ·` block; `v4 · Phase 3` in the meta row and footer |
| `PHASE3_TRACE` | test | 20 requirement ids -> test files |
| `.planning/PROJECT.md` rows | doc | Key Decisions rows with outcome `Implemented (Phase 3)` |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the booking event flow on the dashboard, guarded by the dashboard test</name>
  <files>docs/architecture-dashboard.html, tests/docs/test_dashboard.py</files>
  <behavior>
    - The `flows` section contains an inline SVG for the booking event flow (outbox, Debezium, Kafka topic, notification-trigger, Celery, analytics-projection, reminder workflow) with `role="img"`, `<title>` and `<desc>`
    - The version marker `v4 · Phase 3` appears in the meta row and the footer, and the previous marker no longer appears
    - The page still loads no external script, stylesheet, font or url()
  </behavior>
  <read_first>
    - docs/architecture-dashboard.html (sections `flows`, `decisions`, `qa`, `roadmap`; `sd-*` SVG classes; meta row and footer markers)
    - tests/docs/test_dashboard.py (every assertion, including the version-marker test and the roadmap regex)
    - docs/architecture.md (the Phase 3 sections from plan 03-14: the booking sequence and wording)
  </read_first>
  <action>
    Add the booking event flow SVG to `flows`, reusing the existing `sd-*` classes, and replace both version markers. Update `test_version_marker_in_meta_row_and_footer` to expect `v4 · Phase 3` at least twice and the previous marker zero times; add `test_phase3_booking_event_flow_present` (needles `outbox`, `Debezium`, `smarthealth.appointment.events`, `notification-trigger`, `analytics-projection`) and keep every older assertion.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs/test_dashboard.py -x -q</automated>
    <fails_when>pytest exits non-zero: an external resource appears, an SVG lacks title or role, the old marker remains, or a Phase 3 needle is missing.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs/test_dashboard.py -q` exits 0
    - `grep -c "v4 · Phase 3" docs/architecture-dashboard.html` is at least 2
  </acceptance_criteria>
  <done>The dashboard shows the booking event flow and its test enforces it.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Remaining flows, decisions, Q&A, roadmap and the PROJECT.md decision rows</name>
  <files>docs/architecture-dashboard.html, tests/docs/test_dashboard.py, .planning/PROJECT.md</files>
  <behavior>
    - `flows` covers cancel/reschedule, visit lifecycle, notifications and reminders, and analytics, each as an accessible SVG or a flow block
    - `decisions` has the eight Phase 3 cards with alternatives and reasoning, including the cost accepted for CDC
    - `qa` answers the Phase 3 questions listed in the must-haves and keeps the phrase "not yet verified" for the deferred k6 profiles
    - the roadmap has a `Phase 3 ·` block; Phase 1.1, 2 and 2.1 blocks remain
    - the open-risks callout lists the Phase 3 limitations from docs/architecture.md
    - stale phrases from earlier phases ("Kafka/RabbitMQ stay scaffolding", "Phase 3 replaces" where it is now done) are removed or rewritten, and the test checks they are gone
  </behavior>
  <read_first>
    - docs/architecture-dashboard.html (Task 1 state)
    - docs/architecture.md (Phase 3 decision rows, limitations and operations notes)
    - tests/docs/test_dashboard.py (stale-text and roadmap tests; `test_phase2_content_present` currently expects the "Phase 3 replaces" needle, so update that needle deliberately and say why in the test)
    - .planning/PROJECT.md (Key Decisions table format)
  </read_first>
  <action>
    Add the four flows, the eight decision cards, the Q&A answers, the roadmap block and the open risks, using the same wording as docs/architecture.md. Update `tests/docs/test_dashboard.py`: add `test_phase3_flows_present` (needles per flow), `test_qa_covers_phase3_themes` (CDC, Temporal reminders, at-least-once, PHI, outage, rebuild, trace, NFR), extend the roadmap test for the `Phase 3 ·` block, and adjust the stale-text test for the phrases that are no longer true. Append Phase 3 rows to the PROJECT.md Key Decisions table with outcome `Implemented (Phase 3)`, using the architecture doc's decision wording.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs -x -q</automated>
    <fails_when>pytest exits non-zero: a flow, decision or Q&A theme is missing, a stale phrase returns, or the page stops being self-contained.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs -q` exits 0
    - `grep -c "Implemented (Phase 3)" .planning/PROJECT.md` is at least 6
    - `grep -c "Phase 3 ·" docs/architecture-dashboard.html` is at least 1
  </acceptance_criteria>
  <done>The dashboard is complete and current for Phase 3, and the decision log in PROJECT.md matches it.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: PHASE3_TRACE and the full regression on the finished stack</name>
  <files>tests/docs/test_requirement_traceability.py</files>
  <behavior>
    - `PHASE3_TRACE` contains exactly the 20 Phase 3 ids (EVENT-01..05, NOTIF-01..04, ANLY-01..06, OBS-01..03, DASH-03, DASH-04), each mapped to one or more test files
    - every mapped file exists and defines at least one test function; the Phase 2 checks are unaffected
    - every Phase 3 id is checked off neither here nor by this test (REQUIREMENTS.md is updated by the verifier); the test only checks the ids exist in REQUIREMENTS.md
  </behavior>
  <read_first>
    - tests/docs/test_requirement_traceability.py (PHASE2_TRACE structure and checks)
    - the actual test file names produced by plans 03-01..14 (use the repository, not this plan, if they differ)
  </read_first>
  <action>
    Add `PHASE3_TRACE` and parametrized checks mirroring PHASE2_TRACE. Mapping: EVENT-01 -> `tests/events/test_schemas.py`, `tests/e2e/test_phase3_outbox_cdc.py`, `tests/e2e/test_phase3_booking_events.py`, `tests/e2e/test_phase3_registry_events.py`; EVENT-02 -> `tests/events/test_outbox_roundtrip.py`, `tests/scheduling/test_outbox.py`, `tests/visit/test_outbox.py`, `tests/patient/test_outbox.py`, `tests/provider/test_outbox.py`, `tests/e2e/test_phase3_outbox_cdc.py`, `tests/e2e/test_phase3_visit_downstream.py`; EVENT-03 -> `tests/events/test_consumer_idempotency.py`, `tests/visit/test_visit_consumer.py`, `tests/analytics/test_projections.py`, `tests/e2e/test_phase3_redelivery.py`; EVENT-04 -> `tests/notification/test_tasks.py`, `tests/e2e/test_phase3_notifications.py`; EVENT-05 -> `tests/events/test_consumer_dlq.py`, `tests/notification/test_dlq.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-01 -> `tests/notification/test_triggers.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-02 -> `tests/workflows/test_reminder_workflow.py`, `tests/workflows/test_replay_reminder.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-03 -> `tests/workflows/test_reminder_workflow.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-04 -> `tests/notification/test_dedupe.py`, `tests/e2e/test_phase3_redelivery.py`; ANLY-01..05 -> `tests/analytics/test_projections.py`, `tests/analytics/test_api.py`, `tests/e2e/test_phase3_analytics.py`; ANLY-06 -> `tests/analytics/test_replay_rebuild.py`, `tests/libs/test_service_isolation.py`; OBS-01 -> `tests/infra/test_grafana_provisioning.py`, `tests/infra/test_prometheus_config.py`, `tests/e2e/test_phase3_metrics.py`; OBS-02 -> `tests/events/test_trace_chain.py`, `tests/e2e/test_phase3_trace.py`; OBS-03 -> `tests/libs/test_observability.py`, `tests/e2e/test_phase3_logs.py`; DASH-03 -> `tests/docs/test_dashboard.py`, `tests/docs/test_phase3_docs.py`; DASH-04 -> `tests/docs/test_dashboard.py`.

    Then run the whole non-e2e suite and the whole e2e suite on the stack and fix anything this phase broke (do not weaken a test to pass; record every fix in the SUMMARY). Run Semgrep over the Phase 3 code and record the triage.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -x -q && .venv/bin/python -m pytest tests -m "not e2e" -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q</automated>
    <fails_when>Any of the three pytest runs exits non-zero, a requirement has no existing mapped test file, or an e2e test fails on the stack.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -q` exits 0 with at least 21 passed for the Phase 3 cases plus the existing Phase 2 cases
    - `.venv/bin/python -m pytest tests -m "not e2e" -q` exits 0 (OTEL_SDK_DISABLED not set)
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q` exits 0
    - the SUMMARY records pass counts and the Semgrep status (`semgrep --config auto libs services infra scripts`)
  </acceptance_criteria>
  <done>All 20 requirements trace to tests, and both suites are green on the finished stack.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 4: End-of-phase human checks — Jaeger trace, Grafana dashboards, dashboard HTML, manual k6 smoke</name>
  <what-built>Phase 3 is complete: outbox + Debezium CDC to Kafka with registered Avro schemas, idempotent consumers with DLQs, Celery notifications on RabbitMQ, Temporal reminders and follow-ups, event-derived analytics behind /api/analytics, Prometheus scraping every process, four provisioned Grafana dashboards, one connected trace per booking, and the closed-out dashboard. Automated suites are green (Task 3). The trace id recorded in the 03-13 SUMMARY can be used for step 1.</what-built>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-13-SUMMARY.md (trace id and span counts)
    - .planning/phases/03-event-driven-system-observability/03-12-SUMMARY.md (dashboard names and smoke numbers)
    - loadtest/README.md (manual smoke instructions)
  </read_first>
  <how-to-verify>
    1. Jaeger: open http://localhost:16686, search the trace id from the 03-13 SUMMARY (or book once through the gateway and search service `smarthealth-gateway`); confirm ONE trace contains the gateway, Scheduling, the Temporal workflow and activities, the Kafka processing span in `smarthealth-notification-consumer` and the Celery `notifications.send` span, nested rather than as separate traces.
    2. Grafana: open http://localhost:3000 with your admin login; open `SmartHealth · Booking & NFR`, `Events`, `Notifications` and `Operations`; confirm panels show data after a few bookings (Operations needs a clinic selected).
    3. Dashboard HTML: open `docs/architecture-dashboard.html` offline in a browser at full and narrow width; check the five Phase 3 flows, the decision cards and the Q&A read well.
    4. Manual smoke with your own admin (never stored in the repo): seed with `scripts/loadtest_seed.py` as described in `loadtest/README.md`, raise the gateway budget for the run, run `loadtest/run.sh smoke`, restore the budget; confirm the thresholds pass.
  </how-to-verify>
  <resume-signal>Reply "approved", or describe what looked wrong (step number and what you saw)</resume-signal>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>The docs suite exits non-zero, meaning the artifact under review changed after Task 3.</fails_when>
  </verify>
  <acceptance_criteria>
    - The user's reply for each of the four steps is recorded in the plan SUMMARY
    - Any issue reported is either fixed (with the affected test re-run) or recorded as a known limitation with the user's agreement
  </acceptance_criteria>
  <done>A human confirmed the trace, the dashboards, the HTML and a manual smoke run.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| documentation and dashboard -> readers | stale or overstated content misleads review and operation |
| human checker -> Grafana and seed script | the user's own admin credential is used locally |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-54 | Information Disclosure | dashboard HTML and PROJECT.md | medium | mitigate | Names, placeholder defaults and synthetic examples only; self-containment test; no credentials in the manual steps (the user types their own) |
| T-3-55 | Repudiation | requirement coverage claims | low | mitigate | PHASE3_TRACE fails when a requirement, a mapped file or its test functions disappear; the human sign-off is recorded verbatim |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q`
- Human sign-off (Task 4)
</verification>

<success_criteria>
- DASH-03 and DASH-04 are met: flows, decisions and Q&A current at the Phase 3 close
- All 20 requirements trace to tests; both suites green; human checks approved
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-15-SUMMARY.md` when done
</output>
