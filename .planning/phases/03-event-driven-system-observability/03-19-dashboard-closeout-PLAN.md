---
phase: 03-event-driven-system-observability
plan: 19
type: execute
wave: 12
depends_on: ["03-18"]
files_modified:
  - docs/architecture-dashboard.html
  - tests/docs/test_dashboard.py
  - .planning/PROJECT.md
autonomous: true
requirements: [DASH-03, DASH-04]

estimate:
  tokens: 80000
  raw_tokens: 80000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "The Architecture & Decision dashboard shows the key data and event flows: booking, cancel/reschedule, visit lifecycle, notifications and analytics (DASH-03, roadmap success criterion 5)"
    - "The dashboard's evaluation Q&A is updated for Phase 3 (why CDC, why Temporal reminders, at-least-once honesty, notification idempotency and the real-provider caveat, PHI-free events, outage behaviour, analytics rebuild, trace across Kafka and Celery, NFR status) and the page stays self-contained (DASH-04, D-12)"
    - "The decision cards include the EVENT-04 deviation (projections on a Kafka consumer, not Celery) and the Connect image deviation, each with the user's answer from plan 03-01"
    - "The dashboard test enforces the Phase 3 flows, Q&A themes, roadmap block and version marker"
  artifacts:
    - path: docs/architecture-dashboard.html
      provides: "Phase 3 flow diagrams, decision cards, Q&A, roadmap block, version marker v4 · Phase 3"
      contains: "v4 · Phase 3"
    - path: tests/docs/test_dashboard.py
      provides: "Phase 3 needles, SVG accessibility, self-containment, marker and roadmap checks"
      contains: "test_phase3_flows_present"
  key_links:
    - from: docs/architecture-dashboard.html
      to: docs/architecture.md
      via: "decision cards and Q&A reuse the plan 03-18 wording"
      pattern: "Phase 3"
---

<objective>
Close out the Architecture & Decision dashboard for Phase 3: the five event-flow diagrams, the Phase 3 decision cards, the updated evaluation Q&A, the roadmap block and version marker, and the Phase 3 rows in the PROJECT.md decision log. The traceability map, the phase-gate regression and the human checks follow in plan 03-20.

Purpose: roadmap success criterion 5 (DASH-03 flows, DASH-04 Q&A current at the phase close).

Output: updated `docs/architecture-dashboard.html`, `tests/docs/test_dashboard.py`, Phase 3 rows in `.planning/PROJECT.md` Key Decisions.

Decisions honoured: D-12 (event-flow diagrams for booking, cancel/reschedule, visit lifecycle, notifications and analytics; "why CDC outbox" and "why Temporal reminders"; updated Q&A; docs state only what the code does; interim mechanisms labelled). Wording comes from docs/architecture.md as updated in plan 03-18; keep them consistent. Planner discretion: the version marker becomes `v4 · Phase 3`; the roadmap marks Phase 3 as the current block.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-18-SUMMARY.md
@docs/architecture.md
@docs/architecture-dashboard.html
@tests/docs/test_dashboard.py
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| dashboard `flows` additions | dashboard | five inline SVG flows (booking event, cancel/reschedule, visit lifecycle, notifications and reminders, analytics), each `role="img"` with `<title>` and `<desc>` |
| dashboard `decisions` additions | dashboard | nine cards: CDC outbox vs polling publisher (cost accepted), Debezium Connect image deviation, projections on a Kafka consumer instead of Celery (EVENT-04 deviation), Avro + registry with strict serializers, topic per aggregate, idempotency flavors (including the provider idempotency store), Temporal reminders vs Celery ETA, Celery on RabbitMQ 4.3, sweeper retirement |
| dashboard `qa` additions | dashboard | Phase 3 answers, keeping "not yet verified" where measurement is pending (k6 sustained/burst/contention) |
| dashboard `roadmap` and marker | dashboard | `Phase 3 ·` block; `v4 · Phase 3` in the meta row and footer |
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
    - docs/architecture.md (the Phase 3 sections from plan 03-18: the booking sequence and wording)
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
    - `decisions` has the nine Phase 3 cards with alternatives and reasoning, including the cost accepted for CDC, and the EVENT-04 and Connect image deviations with the user's answers from the plan 03-01 SUMMARY
    - `qa` answers the Phase 3 questions listed in the must-haves (including "what if a real SMS/email provider has no idempotency key?") and keeps the phrase "not yet verified" for the deferred k6 profiles
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
    Add the four flows, the nine decision cards, the Q&A answers, the roadmap block and the open risks, using the same wording as docs/architecture.md. Update `tests/docs/test_dashboard.py`: add `test_phase3_flows_present` (needles per flow), `test_qa_covers_phase3_themes` (CDC, Temporal reminders, at-least-once, idempotency key, PHI, outage, rebuild, trace, NFR), `test_phase3_deviation_cards_present` (needles `EVENT-04` and `quay.io/debezium/connect`), extend the roadmap test for the `Phase 3 ·` block, and adjust the stale-text test for the phrases that are no longer true. Append Phase 3 rows to the PROJECT.md Key Decisions table with outcome `Implemented (Phase 3)`, using the architecture doc's decision wording.
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

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| documentation and dashboard -> readers | stale or overstated content misleads review and operation |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-54 | Information Disclosure / Repudiation | dashboard HTML and PROJECT.md | medium | mitigate | Names, placeholder defaults and synthetic examples only; self-containment test; wording reused from the test-checked architecture doc, including the real-provider caveat and both deviations with the user's answers |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q`
</verification>

<success_criteria>
- DASH-03 and DASH-04 are met: flows, decisions and Q&A current at the Phase 3 close
- The dashboard test enforces the Phase 3 content; PROJECT.md records the Phase 3 decisions
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-19-SUMMARY.md` when done
</output>
