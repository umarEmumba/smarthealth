---
phase: 03-event-driven-system-observability
plan: 19
subsystem: docs
tags: [dashboard, architecture, decision-record, event-flows, evaluation-qa, svg]
requires:
  - phase: 03-event-driven-system-observability
    provides: "plans 03-01..03-18: the event backbone, consumers, notification pipeline, analytics, observability, the measured evidence and docs/architecture.md section 10"
provides:
  - "docs/architecture-dashboard.html v4 · Phase 3: five accessible inline SVG event flows (booking, cancel and reschedule, visit completion, notifications and reminders, analytics), nine Phase 3 decision cards, an updated evaluation Q&A, a Phase 3 limitations callout and the roadmap block"
  - "tests/docs/test_dashboard.py: 18 tests enforcing the Phase 3 content, honesty items, catalog names and the stale-text list"
  - ".planning/PROJECT.md: 10 Key Decisions rows with outcome Implemented (Phase 3) and one Pending row for the deferred NFR measurement"
affects: [03-20]
key-files:
  created: []
  modified:
    - docs/architecture-dashboard.html
    - tests/docs/test_dashboard.py
    - .planning/PROJECT.md
key-decisions:
  - "The dashboard reuses the architecture.md section 10 wording and states only what the code does or what a plan SUMMARY measured; every limitation from 10.9 is on the page"
  - "The roadmap marks Phase 3 active (and Phases 2 and 2.1 complete, as ROADMAP.md does) but its status line says implementation plans executed, phase gate and visual checks pending; a test rejects the word complete there"
  - "A test rejects any sentence on the page that says exactly-once without saying effect-level or denying it, and a test ties every topic and event type the page names to libs/events/catalog"
requirements-completed: []
status: complete
duration: not measured
completed: 2026-11-11
actuals:
  tokens: 29290
  tasks: 2
  commits: 2
plan_head_before: 900c2660c75ff985907dbaef7bd9ee0a73639e2d
plan_head_after: f9e100c2668df364fffcf80e823d76d3aabb474e
---

# Phase 3 Plan 19: Dashboard Closeout Summary

**The Architecture & Decision dashboard is closed out for Phase 3 as `v4 · Phase 3`: five event-flow diagrams, nine decision cards (with the EVENT-04 and Connect-image deviations carrying the user's plan 03-01 answer), a rewritten evaluation Q&A, and a limitations callout that keeps every honesty item. The page was checked structurally only; its rendering was not seen by anyone yet.**

The plan addresses DASH-03 and DASH-04. REQUIREMENTS.md was not edited and no requirement was ticked (`requirements-completed` is empty by instruction).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 8508bbc | Booking event flow as an accessible inline SVG; `v4 · Phase 3` in the meta row and footer; `test_phase3_booking_event_flow_present` and the updated marker test (RED first, then GREEN) |
| 2 | f9e100c | Four more event-flow SVGs, nine decision cards, Q&A, roadmap block, limitations callout, rewritten stale text, the extended test file and the PROJECT.md rows |

The tracer gate: Task 1's `<verify>` passed after the commit (11 passed), the page was still self-contained, so expansion proceeded.

## What the dashboard now says

- **Flows** (new "Event flows (Phase 3)" block after the state machines, one single-column figure each, accent boxes are stores and topics, dashed arrows are timer-driven): booking (`appointment.booked` through outbox, Debezium, the topic, the notification-trigger and analytics-projection groups, the Celery send and the reminder workflow), cancel and reschedule, visit completion (`visit.completed` replaces the sweeper), notifications and reminders (two triggers, one Celery task, delivery log plus provider store, retry, `notifications.dlq`, the real-provider caveat, intent records), analytics (three topics, one transaction, five views, the API and Grafana, cutover and rebuild). Existing step lists were corrected where Phase 3 changed them (booking confirm now writes the outbox row; the notification step is an intent record; the visit flow no longer says a sweeper recovers a lost start).
- **Decision cards (nine):** CDC with Debezium versus a polling publisher (cost accepted: Kafka Connect, `wal_level=logical`, a slot per database retaining WAL, task state, at-least-once); the Connect image deviation (`quay.io/debezium/connect:3.7.1.Final`, `cp-kafka-connect:7.6.13` cannot load Debezium 3.x) with the user's answer "Accept all six (Recommended)"; projections on a Kafka consumer instead of Celery (the EVENT-04 deviation) with the same answer and the note that REQUIREMENTS.md was not edited; Avro with strict serializers; topic per aggregate, with the D-07 scope of "every state change" and the D-13 cutover; three idempotency flavours with the real-provider caveat; Temporal reminders versus Celery ETA or beat; Celery on RabbitMQ 4.3; the sweeper retirement with the safety nets that stay. Older cards that Phase 3 made false were corrected (Event backbone, Background task queue, Observability gained a Phase 3 status line; cross-service integrity no longer promises event-fed copies; the document-store card no longer places analytics projections in Mongo).
- **Q&A:** new questions on lost or duplicated events (no loss, at-least-once on the wire, effect-level exactly-once, with the 5 duplicates of 11 messages and the 2 copies of one event id), why CDC, why Temporal timers, duplicate sends and the real-provider idempotency key, PHI, outage behaviour, analytics rebuild and cutover, why projections are not Celery (EVENT-04), the D-07 scope, and which Phase 2 interim mechanisms stay; rewritten: never-lost-after-commit, tracing (57 spans, 12 services, the long-lived reminder trace, nobody has looked in the Jaeger UI), reference checks, load targets (smoke only; accept latency crept up; sustained, burst, contention deferred by the user; NFR-01..08 not yet measured), and "what is not yet verified after Phase 3".
- **Callouts:** "Interim mechanisms after Phase 3" replaces "Phase 3 replaces them" (sweeper retired; hold reaper, starter, reconcilers, intent-record stubs, polling, the paused poller and `auto-setup` stay, each with its reason); a new "Phase 3 - what was measured, what is only a target, what is open" callout lists the limitations (paused poller and `provider.schedule_changed` published but not consumed, Dockerfile root users accepted for now, no authentication on the dev event network, no retention, WAL retention, Schema Registry as a cached write-path dependency, event-fed copies not built, S3 deferred).
- **Roadmap:** Phase 3 is the active block with the status "implementation plans executed, phase gate and visual checks pending" and states that the phase gate (03-20) has not run. Phases 2 and 2.1 are shown complete, as ROADMAP.md marks them.
- **PROJECT.md:** ten rows with outcome `Implemented (Phase 3)` (CDC, Connect image, EVENT-04, Avro, topic layout, idempotency, Temporal reminders, Celery on RabbitMQ 4.3, sweeper retirement, cutover and wait definition) and one `Pending` row (NFR measurement after Phase 3). Two older rows got one clarifying clause each (event-fed copies were not built; audit-via-outbox was deferred).

## Verification (measured)

- `.venv/bin/python -m pytest tests/docs -q`: **141 passed** (includes the 73 Phase 3 docs-to-code tests and the Phase 2 / 2.1 dashboard tests, none weakened).
- `.venv/bin/python -m pytest tests/docs/test_dashboard.py -q`: **18 passed** (was 9 before this plan; 9 new or rewritten for Phase 3).
- RED was observed before each task's implementation (Task 1: 1 failed; Task 2: 9 failed, then green).
- Structural validation (a scratch script in the session scratchpad, not committed): the HTML parses with balanced tags and no duplicate ids; 9 `<svg` (4 state machines + 5 event flows), each with `role="img"`, `<title>`, `<desc>`, resolving `aria-labelledby` and its own arrow marker; no `<script>`, `<link>`, `@import` or external `url()`/`src`/`href`; text-to-text and text-to-box overlap check on the five new figures after one label was removed (the booking "confirm" label overlapped its neighbour).
- Acceptance greps: `grep -c "v4 · Phase 3"` 2; `grep -c "Phase 3 ·"` 1; `grep -c "Implemented (Phase 3)" .planning/PROJECT.md` 10.
- Diff scanned for key material or credential literals: nothing found (T-3-54). The page names placeholders and synthetic examples only.
- **Not run:** the plan's full `pytest tests -m "not e2e"` (the orchestrator scoped this plan to its named tests plus `tests/docs`); `tests/identity/test_email_migration.py` and the known baseline failure `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` were not run or touched. No stack was started, stopped, rebuilt or restarted; no `.env*` or `infra/secrets/*` content was read; no service code or Compose file changed.

## Rendering was not visually checked

There is no headless browser in this environment, so nobody has seen the page. Layout, SVG legibility (text sizes at the page's scale), diagram arrow routing and the two-column or single-column behaviour are unverified beyond the structural and coordinate checks above. The user checks the page in plan 03-20's final checkpoint. The new figures use a single-column grid (`.sd-wide`, svg `min-width: 720px` inside a horizontally scrolling wrapper) because they are about 800 units wide; that choice is untested in a browser.

## Deviations from Plan

1. **[Rule 1 / 2 - stale content] More of the page was rewritten than the plan lists.** The plan names the Phase 2 "Phase 3 replaces" callout, "Kafka/RabbitMQ stay scaffolding" and the open-risks callout. The same stale statements also appeared in the architecture layers (Messaging "scaffolding only", Observability "workers have no metrics", Orchestration listing the sweeper), the patterns section, the flows step lists, about a dozen older decision cards, the Q&A (reference checks "event infrastructure does not exist yet", "nothing is delegated to Celery yet", "analytics projections in MongoDB", audit "general fix is Phase 3's outbox") and the roadmap. They were rewritten to match architecture.md section 10 so the page does not contradict itself, and `test_stale_text_removed` now lists 15 phrases that must not return.
2. **[Test design] `test_phase2_content_present` needle changed deliberately.** "Phase 3 replaces" became "Interim mechanisms after Phase 3" (the rewritten callout title); the old phrase is asserted gone in `test_stale_text_removed`. The reason is stated in the test.
3. **[Test design] `test_roadmap_marks_phase_2_active` became `test_roadmap_marks_phase_3_active`.** Phase 3 is now the single active block; Phases 1.1, 2 and 2.1 must contain "complete" and not be active (ROADMAP.md marks 2 and 2.1 complete on 2026-11-10); the Phase 3 status line must say "phase gate" and "pending" and must not say "complete". The earlier test kept Phase 2 active because its verification was pending; ROADMAP.md no longer says that.
4. **[Plan wording] More than the plan's tests.** Added `test_phase3_honesty_items_present`, `test_dashboard_never_claims_wire_level_exactly_once` and `test_event_names_the_dashboard_cites_exist_in_the_catalog` (imports `libs.events.catalog`) because the orchestrator's honesty requirements are cheap to enforce and drift otherwise.
5. **[Plan wording] Task 1's figure id.** The booking figure's marker/title ids are `sd-ev-booking-t` and `sd-ev-booking-d` (every event figure uses `sd-ev-<name>-t/-d` and `ah-ev-<name>` so ids stay unique and the Phase 2.1 appointment-diagram test, which looks for `sd-appt-t`, is unaffected).
6. **Smoke wording.** The page says three smoke runs: one before the producers (baseline, 2026-11-10), one after them and one on the finished stack (both 2026-11-11), as `docs/nfr.md` records them. architecture.md 10.9 says "on the Phase 3 stack in three runs", which is looser; the dashboard follows nfr.md and the 03-18 SUMMARY.

## Known Stubs

None. No placeholder text; the fake notification provider is described as the phase's intended provider with its limits.

## Threat Flags

None. No endpoint, auth path or schema changed (a static HTML page, a test file and a planning document). T-3-54: names and synthetic examples only; the self-containment test passes; both deviations carry the user's answer; the real-provider caveat is on the page in four places (callout, card, Q&A, unverified list).

## Self-Check: PASSED

- FOUND: docs/architecture-dashboard.html (`v4 · Phase 3` x2, 5 event-flow SVGs), tests/docs/test_dashboard.py (`test_phase3_flows_present`), `.planning/PROJECT.md` (10 `Implemented (Phase 3)` rows).
- FOUND commits 8508bbc and f9e100c on gsd/phase-02-scheduling-service-workflow; `git rev-list --count 900c266..HEAD` was 2 before this SUMMARY; the working tree has no uncommitted tracked changes.
