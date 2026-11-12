---
phase: 03-event-driven-system-observability
plan: 20
type: execute
wave: 13
depends_on: ["03-19"]
files_modified:
  - tests/docs/test_requirement_traceability.py
autonomous: false
requirements: [DASH-03, DASH-04, EVENT-01, EVENT-02, EVENT-03, EVENT-04, EVENT-05, NOTIF-01, NOTIF-02, NOTIF-03, NOTIF-04, ANLY-01, ANLY-02, ANLY-03, ANLY-04, ANLY-05, ANLY-06, OBS-01, OBS-02, OBS-03]

estimate:
  tokens: 65000
  raw_tokens: 65000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Every Phase 3 requirement maps to existing test files through PHASE3_TRACE, and a test fails if a mapping or a file disappears"
    - "PHASE3_TRACE notes record the EVENT-04 deviation (projections on the analytics-projection Kafka consumer, not Celery) with the user's answer from plan 03-01 Task 2, and the NOTIF-04 real-provider caveat; REQUIREMENTS.md is not edited by this plan"
    - "Phase gate: on a stack rebuilt from the committed code, the full non-e2e suite and the full e2e suite pass and Semgrep is triaged, with the results kept in closeout logs and the SUMMARY"
    - "A human has looked at one booking trace in Jaeger, the four Grafana dashboards with data, the dashboard HTML, and a manual k6 smoke run with their own admin credential"
  artifacts:
    - path: tests/docs/test_requirement_traceability.py
      provides: "PHASE3_TRACE for the 20 Phase 3 requirements and PHASE3_TRACE_NOTES"
      contains: "PHASE3_TRACE"
  key_links:
    - from: tests/docs/test_requirement_traceability.py
      to: tests/e2e/test_phase3_redelivery.py
      via: "EVENT-03 and NOTIF-04 map to the forced-redelivery test"
      pattern: "test_phase3_redelivery"
    - from: tests/docs/test_requirement_traceability.py
      to: tests/notification/test_dedupe.py
      via: "NOTIF-04 maps to the worker-kill-between-provider-call-and-mark_sent test"
      pattern: "test_dedupe"
---

<objective>
Close Phase 3: the requirement-to-test traceability map, the phase-gate regression on a stack rebuilt from the committed code, and the end-of-phase human checks.

Purpose: a regression-proof trail from all 20 Phase 3 requirements to tests, the full-suite gate required before `/gsd-verify-work`, and the visual and manual checks that automation cannot do (Jaeger, Grafana, the dashboard HTML, a manual smoke run).

Output: `PHASE3_TRACE` and `PHASE3_TRACE_NOTES` in `tests/docs/test_requirement_traceability.py`, phase-gate logs under `.planning/phases/03-event-driven-system-observability/closeout-logs/` (untracked, like Phase 2's), the SUMMARY with pass counts and Semgrep triage, and a recorded human sign-off.

Decisions honoured: D-12 (docs and checks reflect what the code does), D-04 (sustained, burst and contention remain deferred; only the smoke profile is run), the user-veto answers from plan 03-01 (quoted, not re-decided). This plan does not edit `.planning/REQUIREMENTS.md`; the verifier and the user own that file.

Live stack rules: Task 2 recreates the application containers from the committed code with `docker compose -f infra/docker-compose.yml up -d --build` (never `down -v`, `system prune` or `volume rm`), then `scripts/compose_healthcheck.sh` and `docker compose -f infra/docker-compose.yml up -d connect-init` (re-registers every connector; the script waits for every task to be RUNNING). Never touch `tjo-*` containers (record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before and after). This is the only wave-13 plan.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-16-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-17-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-19-SUMMARY.md
@tests/docs/test_requirement_traceability.py
@loadtest/README.md
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `PHASE3_TRACE` | test data | 20 requirement ids -> test files |
| `PHASE3_TRACE_NOTES` | test data | `EVENT-04`: the projection deviation and the user's answer; `NOTIF-04`: one send per dedupe key holds at the delivery log and the fake provider's idempotency store, and a real provider must accept an idempotency key |
| closeout logs | logs | `closeout-logs/non-e2e.txt`, `closeout-logs/e2e.txt`, `closeout-logs/semgrep.txt` (pytest and Semgrep output only; no environment dumps) |
| SUMMARY sections | record | "Phase gate" (pass counts, fixes, Semgrep triage, tjo-* comparison), "Human sign-off" (one line per step, verbatim) |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — PHASE3_TRACE ties all 20 requirements to existing test files, with the deviation notes</name>
  <files>tests/docs/test_requirement_traceability.py</files>
  <behavior>
    - `PHASE3_TRACE` contains exactly the 20 Phase 3 ids (EVENT-01..05, NOTIF-01..04, ANLY-01..06, OBS-01..03, DASH-03, DASH-04), each mapped to one or more test files
    - every mapped file exists and defines at least one test function; the Phase 2 checks are unaffected
    - every Phase 3 id exists in REQUIREMENTS.md; the test checks nothing off and edits nothing (the verifier updates REQUIREMENTS.md)
    - `PHASE3_TRACE_NOTES` keys are a subset of `PHASE3_TRACE`; the `EVENT-04` note names `analytics-projection` and contains the user's answer recorded in the plan 03-01 SUMMARY under "User-veto list" (accepted or vetoed); the `NOTIF-04` note names the idempotency key
  </behavior>
  <read_first>
    - tests/docs/test_requirement_traceability.py (PHASE2_TRACE structure and checks)
    - the actual test file names produced by plans 03-01..19 (use the repository, not this plan, if they differ)
    - .planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md ("User-veto list": the EVENT-04 answer)
  </read_first>
  <action>
    Add `PHASE3_TRACE`, `PHASE3_TRACE_NOTES` and parametrized checks mirroring PHASE2_TRACE. Mapping: EVENT-01 -> `tests/events/test_schemas.py`, `tests/e2e/test_phase3_outbox_cdc.py`, `tests/e2e/test_phase3_booking_events.py`, `tests/e2e/test_phase3_registry_events.py`; EVENT-02 -> `tests/events/test_outbox_roundtrip.py`, `tests/scheduling/test_outbox.py`, `tests/visit/test_outbox.py`, `tests/patient/test_outbox.py`, `tests/provider/test_outbox.py`, `tests/e2e/test_phase3_outbox_cdc.py`, `tests/e2e/test_phase3_booking_events.py`, `tests/e2e/test_phase3_visit_downstream.py`; EVENT-03 -> `tests/events/test_consumer_idempotency.py`, `tests/visit/test_visit_consumer.py`, `tests/analytics/test_projections.py`, `tests/e2e/test_phase3_redelivery.py`; EVENT-04 -> `tests/notification/test_tasks.py`, `tests/workflows/test_reminder_workflow.py`, `tests/e2e/test_phase3_notifications.py`; EVENT-05 -> `tests/events/test_consumer_dlq.py`, `tests/notification/test_dlq.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-01 -> `tests/notification/test_triggers.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-02 -> `tests/workflows/test_reminder_workflow.py`, `tests/workflows/test_replay_reminder.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-03 -> `tests/workflows/test_reminder_workflow.py`, `tests/e2e/test_phase3_notifications.py`; NOTIF-04 -> `tests/notification/test_dedupe.py`, `tests/e2e/test_phase3_redelivery.py`; ANLY-01..05 -> `tests/analytics/test_projections.py`, `tests/analytics/test_api.py`, `tests/e2e/test_phase3_analytics.py`, `tests/e2e/test_phase3_metrics.py`; ANLY-06 -> `tests/analytics/test_replay_rebuild.py`, `tests/libs/test_service_isolation.py`; OBS-01 -> `tests/infra/test_grafana_provisioning.py`, `tests/infra/test_prometheus_config.py`, `tests/e2e/test_phase3_metrics.py`; OBS-02 -> `tests/events/test_trace_chain.py`, `tests/e2e/test_phase3_trace.py`; OBS-03 -> `tests/libs/test_observability.py`, `tests/e2e/test_phase3_logs.py`; DASH-03 -> `tests/docs/test_dashboard.py`, `tests/docs/test_phase3_docs.py`; DASH-04 -> `tests/docs/test_dashboard.py`.

    Notes: `EVENT-04` = "projections run on the analytics-projection Kafka consumer (D-10), not a Celery task, because the idempotency claim must share the effect's transaction; Celery covers notifications and follow-up sends; deviation <accepted or vetoed> by the user at plan 03-01 Task 2" (fill in the recorded answer); `NOTIF-04` = "one send per dedupe key at the delivery log and the fake provider's idempotency store; a real SMS/email provider must accept an idempotency key".
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -x -q</automated>
    <fails_when>pytest exits non-zero: a requirement is missing, a mapped file does not exist or has no test function, a note key is not a traced requirement, or the EVENT-04 note lacks the recorded answer.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/docs/test_requirement_traceability.py -q` exits 0 with at least 21 passed for the Phase 3 cases plus the existing Phase 2 cases
    - `git diff --stat HEAD -- .planning/REQUIREMENTS.md` shows no change
  </acceptance_criteria>
  <done>All 20 requirements trace to existing tests, and the deviations are recorded next to the trace.</done>
</task>

<task type="auto">
  <name>Task 2: Phase gate — rebuilt stack, full non-e2e suite, full e2e suite and Semgrep, with a 30-second per-task check on the recorded results</name>
  <files>tests/docs/test_requirement_traceability.py</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-VALIDATION.md (full-suite and e2e commands; do not set `OTEL_SDK_DISABLED`)
    - every 03-NN-SUMMARY.md (known fixes, deviations and measured numbers)
    - .planning/phases/02-scheduling-service-workflow/closeout-logs/ (how Phase 2 kept its gate logs)
  </read_first>
  <action>
    Phase gate (the labelled full-suite step; run it once, in order, and re-run after any fix):
    1. Record the tjo-* status lines. Recreate the application containers from the committed code and confirm health and connector task state (live stack rules).
    2. `.venv/bin/python -m pytest tests -m "not e2e" -q 2>&1 | tee .planning/phases/03-event-driven-system-observability/closeout-logs/non-e2e.txt` (about 19 min; `OTEL_SDK_DISABLED` not set).
    3. `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q 2>&1 | tee .planning/phases/03-event-driven-system-observability/closeout-logs/e2e.txt` (Phase 2 and Phase 3 e2e).
    4. `semgrep --config auto libs services infra scripts 2>&1 | tee .planning/phases/03-event-driven-system-observability/closeout-logs/semgrep.txt`; triage every finding in the SUMMARY (fixed, accepted with reason, or pre-existing and out of scope such as the known Dockerfile root-user findings).
    5. Fix anything this phase broke without weakening a test; record every fix in the SUMMARY, re-run the affected step, and replace its log. If a fix touches the traceability test, keep it in this task's file; any other fix is recorded as a deviation with its file list.
    6. Record the tjo-* status lines again and compare.

    The per-task automated check below reads the recorded logs and runs the fast docs tests; it does not re-run the long suites.
  </action>
  <verify>
    <automated>grep -qE "[0-9]+ passed" .planning/phases/03-event-driven-system-observability/closeout-logs/non-e2e.txt && grep -qE "[0-9]+ passed" .planning/phases/03-event-driven-system-observability/closeout-logs/e2e.txt && ! tail -n 1 .planning/phases/03-event-driven-system-observability/closeout-logs/non-e2e.txt | grep -qE "[0-9]+ (failed|error)" && ! tail -n 1 .planning/phases/03-event-driven-system-observability/closeout-logs/e2e.txt | grep -qE "[0-9]+ (failed|error)" && .venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>A gate log is missing, has no pytest "passed" summary, reports any failed or errored test, or the docs suite exits non-zero (finishes in about 30 s).</fails_when>
  </verify>
  <acceptance_criteria>
    - both gate logs end with a pytest summary line that has passed tests and no failed or errored tests
    - `closeout-logs/semgrep.txt` exists and every finding is triaged in the SUMMARY under "Phase gate"
    - the SUMMARY records the pass counts of both suites, every fix made, and the tjo-* comparison (no restart)
    - `docker compose -f infra/docker-compose.yml exec -T connect curl -s http://localhost:8083/connectors` lists the four connectors, and plan 03-05's `connector_tasks` checks pass
  </acceptance_criteria>
  <done>The phase gate is green on a stack rebuilt from the committed code, and its evidence is recorded.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 3: End-of-phase human checks — Jaeger trace, Grafana dashboards, dashboard HTML, manual k6 smoke</name>
  <what-built>Phase 3 is complete: outbox + Debezium CDC to Kafka with registered Avro schemas, idempotent consumers with DLQs, Celery notifications on RabbitMQ with an idempotent fake provider, Temporal reminders and follow-ups, event-derived analytics behind /api/analytics, Prometheus scraping every process, four provisioned Grafana dashboards, one connected trace per booking, and the closed-out dashboard. The phase gate is green (Task 2). The trace id recorded in the 03-17 SUMMARY can be used for step 1.</what-built>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-17-SUMMARY.md (trace id and span counts)
    - .planning/phases/03-event-driven-system-observability/03-16-SUMMARY.md (dashboard names and smoke numbers)
    - loadtest/README.md (manual smoke instructions)
  </read_first>
  <how-to-verify>
    1. Jaeger: open http://localhost:16686, search the trace id from the 03-17 SUMMARY (or book once through the gateway and search service `smarthealth-gateway`); confirm ONE trace contains the gateway, Scheduling, the Temporal workflow and activities, the Kafka processing span in `smarthealth-notification-consumer` and the Celery `notifications.send` span, nested rather than as separate traces.
    2. Grafana: open http://localhost:3000 with your admin login; open `SmartHealth · Booking & NFR`, `Events`, `Notifications` and `Operations`; confirm panels show data after a few bookings (Operations needs a clinic selected).
    3. Dashboard HTML: open `docs/architecture-dashboard.html` offline in a browser at full and narrow width; check the five Phase 3 flows, the decision cards (including the EVENT-04 and Connect image deviations) and the Q&A read well.
    4. Manual smoke with your own admin (never stored in the repo): seed with `scripts/loadtest_seed.py` as described in `loadtest/README.md`, raise the gateway budget for the run, run `loadtest/run.sh smoke`, restore the budget; confirm the thresholds pass.
  </how-to-verify>
  <resume-signal>Reply "approved", or describe what looked wrong (step number and what you saw)</resume-signal>
  <verify>
    <automated>.venv/bin/python -m pytest tests/docs -q</automated>
    <fails_when>The docs suite exits non-zero, meaning the artifacts under review changed after the phase gate.</fails_when>
  </verify>
  <acceptance_criteria>
    - The user's reply for each of the four steps is recorded in the plan SUMMARY under "Human sign-off"
    - Any issue reported is either fixed (with the affected test re-run) or recorded as a known limitation with the user's agreement
  </acceptance_criteria>
  <done>A human confirmed the trace, the dashboards, the HTML and a manual smoke run.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| traceability map -> reviewers | coverage claims that the verifier relies on |
| phase-gate logs -> the planning directory | test and scanner output written to disk |
| human checker -> Grafana and seed script | the user's own admin credential is used locally |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-55 | Repudiation | requirement coverage claims | low | mitigate | PHASE3_TRACE fails when a requirement, a mapped file or its test functions disappear; deviation notes are test-checked; the gate logs and the human sign-off are recorded verbatim |
| T-3-62 | Information Disclosure | gate logs and the manual checks | medium | mitigate | Logs hold pytest and Semgrep output only (no `env`, `printenv` or Compose config dumps); the logs directory stays untracked like Phase 2's; the human uses their own admin login, typed at the prompt, never written to the repo or the SUMMARY |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/docs -q`
- Phase gate (Task 2): `.venv/bin/python -m pytest tests -m "not e2e" -q` and `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e -m e2e -q`, both green on the rebuilt stack, logs in `closeout-logs/`
- Human sign-off (Task 3)
</verification>

<success_criteria>
- All 20 requirements trace to tests, with the EVENT-04 and NOTIF-04 notes
- Both full suites green on a stack rebuilt from the committed code; Semgrep triaged
- Human checks approved
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-20-SUMMARY.md` when done
</output>
