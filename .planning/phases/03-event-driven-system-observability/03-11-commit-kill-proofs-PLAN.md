---
phase: 03-event-driven-system-observability
plan: 11
type: execute
wave: 5
depends_on: ["03-07", "03-08", "03-09", "03-10"]
files_modified:
  - tests/e2e/test_phase3_booking_events.py
  - tests/e2e/test_phase3_visit_downstream.py
autonomous: true
requirements: [EVENT-02, EVENT-03]

estimate:
  tokens: 65000
  raw_tokens: 65000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Killing the process that committed an appointment transition right after the commit does not lose the event: the cancelled event id still arrives on Kafka (roadmap success criterion 1, delivery half)"
    - "An appointment event committed while Connect is stopped is delivered once by event id after Connect starts (duplicates on the wire are counted, not assumed away)"
    - "Killing the Visit API right after the completion commit, stopping visit-consumer during a completion, or stopping Connect during a completion each still ends with exactly one VisitCompletedWorkflow execution and the marker set: the D-03 kill-between-commit-and-publish gate that must pass before plan 03-12 retires the sweeper"
    - "The k6 smoke profile still passes on a quiet stack after the booking path gained its outbox insert (D-04), and the gateway is back on its default rate budget afterwards"
    - "Every stopped or killed service is restored and healthy when the plan ends, with every registered connector task RUNNING"
  artifacts:
    - path: tests/e2e/test_phase3_booking_events.py
      provides: "kill-after-commit (scheduling-worker) and Connect-down delivery for appointment events"
      contains: "kill_service"
    - path: tests/e2e/test_phase3_visit_downstream.py
      provides: "kill-after-commit (visit API), consumer-down and Connect-down proofs for the visit downstream trigger"
      contains: "list_workflows"
  key_links:
    - from: tests/e2e/test_phase3_visit_downstream.py
      to: services/visit/consumer.py
      via: "after a kill or outage the visit-downstream consumer starts visit-completed-{visit_id} exactly once"
      pattern: "visit-completed-"
    - from: tests/e2e/test_phase3_booking_events.py
      to: tests/e2e/phase3_stack.py
      via: "kill_service / stop_service / start_service / wait_connectors_running helpers scoped to the smarthealth project"
      pattern: "wait_connectors_running"
---

<objective>
Prove, on a quiet stack, that a process killed between its database commit and the event publish never loses the event, for both producers whose downstream effects matter most: Scheduling (appointment events) and Visit (the downstream trigger that replaces the sweeper). Then run the k6 smoke profile on the changed booking path.

Purpose: roadmap success criterion 1 (killing the process between commit and publish never loses the event) and the D-03 precondition: "the replacement must be proven with a kill-between-commit-and-publish test" before plan 03-12 removes the sweeper safety net. D-04: the smoke profile must still pass after the outbox insert joined the confirm step.

Output: the disruptive e2e scenarios moved here from the producer plans (03-07, 03-09) so that no other plan's live tests run while containers are killed, Connect is stopped or the gateway is recreated, plus the smoke result.

Decisions honoured: D-01 (CDC: Connect being down delays, never loses), D-03 (kill-between-commit-and-publish proof before retiring the sweeper; the sweeper is still running here but cannot act within its 30 s grace window, which the visit tests stay inside), D-04 (smoke must pass; sustained, burst and contention stay deferred), research F4 and Pitfall 3 (at-least-once on the wire, duplicates counted).

Live stack rules: this is the only wave-5 plan, so it runs on a quiet stack. Kill, stop and start only `smarthealth` Compose services (`scheduling-worker`, `visit`, `visit-consumer`, `connect`, `gateway`) through `docker compose -f infra/docker-compose.yml ...` via the `phase3_stack.py` helpers; every test restores what it stopped or killed in `finally`, waits for health, and for Connect waits for every registered connector task to be RUNNING (`wait_connectors_running()`). Never touch `tjo-*` containers (record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before and after). No image rebuild in this plan.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-07-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-09-SUMMARY.md
@tests/e2e/phase2_stack.py
@tests/e2e/phase3_stack.py
@tests/e2e/test_phase3_booking_events.py
@tests/e2e/test_phase3_visit_downstream.py
@loadtest/README.md
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `test_cancel_event_survives_worker_kill_after_commit` | e2e test | `tests/e2e/test_phase3_booking_events.py` |
| `test_booked_event_delivered_after_connect_restart` | e2e test | same file; duplicate count logged |
| `test_visit_trigger_survives_api_kill_after_commit`, `test_visit_trigger_catches_up_after_consumer_outage`, `test_visit_trigger_survives_connect_outage` | e2e tests | `tests/e2e/test_phase3_visit_downstream.py`; executions counted with `list_workflows` |
| SUMMARY evidence | record | duplicate copies on the wire after the Connect restart; k6 accept p95/p99 and saga p95/p99 next to the 2026-11-10 baseline (accept p95 21.8 ms, saga p95 0.52 s) |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — the scheduling worker killed right after a cancel commit; the cancelled event still arrives, and a booking committed while Connect is down is delivered after it starts</name>
  <files>tests/e2e/test_phase3_booking_events.py</files>
  <behavior>
    - Cancel a confirmed booking through the gateway; as soon as the appointment reads `cancelled`, kill `scheduling-worker` (the process that committed the transition); the `appointment.cancelled` event id arrives on `smarthealth.appointment.events` within 60 s; the worker is started again and becomes healthy
    - With `connect` stopped, a booking confirms; no booked event for it arrives for 10 s; after Connect starts and every connector task is RUNNING, the booked event id arrives; the number of copies is logged and recorded (not asserted to be one)
  </behavior>
  <read_first>
    - tests/e2e/test_phase3_booking_events.py (plan 03-07: booking, cancel and reschedule helpers), tests/e2e/phase3_stack.py (kill/stop/start/wait helpers, `consume_matching`)
    - tests/e2e/test_phase2_flows.py (cancel through the gateway)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Kill-between-commit-and-publish tests (shape)", Pitfall 3)
  </read_first>
  <action>
    Add the two scenarios to `tests/e2e/test_phase3_booking_events.py`, each in a fresh clinic. Use the `phase3_stack.py` kill and stop helpers (fixed argv through `docker compose -f infra/docker-compose.yml`), restore in `finally`, wait for health and for `wait_connectors_running()`. Identify messages by the `id` header and the appointment key. Record the observed copy count in the SUMMARY.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k "worker_kill or connect_restart" -x -q</automated>
    <fails_when>pytest exits non-zero, the cancelled event id is missing after the kill, the booked event id never arrives after Connect restarts, or `scheduling-worker` or `connect` is not healthy afterwards.</fails_when>
  </verify>
  <acceptance_criteria>
    - both new tests pass
    - after the run `docker compose -f infra/docker-compose.yml ps scheduling-worker connect --format '{{.Service}} {{.Health}}'` reports both healthy
    - the SUMMARY records the copy count after the Connect restart
  </acceptance_criteria>
  <done>Appointment events survive a kill of the committing process and a Connect outage with no loss.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: The D-03 gate — the visit downstream trigger survives a kill of the committing API, a consumer outage and a Connect outage</name>
  <files>tests/e2e/test_phase3_visit_downstream.py</files>
  <behavior>
    - Complete a visit through the gateway and, as soon as the 200 arrives, kill `visit`; downstream work still starts exactly once (one execution of `visit-completed-{visit_id}`, marker set); `visit` is started again and becomes healthy
    - With `visit-consumer` stopped, complete a visit: no workflow and a NULL marker for 15 s; start the consumer: the workflow starts once and the marker is set
    - With `connect` stopped, complete a visit; start `connect`: downstream starts once after every connector task is RUNNING
  </behavior>
  <read_first>
    - tests/e2e/test_phase3_visit_downstream.py (plan 03-09 tracer) and tests/e2e/phase3_stack.py
    - services/visit/consumer.py and services/visit/downstream.py (plan 03-09: deterministic id and conditional marker)
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Kill-between-commit-and-publish tests (shape)")
  </read_first>
  <action>
    Add the three scenarios; each restores what it stopped or killed in `finally` and waits for health (Connect: every registered connector task RUNNING). Count executions with `client.list_workflows(f"WorkflowId = 'visit-completed-{visit_id}'")` on `localhost:7233` and assert exactly one. Keep each outage window at 15 s so the still-running sweeper (30 s grace, retired in plan 03-12) cannot be the one that starts the workflow; note this in the SUMMARY as the evidence that the outbox path, not the sweeper, recovered each case.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -x -q</automated>
    <fails_when>pytest exits non-zero, downstream never starts after a kill or outage, more than one execution exists for a visit, or `visit`, `visit-consumer` or `connect` is not healthy afterwards.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -q` exits 0 with at least 4 passed
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase2_flows.py -m e2e -q` still passes after the kills
    - the SUMMARY states the D-03 gate passed, with the three scenario results
  </acceptance_criteria>
  <done>The outbox trigger is proven against a kill of the committing API, a consumer outage and a Connect outage: the D-03 precondition for retiring the sweeper.</done>
</task>

<task type="auto">
  <name>Task 3: The k6 smoke profile on the quiet stack after the booking path change</name>
  <files>tests/e2e/test_phase3_booking_events.py</files>
  <read_first>
    - loadtest/README.md (smoke profile, raised gateway budget) and tests/e2e/test_loadtest_smoke.py (seeds its own admin and world)
    - docs/nfr.md (baseline numbers and what stays deferred)
  </read_first>
  <action>
    Run the k6 smoke profile through the existing wrapper test with the gateway budget raised for the run only: `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`, then `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q`, then recreate the gateway without the overrides (`docker compose -f infra/docker-compose.yml up -d --no-deps gateway`). The wrapper seeds its own world with a test-created admin; do not create or hardcode any other admin credential. Record the accept p95/p99 and saga p95/p99 from the k6 summary in the SUMMARY next to the 2026-11-10 baseline. Then add `test_gateway_default_rate_budget_restored` to `tests/e2e/test_phase3_booking_events.py`, which reads the gateway's effective default budget through `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` and asserts it is not 100000. Do not run sustained, burst or contention (deferred by the user until after Phase 3).
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_loadtest_smoke.py -m e2e -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py -m e2e -k rate_budget -x -q</automated>
    <fails_when>Either pytest run exits non-zero, k6 reports a crossed smoke threshold, or the gateway still runs with the raised budget.</fails_when>
  </verify>
  <acceptance_criteria>
    - `tests/e2e/test_loadtest_smoke.py` passes and the SUMMARY records accept p95/p99 and saga p95/p99 beside the baseline
    - `test_gateway_default_rate_budget_restored` passes
    - tjo-* status lines before and after the plan show no restart
  </acceptance_criteria>
  <done>The booking path still meets the smoke thresholds after Phase 3's outbox insert, and the gateway is back on its default budget.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| test harness -> live smarthealth containers | tests kill and stop services on the shared dev stack |
| test harness -> gateway rate limit | the budget is raised for one run and must come back |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-60 | Denial of Service | tests leaving the dev stack degraded or the gateway budget raised | medium | mitigate | Alone in wave 5; every kill or stop restored in `finally` with health and connector-task waits; the gateway is recreated with defaults and a test asserts it; only `smarthealth` services touched; tjo-* checked before and after |
| T-3-61 | Repudiation | claiming the D-03 gate or "never lost" without evidence | medium | mitigate | Deterministic kill points (right after the commit is observable), executions counted by `list_workflows`, wire copies counted and recorded, outage windows kept under the sweeper's grace so the sweeper cannot mask a failure |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_booking_events.py tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "connector_tasks" -q` (all four connectors RUNNING after the plan)
- `scripts/compose_healthcheck.sh` passes; tjo-* containers unchanged
</verification>

<success_criteria>
- No event is lost when the committing process is killed or Connect is down; wire duplicates are measured
- The D-03 gate passes for the visit trigger; the smoke thresholds hold on a quiet stack
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-11-SUMMARY.md` when done
</output>
