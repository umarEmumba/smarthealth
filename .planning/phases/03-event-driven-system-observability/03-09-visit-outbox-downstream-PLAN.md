---
phase: 03-event-driven-system-observability
plan: 09
type: execute
wave: 4
depends_on: ["03-04", "03-05"]
files_modified:
  - services/visit/migrations/versions/visit_0003_outbox.py
  - infra/connect/connectors/visit-outbox.json
  - services/visit/events.py
  - services/visit/repository/visit_repo.py
  - services/visit/consumer.py
  - services/visit/config.py
  - services/visit/api/visits.py
  - services/visit/app.py
  - infra/docker-compose.yml
  - tests/visit/conftest.py
  - tests/visit/test_outbox.py
  - tests/visit/test_visit_consumer.py
  - tests/visit/test_downstream.py
  - tests/e2e/test_phase3_visit_downstream.py
autonomous: true
requirements: [EVENT-01, EVENT-02, EVENT-03, ANLY-03, ANLY-05]

estimate:
  tokens: 90000
  raw_tokens: 90000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "visit_db has an insert-only outbox table, a dbz_outbox publication and its own visit-outbox connector whose task is RUNNING (D-01)"
    - "Completing a visit commits a visit.completed outbox row with the status change and history row; the API no longer starts downstream work itself (D-03)"
    - "A visit-downstream consumer turns visit.completed into the existing VisitCompletedWorkflow start (deterministic id visit-completed-{visit_id}) and the conditional downstream marker; a redelivered event starts nothing new (EVENT-03, D-08 refined)"
    - "The kill-between-commit-and-publish proof for this trigger (kill the Visit API right after the completion commit, consumer down, Connect down) runs on a quiet stack in plan 03-11, before plan 03-12 retires the sweeper (D-03 gate)"
    - "visit.completed carries checked_in_at, started_at and completed_at so the analytics wait-time and completed-visit metrics come from the event stream (D-10 revised, ANLY-03, ANLY-05)"
    - "VisitCompletedWorkflow and its activities are unchanged (no workflow.patched); its follow-up and analytics intent stubs stay as intent records"
  artifacts:
    - path: services/visit/consumer.py
      provides: "visit-downstream consumer process (python -m services.visit.consumer) built on EventConsumer"
      contains: "visit-downstream"
    - path: services/visit/events.py
      provides: "VISIT_EVENT_TYPES, emit_visit_completed"
      contains: "enqueue_event"
    - path: tests/e2e/test_phase3_visit_downstream.py
      provides: "gateway completion -> consumer-started workflow (plan 03-11 adds the kill and outage proofs)"
  key_links:
    - from: services/visit/repository/visit_repo.py
      to: services/visit/events.py
      via: "transition_visit emits visit.completed only on the applied COMPLETED branch"
      pattern: "emit_visit_completed"
    - from: services/visit/consumer.py
      to: services/visit/downstream.py
      via: "handler calls ensure_downstream_started(client, db, visit)"
      pattern: "ensure_downstream_started"
---

<objective>
Move the visit-completion downstream trigger onto the outbox (first half of D-03): visit_db gets its outbox and connector, `visit.completed` is committed with the transition, a new visit-downstream consumer starts the unchanged `VisitCompletedWorkflow`, and the API's commit-then-start call is removed. Plan 03-11 then proves the trigger against kills and outages on a quiet stack, and plan 03-12 retires the sweeper Schedule.

Purpose: EVENT-01/02 for the visit aggregate, EVENT-03 (idempotent consumer with deterministic workflow ids), and the event source for completed visits and wait time (ANLY-03/05). D-03 requires the replacement to be proven with a kill-between-commit-and-publish test before the sweeper safety net is removed; that proof is plan 03-11 Task 2.

Output: the `visit_0003` outbox migration, the `visit-outbox` connector, `services/visit/events.py`, emission in `transition_visit`, `services/visit/consumer.py`, consumer settings, the `visit-consumer` Compose service, updated harness tests, and the end-to-end tracer.

Decisions honoured: D-03 (trigger moves to the outbox; the reconciler and other safety nets stay; the sweeper stays registered and running until plan 03-12, so both may race harmlessly through `start_once` and the conditional marker), D-08 refined (Temporal effect idempotency via `visit-completed-{visit_id}` with USE_EXISTING + REJECT_DUPLICATE, no processed-event table), D-10 revised (wait-time timestamps on the event), research Pattern 5 steps 1-3 and 5.

Live stack rules: rebuild `visit` and `visit-worker` and create `visit-consumer` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit visit-worker visit-consumer` (the visit image runs `alembic upgrade head` at start, which applies `visit_0003` on the existing volume), then register only this plan's connector: `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh visit-outbox`. This plan is the only wave-4 plan that edits `infra/docker-compose.yml`. Never touch `tjo-*` containers (record status before and after). Wave 4 coupling: plans 03-07, 03-08 and 03-10 run alongside; none stops, kills or recreates `connect`, `gateway`, `postgres`, `kafka` or another plan's service (the kill and outage proofs for this trigger are in plan 03-11, alone in wave 5); if an e2e seed step fails because a sibling's service was restarting, re-run that test once after `scripts/compose_healthcheck.sh` passes. CDC checks select only `-k "visit-outbox"` because plan 03-08 may add its connector files in the same wave.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-04-SUMMARY.md
@.planning/phases/03-event-driven-system-observability/03-05-SUMMARY.md
@libs/events/migration.py
@infra/connect/connectors/scheduling-outbox.json
@services/visit/repository/visit_repo.py
@services/visit/downstream.py
@services/visit/api/visits.py
@services/visit/worker.py
@services/visit/config.py
@libs/events/consumer.py
@libs/temporal.py
@tests/visit/test_downstream.py
@tests/e2e/phase2_stack.py
@tests/e2e/phase3_stack.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `visit_0003` | Alembic | revision `visit_0003`, down `visit_0002`; `create_outbox` / `drop_outbox` |
| `visit-outbox` | Connect | `visit_db`, `topic.prefix=smarthealth-visit`, slot `dbz_visit`; otherwise identical to `scheduling-outbox.json` |
| `VISIT_EVENT_TYPES`, `emit_visit_completed(db, row)` | module | `services/visit/events.py`; discriminator `completed`; payload `visit_id`, `appointment_id`, `clinic_id`, `patient_id`, `provider_id`, `checked_in_at`, `started_at`, `completed_at` |
| `transition_visit` hook | code | RETURNING extended on the COMPLETED branch; emission only when the transition applied |
| API change | code | the commit-then-start helper and its call in `_transition` removed from `services/visit/api/visits.py`; completion returns as before |
| `services/visit/consumer.py` | process | `build_consumer()`, `handle_visit_completed(envelope)`, `main()`; group `visit-downstream`, topic `smarthealth.visit.events`, DLQ `smarthealth.dlq.visit-downstream`; tracer `smarthealth-visit-consumer` |
| `VisitSettings` additions | settings | `VISIT_CONSUMER_METRICS_PORT` (9100), `VISIT_CONSUMER_HEARTBEAT_PATH` (`/tmp/visit-consumer.alive`) |
| Compose `visit-consumer` | infra | visit image, `python -m services.visit.consumer`, `*visit-env`, visit secrets, depends on visit/temporal/kafka healthy, heartbeat healthcheck, `restart: unless-stopped`, no host port |
| `deliver_visit_events(db, visit_id)` | test helper | `tests/visit/conftest.py`: decodes the visit's outbox rows and runs the consumer handler in-process (harness tests have no running consumer) |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — completing a visit through the gateway starts downstream work via the new visit outbox, its connector, Kafka and the new consumer</name>
  <reversibility rating="costly">D-03: the API no longer starts downstream work; recovery now depends on the outbox, CDC and this consumer (the sweeper is retired in plan 03-12 after plan 03-11's proofs).</reversibility>
  <files>services/visit/migrations/versions/visit_0003_outbox.py, infra/connect/connectors/visit-outbox.json, services/visit/events.py, services/visit/repository/visit_repo.py, services/visit/consumer.py, services/visit/config.py, services/visit/api/visits.py, services/visit/app.py, infra/docker-compose.yml, tests/e2e/test_phase3_visit_downstream.py</files>
  <behavior>
    - After the rebuild and registration, the `visit-outbox` connector task is RUNNING, slot `dbz_visit` is active, and the debezium role gets `permission denied` on `visits` (plan 03-05's parametrized checks)
    - A visit checked in, started and completed through the gateway gets `downstream_started_at` set and a Temporal execution `visit-completed-{visit_id}` within 15 s of the completion (the still-running sweeper's grace is 30 s, so this proves the consumer path)
    - `smarthealth.visit.events` carries one `visit.completed` message keyed by the visit id whose value has the three timestamps
    - The Visit API process makes no Temporal call on completion
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 5 steps 1-3, Pattern 3, Pattern 4 visit-downstream row)
    - services/visit/repository/visit_repo.py (`transition_visit` ~113-150, comment about the marker staying NULL)
    - services/visit/downstream.py (`ensure_downstream_started` ~63-87 and `mark_downstream_started` usage)
    - services/visit/api/visits.py (the commit-then-start helper near line 341 and its call in `_transition` near 393-401; the `request` parameter it needs)
    - services/visit/worker.py (`_connect_with_retry`, heartbeat task pattern) and libs/temporal.py (`connect_client`)
    - libs/events/consumer.py (EventConsumer, NonRetryableEventError, TransientEventError)
    - infra/docker-compose.yml (`visit-worker` service block to mirror)
    - tests/e2e/phase2_stack.py (`book_slot_inside_checkin_window`) and tests/e2e/test_phase2_flows.py (visit flow calls)
    - libs/events/migration.py, services/scheduling/migrations/versions/scheduling_0006_outbox.py, infra/connect/connectors/scheduling-outbox.json (plan 03-05: copy the shape); services/visit/migrations/versions/visit_0002_settlement_markers.py (revision id `visit_0002`)
  </read_first>
  <action>
    Outbox and connector: `visit_0003_outbox.py` (revision `visit_0003`, down `visit_0002`) calling `create_outbox` / `drop_outbox`; `infra/connect/connectors/visit-outbox.json` identical to the scheduling file except `database.dbname=visit_db`, `topic.prefix=smarthealth-visit`, `slot.name=dbz_visit`.

    events.py: `VISIT_EVENT_TYPES = ("visit.completed",)` and `emit_visit_completed(db, row)` calling `enqueue_event(db, event_type="visit.completed", aggregate_id=str(row.id), payload=..., discriminator="completed")`.

    transition_visit: on the COMPLETED target, extend the UPDATE's RETURNING to `id, appointment_id, clinic_id, patient_id, provider_id, checked_in_at, started_at, completed_at`; after the history insert and flush, emit `visit.completed` only when the update applied. Replace the comment about the marker so it says the downstream trigger is the outbox event (D-03). Other targets keep `RETURNING id` behaviour.

    API: delete the commit-then-start helper and its call from `_transition`, and any parameter or import that only it used; keep the response contract identical. Remove the Temporal client wiring from `services/visit/app.py` only if nothing else in the API uses it (check `api/deps.py` usages first); add `await warm_up(VISIT_EVENT_TYPES)` to the lifespan.

    consumer.py: `handle_visit_completed(envelope)` opens a session on the visit database (`get_session_factory(settings.database_url)` created inside the consumer's persistent loop), loads the visit by `envelope.payload["visit_id"]`; missing -> `NonRetryableEventError("VisitNotFound")`; otherwise `await ensure_downstream_started(client, db, visit)`. Temporal connectivity failures (`temporalio.service.RPCError` with an unavailable status, `RuntimeError` from a lost connection) -> `TransientEventError`. `main()` calls `configure_logging()`, `configure_tracer_provider("smarthealth-visit-consumer")`, connects Temporal eagerly with retry (as the worker does), warms the registry, builds `EventConsumer(group_id="visit-downstream", topics=["smarthealth.visit.events"], handlers={"visit.completed": ...}, dlq_topic=DLQ_TOPICS["visit-downstream"], heartbeat_path=..., metrics_port=...)` and runs it. The workflow start happens inside the consumer span, so the downstream workflow joins the visit's trace through `TracingInterceptor`.

    config.py: the two consumer settings from the artifacts table.

    Compose: `visit-consumer` as in the artifacts table (healthcheck: heartbeat file younger than 30 s, `start_period: 40s`).

    e2e (marker e2e): complete a visit through the gateway, then poll visit_db for the marker and `client.get_workflow_handle("visit-completed-{visit_id}").describe()` on `localhost:7233`, asserting both within 15 s, and consume the event.

    Apply: the rebuild command and the connector registration from the objective's live stack rules, then the tests.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "visit-outbox" -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, the visit-outbox task is not RUNNING, the debezium role can read `visits`, the marker or the workflow appears later than 15 s after completion, or no visit.completed message arrives.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/alembic -c services/visit/migrations/alembic.ini upgrade head` exits 0 on the host
    - the tracer test passes on the rebuilt stack
    - `docker compose -f infra/docker-compose.yml ps visit-consumer --format '{{.Health}}'` prints `healthy`
    - `grep -c "emit_visit_completed" services/visit/repository/visit_repo.py` is 1
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase2_flows.py -m e2e -q` still passes (the visit flow finalizes billing through the new path)
    - tjo-* status lines before and after show no restart
  </acceptance_criteria>
  <done>Visit completion reaches downstream work only through the outbox, Kafka and the visit-downstream consumer.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Consumer idempotency and failure handling; harness tests moved from the API trigger to the consumer</name>
  <files>services/visit/consumer.py, tests/visit/conftest.py, tests/visit/test_outbox.py, tests/visit/test_visit_consumer.py, tests/visit/test_downstream.py</files>
  <behavior>
    - `transition_visit(..., target=COMPLETED)` writes exactly one `visit.completed` row committed with the transition; a repeat completion and other targets write none; a rollback writes none; the payload has the visit's ids and timestamps and no free text
    - The handler run twice for the same envelope starts one workflow execution and sets the marker once (second call returns False from `start_once` and changes nothing)
    - A `visit.completed` for an unknown visit id raises `NonRetryableEventError` (dead-lettered by the base)
    - A Temporal client that raises an unavailable RPC error makes the handler raise `TransientEventError` and leaves the marker NULL
    - The harness completion tests that relied on the API trigger now complete via the API, then `deliver_visit_events(db, visit_id)`, and still observe billing finalized, the appointment completed and a single run; the obsolete "start deferred" log test becomes a test that the completion route makes no Temporal call
  </behavior>
  <read_first>
    - services/visit/consumer.py (Task 1)
    - tests/visit/test_downstream.py (tests at lines ~163, 188, 259, 331, 451, 486 depend on the API trigger; the sweep tests at ~210, 302, 361 stay for plan 03-12)
    - tests/visit/conftest.py, tests/temporal_fixtures.py (time-skipping and test-worker fixtures)
    - libs/events/registry.py (`deserializer`) for decoding outbox rows in the helper
  </read_first>
  <action>
    Add `deliver_visit_events(db, visit_id, *, client)` to `tests/visit/conftest.py`: read the visit's outbox rows, decode each payload with `deserializer()`, build `EventEnvelope`s and await `handle_visit_completed` with the test's Temporal client injected (make the handler accept an injected client and session factory for tests, defaulting to the process ones). Write `tests/visit/test_outbox.py` and `tests/visit/test_visit_consumer.py` for the behaviors (marker `integration`). Update the API-trigger-dependent tests in `tests/visit/test_downstream.py` as described, keeping their assertions on billing, appointment completion and single runs; leave the sweep tests untouched (plan 03-12 rewrites them).
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/visit tests/workflows/test_visit_completed.py tests/workflows/test_replay_visit_completion.py -x -q</automated>
    <fails_when>pytest exits non-zero, a redelivered event starts a second run, or a Temporal outage is dead-lettered.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/visit/test_visit_consumer.py tests/visit/test_outbox.py -q` exits 0 with at least 8 passed
    - `.venv/bin/python -m pytest tests/visit -q` exits 0
    - `test_completion_route_makes_no_temporal_call` (a Temporal client double that fails on any call is injected into the Visit app) passes
    - `.venv/bin/python -m pytest tests/workflows -q` exits 0 (VisitCompletedWorkflow replay unchanged)
    - `git diff --stat HEAD -- services/visit/workflows` shows no change in this plan
  </acceptance_criteria>
  <done>The consumer is idempotent and outage-safe, and the harness tests exercise the event path instead of the removed API trigger.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka `smarthealth.visit.events` -> visit-consumer -> Temporal | an event starts billing finalization and appointment completion |
| Visit API -> outbox | the completion commit is now the only trigger |
| visit_db WAL -> Debezium | the `visit-outbox` connector reads the visit database's outbox |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-27 | Tampering | forged or replayed visit.completed | medium | mitigate | Handler loads the visit from visit_db and acts only on a real completed visit; deterministic id `visit-completed-{visit_id}` with REJECT_DUPLICATE and the conditional marker make replays no-ops; unknown ids are dead-lettered |
| T-3-28 | Denial of Service | Temporal outage during consumption | medium | mitigate | `TransientEventError` keeps the event (no commit, re-seek, capped backoff), so an outage delays but never drops a trigger; the visit reconciler still settles started-but-unsettled visits |
| T-3-29 | Information Disclosure | visit.completed payload, consumer logs, `visit-outbox` connector | medium | mitigate | Ids and timestamps only; consumer logs event ids and types; no metrics host port; the connector captures `public.outbox` only and the parametrized e2e check proves `permission denied` on `visits` |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/visit tests/workflows -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase2_flows.py -m e2e -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "visit-outbox" -q` and `.venv/bin/python -m pytest tests/infra/test_cdc_config.py -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- Semgrep: `semgrep --config auto services/visit`; record status in the SUMMARY
</verification>

<success_criteria>
- visit_db relays through its own RUNNING connector; visit.completed is committed with the transition and is the only downstream trigger
- The visit-downstream consumer starts the unchanged workflow exactly once per visit under redelivery; the kill and outage proofs follow in plan 03-11
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-09-SUMMARY.md` when done
</output>
