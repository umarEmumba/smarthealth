---
phase: 03-event-driven-system-observability
plan: 09
subsystem: visit
tags: [transactional-outbox, debezium, kafka, consumer, temporal, idempotency, phi-free-events]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-01 enqueue_event / warm_up, 03-03 VisitCompleted Avro schema, 03-04 EventConsumer, 03-05 create_outbox migration helper, connector registration script and CDC tests"
provides:
  - visit_0003 outbox migration (table, dbz_outbox publication, guarded debezium grant) and the visit-outbox connector (slot dbz_visit), RUNNING
  - services/visit/events.py (VISIT_EVENT_TYPES, emit_visit_completed)
  - visit.completed committed with the completion in transition_visit (applied COMPLETED branch only)
  - services/visit/consumer.py (group visit-downstream; build_consumer, handle_visit_completed, main) and the visit-consumer Compose service
  - the Visit API no longer starts downstream work (D-03 trigger moved to the outbox)
  - 7 outbox tests, 11 consumer tests, 1 e2e tracer, deliver_visit_events harness helper
affects: [03-10, 03-11, 03-12, 03-13, 03-17]
tech-stack:
  added: []
  patterns:
    - "Hook at the repository function that commits: widen RETURNING on the applied branch, flush, emit; no-op and skipped branches emit nothing"
    - "Consumer handler = classify then delegate: NonRetryable for events that can never succeed, Transient for infrastructure outages, everything else left to the base's bounded retries"
    - "Temporal client is connected lazily on the consumer's persistent loop (a client must live on the loop that uses it)"
    - "Harness tests stand in for CDC plus the consumer with deliver_visit_events (decode the real outbox rows, call the handler with an injected client)"
key-files:
  created:
    - services/visit/migrations/versions/visit_0003_outbox.py
    - infra/connect/connectors/visit-outbox.json
    - services/visit/events.py
    - services/visit/consumer.py
    - tests/visit/test_outbox.py
    - tests/visit/test_visit_consumer.py
    - tests/e2e/test_phase3_visit_downstream.py
  modified:
    - services/visit/repository/visit_repo.py
    - services/visit/api/visits.py
    - services/visit/api/deps.py
    - services/visit/app.py
    - services/visit/config.py
    - services/visit/downstream.py
    - infra/docker-compose.yml
    - tests/visit/conftest.py
    - tests/visit/test_downstream.py
    - tests/workflows/test_replay.py
    - tests/workflows/test_replay_visit_billing.py
    - tests/workflows/test_replay_visit_completion.py
key-decisions:
  - "The Temporal client is created lazily (connect_client(lazy=True)) inside the handler on the consumer's persistent loop instead of an eager connect in main(): a client built on a throw-away loop cannot be used from the EventConsumer's Runner loop, and the lazy client lets the process boot while Temporal is down (the first event then retries as transient)"
  - "A visit.completed for a visit that exists but is not completed is NonRetryableEventError('VisitNotCompleted') in addition to the planned unknown-id case: the committed event always implies a completed visit, so this is a forged or replayed event (T-3-27)"
  - "Database outages (OperationalError, InterfaceError, invalidated connection, OSError) are TransientEventError like Temporal outages, so a Postgres restart never dead-letters a valid trigger"
  - "The API's get_temporal_client dependency, the app.state.temporal_client attribute and the opportunistic retry of the start on a repeated complete were removed (nothing else used them)"
requirements-completed: []
status: complete
duration: about 1h50 wall time (image build about 70 s, 7 minute visit/workflows regression, laptop idle gaps)
completed: 2026-11-11
actuals:
  tokens: 14500
  tasks: 2
  commits: 2
plan_head_before: 480f4d6c34217fbd31c0b771c4b18378be24091c
plan_head_after: 1fc848fa188b82dbb5879ac31a87661c1548bf20
---

# Phase 3 Plan 09: Visit Outbox and Downstream Consumer Summary

**Completing a visit now commits a PHI-free `visit.completed` outbox row with the status change; Debezium relays it from visit_db's own connector (visit-outbox, RUNNING) to `smarthealth.visit.events`, and a new `visit-downstream` consumer starts the unchanged `VisitCompletedWorkflow` (deterministic id `visit-completed-{visit_id}`) and sets the conditional marker. The Visit API makes no Temporal call any more.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (EVENT-01/02/03, ANLY-03/05 stay for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | dc1da46 | `visit_0003`, `visit-outbox.json`, `events.py`, `transition_visit` emission, API helper removed, `consumer.py`, settings, `visit-consumer` Compose service, e2e tracer |
| 2 | 1fc848f | `deliver_visit_events` helper, `test_outbox.py` (7), `test_visit_consumer.py` (11), API-trigger tests moved to the consumer in `test_downstream.py`, the three opt-in history-recording tests |

## What was built

- **Outbox and connector.** `visit_0003` (down `visit_0002`) calls `create_outbox` / `drop_outbox`. `visit-outbox.json` differs from the other connector files only in `database.dbname=visit_db`, `topic.prefix=smarthealth-visit`, `slot.name=dbz_visit`; `table.include.list=public.outbox`, so `visits` cannot be captured (the parametrized e2e check proves `permission denied` for the debezium role). The migration was applied on the host (`alembic ... upgrade head`) and again, as a no-op, by the rebuilt `visit` container.
- **Emission.** `transition_visit` widens `RETURNING` to `id, appointment_id, clinic_id, patient_id, provider_id, checked_in_at, started_at, completed_at` on the COMPLETED target only and calls `emit_visit_completed` after the history insert and flush, only when the conditional UPDATE matched. Event id = uuid5(`visit id : visit.completed : completed`), `occurred_at` = the completion time.
- **API.** `_start_downstream`, the `request` parameter, `get_temporal_client` and `app.state.temporal_client` are gone; the response contract is unchanged. The lifespan calls `warm_up(VISIT_EVENT_TYPES)`.
- **Consumer.** `services/visit/consumer.py`: group `visit-downstream`, topic `smarthealth.visit.events`, DLQ `smarthealth.dlq.visit-downstream`, tracer `smarthealth-visit-consumer`, heartbeat `/tmp/visit-consumer.alive`, metrics port 9100 (container-internal; Compose publishes no host port). `handle_visit_completed` loads the visit, raises `NonRetryableEventError` (`VisitNotFound`, `VisitNotCompleted`, `InvalidVisitId`) or `TransientEventError` (Temporal `RPCError` with an unavailable-class status, `RuntimeError`, timeouts, connection and database errors), else calls `ensure_downstream_started` unchanged.
- **Compose.** `visit-consumer` mirrors `visit-worker` (visit image, `*visit-env`, `*visit-secrets`, depends on visit, temporal and kafka healthy, heartbeat healthcheck younger than 30 s, `start_period: 40s`, `restart: unless-stopped`, no ports). `VisitCompletedWorkflow`, its activities and `services/visit/workflows` are untouched (`git diff --stat HEAD -- services/visit/workflows` is empty; replay tests pass).

## Verification (measured)

- `.venv/bin/alembic -c services/visit/migrations/alembic.ini upgrade head`: exit 0.
- `tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "visit-outbox"`: 3 passed (connector and task RUNNING, `permission denied` on `visits`, config shape). `tests/infra` (including `test_cdc_config.py`): 21 passed.
- E2E tracer `tests/e2e/test_phase3_visit_downstream.py -k tracer`: passed (4 s end to end): marker set and Temporal execution `visit-completed-{id}` present within 15 s of the gateway completion, one `visit.completed` message keyed by the visit id with `checked_in_at <= started_at <= completed_at`, billing finalized and appointment completed afterwards. `tests/e2e/test_phase2_flows.py`: 5 passed with the new path (together with the tracer: 6 passed).
- `docker compose ... ps visit-consumer --format '{{.Health}}'`: `healthy`. `scripts/compose_healthcheck.sh`: OK. All four slots (`dbz_scheduling`, `dbz_patient`, `dbz_provider`, `dbz_visit`) active. `grep -c emit_visit_completed services/visit/repository/visit_repo.py` = 1.
- `tests/visit/test_visit_consumer.py` + `tests/visit/test_outbox.py`: 18 passed (plan: at least 8).
- `.venv/bin/python -m pytest tests/visit tests/workflows tests/libs/test_service_isolation.py -q` (run with the live `visit-consumer` stopped, see below): first run 384 passed, 11 skipped, 1 failed (`test_complete_does_not_wait_for_downstream_work`, my incomplete edit of that test: the replace did not match, so it still expected the API to start the workflow). After the fix `tests/visit/test_downstream.py`: 18 passed. The 11 skips are the opt-in history-recording tests and pre-existing skips. Not re-run in one piece after that one-file fix; the other files were unchanged.
- `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` (known unrelated failure) was not run.
- The plan's full `pytest tests -m "not e2e"` was not run (orchestrator: not required).
- Semgrep: `semgrep --config p/python --metrics=off services/visit`: 151 rules, 31 files, **0 findings** (narrower than the plan's `--config auto`, which is refused with `--metrics=off`, as in earlier Phase 3 plans).

## Deviations from Plan

1. **[Rule 1/3 - Design] Lazy Temporal connect instead of eager connect-with-retry in `main()`.** The plan asks for an eager connect as the worker does. A Temporal client must live on the loop that uses it, and the consumer's handlers run on `EventConsumer`'s persistent `asyncio.Runner` loop, so an eager client made in `main()` would belong to another loop. The client is created with `connect_client(lazy=True)` on the first handled event. Side benefit: the container no longer crashes if Temporal is down at start; the event is retried as transient.
2. **[Rule 2] `VisitNotCompleted` and `InvalidVisitId` are non-retryable too,** and database outages are transient (see key-decisions). The plan listed only the unknown-id and Temporal-outage cases.
3. **[Plan wording] `visit.completed` for a visit with no recorded start.** `started_at` is nullable in the Avro schema; the builder passes whatever the row has. The transition path always sets it (steps are one-step forward), so in practice all three timestamps are present.
4. **[Process] Harness tests were run with the live `visit-consumer` stopped** (see "Cross-plan findings"); `docker compose stop visit-consumer`, then `start` after the suite. Nothing else was stopped or recreated.
5. **[Rule 1, my own test edit] First `test_downstream.py` edit missed one test** (the string replace did not match); caught by the full visit run and fixed before the Task 2 commit.
6. **[Plan wording] Task 2 tests were written after the implementation** (the tracer commit already contained the consumer), not red-first.
7. The three opt-in history-recording tests (`SMARTHEALTH_RECORD_HISTORIES=1`; skipped by default) were adapted to deliver the event, since they relied on the API trigger. They were not run.
8. **Semgrep ruleset** `p/python --metrics=off`, not `--config auto`.

No auth gates, no package installs, no architectural changes.

## Cross-plan findings (important for 03-11, 03-12, 03-14, 03-17)

- **The live `visit-consumer` races any harness test that completes a visit.** The in-process harness writes real rows into the live visit_db; Debezium relays them and the live consumer starts a real workflow for the harness visit within about a second (and the live visit worker then runs it against live Billing/Scheduling), so assertions such as "marker is still NULL after completion" (the sweep tests, which stay until plan 03-12) fail or flake while it runs. I ran the harness suites with `docker compose -f infra/docker-compose.yml stop visit-consumer` and started it again afterwards. Plans that run `tests/visit` (03-11, 03-12) should do the same, or rewrite those tests not to read the marker. The same will apply to the notification-trigger and analytics consumers (03-10, 03-14) for tests that write appointment/visit events.
- **Throw-away events reach the DLQ.** After restarting, the consumer drained the backlog: 85 `visit.completed` events for harness visits that no longer exist were dead-lettered as `NonRetryableEventError` (`VisitNotFound`; error type only in the headers), 2 real e2e events processed, group lag 0. `smarthealth.dlq.visit-downstream` therefore holds about 85 throw-away records; plan 03-11/03-17 DLQ checks must filter by key or event id rather than count the topic.
- `deliver_visit_events(db, visit_id, *, client)` (`tests/visit/conftest.py`) is the harness stand-in for CDC plus the consumer; `db` may be None (opens a short-lived engine). `cleanup_visit_outbox` deletes the outbox rows of a world's visits at teardown (hygiene only; the production outbox stays insert-only; rows already relayed stay in Kafka).
- The Phase 2 sweeper Schedule and the run-on-start sweep still run and may race the consumer harmlessly (`start_once` plus the conditional marker). Plan 03-11 owns the kill-between-commit-and-publish proof; plan 03-12 retires the sweeper.

## Live stack

- Migration applied on the host first, then `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit visit-worker visit-consumer` (all three healthy), then `register-connectors.sh visit-outbox` (connector and tasks RUNNING). `gateway`, `connect`, `postgres`, `kafka`, `temporal` were not rebuilt, stopped, killed or recreated. No `down -v`, no volume touched. `.env*` and `infra/secrets/*` were not read.
- `tjo-*` before and after: `tjo-api Up 26 hours (unhealthy)`, `tjo-postgres Up 26 hours (healthy)`, no restart (`tjo-api` was already unhealthy).
- Live data left behind: visit_db's outbox holds the rows of the e2e and regression visits that were not cleaned (harness visits are removed at teardown; e2e visits stay, as in earlier plans) and the topic carries the relayed copies.

## Known Stubs

None. `VisitCompletedWorkflow`'s follow-up and analytics intent activities remain intent records, as the plan specifies.

## Threat Flags

None beyond the plan's register. T-3-27 (forged or replayed event): the handler loads the visit and acts only on a completed one; deterministic id with REJECT_DUPLICATE plus the conditional marker make replays no-ops (test: a second delivery returns False from `start_once`, same run id, same marker); unknown and not-completed ids are non-retryable. T-3-28 (Temporal outage): transient classification keeps the event, proven by the outage tests and by the retried event succeeding later. T-3-29 (disclosure): the payload has exactly the 13 PHI-free fields (a test asserts the set), logs carry event ids and types, DLQ headers carry error type only (seen in the live DLQ), no metrics host port, the connector captures `public.outbox` only.

## Self-Check: PASSED

- FOUND: services/visit/migrations/versions/visit_0003_outbox.py, infra/connect/connectors/visit-outbox.json, services/visit/events.py, services/visit/consumer.py, tests/visit/test_outbox.py, tests/visit/test_visit_consumer.py, tests/e2e/test_phase3_visit_downstream.py (all committed).
- FOUND commits dc1da46, 1fc848f on gsd/phase-02-scheduling-service-workflow (`git rev-list --count 480f4d6..HEAD` = 2 before the docs commit).
