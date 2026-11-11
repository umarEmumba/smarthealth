---
phase: 03-event-driven-system-observability
plan: 11
subsystem: events-reliability
tags: [kill-after-commit, transactional-outbox, debezium, connect-outage, d-03-gate, k6-smoke, temporal]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-05 phase3_stack helpers and connectors, 03-07 scheduling outbox, 03-09 visit outbox and visit-downstream consumer"
provides:
  - "Proof that an appointment event survives a SIGKILL of the committing scheduling-worker and a Connect outage"
  - "The D-03 gate: the visit downstream trigger survives a kill of the committing Visit API, a consumer outage and a Connect outage (exactly one execution, marker set, started by the outbox consumer, not the sweeper)"
  - "k6 smoke on the quiet stack after the booking path gained its outbox insert: all thresholds pass"
affects: [03-12, 03-17, 03-18, 03-20]
tech-stack:
  added: []
  patterns:
    - "A `_restored(service)` context manager: whatever the body kills or stops is started again in finally, then waited healthy (Connect: every connector task RUNNING)"
    - "Sweeper-vs-outbox attribution: the test reads the visit-consumer's own `visit.downstream_started outcome=started` log line for the visit id, so a pass proves the outbox path did the work"
key-files:
  created: []
  modified:
    - tests/e2e/test_phase3_booking_events.py
    - tests/e2e/test_phase3_visit_downstream.py
key-decisions:
  - "The visit tests prove the outbox path by log evidence (the consumer logged outcome=started for that visit id), not only by 'one execution plus marker', because the Phase 2 sweeper (grace 30 s, interval 60 s) could otherwise reach the same end state"
  - "No gateway recreate and no `test_gateway_default_rate_budget_restored` test (see Deviations): the live gateway already runs with the user's raised budget and the orchestrator said to leave it there"
requirements-completed: []
status: complete
duration: about 1h wall time (tests 8 min, smoke 1 min, rest authoring and waiting)
completed: 2026-11-11
actuals:
  tokens: 7500
  tasks: 3
  commits: 2
plan_head_before: efb03e809a3e0d77dbe029b3149a26966e51882a
plan_head_after: 2c24c36f240599ee3e4e2bb1c99ae1947f63d252
---

# Phase 3 Plan 11: Commit-Kill Proofs Summary

**On a quiet stack, killing the process that committed a state change never loses its event: the cancelled event arrives after a SIGKILL of scheduling-worker, a booked event committed while Connect is down arrives (1 copy) after Connect restarts, and the visit downstream trigger (the D-03 gate for retiring the sweeper) starts exactly one `visit-completed-{id}` execution after a kill of the Visit API, a consumer outage and a Connect outage. The k6 smoke passes every threshold on the changed booking path.**

`requirements-completed` is intentionally empty: the orchestrator said not to edit REQUIREMENTS.md or tick requirements (EVENT-02, EVENT-03 stay for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 76369d6 | `test_cancel_event_survives_worker_kill_after_commit`, `test_booked_event_delivered_after_connect_restart` |
| 2 | 2c24c36 | `test_visit_trigger_survives_api_kill_after_commit`, `test_visit_trigger_catches_up_after_consumer_outage`, `test_visit_trigger_survives_connect_outage` |
| 3 | none | k6 smoke run by hand (no file changed; see Deviations) |

## Results

### Task 1: appointment events (2 passed, 75 s)

- **Worker kill:** the cancel was sent through the gateway, `scheduling-worker` was `docker compose kill`ed the moment the appointment read `cancelled`, and `appointment.cancelled` with id `uuid5(appointment:appointment.cancelled:2)` (the only distinct id for the key) arrived within the 60 s window. The worker was started again and healthy.
- **Connect outage:** with `connect` stopped the booking confirmed, zero booked messages for the key during a 10 s quiet read, then after `connect` was healthy and all connector tasks RUNNING the booked id `b834afba-30ef-593c-b3f3-c51e84d6673e` arrived. **Copies on the wire after the Connect restart: 1** (a graceful stop commits the connector's offsets, so no redelivery happened; the test records the count and does not assert it is one, and a SIGKILL of Connect, which would likely produce duplicates, was not part of the plan and not run).

### Task 2: the D-03 gate (3 new tests, 4 passed in the file, 117 s)

| Scenario | Outcome |
|---|---|
| Visit API SIGKILLed right after the completion 200 | marker set and one execution of `visit-completed-{id}` within the 15 s deadline; consumer logged `visit.downstream_started outcome=started`; billing finalized, appointment completed; `visit` restarted healthy and the visit reads `completed` |
| `visit-consumer` stopped for the completion | for 15 s: marker NULL, 0 executions; after the consumer was started: marker set, exactly one execution, consumer logged `outcome=started` |
| `connect` stopped for the completion | for 15 s: marker NULL, 0 executions; after Connect healthy and every connector task RUNNING: marker set, exactly one execution, consumer logged `outcome=started` |

Executions were counted with `client.list_workflows("WorkflowId = 'visit-completed-{id}'")` (after a 5 s settle). In every scenario the visit-consumer's own log line for that visit id says the consumer, not the Phase 2 sweeper, started the workflow, which is the evidence that the outbox path recovered each case. **The D-03 gate passed**, so plan 03-12 may retire the sweeper. `tests/e2e/test_phase2_flows.py`: 5 passed after the kills.

### Task 3: k6 smoke (exit 0, every threshold passed)

Run with `loadtest/run.sh smoke` on a world seeded for it (`--providers 2 --patients 250 --days 2`, 541-entry plan), host load average about 3 (the laptop was not idle), gateway at 100000/min default and 1000/min auth budget.

| Metric | Phase 3 (2026-11-11) | Baseline (2026-11-10, quiet host) | Threshold |
|---|---|---|---|
| booking accept p95 | 29.3 ms | 21.8 ms | 300 ms |
| booking accept p99 | 170.0 ms | 78.9 ms | 800 ms |
| saga completion p95 | 0.519 s | 0.52 s | 5 s |
| saga completion p99 | 0.521 s | n/a recorded | 15 s |
| read appointment p95 / p99 | 9.7 / 11.8 ms | n/a | 200 / 500 ms |
| read slots p95 / p99 | 15.3 / 16.8 ms | n/a | 200 / 500 ms |
| errors on book | 0 of 151 | 0 | under 1 % |
| dropped iterations | 0 | 0 | 0 |
| confirm timeouts | 0 | 0 | 0 |

Honest reading: accept latency is higher than the baseline (p95 +7.5 ms, p99 about +91 ms, the p99 driven by one 543 ms max request in 151), measured with a busier host than the baseline's quiet one; the outbox insert is at the confirm step in the worker, not on the 202 accept path, so I attribute the gap to the first-request and host-load noise rather than to the outbox, but I did not isolate it (one run, no A/B). Saga completion time is unchanged. No threshold was crossed and none was loosened. Sustained, burst and contention were not run (D-04, deferred by the user).

## Deviations from Plan

1. **[Orchestrator instruction over plan] Smoke through `scripts/loadtest_seed.py` and `loadtest/run.sh smoke`, not `tests/e2e/test_loadtest_smoke.py`.** The wrapper test inserts a fresh admin with a hard-coded password into identity_db; the orchestrator authorized instead a new random password on the existing `loadtest-admin@example.com` account only. The password was generated with `secrets.token_urlsafe(24)`, hashed with `hash_password`, written with one UPDATE of that single row (exactly one row updated, role admin), kept in a 0600 file under the scratchpad, passed through `LOADTEST_ADMIN_*` environment variables, never printed. The password file and `loadtest/data/plan.json` were deleted afterwards; no other account was touched. (My first attempt updated the row, then failed my own output check before saving the file; the password was replaced by a second update of the same row, so no password was ever printed or lost track of.)
2. **[Orchestrator instruction over plan] The gateway was not recreated and `test_gateway_default_rate_budget_restored` was not added.** The live gateway already runs with `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` and `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000` (the user's standing setting), and the orchestrator said to leave it at those values. The plan's test asserts the budget is not 100000, which would contradict that instruction and fail. The Compose defaults (300 and 20) remain in `infra/docker-compose.yml`. Consequence: the plan's "gateway back on its default budget" criterion is intentionally not met; the plan's `files_modified` for Task 3 therefore has no change.
3. **[Rule 2, test strength] Outbox-path attribution by log evidence.** The plan only asked for one execution and the marker. Because the sweeper could produce the same end state, each visit scenario also asserts the consumer's `visit.downstream_started outcome=started` log line for that visit id.
4. **[Plan wording] No `docker compose ps ... --format '{{.Service}} {{.Health}}'` before the plan's gateway step** since no gateway step ran; health was verified at the end for all touched services.

## Live stack

- Killed or stopped (each restored in `finally`): `scheduling-worker` (kill), `connect` (stop, twice), `visit` (kill), `visit-consumer` (stop). Never run: `down -v`, any volume operation, any image rebuild. `gateway` untouched.
- End state: `scripts/compose_healthcheck.sh` OK; `connect`, `visit`, `visit-consumer`, `scheduling-worker` healthy; all four replication slots (`dbz_scheduling`, `dbz_patient`, `dbz_provider`, `dbz_visit`) active; `tests/e2e/test_phase3_outbox_cdc.py -k connector_tasks`: 4 passed (every connector and task RUNNING).
- `tjo-*` before and after: `tjo-api Up 26 hours (unhealthy)`, `tjo-postgres Up 26 hours (healthy)`; no restart (`tjo-api` was already unhealthy).
- Plan verification run: `test_phase3_booking_events.py` + `test_phase3_visit_downstream.py` + `test_phase2_flows.py`: 14 passed in 273 s (the pytest `-m e2e` run is against the live stack, started after the smoke).
- Live data left behind: throwaway clinics, appointments, visits and their outbox rows and relayed Kafka copies from the six new runs (outbox is insert-only by design); a smoke world of 2 providers, 250 patients (plus 200 contention patients), 768 slots and 151 smoke appointments under one clinic. `smarthealth.dlq.visit-downstream` still holds the earlier harness records; the new tests never count it.
- No `.env*` or `infra/secrets/*` content was read or printed.

## Known Stubs

None.

## Threat Flags

None. T-3-60 (degraded stack): every kill or stop restored in `finally` with health and connector-task waits, end state verified above. T-3-61 (unproven claims): kill points are observable commit points (cancel read back as `cancelled`, completion 200), executions counted via `list_workflows`, wire copies counted (1), outage windows 15 s with outbox-path log evidence. T-3-SC: no package installed.

## Self-Check: PASSED

- FOUND: tests/e2e/test_phase3_booking_events.py and tests/e2e/test_phase3_visit_downstream.py (committed, changes present).
- FOUND commits 76369d6 and 2c24c36 on gsd/phase-02-scheduling-service-workflow (`git rev-list --count efb03e8..HEAD` = 2 before the docs commit).
- FOUND absent: `loadtest/data/plan.json` and the scratchpad password file (deleted).
