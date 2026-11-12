---
phase: 03-event-driven-system-observability
plan: 26
subsystem: docs-and-gate
tags: [docs, dashboard, final-gate, semgrep, gap-closure, follow-ups]
requires:
  - phase: 03-event-driven-system-observability
    provides: "plans 03-21..03-25 (consumer reliability, trigger recovery, connection budget, multiprocess metrics)"
provides:
  - "docs/architecture.md 4f, 10.2, 10.4, 10.7, 10.8, 10.9 state what the code does about outages, dead letters, recovery, multiprocess metrics and the connection budget"
  - "guard tests that fail if the false outage claim returns to the docs or the dashboard"
  - "the final gate on the finished stack: full non-e2e and full e2e clean, connection peak 105"
  - "gateway back on its default budget (20 and 300)"
affects: []
status: complete
requirements-completed: []
actuals:
  tokens: 6500
  tasks: 2
  commits: 2
plan_head_before: 4a889713dc369ab2a57d47f37c12f02f53d8a403
plan_head_after: a2ec6c6
---

# Phase 3 Plan 26: Gap Closeout Summary

Status `complete` (human sign-off recorded 2026-11-12). Tasks 1 and 2 (automated) and Task 3 (the user's re-check of exactly the three changed dashboard sentences) are done; the phase is NOT marked complete by this plan, REQUIREMENTS.md was not edited (`git diff HEAD -- .planning/REQUIREMENTS.md` is empty) and no requirement was ticked.

`plan_head_after` is the last task commit; the commit that adds this summary and STATE.md follows it. Two task commits measured: febde67 (Task 1) and a2ec6c6 (the gate's connection peak recorded in 10.9).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | febde67 | guard tests (red first), docs 4f / 10.2 / 10.4 / 10.7 / 10.8 / 10.9 corrected, three dashboard sentences, README `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` rows |
| 2 | a2ec6c6 | the final gate's measured connection peak recorded next to the 304 of the first gate (10.9) |

## Task 1: guard tests, then the corrections

Red run before any document edit (`pytest tests/docs -q`): **10 failed, 162 passed**. The failures were the new guards doing their job: `test_stale_text_removed` (dashboard), `test_qa_outage_answer_states_the_cap_and_the_recovery_tools`, `test_documents_no_longer_make_claims_phase3_made_false[architecture.md]`, `test_every_document_is_free_of_the_claims_phase3_made_false[architecture.md]`, the five `test_the_phase3_architecture_text_names_the_gap_closure_mechanisms[...]` cases (`TransientExhausted`, `event_commit_failures_total`, `restart_lost_triggers`, `replay_dlq`, `MultiProcessCollector`) and `test_every_script_the_phase3_section_names_exists` (the docs did not yet name `scripts/replay_dlq.py`). `test_the_stale_outage_guard_bites` (the patterns reject the three old phrasings and not the unrelated analytics "absorbed, never dead-lettered" sentence) passed from the start.

Green run after the edits: `pytest tests/docs tests/loadtest -q` **271 passed**; `tests/docs` alone 172 passed (also at the end, after the gateway restore). No existing test was weakened. Acceptance: `grep -c TransientExhausted` prints 5 for `docs/architecture.md` and 2 for `docs/architecture-dashboard.html`; `git diff --numstat -- docs/architecture-dashboard.html` is `3 3` (three lines, one per passage); the version marker is unchanged.

What the docs now say (all by real names, nothing aspirational):

- **10.2:** one classifier (`libs/events/classify.py`, guarded by a test), SQLSTATE classes 08/53/57/40 (53300, 57P03, 40P01 as examples; 23505 and 42P01 never transient), SQLAlchemy connection errors and pool timeouts, Schema Registry 408/429/5xx, the seven Temporal statuses and the closed-client error, broker/Mongo/network errors; capped backoff; `TransientExhausted` after at least 10 attempts over at least 15 minutes, counted in `dlq_events_total`, original key/value/headers kept; commit retry with `event_commit_failures_total`; a sentence that the paragraph used to claim the opposite and that gap verification found it false (CR-01). Per-group recovery table (visit-downstream: reconciler lost-trigger pass after `VISIT_RECONCILE_GRACE_MINUTES`, backfill `stale_queued` / `restarted`, `scripts/replay_dlq.py`; analytics-projection: `scripts/replay_dlq.py` or `services.analytics.rebuild`; notification-trigger: `scripts/replay_dlq.py`), the replay script as it is (dry run default, `--apply`, catalogue-topic check, filters, never moves offsets, each group absorbs the duplicate), and the **head-of-line effect** of the cap (each message gets its own 900 s cap, so a long outage dead-letters a run of healthy events one per 15 minutes per partition; replay is a required operational step).
- **4f / 10.4 / 10.8:** the visit reconciler's lost-trigger pass (`list_lost_triggers`, `restart_lost_triggers`, `ensure_downstream_started`, worker start and the 60 s Schedule; the sweeper stays retired); the live lost-trigger test; the backfill's counts line and `--stale-after-minutes`; WR-06 stays open.
- **10.7:** the nine APIs run two gunicorn workers in Prometheus multiprocess mode (`MultiProcessCollector`, directory emptied at start, `child_exit` marks dead workers, no default process collectors on an API scrape, directory variable exported only by the API image command), all nine rebuilt (03-24, 03-25); red before / green after evidence with zero resets; accept-path overhead not isolated (no A/B run). The "only scheduling was rebuilt" sentence is gone.
- **10.9:** connection budget (pool settings, 241 of 297 derived by `test_compose_connection_budget`, Debezium's 2 per connector assumed and Temporal's 40 observed, not derived), measured peaks (304 before; 105 in this plan's final gate over the full suites; 108 for the `tests/visit` + `tests/workflows` subset in 03-23; 91 during the smoke); not done: raise dev `max_connections`, cap Jaeger memory; the Kafka OOM-kill during image rebuilds on the 7.65 GiB Docker VM with no Kafka memory limit; `scripts/compose_healthcheck.sh` printed OK while Kafka was down because it checks only running services (follow-up; the script was not changed); the load item with the four smoke runs (21.8/78.9, 29.3/170, 49.1/200.8, 41.85/245.5 ms accept p95/p99; saga about 0.5 s; all thresholds passed; NFR-01..08 still unmeasured; sustained/burst/contention deferred); the proof level (I6: unit-level `_handle_message` with real asyncpg error classes plus the live lost-trigger e2e, no live 53300 injection; registry 408/429/5xx unit-tested, not live); the open follow-ups.
- **README:** `DB_POOL_SIZE` (default 5) and `DB_MAX_OVERFLOW` (default 10) rows with the Compose values.

### Dashboard sentences changed (D-14): before and after, word for word

Only the three passages below changed (3 lines in the file; the version marker and everything else are untouched). The visible text of each is quoted; the one markup difference is that `TransientExhausted` is wrapped in `<code>` in the passages that name it, as the page already does for identifiers.

**(1) "Three consumer groups" node (line about 504), last clause**

- Before: `... poison messages go to the group's dead-letter topic; an infrastructure outage is retried and never dead-lettered.`
- After: `... poison messages go to the group's dead-letter topic; an outage is retried with capped backoff and, after the cap, dead-lettered as TransientExhausted for recovery.`

**(2) "Visit sweeper retired: visit.completed is the trigger" card, Tradeoff (line about 680), one sentence appended**

- Before (ends): `... Visits completed before Phase 3 are covered by a backfill script; its dry run against the live database found 0 candidates, so it was never applied.`
- After (ends): `... Visits completed before Phase 3 are covered by a backfill script; its dry run against the live database found 0 candidates, so it was never applied. A dead-lettered or dropped visit.completed is restarted by the visit reconciler after its grace.`

**(3) Q&A "What happens when Kafka Connect, Schema Registry, RabbitMQ or Temporal is down? (outage behaviour)" (line about 1406), the sentence in the first paragraph**

- Before: `A consumer's dependency down (Schema Registry, Postgres, Temporal, RabbitMQ): the offset is held, the consumer seeks back and retries with capped backoff, and the message is never dead-lettered, so an outage delays an event and never drops it.`
- After: `A consumer's dependency down (Schema Registry, Postgres, Temporal, RabbitMQ): the offset is held and retried with capped backoff for up to the cap (10 attempts and 15 minutes), after which the message is dead-lettered as TransientExhausted and recovered by the visit reconciler, the analytics rebuild or the DLQ replay script.`

The word "outage" is kept in that answer. **Not changed, for the user to decide (the three-sentence limit):** the next paragraph's sentence `Not verified: the consumer's decode-time registry-outage path has no automated test, a Kafka broker outage on its own was not exercised, and the path after a slot exceeds its cap was not exercised.` now understates the coverage (plan 03-21 added unit tests for the decode path, including the 408/429/5xx classification; there is still no live registry outage). Reply "leave it" or "update it". It is recorded as a STATE.md follow-up unless the user asks for the update.

## Task 2: the final gate on the finished stack

### Before the gate

- Precondition: `tail -n 1 closeout-logs/gap-smoke.txt` printed `k6_exit=0`.
- `docker compose -f infra/docker-compose.yml ps -a` showed every long-running service `running` (healthy where a healthcheck exists: kafka, connect, jaeger, grafana, prometheus, temporal-ui and all APIs, workers, consumers); the only `exited` containers were the five one-shot init jobs (`connect-init`, `db-init`, `kafka-init`, `rabbitmq-init`, `schema-init`, all `Exited (0)`). Nothing needed starting; no container was recreated. Kafka had been up 12 minutes (started by plan 03-25 after its OOM kill), `OOMKilled=false`, restarts 0.
- `scripts/compose_healthcheck.sh`: OK. Four replication slots `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` active. Four connectors (`patient-outbox`, `provider-outbox`, `scheduling-outbox`, `visit-outbox`) RUNNING with one RUNNING task each. Gateway found at 1000 / 100000 (raised, as left by 03-25), so no re-apply was needed. No images were rebuilt and nothing heavy ran beside the gate (Semgrep ran only after it finished).
- `tjo-*` before: `tjo-api Up 37 hours (unhealthy)`, `tjo-migrate Exited (0) 5 days ago`, `tjo-postgres Up 37 hours (healthy)`.

### The runs (one driver script, one sampler, run sequentially; stdout and stderr in separate files)

Driver: stopped `visit-consumer`; full non-e2e; `start --wait --wait-timeout 180 visit-consumer` (exit 0); then the full e2e. During both runs it wrote a 20 s wall-clock heartbeat, a 15 s `pg_stat_activity` sampler (superuser over the container socket, one line per sample) and a `docker events` log (oom/die/kill). Run window 2026-11-11T20:03:28Z to 20:52:05Z.

| Run | Command | Result | Duration |
|---|---|---|---|
| non-e2e | `pytest tests -m "not e2e" -q -rfEs --deselect tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` | **2049 passed, 11 skipped, 98 deselected, 0 failed, 0 errors**, 128 warnings | 1618.63 s (26:58) |
| e2e | `SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e -m e2e -q -rA` | **97 passed, 5 deselected, 0 failed, 0 errors, 0 skipped**, 1 warning | 1280.55 s (21:20) |

Logs (untracked, never committed, may hold dev-default passwords in tracebacks): `.planning/phases/03-event-driven-system-observability/closeout-logs/gap-non-e2e.txt` / `gap-non-e2e.stderr.txt`, `gap-e2e.txt` / `gap-e2e.stderr.txt`, `gap-pg-samples.txt`, `gap-semgrep-auto.txt`, `gap-semgrep-python.txt`, `gap-semgrep-security-audit.txt`. The last stdout line of each run is pytest's summary line (quoted above). The first attempt of each run was clean: no rerun, no `-attemptN` logs.

Validity of the runs: no laptop sleep (heartbeat max gap 21 s against the 20 s interval; pg-sample max gap 16 s), no `E` marks, no `oom` event in the `docker events` log. The only `die`/`kill` events are the tests' own deliberate kills (Connect SIGKILLed 8 times, visit-consumer / analytics-consumer / notification-consumer / scheduling-worker / visit SIGKILLed by the D-03 and durability tests, exit 137) and the stop and start of `visit-consumer` by the driver, plus one-shot init jobs exiting 0 when tests re-run them. After the gate: no `OOMKilled=true` container, every long-running service `running` and healthy, slots active, connectors RUNNING.

Deselected by node id (and the only one): `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway`, the known baseline failure from plan 03-01 (deferred-items.md: leftover provider specialty rows and no ORDER BY, the POST and the GET list specialties in different order). Kept as a STATE.md follow-up. The "98 deselected" of the non-e2e run is the 97 e2e-marked tests plus this one; the "5 deselected" of the e2e run are the non-e2e-marked tests in `tests/e2e`.

Skips by reason (non-e2e run, `-rs`): all 11 are `tests/workflows/test_replay*.py` cases "re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1" (test_replay.py x4, test_replay_booking_patches, test_replay_reconciler, test_replay_reminder x2, test_replay_schedule_change, test_replay_visit_billing, test_replay_visit_completion). They are the deliberate opt-in recorders, identical to the 11 skips of earlier runs. No test skipped for the clinic-local-midnight reason: the e2e ran from 01:30 to 01:52 Asia/Karachi, outside the helper's 40-minute refusal window, and the e2e run has 0 skips (`grep -c "^SKIPPED"` = 0).

Known-flaky items: `tests/identity/test_email_migration.py` passed in this full run; no SIGTRAP (exit -5) crash occurred; no `TooManyConnectionsError`.

Failures to classify: none (0 failed, 0 errors in both runs).

### Connection peak (bound to the run)

`gap-pg-samples.txt`: **192 samples** every 15 s from 20:03:28Z to 20:51:58Z (the same window as the two pytest runs), **peak total 105**, minimum 84, maximum gap between samples 16 s. Gate ceiling 270, first gate's peak 304, 03-23's subset peak 108. The total counts every backend with a user name (so it includes Debezium's walsenders, which PostgreSQL 15 does not count against `max_connections`, and the sampler's own connection), so it is slightly conservative.

### Required items from the verify

- `test_lost_visit_trigger_is_recovered_by_the_reconciler` PASSED in `gap-e2e.txt` (count 1).
- Nine `test_http_request_counters_have_no_resets_and_never_decrease[...]` cases PASSED: analytics, billing, gateway, identity, organization, patient, provider, scheduling, visit.
- `test_sweeper_schedule_retired_on_live_server` appears in the e2e log (the retired `visit-downstream-sweeper` Schedule is still absent).
- The plan's whole verify command, run after the gateway restore, exited 0 (9 no-reset cases, 192 samples with peak 105, gateway 20 and 300, `tests/docs` 172 passed).

### Gap checklist with evidence

| Gap item | Evidence |
|---|---|
| CR-01 classification | 03-21: unit tests at the real `_handle_message` with real asyncpg error classes (53300, 57P03, 40P01 raw and wrapped), the cap with a fake clock and on a live broker, `tests/events` 213 passed; proof level (I6) stated in docs 10.9; no live 53300 injection. In this gate: `tests/events` and `tests/analytics` inside the 2049 passed |
| CR-01 recovery | `test_lost_visit_trigger_is_recovered_by_the_reconciler` PASSED in the e2e log; the replay script and backfill tests of 03-22 inside the non-e2e pass |
| WR-02, WR-03, WR-07, IN-01 | 03-21 tests (classification in analytics, the TransientExhausted cap, the commit retry, the AST single-classifier guard with positive controls): all inside the 2049 passed |
| WR-05 | 03-23 registry tests and `test_compose_connection_budget` pass; this run's peak 105 (304 before) |
| CR-02 | nine no-reset cases PASSED in the e2e log; 03-25's smoke resets file: 0 for each of the nine jobs |
| Smoke | 03-25 Task 3: `k6_exit=0`, 0 of 252 requests failed, accept p95 41.85 ms / p99 245.49 ms, saga about 0.5 s; four-run comparison in docs 10.9 and `docs/nfr.md` |
| Docs | Task 1 (this plan) |

### Semgrep status (required for BE changes)

`/Library/Frameworks/Python.framework/Versions/3.14/bin/semgrep` (not in `.venv`), after all code was committed:

- `semgrep scan --config auto --error services libs scripts infra` (metrics ON, as in 03-20): 24 findings, exit 1 (findings present). Identical to the 03-20 triage; **no new finding**:

| Findings | Location | Class |
|---|---|---|
| 10 x `dockerfile.security.missing-user` | gateway, billing, identity, organization, patient, provider, scheduling, visit, analytics, notification Dockerfiles | ACCEPTED by the user (eight at the Phase 2 closeout, two at the 03-20 gate); production hardening deferred |
| 6 (3 x `formatted-sql-query` + 3 x `sqlalchemy-execute-raw-query`) | `libs/events/migration.py` | false positive: Alembic DDL built from module constants (`PUBLICATION_NAME`, `DEBEZIUM_ROLE`); no request input reaches it |
| 2 | `services/analytics/migrations/versions/analytics_0001_projections.py` | false positive: `DROP VIEW IF EXISTS {view}` iterates the module constant `_VIEWS` |
| 1 `python37-compatibility-importlib2` | `libs/events/registry.py` | false positive: stdlib `importlib.resources`; the project requires Python 3.12 |
| 2 | `services/scheduling/migrations/versions/scheduling_0004_appointment_states.py` | Phase 2 false positive: only the internal constant `active_predicate` is interpolated |
| 1 `avoid-sqlalchemy-text` | `services/scheduling/repository/idempotency_repo.py` | Phase 2 false positive: only `KEY_TTL_HOURS`, an integer constant |
| 1 `jwt-python-exposed-data` | `services/identity/security/signing.py` | Phase 2 false positive: the kid header is public, the payload is the token's own claims |
| 1 `insecure-file-permissions` | `scripts/generate_service_secrets.py` | Phase 2 false positive: `0700` is the restrictive mode |

(The plan 03-21..03-25 code, including `libs/events/classify.py`, `libs/db/postgres.py`, `libs/observability/*`, `scripts/replay_dlq.py` and the visit reconciler, produced none of the 24.)

- `--config p/python --metrics=off` over `libs/events libs/db libs/observability libs/config.py services/visit services/analytics/consumer.py services/notification/triggers.py services/gateway/app.py scripts/backfill_visit_completed_events.py scripts/replay_dlq.py`: 151 rules, 57 files, **0 findings**. `--config p/security-audit` on the same paths: 80 rules, 70 files, **0 findings**.
- No code was changed for Semgrep.

### Last automated step: gateway restore

`GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=20 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=300 docker compose -f infra/docker-compose.yml up -d --no-deps gateway` (recreated, only the gateway), waited for `healthy`. `docker compose ... exec -T gateway env | grep GATEWAY_RATE_LIMIT` printed `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=20` and `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=300`; `printenv` printed `20` and `300`. Nothing ran against the gateway afterwards.

### End state

- `scripts/compose_healthcheck.sh`: OK.
- `tjo-*` after: `tjo-api Up 38 hours (unhealthy)` (pre-existing), `tjo-migrate Exited (0) 5 days ago`, `tjo-postgres Up 38 hours (healthy)`; identical to before apart from uptime.
- `docker compose -f infra/docker-compose.yml ps -a`: every long-running service `running` (healthy where a healthcheck exists): analytics, analytics-consumer, billing, connect, gateway, grafana, identity, jaeger, kafka, mongo, notification-celery, notification-consumer, notification-temporal-worker, organization, patient, postgres, prometheus, provider, rabbitmq, redis, scheduling, scheduling-worker, schema-registry, temporal, temporal-ui, visit, visit-consumer, visit-worker; the one-shot jobs `connect-init`, `db-init`, `kafka-init`, `rabbitmq-init`, `schema-init` are `Exited (0)` by design. Four replication slots active; four connectors RUNNING.
- `git diff HEAD -- .planning/REQUIREMENTS.md`: empty. No `down -v`, no volume removed, no image rebuilt, no package installed, no `.env*` or `infra/secrets/*` read.

## Follow-ups recorded (STATE.md and here; out of scope by D-14)

WR-01 (reminder reschedule-signal race; needs `workflow.patched` plus a replay test); WR-04 (debezium role privileges, unauthenticated Connect REST, FileConfigProvider allowed.paths; PHI-relevant, local-dev scope); WR-06 (backfill / replay sends follow-ups for old visits); IN-02..IN-06; `scripts/compose_healthcheck.sh` does not flag a stopped required container; no memory limit on Kafka in Compose (OOM-killed during seven concurrent image builds); Temporal's connection count (40) is observed, not derived (`SQL_MAX_CONNS` / `SQL_VIS_MAX_CONNS` not set); head-of-line effect of the transient cap (replay is a required step after a long outage); ten root-user Dockerfiles; k6 `sustained`, `burst`, `contention` after Phase 3 (accept-path overhead of multiprocess metrics not isolated by an A/B run); EVENT-04 wording decision (REQUIREMENTS.md untouched); dev `max_connections` not raised (less urgent after WR-05); Jaeger memory not capped; the SIGTRAP fork class; identity migration test on the live DB; provider specialty-ordering test (deselected by node id); the live `visit-downstream` DLQ holds 814 `NonRetryableEventError` records for deleted test visits (the replay script now skips poison by default and `--apply` needs a selector).

## Deviations from Plan

1. **[Minor] One driver script ran both suites with one sampler** instead of two separately launched runs, so the sample file, the run window and the docker-events log bind to the same run; the sequence and every command are the plan's.
2. **[Minor] The gateway was not recreated at the raised budget before the e2e:** it already ran 1000 / 100000 (left by 03-25), verified by `env` before the gate.
3. **[Minor] An extra docs commit (a2ec6c6)** records the gate's measured peak in 10.9 (not in the plan's file list sequence, same file).
4. **[Design] `TransientExhausted` is wrapped in `<code>` in two dashboard passages** (markup inside the changed sentences only).
5. **[Process] A stray `docker run --rm busybox true` was issued once by mistake** while checking state; it printed nothing and changed nothing on the stack (`2>/dev/null`; no container or volume of the project involved).

No auth gates, no fix attempts, no deferred test failures.

## Known Stubs

None.

## Threat Flags

None new. T-3-82 (gateway restore, verified by `printenv`), T-3-83 (guard tests, script-path check, user re-check pending), T-3-84 (logs untracked, only summary lines and test ids quoted) are mitigated as planned.

## Human sign-off

User reply (verbatim answers to the checkpoint questions, 2026-11-12): dashboard re-check = "Approved; update the fourth (Recommended)"; gap-review warnings = "Fix WR-01, WR-02, WR-07 now (Recommended)". The three changed sentences were approved. The fourth "Not verified" sentence was updated afterwards in quick task 261112-8r6 (commit 62fc0a3), which also fixed WR-01 (lost-trigger pass starved by the settle pass), WR-07 (age ceiling, kill switch, per-visit backoff) and WR-02 (`replay_dlq.py --apply` needs an explicit selector, poison skipped by default; live dry run on the visit DLQ: 814 NonRetryableEventError records skipped, 0 selected). Still open as recorded follow-ups: gap-review WR-03..WR-06 and IN-01..IN-06, the dashboard "Failure paths:" sentence that still repeats the old decode-time claim, and the first-review WR-01, WR-04 and WR-06. Code-review gates over the gap-closure code: Atlas security/compliance APPROVED (3 low notes); GSD review 0 critical, 7 warning, 6 info (03-REVIEW-GAP.md).

## Self-Check: PASSED (automated tasks)

- Commits febde67 and a2ec6c6 are in `git log`; `git rev-list --count 4a88971..a2ec6c6` = 2.
- `docs/architecture.md`, `docs/architecture-dashboard.html`, `README.md`, `tests/docs/test_phase3_docs.py`, `tests/docs/test_dashboard.py` modified; `.planning/STATE.md` follow-ups updated; the gate logs exist under `closeout-logs/` (untracked).
- The Task 2 verify command exits 0.
