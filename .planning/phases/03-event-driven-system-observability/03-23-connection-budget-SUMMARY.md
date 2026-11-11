---
phase: 03-event-driven-system-observability
plan: 23
subsystem: platform
tags: [postgres, connection-pool, sqlalchemy, asyncpg, compose, budget, gap-closure, wr-05]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-20 measured connection ceiling (304 of 300 at the gate), 03-21 consumer reliability (a 53300 no longer loses an event)"
provides:
  - "libs/db/postgres.py registry keyed by (database URL, loop); closed-loop entries retired at every lookup with their idle connections closed (WR-05)"
  - "SharedSettings.db_pool_size / db_max_overflow (DB_POOL_SIZE / DB_MAX_OVERFLOW, code defaults 5 and 10)"
  - "Compose connection budget: x-app-env 5+5, visit-consumer and analytics-consumer 2+2, budget comment (241 of 297)"
  - "test_compose_connection_budget: every term derived from Compose, Dockerfiles, connector files and Grafana provisioning"
affects: [03-24, 03-25, 03-26]
tech-stack:
  added: []
  patterns:
    - "retire a closed event loop's asyncpg connections by shutting down each idle connection's socket through a dup of its descriptor (terminate() raises on a closed loop)"
    - "a static budget test that parses Compose with yaml.safe_load (merge keys resolved) and compares its derived total with the number written in the Compose comment"
key-files:
  created:
    - tests/libs/test_postgres_registry.py
  modified:
    - libs/db/postgres.py
    - libs/config.py
    - infra/docker-compose.yml
key-decisions:
  - "Retire = drain the idle QueuePool records and shut down each raw connection's socket (dup of the fd), then sync_engine.dispose(close=False); private attribute access only inside _retire, every step best-effort"
  - "Temporal stays an observed constant (40): the config template caps are 20 (default store, SQL_MAX_CONNS) + 10 (visibility store, SQL_VIS_MAX_CONNS) per pool, Compose sets neither, so the figure is not derivable from the repository"
  - "Debezium term stays 2 per connector (client backends); the 4 walsenders also seen in pg_stat_activity are not counted against max_connections on PostgreSQL 15"
patterns-established:
  - "Pool bounds are settings; the per-container budget lives in Compose and is enforced by a test"
requirements-completed: [EVENT-05, OBS-01]
status: complete
commits: 2
plan_head_before: f82829ee0339c7117ac843a74ce3daa1aec8ebbb
plan_head_after: 73d1b99cc631324cf353d95e04d95988f9316b4f
actuals:
  tokens: 5600
  tasks: 2
  commits: 2
duration: about 1h50 (about 55 min of it waiting for clinic-local midnight so the visit tracer could run)
completed: 2026-11-12
---

# Phase 3 Plan 23: Connection Budget Summary

The engine registry no longer orphans a closed loop's engine (WR-05), pool size and overflow are settings, the Compose stack's derived worst case is 241 of 297 connections, and a full `tests/visit tests/workflows` run peaked at 108 of 300 connections (the gate reached 304).

## What was built

**Task 1 (tracer), commit 2de80cd.** `libs/db/postgres.py`: the registry is keyed by `(database URL, id(loop))` and the entry's loop is compared by identity. At the top of every `_entry_for` call, entries whose loop `is_closed()` are removed and passed to `_retire`: the idle (checked-in) records of the engine's pool are drained, the socket under each raw asyncpg connection is shut down (through a `dup` of its descriptor, so no private socket attribute is touched; `terminate()` raises on a closed loop), then `sync_engine.dispose(close=False)`. Entries of live loops are never touched from another loop. The private attributes (`dbapi_connection._connection`, asyncpg `Connection._transport`, the pool's `_pool`) are used only inside `_retire`, every step under suppression, verified against SQLAlchemy 2.0.52 and asyncpg 0.31.0 and named in the docstring.

- Red run on the old code (before the change): `test_sequential_loops_leave_at_most_one_backend_alive` failed with `4 of 8 backends of closed loops survive: [9508, 9509, 9512, 9515]`; the alternation test failed with 6 engines created for one URL (limit 2); the closed-loop retirement test failed. The fourth test (same engine for one long-lived loop) passed, as intended (production behaviour unchanged).
- Green run after the change: `pytest tests/libs/test_postgres_registry.py tests/libs/test_observability.py -x -q` 11 passed; `pytest tests/libs -q` 167 passed (172 after Task 2).

**Task 2 (auto), commit 73d1b99.**
- `SharedSettings.db_pool_size` (5) and `db_max_overflow` (10), passed to `create_async_engine` in `_entry_for`.
- Compose: `DB_POOL_SIZE: "5"` / `DB_MAX_OVERFLOW: "5"` in `x-app-env` with the budget arithmetic comment; `visit-consumer` and `analytics-consumer` now merge their env alias and add `"2"` / `"2"`. No gunicorn worker variable and no Prometheus multiprocess variable anywhere in Compose (`grep -c "PROMETHEUS_MULTIPROC_DIR\|WEB_CONCURRENCY"` prints 0; `grep -c DB_MAX_OVERFLOW` prints 4).
- Tests added to `tests/libs/test_postgres_registry.py`: pool settings reach the engine (size 2, overflow 1), code defaults and env override, a derivation-helper unit test (Dockerfile `-w ${X:-3}`, Compose override in dict and list form, missing/templated pool value fails), and `test_compose_connection_budget`.

### Budget breakdown printed by `test_compose_connection_budget -s`

| Term | Connections | Derivation |
|---|---|---|
| 8 APIs (analytics, billing, identity, organization, patient, provider, scheduling, visit) | 8 x 20 = 160 | 2 gunicorn workers (Dockerfile CMD default) x pool 10 |
| scheduling-worker, visit-worker | 10 + 10 | 1 process x pool 10 |
| analytics-consumer, visit-consumer | 4 + 4 | 1 process x pool 4 |
| Debezium | 8 | 4 connector files x 2 (assumed, not measured) |
| Temporal | 40 | observed fallback constant (Compose sets no SQL_MAX_CONNS) |
| Grafana | 5 | 1 Postgres datasource x maxOpenConns 5 |
| **Total** | **241 of 297** | max_connections 300 (postgres command) minus 3 reserved |

The Compose comment states `Worst-case connection total: 241 of 297`; the test fails when it differs from the derived figure.

### Temporal template lines (read-only, `grep -n -i maxconns /etc/temporal/config/config_template.yaml`)

```
29:  maxConns: {{ default .Env.CASSANDRA_MAX_CONNS "20" }}
61:  maxConns: {{ default .Env.SQL_MAX_CONNS "20" }}
91:  maxConns: {{ default .Env.SQL_VIS_MAX_CONNS "10" }}
110: maxConns: {{ default .Env.SQL_MAX_CONNS "20" }}
136: maxConns: {{ default .Env.SQL_VIS_MAX_CONNS "10" }}
```

Lines 110 and 136 are the postgres12 block: default store 20, visibility store 10 (not 20 per store as the plan assumed). See Deviation 1.

## Live results

- Recreated only `scheduling-worker`, `visit-worker`, `visit-consumer`, `analytics-consumer` (`up -d --build --no-deps`); all four healthy; printed settings: visit-consumer `2 2`, analytics-consumer `2 2`, scheduling-worker `5 5`, visit-worker `5 5`.
- The nine APIs were NOT recreated: plans 03-24 (gateway, scheduling) and 03-25 (the other seven) rebuild them and pick up this plan's code and `x-app-env` values then. Until then the running APIs keep the old code and SQLAlchemy defaults (5+10).
- Gateway left as is (printed `1000` and `100000`; plan 03-26 restores 20/300). Postgres not restarted, no volume touched, `tjo-*` status identical apart from uptime (`tjo-api Up (unhealthy)`, `tjo-postgres Up (healthy)`, both before and after; tjo-api was already unhealthy).
- e2e tracers (`tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py -m e2e -k tracer`, `SMARTHEALTH_E2E_REQUIRE_STACK=1`): first run at 23:11 local gave 1 passed, 1 skipped (the visit downstream tracer skips between 23:2x and midnight clinic-local: "the next half-hour slot would reach clinic-local midnight", environmental and time-of-day, unrelated to this plan). Rerun at 00:00 local: **2 passed, 0 skipped**.
- Measured suite: `tests/visit tests/workflows` (visit-consumer stopped) under a 15 s `pg_stat_activity` sampler, run by one driver script that truncates the sample file, records the run window (`gap-03-23-run-window.txt`: start 2026-11-11T18:12:02Z, end 18:19:41Z), starts the sampler, runs pytest, stops it, restarts the consumer with `start --wait` and then checks the samples against the recorded window. The check asserts at least 12 samples, all timestamps inside the window, no gap above 60 s (sleep detection) and peak below 270. Driver and checker are in the executor scratchpad (`measure.sh`, `sample_once.sh`, `check_samples.py`); evidence in `closeout-logs/gap-03-23-pg-samples.txt` (untracked).
  - pytest: **405 passed, 11 skipped, 0 failed** in 456 s (skip reasons not investigated; the marks are in the existing tests).
  - **32 samples**, max gap 16 s (no sleep gap), all inside the window.
  - Idle baseline (first sample): **100** total. **Peak total: 108** (the gate's 304; the ceiling for this plan was 270): **PASSED**, no ceiling adjustment.
  - Per-role peak: smarthealth (superuser: Temporal's two databases plus the sampler) 32, scheduling_svc 15, debezium 12, identity_svc 12, patient_svc 11, organization_svc 11, billing_svc 6, visit_svc 4, provider_svc 4, analytics_svc 3. The sampler's total includes the 4 Debezium walsenders (not counted against max_connections on PostgreSQL 15), so it is slightly conservative.
- Additional suites for the shared lib (the plan did not require them): `tests/scheduling tests/patient tests/organization tests/billing tests/provider` (known baseline failure `test_admin_registers_provider_through_gateway` deselected): **545 passed**, 684 s. `tests/libs`: 172 passed (includes `test_service_isolation.py`).
- Final state: `scripts/compose_healthcheck.sh` OK; replication slots `dbz_patient`, `dbz_provider`, `dbz_scheduling`, `dbz_visit` all active; `visit-downstream-sweeper` Schedule still absent (Temporal schedule list shows the scheduling and visit-reconciler schedules only); visit-consumer started again and healthy.
- Semgrep (`p/python`, `p/security-audit`, `--metrics=off`) on `libs/db/postgres.py`, `libs/config.py`, `tests/libs/test_postgres_registry.py` after committing: 0 findings in both configs.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Incorrect plan premise] Temporal template defaults are 20 (default store) + 10 (visibility store), not 20 per store**
- **Found during:** Task 2, reading the Temporal config template as the plan instructed.
- **Issue:** the plan's constant "40 = 2 x SQL_MAX_CONNS default 20". The template says default store `SQL_MAX_CONNS` 20 and visibility store `SQL_VIS_MAX_CONNS` 10, per pool.
- **Fix:** when Compose sets `SQL_MAX_CONNS` the test uses `SQL_MAX_CONNS + SQL_VIS_MAX_CONNS` (visibility default 10) instead of `2 x SQL_MAX_CONNS`. The fallback constant stays 40 (derived-total stays 241) because the observed idle total was 40 at the gate and 29 in this plan, which already exceeds one 20 + 10 pool pair; the comment names the template caps and says the figure is observed, not derivable. A true upper bound for Temporal (several services each with their own pools) is not established by this plan; the measured suite peak of 108 total includes Temporal's load during the run.
- **Files modified:** tests/libs/test_postgres_registry.py
- **Commit:** 73d1b99

**2. [Rule 3 - Blocking] Tracer skipped near clinic-local midnight**
- The visit downstream tracer skipped at 23:11 local (environmental, by design of the harness). I waited for midnight and reran; both tracers passed. Recorded under live results.

## Known Stubs

None.

## Threat Flags

None. No new network endpoint, auth path or schema; the retire function only closes connections of loops that are already closed (T-3-75 covered by the live-loop alternation and retirement tests).

## Deferred / Notes for later plans

- The APIs keep the old registry code and default pool (5+10) until 03-24/03-25 rebuild them; the derived budget (241 of 297) assumes the new `x-app-env` values are in place everywhere after those rebuilds.
- Temporal's connection count is observed, not bounded: setting `SQL_MAX_CONNS` / `SQL_VIS_MAX_CONNS` on the `temporal` service would make it derivable (not done: recreating Temporal was outside this plan's allowed recreations).

## Self-Check: PASSED

- FOUND: libs/db/postgres.py, libs/config.py, infra/docker-compose.yml, tests/libs/test_postgres_registry.py
- FOUND commits 2de80cd (Task 1) and 73d1b99 (Task 2); `git rev-list --count f82829e..HEAD` = 2 at SUMMARY time (before the docs commit)
