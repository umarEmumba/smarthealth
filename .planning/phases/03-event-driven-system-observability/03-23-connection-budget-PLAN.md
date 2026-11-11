---
phase: 03-event-driven-system-observability
plan: 23
type: execute
wave: 16
depends_on: ["03-22"]
files_modified:
  - libs/db/postgres.py
  - libs/config.py
  - infra/docker-compose.yml
  - tests/libs/test_postgres_registry.py
autonomous: true
gap_closure: true
requirements: [EVENT-05, OBS-01]

estimate:
  tokens: 60000
  raw_tokens: 60000
  tasks: 2
  confidence: low

must_haves:
  truths:
    - "WR-05 (the trigger of CR-01): when the running event loop changes, the engine registry no longer orphans the previous loop's engine; an entry whose loop is closed is retired and its pooled server connections are released, so N sequential event loops in one process leave at most one of their backends alive instead of accumulating them"
    - "An entry whose loop is still alive (a consumer thread's loop next to the test's loop) is never torn down from another loop, and alternating between two live loops uses one engine per loop instead of rebuilding an engine on every switch"
    - "Pool size and overflow are settings (DB_POOL_SIZE, DB_MAX_OVERFLOW; code defaults 5 and 10, unchanged for host tests and scripts), and the Compose stack's worst-case connection budget fits under the dev Postgres max_connections=300 minus the 3 reserved slots: API workers 5+5, Temporal workers 5+5, the two single-flow consumers 2+2, plus Temporal, Debezium and Grafana"
    - "The four long-running non-API DB processes (scheduling-worker, visit-worker, visit-consumer, analytics-consumer) run with these settings and stay healthy; the nine APIs pick up the code and the x-app-env values when plan 03-24 rebuilds them"
    - "A background run of tests/visit and tests/workflows against the live stack peaks far below the 304 connections measured at the phase gate (peak recorded in the SUMMARY)"
  artifacts:
    - path: libs/db/postgres.py
      provides: "registry keyed by database URL and loop; closed-loop entries retired with their idle connections closed; pool_size / max_overflow from settings"
      contains: "db_pool_size"
    - path: libs/config.py
      provides: "SharedSettings.db_pool_size (5) and db_max_overflow (10)"
      contains: "db_max_overflow"
    - path: infra/docker-compose.yml
      provides: "DB_POOL_SIZE / DB_MAX_OVERFLOW in x-app-env (5/5) and on visit-consumer and analytics-consumer (2/2); a budget comment"
      contains: "DB_MAX_OVERFLOW"
    - path: tests/libs/test_postgres_registry.py
      provides: "bounded-connections test, live-loop alternation test, pool settings test, Compose budget test"
  key_links:
    - from: libs/db/postgres.py
      to: libs/config.py
      via: "create_async_engine(url, pool_pre_ping=True, pool_size=shared_settings.db_pool_size, max_overflow=shared_settings.db_max_overflow)"
      pattern: "max_overflow"
    - from: infra/docker-compose.yml
      to: libs/config.py
      via: "DB_POOL_SIZE / DB_MAX_OVERFLOW environment values read by SharedSettings in every service process"
      pattern: "DB_POOL_SIZE"
---

<objective>
Remove the mechanism that pushed the dev Postgres to its 300-connection ceiling at the phase gate (WR-05), which is what turned CR-01 from theory into an observed failure: `libs/db/postgres.py` `_entry_for` replaces the registry entry whenever the running loop differs and never disposes the old engine, so every per-test loop leaves a pool of live server connections behind; and the Compose stack's worst-case pool budget is above the ceiling. Fix the registry, make the pool size a setting, size the long-running processes so the budget fits, and measure it.

Purpose: with plans 03-21 and 03-22 a connection error no longer loses an event, but the stack should not hit the ceiling in normal use either. The user accepted the gate's test-environment symptom, not this production-path cause. Do not restart Postgres and do not change `max_connections` (not needed by this plan).

Planning spike (2026-11-11, read-only SELECTs on visit_db, SQLAlchemy 2.0.52, asyncpg 0.31.0): six sequential `asyncio.run` loops through `get_session_factory` left all six backends alive while the old entries were still referenced and three of five alive when they were not; `engine.sync_engine.dispose()` on a closed loop left three of six alive; draining the closed loop's idle pool records and closing each raw connection's socket left only the current entry's single backend alive (`AsyncAdapt_asyncpg_connection.terminate()` raises RuntimeError on a closed loop, the socket shutdown does not).

Output: the registry fix, the two settings, the Compose budget, four tests, the four non-API DB processes recreated, a measured peak.

Live stack rules:
- Recreate only `scheduling-worker`, `visit-worker`, `visit-consumer`, `analytics-consumer` with `docker compose -f infra/docker-compose.yml up -d --build --no-deps scheduling-worker visit-worker visit-consumer analytics-consumer`. Do not recreate the nine APIs here: plan 03-24 rebuilds all of them and they pick up this plan's code and `x-app-env` values then (say so in the SUMMARY). Never `down -v`, never remove a volume, never restart `postgres`, never touch `tjo-*` containers (record their status before and after).
- Stop `visit-consumer` around `tests/visit` runs and start it again afterwards (wait for `healthy`).
- Gateway budget rule: before the first e2e run read `docker compose -f infra/docker-compose.yml exec -T gateway printenv GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE`; if it does not print 1000 and 100000, recreate the gateway for the run with `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 docker compose -f infra/docker-compose.yml up -d --no-deps gateway`. Plan 03-25 restores the defaults.
- One pytest process at a time; long runs in the background with polling and a 20 s wall-clock heartbeat (a gap above 60 s means the laptop slept: rerun).
- Connection sampling uses the superuser over the container's local socket, as `tests/e2e/phase3_stack.py` `psql_superuser` does: `docker compose -f infra/docker-compose.yml exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d postgres -Atc "SELECT usename, count(*) FROM pg_stat_activity GROUP BY 1"'`. No `.env*` or `infra/secrets/*` reads, no package installs.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-REVIEW.md
@.planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md
@libs/db/postgres.py
@libs/config.py
@infra/docker-compose.yml
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| `libs/db/postgres.py` registry | code | keyed by database URL and loop (identity-checked); at every `_entry_for` call, entries whose loop is closed are retired; entries of live loops are never touched from another loop |
| `_retire(entry)` | function | drains the closed loop's idle pool records, closes each raw connection's socket (errors suppressed), then `sync_engine.dispose(close=False)`; private attribute access only here, with the verified versions named in a comment |
| `SharedSettings.db_pool_size` / `db_max_overflow` | settings | env `DB_POOL_SIZE` (default 5), `DB_MAX_OVERFLOW` (default 10) |
| Compose budget | config | `x-app-env`: `DB_POOL_SIZE: "5"`, `DB_MAX_OVERFLOW: "5"`; `visit-consumer` and `analytics-consumer`: `"2"` / `"2"`; comment with the arithmetic (8 APIs x 2 workers x 10 + 2 workers x 10 + 2 consumers x 4 + about 40 Temporal + 4 Debezium + 5 Grafana = 237 of 297) |

<tasks>

<task type="tracer" tdd="true">
  <name>Task 1: Tracer — repeated event loops in one process no longer accumulate Postgres connections</name>
  <reversibility rating="reversible">Internal engine cache behaviour behind the unchanged get_engine / get_session_factory API.</reversibility>
  <files>libs/db/postgres.py, tests/libs/test_postgres_registry.py</files>
  <behavior>
    - Eight sequential `asyncio.run` loops on visit_db, each opening a session through `get_session_factory` and recording `pg_backend_pid()`, leave at most one of those backends alive afterwards (checked from a separate `NullPool` engine that is disposed); this test fails on the current code before the change
    - A thread that owns a long-lived `asyncio.Runner` loop and the test's own loop alternate six calls on the same URL: at most two engines are ever created for that URL (one per live loop) and every call succeeds
    - After a loop closes, the next `get_session_factory` call for ANY URL retires that loop's entries; an entry whose loop is alive is not retired
    - One long-lived loop gets the same engine object on repeated calls (production behaviour unchanged)
  </behavior>
  <read_first>
    - libs/db/postgres.py (whole file; module docstring explains the loop-aware design and why it exists)
    - tests/libs/test_observability.py (the only current test touching the registry) and tests/conftest.py (how other tests reach live databases)
    - services/visit/config.py (the host default `VISIT_DATABASE_URL` used by the harness)
    - libs/events/consumer.py (the consumer's persistent `asyncio.Runner`, the alternate-loop case in tests)
  </read_first>
  <action>
    Key the registry by database URL and loop (store the loop on the entry and compare by identity, so a recycled `id()` cannot match). At the top of `_entry_for`, retire every entry whose loop `is_closed()`; never retire or dispose an entry whose loop is still open. Retirement drains the idle (checked-in) connection records from the engine's QueuePool and closes the server side of each raw asyncpg connection by shutting down its transport socket, each step under `contextlib.suppress(Exception)`, then calls `engine.sync_engine.dispose(close=False)` and drops the entry (see the planning spike in the objective for why the socket shutdown and not `terminate()` or a plain `dispose()`). Keep the private-attribute access inside the retire function with `getattr` defaults and a comment naming SQLAlchemy 2.0.52 and asyncpg 0.31.0. Update the module docstring (registry per URL and loop; closed loops are retired with their idle connections closed; why).

    Write `tests/libs/test_postgres_registry.py` with plain `def` tests (no pytest-asyncio loop; each test drives its own loops) for the four behaviors; run the bounded-connections test once before the change and record that it fails (red), then make it pass.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_postgres_registry.py tests/libs/test_observability.py -x -q</automated>
    <fails_when>pytest exits non-zero, more than one backend of the sequential loops survives, the alternating live loops create more than two engines, or an open loop's entry is retired.</fails_when>
  </verify>
  <acceptance_criteria>
    - the SUMMARY records the red run of the bounded-connections test on the old code and the green run after
    - `.venv/bin/python -m pytest tests/libs -q` exits 0
  </acceptance_criteria>
  <done>A process that runs many event loops (the test suite) releases each closed loop's connections instead of holding them until garbage collection.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Pool settings, the Compose budget, the four long-running DB processes, and a measured peak</name>
  <reversibility rating="reversible">Environment values and two settings with defaults; reverting is a Compose edit and a recreate.</reversibility>
  <files>libs/config.py, libs/db/postgres.py, infra/docker-compose.yml, tests/libs/test_postgres_registry.py</files>
  <behavior>
    - `SharedSettings` has `db_pool_size` (default 5, env `DB_POOL_SIZE`) and `db_max_overflow` (default 10, env `DB_MAX_OVERFLOW`); an engine built after setting `DB_POOL_SIZE=2` and `DB_MAX_OVERFLOW=1` (monkeypatched settings) has pool size 2 and overflow 1
    - A static Compose test parses `infra/docker-compose.yml` with merge keys resolved and computes the worst case: every service with a `*_DATABASE_URL` and no `command` override counts as an API with 2 gunicorn workers, every service with a `command` override and a `*_DATABASE_URL` counts as one process, each multiplied by its `DB_POOL_SIZE` + `DB_MAX_OVERFLOW`, plus 40 for Temporal, one per connector file in `infra/connect/connectors/` for Debezium, and Grafana's `maxOpenConns` from `infra/grafana/provisioning/datasources/datasources.yml`; the sum is at most `max_connections` from the postgres `command` minus 3
    - Live: after the recreate all four processes are healthy; `docker compose -f infra/docker-compose.yml exec -T visit-consumer python -c "from libs.config import shared_settings as s; print(s.db_pool_size, s.db_max_overflow)"` prints `2 2`, the same for `analytics-consumer`, and `5 5` for `scheduling-worker` and `visit-worker`
    - Live: the visit downstream and analytics e2e tracers pass after the recreate
    - Evidence: `tests/visit tests/workflows` run in the background (visit-consumer stopped) while `pg_stat_activity` is sampled every 15 s; the SUMMARY records the idle baseline, the peak total and the per-role peak, next to the gate's 304
  </behavior>
  <read_first>
    - libs/config.py (field and alias conventions; the `worker_metrics_port` pattern of a plain field read from its upper-case env name)
    - infra/docker-compose.yml (lines 15-45 `x-app-env` and its comment; the postgres `command` with `max_connections=300`; `visit-consumer` and `analytics-consumer` use `environment: *visit-env` / `*analytics-env` aliases, which must become merges to add keys)
    - tests/libs/test_shared_settings.py and tests/libs/test_config.py (field-set assertions that must keep passing)
    - infra/grafana/provisioning/datasources/datasources.yml (`maxOpenConns: 5`)
    - .planning/phases/03-event-driven-system-observability/03-20-phase-gate-SUMMARY.md (connection-ceiling measurements: about 110 idle in containers, peak 304)
  </read_first>
  <action>
    Add the two settings to `SharedSettings` with a comment (the budget lives in Compose; code defaults keep SQLAlchemy's 5 and 10 for host tests and scripts) and pass them to `create_async_engine` in `_entry_for`. In Compose add `DB_POOL_SIZE: "5"` and `DB_MAX_OVERFLOW: "5"` to `x-app-env` with a comment carrying the arithmetic from the artifacts table, and change `visit-consumer` and `analytics-consumer` to a merged environment (`<<: *visit-env` / `<<: *analytics-env`) adding `DB_POOL_SIZE: "2"` and `DB_MAX_OVERFLOW: "2"` with a one-line reason (one event at a time). Do not add a gunicorn worker-count variable or the Prometheus multiprocess directory variable anywhere in Compose, not even in a comment (plan 03-24 owns the API processes; two workers per API stay, per the user's decision). Add the settings and Compose budget tests to `tests/libs/test_postgres_registry.py`.

    Recreate the four processes (live stack rules), check health and the printed settings, apply the gateway budget rule and run the two e2e tracers. Then, with `visit-consumer` stopped, run `tests/visit tests/workflows` in the background with a 15 s `pg_stat_activity` sampler writing to a scratchpad file, start the consumer again afterwards, and record the numbers. If the peak is not clearly below the gate's 304, report the per-role breakdown and stop for review instead of tuning further.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/libs/test_postgres_registry.py tests/libs/test_shared_settings.py tests/libs/test_config.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py -m e2e -k tracer -x -q</automated>
    <fails_when>Either pytest run exits non-zero, the Compose worst-case budget exceeds max_connections minus 3, a recreated process is unhealthy or prints other pool values, or the e2e tracers fail.</fails_when>
  </verify>
  <acceptance_criteria>
    - `grep -c "DB_MAX_OVERFLOW" infra/docker-compose.yml` prints at least 3
    - `grep -c "PROMETHEUS_MULTIPROC_DIR\|WEB_CONCURRENCY" infra/docker-compose.yml` prints 0
    - `docker compose -f infra/docker-compose.yml ps scheduling-worker visit-worker visit-consumer analytics-consumer --format '{{.Service}} {{.Health}}'` shows `healthy` four times
    - the SUMMARY records the sampled baseline and peak per role and the computed Compose budget
    - tjo-* status lines before and after are identical apart from uptime
  </acceptance_criteria>
  <done>The connection budget fits the dev ceiling by construction, the long-running processes run with it, and a suite run no longer drives Postgres toward 300 connections.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| service processes -> Postgres | a shared, finite connection pool (max_connections=300) used by every service, Temporal, Debezium and Grafana |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-71 | Denial of Service | Postgres connection slots (exhaustion makes every service fail with 53300, the CR-01 trigger) | high | mitigate | Closed-loop engines retired with their connections closed; pool sizes as settings; Compose worst case 237 of 297 enforced by a static test; peak measured live |
| T-3-72 | Denial of Service | `_retire` (closing connections that a live loop in another thread still uses) | medium | mitigate | Only entries whose loop is closed are retired; a test alternates two live loops and asserts both keep working |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | No package installed in this plan |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/libs -q`
- `.venv/bin/python -m pytest tests/visit tests/workflows -q` (background, visit-consumer stopped, then started)
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_visit_downstream.py tests/e2e/test_phase3_analytics.py -m e2e -k tracer -q`
- Semgrep: `semgrep --config p/python --metrics=off libs/db libs/config.py`; record the status in the SUMMARY
</verification>

<success_criteria>
- WR-05 closed: no orphaned engines, pool size as a setting, the Compose worst case fits under the ceiling, measured
- Nothing restarted beyond the four named processes
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-23-connection-budget-SUMMARY.md` when done
</output>
