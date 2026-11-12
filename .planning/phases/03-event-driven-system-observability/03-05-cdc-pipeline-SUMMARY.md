---
phase: 03-event-driven-system-observability
plan: 05
subsystem: infra
tags: [debezium, kafka-connect, cdc, postgres-logical-replication, transactional-outbox, schema-registry]
requires:
  - phase: 03-event-driven-system-observability
    provides: "03-01 outbox_table_ddl, Debezium Connect image approval; 03-03 ten registered-able Avro schemas, libs.events.register, infra/secrets/infra files"
provides:
  - Postgres wal_level=logical on the existing volume (max_slot_wal_keep_size=2GB)
  - Compose services connect, kafka-init, schema-init, connect-init
  - libs.events.migration.create_outbox / drop_outbox (table, publication dbz_outbox, guarded SELECT grant to debezium)
  - scheduling_0006 outbox migration and the scheduling-outbox Debezium connector
  - infra/connect/register-connectors.sh (idempotent PUT + task-state poll, registers by name)
  - db-init roles debezium (LOGIN REPLICATION), grafana_ro, analytics service db
  - tests/e2e/phase3_stack.py helpers and connector-parametrized CDC tests
affects: [03-07, 03-08, 03-09, 03-10, 03-11, 03-13, 03-16, 03-17, 03-18]
tech-stack:
  added: ["quay.io/debezium/connect:3.7.1.Final (approved image, no pip/npm install)"]
  patterns:
    - "One connector per outbox database, publication dbz_outbox created by the table owner inside the service migration"
    - "Connector password via Compose secret + FileConfigProvider reference; db-init reads the same secret with \\getenv"
    - "Connector tests parametrized over the JSON files present, so later connectors are covered without editing tests"
key-files:
  created:
    - infra/connect/register-connectors.sh
    - infra/connect/connectors/scheduling-outbox.json
    - infra/kafka/create-topics.sh
    - libs/events/migration.py
    - services/scheduling/migrations/versions/scheduling_0006_outbox.py
    - tests/e2e/phase3_stack.py
    - tests/e2e/test_phase3_outbox_cdc.py
    - tests/infra/__init__.py
    - tests/infra/test_cdc_config.py
  modified:
    - infra/docker-compose.yml
    - infra/postgres-init/create-service-databases.sh
    - infra/.env.example
key-decisions:
  - "Connect runs on quay.io/debezium/connect:3.7.1.Final (documented deviation from the same-CP-line rule; broker and registry stay 7.6.13)"
  - "The status parser in register-connectors.sh uses grep/sed, not python: the Debezium image ships curl and bash only"
  - "The disposable snapshot-probe connector routes to its own topic and is restricted by snapshot.select.statement.overrides to the one probe aggregate, so it can never re-emit real outbox rows into a real topic"
  - "A restart policy of unless-stopped on connect means docker kill is followed by an automatic restart; the kill test treats start as idempotent"
requirements-completed: []
status: complete
duration: 14 min of executor wall time after the plan was read (07:51Z to 08:05Z, excluding the image build wait)
completed: 2026-11-11
actuals:
  tokens: 16200
  tasks: 3
  commits: 3
plan_head_before: d020d7b67ea7a837ef6be6d40d55966dc10dbd04
plan_head_after: 52e13c3d506ea45704b8f1c7dabfa37279b702f5
---

# Phase 3 Plan 05: CDC Pipeline Summary

**Debezium CDC relay on Postgres logical decoding: a row committed to scheduling_db.outbox reaches `smarthealth.<aggregate>.events` with key = aggregate id, headers id/eventType/traceparent/schemaVersion and a registry-framed Avro value, survives Connect stop and SIGKILL with no loss, and uses a least-privilege replication role whose password never leaves a Compose secret.**

`requirements-completed` is intentionally empty: the orchestrator instructed not to touch REQUIREMENTS.md or tick requirements (EVENT-01, EVENT-02 remain for the phase gate).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 (tracer) | 860d93a | Compose services, Postgres flags, db-init roles, topics, schema-init, connector + registration script, outbox migration helper and scheduling_0006, e2e helpers, tracer test |
| 2 | f0133b6 | Static config tests (15) and per-connector task/slot/publication/least-privilege e2e tests, Connect port-not-published test |
| 3 | 52e13c3 | Stop/start ordering, SIGKILL no-loss, initial-snapshot proof with full teardown |

## Test results (honest)

- `tests/infra`: 15 passed.
- Tracer: passed against the live stack (first run failed on a test bug: Avro `uuid` decodes to `uuid.UUID`; assertion fixed, delivery itself worked first time).
- `tests/e2e/test_phase3_outbox_cdc.py -m e2e`: **10 passed** (tracer, connector tasks running, slots and publications, debezium least privilege, Connect stopped ordering, hard kill, snapshot, everything running afterwards, no orphan slot, REST port not published) in 67 s. One earlier run of the REST-port test failed because `docker compose port connect 8083` prints `:0` when nothing is published; the test now reads `Publishers` from `compose ps --format json`.
- Regression: `tests/infra tests/libs/test_service_isolation.py tests/events`: 138 passed (99 s). `tests/e2e/test_phase2_flows.py`: 5 passed after the gateway budgets were restored (see Deviations; the first run hit a 429).
- Semgrep (`--config p/python --metrics=off`) over the four new Python files: 151 rules, 0 findings. The full non-e2e suite was not run, as instructed.
- Known unrelated failure not touched: `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway`.

## Observed duplicate count after the hard kill

**5 duplicates** (11 messages for the aggregate, 6 distinct event ids): Connect was killed within seconds of the first five events being delivered, before its offset flush, so on restart the same five were re-emitted. This is the evidence for the at-least-once-on-the-wire statement in the docs (research F4: 6 -> 11 in the earlier spike was the same shape). Nothing was lost, and the sixth event, committed while Connect was down, came last.

## Live-stack operations record

- **Pre-restart backup:** `pg_dumpall` (superuser taken from the container's own `POSTGRES_USER`, nothing printed) written mode 0600 to `/private/tmp/claude-501/-Users-emumba-smart-Health/499f0e91-fd2c-4808-a531-ce7d2c029e29/scratchpad/pg_dumpall_before_03-05.sql`, **69,580,675 bytes**, ends with "PostgreSQL database cluster dump complete". Not in the repo.
- **Command:** one `docker compose -f infra/docker-compose.yml up -d --build` (all application images rebuilt, `postgres` recreated once, `db-init` re-run, new services created). No `down -v`, no volume removed, no `docker restart`/`kill`/`stop` outside the tests' Compose-scoped Connect stop/kill. `rabbitmq` and `mongo` were not recreated.
- **Same volume, data survived:** `postgres` mounts `smarthealth_postgres_data` before and after. Row counts identical before and after: users (identity_db) 5755, appointments (scheduling_db) 312, provider_slots 4819, patients (patient_db) 1801, visits (visit_db) 1. Only intended differences: scheduling alembic `scheduling_0005 -> scheduling_0006`, `wal_level replica -> logical`.
- **Health:** `scripts/compose_healthcheck.sh --wait 120` printed OK with every required service and every service with a healthcheck healthy (including `connect`). `kafka-init`, `schema-init`, `connect-init` and `db-init` all `exited 0`.
- **Registry:** `GET /subjects` lists the 10 `com.smarthealth.events.*` subjects, registered by `schema-init`.
- **Connector:** `GET /connectors/scheduling-outbox/status` via the Connect REST API reports connector RUNNING and task 0 RUNNING (task state is what the script and tests check). Slot `dbz_scheduling` active, about 17 MB retained WAL at the end. No `dbz_snapshot_probe` slot or snapshot-probe topic remains.
- **tjo-* containers** before: `tjo-api Up 25 hours (unhealthy)`, `tjo-postgres Up 25 hours (healthy)`. After the last Compose change: identical (`Up 25 hours`, same health). `tjo-api` was already unhealthy before this plan; not touched.
- **Topics:** the five event topics and three DLQ topics have 3 partitions and `retention.ms=-1` (checked on `smarthealth.appointment.events`); Connect's own three storage topics exist. The `p3test.*` topics seen in the list belong to plan 03-04's tests.
- **Secrets:** no `.env*` or `infra/secrets/*` content was read or printed; `printenv` in the connect container shows no password variable (only a count was taken).

## Deviations from Plan

**1. [Rule 3 - Blocking/regression] Gateway recreated with Compose default rate budgets, then restored.** The plan's `up -d --build` rebuilt and recreated `gateway`. The previously running gateway carried raised budgets passed on the command line in earlier plans (`GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000`, `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000`, per the 02-12 and 02.1-06 SUMMARYs); recreating it with Compose defaults (20/300) made the first `test_phase2_flows` run hit `429 rate limit exceeded`. I recreated only `gateway` (`up -d gateway`, no build) with those two documented values; the container now shows 1000 and 100000, and `test_phase2_flows` passed 5/5. I could not read the exact pre-existing values from the old container (it no longer exists), so these are the values the earlier SUMMARYs record. If a different pair was in use, re-set it. Not a code change.

**2. [Test helper design] Connector tests parametrize over `connector_names()` at collection time;** the snapshot test builds its disposable connector from `scheduling-outbox.json` and overrides the route topic, prefix, slot and snapshot select so it cannot re-emit real outbox rows (not spelled out in the plan; Rule 2, protects real topics once producers write real events).

**3. [Rule 1 - Bug in my own tests, fixed before commit]** Avro `uuid` logical type decodes to `uuid.UUID`; the `compose port` output for an unpublished port is `:0`. Both assertions corrected.

**4. [Plan wording] Task 2 is committed without the Task 3 tests in the same file** (the file was assembled in pieces so the three commits stay atomic); the final file is the union.

No architectural deviation, no auth gate, no package installs. `infra/.env.example` accepted the `ANALYTICS_DB_PASSWORD` addition (no harness block).

## Things later plans should know

- The registration command for later connectors: `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh <name>`; the Compose `connect-init` registers every file present on the next `up`.
- `snapshot.mode=initial` plus `snapshot.locking.mode=none` worked on first registration with no outbox rows then present.
- `FileConfigProvider` via `CONNECT_CONFIG_PROVIDERS*` env vars worked on the Debezium image as-is (no worker properties fragment needed), and the secret file (mode 0600 on the host) was readable inside the container.
- `restart: unless-stopped` on `connect` restarts it after `docker compose kill`, so the kill test's `start_service` is a harmless no-op in practice.
- `analytics_svc` / `analytics_db` now exist (db-init `SERVICES`); `grafana_ro` has CONNECT, schema USAGE and default SELECT on future analytics_svc tables.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-3-05 (no host port, asserted by a static and an e2e test), T-3-06 (FileConfigProvider reference required by the static test, `printenv` check), T-3-07 (REPLICATION role, CONNECT on four DBs, SELECT on outbox only, `permission denied` proven on `appointments`), T-3-09 (`max_slot_wal_keep_size=2GB`, heartbeat 10 s, probe slot dropped, no orphan) and T-3-10 (only `schema-init` registers) are implemented.

## Self-Check: PASSED

- Files exist: infra/connect/register-connectors.sh, infra/connect/connectors/scheduling-outbox.json, infra/kafka/create-topics.sh, libs/events/migration.py, services/scheduling/migrations/versions/scheduling_0006_outbox.py, tests/e2e/phase3_stack.py, tests/e2e/test_phase3_outbox_cdc.py, tests/infra/test_cdc_config.py (all committed).
- Commits 860d93a, f0133b6, 52e13c3 exist on gsd/phase-02-scheduling-service-workflow (`git rev-list --count d020d7b..HEAD` = 3 at write time).
