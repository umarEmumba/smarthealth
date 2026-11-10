---
phase: 03-event-driven-system-observability
plan: 02
type: execute
wave: 2
depends_on: ["03-01"]
files_modified:
  - infra/docker-compose.yml
  - infra/postgres-init/create-service-databases.sh
  - infra/kafka/create-topics.sh
  - infra/connect/register-connectors.sh
  - infra/connect/connectors/scheduling-outbox.json
  - infra/connect/connectors/visit-outbox.json
  - infra/connect/connectors/provider-outbox.json
  - infra/connect/connectors/patient-outbox.json
  - infra/.env.example
  - libs/events/migration.py
  - services/scheduling/migrations/versions/scheduling_0006_outbox.py
  - services/visit/migrations/versions/visit_0003_outbox.py
  - services/provider/migrations/versions/provider_0003_outbox.py
  - services/patient/migrations/versions/patient_0002_outbox.py
  - tests/e2e/phase3_stack.py
  - tests/e2e/test_phase3_outbox_cdc.py
  - tests/infra/__init__.py
  - tests/infra/test_cdc_config.py
autonomous: true
requirements: [EVENT-01, EVENT-02]

estimate:
  tokens: 115000
  raw_tokens: 115000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Postgres runs with wal_level=logical on the existing data volume; no data is lost and no tjo-* container is restarted (D-01)"
    - "A row committed to an outbox table reaches its Kafka topic through Debezium with key = aggregate id and headers id, eventType, traceparent, schemaVersion, and its value decodes against the live Schema Registry (D-01, D-05, D-06)"
    - "Each of the four outbox databases has its own connector whose task (not only the connector) is RUNNING, checked by the init job and by tests (Pitfall 2)"
    - "Events committed while Connect is stopped are delivered, in order per aggregate, after it starts; a hard kill of Connect loses nothing (duplicates on the wire are allowed and counted, research F4)"
    - "The Debezium role can replicate and read only the outbox tables; its password never appears in a connector config, an env value or an image (Compose secret + FileConfigProvider)"
    - "Schemas are registered on the live registry only by the schema-init job; topics are pre-created with explicit partitions and unlimited retention"
  artifacts:
    - path: infra/connect/register-connectors.sh
      provides: "idempotent PUT of every connector JSON and a task-state poll"
      contains: "tasks"
    - path: libs/events/migration.py
      provides: "create_outbox(op, service_role) / drop_outbox(op): table, publication dbz_outbox, guarded SELECT grant to debezium"
      contains: "CREATE PUBLICATION dbz_outbox"
    - path: tests/e2e/test_phase3_outbox_cdc.py
      provides: "probe-row delivery, Connect stop/start ordering, hard-kill no-loss, slot/publication/privilege checks"
    - path: tests/e2e/phase3_stack.py
      provides: "compose, connector_status, wait_connectors_running, consume_matching, kill/stop/start/wait_healthy helpers"
  key_links:
    - from: infra/connect/connectors/scheduling-outbox.json
      to: services/scheduling/migrations/versions/scheduling_0006_outbox.py
      via: "publication dbz_outbox and table public.outbox created by the migration, read by the connector"
      pattern: "dbz_outbox"
    - from: infra/docker-compose.yml
      to: libs/events/register.py
      via: "schema-init job runs python -m libs.events.register before the producing services start"
      pattern: "libs.events.register"
---

<objective>
Stand up the CDC relay (D-01): Postgres logical decoding, Kafka Connect with Debezium, one connector per outbox database, pre-created topics, deploy-time schema registration, and the four outbox tables. After this plan, any row a service commits to its `outbox` reaches Kafka with no relay code in the service.

Purpose: EVENT-02 (no dual write) and the delivery half of roadmap success criterion 1: "never lost" is proven here against Connect stop/start and a hard kill; "never duplicated" is defined honestly as effect-level exactly-once and proven by the consumer plans (03-03, 03-07, 03-09, 03-11, 03-13).

Output: Compose services `connect`, `kafka-init`, `schema-init`, `connect-init`; Postgres flags; db-init roles for Debezium, analytics and Grafana read-only; four outbox migrations; connector templates; CDC e2e and static config tests; `tests/e2e/phase3_stack.py` helpers reused by later e2e plans.

Decisions honoured: D-01 (CDC, not a poller; one connector per outbox database; `wal_level=logical` keeps the volume; init and tests check task state), D-01 detail (Connect on `quay.io/debezium/connect:3.7.1.Final`, a documented deviation from the "same CP line" rule; broker and registry stay 7.6.13), D-05 detail (`BinaryDataConverter` passthrough, registration by `schema-init`), D-06 revised (topic per aggregate via `route.by.field=aggregatetype`, key = `aggregateid`), D-10 revised (outbox insert-only, it is the replay source; `snapshot.mode=initial` covers rows written before the connector first starts). Claude's discretion: 3 partitions, `retention.ms=-1`, `max_slot_wal_keep_size=2GB`, `heartbeat.interval.ms=10000`, Connect heap capped at 768 MiB.

Live stack rules (every Compose command in this plan): use only `docker compose -f infra/docker-compose.yml ...` (project `smarthealth`); never `down -v`, `system prune`, `volume rm`, or any `docker restart`/`kill`/`stop` not scoped to a `smarthealth` Compose service. Containers named `tjo-*` belong to another project: record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before the first Compose change and compare after the last; none may show a fresh "Up N seconds". Migrations run at container start (`alembic upgrade head` in each image's CMD) on the existing volumes; host-side tests also run `.venv/bin/alembic -c services/<svc>/migrations/alembic.ini upgrade head`. This plan rebuilds every application image (plan 03-01 changed `pyproject.toml`) and recreates `postgres` once (Pitfall 14: every service connection blips; run `scripts/compose_healthcheck.sh` after). Plans 03-03 and 03-04 run in the same wave against the live Kafka, RabbitMQ, Mongo and Postgres: if their integration tests fail during this plan's rebuild, they re-run once the health check passes.
</objective>

<execution_context>
@/Users/emumba/smart Health/.claude/gsd-core/workflows/execute-plan.md
@/Users/emumba/smart Health/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/phases/03-event-driven-system-observability/03-CONTEXT.md
@.planning/phases/03-event-driven-system-observability/03-RESEARCH.md
@.planning/phases/03-event-driven-system-observability/03-01-SUMMARY.md
@infra/docker-compose.yml
@infra/postgres-init/create-service-databases.sh
@libs/events/outbox.py
@libs/events/catalog.py
@tests/e2e/phase2_stack.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| Compose `postgres` command | infra | adds `-c wal_level=logical -c max_slot_wal_keep_size=2GB` |
| Compose `connect` | infra | `quay.io/debezium/connect:3.7.1.Final`, no host port, `CONNECT_CONFIG_PROVIDERS=file`, secret `debezium.properties`, heap capped, healthcheck `curl -fs http://localhost:8083/connectors` |
| Compose `kafka-init` | one-shot | `confluentinc/cp-kafka:7.6.13`, runs `infra/kafka/create-topics.sh` |
| Compose `schema-init` | one-shot | scheduling image, `python -m libs.events.register`; added to the `x-app-depends-on` anchor |
| Compose `connect-init` | one-shot | Debezium image, runs `infra/connect/register-connectors.sh` after the four producing APIs are healthy |
| `x-app-env` additions | infra | `KAFKA_BOOTSTRAP_SERVERS: kafka:29092`, `SCHEMA_REGISTRY_URL: http://schema-registry:8081` |
| Compose secrets | infra | `debezium_db_password` (`./secrets/infra/debezium.properties`), `grafana_ro_password` (`./secrets/infra/grafana_ro.secret`) |
| db-init additions | infra | role `debezium` (LOGIN REPLICATION, CONNECT on scheduling/visit/provider/patient DBs), `analytics` in `SERVICES` (`analytics_svc`/`analytics_db`, `ANALYTICS_DB_PASSWORD`), role `grafana_ro` (CONNECT + default SELECT in analytics_db) |
| Topics | Kafka | `smarthealth.{appointment,visit,provider,patient,cdcprobe}.events` (3 partitions), `smarthealth.dlq.{visit-downstream,notification-trigger,analytics-projection}`, all `retention.ms=-1` |
| Connectors | Connect | `scheduling-outbox`, `visit-outbox`, `provider-outbox`, `patient-outbox`; slots `dbz_scheduling`, `dbz_visit`, `dbz_provider`, `dbz_patient`; publication `dbz_outbox` per DB |
| `libs/events/migration.py` | module | `create_outbox(op)`, `drop_outbox(op)` |
| Migrations | Alembic | `scheduling_0006`, `visit_0003`, `provider_0003`, `patient_0002` (each: outbox table, publication, guarded grant) |
| `tests/e2e/phase3_stack.py` | test helper | `compose()`, `connector_status()`, `wait_connectors_running()`, `consume_matching()`, `kill_service()`, `stop_service()`, `start_service()`, `wait_service_healthy()`, `live_registry()`, `insert_probe_rows()` |

<tasks>

<task type="tracer">
  <name>Task 1: Tracer — a probe row committed to scheduling_db.outbox arrives on Kafka through Debezium and decodes against the live registry</name>
  <reversibility rating="costly">D-01: logical replication, replication slots and a Connect service in Compose; switching to a poller later means writing a relay and removing connectors and slots.</reversibility>
  <precondition>`infra/secrets/infra/debezium.properties` and `infra/secrets/infra/grafana_ro.secret` exist (written by plan 03-01 Task 4); check with `test -f`, never print them.</precondition>
  <files>infra/docker-compose.yml, infra/postgres-init/create-service-databases.sh, infra/kafka/create-topics.sh, infra/connect/register-connectors.sh, infra/connect/connectors/scheduling-outbox.json, libs/events/migration.py, services/scheduling/migrations/versions/scheduling_0006_outbox.py, tests/e2e/phase3_stack.py, tests/e2e/test_phase3_outbox_cdc.py</files>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md (Pattern 2 connector JSON and Compose notes, Runtime State Inventory, Pitfalls 1, 2, 4, 14, 16, 17)
    - infra/docker-compose.yml (anchors `x-app-env`, `x-app-depends-on`, db-init block, secrets block, worker heartbeat pattern)
    - infra/postgres-init/create-service-databases.sh (idempotent role/database pattern, `\getenv`, never `set -x`)
    - libs/events/outbox.py (`outbox_table_ddl`), libs/events/catalog.py (topic names, `CDC_PROBE_TOPIC`), libs/events/register.py
    - services/scheduling/migrations/versions/scheduling_0005_schedule_change_flags.py (revision id style, upgrade/downgrade layout)
    - tests/e2e/phase2_stack.py (`require_stack`, `poll`, skip/fail convention with `SMARTHEALTH_E2E_REQUIRE_STACK`)
  </read_first>
  <action>
    Postgres: change the `postgres` command to add `-c wal_level=logical -c max_slot_wal_keep_size=2GB`, with a comment (D-01, Pitfall 4: an inactive slot retains WAL; the cap bounds disk use; deleting a connector does not drop its slot).

    db-init (create-service-databases.sh + its Compose block): mount the two secrets (`debezium_db_password` as `debezium.properties`, `grafana_ro_password` as `grafana_ro.secret`); read the Debezium password from the `password=` line of `/run/secrets/debezium.properties` into an environment variable passed to psql via `\getenv` (never on a command line, never echoed); create-or-resync role `debezium` with `LOGIN REPLICATION` and no other attributes; `GRANT CONNECT` on scheduling_db, visit_db, provider_db and patient_db to it (the script revokes PUBLIC connect, so this grant is required). Add `analytics` to `SERVICES` and `ANALYTICS_DB_PASSWORD: ${ANALYTICS_DB_PASSWORD:-changeme-analytics-local-dev-only}` to db-init's environment (same pattern as the seven existing service passwords). Create-or-resync role `grafana_ro` (LOGIN, password from `/run/secrets/grafana_ro.secret`), `GRANT CONNECT ON DATABASE analytics_db`, and, connected to analytics_db as the superuser, `GRANT USAGE ON SCHEMA public` plus `ALTER DEFAULT PRIVILEGES FOR ROLE analytics_svc IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro`. Add `ANALYTICS_DB_PASSWORD` to `infra/.env.example` (if the harness blocks that edit, list it in the SUMMARY as plan 01.1-09 did).

    Migration helper (libs/events/migration.py): `create_outbox(op)` executes `outbox_table_ddl()`, `CREATE PUBLICATION dbz_outbox FOR TABLE outbox`, and a `DO` block that grants `SELECT ON outbox TO debezium` only when that role exists; `drop_outbox(op)` drops the publication and the table (the downgrade loses the replay source; say so in the docstring). `scheduling_0006_outbox.py` (revision `scheduling_0006`, down `scheduling_0005`) calls them.

    Topics (infra/kafka/create-topics.sh): with `kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists`, create the five event topics with 3 partitions and the three DLQ topics with 3 partitions, all `--config retention.ms=-1`; exit non-zero on any failure. Compose `kafka-init`: image `confluentinc/cp-kafka:7.6.13`, entrypoint `bash /kafka-init/create-topics.sh`, mount `./kafka:/kafka-init:ro`, depends on kafka healthy, `restart: "no"`.

    Registry job: Compose `schema-init` builds `services/scheduling/Dockerfile`, command `python -m libs.events.register`, env `SCHEMA_REGISTRY_URL: http://schema-registry:8081`, depends on schema-registry healthy, `restart: "no"`. Add `schema-init: {condition: service_completed_successfully}` to `x-app-depends-on`, and `KAFKA_BOOTSTRAP_SERVERS: kafka:29092` plus `SCHEMA_REGISTRY_URL: http://schema-registry:8081` to `x-app-env` (literal values, matching the anchor's comment).

    Connect: Compose `connect` with image `quay.io/debezium/connect:3.7.1.Final` and a comment recording the D-01 deviation and its reason (Java 11 in cp-kafka-connect cannot load Debezium 3.x); env `BOOTSTRAP_SERVERS=kafka:29092`, `GROUP_ID=smarthealth-connect`, `CONFIG_STORAGE_TOPIC=smarthealth_connect_configs`, `OFFSET_STORAGE_TOPIC=smarthealth_connect_offsets`, `STATUS_STORAGE_TOPIC=smarthealth_connect_statuses`, the three `*_STORAGE_REPLICATION_FACTOR=1`, `KAFKA_HEAP_OPTS=-Xms256m -Xmx768m`, `CONNECT_CONFIG_PROVIDERS=file`, `CONNECT_CONFIG_PROVIDERS_FILE_CLASS=org.apache.kafka.common.config.provider.FileConfigProvider`; secret `debezium_db_password` targeted at `debezium.properties`; NO `ports:` entry (the REST API stays on the Compose network); healthcheck `curl -fs http://localhost:8083/connectors`; depends on kafka healthy and kafka-init completed; `restart: unless-stopped`.

    Connector (infra/connect/connectors/scheduling-outbox.json): the research Pattern 2 config with `database.dbname=scheduling_db`, `topic.prefix=smarthealth-scheduling`, `slot.name=dbz_scheduling`, and `database.password` set to the FileConfigProvider reference `${file:/run/secrets/debezium.properties:password}`. Keep `publication.autocreate.mode=disabled`, `slot.drop.on.stop=false`, `snapshot.mode=initial`, `snapshot.locking.mode=none` (assumption A2), `table.include.list=public.outbox`, `BinaryDataConverter`, the `EventRouter` field mapping and `route.topic.replacement=smarthealth.${routedByValue}.events`, header placement `type:header:eventType,traceparent:header:traceparent,schema_version:header:schemaVersion`, `heartbeat.interval.ms=10000`.

    Registration (infra/connect/register-connectors.sh, bash, no `set -x`): for every `/connect-init/connectors/*.json`, `PUT http://connect:8083/connectors/<basename>/config` (idempotent), then poll `GET /connectors/<name>/status` every 3 s up to 120 s until the connector state and EVERY task state are `RUNNING`; on `FAILED` or timeout print the connector name and the states only (never the trace or config) and exit 1. Compose `connect-init`: same Debezium image, entrypoint `bash /connect-init/register-connectors.sh`, mount `./connect:/connect-init:ro`, depends on connect healthy and on `scheduling` healthy (its Alembic run creates the table and publication), `restart: "no"`. If the image does not honour the `CONNECT_CONFIG_PROVIDERS*` variables (task fails with an unresolved placeholder), fix it by mounting a worker properties fragment; never fall back to a literal password in any JSON or env value.

    Test helpers (tests/e2e/phase3_stack.py): fixed-argv subprocess wrappers that always pass `-f infra/docker-compose.yml` from the repo root (`compose(*args)`, `connector_status(name)` via `compose exec -T connect curl -s ...`, `wait_connectors_running(names, timeout)`, `kill_service`, `stop_service`, `start_service`, `wait_service_healthy`), `live_registry()` (client on `http://localhost:8081`), `consume_matching(topic, predicate, timeout, min_count=1)` (host `confluent_kafka.Consumer` on `localhost:9092`, unique group id, earliest, returns matching messages with key, headers, partition, offset and decoded value), and `insert_probe_rows(db_url, rows)` (serializes AppointmentBooked samples and inserts them with `aggregatetype='cdcprobe'` so they route to `smarthealth.cdcprobe.events`, which no consumer reads). Reuse `require_stack`'s skip/fail convention.

    Tracer test (tests/e2e/test_phase3_outbox_cdc.py, marker e2e): insert one probe row into scheduling_db's real outbox as scheduling_svc, then assert one message on the probe topic with key = the row's aggregate id, headers `id` (= row id), `eventType`, `traceparent`, `schemaVersion`, and a value that `deserializer()` on the live registry decodes to the inserted fields.

    Bring up: `docker compose -f infra/docker-compose.yml up -d --build`, then `scripts/compose_healthcheck.sh`, `docker compose -f infra/docker-compose.yml exec -T postgres psql -U smarthealth -d postgres -Atc 'SHOW wal_level'` (expect `logical`), and the tjo-* comparison.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k tracer -x -q</automated>
    <fails_when>pytest exits non-zero, the probe message does not arrive within 60 s, a header is missing, the value fails to decode, or `SHOW wal_level` prints `replica`.</fails_when>
  </verify>
  <acceptance_criteria>
    - `docker compose -f infra/docker-compose.yml exec -T postgres psql -U smarthealth -d postgres -Atc 'SHOW wal_level'` prints `logical`
    - `docker compose -f infra/docker-compose.yml ps -a connect-init --format '{{.State}} {{.ExitCode}}'` shows `exited 0`, and the same for `kafka-init` and `schema-init`
    - `curl -s http://localhost:8081/subjects` lists 10 subjects starting with `com.smarthealth.events.`
    - `grep -c "8083:" infra/docker-compose.yml` is 0 and `grep -c "quay.io/debezium/connect:3.7.1.Final" infra/docker-compose.yml` is at least 1
    - the tjo-* status lines recorded before and after are identical apart from uptime growth (no container restarted)
    - the tracer test passes
  </acceptance_criteria>
  <done>A committed outbox row in scheduling_db reaches Kafka through Debezium with the agreed key, headers and Avro value; the stack is healthy on the existing volumes.</done>
</task>

<task type="auto">
  <name>Task 2: The other three outbox databases, their connectors, and least-privilege checks</name>
  <files>services/visit/migrations/versions/visit_0003_outbox.py, services/provider/migrations/versions/provider_0003_outbox.py, services/patient/migrations/versions/patient_0002_outbox.py, infra/connect/connectors/visit-outbox.json, infra/connect/connectors/provider-outbox.json, infra/connect/connectors/patient-outbox.json, infra/docker-compose.yml, tests/infra/__init__.py, tests/infra/test_cdc_config.py</files>
  <read_first>
    - libs/events/migration.py and scheduling_0006_outbox.py (Task 1, copy the shape)
    - services/visit/migrations/versions/visit_0002_settlement_markers.py, services/provider/migrations/versions/provider_0002_schedule_change_events.py, services/patient/migrations/versions/patient_0001_patients.py (revision ids `visit_0002`, `provider_0002`, `patient_0001`)
    - infra/connect/connectors/scheduling-outbox.json (Task 1)
  </read_first>
  <action>
    Migrations: `visit_0003_outbox.py` (revision `visit_0003`, down `visit_0002`), `provider_0003_outbox.py` (revision `provider_0003`, down `provider_0002`), `patient_0002_outbox.py` (revision `patient_0002`, down `patient_0001`), each calling `create_outbox` / `drop_outbox`. Billing and identity get no outbox (no D-07 events).

    Connectors: `visit-outbox.json` (`visit_db`, `topic.prefix=smarthealth-visit`, slot `dbz_visit`), `provider-outbox.json` (`provider_db`, `smarthealth-provider`, `dbz_provider`), `patient-outbox.json` (`patient_db`, `smarthealth-patient`, `dbz_patient`), otherwise identical to the scheduling file. Extend `connect-init`'s `depends_on` with `visit`, `provider` and `patient` healthy.

    Static test (tests/infra/test_cdc_config.py, no stack needed; parse YAML with `yaml.safe_load` if PyYAML is importable, else fall back to text checks): every connector JSON has the required keys and values listed in Task 1, a `database.password` that starts with `${file:`, distinct `slot.name` and `database.dbname` values, and `table.include.list=public.outbox`; the Compose file pins `cp-kafka` and `cp-schema-registry` to the same `7.6.13` tag, pins Connect to `quay.io/debezium/connect:3.7.1.Final`, gives `connect` no `ports`, puts `wal_level=logical` in the postgres command, and declares the two new secrets with `./secrets/infra/` paths.

    Rebuild only what changed: `docker compose -f infra/docker-compose.yml up -d --build --no-deps visit visit-worker provider patient`, then `docker compose -f infra/docker-compose.yml up -d connect-init` (re-runs the registration for all four files), then the tjo-* comparison.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra/test_cdc_config.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "connectors or privileges or slots" -x -q</automated>
    <fails_when>Either pytest run exits non-zero, a connector task is not RUNNING, a slot or publication is missing, or the debezium role can read a non-outbox table.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/infra/test_cdc_config.py -q` exits 0
    - the e2e test `test_all_four_connector_tasks_running` asserts every `tasks[*].state == "RUNNING"` for the four connectors (connector state alone is not accepted)
    - the e2e test `test_slots_and_publications` finds `dbz_scheduling`, `dbz_visit`, `dbz_provider`, `dbz_patient` in `pg_replication_slots` with `active = true` and `outbox` in `pg_publication_tables` of each database
    - the e2e test `test_debezium_role_least_privilege` (password read from the secret file, never printed) can `SELECT count(*) FROM outbox` in scheduling_db and gets `permission denied` for `SELECT 1 FROM appointments`
    - `.venv/bin/alembic -c services/visit/migrations/alembic.ini upgrade head` and the same for provider and patient exit 0 on the host
  </acceptance_criteria>
  <done>All four outbox databases relay through their own RUNNING connector task with a least-privilege replication role.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Durability proofs — Connect stopped, Connect hard-killed, ordering per aggregate</name>
  <files>tests/e2e/test_phase3_outbox_cdc.py, tests/e2e/phase3_stack.py</files>
  <behavior>
    - With `connect` stopped, three probe rows for one aggregate committed in three transactions produce no message for 10 s; after `connect` starts and the task is RUNNING, the three event ids arrive on one partition in commit order
    - Five probe rows delivered, then `docker compose kill connect` within 5 s, restart, one more row: all six event ids are present on the topic (no loss); the number of extra copies is logged and not asserted to be zero (at-least-once on the wire, research F4)
    - A probe row committed before a connector first registered is delivered by the initial snapshot (`snapshot.mode=initial`): exercised by deleting and re-registering the scheduling connector with a fresh `slot.name` suffix in a disposable copy of the config, then removing that connector and dropping its slot in teardown
  </behavior>
  <read_first>
    - .planning/phases/03-event-driven-system-observability/03-RESEARCH.md ("Kill-between-commit-and-publish tests (shape)", Pitfalls 3, 4)
    - tests/e2e/test_phase3_outbox_cdc.py and tests/e2e/phase3_stack.py (Task 1)
  </read_first>
  <action>
    Add the three scenarios above to `tests/e2e/test_phase3_outbox_cdc.py` using the helpers (all through `compose`, so only the `smarthealth` project is touched). Each test uses its own random aggregate id and the probe topic. Every test that stops or kills `connect` restores it in a `finally` block and waits for all four connector tasks to be RUNNING again before returning, so later tests and other plans' executors see a healthy relay. The snapshot test's disposable connector uses a distinct name and slot (for example `scheduling-outbox-snapshot-probe` / `dbz_snapshot_probe`) and its teardown deletes the connector through the REST API and runs `SELECT pg_drop_replication_slot('dbz_snapshot_probe')` as the superuser so no orphaned slot retains WAL (Pitfall 4). Add any small helper this needs to `phase3_stack.py`. Record in the SUMMARY the duplicate count observed after the hard kill: it is the evidence for the "at-least-once on the wire" statement in the docs.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -x -q</automated>
    <fails_when>pytest exits non-zero, an event id is missing after the hard kill, the stop/start order differs from commit order, or `pg_replication_slots` still lists `dbz_snapshot_probe` after the run.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -q` exits 0 with at least 6 passed
    - after the run, `docker compose -f infra/docker-compose.yml exec -T postgres psql -U smarthealth -d postgres -Atc "select count(*) from pg_replication_slots where slot_name = 'dbz_snapshot_probe'"` prints 0
    - after the run, all four connector tasks report RUNNING
    - the SUMMARY records the observed duplicate count after the hard kill
  </acceptance_criteria>
  <done>Delivery survives Connect being stopped or hard-killed with no loss and per-aggregate order, and the at-least-once behaviour is measured and recorded.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Postgres WAL -> Kafka Connect | a replication role reads every change in four databases' outbox tables |
| Compose network -> Connect REST API | anyone who can reach :8083 can create a connector that reads any database it can log in to |
| outbox table -> Kafka topics | whatever lands in the outbox is published and replayed forever |
| host secret files -> containers | Debezium and Grafana read-only passwords |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-3-05 | Elevation of Privilege | Kafka Connect REST API | high | mitigate | No host port for :8083 (static test asserts it); Compose-network only; the connector role can only replicate and SELECT outbox tables |
| T-3-06 | Information Disclosure | Debezium database credential | high | mitigate | Compose secret file + FileConfigProvider reference in the connector JSON (static test requires the `${file:` form); db-init reads it via `\getenv`; never in env values, images, logs or the Connect config topic |
| T-3-07 | Elevation of Privilege | `debezium` Postgres role | high | mitigate | LOGIN REPLICATION only, CONNECT on the four outbox databases only, SELECT on `outbox` only; e2e test proves `permission denied` on `appointments` |
| T-3-08 | Tampering | outbox rows / topics | medium | mitigate | Only the owning service role can INSERT (table owner); Debezium is read-only; values are registry-framed Avro that consumers validate; local broker has no ACLs (accepted for dev; production note: SASL/TLS + topic ACLs, documented in 03-14) |
| T-3-09 | Denial of Service | inactive replication slot retaining WAL | medium | mitigate | `max_slot_wal_keep_size=2GB`, `heartbeat.interval.ms=10000`, probe-slot teardown in tests, slot panel in plan 03-12, ops note in 03-14 |
| T-3-10 | Tampering | live Schema Registry | medium | mitigate | Only the `schema-init` job registers; services run strict serializers; BACKWARD checked before every registration |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | This plan installs no package; Docker images are pinned by exact tag (`quay.io/debezium/connect:3.7.1.Final`, `confluentinc/cp-kafka:7.6.13`) |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/infra -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -q`
- `scripts/compose_healthcheck.sh` passes and the existing e2e suite still passes: `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- tjo-* containers unchanged (before/after record in the SUMMARY)
</verification>

<success_criteria>
- Logical decoding on, four RUNNING connector tasks, pre-created topics, schemas registered by the job only
- Delivery proven across Connect stop/start and hard kill with per-aggregate order and no loss
- Least-privilege replication role with a file-provided password
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-02-SUMMARY.md` when done
</output>
