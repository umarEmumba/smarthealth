---
phase: 03-event-driven-system-observability
plan: 05
type: execute
wave: 3
depends_on: ["03-01", "03-03"]
files_modified:
  - infra/docker-compose.yml
  - infra/postgres-init/create-service-databases.sh
  - infra/kafka/create-topics.sh
  - infra/connect/register-connectors.sh
  - infra/connect/connectors/scheduling-outbox.json
  - infra/.env.example
  - libs/events/migration.py
  - services/scheduling/migrations/versions/scheduling_0006_outbox.py
  - tests/e2e/phase3_stack.py
  - tests/e2e/test_phase3_outbox_cdc.py
  - tests/infra/__init__.py
  - tests/infra/test_cdc_config.py
autonomous: true
requirements: [EVENT-01, EVENT-02]

estimate:
  tokens: 105000
  raw_tokens: 105000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "Postgres runs with wal_level=logical on the existing data volume; no data is lost and no tjo-* container is restarted (D-01)"
    - "A row committed to an outbox table reaches its Kafka topic through Debezium with key = aggregate id and headers id, eventType, traceparent, schemaVersion, and its value decodes against the live Schema Registry (D-01, D-05, D-06)"
    - "Every outbox database gets its own connector (scheduling here; patient and provider in plan 03-08, visit in plan 03-09, each registered by name with this plan's script) whose task (not only the connector) is RUNNING, checked by the registration script and by tests parametrized over the connector files present (Pitfall 2)"
    - "Events committed while Connect is stopped are delivered, in order per aggregate, after it starts; a hard kill of Connect loses nothing (duplicates on the wire are allowed and counted, research F4)"
    - "The Debezium role can replicate and read only the outbox tables; its password never appears in a connector config, an env value or an image (Compose secret + FileConfigProvider)"
    - "Schemas are registered on the live registry only by the schema-init job; topics are pre-created with explicit partitions and unlimited retention"
  artifacts:
    - path: infra/connect/register-connectors.sh
      provides: "idempotent PUT of every connector JSON (or only the names given) and a task-state poll"
      contains: "tasks"
    - path: libs/events/migration.py
      provides: "create_outbox(op, service_role) / drop_outbox(op): table, publication dbz_outbox, guarded SELECT grant to debezium"
      contains: "CREATE PUBLICATION dbz_outbox"
    - path: tests/e2e/test_phase3_outbox_cdc.py
      provides: "probe-row delivery, Connect stop/start ordering, hard-kill no-loss, slot/publication/privilege checks"
    - path: tests/e2e/phase3_stack.py
      provides: "compose, connector_status, wait_connectors_running, register_connector, consume_matching, kill/stop/start/wait_healthy helpers"
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
Stand up the CDC relay (D-01): Postgres logical decoding, Kafka Connect with Debezium, pre-created topics, deploy-time schema registration, the reusable outbox migration helper and connector registration script, and the first outbox database end to end (scheduling). After this plan, any row Scheduling commits to its `outbox` reaches Kafka with no relay code in the service; plans 03-08 and 03-09 add the patient, provider and visit outboxes with the same helper and script when they make those services producers.

Purpose: EVENT-02 (no dual write) and the delivery half of roadmap success criterion 1: "never lost" is proven here against Connect stop/start and a hard kill; "never duplicated" is defined honestly as effect-level exactly-once and proven by the consumer plans (03-04, 03-09, 03-10, 03-11, 03-14, 03-17).

Output: Compose services `connect`, `kafka-init`, `schema-init`, `connect-init`; Postgres flags; db-init roles for Debezium (CONNECT on all four outbox databases from the start), analytics and Grafana read-only; the scheduling outbox migration; the scheduling connector; CDC e2e and static config tests that automatically cover every later connector file; `tests/e2e/phase3_stack.py` helpers reused by later e2e plans.

Decisions honoured: D-01 (CDC, not a poller; one connector per outbox database; `wal_level=logical` keeps the volume; init and tests check task state), D-01 detail (Connect on `quay.io/debezium/connect:3.7.1.Final`, a documented deviation from the "same CP line" rule; broker and registry stay 7.6.13), D-05 detail (`BinaryDataConverter` passthrough, registration by `schema-init`), D-06 revised (topic per aggregate via `route.by.field=aggregatetype`, key = `aggregateid`), D-10 revised (outbox insert-only, it is the replay source; `snapshot.mode=initial` covers rows written before the connector first starts). Claude's discretion: 3 partitions, `retention.ms=-1`, `max_slot_wal_keep_size=2GB`, `heartbeat.interval.ms=10000`, Connect heap capped at 768 MiB.

Live stack rules (every Compose command in this plan): use only `docker compose -f infra/docker-compose.yml ...` (project `smarthealth`); never `down -v`, `system prune`, `volume rm`, or any `docker restart`/`kill`/`stop` not scoped to a `smarthealth` Compose service. Containers named `tjo-*` belong to another project: record `docker ps --filter name=tjo- --format '{{.Names}} {{.Status}}'` before the first Compose change and compare after the last; none may show a fresh "Up N seconds". Migrations run at container start (`alembic upgrade head` in each image's CMD) on the existing volumes; host-side tests also run `.venv/bin/alembic -c services/<svc>/migrations/alembic.ini upgrade head`. This plan rebuilds every application image (plan 03-01 changed `pyproject.toml`) and recreates `postgres` once (Pitfall 14: every service connection blips; run `scripts/compose_healthcheck.sh` after). Wave 3 coupling: this is the only wave-3 plan that touches Postgres, Kafka, Connect or an application container. Plan 03-06 runs alongside but uses only the in-process Temporal time-skipping environment, a throwaway RabbitMQ vhost and Mongo; this plan does not change the `rabbitmq` or `mongo` service definitions, so `up -d --build` does not recreate them.
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
@.planning/phases/03-event-driven-system-observability/03-03-SUMMARY.md
@libs/events/outbox.py
@libs/events/catalog.py
@libs/events/register.py
@tests/e2e/phase2_stack.py
</context>

## Artifacts this phase produces

| Symbol / path | Kind | Notes |
|---|---|---|
| Compose `postgres` command | infra | adds `-c wal_level=logical -c max_slot_wal_keep_size=2GB` |
| Compose `connect` | infra | `quay.io/debezium/connect:3.7.1.Final` (the image approved in plan 03-01 Task 1), no host port, `CONNECT_CONFIG_PROVIDERS=file`, secret `debezium.properties`, read-only mount `./connect:/connect-init:ro` (so a later plan registers its connector by name inside the container), heap capped, healthcheck `curl -fs http://localhost:8083/connectors` |
| Compose `kafka-init` | one-shot | `confluentinc/cp-kafka:7.6.13`, runs `infra/kafka/create-topics.sh` |
| Compose `schema-init` | one-shot | scheduling image, `python -m libs.events.register`; added to the `x-app-depends-on` anchor |
| Compose `connect-init` | one-shot | Debezium image, runs `infra/connect/register-connectors.sh` after `scheduling`, `visit`, `provider` and `patient` are healthy (it registers whichever connector files exist, so later plans need no Compose change) |
| Connector registration by name | command | `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh <name>` (used by plans 03-08 and 03-09; different names may be registered concurrently) |
| `x-app-env` additions | infra | `KAFKA_BOOTSTRAP_SERVERS: kafka:29092`, `SCHEMA_REGISTRY_URL: http://schema-registry:8081` |
| Compose secrets | infra | `debezium_db_password` (`./secrets/infra/debezium.properties`), `grafana_ro_password` (`./secrets/infra/grafana_ro.secret`) |
| db-init additions | infra | role `debezium` (LOGIN REPLICATION, CONNECT on scheduling/visit/provider/patient DBs), `analytics` in `SERVICES` (`analytics_svc`/`analytics_db`, `ANALYTICS_DB_PASSWORD`), role `grafana_ro` (CONNECT + default SELECT in analytics_db) |
| Topics | Kafka | `smarthealth.{appointment,visit,provider,patient,cdcprobe}.events` (3 partitions), `smarthealth.dlq.{visit-downstream,notification-trigger,analytics-projection}`, all `retention.ms=-1` |
| Connectors | Connect | `scheduling-outbox` (slot `dbz_scheduling`) here; `patient-outbox` (`dbz_patient`) and `provider-outbox` (`dbz_provider`) in plan 03-08; `visit-outbox` (`dbz_visit`) in plan 03-09; publication `dbz_outbox` per DB |
| `libs/events/migration.py` | module | `create_outbox(op)`, `drop_outbox(op)` (reused by plans 03-08 and 03-09) |
| Migrations | Alembic | `scheduling_0006` (outbox table, publication, guarded grant) |
| `tests/e2e/phase3_stack.py` | test helper | `compose()`, `connector_status()`, `connector_names()` (from the JSON files present), `wait_connectors_running(names=None)` (None = every connector file present), `register_connector(name)`, `consume_matching()`, `kill_service()`, `stop_service()`, `start_service()`, `wait_service_healthy()`, `live_registry()`, `insert_probe_rows()` |
| `NON_OUTBOX_PROBE_TABLE` | test constant | `tests/e2e/test_phase3_outbox_cdc.py`: database -> a table the debezium role must NOT read (`scheduling_db: appointments`, `patient_db: patients`, `provider_db: providers`, `visit_db: visits`), used per connector file present |

<tasks>

<task type="tracer">
  <name>Task 1: Tracer — a probe row committed to scheduling_db.outbox arrives on Kafka through Debezium and decodes against the live registry</name>
  <reversibility rating="costly">D-01: logical replication, replication slots and a Connect service in Compose; switching to a poller later means writing a relay and removing connectors and slots.</reversibility>
  <precondition>`infra/secrets/infra/debezium.properties` and `infra/secrets/infra/grafana_ro.secret` exist (written by plan 03-03 Task 3); check with `test -f`, never print them. The plan 03-01 SUMMARY records the Connect image as approved (Task 1) and the image deviation as accepted (Task 2 user-veto item 1); if either is missing or vetoed, stop.</precondition>
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

    Connect: Compose `connect` with image `quay.io/debezium/connect:3.7.1.Final` and a comment recording the D-01 deviation and its reason (Java 11 in cp-kafka-connect cannot load Debezium 3.x); env `BOOTSTRAP_SERVERS=kafka:29092`, `GROUP_ID=smarthealth-connect`, `CONFIG_STORAGE_TOPIC=smarthealth_connect_configs`, `OFFSET_STORAGE_TOPIC=smarthealth_connect_offsets`, `STATUS_STORAGE_TOPIC=smarthealth_connect_statuses`, the three `*_STORAGE_REPLICATION_FACTOR=1`, `KAFKA_HEAP_OPTS=-Xms256m -Xmx768m`, `CONNECT_CONFIG_PROVIDERS=file`, `CONNECT_CONFIG_PROVIDERS_FILE_CLASS=org.apache.kafka.common.config.provider.FileConfigProvider`; secret `debezium_db_password` targeted at `debezium.properties`; read-only volume `./connect:/connect-init:ro` (lets a later plan run the registration script for one connector inside this container); NO `ports:` entry (the REST API stays on the Compose network); healthcheck `curl -fs http://localhost:8083/connectors`; depends on kafka healthy and kafka-init completed; `restart: unless-stopped`.

    Connector (infra/connect/connectors/scheduling-outbox.json): the research Pattern 2 config with `database.dbname=scheduling_db`, `topic.prefix=smarthealth-scheduling`, `slot.name=dbz_scheduling`, and `database.password` set to the FileConfigProvider reference `${file:/run/secrets/debezium.properties:password}`. Keep `publication.autocreate.mode=disabled`, `slot.drop.on.stop=false`, `snapshot.mode=initial`, `snapshot.locking.mode=none` (assumption A2), `table.include.list=public.outbox`, `BinaryDataConverter`, the `EventRouter` field mapping and `route.topic.replacement=smarthealth.${routedByValue}.events`, header placement `type:header:eventType,traceparent:header:traceparent,schema_version:header:schemaVersion`, `heartbeat.interval.ms=10000`.

    Registration (infra/connect/register-connectors.sh, bash, no `set -x`): optional arguments are connector names; with none, it takes every `/connect-init/connectors/*.json`. For each, `PUT ${CONNECT_URL:-http://connect:8083}/connectors/<basename>/config` (idempotent), then poll `GET /connectors/<name>/status` every 3 s up to 120 s until the connector state and EVERY task state are `RUNNING`; on `FAILED` or timeout print the connector name and the states only (never the trace or config) and exit 1. Compose `connect-init`: same Debezium image, entrypoint `bash /connect-init/register-connectors.sh`, mount `./connect:/connect-init:ro`, depends on connect healthy and on `scheduling`, `visit`, `provider` and `patient` healthy (each one's Alembic run creates its outbox table and publication; only scheduling has one in this plan, and the script registers only the files present), `restart: "no"`. A later plan registers its own connector without touching Compose: `docker compose -f infra/docker-compose.yml exec -T connect bash /connect-init/register-connectors.sh <name>`. If the image does not honour the `CONNECT_CONFIG_PROVIDERS*` variables (task fails with an unresolved placeholder), fix it by mounting a worker properties fragment; never fall back to a literal password in any JSON or env value.

    Test helpers (tests/e2e/phase3_stack.py): fixed-argv subprocess wrappers that always pass `-f infra/docker-compose.yml` from the repo root (`compose(*args)`, `connector_status(name)` via `compose exec -T connect curl -s ...`, `connector_names()` (basenames of the connector JSON files present), `wait_connectors_running(names=None, timeout=120)` (None = every connector file present), `register_connector(name)` (runs the registration script for one name inside `connect`), `kill_service`, `stop_service`, `start_service`, `wait_service_healthy`), `live_registry()` (client on `http://localhost:8081`), `consume_matching(topic, predicate, timeout, min_count=1)` (host `confluent_kafka.Consumer` on `localhost:9092`, unique group id, earliest, returns matching messages with key, headers, partition, offset and decoded value), and `insert_probe_rows(db_url, rows)` (serializes AppointmentBooked samples and inserts them with `aggregatetype='cdcprobe'` so they route to `smarthealth.cdcprobe.events`, which no consumer reads). Reuse `require_stack`'s skip/fail convention.

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
    - `curl -s http://localhost:8081/subjects` lists 10 subjects starting with `com.smarthealth.events.` (registered by `schema-init` from plan 03-03's catalogue)
    - `grep -c "8083:" infra/docker-compose.yml` is 0 and `grep -c "quay.io/debezium/connect:3.7.1.Final" infra/docker-compose.yml` is at least 1 (or the replacement tag recorded in the plan 03-01 SUMMARY)
    - the tjo-* status lines recorded before and after are identical apart from uptime growth (no container restarted)
    - the tracer test passes
  </acceptance_criteria>
  <done>A committed outbox row in scheduling_db reaches Kafka through Debezium with the agreed key, headers and Avro value; the stack is healthy on the existing volumes.</done>
</task>

<task type="auto">
  <name>Task 2: Static connector/Compose checks and per-connector task-state, slot and least-privilege tests (cover every later connector file automatically)</name>
  <files>tests/infra/__init__.py, tests/infra/test_cdc_config.py, tests/e2e/test_phase3_outbox_cdc.py</files>
  <read_first>
    - infra/connect/connectors/scheduling-outbox.json, infra/docker-compose.yml, infra/connect/register-connectors.sh (Task 1)
    - tests/e2e/test_phase3_outbox_cdc.py and tests/e2e/phase3_stack.py (Task 1 helpers)
    - infra/postgres-init/create-service-databases.sh (the debezium grants from Task 1)
  </read_first>
  <action>
    Static test (tests/infra/test_cdc_config.py, no stack needed; parse YAML with `yaml.safe_load` if PyYAML is importable, else fall back to text checks), parametrized over every connector JSON present in `infra/connect/connectors/` (so the files plans 03-08 and 03-09 add are checked without editing this test): each has the required keys and values listed in Task 1, a `database.password` that starts with `${file:`, `table.include.list=public.outbox`, and across files the `slot.name`, `database.dbname` and `topic.prefix` values are distinct. The Compose file pins `cp-kafka` and `cp-schema-registry` to the same `7.6.13` tag, pins Connect to the approved Debezium image, gives `connect` no `ports`, mounts `./connect` read-only into `connect` and `connect-init`, puts `wal_level=logical` in the postgres command, makes `connect-init` depend on the four producing APIs, and declares the two new secrets with `./secrets/infra/` paths.

    E2e tests in `tests/e2e/test_phase3_outbox_cdc.py` (marker e2e), each parametrized with ids = connector names from `connector_names()` so a later plan can select its own with `-k`: `test_connector_tasks_running[<name>]` (every `tasks[*].state == "RUNNING"`, connector state alone not accepted); `test_slots_and_publications[<name>]` (the connector's slot is in `pg_replication_slots` with `active = true`, and `outbox` is in `pg_publication_tables` of its database); `test_debezium_role_least_privilege[<name>]` (connect as `debezium` to the connector's database with the password read from the secret file, never printed: `SELECT count(*) FROM outbox` succeeds and `SELECT 1 FROM <NON_OUTBOX_PROBE_TABLE[db]>` gets `permission denied`). Define `NON_OUTBOX_PROBE_TABLE` for all four databases now (artifacts table); an entry is exercised only when its connector file exists.
  </action>
  <verify>
    <automated>.venv/bin/python -m pytest tests/infra/test_cdc_config.py -x -q && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "connector_tasks or privilege or slots" -x -q</automated>
    <fails_when>Either pytest run exits non-zero, the scheduling connector task is not RUNNING, its slot or publication is missing, or the debezium role can read `appointments`.</fails_when>
  </verify>
  <acceptance_criteria>
    - `.venv/bin/python -m pytest tests/infra/test_cdc_config.py -q` exits 0
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -k "scheduling-outbox" -q` runs the three parametrized checks for `scheduling-outbox` and all pass
    - `grep -c "NON_OUTBOX_PROBE_TABLE" tests/e2e/test_phase3_outbox_cdc.py` is at least 1
  </acceptance_criteria>
  <done>The scheduling relay is checked for task state, slot, publication and least privilege, and the same checks apply to every later connector file without editing the tests.</done>
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
    Add the three scenarios above to `tests/e2e/test_phase3_outbox_cdc.py` using the helpers (all through `compose`, so only the `smarthealth` project is touched). Each test uses its own random aggregate id and the probe topic. Every test that stops or kills `connect` restores it in a `finally` block and waits for every registered connector's tasks to be RUNNING again (`wait_connectors_running()`) before returning, so later tests and later plans see a healthy relay. The snapshot test's disposable connector uses a distinct name and slot (for example `scheduling-outbox-snapshot-probe` / `dbz_snapshot_probe`) and its teardown deletes the connector through the REST API and runs `SELECT pg_drop_replication_slot('dbz_snapshot_probe')` as the superuser so no orphaned slot retains WAL (Pitfall 4). Add any small helper this needs to `phase3_stack.py`. Record in the SUMMARY the duplicate count observed after the hard kill: it is the evidence for the "at-least-once on the wire" statement in the docs.
  </action>
  <verify>
    <automated>SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -x -q</automated>
    <fails_when>pytest exits non-zero, an event id is missing after the hard kill, the stop/start order differs from commit order, or `pg_replication_slots` still lists `dbz_snapshot_probe` after the run.</fails_when>
  </verify>
  <acceptance_criteria>
    - `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -q` exits 0 with at least 6 passed
    - after the run, `docker compose -f infra/docker-compose.yml exec -T postgres psql -U smarthealth -d postgres -Atc "select count(*) from pg_replication_slots where slot_name = 'dbz_snapshot_probe'"` prints 0
    - after the run, every registered connector's tasks report RUNNING
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
| T-3-07 | Elevation of Privilege | `debezium` Postgres role | high | mitigate | LOGIN REPLICATION only, CONNECT on the four outbox databases only, SELECT on `outbox` only (guarded grant inside `create_outbox`, so each later outbox migration grants exactly that table); parametrized e2e test proves `permission denied` on a non-outbox table for every connector database present |
| T-3-08 | Tampering | outbox rows / topics | medium | mitigate | Only the owning service role can INSERT (table owner); Debezium is read-only; values are registry-framed Avro that consumers validate; local broker has no ACLs (accepted for dev; production note: SASL/TLS + topic ACLs, documented in 03-18) |
| T-3-09 | Denial of Service | inactive replication slot retaining WAL | medium | mitigate | `max_slot_wal_keep_size=2GB`, `heartbeat.interval.ms=10000`, probe-slot teardown in tests, slot panel in plan 03-16, ops note in 03-18 |
| T-3-10 | Tampering | live Schema Registry | medium | mitigate | Only the `schema-init` job registers; services run strict serializers; BACKWARD checked before every registration |
| T-3-SC | Tampering | npm/pip/cargo installs | low | accept | This plan installs no package; Docker images are pinned by exact tag (`quay.io/debezium/connect:3.7.1.Final`, approved in plan 03-01 Task 1, and `confluentinc/cp-kafka:7.6.13`) |
</threat_model>

<verification>
- `.venv/bin/python -m pytest tests/infra -q`
- `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase3_outbox_cdc.py -m e2e -q`
- `scripts/compose_healthcheck.sh` passes and the existing e2e suite still passes: `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/python -m pytest tests/e2e/test_phase2_flows.py -m e2e -q`
- `.venv/bin/python -m pytest tests -m "not e2e" -q` (no OTEL_SDK_DISABLED)
- tjo-* containers unchanged (before/after record in the SUMMARY)
</verification>

<success_criteria>
- Logical decoding on, the scheduling connector task RUNNING with checks that cover every later connector, pre-created topics, schemas registered by the job only
- Delivery proven across Connect stop/start and hard kill with per-aggregate order and no loss
- Least-privilege replication role with a file-provided password
</success_criteria>

<output>
Create `.planning/phases/03-event-driven-system-observability/03-05-SUMMARY.md` when done
</output>
