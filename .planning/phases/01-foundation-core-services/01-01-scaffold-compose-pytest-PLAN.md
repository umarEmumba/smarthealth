---
phase: 01-foundation-core-services
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - pyproject.toml
  - .env.example
  - .gitignore
  - infra/docker-compose.yml
  - infra/.env.example
  - smarthealth/__init__.py
  - smarthealth/config.py
  - libs/__init__.py
  - services/__init__.py
  - services/identity/__init__.py
  - tests/__init__.py
  - tests/conftest.py
  - tests/test_smoke.py
  - scripts/compose_healthcheck.sh
  - Dockerfile
autonomous: true
requirements: [OBS-04]
user_setup:
  - service: docker
    why: "OBS-04 requires the full stack to run locally via Docker Compose with passing health checks. Docker Desktop / Docker Engine + Compose v2 must be installed and running on the developer machine."
    dashboard_config:
      - task: "Install and start Docker Desktop (macOS) or Docker Engine + Compose v2 (Linux)"
        location: "https://docs.docker.com/get-docker/ — verify with `docker compose version`"

estimate:
  tokens: 62000
  raw_tokens: 40000
  tasks: 3
  confidence: low

must_haves:
  truths:
    - "`docker compose -f infra/docker-compose.yml up -d` brings up Postgres, MongoDB, Redis, RabbitMQ, Kafka (KRaft), Schema Registry, Temporal (auto-setup) + Temporal UI, Jaeger, Prometheus, Grafana and every service that another service depends_on reports a passing health status"
    - "`pip install -e .` (or `pip install` against pyproject) installs the exact CLAUDE.md-pinned versions with no version-resolution error"
    - "`pytest tests/` runs and the smoke test passes (asyncio_mode=auto is honored; an async test executes without a plugin error)"
    - "smarthealth/config.py exposes a pydantic-settings BaseSettings object that loads DB/broker/OTLP URLs from environment with no hardcoded secrets"
    - "the Docker Compose Kafka broker runs in KRaft mode with a fixed CLUSTER_ID env var and no ZooKeeper service is present"
    - "cp-kafka and cp-schema-registry images are pinned to the identical Confluent Platform version line (7.6.13)"
    - "the Temporal service uses the temporalio/auto-setup image and is labeled a non-production convenience in a comment"
    - { statement: "Running `docker compose up` a second time (idempotency of OBS-04 bring-up) converges to the same all-healthy state without erroring on already-created volumes/networks", verification: backstop }
  artifacts:
    - path: pyproject.toml
      provides: "Project metadata + exact CLAUDE.md-pinned dependency set + pytest config (asyncio_mode=auto)"
      contains: "asyncio_mode"
    - path: infra/docker-compose.yml
      provides: "Full local stack with health checks and depends_on: condition: service_healthy gating"
      contains: "service_healthy"
    - path: smarthealth/config.py
      provides: "Shared pydantic-settings BaseSettings config module imported by every process"
      contains: "BaseSettings"
    - path: tests/conftest.py
      provides: "Shared pytest fixtures: async Postgres session (transactional rollback per test), async Mongo client, async Redis client, FastAPI TestClient/AsyncClient"
    - path: Dockerfile
      provides: "API image build (python:3.12, gunicorn -k uvicorn.workers.UvicornWorker)"
      contains: "3.12"
    - path: scripts/compose_healthcheck.sh
      provides: "Smoke script asserting every compose service reaches healthy"
  key_links:
    - from: infra/docker-compose.yml
      to: smarthealth/config.py
      via: "single .env file read by both Compose env_file and pydantic-settings so broker/DB URLs stay identical between container wiring and app config"
      pattern: "env_file"
    - from: tests/conftest.py
      to: infra/docker-compose.yml
      via: "test fixtures connect to the Postgres/Mongo/Redis services the compose stack brings up (or a test-scoped equivalent)"
      pattern: "postgres|mongo|redis"
  prohibitions:
    - "The stack MUST NOT be brought up with any service left without a health status that another service gates on — a silent 'container started but not ready' race is the exact failure OBS-04 exists to prevent"
    - "Secrets (DB passwords, JWT secret, broker credentials) MUST NOT be committed as literal values in docker-compose.yml, pyproject.toml, or any tracked file — only .env.example placeholders are tracked"
---

<objective>
Establish the greenfield SmartHealth repository skeleton: Python 3.12 project scaffolding with the exact CLAUDE.md-pinned dependency set, a Docker Compose file bringing up the FULL mandated stack (Postgres, MongoDB, Redis + Kafka/Schema Registry, RabbitMQ, Temporal, Jaeger, Prometheus, Grafana scaffolding) with passing health checks, the shared pydantic-settings config module, and the pytest test infrastructure every later wave's verify commands depend on.

Purpose: This is Wave 1 — the foundation. No auth, data model, or CRUD exists yet; every subsequent plan's `<automated>` verify command needs pytest configured and (for integration tests) the Postgres/Mongo/Redis containers running. OBS-04 ("full stack runs locally via Docker Compose with health checks") is fully satisfied here. The scaffolding services (Kafka/Temporal/RabbitMQ/observability) must reach a passing health state even though NO application code wires into them this phase (that is Phase 2/3).

Output: pyproject.toml, infra/docker-compose.yml, smarthealth/config.py, Dockerfile, tests/conftest.py + smoke test, .env.example, project package skeleton.

**Flagged assumption (no CONTEXT.md locked this):** Temporal uses the `temporalio/auto-setup:1.31.2` image for local-dev convenience. Per CLAUDE.md's "What NOT to Use" table this image is explicitly NOT for production; it is labeled as a known non-prod tradeoff in a compose comment and must be surfaced in the dashboard (Plan 06) as a documented tradeoff. Recorded per STATE.md blocker "Decide auto-setup vs. production-shaped Temporal deployment."
</objective>

<execution_context>
@~/.claude/gsd-core/workflows/execute-plan.md
@~/.claude/gsd-core/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/STATE.md
@.claude/CLAUDE.md
@.planning/phases/01-foundation-core-services/01-RESEARCH.md
@.planning/research/STACK.md

<interfaces>
<!-- This plan CREATES the foundational contracts every later plan consumes. No existing code to read. -->
<!-- Config module contract that Plans 02-06 import: -->
smarthealth/config.py exports a module-level `settings` instance of a pydantic-settings BaseSettings subclass named `Settings` carrying at minimum:
  - database_url (postgresql+asyncpg://...)
  - mongo_url, mongo_db_name
  - redis_url
  - jwt_secret, jwt_algorithm ("HS256"), access_token_expire_minutes (15), refresh_token_expire_days (7)
  - otlp_endpoint (Jaeger OTLP http/grpc)
tests/conftest.py exports pytest fixtures downstream test files use by name:
  - `db_session` (AsyncSession, rolled back per test)
  - `mongo_client` (AsyncMongoClient against a test db)
  - `redis_client` (redis.asyncio.Redis against a test db index)
  - `client` (httpx.AsyncClient wired to the FastAPI app for integration tests)
</interfaces>
</context>

<artifacts_this_phase_produces>
This plan produces (later plans and executors depend on these exact symbols/paths):
- Package roots: `smarthealth/`, `libs/`, `services/`, `services/identity/`, `tests/`
- `smarthealth/config.py` → `Settings` class + module-level `settings` instance (pydantic-settings BaseSettings)
- `infra/docker-compose.yml` → services: `postgres`, `mongo`, `redis`, `rabbitmq`, `kafka`, `schema-registry`, `temporal`, `temporal-ui`, `jaeger`, `prometheus`, `grafana`, `api`
- `pyproject.toml` → `[project.dependencies]` (exact pins) + `[tool.pytest.ini_options]` with `asyncio_mode = "auto"`
- `Dockerfile` → API image (python:3.12-slim, gunicorn + uvicorn worker)
- `tests/conftest.py` → fixtures `db_session`, `mongo_client`, `redis_client`, `client`
- `scripts/compose_healthcheck.sh` → health smoke script
- Env var names (defined here, consumed everywhere): `DATABASE_URL`, `MONGO_URL`, `MONGO_DB_NAME`, `REDIS_URL`, `JWT_SECRET`, `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`, `REFRESH_TOKEN_EXPIRE_DAYS`, `OTLP_ENDPOINT`, `KAFKA_CLUSTER_ID`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
</artifacts_this_phase_produces>

<tasks>

<task type="auto">
  <name>Task 1: Python project scaffolding + pinned dependencies + shared config</name>
  <read_first>.claude/CLAUDE.md (Technology Stack table + Installation + "What NOT to Use"), .planning/phases/01-foundation-core-services/01-RESEARCH.md (Standard Stack subset + Installation block), .planning/research/STACK.md (version pins)</read_first>
  <precondition>Python 3.12 is available on PATH (`python3.12 --version` succeeds) — required for the mandated Temporal SDK / temporalio (>=3.10), SQLAlchemy async, and pydantic v2 wheels</precondition>
  <reversibility rating="costly">The dependency pin set and project layout are inherited by every later plan and both future phases; changing the package-root structure after CRUD lands touches many import paths.</reversibility>
  <files>pyproject.toml, .gitignore, .env.example, smarthealth/__init__.py, smarthealth/config.py, libs/__init__.py, services/__init__.py, services/identity/__init__.py</files>
  <action>Create a src-less flat-package Python 3.12 project. In pyproject.toml, declare project.dependencies pinned EXACTLY to CLAUDE.md: fastapi[standard]==0.141.1, uvicorn==0.52.4, gunicorn, pydantic==2.13.5, pydantic-settings==2.15.0, sqlalchemy[asyncio]==2.0.52, asyncpg==0.31.0, alembic==1.19.2, pymongo==4.18.0, redis==8.1.0, python-jose[cryptography]==3.5.0, passlib[bcrypt]==1.7.4, bcrypt==5.0.0, httpx, structlog (pin an explicit released version — close RESEARCH assumption A5, do NOT leave "latest"), opentelemetry-api==1.44.0, opentelemetry-sdk==1.44.0, opentelemetry-exporter-otlp==1.44.0, opentelemetry-instrumentation-fastapi==0.65b0, prometheus-fastapi-instrumentator==8.1.0. Dev-deps group: pytest, pytest-asyncio. Do NOT include motor, aioredis, psycopg2, or opentelemetry-exporter-jaeger (forbidden by CLAUDE.md). Add [tool.pytest.ini_options] with asyncio_mode = "auto" and testpaths = ["tests"]. Create smarthealth/config.py defining a pydantic-settings Settings(BaseSettings) with the fields listed in the interfaces block, reading from environment / a single .env, and a module-level `settings = Settings()`. Create empty __init__.py for smarthealth, libs, services, services/identity packages. Create .env.example with placeholder (non-secret) values for every env var name in artifacts_this_phase_produces. Add .gitignore covering .env, __pycache__, .pytest_cache, *.pyc.</action>
  <acceptance_criteria>
    - `pip install -e '.[dev]'` (or equivalent against pyproject) completes with exit 0 and installs fastapi 0.141.1, sqlalchemy 2.0.52, pymongo 4.18.0 (assert via `pip show`)
    - `python -c "from smarthealth.config import settings; print(settings.jwt_algorithm)"` prints "HS256"
    - `grep -c -E 'motor|aioredis|psycopg2|opentelemetry-exporter-jaeger' pyproject.toml` returns 0
    - .env.example exists and contains no real secret values (only placeholders)
  </acceptance_criteria>
  <verify>
    <automated>pip install -e '.[dev]' && python -c "from smarthealth.config import settings; assert settings.jwt_algorithm=='HS256'"</automated>
    <fails_when>pip exits non-zero (version resolution conflict), or the python import raises ModuleNotFoundError / AssertionError</fails_when>
    <automated>grep -vE '^\s*#' pyproject.toml | grep -cE 'motor|aioredis|psycopg2|opentelemetry-exporter-jaeger'</automated>
    <fails_when>the count is greater than 0 — a forbidden library is declared as a dependency</fails_when>
  </verify>
  <done>Project installs with exact pinned versions, config module imports and exposes settings, no forbidden library is declared.</done>
</task>

<task type="auto">
  <name>Task 2: Docker Compose full stack with health-check ordering (OBS-04)</name>
  <read_first>.claude/CLAUDE.md ("Docker Compose Wiring (Local Dev)" table — exact image tags + KRaft/CLUSTER_ID/Schema-Registry/Temporal gotchas), .planning/phases/01-foundation-core-services/01-RESEARCH.md ("Docker Compose Health-Check Ordering" section), .planning/research/STACK.md (Docker Compose Wiring authoritative image pins)</read_first>
  <precondition>`docker compose version` reports Compose v2 (user_setup step complete) — OBS-04 cannot be verified without a running Docker engine</precondition>
  <files>infra/docker-compose.yml, infra/.env.example, Dockerfile</files>
  <action>Create infra/docker-compose.yml with the FULL mandated stack, exact image tags from CLAUDE.md: postgres:15-trixie, mongo:8.0, redis:8.2-trixie, rabbitmq:4.3-management, confluentinc/cp-kafka:7.6.13 (KRaft mode — KAFKA_PROCESS_ROLES=broker,controller + a fixed base64 KAFKA_CLUSTER_ID env var, NO zookeeper service), confluentinc/cp-schema-registry:7.6.13 (SAME CP version line as kafka; SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS must point at the broker's INTERNAL compose-network listener, not localhost:9092), temporalio/auto-setup:1.31.2 (add a comment: "non-production convenience image per CLAUDE.md — production would use temporalio/server + schema-setup"), temporalio/ui:latest, jaegertracing/all-in-one:1.76.0 (expose OTLP 4317/4318 + UI 16686), prom/prometheus:v3.13.3, grafana/grafana:13.0.8, plus an `api` service built from the Dockerfile. Give postgres, mongo, redis, rabbitmq, kafka an explicit healthcheck block using each service's NATIVE readiness tool (postgres: pg_isready; mongo: mongosh --eval "db.adminCommand('ping')"; redis: redis-cli ping; rabbitmq: rabbitmq-diagnostics -q ping; kafka: kafka broker api-versions or a native check) — never curl/wget (may be absent in slim images). The api service declares depends_on: {postgres: {condition: service_healthy}, mongo: {condition: service_healthy}, redis: {condition: service_healthy}}. Use one env_file (infra/.env.example copied to .env by the developer) so URLs match app config. Create the Dockerfile: FROM python:3.12-slim, install the project, CMD gunicorn with uvicorn worker (worker count = CPU cores per CLAUDE.md). Create scripts/compose_healthcheck.sh that runs `docker compose ps --format json` and asserts every service with a healthcheck reports "healthy".</action>
  <acceptance_criteria>
    - `docker compose -f infra/docker-compose.yml config` validates with exit 0 (well-formed YAML + resolvable interpolation)
    - the file contains no `zookeeper` service and contains `KAFKA_CLUSTER_ID`
    - cp-kafka and cp-schema-registry both reference the 7.6.13 tag
    - every service listed under another service's `depends_on: condition: service_healthy` has its own `healthcheck:` block
    - a comment marks temporalio/auto-setup as non-production
  </acceptance_criteria>
  <verify>
    <automated>docker compose -f infra/docker-compose.yml config >/dev/null</automated>
    <fails_when>docker compose config exits non-zero (invalid YAML, unresolved interpolation, or a depends_on condition referencing a service with no healthcheck)</fails_when>
    <automated>grep -c 'zookeeper' infra/docker-compose.yml; grep -c 'KAFKA_CLUSTER_ID' infra/docker-compose.yml; grep -c '7.6.13' infra/docker-compose.yml</automated>
    <fails_when>the zookeeper count is greater than 0, OR the KAFKA_CLUSTER_ID count is 0, OR the 7.6.13 count is less than 2 (both cp-kafka and cp-schema-registry must pin it)</fails_when>
  </verify>
  <done>Compose config validates, KRaft Kafka with CLUSTER_ID and no ZooKeeper, matched CP versions, native healthchecks on all depended-on services, Temporal labeled non-prod.</done>
</task>

<task type="auto">
  <name>Task 3: pytest infrastructure — conftest fixtures + smoke test</name>
  <read_first>.planning/phases/01-foundation-core-services/01-RESEARCH.md ("Validation Architecture" section — Test Framework + Wave 0 Gaps), smarthealth/config.py (settings fields, created in Task 1)</read_first>
  <precondition>Postgres, Mongo, and Redis containers from Task 2 are reachable (integration fixtures connect to them), OR the fixtures fall back to skippable markers — assert reachability with a read-only ping before running integration-marked tests</precondition>
  <files>tests/__init__.py, tests/conftest.py, tests/test_smoke.py</files>
  <action>Create tests/conftest.py exposing the fixtures named in the interfaces block: `db_session` (async SQLAlchemy AsyncSession created via create_async_engine on settings.database_url, wrapped so each test runs in a transaction rolled back at teardown for isolation), `mongo_client` (pymongo AsyncMongoClient against a test database), `redis_client` (redis.asyncio.Redis against a dedicated test db index, flushed between tests), and `client` (httpx.AsyncClient — App wiring is added in Plan 03; for now the fixture may target a minimal FastAPI app stub or be marked so Plan 03 fills the app import). Mark integration-requiring fixtures so a `-k "not integration"` subset can run without live services. Create tests/test_smoke.py with one async test asserting `settings` imports and one trivial async assertion proving asyncio_mode=auto works (an `async def test_...` that awaits `asyncio.sleep(0)` and asserts True). Do NOT put any PHI or real credentials in fixtures.</action>
  <acceptance_criteria>
    - `pytest tests/test_smoke.py -x` collects and passes at least 1 async test with no "async def functions are not natively supported" plugin error
    - conftest defines fixtures literally named db_session, mongo_client, redis_client, client (grep confirms)
    - the fast subset command `pytest tests/ -x -k "not integration"` runs to a pass/skip result without requiring live DB containers
  </acceptance_criteria>
  <verify>
    <automated>pytest tests/test_smoke.py -x -k "not integration"</automated>
    <fails_when>pytest exits non-zero, or the summary line reports "0 passed", or output contains "async def functions are not natively supported"</fails_when>
    <automated>grep -cE 'def db_session|def mongo_client|def redis_client|def client' tests/conftest.py</automated>
    <fails_when>the count is less than 4 — a required shared fixture is missing (later plans depend on all four by name)</fails_when>
  </verify>
  <done>pytest runs async tests via asyncio_mode=auto, the four shared fixtures exist by name, and a fast non-integration subset passes.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| developer machine → committed repo | Secrets must never cross into tracked files; only .env.example placeholders are committed |
| host network → Docker Compose network | Broker/DB listeners exposed on host ports must not use production credentials; local-dev-only passwords via env |

## STRIDE Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation Plan |
|-----------|----------|-----------|----------|-------------|-----------------|
| T-01-01 | Information Disclosure | docker-compose.yml / pyproject.toml / .env | high | mitigate | No literal secrets in tracked files; only .env.example placeholders; .env is gitignored (Task 1 .gitignore) |
| T-01-02 | Tampering | pip package installs | high | mitigate | Package legitimacy audit already completed in RESEARCH.md (all [SUS]→approved, none [SLOP]); exact version pins from CLAUDE.md prevent dependency-confusion via floating versions |
| T-01-03 | Denial of Service | Compose startup race (service accepts TCP before ready) | medium | mitigate | Native-tool healthchecks + depends_on: condition: service_healthy gate the api on Postgres/Mongo/Redis actually being ready (RESEARCH "Docker Compose Health-Check Ordering") |
| T-01-04 | Spoofing | Kafka KRaft misconfig / mixed CP versions | low | accept | Local-dev single-node broker; CLUSTER_ID fixed and CP versions matched to avoid protocol issues; not internet-exposed |
</threat_model>

<verification>
- `pip install -e '.[dev]'` succeeds with exact pinned versions; no forbidden library present.
- `docker compose -f infra/docker-compose.yml config` validates; KRaft Kafka + CLUSTER_ID, no ZooKeeper, matched CP 7.6.13, all depended-on services have healthchecks.
- `docker compose -f infra/docker-compose.yml up -d` then `scripts/compose_healthcheck.sh` reports every healthcheck-bearing service healthy (OBS-04 smoke).
- `pytest tests/ -x -k "not integration"` passes with asyncio_mode=auto.
</verification>

<success_criteria>
OBS-04 satisfied: the full mandated stack comes up via Docker Compose with passing health checks. The Python project installs with exact CLAUDE.md pins, the shared config module and pytest infrastructure exist so every later wave has runnable verify commands.
</success_criteria>

<output>
Create `.planning/phases/01-foundation-core-services/01-01-SUMMARY.md` when done. Record the pinned structlog version chosen (closes RESEARCH assumption A5) and note the Temporal auto-setup non-prod tradeoff for the dashboard (Plan 06).
</output>
