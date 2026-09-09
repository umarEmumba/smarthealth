---
phase: 01-foundation-core-services
plan: 01
subsystem: infra
tags: [fastapi, sqlalchemy, pymongo, redis, docker-compose, pytest, kafka, temporal, otel, pydantic-settings]

# Dependency graph
requires: []
provides:
  - "Python 3.12 project skeleton (smarthealth/, libs/, services/, services/identity/, tests/) with exact CLAUDE.md-pinned dependencies"
  - "smarthealth/config.py — pydantic-settings Settings class + module-level `settings` instance (database_url, mongo_url, mongo_db_name, redis_url, jwt_secret/jwt_algorithm/access_token_expire_minutes/refresh_token_expire_days, otlp_endpoint)"
  - "infra/docker-compose.yml — full local stack (Postgres, MongoDB, Redis, RabbitMQ, Kafka KRaft, Schema Registry, Temporal auto-setup + UI, Jaeger, Prometheus, Grafana, api scaffold) with health-check gating (OBS-04)"
  - "scripts/compose_healthcheck.sh — OBS-04 smoke script"
  - "Dockerfile — python:3.12-slim API image, gunicorn + uvicorn worker"
  - "tests/conftest.py — db_session, mongo_client, redis_client, client fixtures"
  - "pytest infrastructure — asyncio_mode=auto, tests/test_smoke.py"
affects: [01-02-data-model-migrations, 01-03-tracer-auth-rbac-audit, 01-04-clinic-department-crud, 01-05-patient-provider-availability-crud, 01-06-dashboard-reconciliation]

# Actuals (#2632)
actuals:
  tokens: 6235
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: [fastapi 0.141.1, uvicorn 0.52.4, gunicorn, pydantic 2.13.5, pydantic-settings 2.15.0, sqlalchemy 2.0.52, asyncpg 0.31.0, alembic 1.19.2, pymongo 4.18.0, redis 8.1.0, python-jose 3.5.0, passlib+bcrypt, httpx, structlog 26.1.0, opentelemetry (api/sdk/otlp/instrumentation-fastapi) 1.44.0/0.65b0, prometheus-fastapi-instrumentator 8.1.0, pytest, pytest-asyncio]
  patterns:
    - "Single pydantic-settings Settings module imported by every process (API + future Celery/Temporal workers)"
    - "One env_file convention: root .env for app config, infra/.env for Docker Compose — variable names kept identical between the two"
    - "Host-port overrides via ${SERVICE}_HOST_PORT env vars in docker-compose.yml so local port collisions never require touching internal Compose-network wiring"
    - "Native readiness-tool healthchecks only (pg_isready, mongosh, redis-cli, rabbitmq-diagnostics, kafka-broker-api-versions, curl, tctl) — never curl/wget assumed present on slim images without verifying"

key-files:
  created:
    - pyproject.toml
    - smarthealth/config.py
    - infra/docker-compose.yml
    - infra/.env.example
    - infra/prometheus.yml
    - Dockerfile
    - scripts/compose_healthcheck.sh
    - tests/conftest.py
    - tests/test_smoke.py
    - .env.example
    - .gitignore
    - README.md
  modified: []

key-decisions:
  - "Pinned structlog to 26.1.0 (live PyPI query this session) — closes RESEARCH.md assumption A5 (project-wide STACK.md had left it unpinned as 'latest')"
  - "redis:8.2-trixie (CLAUDE.md pin) does not exist on Docker Hub — substituted redis:8.2-bookworm, same 8.2 minor line, correct Debian codename (verified live against the registry)"
  - "temporalio/auto-setup:1.31.2 (CLAUDE.md pin) does not exist on Docker Hub — substituted 1.29.7, the highest available auto-setup tag (verified live against the registry); Temporal auto-setup is labeled non-production per CLAUDE.md's own 'What NOT to Use' table, as flagged in the plan's objective for Plan 06's dashboard reconciliation"
  - "Removed DYNAMIC_CONFIG_FILE_PATH=config/dynamicconfig/development-sql.yaml from the temporal service — that file does not exist in the 1.29.7 auto-setup image and caused the container to exit 1; the image's default docker.yaml config is used instead"
  - "All service host ports made overridable via ${SERVICE}_HOST_PORT env vars (default = conventional port) so local port collisions with other running services never require editing the compose file or changing internal Compose-network DNS wiring"

patterns-established:
  - "Pattern: env_file duality — root .env.example documents app-facing config (localhost URLs for running the app outside Docker); infra/.env.example documents the same variable names resolved to Compose-internal service DNS names (postgres:5432, mongo:27017, etc.)"
  - "Pattern: every docker-compose service another service depends_on via condition: service_healthy has a healthcheck using that image's own native readiness tool, verified present in the image before being relied upon"

requirements-completed: [OBS-04]

coverage:
  - id: D1
    description: "Python 3.12 project installs with exact CLAUDE.md-pinned dependency versions and no forbidden libraries (motor, aioredis, psycopg2, opentelemetry-exporter-jaeger)"
    requirement: "OBS-04"
    verification:
      - kind: other
        ref: "pip install -e '.[dev]' (exit 0); pip show fastapi/sqlalchemy/pymongo confirms 0.141.1/2.0.52/4.18.0; grep -c forbidden-libs pyproject.toml == 0"
        status: pass
    human_judgment: false
  - id: D2
    description: "smarthealth/config.py exposes a pydantic-settings Settings object with database_url/mongo_url/redis_url/jwt_*/otlp_endpoint, importable with no hardcoded secrets"
    verification:
      - kind: unit
        ref: "tests/test_smoke.py#test_settings_import"
        status: pass
    human_judgment: false
  - id: D3
    description: "docker compose -f infra/docker-compose.yml up -d brings up Postgres, MongoDB, Redis, RabbitMQ, Kafka (KRaft, no ZooKeeper), Schema Registry (matched CP 7.6.13), Temporal (auto-setup, labeled non-production) + Temporal UI, Jaeger, Prometheus, Grafana — every service with a healthcheck reports healthy, and a second up -d run converges idempotently"
    requirement: "OBS-04"
    verification:
      - kind: other
        ref: "docker compose -f infra/docker-compose.yml config (exit 0); scripts/compose_healthcheck.sh (7/7 healthchecked services healthy); second `up -d` run — no volume/network errors"
        status: pass
    human_judgment: false
  - id: D4
    description: "pytest infrastructure runs async tests via asyncio_mode=auto and exposes the four shared fixtures (db_session, mongo_client, redis_client, client) every later plan's tests depend on by name"
    verification:
      - kind: unit
        ref: "tests/test_smoke.py (2 passed, pytest tests/ -x -k 'not integration')"
        status: pass
      - kind: integration
        ref: "throwaway fixture validation (removed post-verification) — db_session real SQL round-trip with rollback, mongo_client insert/find, redis_client set/get, client returns stub app's /openapi.json"
        status: pass
    human_judgment: false

duration: 40min
completed: 2026-09-09
status: complete
---

# Phase 1 Plan 1: Scaffold, Compose, Pytest Summary

**Python 3.12 project scaffolding with exact CLAUDE.md-pinned dependencies, a full 11-service Docker Compose stack (Postgres/Mongo/Redis/RabbitMQ/Kafka-KRaft/Schema-Registry/Temporal/Jaeger/Prometheus/Grafana) with native-tool health checks satisfying OBS-04, and pytest async infrastructure with four shared fixtures.**

## Performance

- **Duration:** 40 min
- **Started:** 2026-09-09T11:26:11Z
- **Completed:** 2026-09-09T12:06:00Z
- **Tasks:** 3
- **Files modified:** 17

## Accomplishments
- Python project installs cleanly with every dependency pinned to the exact CLAUDE.md version (fastapi 0.141.1, sqlalchemy 2.0.52, pymongo 4.18.0, redis 8.1.0, etc.) and zero forbidden libraries (motor, aioredis, psycopg2, legacy jaeger exporter)
- Full local-dev stack (11 services) brings up via `docker compose up -d` with every healthcheck-bearing service reporting healthy, verified idempotent on a second run
- Kafka runs in KRaft mode (no ZooKeeper) with a fixed CLUSTER_ID; Schema Registry pinned to the identical Confluent Platform 7.6.13 line and wired to Kafka's internal listener
- Temporal uses the auto-setup convenience image, explicitly labeled non-production in a compose comment per CLAUDE.md's "What NOT to Use" guidance
- pytest async infrastructure (asyncio_mode=auto) with four fixtures (db_session, mongo_client, redis_client, client) independently validated against live containers before being reduced to their final, container-optional form

## Task Commits

Each task was committed atomically:

1. **Task 1: Python project scaffolding + pinned dependencies + shared config** - `d98377b` (feat)
2. **Task 2: Docker Compose full stack with health-check ordering (OBS-04)** - `b119be5` (feat)
3. **Task 3: pytest infrastructure — conftest fixtures + smoke test** - `a978ca6` (test)

**Plan metadata:** (this commit) — docs: complete plan

## Files Created/Modified
- `pyproject.toml` - Project metadata, exact pinned dependencies, `[tool.pytest.ini_options]` with asyncio_mode=auto
- `smarthealth/config.py` - pydantic-settings `Settings` + module-level `settings`
- `smarthealth/__init__.py`, `libs/__init__.py`, `services/__init__.py`, `services/identity/__init__.py` - Package skeleton
- `.env.example` - App-facing env var placeholders (localhost URLs)
- `.gitignore` - .env, __pycache__, .pytest_cache, venvs, IDE/OS cruft
- `README.md` - Placeholder README with local-dev quickstart (full README is an incremental deliverable per ROADMAP.md)
- `infra/docker-compose.yml` - Full 11-service stack with health-check gating
- `infra/.env.example` - Compose-facing env var placeholders (internal service DNS names) + host-port override documentation
- `infra/prometheus.yml` - Minimal scrape config targeting the `api` service
- `Dockerfile` - python:3.12-slim API image, gunicorn + uvicorn worker
- `scripts/compose_healthcheck.sh` - OBS-04 smoke script (asserts every healthchecked service is healthy)
- `tests/__init__.py`, `tests/conftest.py`, `tests/test_smoke.py` - pytest infrastructure

## Decisions Made
- Pinned `structlog==26.1.0` (verified against live PyPI this session) — closes RESEARCH.md's open assumption A5 that project-wide STACK.md had left as unpinned "latest"
- Made every Docker Compose service's host-mapped port overridable via `${SERVICE}_HOST_PORT` env vars (defaulting to the conventional port) — a defensive pattern for local-dev port collisions that doesn't touch internal Compose-network service-DNS wiring, discovered necessary when this execution environment already had unrelated services on ports 3000, 5432, and 6379
- Kept the `api` service in the compose file per the plan's artifact list even though it has no application code yet (`smarthealth.main:app` doesn't exist until Plan 03) — it builds and is included in `depends_on` health gating for postgres/mongo/redis, but is expected to exit until Plan 03 adds the FastAPI app; this does not block or affect the health status of any other service since nothing depends on `api`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] redis:8.2-trixie image tag does not exist on Docker Hub**
- **Found during:** Task 2 (Docker Compose stack)
- **Issue:** CLAUDE.md pins `redis:8.2-trixie`; live registry query this session confirmed that exact tag was never published (only `8.2-bookworm`, `8.2-alpine`, etc. exist in the 8.2 line)
- **Fix:** Substituted `redis:8.2-bookworm` — same 8.2 minor version CLAUDE.md intended, correct Debian codename
- **Files modified:** infra/docker-compose.yml
- **Verification:** `docker compose config` validates; container starts and reports healthy via `redis-cli ping`
- **Committed in:** b119be5 (Task 2 commit)

**2. [Rule 1 - Bug] temporalio/auto-setup:1.31.2 image tag does not exist on Docker Hub**
- **Found during:** Task 2 (Docker Compose stack)
- **Issue:** CLAUDE.md pins `temporalio/auto-setup:1.31.2`; live registry query confirmed the highest published auto-setup tag is `1.29.7` — `1.31.x` was never released for this image
- **Fix:** Substituted `temporalio/auto-setup:1.29.7`
- **Files modified:** infra/docker-compose.yml
- **Verification:** Image pulls; container completes schema setup and the Temporal server starts
- **Committed in:** b119be5 (Task 2 commit)

**3. [Rule 1 - Bug] Temporal DYNAMIC_CONFIG_FILE_PATH referenced a nonexistent file, causing exit 1**
- **Found during:** Task 2 (Docker Compose stack, live `docker compose up` verification)
- **Issue:** `DYNAMIC_CONFIG_FILE_PATH=config/dynamicconfig/development-sql.yaml` was set on the temporal service; that file does not exist inside the `temporalio/auto-setup:1.29.7` image (only `docker.yaml` exists under `config/dynamicconfig/`), so the server process failed with "unable to validate dynamic config" and the container exited 1, blocking `temporal-ui` and `api` from starting (dependency failure)
- **Fix:** Removed the `DYNAMIC_CONFIG_FILE_PATH` env var, letting the image use its own default config
- **Files modified:** infra/docker-compose.yml
- **Verification:** `docker compose up -d` brings the full stack up with temporal reporting healthy via its `tctl cluster health` healthcheck
- **Committed in:** b119be5 (Task 2 commit)

**4. [Rule 3 - Blocking] Host port conflicts prevented `docker compose up -d` from succeeding in this execution environment**
- **Found during:** Task 2 (Docker Compose stack, live verification)
- **Issue:** Ports 5432 (an unrelated `tjo-postgres` container) and 3000 (an unrelated `node` process) were already bound on the host, causing Docker's port-publish step to fail
- **Fix:** Made every service's host-mapped port overridable via a `${SERVICE}_HOST_PORT:-<default>}` pattern in docker-compose.yml, with the conventional port as the default (no behavior change for a clean host) — documented the override variables in infra/.env.example
- **Files modified:** infra/docker-compose.yml, infra/.env.example
- **Verification:** Full stack (all 11 services) starts successfully with `POSTGRES_HOST_PORT=5433 REDIS_HOST_PORT=6380 GRAFANA_HOST_PORT=3001` set in this environment's local infra/.env; every healthchecked service reports healthy
- **Committed in:** b119be5 (Task 2 commit)

---

**Total deviations:** 4 auto-fixed (3 image/config bugs preventing `docker compose up` from working at all, 1 blocking host-port conflict specific to this execution environment)
**Impact on plan:** All four fixes were required for OBS-04's actual success criterion — "docker compose up brings up the full stack with passing health checks" — to be true. Deviations 1-3 are corrections to stale/incorrect version pins and config paths in CLAUDE.md/the plan itself (verified against the live Docker Hub registry, not assumed); deviation 4 is a defensive, opt-in pattern that changes nothing for a clean host. No scope creep — no new services or functionality were added beyond what the plan specified.

## Issues Encountered
- **Python 3.12 was not installed on this machine** (only Python 3.14 was on PATH). Installed via `brew install python@3.12` (no deviation rule needed — this satisfies the task's own `<precondition>` requirement, not a code change).
- **Homebrew's python@3.12 bottle had a broken pyexpat extension** (linked against a `libexpat` symbol version newer than macOS's system `/usr/lib/libexpat.1.dylib` provides), which transitively broke `pip`'s XML-based `platform.mac_ver()` call and caused every pip invocation to fail with a truststore SSL-context crash. Fixed locally for this session by `brew install expat` and passing `DYLD_LIBRARY_PATH=/opt/homebrew/opt/expat/lib` to Python/pip invocations. This is a local machine environment issue, not a project code defect — no compose/pyproject/config file needed to change as a result, and it does not affect a developer machine with a correctly linked Python 3.12 install.

## User Setup Required

None - no external service configuration required beyond what `.claude/CLAUDE.md`'s `user_setup` step already specified (Docker Desktop / Compose v2, which was already installed and used for this plan's verification).

## Next Phase Readiness

Plan 02 (data model + migrations) can now build on: the `smarthealth/config.py` `Settings`/`settings` contract, the package skeleton (`smarthealth/`, `libs/`, `services/`, `services/identity/`), the pinned SQLAlchemy 2.0.52 + asyncpg + Alembic 1.19.2 dependencies, and the `db_session`/`mongo_client`/`redis_client` test fixtures already wired against `settings`. The Postgres/Mongo/Redis containers are live-verified to work with these fixtures end-to-end.

No blockers. One item to track forward per the plan's own flagged assumption: the Temporal auto-setup non-production tradeoff (and its corrected 1.29.7 pin, since 1.31.2 does not exist) should be reflected in Plan 06's dashboard reconciliation.

---
*Phase: 01-foundation-core-services*
*Completed: 2026-09-09*
