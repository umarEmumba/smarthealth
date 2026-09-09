# Stack Research

**Domain:** Large-scale distributed healthcare-operations backend (SmartHealth Part A) — single FastAPI service/monorepo integrating PostgreSQL, MongoDB, Redis, Celery+RabbitMQ, Kafka+Schema Registry, Temporal, and full observability
**Researched:** 2026-09-09
**Confidence:** MEDIUM (all findings from live web search against current PyPI/DockerHub/GitHub data, cross-checked against 2-3 independent sources per claim; no official-docs MCP/Context7 provider was available in this environment, so treat as MEDIUM not HIGH — verify exact pinned versions against PyPI/DockerHub at implementation time since these are fast-moving projects)

**Stack is mandated, not chosen.** This document does not re-litigate framework/database selection. It answers: which library within each mandated component, at what version, wired together how.

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Python | 3.12 | Language runtime | 3.12 is the practical sweet spot in 2026 — mature enough that every mandated library (Temporal SDK, confluent-kafka, motor→pymongo, OpenTelemetry) fully supports it, faster than 3.11, and one step behind bleeding-edge 3.13 so third-party wheels are reliably available. Avoid 3.9/3.10-only targets — several mandated libs (fastapi, redis-py, aiokafka, temporalio) already require >=3.10. |
| FastAPI | 0.141.1 | Web framework / API layer | Current stable on PyPI as of research date. Pin the exact version you test against — FastAPI ships frequent minor releases; don't float `latest` in production. Install as `fastapi[standard]` to pull in Pydantic v2, Starlette, and the `fastapi` CLI together. |
| Uvicorn | 0.52.4 | ASGI server | Standard FastAPI companion. Run under Gunicorn as a process manager in containers (`gunicorn -k uvicorn.workers.UvicornWorker`), with worker count = CPU cores (not the `(2×cores)+1` formula used for sync WSGI — Uvicorn workers are async and each handles many concurrent requests on one thread). |
| Pydantic | 2.13.5 | Data validation / schemas | FastAPI 0.141.x is built natively on Pydantic v2 — no choice here, but confirms the version to pin. Use for request/response models AND as the shared schema layer between FastAPI request bodies, Kafka event payloads (pre-Avro), and Celery task arguments. |
| pydantic-settings | 2.15.0 | Typed configuration | `BaseSettings` for 12-factor env-based config across all services (DB URLs, broker URLs, Temporal host, OTLP endpoint) — one settings module imported by the API process, Celery workers, and Temporal workers alike. |
| SQLAlchemy | 2.0.52 | PostgreSQL ORM (async) | The 2.0 async API (`AsyncSession`, `create_async_engine`) is the only sane choice for a FastAPI service — sync SQLAlchemy inside `async def` endpoints silently blocks the event loop under load, which is exactly the failure mode this project's reliability/scalability requirements (tens of thousands of concurrent appointment requests) cannot tolerate. |
| asyncpg | 0.31.0 | PostgreSQL async driver | Fastest async Postgres driver for Python, used as SQLAlchemy's async dialect (`postgresql+asyncpg://`). Do not use `psycopg2` under async endpoints. |
| Alembic | 1.19.2 | Schema migrations | Standard SQLAlchemy migration tool; supports SQLAlchemy 2.0 async engines via `run_sync` bridging in `env.py`. Required deliverable per requirements doc ("Database schema ready" in Week 1). |
| PyMongo (async API) | 4.18.0 (use `AsyncMongoClient`, not Motor) | MongoDB driver | See "What NOT to Use" — Motor is deprecated as of May 2025. PyMongo 4.9+ ships a native async API (`pymongo.AsyncMongoClient`) that MongoDB now recommends over Motor. Use PyMongo async directly; do not introduce Motor for a greenfield project in 2026. |
| redis-py | 8.1.0 | Redis client | Single unified client with native asyncio support (`redis.asyncio`) — the old `aioredis` package was merged into redis-py years ago; do not add `aioredis` as a separate dependency. Use for: (1) hot-read cache of provider availability, (2) distributed locks during slot reservation (Redlock pattern) to prevent double-booking races, (3) rate limiting during peak booking windows. |
| Celery | 5.6.3 | Background task workers | Mandated. Requires Python >=3.9. Use for discrete background jobs: notification sends, reminder scheduling, follow-up jobs — NOT for the appointment-booking saga itself (that's Temporal's job; see Architecture rationale below). |
| kombu | 5.6.2 | Celery's messaging abstraction | Installed automatically as a Celery dependency; pin explicitly since it is the layer that actually speaks AMQP to RabbitMQ. Verify version compatibility with your pinned Celery release before upgrading either independently. |
| confluent-kafka | 2.15.0 | Kafka producer/consumer + Schema Registry client | See rationale below — chosen over `aiokafka`. Built on `librdkafka` (C library), production-grade throughput, and ships its own `confluent_kafka.schema_registry` module with `SchemaRegistryClient` + `AvroSerializer`/`ProtobufSerializer` built in (no extra package needed for schema-registry integration). |
| temporalio | 1.32.0 | Temporal Python SDK | Mandated. Requires Python >=3.10. Pair with **Temporal server 1.31.x** (see Docker Compose section) — the SDK and server are versioned independently; don't assume lockstep numbering. |
| opentelemetry-api / opentelemetry-sdk | 1.44.0 | Tracing/metrics instrumentation core | Current stable. Use alongside `opentelemetry-instrumentation-fastapi` for auto-instrumented spans. |
| opentelemetry-instrumentation-fastapi | 0.65b0 | FastAPI auto-instrumentation | Still ships under `0bX` versioning (beta) even though widely used in production — this is normal for OTel Python contrib instrumentation packages, not a signal of immaturity. Pin exactly; the contrib package version tracks the API/SDK minor but isn't identical. |
| opentelemetry-exporter-otlp | match SDK (1.44.0 line) | Exports traces to Jaeger | Jaeger now accepts OTLP natively (gRPC :4317 / HTTP :4318) — do not use the legacy `opentelemetry-exporter-jaeger` package, which is deprecated in favor of OTLP-native ingestion. |
| prometheus-fastapi-instrumentator | 8.1.0 | Prometheus metrics for FastAPI | One-line middleware exposing `/metrics`; current stable on PyPI. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| python-jose[cryptography] | 3.5.0 | JWT encode/decode | Auth token issuance/verification for the 4-role RBAC model (patient, provider, front-desk, admin). |
| passlib[bcrypt] + bcrypt | 1.7.4 / 5.0.0 | Password hashing | Standard pairing for credential storage; pin bcrypt explicitly since passlib's own release cadence lags bcrypt's. |
| httpx | latest matching FastAPI's Starlette version | Async HTTP client | For any outbound calls (billing pre-check stub, notification providers) — never use `requests` inside async endpoints. |
| beanie or plain pymongo async | — | MongoDB ODM (optional) | Only add Beanie (Pydantic-based ODM over PyMongo) if you want schema-like structure over MongoDB documents; otherwise plain `AsyncMongoClient` collections are sufficient for audit-trail/log-shaped documents and keep the dependency surface smaller. Recommendation: start with plain PyMongo async — this project's Mongo use case (audit trails, visit/notification logs) is append-heavy and doesn't need ODM validation duplicating what Pydantic already does at the API boundary. |
| tenacity | latest | Retry/backoff decorator | Wrap Activities (Temporal) and Celery tasks that call external systems, on top of Temporal's/Celery's own retry policies where finer-grained backoff control is needed. |
| structlog | latest | Structured JSON logging | Pair with OpenTelemetry trace/span IDs injected into log records so logs, traces, and metrics correlate — required for the observability maturity evaluation criterion. |

## Installation

```bash
# Core web + validation
pip install "fastapi[standard]==0.141.1" "uvicorn[standard]==0.52.4" "pydantic==2.13.5" "pydantic-settings==2.15.0"

# PostgreSQL (system of record)
pip install "sqlalchemy[asyncio]==2.0.52" "asyncpg==0.31.0" "alembic==1.19.2"

# MongoDB (audit trail / logs / flexible operational docs)
pip install "pymongo==4.18.0"   # use pymongo.AsyncMongoClient — do NOT add motor

# Redis (cache + locks + rate limiting)
pip install "redis==8.1.0"

# Celery + RabbitMQ (background jobs)
pip install "celery==5.6.3" "kombu==5.6.2"

# Kafka + Schema Registry
pip install "confluent-kafka[avro,schemaregistry]==2.15.0"

# Temporal (workflow orchestration)
pip install "temporalio==1.32.0"

# Observability
pip install "opentelemetry-distro==1.44.0" "opentelemetry-exporter-otlp==1.44.0" \
            "opentelemetry-instrumentation-fastapi==0.65b0" \
            "prometheus-fastapi-instrumentator==8.1.0"

# Auth
pip install "python-jose[cryptography]==3.5.0" "passlib[bcrypt]==1.7.4" "bcrypt==5.0.0"

# Dev / quality
pip install -D pytest pytest-asyncio httpx ruff mypy
```

Run the OTel auto-instrumentation bootstrap once per environment image:
```bash
opentelemetry-bootstrap --action=install
# then launch the app as:
opentelemetry-instrument uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## How the Pieces Integrate in One FastAPI Service

**Process topology (still "one clean backend," multiple deployable units from one codebase/monorepo):**

1. **`api` process** — FastAPI app (Uvicorn/Gunicorn). Talks to Postgres (SQLAlchemy async), MongoDB (PyMongo async), Redis, and starts Temporal workflows via the Temporal client SDK. Produces Kafka events for booking/cancellation/reschedule/visit lifecycle. Never runs Celery tasks or Temporal activities in-process.
2. **`temporal-worker` process** — hosts Workflow + Activity definitions (appointment scheduling saga: validate → reserve slot → prevent conflicts → schedule notifications → billing pre-check → confirm). Activities do the actual I/O (DB writes, Kafka publish, Celery task dispatch); Workflows stay deterministic and replay-safe.
3. **`celery-worker` process** — consumes RabbitMQ queues for discrete background jobs (send notification, send reminder, send follow-up) dispatched either directly from API endpoints for non-critical fire-and-forget work, or from Temporal Activities when a step in the durable saga needs to enqueue a job.
4. **`kafka-consumer` process(es)** — separate lightweight consumers (still confluent-kafka) for analytics event processing (appointments over time, cancellation rate, wait time), decoupled from the request path.

**Why both Kafka and Celery/RabbitMQ (per PROJECT.md decision, confirmed by research):** they solve different problems and both being present is correct, not redundant. Kafka is the durable, replayable, multi-consumer event log (booking/visit/analytics events other services or future consumers can replay from). RabbitMQ/Celery is a work queue for "run this one job, once, now" (send this reminder). Don't try to make Kafka do Celery's job (no built-in per-task retry/ack semantics as ergonomic as Celery) or Celery do Kafka's job (no replay log, single consumer group model doesn't fit fan-out analytics).

**Why Temporal AND Celery both exist (not redundant either):** Temporal owns the appointment-booking *saga* — the multi-step, stateful, must-not-corrupt-data workflow with compensation/retry semantics. Celery owns *tasks that are a single step dispatched by that saga* (e.g., "send this reminder") where you don't need durable multi-step state, just reliable at-least-once execution. A Temporal Activity is a fine place to call `celery_app.send_task(...)` when the actual notification-sending logic already lives in Celery-land; don't duplicate notification-sending logic in both.

**Kafka + FastAPI async integration gotcha:** confluent-kafka-python's Consumer API is fundamentally blocking (`consumer.poll()`), and native asyncio consumer support is still an open community discussion (see confluentinc/confluent-kafka-python#1723 — unresolved as of this research). Do not call `consumer.poll()` directly inside an `async def` route or inside the main event loop. Two safe patterns:
- **Producing** (used from API request paths): use the `confluent_kafka.Producer`'s async-friendly callback pattern, or wrap `produce()`/`flush()` in `loop.run_in_executor(None, ...)`.
- **Consuming** (used only in the dedicated `kafka-consumer` process, never in the `api` process): run the consumer loop in its own thread or dedicated process, wrapping the blocking `poll()` call in `run_in_executor` if it must coexist with any asyncio code in that same process; simplest is to keep consumer processes single-purpose and synchronous (plain `while True: msg = consumer.poll(1.0)`), since they don't serve HTTP requests and don't need an event loop at all.

**Schema Registry integration:** use `confluent_kafka.schema_registry.SchemaRegistryClient` + `AvroSerializer` (bundled in the `confluent-kafka` package itself via the `[avro,schemaregistry]` extras) directly in both the `api` producer code and any consumer process. Define one shared `schemas/` directory of `.avsc` files (or Protobuf `.proto`) in the monorepo, imported by both producer and consumer code, so schema drift between services is caught at the Schema Registry compatibility-check level, not at runtime deserialization.

**Temporal + PostgreSQL note:** Temporal server needs its OWN Postgres database(s) for its internal event-history persistence (`temporal` and `temporal_visibility` databases) — separate from your application's `smarthealth` database, even though both can live in the same Postgres container/instance for local dev. Do not point Temporal's persistence config at your application schema.

**MongoDB use case boundary:** use MongoDB specifically for the document-shaped, append-heavy, schema-flexible data called out in PROJECT.md — audit trails, visit/service records, notification logs. Keep transactional entities (patients, providers, appointments, billing) in PostgreSQL for strong consistency; do not let MongoDB become a second system of record for the same entities Postgres already owns (data-consistency risk, and it directly contradicts the project's "strong consistency for transactional entities" decision).

## Docker Compose Wiring (Local Dev)

Recommended image pins (verified against Docker Hub at research time):

| Service | Image:Tag | Notes |
|---------|-----------|-------|
| PostgreSQL | `postgres:15-trixie` (or `postgres:16` if you want newer; both are fine — 15 is the safer, longer-track-record choice for a 3-week assignment) | One instance can host both `smarthealth` and Temporal's `temporal`/`temporal_visibility` databases as separate DBs on the same server for local dev. |
| MongoDB | `mongo:8.0` (`mongo:8` tag line) | MongoDB 8.0 is the current stable major (8.2.x latest patch line) — use 8.0 for a well-trodden LTS-style track. |
| Redis | `redis:8.2-trixie` (or pin to the `8.x` line you test against) | Redis 8.x current; no auth needed for local dev, but wire a password via env var to mirror prod hygiene. |
| RabbitMQ | `rabbitmq:4.3-management` | 4.3.x is current stable; `-management` tag includes the web UI on :15672 for visually confirming queues during mentor demos. |
| Kafka | `confluentinc/cp-kafka:7.6.13` (or `8.0.7` if you want to track Confluent's newest major) | Recommend `7.6.13` for local dev — it's the widely-used, well-documented line; run in **KRaft mode** (no ZooKeeper) since ZooKeeper is deprecated/removed in current Kafka releases. |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.13` | **Must match the same Confluent Platform version line as the Kafka image** — mixing e.g. `cp-kafka:8.0.7` with `cp-schema-registry:7.6.13` is a known source of protocol/compatibility issues. Pin both to the same CP version. |
| Temporal server | `temporalio/auto-setup:1.31.2` for local dev convenience (auto-provisions schema on first boot) | Auto-setup image is explicitly **not for production** — fine here since this is a learning-assignment local/dev environment, but note that distinction in your README since "production-grade architecture" is an evaluation criterion; mention the auto-setup→server split as a known tradeoff. |
| Temporal UI | `temporalio/ui:latest` (pin a specific tag once confirmed) | Web UI on :8080 for visually inspecting workflow execution history — valuable for mentor demos of the booking saga. |
| Jaeger | `jaegertracing/all-in-one:1.76.0` | Exposes OTLP gRPC (:4317) and HTTP (:4318) ingestion natively plus the query UI (:16686). Do not use the legacy Jaeger-native-protocol exporter from the Python side — export via OTLP. |
| Prometheus | `prom/prometheus:v3.13.3` | Scrapes `/metrics` from the FastAPI app (via prometheus-fastapi-instrumentator) and, optionally, Celery/Temporal worker metrics endpoints if added later. |
| Grafana | `grafana/grafana:13.0.8` | Point at the Prometheus datasource; build the "operational analytics" dashboards (appointments over time, cancellation rate, wait time) here or in the app-level HTML dashboard, per PROJECT.md's dual-dashboard structure (Grafana for ops metrics, the self-contained HTML for architecture/decision record). |

**Compose wiring gotchas:**

- **Startup ordering / health checks matter more than `depends_on` alone.** Kafka + Schema Registry + Temporal all need their dependencies to be *actually ready* (not just "container started") before the app boots. Use Compose `healthcheck:` blocks + `depends_on: condition: service_healthy` for Postgres, Kafka, and Temporal at minimum — a common local-dev failure mode is the FastAPI app or Temporal worker crash-looping because Postgres/Kafka accepted the TCP connection but wasn't done initializing.
- **Temporal's own Postgres dependency creates a two-hop startup chain:** Postgres → Temporal auto-setup (runs schema migration on boot) → Temporal server ready → your `temporal-worker` process can connect. Budget for this in health-check `start_period`/retries; Temporal's schema setup on first boot can take longer than a typical DB healthcheck timeout.
- **Kafka KRaft mode needs a `CLUSTER_ID`** environment variable (a fixed base64 UUID) set consistently across the single broker's env vars for local single-node dev — forgetting this is a common "Kafka container won't start" issue with newer Confluent images that default to KRaft.
- **Schema Registry needs `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`** pointed at the Kafka broker's *internal* Compose network listener, not the host-mapped port — a frequent local-dev mistake is reusing the host-facing `localhost:9092` advertised listener inside the Docker network, which only resolves from the host machine, not from other containers.
- **One `.env` file, read by both the app's `pydantic-settings` config AND Docker Compose's `env_file:`** — keeps broker URLs, DB URLs, and OTLP endpoint consistent between how the app expects to connect and how Compose wires the network, avoiding "works differently in container vs local run" drift.

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|--------------------------|
| confluent-kafka-python | aiokafka | Choose aiokafka only if you need a pure-asyncio-native consumer API with no thread/executor wrapping and are willing to trade off Confluent's official Schema Registry integration and librdkafka's throughput — reasonable for a smaller, latency-insensitive service, but for this project's "tens of thousands of requests" scale target, confluent-kafka's production throughput profile wins. |
| PyMongo async (`AsyncMongoClient`) | Motor | Only use Motor if you're maintaining an existing Motor codebase pre-dating May 2025 and can't migrate yet. For a greenfield 2026 project, starting on Motor is adopting a deprecated library on day one. |
| SQLAlchemy 2.0 async + asyncpg | Sync SQLAlchemy + psycopg2, or raw asyncpg without an ORM | Raw asyncpg (no ORM) is defensible if the team wants maximum control/performance and is comfortable hand-writing SQL — valid alternative for a systems-thinking assignment, but SQLAlchemy 2.0's async ORM is the more broadly recognized "correct" choice mentors will expect and pairs cleanly with Alembic migrations. |
| `temporalio/auto-setup` image for local dev | `temporalio/server` + manual schema setup | Switch to the plain `server` image (with schema pre-provisioned via `temporal-sql-tool` or `schema` container job) once you want the Compose setup to mirror a production-shaped deployment more closely — worth doing if the mentor evaluation specifically probes "how would this run in production." |
| Celery + RabbitMQ | Celery + Redis as broker | Not recommended here since RabbitMQ is explicitly mandated in the stack; noted only because it's the most common Celery alternative broker pattern if the constraint were ever relaxed. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Motor | Deprecated by MongoDB as of May 14, 2025; no new features, EOL May 2026 (critical fixes through 2027). Starting a new project on it in 2026 means adopting a sunset library immediately. | `pymongo` 4.18.0+ using the native `AsyncMongoClient` async API. |
| `aioredis` (standalone package) | Merged into `redis-py` years ago; the standalone package is unmaintained and creates dependency confusion with modern `redis-py`. | `redis` 8.1.0 with `redis.asyncio` module. |
| Sync psycopg2 inside `async def` FastAPI routes | Blocks the single event loop thread per request; under this project's concurrent-load requirements (peak booking windows, tens of thousands of requests) this serializes what should be concurrent I/O and defeats the entire point of an async framework. | `asyncpg` via SQLAlchemy 2.0 async engine; if a sync driver is truly needed somewhere, only call it from a plain `def` endpoint (FastAPI runs those in a threadpool) or explicitly offload via `run_in_executor`. |
| `opentelemetry-exporter-jaeger` (legacy native-protocol exporter) | Deprecated in favor of OTLP; Jaeger itself now ingests OTLP natively, and the OTel Python project has been steering all exporters toward OTLP-only for years. | `opentelemetry-exporter-otlp` pointed at Jaeger's OTLP ports (4317 gRPC / 4318 HTTP). |
| ZooKeeper-mode Kafka | Removed/deprecated in current Kafka releases; ZooKeeper coordination is legacy architecture. | KRaft-mode Kafka (`confluentinc/cp-kafka` configured with `KAFKA_PROCESS_ROLES=broker,controller` and a `CLUSTER_ID`), which is now the standard for new Kafka deployments including local dev. |
| Passing raw Pydantic model instances directly as Celery task arguments | Celery's default JSON serializer doesn't know how to (de)serialize Pydantic models natively; this silently breaks or requires brittle workarounds. | Serialize to `.model_dump()` dict (or JSON string) before calling `.delay()`/`.apply_async()`, and reconstruct with `Model.model_validate(...)` inside the task — or register a custom Celery serializer once, centrally. |
| Mixing Confluent Platform image versions (e.g., `cp-kafka:8.0.7` with `cp-schema-registry:7.6.13`) | Known source of wire-protocol/compatibility issues between broker and registry. | Pin `cp-kafka` and `cp-schema-registry` to the identical Confluent Platform version tag. |
| `temporalio/auto-setup` in any environment described as "production" in your docs | The image itself documents that it is not meant for production use — it exists purely to auto-provision schema for quick starts. | For anything you present as production-shaped, use `temporalio/server` with schema pre-provisioned via the dedicated schema-setup tooling. For local dev in this 3-week assignment, `auto-setup` is fine and saves time — just label it correctly in your architecture docs. |

## Stack Patterns by Variant

**If the mentor evaluation wants to see production-shaped Temporal deployment (not just quick local dev):**
- Swap `temporalio/auto-setup` for `temporalio/server` + a one-shot schema-setup job/container.
- Because auto-setup explicitly disclaims production readiness, and "maintainable production-grade architecture" is a listed expected outcome in the requirements doc.

**If Kafka message volume/replay requirements grow beyond local-dev scale during Week 3:**
- Consider adding a dead-letter topic pattern and consumer-group-per-service-role convention early (e.g., `analytics-consumer-group`, `notification-trigger-consumer-group`) rather than one shared consumer group.
- Because the requirements explicitly call for events to "avoid double-processing" and "handle failures gracefully" — consumer group design is the lever for both.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|------------------|-------|
| fastapi 0.141.1 | pydantic 2.13.5, starlette (bundled via `fastapi[standard]`) | Always install FastAPI via the `[standard]` extra to keep Pydantic/Starlette versions in the combination FastAPI actually tests against, rather than pinning them independently and risking drift. |
| sqlalchemy 2.0.52 | asyncpg 0.31.0, alembic 1.19.2 | All three track the SQLAlchemy 2.0 async API; older Alembic releases (<1.7) predate async engine support entirely — irrelevant here since 1.19.2 is current, but relevant if a future dependency bump ever floors an old Alembic transitively. |
| confluent-kafka 2.15.0 | cp-kafka / cp-schema-registry (any 7.x or 8.x CP line) | The Python client is broadly protocol-compatible across recent broker versions; the compatibility risk is broker↔schema-registry version matching (see above), not client↔broker. |
| temporalio (Python SDK) 1.32.0 | Temporal server 1.31.x line | SDK and server are versioned independently — there is no official strict lockstep matrix; the practical rule is "recent SDK + recent server line, both within the last few months of release" rather than exact version pinning. Re-verify against `docs.temporal.io/develop/python/versioning` before locking a specific server minor for the submitted assignment. |
| opentelemetry-instrumentation-fastapi 0.65b0 | opentelemetry-api/sdk 1.44.0 | OTel Python contrib instrumentation packages use a `0bX` version scheme distinct from (but released alongside) the core `1.x` API/SDK — when upgrading, bump both together and check the contrib repo's compatibility table rather than assuming semver alignment. |
| celery 5.6.3 | kombu 5.6.2 | Celery pins a compatible kombu range in its own `setup.py`; don't independently pin a kombu version outside what your chosen Celery release declares as compatible. |

## Sources

- pypi.org JSON API (direct query, 2026-09-09) — exact current versions for fastapi, uvicorn, sqlalchemy, asyncpg, motor, pymongo, redis, celery, kombu, confluent-kafka, aiokafka, temporalio, opentelemetry-api/sdk, opentelemetry-instrumentation-fastapi, prometheus-fastapi-instrumentator, alembic, pydantic, pydantic-settings, gunicorn, python-jose, passlib, bcrypt — confidence: HIGH (primary registry API, not a search result)
- hub.docker.com v2 API (direct query, 2026-09-09) — image tags for postgres, mongo, redis, rabbitmq, confluentinc/cp-kafka, confluentinc/cp-schema-registry, temporalio/auto-setup, temporalio/server, jaegertracing/all-in-one, prom/prometheus, grafana/grafana — confidence: HIGH (primary registry API)
- github.com/temporalio/temporal releases API — confirms Temporal server v1.31.2 as current — confidence: HIGH (primary source)
- WebSearch: "FastAPI production deployment best practices" / render.com, zestminds.com — worker-count guidance — confidence: MEDIUM
- WebSearch: SQLAlchemy 2.0 + asyncpg + FastAPI best practices (leapcell.io, oneuptime.com, multiple blog cross-checks) — lifespan-scoped engine pattern — confidence: MEDIUM
- WebSearch: MongoDB official docs migration guide (mongodb.com/docs/languages/python/pymongo-driver) — Motor deprecation timeline — confidence: MEDIUM-HIGH (official vendor docs surfaced via search)
- WebSearch: confluentinc/confluent-kafka-python GitHub issue #1723, Confluent official blog (confluent.io/blog/kafka-python-asyncio-integration) — async consumer/producer pattern guidance — confidence: MEDIUM
- WebSearch: Temporal docs (docs.temporal.io/develop/python/versioning), temporalio/docker-compose GitHub repo — server/SDK versioning and Compose wiring — confidence: MEDIUM (no definitive compatibility matrix found; documented as an open gap)
- WebSearch: OpenTelemetry Python + FastAPI + Jaeger setup (realpython.com, opentelemetry-python-contrib docs, signoz.io) — OTLP-native Jaeger ingestion, bootstrap/instrument CLI pattern — confidence: MEDIUM

---
*Stack research for: SmartHealth Part A (healthcare-operations backend)*
*Researched: 2026-09-09*
