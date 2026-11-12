---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# External Integrations

**Analysis Date:** 2026-09-30

## APIs & External Services

**None currently active.** Phase 1 focuses on core platform infrastructure. Planned integrations (billing pre-checks, notification providers, EHR systems) are scaffolded in docker-compose.yml but not yet called by app code.

## Data Storage

**Databases:**

- **PostgreSQL 15**
  - Role: System of record (patient data, provider schedules, appointment state, clinic hierarchies)
  - Connection: `DATABASE_URL` env var → `postgresql+asyncpg://smarthealth:changeme@localhost:5432/smarthealth`
  - Client: SQLAlchemy 2.0 async ORM + asyncpg driver
  - Access: `smarthealth/db.py:get_db()` FastAPI dependency yields `AsyncSession`
  - Migrations: Alembic in `infra/migrations/`
  - Container: `postgres:15-trixie` (docker-compose.yml)
  - Health check: `pg_isready` command

- **MongoDB 8.0**
  - Role: Audit trail, operational logs, flexible schema event storage (visit completion events, appointment cancellation logs, notification delivery attempts)
  - Connection: `MONGO_URL` env var → `mongodb://mongo:27017`
  - Database: `MONGO_DB_NAME` env var (default: `smarthealth`)
  - Client: PyMongo 4.18.0 native async API (`AsyncMongoClient`)
  - Access: `smarthealth/db.py:get_mongo_client()` FastAPI dependency
  - Collections: `audit` (indexed by `user_id`, `resource_type`, `timestamp`)
  - Indexes: Initialized on app startup via `libs/audit/mongo_indexes.py:ensure_audit_indexes()`
  - Container: `mongo:8.0` (docker-compose.yml)
  - Health check: `mongosh --quiet --eval db.adminCommand('ping')`

**File Storage:**
- Not yet integrated. Local filesystem only for test/dev artifacts (no S3/cloud storage in Phase 1).

**Caching:**
- **Redis 8.2**
  - Connection: `REDIS_URL` env var → `redis://localhost:6379/0`
  - Client: redis 8.1.0 with `redis.asyncio` module (native async)
  - Use cases:
    - **Token denylist:** revoked JWT tokens cached with TTL
      - Key pattern: `denylist:{jti}` → expires after token's remaining lifetime
      - Implementation: `libs/auth/denylist.py:revoke_token()`, `libs/auth/denylist.py:is_revoked()`
    - **Distributed locks** (planned Phase 2): Redlock pattern for slot-reservation races
    - **Rate limiting** (planned Phase 2): Sliding-window counters for peak-booking windows
  - Container: `redis:8.2-bookworm` (docker-compose.yml)
  - Health check: `redis-cli ping`

## Authentication & Identity

**Auth Provider:**
- Custom JWT-based (in-app, no external OAuth/SAML)

**Implementation:**
- JWT issuance: `libs/auth/jwt.py:create_access_token()` / `create_refresh_token()`
  - Symmetric signing: HMAC-SHA256 (HS256)
  - Secret: `JWT_SECRET` env var (production must set via secret-manager)
  - Access token lifetime: 15 minutes (configurable `access_token_expire_minutes`)
  - Refresh token lifetime: 7 days (configurable `refresh_token_expire_days`)
  - Claims structure:
    - **Access token:** `sub` (user_id), `role`, `clinic_id`, `jti`, `type=access`, `iat`, `exp`
    - **Refresh token:** `sub` (user_id), `jti`, `type=refresh`, `iat`, `exp` (no role/clinic; re-derived on refresh)

**Token Validation:**
- `libs/auth/jwt.py:decode_token()` — always pins `algorithms=[settings.jwt_algorithm]` to prevent alg:none forgery
- Route-level: `services/identity/security/current_user.py:get_current_user()` FastAPI dependency
  - Extracts token from `Authorization: Bearer <token>` header
  - Validates signature, expiry, and type
  - Checks denylist (revoked tokens) via Redis
  - Returns `AuthenticatedUser` model with `user_id`, `role`, `clinic_id`

**Role-Based Access Control (RBAC):**
- 4 roles: `patient`, `provider`, `front-desk`, `admin`
- Stored in access token claims; re-derived on refresh from current DB state
- Route-level enforcement: FastAPI dependencies check `current_user.role`

**Password Management:**
- Hashing: passlib + bcrypt (5.0.0)
- Implementation: `libs/auth/passwords.py`
- Not yet exposed in Phase 1 (auth routes use API-key-style token exchange for demo)

## Monitoring & Observability

**Error Tracking:**
- None currently. Error logs go to stdout (container-friendly).

**Logs:**
- **Output:** Structured JSON to stdout
- **Format:** structlog processors emit `{"timestamp": "...", "level": "...", "event": "...", "trace_id": "...", "span_id": "...", ...}` per event
- **Correlation:** Trace ID and span ID automatically injected from active OTel span context
- **Setup:** `libs/observability/logging.py:configure_logging()`
- **PHI safety:** Only identifiers logged (user_id, patient_id, clinic_id, jti) — never PHI field values

**Traces:**
- **Tracer provider:** OpenTelemetry SDK
- **Exporter:** OTLP gRPC to Jaeger
- **Endpoint:** `OTLP_ENDPOINT` env var (default: `http://localhost:4317`)
- **Auto-instrumentation:** FastAPI middleware via `opentelemetry-instrumentation-fastapi`
  - Captures: HTTP method, route, status code, latency
  - PHI safety: Body capture disabled (no request/response payloads in spans)
- **Setup:** `libs/observability/otel.py:setup_tracing(app)`
- **Manual tracing:** `libs/observability/otel.py:get_tracer()` for application code

**Metrics:**
- **Exporter:** Prometheus (scrape-based)
- **Endpoint:** `/metrics` exposed by `prometheus-fastapi-instrumentator`
- **Metrics captured:** HTTP request count, duration, status codes, exceptions
- **Scrape config:** Prometheus container in docker-compose.yml (default: 15s interval)

## CI/CD & Deployment

**Hosting:**
- Docker container orchestration (local dev: Docker Compose; production: Kubernetes or cloud platform)
- Image: Built via `Dockerfile` from `python:3.12-slim` base
- Entry point: `gunicorn -k uvicorn.workers.UvicornWorker ... smarthealth.main:app`

**CI Pipeline:**
- None yet configured. Placeholder for Phase 2 (likely GitLab CI or GitHub Actions).

**Local Orchestration:**
- **Docker Compose:** `infra/docker-compose.yml`
  - Services: api, postgres, mongo, redis, rabbitmq, kafka, schema-registry, temporal, temporal-ui, jaeger, prometheus, grafana
  - Health checks: Required for postgres, mongo, redis, rabbitmq, kafka, schema-registry, temporal before api starts
  - Volumes: Persistent storage for databases and metrics
  - Environment: Sourced from `.env` file (not committed; see `.env.example`)

## Environment Configuration

**Required env vars:**
- `DATABASE_URL` - PostgreSQL async connection string
- `MONGO_URL` - MongoDB connection URI
- `MONGO_DB_NAME` - MongoDB database name
- `REDIS_URL` - Redis connection URI
- `JWT_SECRET` - Symmetric key for JWT signing (production-only; development uses default)
- `ENVIRONMENT` - Deployment stage (`development`, `staging`, `production`)
- `OTLP_ENDPOINT` - OpenTelemetry collector gRPC endpoint

**Optional env vars (with defaults):**
- `JWT_ALGORITHM` - JWT signing algorithm (default: `HS256`)
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Short-lived token TTL (default: 15)
- `REFRESH_TOKEN_EXPIRE_DAYS` - Long-lived token TTL (default: 7)
- `WEB_CONCURRENCY` - Gunicorn worker count (default: 4 from Dockerfile)
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` - Docker Compose local dev (defaults: `smarthealth`/`changeme-local-dev-only`/`smarthealth`)
- `POSTGRES_HOST_PORT` - Local host port mapping (default: 5433)
- `MONGO_HOST_PORT` - Local host port mapping (default: 27017)
- `REDIS_HOST_PORT` - Local host port mapping (default: 6379)
- `RABBITMQ_*` - RabbitMQ credentials and ports (scaffolded; not used in Phase 1)
- `KAFKA_*` - Kafka broker and Schema Registry ports (scaffolded; not used in Phase 1)

**Secrets location:**
- **Local dev:** `.env` file (never committed, see `.env.example` template)
- **Production:** Environment variables or secret-manager integration (AWS Secrets Manager, Azure Key Vault, etc.)
- **CI/CD:** Secrets injected at runtime via platform-native secret stores
- **Mandatory failure mode:** `smarthealth/config.py:Settings.model_post_init()` raises `RuntimeError` if `ENVIRONMENT != 'development'` and `JWT_SECRET` is unset or uses the default value (CR-03 security control)

## Webhooks & Callbacks

**Incoming:**
- None currently. HTTP POST endpoints in `services/identity/api/*` are internal API routes, not webhook receivers.

**Outgoing:**
- None currently. Notifications/event-driven callbacks are scaffolded in Phase 2+ (Celery tasks, Temporal workflows).

## Infrastructure Services (Scaffolded, Not Yet Used)

**RabbitMQ 4.3:**
- Role: Message broker for Celery background tasks (Phase 2)
- Connection: Celery broker URL (not yet configured in app)
- Container: `rabbitmq:4.3-management` with management UI on :15672
- Health check: `rabbitmq-diagnostics -q ping`

**Kafka 7.6.13 + Schema Registry 7.6.13:**
- Role: Event streaming, schema-versioned topics (Phase 3)
- Kafka mode: KRaft (broker + controller combined; no ZooKeeper)
- Schema Registry: Confluent Platform v7.6.13 (must match Kafka version)
- Containers: `confluentinc/cp-kafka:7.6.13`, `confluentinc/cp-schema-registry:7.6.13`
- Health checks: Kafka broker API version endpoint, Schema Registry subject endpoint

**Temporal 1.29.7:**
- Role: Distributed workflow orchestration (appointment booking saga — Phase 3)
- Container: `temporalio/auto-setup:1.29.7` (convenience image for local dev; NOT production)
  - Postgres backend for temporal schema
  - Auto-provisions schema on first boot
  - Production would use `temporalio/server` + dedicated schema-setup job
- UI: `temporalio/ui:latest` on :8080 for workflow history inspection
- Health check: `tctl cluster health` command

**Prometheus 3.13.3:**
- Role: Metrics collection and storage
- Scrape config: `infra/prometheus.yml`
- Targets: FastAPI `/metrics` endpoint (exposed by prometheus-fastapi-instrumentator)
- Container: `prom/prometheus:v3.13.3`
- Data volume: `prometheus_data` (persistent)

**Grafana 13.0.8:**
- Role: Operational analytics dashboards
- Data source: Prometheus
- Admin password: `GF_SECURITY_ADMIN_PASSWORD` env var (default: `changeme-local-dev-only`)
- Container: `grafana/grafana:13.0.8`
- Data volume: `grafana_data` (persistent)
- Port: :3000

**Jaeger 1.76.0:**
- Role: Distributed tracing visualization
- OTLP ingestion: gRPC :4317, HTTP :4318 (native OTLP support, no legacy protocol)
- Query UI: :16686
- Container: `jaegertracing/all-in-one:1.76.0` (all-in-one for local dev)
- No persistent storage in local dev (in-memory only)

---

*Integration audit: 2026-09-30*
