---
last_mapped_commit: bcb88ef28875eaa61f6de4649a9ff16943f9f650
last_mapped_at: 2026-09-30
---
# Technology Stack

**Analysis Date:** 2026-09-30

## Languages

**Primary:**
- Python 3.12 - Backend runtime (API services, workers, migrations)

## Runtime

**Environment:**
- Python 3.12 (pinned via `pyproject.toml` `requires-python = ">=3.12,<3.13"`)

**ASGI Server:**
- Uvicorn 0.52.4 - ASGI application server for FastAPI
- Gunicorn - Process manager for Uvicorn workers (`gunicorn -k uvicorn.workers.UvicornWorker`)
  - Worker count: CPU cores (async workers, not sync formula)
  - Config: `Dockerfile` CMD targets `smarthealth.main:app`

**Package Manager:**
- pip (via setuptools build backend in `pyproject.toml`)
- Lockfile: Not used (direct `pyproject.toml` dependency pinning)

## Frameworks

**Core:**
- FastAPI 0.141.1 - Web framework with auto-API documentation
  - Installed via `fastapi[standard]` extra to bundle Pydantic v2 and Starlette
  - App factory: `services/identity/app.py:create_app()`
  - Lifespan context manager for startup/shutdown hooks
- Starlette (bundled via `fastapi[standard]`) - ASGI toolkit

**Data Validation:**
- Pydantic 2.13.5 - Request/response models, settings validation
- pydantic-settings 2.15.0 - Environment-based configuration loading (`smarthealth/config.py`)

**Database ORM:**
- SQLAlchemy 2.0.52 - Async-native ORM for PostgreSQL
  - Engine: `create_async_engine(postgresql+asyncpg://...)`
  - Sessions: `AsyncSession` with `async_sessionmaker`
  - Dependency: `smarthealth/db.py:get_db()` for FastAPI routes
  - Multi-loop safety: Transparent re-creation when event loop changes (pytest compatibility)

**Schema Migrations:**
- Alembic 1.19.2 - Database schema versioning
  - Config: `infra/migrations/` directory
  - Async bridge: `run_sync()` pattern in `env.py`

**Testing:**
- pytest - Test runner
- pytest-asyncio - Async test support
  - Configured: `asyncio_mode = "auto"` in `pyproject.toml`
  - Test discovery: `tests/` directory

**HTTP Client:**
- httpx - Async HTTP client (for outbound service calls)

## Key Dependencies

**Critical:**

- **asyncpg 0.31.0** - PostgreSQL async driver
  - Used by SQLAlchemy 2.0 as the async dialect (`postgresql+asyncpg://`)
  - Never use sync `psycopg2` in async contexts (blocks event loop)

- **pymongo 4.18.0** - MongoDB driver
  - Uses native async API: `AsyncMongoClient` (never Motor, which is deprecated as of May 2025)
  - Used for audit trail and operational logs
  - Multi-loop safety: Transparent re-creation when event loop changes
  - Dependency: `smarthealth/db.py:get_mongo_client()`

- **redis 8.1.0** - Redis client
  - Native asyncio support via `redis.asyncio` module
  - Unified single package (old `aioredis` standalone is unmaintained)
  - Used for: token denylist, distributed locks (slot reservation), rate limiting
  - Dependency: `services/identity/security/current_user.py:get_redis()`

**Observability:**

- **opentelemetry-api 1.44.0** - Tracing instrumentation core
- **opentelemetry-sdk 1.44.0** - Tracer provider implementation
- **opentelemetry-instrumentation-fastapi 0.65b0** - FastAPI auto-instrumentation
  - Non-beta status: `0.65b0` versioning (beta) is standard for OTel contrib; not a maturity signal
  - PHI safety: Body capture disabled by default; implementation enforces identifiers-only in attributes
  - Setup: `libs/observability/otel.py:setup_tracing(app)`

- **opentelemetry-exporter-otlp 1.44.0** - OTLP gRPC exporter to Jaeger
  - Endpoint: `settings.otlp_endpoint` (default: `http://localhost:4317`)
  - Never use deprecated `opentelemetry-exporter-jaeger` — Jaeger ingests OTLP natively

- **prometheus-fastapi-instrumentator 8.1.0** - Prometheus metrics middleware
  - Exposes `/metrics` endpoint for Prometheus scraping
  - Setup: `services/identity/app.py:Instrumentator().instrument(app).expose(app)`

- **structlog 26.1.0** - Structured JSON logging
  - Processors: trace ID/span ID injection, JSON rendering, timestamp, log level
  - Logger factory: `PrintLoggerFactory` to stdout (container-friendly)
  - Configuration: `libs/observability/logging.py:configure_logging()`
  - Correlation: Trace ID/span ID automatically injected into every log event

**Authentication & Security:**

- **python-jose[cryptography] 3.5.0** - JWT encode/decode
  - Implementation: `libs/auth/jwt.py`
  - Algorithm pinning: `algorithms=[settings.jwt_algorithm]` on every decode (prevents alg:none footgun)
  - Token types: short-lived access (~15 min), long-lived refresh (~7 days)

- **passlib[bcrypt] 1.7.4** - Password hashing interface
- **bcrypt 5.0.0** - Password hashing (explicitly pinned alongside passlib)

## Configuration

**Environment:**
- **Method:** pydantic-settings `BaseSettings` reading `.env` file (local dev) or OS environment variables
- **File:** `.env` (development, not committed)
- **Module:** `smarthealth/config.py:Settings` — imported by API process and all worker processes
- **Key configs required:**
  - `DATABASE_URL` - PostgreSQL async connection string (default: `postgresql+asyncpg://smarthealth:changeme@localhost:5432/smarthealth`)
  - `MONGO_URL` - MongoDB connection string (default: `mongodb://localhost:27017`)
  - `MONGO_DB_NAME` - MongoDB database name (default: `smarthealth`)
  - `REDIS_URL` - Redis connection string (default: `redis://localhost:6379/0`)
  - `JWT_SECRET` - Symmetric key for token signing (production must set via secret manager; development defaults to `changeme-local-dev-only`)
  - `ENVIRONMENT` - Deployment stage (`development`, `staging`, `production`; defaults to `development`)
  - `OTLP_ENDPOINT` - OpenTelemetry collector endpoint (default: `http://localhost:4317`)

**Build:**
- **Entrypoint:** `Dockerfile` builds `python:3.12-slim` image
  - Installs system deps: `build-essential` (for wheel compilation: asyncpg, cryptography, bcrypt)
  - Copies `pyproject.toml`, source code, and installs via `pip install .`
  - CMD: Gunicorn with 4 Uvicorn workers by default (`WEB_CONCURRENCY` env override)
  - App target: `smarthealth.main:app`

- **Build backend:** setuptools (`setuptools>=68`)
- **Packages discovered:** `smarthealth`, `libs`, `services`, `services.identity`

## Platform Requirements

**Development:**
- Python 3.12+
- Docker & Docker Compose (for local infra: Postgres, Mongo, Redis, RabbitMQ, Kafka, Temporal, Jaeger, Prometheus, Grafana)
- System packages on `slim` image: `build-essential` (asyncpg/cryptography/bcrypt compilation)

**Production:**
- Containerized deployment via Docker image built with `Dockerfile`
- ASGI-compatible runtime (Gunicorn + Uvicorn)
- External services:
  - PostgreSQL 15+ (system of record)
  - MongoDB 8.0+ (audit/log storage)
  - Redis 8.x (cache/locks/denylist)
  - RabbitMQ 4.3+ (Celery broker — Phase 2 integration planned)
  - Kafka 7.6.13+ (event streaming — Phase 3 integration planned)
  - Temporal server 1.31.x (workflow orchestration — Phase 3 integration planned)
  - Jaeger (trace ingestion via OTLP gRPC :4317 or HTTP :4318)
  - Prometheus (metrics scraping)

---

*Stack analysis: 2026-09-30*
