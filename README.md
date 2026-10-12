# SmartHealth — Intelligent Healthcare Operations Platform (Part A)

MediNova's foundational healthcare-operations backend: a distributed platform for
patient/provider management, appointment scheduling, visit workflows, billing
pre-checks, notifications, and operational analytics.

This is Part A — the core platform. Phase 1 delivered the first working slice as a
single FastAPI service; **Phase 1.1 re-delivered it as six independently deployable
services** after the week-1 mentor review. Appointments, the booking saga, the
event backbone and analytics are Phases 2 and 3 and are **not built yet**.

- Architecture & decision record (open directly from disk, no server needed):
  `docs/architecture-dashboard.html`
- Service breakdown and flows: `docs/architecture.md` (Mermaid) / `docs/architecture.html`
- Per-service schemas: `docs/erd.md` / `docs/erd.html`
- Planning context: `.planning/PROJECT.md`

## What exists today

| Layer | Status |
|---|---|
| API Gateway, Identity, Organization, Provider, Patient services | Built and running in Docker Compose |
| Scheduling service | Skeleton only: health endpoint plus `provider_slots` and its double-booking exclusion constraint; no `/api` routes yet |
| PostgreSQL (one database per service), MongoDB `audit_log`, Redis | Built and in use |
| Jaeger, Prometheus, Grafana | Tracing and per-service metrics wired; Grafana dashboards not built yet |
| Temporal, Kafka + Schema Registry, Celery + RabbitMQ | Containers run and report healthy; **no application code uses them yet** (Phase 2/3) |

This README describes what the repository contains; it does not claim the platform
is production-hardened. Known gaps are listed under [Known limitations](#known-limitations).

## Service map

```
                          clients (patient / front-desk / provider / admin)
                                              |
                                              v
                    +-------------------------------------------------+
                    |  API Gateway  (FastAPI + httpx proxy)   :8000   |  the only published
                    |  route table, /internal and unsafe paths -> 404 |  application port
                    |  per-IP rate limits (Redis, fail-open)          |
                    |  no JWT verification, no response aggregation   |
                    +----+-------+-----------+-----------+------------+
                         |       |           |           |
       /api/auth         |       | /api/     | /api/     | /api/
       /api/admin        |       | clinics   | providers | patients
       /.well-known/     v       v           v           v
       jwks.json    +----------+ +--------------+ +----------+ +---------+
                    | Identity | | Organization | | Provider | | Patient |     +------------+
                    | users    | | clinics      | | providers| | patients|     | Scheduling |
                    | RS256    | | departments  | | specialt.| | (PHI)   |     | (skeleton) |
                    | signer   | |              | | avail.   | |         |     | provider_  |
                    |          | |              | | templates| |         |     | slots      |
                    +----+-----+ +------+-------+ +----+-----+ +----+----+     +------+-----+
                         |              |              |            |                 |
                    identity_db   organization_db  provider_db  patient_db       scheduling_db
                    (one Postgres container; one database + one role per service)

   Cross-service reference checks (synchronous GET to the owner's /internal endpoint, D-05):
     Identity -> Organization (clinic)      Patient  -> Organization (clinic), Identity (user)
     Provider -> Organization (department)  Provider -> Identity (user)

   Shared infrastructure (not service databases):
     MongoDB  audit_log   - every service appends audit records (field names only, never PHI)
     Redis                - JWT jti denylist (Identity writes, every service reads) + gateway rate limits
     JWKS                 - Identity publishes public keys; every other service verifies tokens locally
```

Key rules (see the dashboard's decision cards for alternatives and tradeoffs):

- **Database per service.** Each role can connect only to its own database; no foreign key
  crosses a service. Cross-service references are plain UUIDs checked at write time.
- **RS256 + JWKS.** Only Identity holds the private key; services verify from a cached
  JWKS (`kid` `smarthealth-identity-1`) and check the shared Redis denylist.
- **Fail closed on references.** A missing or inactive referenced record is `422`; an owner
  service that cannot answer is `503`; nothing is written in either case. Deactivation never
  cascades, and reads return ids only (clients combine them).
- **One connected trace** per request across services (W3C `traceparent`), and one Prometheus
  job per service.

## Local development

Prerequisites: Python 3.12, Docker with Compose.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

cp .env.example .env
cp infra/.env.example infra/.env

# Identity's RS256 signing key. Required before `up`: Compose refuses to start
# `identity` without it (fail-closed, no fallback key). Written to the gitignored
# infra/secrets/jwt_private.pem with mode 0600; never commit or share it.
python scripts/generate_jwt_keypair.py

docker compose -f infra/docker-compose.yml up -d --build
scripts/compose_healthcheck.sh --wait 420
```

`compose_healthcheck.sh` requires the six application services (gateway, identity,
organization, provider, patient, scheduling) to be present **and** healthy, plus every
other service that declares a healthcheck. A missing required service is a failure.
Startup is ordered: Postgres → the one-shot `db-init` job (creates the five databases and
roles) → each database-owning service, which runs its own Alembic migrations before serving →
the gateway.

Local resource use: the stack is now the infrastructure containers plus six application
containers (two gunicorn workers each by default, `WEB_CONCURRENCY`). Expect noticeably
higher memory use than Phase 1 on a small machine. Postgres runs with
`max_connections=300` because Temporal and six application pools exhausted the default 100.

The legacy single `smarthealth` database from Phase 1 is **left untouched and is not
migrated**; none of the new services use it. Local dev data is treated as disposable.
Dropping it (it also holds a few stray empty objects from an early migration run) is an
operator decision and is not done by any script here.

## First-admin bootstrap

`POST /api/auth/register` (public, unauthenticated) always creates a `patient` account, and
the only route that can create an `admin`/`provider`/`front_desk` account —
`POST /api/auth/admin/users` — is itself restricted to admins. That closes the
privilege-escalation gap where an unauthenticated caller could self-assign an elevated role,
but it also means a fresh deployment cannot create its *first* admin through the API.

`scripts/bootstrap_admin.py` is the operational escape hatch: a one-shot CLI script,
deliberately **not** an HTTP route, so it is never reachable by a network caller. It writes
to Identity's own database, `identity_db`, addressed by `IDENTITY_DATABASE_URL` (there is no
shared `DATABASE_URL` any more):

```bash
IDENTITY_DATABASE_URL='postgresql+asyncpg://identity_svc:<IDENTITY_DB_PASSWORD>@localhost:5433/identity_db' \
BOOTSTRAP_ADMIN_EMAIL=admin@example.com \
BOOTSTRAP_ADMIN_PASSWORD='<a-strong-generated-password>' \
python scripts/bootstrap_admin.py
```

Behavior:

- **Idempotent / safe to re-run.** If an `admin`-role user already exists in `identity_db`
  the script no-ops (`"already_exists"`); it never creates a second admin and never
  overwrites an existing user's password or any other field.
- Hashes the password with the same bcrypt path the Identity service uses.
- Writes an `audit_log` entry (`action="user.bootstrap"`, `actor_id="system"`) to the shared
  MongoDB audit collection.
- Exits `1` with a stderr message if `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD`
  are not set, or if the requested email is already registered to a non-admin user.

Run it once after the stack is up (Identity's migration has created the schema in
`identity_db`), then log in via `POST /api/auth/login` and use `POST /api/auth/admin/users`
for all further staff, provider and admin accounts.

## Running tests

```bash
# Quick: skips tests marked or named integration and e2e
pytest tests/ -x -k "not integration and not e2e"

# In-process suites (all services mounted in one process; needs the Compose
# Postgres/Mongo/Redis containers for the integration tests)
pytest tests/ -x -m "not e2e"

# Real stack through the published gateway port (stack must be up).
# Set SMARTHEALTH_E2E_REQUIRE_STACK=1 to fail instead of skip when no stack is reachable.
pytest tests/e2e/test_gateway_smoke.py -m e2e
```

The in-process suites mount every service ASGI app into one HTTP client, so cross-service
calls run without sockets. They do **not** exercise real network behaviour: a container going
down mid-request, real retry timing and a cold start from an empty volume have not been
verified (see [Known limitations](#known-limitations)).

## API overview

Everything external goes through the gateway (default `http://localhost:8000`). The gateway
serves no OpenAPI or docs pages of its own; its only own routes are `/health` and an
internal-only `/metrics`.

| Gateway route | Owning service | Notes |
|---|---|---|
| `POST /api/auth/register` | Identity | Public; always creates a `patient` account |
| `POST /api/auth/login`, `/refresh`, `/logout` | Identity | Access (~15 min) and refresh (~7 days) tokens; logout denylists the `jti` |
| `POST /api/auth/admin/users` | Identity | Admin only; creates staff/provider/admin accounts |
| `GET /api/admin/ping` | Identity | Admin-only RBAC probe |
| `GET /.well-known/jwks.json` | Identity | Public RS256 keys |
| `/api/clinics`, `/api/clinics/{id}`, `.../deactivate` | Organization | Admin only; create, list, get, PATCH, soft-delete |
| `/api/clinics/{clinic_id}/departments[...]` | Organization | Admin only; same verbs |
| `/api/providers`, `/api/providers/{id}`, `.../deactivate` | Provider | Admin only; providers with specialties |
| `/api/providers/{id}/availability[...]` | Provider | Provider (own) or admin; weekly availability templates |
| `/api/patients`, `/api/patients/{id}`, `.../deactivate` | Patient | Front-desk (clinic-scoped) or admin; the only PHI |
| `/health` | each service / gateway | Liveness |

Not routed by the gateway (they return `404` at the edge): `/internal/*` (reference lookups
between services) and any path containing dot segments, encoded slashes or NUL. The Scheduling
service has no public routes yet. Soft-delete is `is_active=false` plus `deactivated_at`;
deactivating something already inactive returns `200` as a no-op.

Behaviour change from Phase 1: creating a provider under a missing (or inactive) department
now returns **`422`** (it was `404`), because the department is a cross-service reference
checked at write time. An unreachable owning service returns `503`.

Gateway rate limits are per client IP, fixed one-minute windows: 20 requests/min for login,
register and refresh; 300/min for everything else. Rate limiting fails **open** if Redis is
unreachable (a warning log and the `gateway_ratelimit_fail_open_total` counter record it).

## Configuration

Names and defaults only; values shown are local-development placeholders. Never commit real
credentials, and never place the private key in an environment variable or image.

| Variable | File | Default | Purpose |
|---|---|---|---|
| `ENVIRONMENT` | `.env` | `development` | Runtime environment label |
| `IDENTITY_DATABASE_URL` | `.env` | `postgresql+asyncpg://identity_svc:<password>@localhost:5433/identity_db` | Identity's own database (host runs) |
| `ORGANIZATION_DATABASE_URL` | `.env` | `...organization_svc:<password>@localhost:5433/organization_db` | Organization's own database |
| `PROVIDER_DATABASE_URL` | `.env` | `...provider_svc:<password>@localhost:5433/provider_db` | Provider's own database |
| `PATIENT_DATABASE_URL` | `.env` | `...patient_svc:<password>@localhost:5433/patient_db` | Patient's own database (the only one holding PHI) |
| `SCHEDULING_DATABASE_URL` | `.env` | `...scheduling_svc:<password>@localhost:5433/scheduling_db` | Scheduling's own database |
| `IDENTITY_DB_PASSWORD`, `ORGANIZATION_DB_PASSWORD`, `PROVIDER_DB_PASSWORD`, `PATIENT_DB_PASSWORD`, `SCHEDULING_DB_PASSWORD` | `infra/.env` | `changeme-<service>-local-dev-only` | Role passwords set by the `db-init` job; Compose builds each container's `<SERVICE>_DATABASE_URL` from them using service DNS names |
| `MONGO_URL` | `.env` | `mongodb://localhost:27017` | Shared audit sink (Compose hard-wires the container-network value) |
| `MONGO_DB_NAME` | both | `smarthealth` | Shared audit database name |
| `REDIS_URL` | `.env` | `redis://localhost:6379/0` | Shared JWT denylist and rate-limit counters |
| `OTLP_ENDPOINT` | `.env` | `http://localhost:4317` | OpenTelemetry / Jaeger OTLP gRPC |
| `JWT_PRIVATE_KEY_PATH` | `.env` | `infra/secrets/jwt_private.pem` | Identity's RS256 private key **file** (generate with `scripts/generate_jwt_keypair.py`; gitignored) |
| `JWT_KEY_ID` | both | `smarthealth-identity-1` | `kid` published in JWKS |
| `ACCESS_TOKEN_EXPIRE_MINUTES` / `REFRESH_TOKEN_EXPIRE_DAYS` | `.env` (optional) | `15` / `7` | Identity token lifetimes |
| `SERVICE_TOKEN_EXPIRE_SECONDS` | `.env` (optional) | `60` | Lifetime of Identity-minted service tokens (internal calls with no user) |
| `IDENTITY_SERVICE_URL`, `ORGANIZATION_SERVICE_URL`, `PROVIDER_SERVICE_URL`, `PATIENT_SERVICE_URL`, `SCHEDULING_SERVICE_URL` | `.env` (optional) | `http://<service>:8000` | Internal base URLs (container-network names) |
| `JWKS_CACHE_TTL_SECONDS` / `JWKS_MISS_BACKOFF_SECONDS` / `INTERNAL_HTTP_TIMEOUT_SECONDS` | `.env` (optional) | `300` / `30` / `2.0` | JWKS cache and internal HTTP behaviour |
| `GATEWAY_HOST_PORT` | `infra/.env` | `8000` | The only published application port |
| `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` | both | `300` | Per-IP default budget (one-minute window) |
| `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE` | both | `20` | Per-IP budget for login/register/refresh |
| `GATEWAY_UPSTREAM_TIMEOUT_SECONDS` | `.env` (optional) | `10` | Per upstream call timeout (`504` at the edge) |
| `GATEWAY_METRICS_PORT` | `.env` (optional) | `9100` | Internal-only `/metrics` listener; Compose does not publish it |
| `SMARTHEALTH_GATEWAY_URL` | `.env` (tests) | `http://localhost:8000` | Target of `tests/e2e/test_gateway_smoke.py` |
| `SMARTHEALTH_E2E_REQUIRE_STACK` | tests only | unset | `1` makes the smoke test fail instead of skip when no stack is up |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD` | shell only | none | Inputs to `scripts/bootstrap_admin.py`; never put in a file |
| `POSTGRES_USER/PASSWORD/DB`, `RABBITMQ_DEFAULT_USER/PASS`, `KAFKA_CLUSTER_ID`, `GF_SECURITY_ADMIN_PASSWORD`, `*_HOST_PORT` | `infra/.env` | unchanged | Phase 1 infrastructure variables |

Retired in Phase 1.1 and no longer read by any service: a shared `DATABASE_URL`, any shared
JWT secret (`JWT_SECRET_KEY`), `JWT_ALGORITHM`, and the old single-API host port
(`API_HOST_PORT`). If they appear in an older `.env`, delete them.

> **Open housekeeping:** `.env.example` and `infra/.env.example` have not yet been updated to
> match this table, and still need a manual edit by a person.

Host ports (defaults in `infra/docker-compose.yml`, overridable through `infra/.env`):
gateway 8000, PostgreSQL 5433, MongoDB 27017, Redis 6379, RabbitMQ 5672 / UI 15672,
Kafka 9092, Schema Registry 8081, Temporal 7233 / UI 8080, Jaeger UI 16686,
Prometheus 9090, Grafana 3000.

## Technology stack

Python 3.12 · FastAPI · SQLAlchemy 2.0 async + asyncpg · Alembic (per service) · PostgreSQL ·
MongoDB (async PyMongo) · Redis · python-jose (RS256) · httpx · structlog · OpenTelemetry →
Jaeger (OTLP) · Prometheus + Grafana · Docker Compose. Present as scaffolding for later
phases: Temporal, Kafka + Schema Registry, Celery + RabbitMQ.

## Known limitations

Recorded risks and deferrals at the close of Phase 1.1 (details in
`.planning/phases/01.1-microservice-re-architecture/deferred-items.md`):

- The gateway buffers whole request bodies with **no size cap** (the rate limiter bounds
  request count, not size).
- One **shared Redis** holds both the JWT denylist and the rate-limit counters. An outage fails
  the limiter open and token verification closed (`503`); a flush would drop revocations.
- Service tokens carry **no `aud` claim**; to be fixed before Phase 2 Scheduling calls Provider.
- Application containers **run as root**.
- Synchronous reference checks make a write depend on the owning service being up (removed by
  event-fed local copies in Phase 3). Real-socket outage behaviour, a cold start from an empty
  volume, and a person viewing the connected trace in the Jaeger UI have **not been verified**.
- Assumptions awaiting mentor confirmation: the provider → department → clinic scope chain
  holds only through write-time validation (ORG-03); overlapping availability templates remain
  allowed (PPL-03); a client combining services may briefly see stale cross-service state (PPL-04).
- Key rotation is supported through `kid` but has not been exercised.
