# SmartHealth — Intelligent Healthcare Operations Platform (Part A)

MediNova's foundational healthcare-operations backend: a distributed platform for
patient/provider management, appointment scheduling, visit workflows, billing
pre-checks, notifications, and operational analytics.

This is Part A — the core platform. Phase 1 delivered the first working slice as a
single FastAPI service; **Phase 1.1 re-delivered it as six independently deployable
services** after the week-1 mentor review. **Phase 2 adds scheduling, visits and billing**:
slots, asynchronous booking as a Temporal saga with compensation, cancel, reschedule, a
waitlist that auto-books freed slots, the visit lifecycle with non-blocking downstream work,
and a simulated billing pre-check — run by two dedicated Temporal worker containers. The
event backbone (Kafka), Celery jobs and operational analytics are Phase 3 and are **not
built yet**.

- Architecture & decision record (open directly from disk, no server needed):
  `docs/architecture-dashboard.html`
- Service breakdown and flows: `docs/architecture.md` (Mermaid) / `docs/architecture.html`
- Per-service schemas: `docs/erd.md` / `docs/erd.html`
- Planning context: `.planning/PROJECT.md`

## What exists today

| Layer | Status |
|---|---|
| API Gateway, Identity, Organization, Provider, Patient services | Built and running in Docker Compose |
| Scheduling service (Phase 2) | Slots, appointments (202 + polling), cancel, reschedule, waitlist; booking / cancel / reschedule / waitlist-advance workflows |
| Billing service (Phase 2) | Simulated, deterministic pre-check and the billing record state machine; internal API plus a read-only `/api/billing` route |
| Visit service (Phase 2) | Check-in → in progress → completed; completion starts a downstream workflow after its commit |
| `scheduling-worker`, `visit-worker` (Phase 2) | Temporal workers (same images as their APIs, separate containers) polling `scheduling-tq` / `visit-tq` |
| PostgreSQL (one database per service), MongoDB `audit_log`, Redis | Built and in use |
| Temporal (`auto-setup`, local dev only) | In use since Phase 2: sagas, retries, Schedules (hold reaper, pending-booking starter, visit sweeper) |
| Jaeger, Prometheus, Grafana | Tracing and per-service metrics wired; Grafana dashboards not built yet; the workers expose no metrics yet |
| Kafka + Schema Registry, Celery + RabbitMQ | Containers run and report healthy; **no application code uses them yet** (Phase 3) |

This README describes what the repository contains; it does not claim the platform
is production-hardened. Known gaps are listed under [Known limitations](#known-limitations).

## Service map

```
                          clients (patient / front-desk / provider / admin)
                                              |
                                              v
          +-----------------------------------------------------------------------+
          |  API Gateway (FastAPI + httpx proxy) :8000   the only published app port|
          |  route table; /internal and unsafe paths -> 404; per-IP rate limits      |
          +---+--------+----------+----------+------------+-----------+---------+--+
              |        |          |          |            |           |         |
        /api/auth  /api/clinics /api/     /api/      /api/slots   /api/visits /api/billing
        /api/admin            providers  patients   /api/appointments
        jwks                                         /api/waitlist
              v        v          v          v            v           v         v
         +--------+ +-------+ +--------+ +-------+ +------------+ +-------+ +---------+
         |Identity| |Organiz| |Provider| |Patient| | Scheduling | | Visit | | Billing |
         |users   | |clinics| |provid. | |(PHI)  | | slots      | | visits| | records |
         |RS256   | |depts  | |templ.  | |       | | appts      | |       | | history |
         |svc-tok | |       | |        | |       | | waitlist   | |       | |         |
         +---+----+ +---+---+ +---+----+ +---+---+ +-----+------+ +---+---+ +----+----+
             |          |         |          |           |            |          |
        identity_db  org_db  provider_db patient_db scheduling_db  visit_db  billing_db
                     (one Postgres container; one database + one role per service)

                              +------------------ Temporal (temporal:7233, UI :8080) ------+
   scheduling API --start-->  | scheduling-tq: Booking, Cancel, Reschedule, WaitlistAdvance |
                              |   + Schedules: hold reaper, pending-booking starter          |
   visit API --start------->  | visit-tq: VisitCompleted + Schedule: downstream sweeper      |
                              +------------------------------------------------------------+
                                   ^ polled by                     ^ polled by
                            scheduling-worker                 visit-worker
                     (activities call Patient/Provider/      (activities call Billing
                      Organization/Billing /internal         /internal with Visit's
                      with Scheduling's service token)       service token)

   Synchronous /internal reference checks (D-05 of Phase 1.1, aud-scoped service tokens in Phase 2):
     Identity -> Organization        Patient -> Organization, Identity     Provider -> Organization, Identity
     Scheduling -> Patient, Provider, Organization, Billing                Visit -> Scheduling, Provider, Billing

   Shared infrastructure (not service databases):
     MongoDB  audit_log   - every service and worker appends audit records (ids only, never PHI)
     Redis                - JWT jti denylist + gateway rate limits
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
  job per API service.
- **Durable workflows for multi-step changes (Phase 2).** Booking, cancel, reschedule, waitlist
  advance and visit completion run as Temporal workflows in the service that owns the aggregate;
  each step is an idempotent activity, failures compensate with forward state transitions
  (never deletes), and Postgres constraints — not the workflow — are the final arbiter
  against double-booking (`uq_appointments_active_slot`).
- **Service tokens for workers.** Workflows outlive the HTTP request, so activities call
  `/internal/*` with short-lived, audience-scoped service tokens minted by Identity from a
  per-service client secret (Compose secrets; never env values or image content).

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

# Per-service client secrets for Identity's service-token mint (Phase 2). Writes
# infra/secrets/service-clients/{scheduling,visit,billing}.secret (gitignored, 0600);
# Compose delivers them as secrets. Refuses to overwrite unless --force (rotation).
python scripts/generate_service_secrets.py

docker compose -f infra/docker-compose.yml up -d --build
scripts/compose_healthcheck.sh --wait 420
```

`compose_healthcheck.sh` requires the application services and workers (gateway, identity,
organization, provider, patient, scheduling, billing, visit, `scheduling-worker`,
`visit-worker`) to be present **and** healthy, plus every other service that declares a
healthcheck. A missing required service is a failure. APIs report health on `/health`; the
two workers have no HTTP port and report health through a heartbeat file they touch every
10 s (stale after 30 s). Startup is ordered: Postgres → the one-shot `db-init` job (creates
the seven databases and roles) → each database-owning service, which runs its own Alembic
migrations before serving → the workers (after their API is healthy and Temporal is up;
they never run migrations) → the gateway.

The Temporal UI (`http://localhost:8080`, service `temporal-ui`) shows every workflow's
history: `booking-<appointment id>`, `cancel-…`, `reschedule-…`, `waitlist-advance-…`,
`visit-completed-<visit id>` and the three Schedules.

Local resource use: the stack is the infrastructure containers plus eight API containers
(two gunicorn workers each by default, `WEB_CONCURRENCY`) and two worker containers; the
application side alone uses about 2 GB of memory. Postgres runs with `max_connections=300`
because Temporal and the per-service pools exhausted the default 100; after an e2e run the
stack held 62 client connections.

### Concurrent-booking demo

One command fires N parallel bookings at one slot through the gateway and shows a single
winner (the graded evidence is the pytest concurrency suite; this is the live illustration):

```bash
# The demo sends ~2N+25 requests from one client: raise the gateway limits for the run.
GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 \
  docker compose -f infra/docker-compose.yml up -d gateway
BOOKING_DEMO_ADMIN_EMAIL=admin@example.com BOOKING_DEMO_ADMIN_PASSWORD='<password>' \
  python scripts/demo_concurrent_booking.py --parallel 50
```

It prints `202 accepted: 1`, `409 slot not avail.: 49`, the winner `confirmed` and one active
appointment on the slot, and exits non-zero on any other outcome (`--json` for a summary).
The admin falls back to `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD`.

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
# The Phase 2 suites seed several accounts per test, so start the gateway with
# GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 (and the default limit raised, for the demo test).
pytest tests/e2e -m e2e
```

`tests/e2e/test_phase2_flows.py` proves on the deployed stack: a booking is confirmed by the
containerized worker; a forced billing denial (`coverage_ref` starting `DENY-`) ends `failed`
with the slot re-offered and billing `voided`; a cancel refunds and the waitlist auto-books the
next patient; check-in → start → complete finalizes billing. `tests/e2e/test_worker_crash.py`
kills a worker mid-saga on the Compose Temporal and shows the booking still confirms.

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
| `POST /api/slots/generate`, `GET /api/slots`, `POST /api/slots/{id}/cancel` | Scheduling | Generate slots from templates (admin/front-desk, ≤31 days), search by provider and range, cancel an unbooked slot |
| `POST /api/appointments` | Scheduling | **`Idempotency-Key` header required**; writes `pending`, starts the booking saga, returns **202** with a status URL. Same key + same body replays; a different body is `422`; a taken slot is `409` |
| `GET /api/appointments/{id}`, `GET /api/appointments` | Scheduling | Poll status (`pending` → `confirmed` / `failed` with a reason) and history; list by clinic/patient/provider/range |
| `POST /api/appointments/{id}/cancel`, `.../reschedule` | Scheduling | **202**; cancel refunds before a 24 h cutoff (staff may waive), reschedule holds the new slot then swaps in one transaction |
| `/api/waitlist`, `/api/waitlist/{id}`, `.../cancel` | Scheduling | Join a provider's waitlist (optional window); a freed slot auto-books the first matching entry |
| `POST /api/visits`, `GET /api/visits`, `GET /api/visits/{id}`, `.../start`, `.../complete` | Visit | Check-in (front-desk/admin, confirmed appointments only), provider starts/completes; clinic-scoped status list for ops (polling) |
| `GET /api/billing/records?appointment_id=`, `/api/billing/records/{id}` | Billing | Read-only billing record and its status history (front-desk/admin, clinic-scoped) |
| `/health` | each service / gateway | Liveness |

Not routed by the gateway (they return `404` at the edge): `/internal/*` (reference lookups
between services) and any path containing dot segments, encoded slashes or NUL. Billing has no
write route at the edge: its records change only through Scheduling's and Visit's workflows.
Every booking-type change is asynchronous — clients poll the returned status URL; there are no
WebSocket/SSE pushes yet. Soft-delete is `is_active=false` plus `deactivated_at`;
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
| `VISIT_DATABASE_URL` / `BILLING_DATABASE_URL` | `.env` | `...visit_svc:<password>@localhost:5433/visit_db` / `...billing_svc:...` | Visit's and Billing's own databases (Phase 2) |
| `IDENTITY_DB_PASSWORD`, `ORGANIZATION_DB_PASSWORD`, `PROVIDER_DB_PASSWORD`, `PATIENT_DB_PASSWORD`, `SCHEDULING_DB_PASSWORD`, `VISIT_DB_PASSWORD`, `BILLING_DB_PASSWORD` | `infra/.env` | `changeme-<service>-local-dev-only` | Role passwords set by the `db-init` job; Compose builds each container's `<SERVICE>_DATABASE_URL` from them using service DNS names |
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
| `SERVICE_CLIENT_SECRETS_DIR` | Compose sets `/run/secrets` | `infra/secrets/service-clients` (host runs) | Identity reads `<client>.secret` here to mint service tokens |
| `SCHEDULING_CLIENT_SECRET_PATH` / `VISIT_CLIENT_SECRET_PATH` | Compose sets `/run/secrets/<svc>.secret` | `infra/secrets/service-clients/<svc>.secret` | Each caller's own client-secret **file** (Billing calls no one and has none) |
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | `.env` (host runs; Compose sets `temporal:7233`) | `temporal:7233` / `default` | Temporal frontend used by the scheduling/visit APIs and workers |
| `SCHEDULING_HOLD_TTL_MINUTES` | both | `10` | How long a booking hold lasts before the reaper may release it |
| `SCHEDULING_CANCEL_CUTOFF_HOURS` | both | `24` | Cancels earlier than this are refunded; later ones are `no_refund` unless staff waive |
| `SCHEDULING_SLOT_HORIZON_DAYS` / `SCHEDULING_SLOT_MAX_RANGE_DAYS` | `.env` (optional) | `28` / `31` | Rolling slot-generation horizon and the widest generate/search range |
| `SCHEDULING_REAPER_INTERVAL_SECONDS` / `SCHEDULING_STARTER_INTERVAL_SECONDS` / `SCHEDULING_STARTER_GRACE_SECONDS` | `.env` (optional) | `60` / `30` / `30` | Hold-reaper and pending-booking-starter Schedules |
| `VISIT_SWEEPER_INTERVAL_SECONDS` / `VISIT_SWEEPER_GRACE_SECONDS` | `.env` (optional) | `60` / `30` | Visit downstream sweeper Schedule |
| `BILLING_PRECHECK_FAIL_PREFIX` | both | `DENY-` | A `coverage_ref` with this prefix is deterministically denied (demo/test lever) |
| `BOOKING_DEMO_ADMIN_EMAIL` / `BOOKING_DEMO_ADMIN_PASSWORD` | shell only | none | Admin for `scripts/demo_concurrent_booking.py` |
| `GATEWAY_HOST_PORT` | `infra/.env` | `8000` | The only published application port |
| `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE` | both | `300` | Per-IP default budget (one-minute window) |
| `GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE` | both | `20` | Per-IP budget for login/register/refresh |
| `GATEWAY_UPSTREAM_TIMEOUT_SECONDS` | `.env` (optional) | `10` | Per upstream call timeout (`504` at the edge) |
| `GATEWAY_METRICS_PORT` | `.env` (optional) | `9100` | Internal-only `/metrics` listener; Compose does not publish it |
| `SMARTHEALTH_GATEWAY_URL` | `.env` (tests) | `http://localhost:8000` | Target of the `tests/e2e` suites and the demo script |
| `SMARTHEALTH_E2E_REQUIRE_STACK` | tests only | unset | `1` makes the smoke test fail instead of skip when no stack is up |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD` | shell only | none | Inputs to `scripts/bootstrap_admin.py`; never put in a file |
| `POSTGRES_USER/PASSWORD/DB`, `RABBITMQ_DEFAULT_USER/PASS`, `KAFKA_CLUSTER_ID`, `GF_SECURITY_ADMIN_PASSWORD`, `*_HOST_PORT` | `infra/.env` | unchanged | Phase 1 infrastructure variables |

Retired in Phase 1.1 and no longer read by any service: a shared `DATABASE_URL`, any shared
JWT secret (`JWT_SECRET_KEY`), `JWT_ALGORITHM`, and the old single-API host port
(`API_HOST_PORT`). If they appear in an older `.env`, delete them.

Host ports (defaults in `infra/docker-compose.yml`, overridable through `infra/.env`):
gateway 8000, PostgreSQL 5433, MongoDB 27017, Redis 6379, RabbitMQ 5672 / UI 15672,
Kafka 9092, Schema Registry 8081, Temporal 7233 / UI 8080, Jaeger UI 16686,
Prometheus 9090, Grafana 3000.

## Technology stack

Python 3.12 · FastAPI · SQLAlchemy 2.0 async + asyncpg · Alembic (per service) · PostgreSQL ·
MongoDB (async PyMongo) · Redis · python-jose (RS256) · httpx · structlog · Temporal
(`temporalio` 1.34.0 SDK, `auto-setup` server for local dev) · OpenTelemetry → Jaeger (OTLP) ·
Prometheus + Grafana · Docker Compose. Present as scaffolding for Phase 3: Kafka + Schema
Registry, Celery + RabbitMQ.

## Known limitations

This is a **local/dev system holding synthetic data only — never real patient data.**
Logout, PHI log-safety and database isolation are implemented but **not fully hardened**.

Phase 2 (scheduling, visits, billing):

- **Interim mechanisms that Phase 3 replaces:** the pending-booking starter and the visit
  downstream sweeper (Temporal Schedules) recover a workflow start lost between a commit and
  `start_workflow` — Phase 3 replaces them with a transactional outbox. Notification,
  follow-up and analytics steps are **stubs** (a PHI-free log line plus an audit entry) — Phase
  3 replaces them with Celery jobs and Kafka events. Status is **polling only**.
- Temporal runs from the `auto-setup` image, which is **not a production topology**
  (production would use `temporalio/server` with a separate schema job). Workflow histories
  are kept for the namespace's 24 h retention only.
- The two workers expose **no metrics** endpoint (no HTTP listener); only the APIs are scraped.
- Waitlist: matching is FIFO per provider with an optional window; at most 5 entries are
  tried per freed slot, there is no priority or notification to the matched patient, and an
  entry's `coverage_ref` is reused as stored.
- Audit writes go to Mongo after the Postgres commit and are **not atomic** with it (WR-04): a
  crash between the two can lose an audit entry (workflow steps re-audit idempotently on retry).
- The booking saga order is validate, hold, billing pre-check, confirm, and then a
  best-effort notification intent requested only after confirm (plan 02.1-07, S7). SCHED-02
  lists the notification earlier and the implementation deliberately differs: a booking that
  fails at confirm (for example a lost hold) sends no notification. Histories recorded before the
  change still replay (`workflow.patched`, proven by `tests/workflows/test_replay_booking_patches.py`).
- A freed slot is offered to the waitlist again when a direct (top-level) booking fails after
  it held the slot, for any reason except losing the race for it; a waitlist-started booking is
  decided by its parent loop (S5).
- Booking a slot whose start time has passed is rejected: `422 slot has already started` at
  the API (nothing written) and `SlotInPast` in the saga's validate step if the slot starts
  while the booking is pending (S6).
- Compensations never undo a committed booking (S3): `void_billing` and `release_slot` do
  nothing for a confirmed, checked_in, completed or no_show appointment, and an audit write
  that fails after a committed confirm, cancel, swap, check-in or complete is logged
  (`audit.write_failed`) instead of changing the outcome. The transition stays durable in
  `appointment_status_history`; the lost audit entry is the known gap until Phase 3's
  transactional outbox.
- No load test beyond the concurrency suites and the demo.

Carried from Phase 1.1 (details in
`.planning/phases/01.1-microservice-re-architecture/deferred-items.md`):

- The gateway buffers whole request bodies with **no size cap** (the rate limiter bounds
  request count, not size).
- One **shared Redis** holds both the JWT denylist and the rate-limit counters. An outage fails
  the limiter open and token verification closed (`503`); a flush would drop revocations.
- Service tokens now carry `aud` and an explicit allowed-caller list per internal route
  (fixed in Phase 2, plan 02-01).
- Application containers **run as root**.
- Synchronous reference checks make a write depend on the owning service being up (removed by
  event-fed local copies in Phase 3). Real-socket outage behaviour, a cold start from an empty
  volume, and a person viewing the connected trace in the Jaeger UI have **not been verified**.
- Assumptions awaiting mentor confirmation: the provider → department → clinic scope chain
  holds only through write-time validation (ORG-03); overlapping availability templates remain
  allowed (PPL-03); a client combining services may briefly see stale cross-service state (PPL-04).
- Key rotation is supported through `kid` but has not been exercised.
- The `scripts/bootstrap_admin.py` admin-creation branch (new admin row plus the `user.bootstrap`
  audit entry) is untested: the test harness always has an admin in `identity_db`, so only the
  no-op and missing-input paths are covered. Run it once against a fresh `identity_db` to confirm.
