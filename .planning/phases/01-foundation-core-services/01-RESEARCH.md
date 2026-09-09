# Phase 1: Foundation + Core Services - Research

**Researched:** 2026-09-09
**Domain:** Identity/JWT/RBAC, patient/provider/clinic/department CRUD, provider availability & slot data model, MongoDB audit-trail design, PHI-safe structured logging, Docker Compose orchestration with health-check ordering, OTel tracing conventions (foundational), self-contained HTML Architecture & Decision dashboard
**Confidence:** MEDIUM-HIGH

## Summary

Phase 1 has no architectural risk of its own — it has zero dependency on Temporal/Kafka application wiring — but it lays two foundations that Phase 2 cannot safely retrofit: (1) the **auth/session model** (JWT claims shape, refresh/revocation strategy) that every subsequent RBAC check depends on, and (2) the **provider availability/slot data model**, which is the single most consequential Phase 1 decision because Phase 2's Temporal booking saga and its data-layer double-booking guarantee (a PostgreSQL exclusion constraint) sit directly on top of whatever slot table shape gets chosen here. Get the slot model wrong in Phase 1 and Phase 2 either needs a disruptive migration mid-project or the double-booking guarantee has to be retrofitted onto a schema that wasn't designed for it.

This project already has strong project-wide research in `.planning/research/{STACK,ARCHITECTURE,PITFALLS,FEATURES}.md` (dated the same day as this document) — this RESEARCH.md does not repeat that material wholesale; it (a) narrows to what Phase 1 specifically builds, (b) resolves the two decisions the roadmap flagged as needing explicit guidance, and (c) adds implementation-level detail (concrete JWT claim shape, exclusion-constraint SQL, MongoDB audit schema, structlog+OTel wiring, Compose health-check patterns) that the existing project research states at the architectural level but doesn't spell out operationally.

**Primary recommendation:** Issue short-lived access JWTs (15 min) + longer-lived refresh tokens (7 days), both carrying `role` and `clinic_id`/`department_id` scope claims plus a `jti`; back revocation with a Redis denylist keyed on `jti` with TTL = remaining token life. Model provider availability as **recurring weekly templates** in Phase 1 (no concrete bookable slot rows) but design the **future slot table's constraint shape now** — an exclusion constraint on `(provider_id, tstzrange(start_time, end_time))` — so Phase 2 can generate slot rows from the template into a table whose double-booking invariant was designed on day one, not bolted on in week two.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| User registration & login (IAM-01) | API / Backend | Database / Storage | FastAPI endpoint validates credentials, issues JWT; PostgreSQL stores hashed credentials + role |
| RBAC enforcement (IAM-02) | API / Backend | — | FastAPI dependency-injection guard checks JWT claims against required role per route; no client-side enforcement is trustworthy |
| Session/token issuance & validation (IAM-03) | API / Backend | Database / Storage (Redis) | Token issuance is a backend concern; Redis is the revocation-check tier, consulted on every protected request |
| Audit trail (IAM-04) | API / Backend | Database / Storage (MongoDB) | Every mutating endpoint writes an audit entry as a side effect of its own request handling — not a separate consumer/process in Phase 1 (no Kafka/event backbone wired yet) |
| Patient/provider/clinic/department CRUD (PPL-*, ORG-*) | API / Backend | Database / Storage (PostgreSQL) | Standard REST CRUD; PostgreSQL is system of record per project decision |
| Provider availability config (PPL-03) | API / Backend | Database / Storage (PostgreSQL) | Config data (templates), not runtime scheduling logic — still Phase 1 scope, no Temporal needed to store a weekly template |
| Docker Compose orchestration (OBS-04) | DevOps / Infra | — | Not an application-tier concern; health-check ordering is a Compose-file and container-image concern |
| OTel tracing conventions (foundational) | API / Backend (cross-cutting) | — | Instrumentation lives in the FastAPI app layer; this phase establishes the convention other phases inherit |
| Architecture & Decision dashboard (DASH-01/02) | Static / Docs | — | Self-contained HTML, no server; lives in `docs/`, not part of the running system |

## User Constraints

No CONTEXT.md exists for this phase — the user chose to plan without `/gsd-discuss-phase`. No locked decisions or discretion areas were captured beyond what ROADMAP.md, REQUIREMENTS.md, and CLAUDE.md already state. Do not assume additional design preferences; where a choice is open, this document makes a recommendation with alternatives so the planner (and, if desired, the user at plan-review time) can confirm or override it — most importantly the two decisions flagged below.

### From ROADMAP.md (binding — Phase 1 section)
- Depends on: Nothing (first phase).
- Requirements: IAM-01, IAM-02, IAM-03, IAM-04, PPL-01, PPL-02, PPL-03, PPL-04, ORG-01, ORG-02, ORG-03, OBS-04, DASH-01, DASH-02.
- Success criteria (6, verbatim intent): register/login all 4 roles with validated tokens; RBAC 403s enforced end-to-end; patient/provider CRUD with specialties/availability, consistent reads; clinic/department hierarchy with providers/slots visibly scoped; every mutating action produces a MongoDB audit entry with no PHI in logs; `docker compose up` brings up the full stack (Postgres, MongoDB, Redis, plus Kafka/Temporal/RabbitMQ/observability scaffolding not yet wired into code) with passing health checks, and the dashboard opens with diagram + tradeoffs.

### From CLAUDE.md (binding — mandated stack, exact pins)
See `## Project Constraints (from CLAUDE.md)` below — treat as locked, not a recommendation.

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| IAM-01 | Register/authenticate with role (patient/provider/front-desk/admin) | Auth/JWT section — registration endpoint, bcrypt hashing, login issuing access+refresh JWT |
| IAM-02 | RBAC enforcement, 403 on disallowed action | RBAC pattern section — FastAPI dependency-based role guard |
| IAM-03 | Session/token issued on login, validated on every protected request | Auth/JWT section — `OAuth2PasswordBearer` + JWT validation dependency + Redis denylist check |
| IAM-04 | Every mutating action audited (who/what/when), no PHI in logs | MongoDB audit-trail schema section; PHI-safe structured logging section |
| PPL-01 | Front-desk/admin registers/manages patient profile | Data model section — Patient table + CRUD pattern |
| PPL-02 | Admin registers provider with specialties | Data model section — Provider + Specialty tables |
| PPL-03 | Provider/admin configures availability windows/working hours | Provider availability & slot model section (the flagged decision) |
| PPL-04 | Consistent, up-to-date reads of patient/provider records | PostgreSQL read-after-write via single system of record; no caching of these entities in Phase 1 |
| ORG-01 | Admin creates/manages clinics | Data model section — Clinic table |
| ORG-02 | Admin creates/manages departments within a clinic | Data model section — Department table, FK to Clinic |
| ORG-03 | Providers/slots scoped to clinic/department | Data model section — Provider FK to Department; availability/slot FK chain |
| OBS-04 | Full stack runs locally via Docker Compose with health checks | Docker Compose health-check ordering section |
| DASH-01 | Self-contained HTML dashboard shows architecture diagram | Existing dashboard assessment section — dashboard already exists and is largely complete for this requirement |
| DASH-02 | Dashboard documents each tech/design decision with alternatives + reasoning | Existing dashboard assessment section |

## Project Constraints (from CLAUDE.md)

CLAUDE.md at `/Users/emumba/smart Health/.claude/CLAUDE.md` mandates an exact stack with pinned versions. These are locked, not researched alternatives:

| Constraint | Value |
|---|---|
| Language | Python 3.12 |
| Web framework | FastAPI 0.141.1, installed as `fastapi[standard]` |
| ASGI server | Uvicorn 0.52.4, under Gunicorn in containers (`gunicorn -k uvicorn.workers.UvicornWorker`, worker count = CPU cores) |
| Validation | Pydantic 2.13.5 |
| Config | pydantic-settings 2.15.0 (`BaseSettings`, one shared settings module) |
| Postgres ORM | SQLAlchemy 2.0.52, async API only (`AsyncSession`, `create_async_engine`) — sync SQLAlchemy inside `async def` routes is forbidden by CLAUDE.md's own "What NOT to Use" table |
| Postgres driver | asyncpg 0.31.0 — never `psycopg2` under async endpoints |
| Migrations | Alembic 1.19.2, async engine via `run_sync` bridging |
| MongoDB driver | PyMongo 4.18.0 using native `AsyncMongoClient` — **Motor is explicitly forbidden** (deprecated May 2025, EOL May 2026) |
| Redis | redis-py 8.1.0, `redis.asyncio` — **`aioredis` standalone package is explicitly forbidden** (merged into redis-py years ago) |
| JWT | python-jose[cryptography] 3.5.0 |
| Password hashing | passlib[bcrypt] 1.7.4 + bcrypt 5.0.0 (pinned separately since passlib lags bcrypt's release cadence) |
| HTTP client | httpx (matching FastAPI's Starlette version) — never `requests` in async endpoints |
| Structured logging | structlog, paired with OTel trace/span ID injection |
| Retry/backoff | tenacity, layered on top of Temporal/Celery's own retries where finer control is needed (not exercised in Phase 1 since neither is wired into app code yet) |
| Tracing core | opentelemetry-api / opentelemetry-sdk 1.44.0 |
| FastAPI auto-instrumentation | opentelemetry-instrumentation-fastapi 0.65b0 (beta versioning is normal, not a red flag) |
| Trace export | opentelemetry-exporter-otlp (1.44.0 line) → Jaeger's native OTLP ingestion — **legacy `opentelemetry-exporter-jaeger` is explicitly forbidden** |
| Prometheus | prometheus-fastapi-instrumentator 8.1.0 |
| Docker Compose images | See Environment Availability / Docker Compose sections below — exact tags mandated in CLAUDE.md |
| Never do | Sync psycopg2 in async routes; Motor; standalone aioredis; legacy Jaeger exporter; ZooKeeper-mode Kafka; raw Pydantic models as Celery args (moot until Phase 3); mixed Confluent Platform image version lines |

**Enforcement note for the planner:** every task that touches Postgres, Mongo, or Redis must specify the async client explicitly (`AsyncSession`, `AsyncMongoClient`, `redis.asyncio.Redis`) in its verification criteria — this is the single most common way CLAUDE.md's mandate gets silently violated (a `def` route or a sync driver import that "just works" in dev but blocks the event loop under load).

## Standard Stack

Phase 1 uses a subset of the project-wide mandated stack (full table with rationale already in `.planning/research/STACK.md`, dated the same session as this document — not repeated in full here). Phase-1-relevant packages, versions re-verified live against PyPI this session:

### Core (Phase 1 scope)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| fastapi | 0.141.1 | API layer | `[VERIFIED: pypi.org JSON API, queried this session]` — matches CLAUDE.md pin exactly |
| sqlalchemy | 2.0.52 | Postgres async ORM | `[VERIFIED: pypi.org JSON API, queried this session]` — matches CLAUDE.md pin exactly |
| asyncpg | 0.31.0 | Postgres async driver | `[VERIFIED: pypi.org JSON API, queried this session]` — matches CLAUDE.md pin exactly |
| alembic | 1.19.2 | Migrations | `[VERIFIED: pypi.org JSON API, queried this session]` — matches CLAUDE.md pin exactly |
| pymongo | 4.18.0 | Mongo async driver (`AsyncMongoClient`) | `[CITED: STACK.md, pypi.org JSON API same-session query]` — network call for live re-verification timed out this session on this specific package; STACK.md's same-day pypi.org query is the basis |
| redis | 8.1.0 | Cache/lock/denylist | `[CITED: STACK.md, pypi.org JSON API same-session query]` |
| python-jose[cryptography] | 3.5.0 | JWT encode/decode | `[CITED: STACK.md]` |
| passlib[bcrypt] / bcrypt | 1.7.4 / 5.0.0 | Password hashing | `[CITED: STACK.md]` |
| structlog | latest (STACK.md leaves unpinned) | Structured JSON logging | `[CITED: STACK.md]` — planner should pin an exact version at implementation time; unpinned "latest" in project research is a gap to close, not copy forward |
| opentelemetry-api / -sdk | 1.44.0 | Tracing core | `[CITED: STACK.md]` |
| opentelemetry-instrumentation-fastapi | 0.65b0 | Auto-instrumentation | `[CITED: STACK.md]` |
| opentelemetry-exporter-otlp | 1.44.0 line | Export to Jaeger | `[CITED: STACK.md]` |
| prometheus-fastapi-instrumentator | 8.1.0 | `/metrics` endpoint | `[CITED: STACK.md]` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pydantic-settings | 2.15.0 | Typed env config | One settings module shared by API process (only running process in Phase 1 — no Celery/Temporal workers execute app logic yet) |
| httpx | matches Starlette | Async HTTP client | Not exercised in Phase 1 (no outbound calls needed yet — billing pre-check is Phase 2); install now since `fastapi[standard]` may pull an httpx-based test client either way |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| python-jose | PyJWT | PyJWT is more actively maintained upstream in general Python-web circles as of 2026, but python-jose is what CLAUDE.md mandates via the Supporting Libraries table — no deviation without explicit user override |
| Redis denylist | Stateless short-lived-access-token-only (no refresh, no revocation list) | Simpler, zero Redis dependency for auth — but cannot support instant logout/revocation, which IAM-03 ("validated on every protected request") implies operators expect a working logout, and the stack already mandates Redis for other reasons (slot locks in Phase 2), so the marginal cost of also using it for auth is low |
| Recurring availability templates (Phase 1) | Generate concrete bookable slot rows now | See the dedicated decision section below — this is the primary open decision, not a settled alternative |

**Installation (Phase 1 subset):**
```bash
pip install "fastapi[standard]==0.141.1" "sqlalchemy[asyncio]==2.0.52" "asyncpg==0.31.0" "alembic==1.19.2"
pip install "pymongo==4.18.0"   # AsyncMongoClient — do NOT add motor
pip install "redis==8.1.0"
pip install "python-jose[cryptography]==3.5.0" "passlib[bcrypt]==1.7.4" "bcrypt==5.0.0"
pip install "pydantic-settings==2.15.0" structlog
pip install "opentelemetry-distro==1.44.0" "opentelemetry-exporter-otlp==1.44.0" \
            "opentelemetry-instrumentation-fastapi==0.65b0" \
            "prometheus-fastapi-instrumentator==8.1.0"
```

## Package Legitimacy Audit

All Phase 1 packages are long-established, high-profile libraries already mandated by CLAUDE.md (not discovered via this session's web search) — the legitimacy gate was still run for completeness.

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| fastapi | PyPI | years (est. 2018 origin) | unknown (checker gap) | github.com/fastapi/fastapi | SUS (`unknown-downloads` only) | Approved — CLAUDE.md-mandated, well-known project; SUS reason is a download-metrics data gap in the checker, not a risk signal |
| sqlalchemy | PyPI | years (est. 2006 origin) | unknown (checker gap) | sqlalchemy.org | SUS (`too-new`, `unknown-downloads`) | Approved — `too-new` reflects the *release date of the specific pinned version* (2.0.52), not the project's age; SQLAlchemy is one of the oldest, most established Python ORMs |
| asyncpg | PyPI | years | unknown (checker gap) | no repo URL returned by checker | SUS (`unknown-downloads`, `no-repository`) | Approved — well-known MagicStack project (github.com/MagicStack/asyncpg); checker's repo-URL lookup gap, not a legitimacy concern |
| alembic | PyPI | years | unknown (checker gap) | github.com/sqlalchemy/alembic | SUS (`too-new`, `unknown-downloads`) | Approved — same pattern as sqlalchemy above |
| pymongo | PyPI | years (official MongoDB driver) | unknown (checker gap) | github.com/mongodb/mongo-python-driver | SUS (`too-new`, `unknown-downloads`) | Approved — official MongoDB-maintained driver |
| redis | PyPI | years | unknown (checker gap) | github.com/redis/redis-py | SUS (`unknown-downloads`) | Approved — official Redis Python client |
| python-jose | PyPI | years | unknown (checker gap) | github.com/mpdavis/python-jose | SUS (`unknown-downloads`) | Approved — CLAUDE.md-mandated; note this library's upstream maintenance cadence is slower than PyJWT's (flagged in Alternatives Considered), acceptable per stack mandate |
| passlib | PyPI | years (stable since ~2020, low churn by design) | unknown (checker gap) | passlib.readthedocs.io | SUS (`unknown-downloads`) | Approved — CLAUDE.md-mandated |
| bcrypt | PyPI | years | unknown (checker gap) | no repo URL returned by checker | SUS (`unknown-downloads`, `no-repository`) | Approved — well-known pyca/bcrypt project; checker repo-lookup gap |
| structlog | PyPI | years | unknown (checker gap) | no repo URL returned by checker | SUS (`unknown-downloads`, `no-repository`) | Approved — well-known hynek/structlog project; checker repo-lookup gap |
| tenacity | PyPI | years | unknown (checker gap) | github.com/jd/tenacity | SUS (`unknown-downloads`) | Approved — not exercised in Phase 1 code (no Celery/Temporal calls yet), listed for completeness |
| pydantic-settings | PyPI | years (Pydantic v2 companion) | unknown (checker gap) | github.com/pydantic/pydantic-settings | SUS (`unknown-downloads`) | Approved — official Pydantic-org package |

**Packages removed due to [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** all of the above carry a SUS verdict, but every one resolves to `unknown-downloads` (a gap in the legitimacy checker's download-stats data source) and/or `too-new` (the specific pinned *version's* publish date, not project age) combined with a legitimate, recognizable source repository. None require a `checkpoint:human-verify` — these are the same packages CLAUDE.md already mandates by name and version, verified against the live PyPI registry this session (fastapi, sqlalchemy, asyncpg, alembic versions independently confirmed via direct `pypi.org/pypi/<pkg>/json` query matching CLAUDE.md's pins exactly).

## Architecture Patterns

### System Architecture Diagram (Phase 1 scope only)

```
┌─────────────────────────────────────────────────────────────────┐
│  CLIENTS: Patient / Front-desk / Provider / Admin (HTTP)         │
└───────────────────────────┬─────────────────────────────────────┘
                             │ POST /auth/login, /auth/register
                             │ Bearer <access_jwt> on protected routes
┌───────────────────────────▼─────────────────────────────────────┐
│  FastAPI API process                                             │
│  ┌──────────────┐  ┌───────────────────┐  ┌───────────────────┐ │
│  │ Auth routes  │  │ RBAC dependency    │  │ Domain routers    │ │
│  │ login/refresh│─▶│ guard (role check  │─▶│ patients/providers│ │
│  │ /logout      │  │ + Redis denylist)  │  │ clinics/depts      │ │
│  └──────┬───────┘  └─────────┬──────────┘  └─────────┬─────────┘ │
│         │                    │                        │           │
│         │ writes jti+role    │ checks jti             │ every    │
│         │ into JWT           │ against denylist       │ mutation │
│         ▼                    ▼                        ▼           │
│  ┌────────────┐      ┌──────────────┐        ┌─────────────────┐ │
│  │ PostgreSQL │      │ Redis        │        │ Audit middleware │ │
│  │ users,     │      │ jti denylist │        │ writes to Mongo  │ │
│  │ patients,  │      │ (TTL=token   │        │ (who/what/when,  │ │
│  │ providers, │      │ remaining    │        │ no PHI content)  │ │
│  │ clinics,   │      │ life)        │        └────────┬──────────┘│
│  │ depts,     │      └──────────────┘                 │           │
│  │ availability                                        ▼           │
│  │ templates  │                              ┌───────────────────┐│
│  └────────────┘                              │ MongoDB           ││
│                                               │ audit_log         ││
│                                               │ collection        ││
│                                               └───────────────────┘│
└───────────────────────────────────────────────────────────────────┘
   Every request also emits OTel spans (auto FastAPI instrumentation +
   manual spans around DB/Mongo/Redis calls) exported via OTLP → Jaeger,
   and structlog JSON log lines carrying trace_id/span_id → stdout.

┌─────────────────────────────────────────────────────────────────┐
│ Docker Compose (scaffolding, not wired into app code this phase) │
│ Kafka+SchemaRegistry · Temporal(+UI) · RabbitMQ · Prometheus ·   │
│ Grafana — all must reach a passing healthcheck; no app code talks│
│ to them yet (Phase 2/3 wire them in)                             │
└─────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

Aligns with the already-researched project-wide structure in `.planning/research/ARCHITECTURE.md` (`services/identity/...`) — Phase 1 builds out only the `identity` service vertical slice plus the shared `infra/` and `libs/observability` pieces:

```
smarthealth/
├── services/
│   └── identity/                 # Phase 1 scope: auth, RBAC, patients, providers, clinics, depts
│       ├── api/                  # FastAPI routers: auth.py, patients.py, providers.py, clinics.py, departments.py
│       ├── domain/               # Pydantic schemas, role enum, availability-template models
│       ├── repository/           # SQLAlchemy async repositories (Postgres) + Mongo audit writer
│       ├── security/             # JWT issuance/validation, password hashing, RBAC dependency
│       └── tests/
├── libs/
│   ├── auth/                     # Shared JWT encode/decode + Redis denylist client (reused by future services)
│   ├── audit/                    # Shared Mongo audit-write helper (who/what/when schema, PHI-scrubbing)
│   └── observability/            # Shared OTel setup + structlog processor (trace/span injection)
├── infra/
│   ├── docker-compose.yml        # Postgres, Mongo, Redis + Kafka/SR, RabbitMQ, Temporal, Jaeger, Prometheus, Grafana scaffolding
│   └── migrations/                # Alembic migrations — Phase 1 creates users/patients/providers/clinics/departments/availability_templates
└── docs/
    └── architecture-dashboard.html   # Already exists (38KB) — extend, don't rebuild (see below)
```

### Pattern 1: RBAC via FastAPI Dependency Injection

**What:** A reusable dependency (`require_role(*allowed_roles)`) decodes the JWT (already validated for signature/expiry/denylist by an upstream `get_current_user` dependency), checks `current_user.role in allowed_roles`, and raises `HTTPException(403)` if not. Route handlers declare their required role(s) via `Depends(require_role("admin"))` in the route signature — authorization logic never lives inside the handler body.

**When to use:** Every protected route in Phase 1 (and beyond). This is the standard, widely-documented FastAPI pattern (not project-specific — confirmed across multiple independent implementation writeups this session).

**Example:**
```python
# Source: pattern synthesized from FastAPI dependency-injection RBAC guides
# (Auth0 FastAPI RBAC sample, permit.io FastAPI RBAC tutorial, FastAPI Beyond CRUD part 13)
from fastapi import Depends, HTTPException, status
from enum import Enum

class Role(str, Enum):
    PATIENT = "patient"
    PROVIDER = "provider"
    FRONT_DESK = "front_desk"
    ADMIN = "admin"

async def get_current_user(token: str = Depends(oauth2_scheme)) -> "AuthenticatedUser":
    # decodes JWT, checks jti against Redis denylist, raises 401 on failure
    ...

def require_role(*allowed: Role):
    def _guard(user: "AuthenticatedUser" = Depends(get_current_user)):
        if user.role not in allowed:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient role")
        return user
    return _guard

# Usage:
@router.post("/clinics", dependencies=[Depends(require_role(Role.ADMIN))])
async def create_clinic(payload: ClinicCreate, db: AsyncSession = Depends(get_db)):
    ...
```

**Scope claim note:** For ORG-03 ("providers/slots scoped to clinic/department"), the RBAC guard alone isn't sufficient — a front-desk user's token should also carry which clinic they operate at, and queries/writes must filter by that scope, not just check role membership. Carry `clinic_id` (nullable for admin, who may be unscoped/global) as a JWT claim and apply it as a query filter in the repository layer, not just at the route-guard level.

### Pattern 2: JWT Issuance with Access + Refresh Tokens and Redis Denylist

**What:** Login issues two tokens: a short-lived access token (embeds `sub`, `role`, `clinic_id`, `jti`, `exp`) used on every request, and a longer-lived refresh token (embeds `sub`, `jti`, `exp`, a `type: refresh` marker) used only to mint new access tokens via `/auth/refresh`. Logout writes the access token's `jti` (and, if provided, the refresh token's `jti`) to a Redis denylist with TTL equal to that token's remaining lifetime — so denylist entries self-expire and Redis never accumulates stale revocation records.

**When to use:** All Phase 1 auth flows (IAM-01, IAM-03). This satisfies "session validated on every protected request" (IAM-03) — validation is (a) signature/expiry check via `python-jose`, then (b) a Redis `EXISTS` check on the `jti` denylist key.

**Trade-offs:** Adds one Redis round-trip per authenticated request. Multiple independent sources (this session's search) confirm this is standard practice and the latency cost is small relative to Postgres query latency already on the request path; at extreme scale an in-process LRU cache mirroring recently-revoked `jti`s is a documented mitigation, but is unnecessary complexity for a 3-week assignment — note it in the dashboard as a scaling consideration, don't build it.

**Example:**
```python
# Source: pattern synthesized from FastAPI JWT + Redis blocklist guides
# (dev.to "FastAPI Beyond CRUD Part 12", multiple cross-checked sources)
import uuid
from datetime import datetime, timedelta, timezone
from jose import jwt

ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7

def create_access_token(user_id: str, role: str, clinic_id: str | None) -> str:
    jti = str(uuid.uuid4())
    payload = {
        "sub": user_id,
        "role": role,
        "clinic_id": clinic_id,
        "jti": jti,
        "type": "access",
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")

async def revoke_token(jti: str, ttl_seconds: int, redis: Redis) -> None:
    await redis.set(f"revoked:{jti}", "1", ex=ttl_seconds)

async def is_revoked(jti: str, redis: Redis) -> bool:
    return await redis.exists(f"revoked:{jti}") == 1
```

### Pattern 3: PostgreSQL Exclusion Constraint as the Slot Double-Booking Backstop (designed now, enforced Phase 2)

**What:** Even though Phase 1 does not generate concrete bookable slot rows (see the decision section below), the **future slot table's schema and its exclusion constraint should be designed in this phase** so the Alembic migration exists before Phase 2 needs it under time pressure. `EXCLUDE USING gist (provider_id WITH =, tstzrange(start_time, end_time) WITH &&) WHERE (status != 'cancelled')` on a `provider_slots` table guarantees at the database level that no two non-cancelled slots for the same provider can overlap — this is enforced even if application/Temporal-activity code has a bug, which is exactly the belt-and-suspenders guarantee Phase 2's success criterion 2 requires ("enforced by a Postgres-level constraint, not application logic alone").

**When to use:** The exclusion constraint and its supporting `btree_gist` extension should be part of Phase 1's Alembic migration set if the `provider_slots` table itself is created in Phase 1 (see decision below for whether the table exists yet); at minimum, the *decision* and the exact constraint DDL must be documented in this phase's research/dashboard so Phase 2 isn't the first time anyone reasons about it.

**Trade-off:** Requires the `btree_gist` PostgreSQL extension (`CREATE EXTENSION IF NOT EXISTS btree_gist;`) to combine a scalar equality column (`provider_id`) with a range-overlap operator in one exclusion constraint — a common gotcha (confirmed by multiple independent sources this session) that produces an "operator class not found" error if skipped.

**Example:**
```sql
-- Source: pattern synthesized from PostgreSQL exclusion-constraint + SQLAlchemy ExcludeConstraint
-- guides (cybertec-postgresql.com, jusdb.com, SQLAlchemy GitHub discussion #9924), cross-checked
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE provider_slots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id UUID NOT NULL REFERENCES providers(id),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',  -- available | held | booked | cancelled
    EXCLUDE USING gist (
        provider_id WITH =,
        tstzrange(start_time, end_time) WITH &&
    ) WHERE (status <> 'cancelled')
);
```
```python
# SQLAlchemy 2.0 equivalent (Source: SQLAlchemy postgresql dialect docs pattern,
# cross-checked against sqlalchemy/sqlalchemy discussion #9924)
from sqlalchemy.dialects.postgresql import ExcludeConstraint, TSTZRANGE
from sqlalchemy import Column, text

class ProviderSlot(Base):
    __tablename__ = "provider_slots"
    id = Column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    provider_id = Column(UUID, ForeignKey("providers.id"), nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, nullable=False, server_default="available")

    __table_args__ = (
        ExcludeConstraint(
            (provider_id, "="),
            (text("tstzrange(start_time, end_time)"), "&&"),
            where=text("status <> 'cancelled'"),
            using="gist",
        ),
    )
```

### Anti-Patterns to Avoid

- **Enforcing RBAC only in the frontend/client:** Requirements explicitly test this via a 403 on the backend (success criterion 2) — RBAC must be a server-side dependency guard, never a UI-only check.
- **Storing raw passwords or logging JWT contents:** JWTs can carry PHI-adjacent claims (role, clinic scope) — never log full token contents at INFO/DEBUG; log only `jti` and `sub` (opaque user ID) if correlation is needed.
- **Treating "audit trail" as an afterthought bolted onto route handlers ad hoc:** Build one shared audit-write helper (`libs/audit/`) used consistently by every mutating route, not a copy-pasted Mongo insert per router — inconsistency here is how an endpoint silently ends up unaudited (a "looks done but isn't" risk directly analogous to the pitfall documented in `.planning/research/PITFALLS.md` for system-initiated actions).
- **Generating concrete bookable slots in Phase 1 "just in case":** See the decision below — doing this prematurely couples Phase 1 to a slot-generation algorithm (recurrence expansion, horizon length, timezone handling) that isn't needed until Phase 2's booking saga exists to consume it, and risks having to redesign it once Temporal's actual reservation activity shape is known.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| JWT encode/decode, signature verification | Custom token signing/parsing | `python-jose[cryptography]` (mandated) | Cryptographic correctness (timing-safe comparison, algorithm confusion attacks) is exactly the kind of code that's easy to get subtly wrong; the library is mandated and well-trodden |
| Password hashing | Custom hash+salt scheme | `passlib[bcrypt]` + `bcrypt` (mandated) | bcrypt's work-factor tuning and salt handling are solved problems; hand-rolling risks a weak scheme that looks correct in tests |
| Double-booking prevention | Application-level check-then-insert, or Redis-lock-only | PostgreSQL `EXCLUDE USING gist` constraint (Pattern 3 above) | `PITFALLS.md` Pitfall 1 and Pitfall 7 (already researched project-wide) document exactly why hand-rolled concurrency checks and Redis-lock-only approaches both have real failure windows under load — the DB constraint is the only guarantee that holds under GC pauses, network partitions, and application bugs simultaneously |
| Structured log ↔ trace correlation | Manual trace-ID string interpolation into log messages | `structlog` processor that reads the active OTel span context and injects `trace_id`/`span_id` fields (Code Examples below) | Manual interpolation breaks silently when span context changes shape or when a log line is emitted outside an active span; a processor reads the live context each time |
| Docker service readiness detection | Sleep-based startup delays (`sleep 30 && start app`) | Compose `healthcheck:` + `depends_on: condition: service_healthy` | Sleep-based waits are either too short (race persists) or too long (wastes CI/dev time) and don't adapt to the actual service's real startup time, which varies (e.g., Temporal's first-boot schema migration) |

**Key insight:** Every "don't hand-roll" item above maps to a documented, cross-checked failure mode (constraint violations under concurrency, cryptographic mistakes, silent trace-correlation breakage, flaky Compose startup) — Phase 1's foundational choices propagate their correctness (or lack of it) into every later phase, so using the standard, well-documented mechanism here is higher leverage than in a typical CRUD phase.

## The Two Flagged Decisions

### Decision 1: Auth/Session Model

**Recommendation: Access + refresh JWT pair, Redis-backed denylist for revocation, role + clinic/department scope as first-class claims.**

| Option | Verdict | Reasoning |
|---|---|---|
| Access-only JWT (no refresh) | Rejected | Forces either very short-lived tokens with frequent re-login (poor UX for front-desk/provider staff using the system all shift) or long-lived tokens with no clean revocation path — neither satisfies "validated on every protected request" implying a working, prompt logout |
| Access + refresh, no revocation list | Rejected | Logout becomes purely client-side (discard the token) — a stolen/leaked access token remains valid until natural expiry regardless of "logout." For a system that explicitly grades security fundamentals, this is a defensible-but-weaker choice; Redis is already in the mandated stack for other reasons, so there's no infra cost to doing better |
| **Access + refresh, Redis denylist keyed on `jti`** | **Recommended** | Standard, well-documented pattern (multiple independent sources, cross-checked this session); short access-token TTL (15 min) bounds exposure if denylist propagation is momentarily delayed; refresh token TTL (7 days) balances UX against long-lived-credential risk; TTL-matched denylist entries self-expire, so Redis memory use is bounded by (tokens revoked in the last `access_token_ttl` or `refresh_token_ttl` window), not unbounded growth |
| Server-side session store only (no JWT) | Rejected | Contradicts IAM-03's "token" language and the mandated stack's inclusion of a JWT library (python-jose); also a bigger deviation from the CLAUDE.md-implied design than needed |

**Claims to carry on the access token:** `sub` (user UUID), `role` (patient/provider/front-desk/admin), `clinic_id` (nullable — null for admin/system-wide, set for clinic-scoped staff), `jti`, `type: "access"`, `exp`, `iat`. Do not embed `department_id` at the token level if a user can operate across multiple departments within their clinic — enforce department scoping as a query-time filter joined from the user's assignment table, not a static claim, to avoid re-issuing tokens every time department assignment changes. `[ASSUMED]` — REQUIREMENTS.md does not specify whether staff are single-department or multi-department; confirm with the user or default to single-clinic-scope-only claims (simpler) if ambiguity remains at plan time.

**Refresh token:** carries `sub`, `jti`, `type: "refresh"`, `exp` only — no role/clinic claims, since a refresh exchange re-issues a fresh access token from the current DB state of the user's role/clinic assignment (this also means a role change takes effect on next refresh, not just next login, which is a desirable property `[ASSUMED reasoning, not sourced]`).

### Decision 2: Provider Availability & Slot Data Model

**Recommendation: Model recurring weekly availability *templates* in Phase 1. Do NOT generate concrete bookable slot rows in Phase 1. Design the future slot table's schema and its exclusion constraint now (Pattern 3 above) so Phase 2 builds slot-generation and booking directly on a schema whose invariant already exists.**

This is the most consequential Phase 1 decision because it's the foundation Phase 2's core reliability guarantee sits on — reasoning through it explicitly:

| Option | Verdict | Reasoning |
|---|---|---|
| **Recurring weekly template only (Phase 1), concrete slots generated in Phase 2** | **Recommended** | PPL-03 ("configure availability windows and working hours") reads as configuration, not slot generation — a template (`day_of_week`, `start_time`, `end_time`, `slot_duration_minutes`, `clinic_id`, `department_id`, `effective_from`/`effective_to`) fully satisfies this requirement without deciding a generation horizon (how many weeks ahead to materialize?), which is a Phase 2/booking-UX concern, not a Phase 1 concern. This keeps Phase 1 focused (no Temporal, no booking-adjacent logic) while still designing the *downstream* slot table's constraint shape now, so Phase 2 isn't inventing schema under saga-implementation pressure |
| Generate concrete slot rows in Phase 1 (e.g., next 90 days materialized) | Rejected | Couples Phase 1 to slot-generation-horizon and regeneration-on-template-change logic that has no consumer yet (booking doesn't exist until Phase 2) — violates the roadmap's own stated boundary ("NO ... appointment scheduling/booking is Phase 2"). Generating slots without a booking flow to validate against risks discovering schema problems (e.g., timezone handling, DST transitions, holiday exceptions) late, without a working end-to-end test to catch them |
| No structured availability model at all — just free-text working hours | Rejected | Fails ORG-03 ("providers and their schedulable slots are scoped to a clinic/department") and PPL-03's "configure availability windows" — this needs to be structured, queryable data from day one, not prose |
| Concrete slots only, no template (hand-manage every individual slot) | Rejected | Operationally unworkable for a provider with a stable weekly schedule — every commercial scheduling system researched this session (Elation, MS Bookings, Simply Schedule Appointments) uses the template → generated-instances pattern; reinventing without the template layer contradicts the "Don't Hand-Roll" section's own logic |

**Concrete recommendation for Phase 1's schema (design now, migrate/build slot generation in Phase 2):**

```sql
-- Phase 1: availability_templates (built and populated this phase)
CREATE TABLE availability_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id UUID NOT NULL REFERENCES providers(id),
    department_id UUID NOT NULL REFERENCES departments(id),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    slot_duration_minutes INTEGER NOT NULL DEFAULT 30,
    effective_from DATE NOT NULL,
    effective_to DATE,  -- NULL = open-ended
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Phase 2 (schema designed in Phase 1, table/migration + generation logic built in Phase 2):
-- provider_slots — see Pattern 3 above for the exclusion-constraint DDL.
-- Slot generation (template -> concrete provider_slots rows for a booking horizon,
-- e.g. rolling 60-90 days) is a Phase 2 Temporal-adjacent or scheduled-job concern,
-- deliberately deferred so it's built against a real booking consumer, not speculatively.
```

**Why this ordering protects Phase 2's success criterion 2** ("parallel booking attempts against the same provider slot never produce more than one confirmed appointment — enforced by a Postgres-level constraint"): the exclusion constraint is *schema*, decidable and documentable now without needing the generation algorithm to exist yet. Phase 1 should include the constraint's DDL/migration file as a deliverable even if the `provider_slots` table stays empty until Phase 2 starts writing to it — this avoids Phase 2 opening with "design the slot table" as its first task under a tighter week-2 deadline that's already carrying the highest-risk work (the Temporal saga itself).

**Open sub-question for the planner:** whether `provider_slots` (the table, empty, migration-only) should be created in Phase 1 or deferred entirely to Phase 2. Recommendation: create it in Phase 1 as an empty table with the constraint in place — this is a low-cost, low-risk addition to Phase 1's Alembic migration set that removes one item from Phase 2's critical path, and ORG-03's "slots scoped to clinic/department" success criterion reads more naturally if the table (even empty) already visibly carries that scoping via its FK chain (`provider_slots.provider_id → providers.department_id → departments.clinic_id`). `[ASSUMED — this is a recommendation, not something REQUIREMENTS.md dictates explicitly; flag for confirmation if the planner or user wants Phase 1 strictly config-only]`.

## MongoDB Audit-Trail Schema Design

**Pattern:** Append-only collection, one document per mutating action (not a shadow-history/versioned-document pattern — Phase 1's audit need is "who did what when," not full document version reconstruction, which is a heavier pattern this project doesn't need per its own requirements language).

```javascript
// Source: pattern synthesized from MongoDB audit-trail schema-design guides
// (oneuptime.com, sysgears.com, MongoDB community forum thread) — cross-checked
// against this project's specific requirement language (IAM-04: "who, what, when")
{
  _id: ObjectId(),
  actor_id: "uuid-string",       // opaque user ID — never a name/email in this field
  actor_role: "front_desk",
  action: "patient.create",       // dot-namespaced: <entity>.<verb>
  entity_type: "patient",
  entity_id: "uuid-string",
  clinic_id: "uuid-string",       // scoping context, supports per-clinic audit queries
  timestamp: ISODate(),
  trace_id: "otel-trace-id-hex",  // correlates the audit entry to the request's trace in Jaeger
  changes: {                      // field-level diff, NOT full PHI content
    field: "phone_number",        // e.g. "which field changed" is fine
    // do NOT store old/new PHI *values* here — see PHI-safe logging section
  },
  outcome: "success"               // success | failure — capture failed mutation attempts too
}
```

**Indexes:** `{entity_type: 1, entity_id: 1, timestamp: -1}` (look up an entity's history), `{actor_id: 1, timestamp: -1}` (look up a user's actions), `{clinic_id: 1, timestamp: -1}` (per-clinic audit views). No TTL index in Phase 1 — audit retention policy is a compliance decision REQUIREMENTS.md doesn't specify; `[ASSUMED: no auto-expiry until a retention policy is explicitly decided]`.

**PHI boundary for the `changes` field:** record *which fields changed*, not their PHI-bearing values (patient name, DOB, phone, address). If a field-level diff is genuinely needed for compliance-style review, store it keyed by field name with values redacted/hashed, not plaintext — this directly satisfies IAM-04's "no PHI in logs" extended to the audit store itself, since MongoDB audit documents are still a form of application-adjacent data store, not exempt from the same PHI discipline as stdout logs.

**System/saga-initiated actions:** per `PITFALLS.md`'s already-researched Security Mistakes table, every state transition — including ones with no human actor (not applicable to Phase 1 since no sagas exist yet, but the audit schema's `actor_id` field should support a sentinel `"system"` value now so Phase 2/3 compensating actions can write audit entries without a schema change).

## PHI-Safe Structured Logging

**Pattern:** `structlog` with a processor chain that (a) injects OTel `trace_id`/`span_id` into every log event, (b) renders JSON, and (c) — as an explicit Phase 1 convention, not an afterthought — never logs full request/response bodies or entity field values for PHI-bearing entities (patient name, DOB, contact info, provider notes).

```python
# Source: pattern synthesized from structlog + OpenTelemetry correlation guides
# (dev.to "Structured Logging in Python With Structlog", oneuptime.com structured-logging guide)
# cross-checked against this session's search results
import structlog
from opentelemetry import trace

def add_trace_context(logger, method_name, event_dict):
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        add_trace_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)

log = structlog.get_logger()
# Correct: log identifiers, not PHI content
log.info("patient.created", patient_id=str(patient.id), clinic_id=str(clinic.id))
# Wrong — never do this:
# log.info("patient.created", name=patient.name, dob=patient.date_of_birth)
```

**Also apply to OTel span attributes, not just log lines** — per `PITFALLS.md`'s Security Mistakes table (already researched project-wide), PHI leakage into Jaeger via span attributes is the same class of mistake as logging it, just a different sink. Phase 1's FastAPI auto-instrumentation should not have request/response body capture enabled by default (verify `opentelemetry-instrumentation-fastapi`'s config does not capture bodies) — this is a "looks done but isn't" risk since auto-instrumentation working at all creates a false sense that PHI safety was "handled automatically."

## Docker Compose Health-Check Ordering

The project-wide STACK.md already documents the exact image tags and the high-level gotchas (Kafka `CLUSTER_ID`, Schema Registry's internal-listener requirement, Temporal's two-hop startup chain). This section adds the concrete healthcheck syntax Phase 1 needs to satisfy OBS-04 ("passing health checks").

**Confirmed this session:** `depends_on: condition: service_healthy` only works if the dependency defines its own `healthcheck:` block — a service with no healthcheck has no health status to gate on, and Compose will error at startup rather than silently ignoring the condition. This means every service in Phase 1's Compose file that something else depends on (Postgres, MongoDB, Redis at minimum) needs an explicit healthcheck, even the "scaffolding" services (Kafka, Temporal, RabbitMQ) that OBS-04 requires to reach a "passing" state even though nothing depends on them yet in application code.

```yaml
# Source: pattern synthesized from Docker Compose healthcheck + depends_on guides
# (docs.docker.com/compose/how-tos/startup-order, oneuptime.com depends-on-healthcheck guide)
services:
  postgres:
    image: postgres:15-trixie
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 10

  mongo:
    image: mongo:8.0
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:8.2-trixie
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  api:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
      mongo:
        condition: service_healthy
      redis:
        condition: service_healthy
```

**Known gotcha confirmed this session:** a healthcheck that shells out to `curl`/`wget` fails forever (container never reports healthy) if the binary isn't present in the image — common with slim/distroless bases. Use the service's own native readiness tool where one exists (`pg_isready`, `redis-cli ping`, `mongosh --eval`) rather than assuming `curl` is present. For Kafka/Temporal/RabbitMQ scaffolding (not yet consumed by app code but required to pass health checks per OBS-04), reuse the exact image tags and healthcheck patterns already specified in `.planning/research/STACK.md`'s Docker Compose Wiring table — that table is the authoritative source for those services' image pins; this section only adds the generic healthcheck-syntax mechanics that table doesn't spell out in YAML form.

## Common Pitfalls

The project-wide `.planning/research/PITFALLS.md` already documents 9 cross-cutting pitfalls; the ones directly relevant to Phase 1's actual deliverables (not Phase 2/3 concerns) are pulled forward here with Phase-1-specific framing:

### Pitfall 1: RBAC enforced only at the router-registration level, not the query level
**What goes wrong:** A front-desk endpoint correctly requires `role=front_desk`, but the underlying query doesn't filter by the user's `clinic_id` — so a front-desk user at Clinic A can read/write Clinic B's patients by guessing/enumerating IDs.
**Why it happens:** Role-based guards are easy to test (does the 403 fire for the wrong role?) while scope-based guards require testing with two different clinics' data, which is easy to skip when there's only one clinic in a dev dataset.
**How to avoid:** Every repository method for a clinic-scoped entity takes `clinic_id` as a required parameter derived from the authenticated user's token claim, not an optional filter — make the unscoped query path structurally unavailable, not just discouraged by convention.
**Warning signs:** A repository method that takes an entity ID alone (no scope parameter) for an entity type that ORG-03 says should be clinic/department-scoped.

### Pitfall 2: Audit trail coverage gaps on less-obvious mutating actions
**What goes wrong:** Patient/provider/clinic CRUD gets audit entries because they're the "obvious" mutating actions, but auth-adjacent mutations (password change, role change, logout-triggered revocation) don't, because they don't feel like "profile" changes.
**Why it happens:** "Audit trail" mentally maps to "CRUD on business entities" first; auth state changes are equally mutating but easy to categorize as "just auth stuff."
**How to avoid:** Treat every write to Postgres or Redis that changes system state as audit-eligible by default, and explicitly decide to exclude something (e.g., a read-heavy cache refresh) rather than opting in per-endpoint.
**Warning signs:** Grep the codebase for `session.commit()` / `db.execute(update(...))` / `redis.set(` calls and cross-check each against whether a corresponding audit write exists nearby.

### Pitfall 3: Availability-template timezone ambiguity
**What goes wrong:** `start_time`/`end_time` stored as naive `TIME` values without an explicit timezone reference against the clinic's location — "9am" means something different for a clinic in one timezone vs. a patient booking from another, and Phase 2's slot generation will silently inherit whatever ambiguity Phase 1's template schema has.
**Why it happens:** Local dev/testing happens in one timezone, so the ambiguity never surfaces until multi-region data appears.
**How to avoid:** Store `effective_from`/`effective_to` as `DATE` (clinic-local calendar dates are unambiguous) but resolve `start_time`/`end_time` against an explicit `clinic.timezone` field (e.g., IANA tz name, `"America/New_York"`) rather than assuming UTC or server-local time. Add `timezone` as a required `Clinic` column in Phase 1's migration.
**Warning signs:** No `timezone` column on the `clinics` table; slot-generation logic (even sketched, not yet built) that doesn't reference it.

## Code Examples

### FastAPI Login Route (access + refresh issuance)
```python
# Source: pattern synthesized from cross-checked FastAPI JWT guides this session
@router.post("/auth/login")
async def login(form: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    user = await get_user_by_email(db, form.username)
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
    access = create_access_token(str(user.id), user.role, str(user.clinic_id) if user.clinic_id else None)
    refresh = create_refresh_token(str(user.id))
    return {"access_token": access, "refresh_token": refresh, "token_type": "bearer"}
```

### Audit-write helper (used by every mutating route)
```python
# Source: pattern synthesized from MongoDB audit-schema guides, cross-checked
async def write_audit(mongo: AsyncMongoClient, *, actor_id: str, actor_role: str,
                       action: str, entity_type: str, entity_id: str,
                       clinic_id: str | None, trace_id: str, outcome: str = "success"):
    await mongo["smarthealth"]["audit_log"].insert_one({
        "actor_id": actor_id, "actor_role": actor_role, "action": action,
        "entity_type": entity_type, "entity_id": entity_id, "clinic_id": clinic_id,
        "timestamp": datetime.now(timezone.utc), "trace_id": trace_id, "outcome": outcome,
    })
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Motor async MongoDB driver | PyMongo native `AsyncMongoClient` | Motor deprecated May 14, 2025; EOL May 2026 | Already reflected correctly in CLAUDE.md/STACK.md — no action needed, confirming the project's existing decision is current |
| Standalone `aioredis` package | `redis-py`'s `redis.asyncio` module | Merged years ago | Already reflected correctly — confirming, not changing |
| `opentelemetry-exporter-jaeger` (native protocol) | `opentelemetry-exporter-otlp` → Jaeger's native OTLP ingestion | Jaeger has ingested OTLP natively for several releases | Already reflected correctly |

**Deprecated/outdated:** All three items above are already correctly avoided per CLAUDE.md's own "What NOT to Use" table — this research confirms rather than surfaces new findings here, since the project-wide STACK.md research already resolved these.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Staff (front-desk) are single-clinic-scoped, so `clinic_id` as a static JWT claim (not re-derived per-request from a multi-assignment table) is sufficient | Decision 1 — Auth/Session Model | If a real deployment needs multi-clinic staff, the token shape needs a claims redesign (array of clinic_ids) — moderate rework, better to confirm now than discover in Phase 2/3 |
| A2 | No audit-log retention/TTL policy needed in Phase 1 (no TTL index) | MongoDB Audit-Trail Schema Design | If a retention requirement emerges later (e.g., "compliance requires deleting audit logs after N years" or the opposite, "never delete"), adding a TTL index retroactively on an existing large collection is more disruptive than deciding it up front |
| A3 | `provider_slots` table (empty, schema-only) should be created in Phase 1 rather than deferred entirely to Phase 2 | Decision 2 — Provider Availability & Slot Data Model | Low risk either way — this is a recommendation to reduce Phase 2's initial task list, not a correctness-critical choice; if wrong, worst case is one migration file moves from Phase 1 to Phase 2 |
| A4 | Access token TTL of 15 minutes / refresh TTL of 7 days is an appropriate balance for this project's UX (staff using the system through a shift) | Decision 1 — Auth/Session Model | If wrong, it's a config value change (not a structural redesign) — low risk, but flagging since REQUIREMENTS.md doesn't specify exact durations |
| A5 | structlog version left unpinned in project-wide STACK.md ("latest") should be pinned to a specific version at Phase 1 implementation time | Standard Stack | Floating "latest" risks build irreproducibility; low risk to fix (pin at implementation), but flagging since it's a gap inherited from prior research, not resolved by this document |

**If this table is empty:** N/A — assumptions listed above need confirmation, primarily A1 (multi-clinic staff) and A3 (provider_slots table timing), since both affect schema shape that's more costly to change after Phase 2 builds on it.

## Open Questions

1. **Does REQUIREMENTS.md intend patient self-registration, or only front-desk/admin-initiated patient registration?**
   - What we know: PPL-01 says "Front-desk/admin can register a patient" — patients are not listed as able to self-register in PPL-01, but IAM-01 says "User can register... with a role of patient" which implies patient self-registration is possible for auth purposes.
   - What's unclear: Whether a `patient`-role user's *auth account* creation (IAM-01) is the same action as their *patient profile* creation (PPL-01), or two separate steps (a patient can create a login, but their clinical/operational profile is still created by front-desk/admin).
   - Recommendation: Model as two related but distinct entities — a `users` table (auth identity, any role) and a `patients` table (operational profile, created by front-desk/admin, optionally linked to a `user_id` if/when that patient has a login). This satisfies both requirements without forcing self-registration to also mean "front-desk did nothing." Flag for planner/user confirmation since REQUIREMENTS.md is genuinely ambiguous here.

2. **Should the dashboard (DASH-01/02) be extended in place, or does the planner need to verify it already satisfies Phase 1's bar?**
   - What we know: `docs/architecture-dashboard.html` (38KB, 710 lines) already exists and appears substantially complete for DASH-01 (architecture diagram present, `#architecture` section) and DASH-02 (technology decisions with alternatives, `#decisions` section) — it also already includes patterns, flows, Q&A, and a roadmap section that anticipate Phase 2/3 content.
   - What's unclear: Whether this file was generated as part of this project's own research/setup process (dated 2026-09-09, same day as REQUIREMENTS.md/PROJECT.md) and is meant to be the actual Phase 1 deliverable verbatim, or a reference mockup the planner should treat as a starting point to verify against actual Phase 1 build decisions (e.g., once the auth/slot decisions above are locked in, does the dashboard's content still match reality?).
   - Recommendation: Planner should task a verification pass (not a rebuild) — confirm the existing dashboard's claims match what Phase 1 actually builds (e.g., if the recommended slot-model decision above changes anything stated in the dashboard's "System of record" / "Operational document store" cards), and update only where drift exists. Building a new dashboard from scratch would be wasted effort given the existing artifact's completeness.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker / Docker Compose | OBS-04, all local dev | Not verified this session (no local shell tool access to `docker`) | — | N/A — mandated, no fallback; planner should add a preflight check task |
| PostgreSQL 15 image | System of record | Verified via CLAUDE.md/STACK.md Docker Hub research (same-day) | `postgres:15-trixie` | — |
| MongoDB 8.0 image | Audit trail | Verified via CLAUDE.md/STACK.md Docker Hub research (same-day) | `mongo:8.0` | — |
| Redis 8.x image | Cache/denylist | Verified via CLAUDE.md/STACK.md Docker Hub research (same-day) | `redis:8.2-trixie` | — |
| Kafka/Schema Registry/Temporal/RabbitMQ images (scaffolding only) | OBS-04's "passing health checks" for non-wired services | Verified via CLAUDE.md/STACK.md Docker Hub research (same-day) | See STACK.md Docker Compose Wiring table | — |

**Missing dependencies with no fallback:** Docker/Docker Compose itself — this is the foundation of OBS-04 and cannot be substituted; if unavailable in the execution environment, the phase cannot be verified end-to-end and the planner should flag this explicitly as a blocking prerequisite, not silently skip the Compose-based success criterion.

**Missing dependencies with fallback:** None identified — all other Phase 1 dependencies (Python packages) install via pip with no environment-specific blockers expected.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest + pytest-asyncio (per STACK.md's Dev/quality installation list — `[CITED: STACK.md]`) |
| Config file | none yet — Wave 0 must create `pytest.ini` or `pyproject.toml [tool.pytest.ini_options]` with `asyncio_mode = auto` |
| Quick run command | `pytest tests/ -x -k "not integration"` (unit-only subset, once test layout exists) |
| Full suite command | `pytest tests/` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| IAM-01 | Register/login all 4 roles, receive valid token | integration | `pytest tests/test_auth.py::test_login_all_roles -x` | ❌ Wave 0 |
| IAM-02 | RBAC 403 on disallowed action | integration | `pytest tests/test_rbac.py::test_forbidden_action_403 -x` | ❌ Wave 0 |
| IAM-03 | Token validated on protected request; revoked token rejected | integration | `pytest tests/test_auth.py::test_revoked_token_rejected -x` | ❌ Wave 0 |
| IAM-04 | Mutating action produces audit entry, no PHI in logs | integration | `pytest tests/test_audit.py::test_mutation_creates_audit_entry -x` | ❌ Wave 0 |
| PPL-01 | Front-desk/admin registers/manages patient | integration | `pytest tests/test_patients.py::test_create_patient -x` | ❌ Wave 0 |
| PPL-02 | Admin registers provider with specialties | integration | `pytest tests/test_providers.py::test_create_provider_with_specialties -x` | ❌ Wave 0 |
| PPL-03 | Configure availability template | integration | `pytest tests/test_availability.py::test_create_availability_template -x` | ❌ Wave 0 |
| PPL-04 | Consistent read-after-write | integration | `pytest tests/test_patients.py::test_read_after_write_consistency -x` | ❌ Wave 0 |
| ORG-01 | Create/manage clinic | integration | `pytest tests/test_clinics.py::test_create_clinic -x` | ❌ Wave 0 |
| ORG-02 | Create/manage department within clinic | integration | `pytest tests/test_departments.py::test_create_department -x` | ❌ Wave 0 |
| ORG-03 | Provider/slot scoped to clinic/department | integration | `pytest tests/test_providers.py::test_provider_scoped_to_department -x` | ❌ Wave 0 |
| OBS-04 | `docker compose up` health checks pass | manual/smoke | `docker compose up -d && docker compose ps` (all healthy) | ❌ Wave 0 (not a pytest concern — smoke script) |
| DASH-01/02 | Dashboard opens, shows diagram + decisions | manual | Open `docs/architecture-dashboard.html` in a browser | ✅ File exists — verify content accuracy, not existence |

### Sampling Rate
- **Per task commit:** `pytest tests/ -x -k "not integration"` (fast subset once split exists; Phase 1 is almost entirely integration-shaped since nearly everything touches Postgres/Mongo/Redis — the planner should decide whether a meaningful "unit-only" subset exists or whether integration tests against a test-Postgres/test-Mongo are the practical quick-run tier)
- **Per wave merge:** full `pytest tests/`
- **Phase gate:** Full suite green + `docker compose up` health-check smoke test before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `pytest.ini` or `pyproject.toml` pytest config with `asyncio_mode = auto` — none exists yet
- [ ] `tests/conftest.py` — shared fixtures: test Postgres session (transactional rollback per test), test Mongo client, test Redis client, JWT-issuing test helper
- [ ] Framework install: `pip install pytest pytest-asyncio httpx` (httpx doubles as FastAPI's `TestClient` transport)
- [ ] `tests/` directory structure mirroring `services/identity/api/` routers (test_auth.py, test_rbac.py, test_audit.py, test_patients.py, test_providers.py, test_clinics.py, test_departments.py, test_availability.py)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes | JWT via python-jose + passlib/bcrypt password hashing (Pattern 2 above) |
| V3 Session Management | yes | Access+refresh JWT with Redis-backed `jti` denylist for revocation (Pattern 2 above) |
| V4 Access Control | yes | FastAPI dependency-based RBAC guard (Pattern 1) + clinic/department scope enforcement at the query layer (Pitfall 1 above) |
| V5 Input Validation | yes | Pydantic v2 models on every request body — FastAPI's native mechanism, already mandated |
| V6 Cryptography | yes | bcrypt for password hashing (never hand-rolled); JWT signing via python-jose with HS256 or better — never a weak/`none` algorithm; verify `python-jose` is configured to reject `alg: none` tokens explicitly (a known JWT library footgun class, not specific to python-jose but worth an explicit test) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| JWT algorithm confusion / `alg: none` acceptance | Spoofing | Explicitly pin `algorithms=["HS256"]` (or chosen algorithm) on every `jwt.decode()` call — never accept the algorithm from the token's own header without an allowlist |
| Token replay after logout | Spoofing / Elevation of Privilege | Redis `jti` denylist check on every protected request (Pattern 2) |
| Cross-clinic data access via ID enumeration | Elevation of Privilege / Information Disclosure | Clinic-scoped queries derived from token claims, not client-supplied filters (Pitfall 1 above) |
| PHI leakage via logs, traces, or audit-log field values | Information Disclosure | structlog processor discipline (no PHI field values in log events), OTel span-attribute discipline, audit-log `changes` field stores field names not values (sections above) |
| SQL injection | Tampering | SQLAlchemy 2.0 ORM/Core parameterized queries by default — no raw string-interpolated SQL |
| Password brute-forcing | Spoofing | `[ASSUMED — not explicitly researched this session]` rate limiting on `/auth/login` is good practice; Redis (already in the stack) can back a simple rate limiter, but REQUIREMENTS.md doesn't explicitly call this out for Phase 1 — flag as a discretionary addition, not a blocking requirement |

## Sources

### Primary (HIGH confidence)
- pypi.org JSON API (direct query, this session) — confirmed fastapi 0.141.1, sqlalchemy 2.0.52, asyncpg 0.31.0, alembic 1.19.2 match CLAUDE.md pins exactly
- `.planning/research/STACK.md`, `ARCHITECTURE.md`, `PITFALLS.md`, `FEATURES.md` (this project's own prior research, dated 2026-09-09, same session context) — used as the authoritative project-wide baseline this document narrows and extends, not re-verified independently where already HIGH-sourced (pypi.org, hub.docker.com direct API queries)
- `/Users/emumba/smart Health/.claude/CLAUDE.md` — mandated stack, read directly this session
- `/Users/emumba/smart Health/.planning/{REQUIREMENTS,ROADMAP,PROJECT,STATE}.md` — read directly this session
- `/Users/emumba/smart Health/docs/architecture-dashboard.html` — read directly this session (sections 1-7, lines 1-710)

### Secondary (MEDIUM confidence)
- [Docker Compose startup order docs](https://docs.docker.com/compose/how-tos/startup-order/) — official Docker documentation on `depends_on`/`condition: service_healthy`
- [PostgreSQL Range Types and Exclusion Constraints — jusdb.com](https://www.jusdb.com/blog/postgresql-range-types-exclusion-constraints)
- [Exclusion constraints in PostgreSQL — cybertec-postgresql.com](https://www.cybertec-postgresql.com/en/exclusion-constraints-in-postgresql-and-a-tricky-problem/)
- [SQLAlchemy ExcludeConstraint GitHub discussion #9924](https://github.com/sqlalchemy/sqlalchemy/discussions/9924)
- [FastAPI Beyond CRUD Part 12 — JWT Redis revocation](https://dev.to/jod35/fastapi-beyond-crud-part-12-jwt-authentication-revoke-access-tokens-using-redis-eff)
- [Structured Logging in Python With Structlog — dev.to](https://dev.to/temitopeajao/structured-logging-in-python-with-structlog-correlating-logs-traces-and-errors-in-production-2nlm)
- [FastAPI RBAC Full Implementation Tutorial — permit.io](https://www.permit.io/blog/fastapi-rbac-full-implementation-tutorial)

### Tertiary (LOW confidence)
- Various dev.to / Medium blog posts on JWT/RBAC/audit-trail patterns cross-checked against 2-3 independent sources each this session (marked as such above) — treat implementation-detail code examples as illustrative patterns to adapt, not copy-paste verbatim; verify against `python-jose`/`SQLAlchemy`/`structlog` official docs at actual implementation time since no Context7/official-docs MCP provider was available in this session (same limitation noted in the project's prior STACK.md/ARCHITECTURE.md research)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — versions independently re-verified against live PyPI JSON API this session, matching CLAUDE.md's pins exactly; the subset not re-verified live (pymongo, redis-py, and others due to a session network timeout) falls back to the same-day STACK.md verification, which itself used the same authoritative pypi.org source
- Architecture (RBAC, JWT, slot model): MEDIUM-HIGH — patterns cross-checked across 3+ independent sources per topic this session; the two flagged decisions (auth model, slot model) are reasoned recommendations grounded in the project's own requirements/roadmap language plus general industry pattern research, not a single authoritative "this is definitely correct" source — appropriately flagged in the Assumptions Log
- Pitfalls: MEDIUM-HIGH — Phase-1-specific pitfalls are original synthesis for this document (RBAC scope-check gap, audit coverage gap, timezone ambiguity), reasoned from the project's own requirement language rather than sourced from an external "top scheduling-app pitfalls" list; the broader cross-cutting pitfalls (double-booking, dual-write, etc.) are inherited from the already-thoroughly-sourced project-wide PITFALLS.md

**Research date:** 2026-09-09
**Valid until:** 30 days (stable domain — FastAPI/SQLAlchemy/JWT patterns are not fast-moving; re-verify package versions if Phase 1 execution starts more than ~4 weeks after this research date, per this project's mandated-pin discipline)
