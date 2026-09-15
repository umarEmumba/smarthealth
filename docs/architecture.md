# SmartHealth — Architecture (Part A / Phase 1)

Foundational healthcare-operations backend: identity & RBAC, patient/provider/clinic/department
management, an append-only audit trail with no PHI in logs, first-class tracing, and the full
mandated stack running locally with health checks.

> **Scope note.** Phase 1 delivers the **core platform** and the **observability spine**. The
> event/orchestration components (Kafka, RabbitMQ, Temporal) are stood up in Docker Compose but are
> **scaffolding only** this phase — they are wired into request/workflow flow in Phase 2 (Temporal
> booking sagas) and Phase 3 (Kafka event backbone + Celery workers). The diagrams below label this
> distinction explicitly.

---

## 1. System / Component Diagram

```mermaid
flowchart TB
    subgraph clients["Clients (role-scoped)"]
        C1["Patient"]
        C2["Front-desk"]
        C3["Provider"]
        C4["Admin"]
    end

    subgraph api["API Process — FastAPI (gunicorn + uvicorn workers) :8000"]
        direction TB
        MW["OTel instrumentation · Prometheus /metrics · structlog (PHI-safe)"]
        subgraph sec["Security layer"]
            direction LR
            JWT["JWT issue/verify<br/>access ~15m · refresh ~7d<br/>(python-jose)"]
            RBAC["RBAC require_role<br/>patient · front_desk · provider · admin"]
            CU["current_user<br/>signature + denylist check"]
        end
        subgraph routers["Routers"]
            direction LR
            R1["/api/auth/*"]
            R2["/api/clinics · /departments"]
            R3["/api/patients · /providers · /availability"]
            R4["/api/admin/ping"]
        end
    end

    subgraph data["Data stores (wired — Phase 1)"]
        direction LR
        PG[("PostgreSQL<br/>system of record<br/>SQLAlchemy 2.0 async + asyncpg")]
        MG[("MongoDB<br/>append-only audit_log<br/>PHI-safe, indexed")]
        RD[("Redis<br/>JWT jti-denylist<br/>cache · future locks")]
    end

    subgraph obs["Observability (wired — Phase 1)"]
        direction LR
        JG["Jaeger<br/>traces via OTLP :4317"]
        PR["Prometheus<br/>scrapes /metrics"]
        GF["Grafana<br/>dashboards"]
    end

    subgraph scaffold["Scaffolding only — wired in Phase 2/3"]
        direction LR
        KF["Kafka + Schema Registry<br/>(KRaft) — Phase 3 event backbone"]
        RMQ["RabbitMQ<br/>Celery broker — Phase 3"]
        TMP["Temporal (+ UI)<br/>booking sagas — Phase 2"]
    end

    clients -->|HTTPS + Bearer JWT| MW
    MW --> sec
    sec --> routers
    routers -->|read/write| PG
    routers -->|audit every mutation| MG
    sec -->|revocation check| RD
    api -.->|spans| JG
    PR -->|scrape| api
    GF --> PR
    api -.->|not yet wired| scaffold

    classDef wired fill:#e6f4ea,stroke:#34a853,color:#0b3d1f;
    classDef pending fill:#fef7e0,stroke:#f9ab00,color:#5b4300,stroke-dasharray:5 4;
    class PG,MG,RD,JG,PR,GF wired;
    class KF,RMQ,TMP pending;
```

---

## 2. Request Flow — the end-to-end tracer

This is the "thinnest path through every layer" proven in Plan 03 (the Wave 3 tracer). Every later
route reuses this exact spine.

```mermaid
sequenceDiagram
    autonumber
    participant U as Client
    participant API as FastAPI
    participant SEC as Security (JWT/RBAC)
    participant RD as Redis (denylist)
    participant PG as PostgreSQL
    participant MG as MongoDB (audit)
    participant JG as Jaeger (OTel)

    U->>API: POST /api/auth/login (email, password)
    API->>PG: fetch user by email
    API->>API: bcrypt verify (constant-time even if user absent)
    API-->>U: 200 { access_token, refresh_token }

    U->>API: GET protected route (Authorization: Bearer <access>)
    API->>SEC: validate token
    SEC->>SEC: verify signature + claims (sub, role, clinic_id, jti)
    SEC->>RD: is jti denylisted?
    RD-->>SEC: no
    SEC->>SEC: require_role(...) — RBAC gate
    API->>PG: read / write domain data
    API->>MG: append audit_log entry (actor, action, resource, trace_id) — PHI-free
    API-->>U: 200 (+ PHI-safe structured log line carrying trace_id)
    API-->>JG: export span (OTLP)

    Note over U,API: Logout / revoke → jti added to Redis denylist (auto-expires at token TTL)
```

---

## 3. Data Model (PostgreSQL)

Key decision: **`users` and `patients` are separate tables** (`patients.user_id` is nullable) so a
front-desk-created patient profile need not own a login account (PPL-01), while self-registration
still works (IAM-01). Availability is stored as **weekly templates**; `provider_slots` is created
**empty** but carries a **live GiST exclusion constraint** so Phase 2's booking saga inherits a
table on which double-booking is physically impossible. All operational entities carry
`is_active` / `deactivated_at` for **soft-delete** (no hard deletes — records stay auditable).

```mermaid
erDiagram
    USERS ||--o| PATIENTS : "may own (nullable)"
    USERS ||--o| PROVIDERS : "may own (nullable)"
    CLINICS ||--o{ DEPARTMENTS : contains
    CLINICS ||--o{ PROVIDERS : "scopes (ORG-03)"
    DEPARTMENTS ||--o{ PROVIDERS : "assigned to"
    PROVIDERS }o--o{ SPECIALTIES : has
    PROVIDERS ||--o{ AVAILABILITY_TEMPLATES : "weekly windows"
    PROVIDERS ||--o{ PROVIDER_SLOTS : "empty in P1; GiST no-overlap"

    USERS {
        uuid id PK
        string email UK
        string password_hash
        enum role "patient|front_desk|provider|admin"
        bool is_active
    }
    PATIENTS {
        uuid id PK
        uuid user_id FK "nullable"
        uuid clinic_id FK
        bool is_active
        timestamptz deactivated_at
    }
    PROVIDERS {
        uuid id PK
        uuid user_id FK "nullable"
        uuid clinic_id FK
        uuid department_id FK
        bool is_active
    }
    PROVIDER_SLOTS {
        uuid id PK
        uuid provider_id FK
        tstzrange window "EXCLUDE USING gist (provider_id =, window &&) WHERE status<>'cancelled'"
        string status
    }
    AVAILABILITY_TEMPLATES {
        uuid id PK
        uuid provider_id FK
        int day_of_week "0-6"
        time start_time
        time end_time
    }
    CLINICS { uuid id PK
        string name
        bool is_active }
    DEPARTMENTS { uuid id PK
        uuid clinic_id FK
        string name }
    SPECIALTIES { uuid id PK
        string name }
```

---

## 4. Technology Stack

| Concern | Technology | Role in Phase 1 |
|---|---|---|
| API framework | FastAPI + Pydantic v2 (gunicorn / uvicorn workers) | HTTP surface, validation |
| System of record | PostgreSQL 15 · SQLAlchemy 2.0 async · asyncpg · Alembic | Durable relational state + migrations |
| Audit trail | MongoDB 8 (async PyMongo) | Append-only, PHI-safe `audit_log` |
| Cache / revocation | Redis 8 | JWT `jti`-denylist; cache; future slot locks |
| AuthN / AuthZ | python-jose (JWT) · bcrypt · custom RBAC | Access+refresh tokens, 4-role RBAC |
| Tracing | OpenTelemetry SDK → OTLP → Jaeger | Distributed traces, `trace_id` in logs |
| Metrics | Prometheus + Grafana | `/metrics` scrape + dashboards |
| Logging | structlog (JSON, PHI-safe) | Correlated logs (trace/span IDs) |
| **Scaffolding (P2/P3)** | Kafka + Schema Registry, RabbitMQ, Temporal | Event backbone, workers, booking sagas |

### Local ports (Docker Compose)

| Service | Host port | Service | Host port |
|---|---|---|---|
| API (Swagger `/docs`) | 8000 | Temporal | 7233 |
| PostgreSQL | 5433 → 5432 | Temporal UI | 8080 |
| MongoDB | 27017 | Jaeger UI | 16686 |
| Redis | 6380 → 6379 | Prometheus | 9090 |
| RabbitMQ / mgmt | 5672 / 15672 | Grafana | 3001 → 3000 |
| Kafka | 9092 | Schema Registry | 8081 |

---

## 5. Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Auth & session | Access (~15m) + refresh (~7d) JWT; Redis `jti`-denylist revocation | Real logout/revocation (IAM-03); short access TTL bounds a leaked token; no new infra (Redis already mandated) |
| Slot model | Weekly templates now; empty `provider_slots` with live GiST exclusion constraint | Phase 1 stays config-only; Phase 2 inherits a proven double-booking invariant |
| Identity vs profile | Separate `users` / `patients` (nullable `patients.user_id`) | Satisfies both self-registration (IAM-01) and staff-created no-login profiles (PPL-01) |
| Deletion | Soft-delete (`is_active` / `deactivated_at`), never hard delete | Healthcare records must remain auditable and retained |
| PHI safety | Field-name-only audit; structlog PHI redaction; tests assert no PHI in logs | HIPAA — no PHI in logs or traces |
| Async everywhere | SQLAlchemy 2.0 async + asyncpg; async PyMongo; `redis.asyncio` | Sustains concurrent load without blocking the event loop |

See [`architecture-dashboard.html`](./architecture-dashboard.html) for the interactive decision record,
and [`architecture.html`](./architecture.html) for a standalone diagram view.
