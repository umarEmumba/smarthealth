# Phase 2: Scheduling + Service Workflow - Pattern Map

**Mapped:** 2026-10-12
**Files analyzed:** 52 new/modified (grouped below)
**Analogs found:** 44 exact/role-match / 52 (8 have no analog, all Temporal-specific)

All analog paths were taken from `git ls-files` output (tracked source). No gitignored mirror paths are used.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match |
|---|---|---|---|---|
| `libs/auth/service_tokens.py` (ServiceTokenProvider) | utility | request-response | `libs/service_client.py` + `services/identity/security/signing.py:70-87` | partial |
| `services/identity/api/internal.py` (+`POST /internal/service-tokens`) | route | request-response | `services/identity/api/internal.py` (self) + `services/provider/api/internal.py` | role |
| `services/identity/security/signing.py` (add `aud`) | utility | transform | same file `create_service_token` (lines 70-87) | exact (modify) |
| `libs/auth/verification.py` (`decode_token(audience=)`, `require_exp`) | utility | transform | same file | exact (modify) |
| `libs/auth/internal.py` (aud check, allowed-callers) | middleware | request-response | same file lines 73-159 | exact (modify) |
| `libs/internal_client.py` (POST/PATCH, no retries) | utility | request-response | `libs/service_client.py` lines 75-132 | role |
| `libs/temporal.py` (client factory, TQ constants, retry presets) | config/utility | event-driven | none | none |
| `libs/audit/writer.py` (+`dedupe_key`), `libs/audit/mongo_indexes.py` | utility | CRUD | same files | exact (modify) |
| `libs/config.py` (+`visit_service_url`, `billing_service_url`, temporal settings) | config | n/a | `libs/config.py:53-57` | exact (modify) |
| `services/scheduling/repository/models.py` (+Appointment, IdempotencyKey, WaitlistEntry, StatusHistory; slot cols) | model | CRUD | same file lines 36-77 | exact (modify) |
| `services/scheduling/migrations/versions/scheduling_0002_*.py` | migration | CRUD | `scheduling_0001_provider_slots.py` | exact |
| `services/scheduling/repository/{slot,appointment,idempotency,waitlist}_repo.py` | repository | CRUD | `services/patient/repository/patient_repo.py` | role |
| `services/scheduling/api/{appointments,slots,waitlist}.py` | route | request-response | `services/patient/api/patients.py` | role |
| `services/scheduling/api/internal.py` | route | request-response | `services/provider/api/internal.py` | exact |
| `services/scheduling/domain/{schemas,contracts,enums}.py` | model/schema | transform | `services/scheduling/domain/enums.py`, `services/patient/domain/schemas.py` | role |
| `services/scheduling/app.py` (mount routers, audit indexes, temporal client) | config | n/a | `services/patient/app.py` | exact (modify) |
| `services/scheduling/config.py` (+temporal, TTL, cutoff) | config | n/a | same file | exact (modify) |
| `services/scheduling/workflows/*.py`, `activities/*.py`, `worker.py` | workflow/service | event-driven | none in repo (RESEARCH Patterns 2-4, Code Ex. 3-4) | none |
| `services/visit/**` (app, config, db, api, repo, models, migration, Dockerfile) | service | CRUD | `services/patient/**` (whole skeleton) | exact |
| `services/visit/workflows/visit_completed.py`, `worker.py`, sweeper | workflow | event-driven | none | none |
| `services/billing/**` (app, config, db, api, repo, models, migration, Dockerfile) | service | CRUD | `services/provider/**` / `services/patient/**` | exact |
| `services/patient/api/internal.py` (new) | route | request-response | `services/provider/api/internal.py` | exact |
| `services/provider/api/internal.py` (+availability, by-user) | route | request-response | same file lines 36-61 | exact (modify) |
| `services/organization/api/internal.py` (+`timezone` on ClinicRef) | route | request-response | same file lines 41-44 | exact (modify) |
| `services/gateway/routing.py` (5 route rows) | config | request-response | same file lines 25-32 | exact (modify) |
| `infra/postgres-init/create-service-databases.sh` (+visit billing) | config | batch | same file line 20 | exact (modify) |
| `infra/docker-compose.yml` (visit, billing, 2 workers, db-init env) | config | n/a | compose lines 15-41, 311-322, 359-370 | exact |
| `infra/prometheus.yml` (+visit, billing jobs) | config | n/a | lines 29-31 | exact |
| `tests/scheduling/*`, `tests/visit/*`, `tests/billing/*` | test | CRUD | `tests/scheduling/test_slot_constraint.py`, `tests/provider/test_internal_providers.py` | role |
| `tests/workflows/*` | test | event-driven | none (RESEARCH Validation section) | none |
| `tests/conftest.py` (`HARNESS_SERVICES`), `tests/libs/test_service_isolation.py` maps | test | n/a | same files | exact (modify) |
| `docs/architecture-dashboard.html`, `README.md`, `docs/architecture.md`, `docs/erd.md` | docs | n/a | same files | exact (modify) |

## Pattern Assignments

### New services `services/visit/**` and `services/billing/**` (service skeleton, CRUD)

**Analog:** `services/patient/**` (app factory, config, db, api, repo) plus `services/scheduling/**` for `config.py`/`db.py`/Dockerfile. Copy the file-for-file layout: `__init__.py`, `app.py`, `config.py`, `db.py`, `Dockerfile`, `api/`, `domain/`, `repository/{models,*_repo}.py`, `migrations/{alembic.ini,env.py,script.py.mako,versions/}`.

**config.py** (`services/scheduling/config.py` lines 11-25), copy and rename; DB URL bound ONLY to `<NAME>_DATABASE_URL`, no bare `DATABASE_URL`:
```python
from pydantic import Field
from libs.config import SharedSettings

class SchedulingSettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://scheduling_svc:changeme-scheduling-local-dev-only@localhost:5433/scheduling_db",
        validation_alias="SCHEDULING_DATABASE_URL",
    )

settings = SchedulingSettings()
```
Visit/Billing: `visit_svc`/`visit_db`, `billing_svc`/`billing_db`, placeholder password `changeme-<name>-local-dev-only`.

**db.py** (`services/scheduling/db.py` lines 5-8):
```python
from libs.db.postgres import make_session_dependency
from services.scheduling.config import settings
get_db = make_session_dependency(lambda: settings.database_url)
```

**app.py** (`services/patient/app.py` lines 14-60): lifespan calls `configure_logging()` and `await ensure_audit_indexes(get_mongo_client())`; `create_app()` includes `build_health_router("<name>", lambda: settings.database_url)`, the routers, then:
```python
app.state.key_resolver = JwksClient.from_settings(settings)
app.state.internal_http_client = None   # None = default bounded client; tests inject the mesh
setup_tracing(app, service_name="smarthealth-patient")
Instrumentator().instrument(app).expose(app)
```

**Models** (`services/scheduling/repository/models.py` lines 36-47): each service declares its own `DeclarativeBase` and a per-call `_uuid_pk()` (server_default `gen_random_uuid()`); no cross-database FKs; intra-service FKs allowed.

**Migrations**: copy `services/scheduling/migrations/env.py` (async engine + `run_sync`, `target_metadata = models.<X>Base.metadata`, imports `settings` and `models` first). Revision naming `visit_0001_...`, `billing_0001_...`, `down_revision = None`.

**Dockerfile** (`services/scheduling/Dockerfile` lines 9-28): copy `libs` plus `services/__init__.py` plus only this service's package; CMD runs `alembic -c services/<name>/migrations/alembic.ini upgrade head && exec gunicorn -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2} -b 0.0.0.0:8000 services.<name>.app:app`. Workers reuse the image with a command override and must NOT run migrations (RESEARCH anti-pattern).

---

### `services/scheduling/migrations/versions/scheduling_0002_*.py` (migration)

**Analog:** `scheduling_0001_provider_slots.py` (lines 25-82).

Header conventions to copy: module docstring explaining intent, `from __future__ import annotations`, `revision: str = "scheduling_0002"`, `down_revision = "scheduling_0001"`, and a **working `downgrade()`** (global rule). Table creation uses `op.create_table(... sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")) ...)`, named `sa.CheckConstraint(..., name="ck_...")`, then `op.create_index`. Raw DDL for non-ORM constraints uses `op.execute("""ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE USING gist (...) WHERE (...)""")` (lines 67-76), which is the pattern for the patient-overlap `EXCLUDE` (needs denormalized `slot_start`/`slot_end` on `appointments`). Downgrade drops constraints, indexes, then tables, leaving `btree_gist` in place (lines 79-82). SQL for the partial unique index, `ck_provider_slots_end_after_start`, `uq_provider_slots_provider_start`, `idempotency_keys` is in RESEARCH Code Example 1; mirror it in the ORM with `Index("uq_appointments_active_slot", ..., unique=True, postgresql_where=text("status IN ('pending','confirmed')"))`.

---

### `services/scheduling/api/appointments.py`, `slots.py`, `waitlist.py` (route, request-response)

**Analog:** `services/patient/api/patients.py`

**Imports** (lines 34-63): `APIRouter, Depends, HTTPException, status`; `AsyncSession`; `AsyncMongoClient`; `write_audit`; `AuthenticatedUser`; `require_role`; `Role`; `get_mongo`; `get_internal_http_client`; `get_logger`; reference-client exceptions; `get_db`; repo module (routers call repositories, never inline ORM).

**Auth/guard + scope** (lines 185-193 and 95-111):
```python
router = APIRouter(prefix="/api/patients", tags=["patients"])
user: AuthenticatedUser = Depends(require_role(Role.FRONT_DESK, Role.ADMIN)),
db: AsyncSession = Depends(get_db),
mongo: AsyncMongoClient = Depends(get_mongo),
http: httpx.AsyncClient = Depends(get_internal_http_client),
...
_assert_clinic_scope(user, payload.clinic_id)   # non-ADMIN may act only on own clinic_id
```
Use `require_role(Role.PATIENT, Role.FRONT_DESK, Role.ADMIN)` for booking; patient-self rule resolves through Patient `by-user`.

**Reference-check + error mapping** (lines 142-152): `ReferencedResourceInvalid` becomes 422 via `reference_error_detail(exc)`; `UpstreamServiceUnavailable` becomes 503 with `"<service> service unavailable"`; both happen before any write; refusal is audited with `outcome="failure"`.

**Audit** (lines 173-182): `write_audit(mongo, actor_id=user.sub, actor_role=user.role.value, action="appointment.create", entity_type="appointment", entity_id=..., clinic_id=str(...), outcome=..., changed_fields=[names only])`.

**Status codes**: `POST /appointments` returns 202 (not 201) with `Idempotency-Key` header (`Header(alias="Idempotency-Key")`, bounded length). Missing/out-of-scope returns the identical `404 {"detail":"not found"}`. Replace the Patient-specific `IntegrityError` handling with an `is_slot_taken(IntegrityError)` helper keyed on the constraint name `uq_appointments_active_slot` (RESEARCH Pitfall 9).

PHI rule: these services hold IDs only; do not add any column named `phone`, `address`, `date_of_birth` (isolation test, Pitfall 13).

---

### `services/scheduling/repository/*_repo.py` (repository, CRUD)

**Analog:** `services/patient/repository/patient_repo.py`

Keyword-only args, explicit `clinic_id` on every lookup (lines 56-65):
```python
async def get_patient(db: AsyncSession, patient_id: uuid.UUID, clinic_id: uuid.UUID) -> Patient | None:
    result = await db.execute(
        select(Patient).where(Patient.id == patient_id, Patient.clinic_id == clinic_id)
    )
    return result.scalar_one_or_none()
```
Soft-delete/no-op idempotency (lines 112-120) is the model for cancel returning 200 on repeat. One deviation: patient_repo commits inside each function; Phase 2 accept transaction needs idempotency row plus appointment in ONE transaction (RESEARCH Pattern 1), so the new repos should flush and let the caller own `db.begin()`/commit. Use `text()` or Core `update().where(status=...).returning(...)` for the conditional `UPDATE` steps (RESEARCH Code Example 2).

---

### `services/scheduling/api/internal.py`, `services/patient/api/internal.py`, additions to Provider/Organization internal routers (route, request-response)

**Analog:** `services/provider/api/internal.py` (lines 36-61). Exact copy of shape:
```python
router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

@router.get("/providers/{provider_id}", response_model=ProviderRef)
async def lookup_provider(
    provider_id: uuid.UUID,
    caller: InternalCaller = Depends(require_internal_caller(Role.ADMIN)),
    db: AsyncSession = Depends(get_db),
) -> ProviderRef:
    provider = await provider_repo.get_provider(db, provider_id)
    if provider is None:
        log.info("internal.provider_lookup", provider_id=str(provider_id), outcome="not_found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")
```
Rules to keep: not routed by the gateway, `include_in_schema=False`, ids and flags only in the response and logs, reads not audited, one identical 404.

Clinic scope for forwarded user tokens: `clinic_scope_permits(caller, target_clinic_id)` from `libs/auth/internal.py:162-178`, used by `services/organization/api/internal.py` (imports at lines 30-33; role allowlist `Role.ADMIN, Role.FRONT_DESK`). For Phase 2 service-token callers, `clinic_scope_permits` returns True for `kind == "service"`, so internal routes called by activities must take an explicit `clinic_id` query parameter and compare it (RESEARCH G3; Patient repo has no id-only lookup: `get_patient(db, patient_id, clinic_id)`).

Additions: Patient `GET /internal/patients/{id}?clinic_id=` and `/by-user/{user_id}?clinic_id=`; Provider `/providers/{id}/availability` (reuse `provider_repo.list_availability`, `provider_repo.py:208-216`) and `/by-user/{user_id}`; Organization add `timezone: str` to `ClinicRef` (`organization/api/internal.py:41-44`, `Clinic.timezone` exists at `organization/repository/models.py:56`); Scheduling `/internal/appointments/{id}`; Billing `/internal/billing/...` (state-targeted, idempotent, `Idempotency-Key` header stored on history row).

---

### `libs/internal_client.py` (utility, request-response)

**Analog:** `libs/service_client.py` (lines 75-132) for structure; deliberately differs: no tenacity (Temporal owns retries), POST/PATCH allowed, adds `Idempotency-Key` header, bearer from `ServiceTokenProvider`.

Copy these elements: `outgoing_headers({"authorization": f"Bearer {token}"})` for trace propagation (line 94); same error contract, transport error/timeout/401/403/5xx/unreadable body maps to `UpstreamServiceUnavailable` (retryable in activity), clean 404 maps to None/missing; never log bodies, ids and outcomes only (lines 106-111, 117-122). Do NOT add write methods to `libs/service_client.py` (its header lines 22-23 and three tests assert GET-only).

---

### `libs/auth/service_tokens.py`, Identity mint endpoint, `signing.py`, `verification.py`, `internal.py` (auth, request-response)

**Analog (mint):** `services/identity/security/signing.py:70-87`
```python
def create_service_token(service_name: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": f"service:{service_name}", "type": SERVICE_TOKEN_TYPE,
        "jti": str(uuid.uuid4()), "iat": now,
        "exp": now + timedelta(seconds=settings.service_token_expire_seconds),
    }
    return _encode(payload)
```
Change: add `audience: str` parameter and `"aud": audience`. Sole current call site: `services/identity/api/auth.py:140` (`create_service_token("identity")`); update it to pass the target audience (organization or identity per call).

**Analog (verifier):** `libs/auth/internal.py:93-95` calls `decode_token(token, expected_types=INTERNAL_CALLER_TYPES, resolver=resolver, http=http)`. Add `audience` (service's own name) and `options={"require_exp": True}`; at lines 109-122 add rejection of a `type=service` token whose `aud` is missing or excludes this service, plus a per-route allowed-callers set. The `kind == "service"` return at lines 140-143 is where the allow-list check goes. User tokens carry no `aud` and keep working (default `audience=None`). Other `decode_token` call sites to keep compatible: `libs/auth/current_user.py:81`, `services/identity/api/auth.py:390,431`. Must ship in the same plan as the mint endpoint (Pitfall 2).

Tests to extend: `tests/identity/test_service_token.py`, `tests/libs/test_verification.py`.

---

### `libs/audit/writer.py` and `libs/audit/mongo_indexes.py` (utility, CRUD)

**Analog:** same files. Writer currently ends `await collection.insert_one(document)` (writer.py:83). Add optional `dedupe_key: str | None = None`: when set, `await collection.update_one({"dedupe_key": k}, {"$setOnInsert": {**document, "dedupe_key": k}}, upsert=True)` and swallow `pymongo.errors.DuplicateKeyError`. Index spec to append in `mongo_indexes.py` (follow the `index_specs` list pattern lines 34-47): a partial unique index on `dedupe_key` (`partialFilterExpression={"dedupe_key": {"$exists": True}}`); note `create_index(keys, name=...)` call at line 51 needs `unique=True, partialFilterExpression=...` kwargs, so the loop needs per-spec kwargs. Existing Phase 1/1.1 call sites stay untouched (research assumption A3 is unverified).

---

### `services/gateway/routing.py` and `libs/config.py` (config)

**Analog:** `services/gateway/routing.py:25-32` add rows `("/api/appointments", "scheduling")`, `("/api/slots", "scheduling")`, `("/api/waitlist", "scheduling")`, `("/api/visits", "visit")`, `("/api/billing", "billing")`. `resolve_upstream` does `getattr(settings, f"{upstream_name}_service_url")` (line 45), so `libs/config.py` needs `visit_service_url`/`billing_service_url` next to line 57 (`scheduling_service_url: str = "http://scheduling:8000"`). Update the module docstring that says Scheduling has no prefix. Never add an `/internal` prefix. Extend `tests/gateway/test_routing.py`.

---

### Infra: db-init, compose, prometheus (config)

**db-init** (`infra/postgres-init/create-service-databases.sh:20`): change to `SERVICES=(identity organization provider patient scheduling visit billing)`; the loop requires `VISIT_DB_PASSWORD`/`BILLING_DB_PASSWORD` (lines 24-28 fail if unset), so the `db-init` service environment in compose (around line 85 where `SCHEDULING_DB_PASSWORD` is set) must add both. `btree_gist` stays Scheduling-only (lines 55-56).

**Compose app service** (lines 311-322, patient; or 359-370, scheduling):
```yaml
  visit:
    build: {context: .., dockerfile: services/visit/Dockerfile}
    env_file: [{path: .env, required: false}]
    environment:
      <<: *app-env
      VISIT_DATABASE_URL: postgresql+asyncpg://visit_svc:${VISIT_DB_PASSWORD:-changeme-visit-local-dev-only}@postgres:5432/visit_db
    depends_on: *app-depends-on
    healthcheck: *app-healthcheck
```
Add `visit`/`billing` to the gateway `depends_on` (lines 341-353). Workers: same build, `command:` override `python -m services.scheduling.worker`, `depends_on` the API service with `condition: service_healthy` (so migrations ran), plus `temporal: condition: service_healthy`, and NOT `*app-healthcheck` (it hits `:8000/health`); use a heartbeat-file or process healthcheck. Prometheus: copy the job at `infra/prometheus.yml:29-31`.

---

### Tests

**Slot/constraint integration test shape:** `tests/scheduling/test_slot_constraint.py` (lines 14-55): module docstring naming the decision, `pytestmark = pytest.mark.integration`, `pytest.raises(IntegrityError)` around `async with session.begin_nested(): ... flush()`, assert constraint name in `str(excinfo.value)`. Reuse for the `uq_appointments_active_slot` and patient-overlap proofs.

**Fixture limitation:** `tests/scheduling/conftest.py` `scheduling_db_session` rolls back an outer transaction, which cannot test concurrent committed transactions. The SCHED-03/04 concurrency tests need a new fixture with separate committed sessions (`create_async_engine(settings.database_url)`, one `AsyncSession` per `asyncio.gather` task, explicit cleanup), see RESEARCH Code Example 5. Keep parallelism bounded (Pitfall 11).

**Internal-endpoint HTTP tests:** `tests/provider/test_internal_providers.py` (lines 19-62): `from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email`; build fixtures through the gateway (`/api/clinics`, `/api/providers`), call internal routes directly at `service_base_url("provider")`; assert 401/403/identical 404 matrix. Copy for Patient, Provider additions, Scheduling and Billing internal routes.

**Harness wiring:** `tests/conftest.py:85-92` `HARNESS_SERVICES` add `"visit"`, `"billing"`; `harness_apps()` imports `services.{name}.app`. `tests/libs/test_service_isolation.py:43-69` add entries to `OWNED_TABLES` (scheduling gains `appointments`, `idempotency_keys`, `waitlist_entries`, status-history; `visit`: visits, `visit_state_history`; `billing`: `billing_records`, `billing_status_history`), `BASES`, `SETTINGS`; `PHI_COLUMNS` (line 71) forbids `date_of_birth`, `phone`, `address` outside Patient.

---

## Shared Patterns

### Authentication (public routes)
**Source:** `libs/auth/rbac.py` via `services/patient/api/patients.py:188`
`user: AuthenticatedUser = Depends(require_role(Role.X, ...))`. Access tokens only. Roles: `patient`, `provider`, `front_desk`, `admin` (`libs/auth/roles.py:15-18`). Apply to all new `/api/*` routers.

### Authentication (internal routes)
**Source:** `libs/auth/internal.py:73-159`
`caller: InternalCaller = Depends(require_internal_caller(Role.ADMIN, Role.FRONT_DESK))`. Role allow-list applies to user callers only; service callers bypass it (so Phase 2 adds the `aud` and allowed-callers checks). Apply to every `/internal/*` router.

### Clinic scope, one identical 404
**Source:** `services/patient/api/patients.py:95-111` (`_assert_clinic_scope`, public routes, 403) and `libs/auth/internal.py:162-178` (`clinic_scope_permits`, internal routes, 404). Missing and out-of-scope IDs return `HTTPException(404, detail="not found")`. Apply to all new lookups.

### Upstream failure mapping (fail closed)
**Source:** `libs/service_client.py:50-66, 113-122`; mapping in `services/patient/api/patients.py:144-152`. Only a clean 404 or `is_active=false` is an invalid reference (422); transport error/401/403/5xx is 503. In activities, the same split becomes retryable (503 class) vs `ApplicationError(non_retryable=True)` (business).

### Audit (field names only)
**Source:** `libs/audit/writer.py:44-83`; call pattern `services/patient/api/patients.py:173-182`. Every mutation calls `write_audit` with field names only; add `dedupe_key=f"{workflow_id}:{step}"` inside activities. Keep audit as the LAST step of an idempotent activity (WR-04).

### Logging and tracing
**Source:** `libs/observability/logging.get_logger`, `log.info("internal.provider_lookup", provider_id=str(...), outcome="found")` (`provider/api/internal.py:58`): structured event name plus ids/outcome only. `setup_tracing(app, service_name="smarthealth-<name>")` in every app factory; `libs/http.outgoing_headers` for outbound trace headers.

### DB session and loop-aware singletons
**Source:** `libs/db/postgres.py:43-86` (`_entry_for`, `make_session_dependency`). Worker processes obtain sessions via `get_session_factory(settings.database_url)` rather than creating engines ad hoc; make pool size configurable for new services (Pitfall 11).

### Per-service isolation
**Source:** `services/scheduling/repository/models.py:36-47` (own `DeclarativeBase`), `services/scheduling/config.py` (own `*_DATABASE_URL`). No cross-service imports (enforced by `tests/libs/test_service_isolation.py`); anything shared between Visit and Scheduling goes in `libs/`.

## No Analog Found

Planner should use RESEARCH.md patterns (all were executed against the live stack per RESEARCH).

| File | Role | Data Flow | Reason / Where to look |
|---|---|---|---|
| `libs/temporal.py` | config | event-driven | No Temporal code exists. RESEARCH Pattern 2 (`start_once`), Code Example 3 (`Client.connect(lazy=True, interceptors=[TracingInterceptor()])`) |
| `services/scheduling/workflows/*.py` | workflow | event-driven | RESEARCH Pattern 3 (saga, compensations registered before step), Pattern 5, 7; sandbox-safe imports only, plain `str` I/O (Pitfall 1) |
| `services/scheduling/activities/*.py` | service | event-driven | RESEARCH Pattern 4 (class-based, DI, bounded retries, non-retryable business errors), Code Example 2 (conditional SQL) |
| `services/*/worker.py` | entrypoint | event-driven | RESEARCH Code Example 3 and 4 (`Worker(...)`, `create_schedule` + `ScheduleAlreadyRunningError`) |
| `services/visit/workflows/visit_completed.py` + sweeper | workflow | event-driven | RESEARCH Pattern 8, 9 |
| `tests/workflows/*` | test | event-driven | RESEARCH Validation Architecture and Pitfall 8 (`WorkflowEnvironment.start_time_skipping()`, `ActivityEnvironment`, `Replayer`) |
| `scripts/` concurrent-booking demo | script | batch | No demo script exists; closest style `scripts/bootstrap_admin.py`, `scripts/compose_healthcheck.sh`; document `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000` (Pitfall 10) |
| Hand-drawn inline SVG state diagrams in dashboard | docs | n/a | Existing `docs/architecture-dashboard.html` has CSS-variable cards but no state-diagram SVGs; reuse its CSS variables |

## Notes for the planner

- `temporalio` is not installed and was flagged by the legitimacy gate: the plan needs a `checkpoint:human-verify` before adding `"temporalio==1.34.0"` to `pyproject.toml` (deviation from CLAUDE.md pin 1.32.0; call it out).
- Only the first 432 lines plus lines 433-789 of RESEARCH.md were read; the whole file was covered. DASH update details (RESEARCH "Dashboard update") were not re-mapped to HTML line numbers; the executor should Grep `docs/architecture-dashboard.html` for `#flows`, `#decisions`, `#roadmap`, `meta-row` and the footer.
- Existing `libs/service_client.py` and three tests assert GET-only; keep it unchanged.
- Verify these paths remain tracked at plan time: all analogs above came from `git ls-files` listing for `libs/`, `services/`, `infra/`, `tests/`, `docs/`, `scripts/`.

## Metadata

**Analog search scope:** `libs/`, `services/{scheduling,patient,provider,organization,identity,gateway}`, `infra/`, `tests/{scheduling,provider,libs}`, `docs/`, `scripts/`
**Files scanned:** about 30 read in full or in part, about 120 listed
**Pattern extraction date:** 2026-10-12
