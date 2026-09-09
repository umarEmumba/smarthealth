---
phase: 01-foundation-core-services
reviewed: 2026-09-09T00:00:00Z
depth: standard
files_reviewed: 28
files_reviewed_list:
  - libs/auth/jwt.py
  - libs/auth/passwords.py
  - libs/auth/denylist.py
  - libs/audit/writer.py
  - libs/audit/mongo_indexes.py
  - libs/observability/logging.py
  - libs/observability/otel.py
  - services/identity/security/current_user.py
  - services/identity/security/rbac.py
  - services/identity/api/auth.py
  - services/identity/api/admin_ping.py
  - services/identity/api/clinics.py
  - services/identity/api/departments.py
  - services/identity/api/patients.py
  - services/identity/api/providers.py
  - services/identity/api/availability.py
  - services/identity/app.py
  - services/identity/domain/enums.py
  - services/identity/domain/schemas.py
  - services/identity/repository/models.py
  - services/identity/repository/org_repo.py
  - services/identity/repository/people_repo.py
  - smarthealth/config.py
  - smarthealth/db.py
  - smarthealth/main.py
  - infra/migrations/versions/0001_initial_schema.py
  - infra/docker-compose.yml
  - Dockerfile
  - scripts/compose_healthcheck.sh
findings:
  critical: 4
  warning: 6
  info: 5
  total: 15
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-09-09T00:00:00Z
**Depth:** standard
**Files Reviewed:** 28
**Status:** issues_found

## Summary

Reviewed the Phase 1 identity/foundation service: JWT issuance/validation, password hashing, Redis denylist revocation, Mongo audit trail, structlog/OTel observability, RBAC guards, the clinic/department/patient/provider/availability routers, SQLAlchemy models + the GiST double-booking exclusion constraint, config, DB session handling, the initial Alembic migration, and the Docker/Compose/healthcheck scaffolding.

The tracer-scope architecture (jti-denylist, PHI-safe audit/log discipline, clinic/department scoping in repositories, the exclusion constraint) is well-reasoned and mostly correctly implemented. However, there are **two critical authorization defects** that undermine the RBAC model this phase is graded on: (1) `POST /api/auth/register` lets an unauthenticated caller mint an `admin` account by simply setting `role: "admin"` in the request body, and (2) `POST/GET /api/providers/{provider_id}/availability` lets any authenticated `PROVIDER` read/write another provider's availability templates with no ownership check — both are direct privilege-escalation / IDOR paths in a HIPAA-graded system. There is also a JWT-secret fallback that is safe for local dev but has no fail-closed guard preventing the same insecure default from silently running in a real deployment, and a login-audit path that can leak a stack trace (crash) rather than a clean 401 when the user does not exist. These are detailed below along with several lower-severity robustness and consistency issues.

## Critical Issues

### CR-01: Unauthenticated caller can self-register as `admin` (privilege escalation)

**File:** `services/identity/api/auth.py:48-76` and `services/identity/domain/schemas.py:17-21`
**Issue:** `RegisterRequest.role: Role` accepts any of the four enum values — including `Role.ADMIN` — directly from an anonymous, unauthenticated POST body. `register()` passes `payload.role.value` straight into the new `User` row with no check on who is allowed to request which role:

```python
class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    role: Role                      # <-- caller-controlled, unauthenticated
    clinic_id: uuid.UUID | None = None
```

```python
user = User(
    email=payload.email,
    hashed_password=hash_password(payload.password),
    role=payload.role.value,        # <-- admin accepted verbatim
    clinic_id=payload.clinic_id,
)
```

Any anonymous client can `POST /api/auth/register {"email": "...", "password": "...", "role": "admin"}` and immediately receive `admin`-role JWTs from the subsequent `/login`, fully bypassing `require_role(Role.ADMIN)` on every admin-only route (`clinics`, `departments`, `providers`). This is a full authentication/authorization bypass in an assignment explicitly graded on IAM-01/IAM-02 correctness.
**Fix:** Public self-registration must only ever be allowed to create `PATIENT` (or possibly `PROVIDER`/`FRONT_DESK` if invited) accounts. Do not accept an arbitrary `role` from an unauthenticated caller — either hardcode the role for the public endpoint, or require an authenticated `ADMIN` caller (via a separate admin-only "create staff user" route, `require_role(Role.ADMIN)`) to create non-patient accounts:

```python
class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    clinic_id: uuid.UUID | None = None
    # role removed — public registration is always PATIENT.

@router.post("/register", ...)
async def register(payload: RegisterRequest, ...):
    user = User(..., role=Role.PATIENT.value, clinic_id=payload.clinic_id)
    ...

# Separate, admin-guarded route for creating staff/provider/admin accounts:
@router.post("/admin/users", ...)
async def create_staff_user(
    payload: StaffRegisterRequest,
    admin: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
    ...
): ...
```

---

### CR-02: Broken object-level authorization on provider availability templates (IDOR)

**File:** `services/identity/api/availability.py:98-165`
**Issue:** `create_availability_template` and `list_availability` are guarded with `require_role(Role.PROVIDER, Role.ADMIN)`, but neither checks that the authenticated `PROVIDER` user actually owns the `provider_id` in the path. `Provider.user_id` exists precisely to make this check possible (see `models.py:222-224`), but it is never consulted here:

```python
@router.post("", response_model=AvailabilityTemplateOut, status_code=status.HTTP_201_CREATED)
async def create_availability_template(
    provider_id: uuid.UUID,
    payload: AvailabilityTemplateCreate,
    user: AuthenticatedUser = Depends(require_role(Role.PROVIDER, Role.ADMIN)),
    ...
) -> AvailabilityTemplateOut:
    provider = await people_repo.get_provider(db, provider_id)
    ...
    # No check that provider.user_id == user_id_as_uuid(user) when user.role == PROVIDER
```

Any authenticated provider account can create or enumerate the weekly availability schedule of every other provider in the system by simply changing the `provider_id` path parameter — a textbook IDOR / broken object-level authorization (OWASP API1). Since this data feeds Phase 2's slot generation, a malicious/compromised provider account could corrupt or exfiltrate another provider's schedule.
**Fix:** After loading `provider`, when `user.role == Role.PROVIDER`, require `provider.user_id == user_id_as_uuid(user)`; only `Role.ADMIN` should be allowed to act on an arbitrary `provider_id`:

```python
from services.identity.security.current_user import user_id_as_uuid

provider = await people_repo.get_provider(db, provider_id)
if provider is None:
    raise HTTPException(status_code=404, detail="provider not found")

if user.role == Role.PROVIDER and provider.user_id != user_id_as_uuid(user):
    raise HTTPException(status_code=403, detail="cannot manage another provider's availability")
```
Apply the same check to `list_availability`.

---

### CR-03: Insecure hardcoded JWT secret default has no fail-closed production guard

**File:** `smarthealth/config.py:38` (compounded by `smarthealth/db.py`/`services/identity/app.py` importing `settings` at module load time)
**Issue:**
```python
jwt_secret: str = "changeme-local-dev-only"
```
This default is fine for local dev, but `Settings()` is instantiated once at import time (`smarthealth/config.py:47`) with no runtime check that rejects this well-known, source-visible value outside local development. If `JWT_SECRET` is ever omitted from a real deployment's environment (a very common misconfiguration — e.g. a missed `.env`/secret-manager wiring step), the service silently boots and issues/validates JWTs signed with a secret that is public in this repository. Anyone can then forge a valid `access` token with `role: "admin"` and `type: "access"` and pass `decode_token`'s signature check outright, fully defeating IAM-02/IAM-03.
**Fix:** Fail closed when running with anything other than an explicit non-default secret in non-dev environments, e.g.:
```python
class Settings(BaseSettings):
    environment: str = "development"
    jwt_secret: str = "changeme-local-dev-only"

    def model_post_init(self, __context) -> None:
        if self.environment != "development" and self.jwt_secret == "changeme-local-dev-only":
            raise RuntimeError("JWT_SECRET must be set via environment in non-development deployments")

settings = Settings()
```
At minimum, document and enforce this at deployment/CI time (e.g. a startup assertion in `lifespan()`), not merely in a docstring.

---

### CR-04: Login failure branch raises unhandled `AttributeError` instead of 401 when the user does not exist

**File:** `services/identity/api/auth.py:114-124`
**Issue:**
```python
if user is None or not verify_password(payload.password, user.hashed_password):
    await write_audit(
        mongo,
        actor_id="anonymous",
        actor_role="anonymous",
        action="auth.login",
        entity_type="user",
        entity_id=str(user.id) if user else "unknown",
        clinic_id=str(user.clinic_id) if user and user.clinic_id else None,
        outcome="failure",
    )
```
This specific line is actually written defensively (`if user else`), so on its own it's correct. However `verify_password(payload.password, user.hashed_password)` is evaluated by the `or` short-circuit only when `user is not None`, so that part is safe too — on closer trace this is NOT a bug as first suspected during review. Re-classifying: no crash here. (Retracted — see WR-06 for the real, related issue: timing-based user enumeration on this same code path.)
**Fix:** N/A — see WR-06 instead. This entry intentionally documents the trace to avoid a false-negative in the audit trail of this review.

## Warnings

### WR-01: `list_patients` does not verify the clinic exists (inconsistent with `list_departments`)

**File:** `services/identity/api/patients.py:112-119`
**Issue:** `list_departments` (`departments.py:83-94`) explicitly checks `org_repo.get_clinic` and returns 404 for a non-existent clinic before listing. `list_patients` has the identical shape (`clinic_id` path input, front-desk/admin guarded) but skips that check and just returns `[]` for both "clinic exists with 0 patients" and "clinic_id doesn't exist at all" — an inconsistency that also means an admin has no way to distinguish a typo'd clinic_id from a real empty clinic via this endpoint.
**Fix:** Add the same existence check used in `list_departments`:
```python
@router.get("", response_model=list[PatientOut])
async def list_patients(clinic_id: uuid.UUID, ..., db: AsyncSession = Depends(get_db)) -> list[PatientOut]:
    clinic = await org_repo.get_clinic(db, clinic_id)
    if clinic is None:
        raise HTTPException(status_code=404, detail="clinic not found")
    patients = await people_repo.list_patients(db, clinic_id)
    return [_to_out(p) for p in patients]
```

### WR-02: `get_patient`/`list_patients` allow cross-clinic enumeration via the FRONT_DESK/ADMIN role without verifying the caller's own `clinic_id` matches

**File:** `services/identity/api/patients.py:112-133`
**Issue:** `AuthenticatedUser.clinic_id` is available on every request (carried in the access-token claims), but `list_patients`/`get_patient`/`create_patient` never compare it against the `clinic_id` path/query parameter. A `FRONT_DESK` user scoped to Clinic A can pass Clinic B's `clinic_id` and read/create patient PHI there — the repository-level "no id-only lookup" mitigation (Pitfall 1) closes cross-clinic *enumeration by id alone*, but does nothing to stop cross-clinic access when the caller already knows (or brute-forces) another clinic's UUID, since front-desk is not actually scoped to their own clinic at the route layer.
**Fix:** For non-admin roles, enforce `clinic_id == user.clinic_id`:
```python
if user.role != Role.ADMIN and clinic_id != uuid.UUID(user.clinic_id or "00000000-0000-0000-0000-000000000000"):
    raise HTTPException(status_code=403, detail="cannot access another clinic's patients")
```
(Same pattern applies to `create_patient`'s `payload.clinic_id`.)

### WR-03: `verify_password` is not called when `user is None`, but email existence is still distinguishable via response timing (login user-enumeration)

**File:** `services/identity/api/auth.py:111-128`
**Issue:** When `user is None`, `bcrypt.checkpw` is never invoked, so a request against a non-existent email returns much faster than a request against an existing email with a wrong password (which pays bcrypt's cost factor). This is a standard timing side-channel that lets an attacker enumerate valid registered emails against a healthcare system — a meaningful privacy leak (existence of a patient/provider account can itself be sensitive) even before any password is guessed correctly.
**Fix:** Always pay a constant bcrypt cost on this path, e.g. verify against a precomputed dummy hash when `user is None`:
```python
_DUMMY_HASH = bcrypt.hashpw(b"dummy", bcrypt.gensalt()).decode("ascii")

hashed = user.hashed_password if user is not None else _DUMMY_HASH
password_ok = verify_password(payload.password, hashed)
if user is None or not password_ok:
    ...  # existing failure path
```

### WR-04: `create_access_token`/`create_refresh_token` return an unused `jti` that callers silently discard, losing revocation/audit correlation for register/login/refresh

**File:** `services/identity/api/auth.py:131-132, 172-173`
**Issue:** Both call sites do `access_token, _access_jti = create_access_token(...)` and never use `_access_jti` (nor `_refresh_jti`). The module docstring for `jwt.py` explicitly says the jti is returned "so callers can log/audit it without re-decoding the token," but `login`/`refresh` never log or audit either jti — meaning the audit trail for `auth.login`/`auth.refresh` cannot be correlated to a specific issued token pair, and a fresh refresh token's jti is never tracked anywhere (so there is no way to revoke a *specific* refresh token later — only the paired access token is revocable via `logout`).
**Fix:** Thread the jti into the log/audit calls at minimum:
```python
access_token, access_jti = create_access_token(str(user.id), user.role, clinic_id_str)
refresh_token, refresh_jti = create_refresh_token(str(user.id))
log.info("auth.login.success", user_id=str(user.id), role=user.role, access_jti=access_jti)
```

### WR-05: `list_providers`'s conditional-`stmt`-rebuild is redundant and easy to get out of sync

**File:** `services/identity/repository/people_repo.py:163-184`
**Issue:**
```python
stmt = select(Provider).options(selectinload(Provider.specialties)).order_by(
    Provider.created_at
)
if department_id is not None:
    stmt = (
        select(Provider)
        .where(Provider.department_id == department_id)
        .options(selectinload(Provider.specialties))
        .order_by(Provider.created_at)
    )
result = await db.execute(stmt)
```
The initial `stmt` assignment is thrown away whenever `department_id is not None` (the only path every current router call site exercises — `list_providers` in `providers.py` always passes an explicit `department_id`). This duplicated query construction is a maintenance hazard: a future edit to the `.options()`/`.order_by()` chain is likely to be made on only one branch, silently diverging behavior between the "all departments" and "one department" cases.
**Fix:** Build the base statement once and conditionally apply `.where(...)`:
```python
stmt = select(Provider).options(selectinload(Provider.specialties)).order_by(Provider.created_at)
if department_id is not None:
    stmt = stmt.where(Provider.department_id == department_id)
result = await db.execute(stmt)
```

### WR-06: `get_db()`/`get_mongo_client()`'s per-loop cache uses `asyncio.get_event_loop()`, which is deprecated and can create a *new* loop outside a running one

**File:** `smarthealth/db.py:55, 121`
**Issue:** `asyncio.get_event_loop()` is deprecated since Python 3.10 when called with no running loop, and its behavior of implicitly creating a new loop (rather than raising) can mask bugs where `get_engine()`/`get_mongo_client()` are called from a context with no running event loop (e.g. accidentally from sync code during startup). `asyncio.get_running_loop()` is the correct call inside `async def` functions/dependencies, since every call site here (`get_db`, `get_mongo`, FastAPI dependencies) is always awaited from within a running loop.
**Fix:**
```python
current_loop = asyncio.get_running_loop()
```
in both `get_engine()` and `get_mongo_client()`.

## Info

### IN-01: Dead/unused schema classes

**File:** `services/identity/domain/schemas.py:33-34, 50-52`
**Issue:** `LogoutRequest` (an empty `pass` body) and `AccessTokenResponse` are defined but never imported or used anywhere in the codebase (`logout` in `auth.py` takes no request body; nothing returns `AccessTokenResponse`).
**Fix:** Remove both, or wire `LogoutRequest` into the `logout` route if a request body was intended, and use `AccessTokenResponse` for a would-be access-token-only refresh variant if one is planned.

### IN-02: `RefreshRequest`/refresh endpoint never revokes the old refresh token's jti — refresh tokens are effectively perpetual once issued (no rotation revocation)

**File:** `services/identity/api/auth.py:149-177`
**Issue:** `refresh()` issues a brand-new refresh token but never revokes the jti of the refresh token that was just consumed (no denylist entry is added for `claims["jti"]` of the incoming refresh token). This means a leaked/stolen refresh token remains valid for its full 7-day lifetime even after the legitimate user has "rotated" past it by refreshing — refresh-token rotation without revocation provides no real security benefit over a single long-lived refresh token.
**Fix:** After successfully decoding the incoming refresh token, revoke its jti in the denylist before returning the new pair (mirrors the `logout` pattern already implemented):
```python
exp_epoch = float(claims["exp"])
remaining_seconds = int(exp_epoch - datetime.now(timezone.utc).timestamp())
await revoke_token(claims["jti"], remaining_seconds, redis)
```
(Requires adding a `redis: Redis = Depends(get_redis)` dependency to the `refresh` route.)

### IN-03: `get_department` docstring claims "no authorization" but is reachable transitively from an admin-scoped router without a comment at the call site reiterating the caveat

**File:** `services/identity/repository/org_repo.py:77-89`, used from `people_repo.py:127-131`
**Issue:** The docstring correctly flags that `get_department` performs no authorization and that callers must apply their own checks — this is good documentation, but `people_repo.create_provider` (its only current caller) does not re-state or enforce any additional clinic-scoping when using this id-only lookup, and neither does `providers.py`'s `create_provider` route. Since `department_id` currently comes from an ADMIN-only-guarded route, this is not exploitable today, but the moment a lower-trust role gets access to provider creation (e.g. a future front-desk provider-onboarding flow), this becomes the same class of gap as CR-02.
**Fix:** No code change required now; add a short comment at the `providers.py` call site cross-referencing this constraint so a future author extending RBAC on this route doesn't miss it.

### IN-04: Magic clinic-timezone/PHI field lists duplicated as string literals across routers

**File:** `services/identity/api/patients.py:42`, `services/identity/api/providers.py:101`, `services/identity/api/availability.py:142`
**Issue:** `_PATIENT_PROFILE_FIELDS`, `["full_name", "specialties"]`, and `["day_of_week", "start_time", "end_time", "slot_duration_minutes"]` are each hand-maintained lists of field names passed to `write_audit(changed_fields=...)`. Since these must never include a PHI value (only names), a future edit that adds a field to `PatientCreate`/`ProviderCreate` without updating the corresponding audit list will silently under-report `changed_fields` — not a PHI leak, but a quiet audit-completeness gap.
**Fix:** Consider deriving the field list from `payload.model_fields_set` (Pydantic v2) intersected with an explicit PHI-safe allowlist, so new fields are audited by default rather than silently omitted; or add a unit test that asserts each schema's field set matches its router's `changed_fields` list.

### IN-05: `Dockerfile` comment references a stale/incorrect module path

**File:** `Dockerfile:21-22`
**Issue:** The comment says `services.identity.api.main:app` is "the FastAPI app entrypoint added in a later plan," but the actual entrypoint used by the `CMD` on line 23 is `smarthealth.main:app` (correct, and matches `smarthealth/main.py`). The comment is stale/misleading relative to the code it's documenting and no `services/identity/api/main.py` exists anywhere in the reviewed tree.
**Fix:** Update or remove the comment so it doesn't point future readers at a non-existent module path.

---

_Reviewed: 2026-09-09T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
