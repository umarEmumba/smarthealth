---
phase: 02-scheduling-service-workflow
plan: 01
subsystem: auth
tags: [jwt, python-jose, service-tokens, client-credentials, audience, httpx, mongodb, audit, idempotency]

requires:
  - phase: 01.1-microservice-re-architecture
    provides: RS256 signing in Identity, decode_token / require_internal_caller, libs.http outgoing_headers, in-process mesh harness, write_audit
provides:
  - Identity POST /internal/service-tokens (Basic per-service secret -> 60 s token with aud)
  - create_service_token(service_name, *, audience) with aud claim
  - decode_token(audience=) with require_exp on every decode
  - require_internal_caller(*roles, audience=, allowed_services=) and InternalCaller.service_name
  - ServiceTokenProvider / ServiceTokenUnavailable / load_client_secret (libs/auth/service_tokens.py)
  - internal_request + InternalNotFound / InternalConflict / InternalRejected (libs/internal_client.py)
  - write_audit(dedupe_key=) idempotent upsert + ix_audit_dedupe_key partial unique index
  - scripts/generate_service_secrets.py
affects: [02-02, 02-03, 02-04, 02-05, 02-06, 02-07, scheduling, visit, billing, temporal-activities]

actuals:
  tokens: 21246
  tasks: 3
  commits: 5

tech-stack:
  added: []
  patterns:
    - "Client-credentials service tokens: one token per target audience, cached per audience, refreshed 15 s before expiry"
    - "Internal routes declare audience=<this service> and an explicit allowed_services set"
    - "Activity HTTP calls: exactly one attempt, typed outcomes, Temporal owns retries"
    - "Idempotent audit: dedupe_key upsert with $setOnInsert behind a partial unique index, written last"

key-files:
  created:
    - libs/auth/service_tokens.py
    - libs/internal_client.py
    - scripts/generate_service_secrets.py
    - tests/libs/test_service_tokens.py
    - tests/libs/test_internal_client.py
    - tests/libs/test_audit_dedupe.py
  modified:
    - services/identity/api/internal.py
    - services/identity/api/auth.py
    - services/identity/security/signing.py
    - services/identity/config.py
    - services/identity/domain/schemas.py
    - services/organization/api/internal.py
    - services/provider/api/internal.py
    - libs/auth/verification.py
    - libs/auth/internal.py
    - libs/audit/writer.py
    - libs/audit/mongo_indexes.py
    - .env.example
    - tests/conftest.py
    - tests/identity/test_service_token.py
    - tests/identity/test_internal_users.py
    - tests/provider/test_internal_providers.py
    - tests/libs/test_verification.py
    - tests/libs/test_audit_indexes.py

key-decisions:
  - "Identity settings attribute is service_client_credentials_dir (env var SERVICE_CLIENT_SECRETS_DIR); the plan's name tripped the existing no-secret-setting guard"
  - "Unknown client and wrong secret both run one compare_digest and return the identical 401 body"
  - "aud may be a string or a list; a service token passes only if it contains the receiving service"
  - "ServiceTokenProvider measures expiry from when the mint request was sent, so the cached lifetime never outlasts exp"
  - "internal_request treats any status other than 200/201/204/404/409/422 (including 3xx) as UpstreamServiceUnavailable"

patterns-established:
  - "require_internal_caller(Role..., audience=\"<svc>\", allowed_services=frozenset({...})) on one line per route"
  - "Tests mint service tokens with create_service_token(name, audience=...) or via ServiceTokenProvider over the mesh"
  - "verify_identity_token(token, audience=None) skips the aud check so tests can read aud themselves"

requirements-completed: [SCHED-01, SCHED-02, SCHED-08]

coverage:
  - id: D1
    description: "Scheduling mints an aud=provider service token from Identity with its own secret and reads /internal/providers/{id}"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/libs/test_service_tokens.py#test_scheduling_mints_provider_token_and_reads_internal_provider"
        status: pass
    human_judgment: false
  - id: D2
    description: "Internal routes reject wrong/missing aud, missing exp, expired tokens and unlisted service callers; user tokens unchanged"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/libs/test_service_tokens.py (rejection matrix, organization/identity route tests)"
        status: pass
      - kind: unit
        ref: "tests/libs/test_verification.py (audience / require_exp cases)"
        status: pass
    human_judgment: false
  - id: D3
    description: "Mint endpoint: identical 401 for bad secret / unknown client / no credentials, 403 for disallowed audience, 422 for malformed audience"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/libs/test_service_tokens.py#test_mint_*"
        status: pass
    human_judgment: false
  - id: D4
    description: "ServiceTokenProvider caches per audience, refreshes before expiry, shares concurrent mints, fails closed"
    requirement: SCHED-02
    verification:
      - kind: integration
        ref: "tests/libs/test_service_tokens.py#test_provider_*, test_concurrent_gets_share_one_mint"
        status: pass
    human_judgment: false
  - id: D5
    description: "internal_request: one attempt, auth/idempotency/trace headers, typed 404/409/422, everything else retryable"
    requirement: SCHED-02
    verification:
      - kind: unit
        ref: "tests/libs/test_internal_client.py"
        status: pass
    human_judgment: false
  - id: D6
    description: "write_audit(dedupe_key=) leaves one document on repeated and concurrent writes; legacy path unchanged; partial unique index"
    requirement: SCHED-08
    verification:
      - kind: integration
        ref: "tests/libs/test_audit_dedupe.py, tests/libs/test_audit_indexes.py"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 01: Internal Auth Hardening Summary

**Identity now mints 60-second, audience-scoped service tokens to Scheduling/Visit/Billing through a client-credentials endpoint (`POST /internal/service-tokens`). Every internal route requires its own `aud` and an explicit set of allowed callers. Activities get a single-attempt write client, and audit writes can be made idempotent with `dedupe_key`.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-10-13T02:25Z
- **Tasks:** 3
- **Files modified:** 24 (6 created, 18 modified)

## Accomplishments

- **Mint endpoint.** Identity's `POST /internal/service-tokens` checks HTTP Basic per-service secrets with `hmac.compare_digest`. `SERVICE_TOKEN_AUDIENCES` sets which targets each client may ask for. It logs only the client name and audience, and token issuance is not audited (stated in the docstring).
- **Token changes.** `create_service_token` now requires `audience` and adds the `aud` claim. `decode_token` takes `audience` and always sets `require_exp`, which closes WR-03.
- **Internal route checks.** `require_internal_caller` takes a keyword-only `audience` and `allowed_services`. A service token whose `aud` is missing or wrong gets 401. A service not in the route's allowed set gets 403, which closes WR-02. User tokens behave as before.
- **Allowed callers per route:**
  - provider lookup: {scheduling, visit}
  - organization clinics: {identity, scheduling}
  - organization departments: {scheduling}
  - identity users: no service caller allowed
- **`ServiceTokenProvider`.** Keeps one cached token per audience and refreshes it 15 s before expiry. Concurrent requests share one refresh behind an `asyncio.Lock`. The clock can be injected for tests. When it cannot get a token it raises `ServiceTokenUnavailable` instead of proceeding.
- **`internal_request`.** Makes exactly one attempt and imports no retry library. Outcomes are typed: 404 raises `InternalNotFound`, 409 raises `InternalConflict`, 422 raises `InternalRejected`, and everything else raises `UpstreamServiceUnavailable`. `libs/service_client.py` is unchanged.
- **Idempotent audit.** `write_audit(dedupe_key=)` does a `$setOnInsert` upsert backed by the partial unique index `ix_audit_dedupe_key`. Existing call sites, which pass no key, behave as before.

## Task Commits

1. **Task 1: Tracer (Scheduling mints aud=provider, reads /internal/providers/{id})** - `c18cd33` (feat)
2. **Task 2: Audience + allowed-callers on every internal route, rejection matrix** - `29bca1e` (feat)
3. **Fix (deviation 2): rename settings attribute** - `68b3c7a` (fix)
4. **Task 3: internal_request + idempotent audit writer** - `5616f0a` (feat)

**Plan metadata:** this SUMMARY (docs commit)

## Verification

- Task 1 verify `pytest tests/libs/test_service_tokens.py -x`: 1 passed (the tracer).
- Task 2 verify `pytest tests/libs/test_service_tokens.py tests/libs/test_verification.py tests/identity tests/provider tests/organization tests/patient -x -q`: 258 passed. The RED run before the route changes failed only the four route-tightening tests.
- Task 3 verify `pytest tests/libs -x -q`: 131 passed. The RED run failed at collection because `libs.internal_client` did not exist yet.
- Full regression `pytest tests -q -m "not e2e"`: **419 passed**, 1 deselected (baseline 365, plus 54 new).
- All acceptance-criteria greps pass. `require_internal_caller(` without `audience=` returns nothing (exit 1). `test_service_tokens.py` has 17 `def test_` functions; with parametrization that is 25 tests.
- **Semgrep:** not run (not installed in the executor environment).

## Deviations from Plan

**1. [Plan error] Task 1 had to convert every `require_internal_caller` call site**
- **Issue:** The plan said the other routes "will not import-fail" in Task 1 because only the keyword is new. That is wrong: `audience` is keyword-only with no default, so the unconverted Identity and Organization routes would raise `TypeError` when imported.
- **Fix:** Task 1 gave every route its `audience`, with interim `allowed_services` matching the pre-plan behaviour: users {identity}, clinics {identity}, departments {}. Task 2 then wrote the tests first (RED: 4 failures) and tightened these sets to the plan's values (GREEN).
- **Commits:** c18cd33, 29bca1e

**2. [Existing guard] Settings attribute renamed `service_client_secrets_dir` → `service_client_credentials_dir`**
- **Issue:** `tests/libs/test_config.py::test_no_shared_secret_or_algorithm_setting_exists` fails if any settings field name contains "secret". The plan's attribute name broke it.
- **Fix:** Renamed only the Python attribute. The env var is still `SERVICE_CLIENT_SECRETS_DIR`. The existing test was left unchanged.
- **Commit:** 68b3c7a

**3. [Test helper] `verify_identity_token(token, *, audience=None)`**
- **Issue:** Service tokens now carry `aud`, and python-jose rejects a token with `aud` when no audience is passed.
- **Fix:** The helper passes `verify_aud=False` when no audience is given, so tests can read the claims and assert on `aud`. It verifies the audience when one is supplied.

**4. [Test hardening] `tests/identity/test_service_token.py::_service_claims` default now includes `aud="organization"`**
- **Reason:** Without it, the revoked, expired and no-prefix tests would get their 401 from the missing `aud` rather than from the condition each test is meant to check.

**5. [Scope] `ServiceTokenProvider(clock=...)` was added in Task 1, not Task 2.** It is the same file and has no behaviour change.

**Total deviations:** 5, all needed for correctness or test fidelity. No scope creep.

## Issues Encountered

None beyond the deviations above.

## Environment Variables (.env.example updated)

| Variable | Read by | Value |
|---|---|---|
| `SERVICE_CLIENT_SECRETS_DIR` | Identity (mint endpoint) | directory of `<service>.secret` files |
| `SCHEDULING_CLIENT_SECRET_PATH` | Scheduling workers | path to `scheduling.secret` |
| `VISIT_CLIENT_SECRET_PATH` | Visit workers | path to `visit.secret` |
| `BILLING_CLIENT_SECRET_PATH` | Billing workers | path to `billing.secret` |

Generate the files with `python scripts/generate_service_secrets.py`. It writes to `infra/secrets/service-clients/`, which is gitignored. Files are mode 0600 and the directory 0700. Existing files are not overwritten unless you pass `--force`.

## User Setup Required

For a local Compose run, run `python scripts/generate_service_secrets.py` once. The Compose wiring that passes these paths and secret files to the containers belongs to the later plans that add the workers.

## Next Phase Readiness

- Workers build their provider like this: `ServiceTokenProvider("scheduling", load_client_secret(os.environ["SCHEDULING_CLIENT_SECRET_PATH"]), shared_settings.identity_service_url)`. They then call `internal_request(http, "POST", url, token=await provider.get("<target>"), service="<target>", idempotency_key=f"{workflow_id}:{step}", json=...)`.
- Activities should treat `ServiceTokenUnavailable` and `UpstreamServiceUnavailable` as retryable. `InternalNotFound`, `InternalConflict` and `InternalRejected` are business outcomes and should map to non-retryable errors.
- A new internal route must declare `audience="<its service>"` and an explicit `allowed_services`. A new calling service, or a new target for an existing one, must also be added to `SERVICE_TOKEN_AUDIENCES` in `services/identity/api/internal.py`. Right now `billing` maps to an empty set, and Patient, Billing and Scheduling have no internal routes yet.
- The test bootstrap (`tests/conftest.py::_ensure_test_service_client_secrets`) creates throwaway secrets for scheduling, visit and billing and sets all four env vars before import.
- Audit entries for workflow steps should pass `dedupe_key=f"{workflow_id}:{step}"`. Write the audit entry as the last step of the activity, because the audit write is not atomic with Postgres (WR-04).

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
