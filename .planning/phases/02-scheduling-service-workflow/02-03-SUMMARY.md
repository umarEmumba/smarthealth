---
phase: 02-scheduling-service-workflow
plan: 03
subsystem: api
tags: [fastapi, internal-api, service-tokens, clinic-scope, zoneinfo, iana-timezone, rbac]

requires:
  - phase: 02-scheduling-service-workflow (02-01)
    provides: aud-scoped service tokens, require_internal_caller(audience=, allowed_services=), ServiceTokenProvider, SERVICE_TOKEN_AUDIENCES (scheduling->patient/provider/organization already listed)
  - phase: 02-scheduling-service-workflow (02-02)
    provides: scheduling data layer (not used directly here)
provides:
  - "Patient internal router: GET /internal/patients/{patient_id}?clinic_id= and GET /internal/patients/by-user/{user_id}?clinic_id= -> PatientRef {id, clinic_id, user_id, is_active}"
  - "patient_repo.get_patient_by_user(db, user_id, clinic_id) (clinic-scoped)"
  - "Provider: GET /internal/providers/{provider_id}/availability -> [AvailabilityRef] (scheduling) and GET /internal/providers/by-user/{user_id} -> ProviderRef (scheduling, visit)"
  - "provider_repo.get_provider_by_user(db, user_id)"
  - "Organization: ClinicRef.timezone; validate_iana_timezone(value) in services/organization/domain/schemas.py applied to ClinicCreate and ClinicUpdate (422 on non-IANA)"
affects: [02-04, 02-05, 02-06, 02-07, slot-generation, booking-validation, visit-provider-check]

actuals:
  tokens: 10864
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Internal lookups for PHI-owning services take a REQUIRED clinic_id query param and re-apply scope for service callers; user callers additionally pass clinic_scope_permits; all misses are the identical 404 {\"detail\": \"not found\"}"
    - "Literal-segment routes (/by-user/{id}) are declared before /{id} routes"
    - "IANA zone validation via stdlib zoneinfo against the system zone database (no PyPI zone-database package)"

key-files:
  created:
    - services/patient/api/internal.py
    - tests/patient/test_internal_patients.py
  modified:
    - services/patient/app.py
    - services/patient/repository/patient_repo.py
    - services/provider/api/internal.py
    - services/provider/repository/provider_repo.py
    - services/organization/api/internal.py
    - services/organization/api/clinics.py
    - services/organization/domain/schemas.py
    - tests/provider/test_internal_providers.py
    - tests/organization/test_internal_lookups.py
    - tests/organization/test_clinics.py
    - tests/identity/test_service_token.py

key-decisions:
  - "Patient internal routes allow user roles ADMIN and FRONT_DESK and only the scheduling service (billing/visit refused 403)"
  - "Out-of-scope user callers on patient lookups short-circuit before the DB read and get the same 404 as missing"
  - "Availability lookup returns ALL templates (active and inactive, via existing list_availability(include_inactive=True)); 404 when the provider id is unknown; an existing provider with no templates is 200 []"
  - "provider_repo.list_availability already had include_inactive keyword, so no repo signature change was needed"
  - "ClinicRef.timezone returns the stored value as-is (legacy non-IANA rows remain readable; Scheduling guards ZoneInfoNotFoundError in 02-04)"

patterns-established:
  - "PatientRef / AvailabilityRef / ProviderRef / ClinicRef are ids-and-flags-only response models for /internal routes"

requirements-completed: [SCHED-01, VISIT-02]

coverage:
  - id: D1
    description: "Scheduling service token (aud=patient, minted over the mesh) resolves a patient by id and by linked user id within the requested clinic; wrong clinic / missing -> identical 404; missing clinic_id -> 422; ids-only body"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/patient/test_internal_patients.py"
        status: pass
    human_judgment: false
  - id: D2
    description: "Patient internal routes refuse aud=provider tokens (401), billing cannot mint aud=patient (403 at Identity) and a hand-signed billing aud=patient token is 403; not in OpenAPI; gateway 404"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/patient/test_internal_patients.py#test_billing_cannot_mint_or_use_a_patient_token, test_internal_route_is_not_reachable_through_the_gateway, test_internal_routes_are_excluded_from_openapi"
        status: pass
    human_judgment: false
  - id: D3
    description: "Provider availability lookup (scheduling only, includes inactive templates) and provider-by-user lookup (scheduling, visit); by-user not captured by {provider_id}; front-desk 403; billing 403; gateway 404"
    requirement: VISIT-02
    verification:
      - kind: integration
        ref: "tests/provider/test_internal_providers.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "ClinicRef carries the IANA timezone; clinic create/PATCH reject non-IANA zones with 422 and leave stored data unchanged"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/organization/test_clinics.py, tests/organization/test_internal_lookups.py, tests/identity/test_service_token.py"
        status: pass
      - kind: unit
        ref: "tests/organization/test_clinics.py#test_validate_iana_timezone_accepts_iana_names, test_validate_iana_timezone_rejects_non_iana_values"
        status: pass
    human_judgment: false

duration: 15min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 03: Internal Reference Lookups and Clinic Timezone Validation Summary

**Scheduling and Visit can now resolve patients (clinic-scoped, ids only), provider availability templates and provider-by-user over aud-scoped service tokens, and clinic references expose an IANA-validated timezone**

## Performance

- **Duration:** ~15 min of execution (plus Docker stack restart)
- **Started:** 2026-10-13T09:50+05:00
- **Completed:** 2026-10-13T10:12+05:00
- **Tasks:** 3
- **Files modified:** 13 (2 created, 11 modified)

## Accomplishments
- New Patient internal router with `GET /internal/patients/{id}?clinic_id=` and `GET /internal/patients/by-user/{user_id}?clinic_id=`: `audience="patient"`, `allowed_services={"scheduling"}`, required `clinic_id`, identical 404 for missing/out-of-clinic/out-of-scope, `PatientRef` contains only ids and `is_active`.
- Provider: `GET /internal/providers/{id}/availability` (scheduling) returns every template, including inactive ones. `GET /internal/providers/by-user/{user_id}` (scheduling, visit) is declared before `{provider_id}`.
- Organization: `ClinicRef.timezone`. `validate_iana_timezone` uses the stdlib `zoneinfo` module and is applied to `ClinicCreate` and `ClinicUpdate`, so `Mars/Olympus` and `UTC+5` are rejected with 422.

## Task Commits

1. **Task 1: Tracer — Patient internal router, clinic-scoped** - `5c6b49f` (feat)
2. **Task 2: Provider availability and by-user internal lookups** - `ca2f09b` (feat; tests written first and confirmed red: 9 failed before implementation)
3. **Task 3: ClinicRef.timezone and IANA validation** - `13ae924` (feat; tests written first and confirmed red: 17 failed before implementation)

**Plan metadata:** see `docs(02-03): complete plan summary` commit

## Files Created/Modified
- `services/patient/api/internal.py`: new internal router, `PatientRef`
- `services/patient/app.py`: mounts `internal.router`
- `services/patient/repository/patient_repo.py`: `get_patient_by_user` (clinic-scoped)
- `services/provider/api/internal.py`: `AvailabilityRef`, by-user and availability routes, shared `_not_found`
- `services/provider/repository/provider_repo.py`: `get_provider_by_user`
- `services/organization/api/internal.py`: `ClinicRef.timezone`
- `services/organization/api/clinics.py`: `ClinicCreate` timezone validator
- `services/organization/domain/schemas.py`: `validate_iana_timezone`, `ClinicUpdate` timezone validator
- Tests: `tests/patient/test_internal_patients.py` (new, 11), `tests/provider/test_internal_providers.py` (+10), `tests/organization/test_clinics.py` (+13 incl. parametrized), `tests/organization/test_internal_lookups.py` (+1, bodies updated), `tests/identity/test_service_token.py` (ClinicRef body updated)

## Decisions Made
See key-decisions in the frontmatter. In short, the plan was followed as written. Availability returns 200 `[]` for an existing provider that has no templates, and 404 only for an unknown provider.

## Deviations from Plan

None in production code or the plan's contracts. Minor test-scope additions beyond the plan's lists:
- The patient tests also cover inactive-patient reporting, front-desk in/out-of-scope (404), and the no-token 401.
- The provider tests also cover the billing service token getting 403 on both new routes, and gateway 404s for both routes.
- The clinic tests include unit tests for `validate_iana_timezone`, including path-like input `../etc/passwd`. That input raises `ValueError` from zoneinfo, which the validator catches.

**Total deviations:** 0
**Impact on plan:** None.

## Issues Encountered
- The executor container had restarted, so the Docker daemon and the postgres/redis/mongo/temporal containers were down (connection refused on 5433). I restarted `dockerd` and `docker start`ed the existing containers. Their data was intact, so no migrations were needed.
- 02-06 checkpoint (from the brief): the user approved temporalio 1.34.0 ("Approve 1.34.0"). That belongs to plan 02-06 and is not touched here.

## Verification
- `.venv/bin/pytest tests/patient/test_internal_patients.py -x`: 11 passed
- `.venv/bin/pytest tests/provider -x -q`: 62 passed
- `.venv/bin/pytest tests/organization tests/identity/test_service_token.py -x -q`: 80 passed
- Full suite `.venv/bin/pytest tests -q -m "not e2e"`: **481 passed**, 1 deselected (446 before this plan, +35 new)
- Acceptance greps: `audience="patient"` = 2; `include_router(internal` = 1; PHI field names in patient internal router = 0; `/availability` ≥ 1; `by-user` ≥ 1; by-user route line 74 < `{provider_id}` route line 129; `timezone` in org internal.py = 3; `def validate_iana_timezone` = 1; `tzdata` in pyproject.toml = 0
- Semgrep: not run (not installed in executor environment)

## User Setup Required
None. No external service configuration required.

## Next Phase Readiness
- 02-04 slot generation: call `/internal/providers/{id}/availability` (aud=provider) and `/internal/clinics/{id}` (aud=organization), which now returns `timezone`. Filter `is_active` and the effective dates on the caller side. Still guard `ZoneInfoNotFoundError` for legacy clinic rows.
- Booking validation: call `/internal/patients/{id}?clinic_id=` (aud=patient). A patient-role user can be resolved through `/internal/patients/by-user/{user_id}?clinic_id=`.
- Visit provider check (D-15): call `/internal/providers/by-user/{user_id}` with a visit service token (aud=provider).
- No client-side wrapper (e.g. in `libs/service_client`) was added for the new routes. Callers in later plans add their own lookup functions.
