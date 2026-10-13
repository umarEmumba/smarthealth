---
phase: 02-scheduling-service-workflow
plan: 10
subsystem: visit
tags: [visit, state-machine, conditional-update, idempotency, service-token, rbac, gateway]

# Dependency graph
requires:
  - phase: 02-01
    provides: "ServiceTokenProvider / Identity mint (visit -> scheduling, provider audiences already allowed), require_internal_caller(audience, allowed_services), throwaway VISIT_CLIENT_SECRET_PATH in tests"
  - phase: 02-03
    provides: "Provider GET /internal/providers/by-user/{user_id} (allowed: scheduling, visit)"
  - phase: 02-04
    provides: "seed_bookable_world (provider already linked to a PROVIDER login), deps.get_service_tokens / references shape"
  - phase: 02-05
    provides: "visit_db + visit_svc role (db-init), SharedSettings.visit_service_url, Billing skeleton copied for Visit"
  - phase: 02-06
    provides: "appointments table, appointment_repo.get_appointment"
provides:
  - "services/visit: VisitSettings (VISIT_DATABASE_URL, VISIT_CLIENT_SECRET_PATH), VisitBase, visit_0001 (visits, visit_state_history, uq_visits_appointment, uq_visit_history_visit_to, ck_visits_status_valid, ix_visits_clinic_status_created, ix_visits_downstream_pending; full downgrade)"
  - "visit_repo: create_visit (ON CONFLICT), transition_visit (conditional UPDATE + history -> applied/noop/skipped), get_visit, get_by_appointment, get_history, list_visits"
  - "VisitStatus / ORDER / PREVIOUS / TransitionOutcome (domain/states.py, plain str)"
  - "POST /api/visits, GET /api/visits, GET /api/visits/{id}, POST /api/visits/{id}/start, POST /api/visits/{id}/complete"
  - "Scheduling GET /internal/appointments/{id}?clinic_id= (aud=scheduling, allowed_services={visit}, ADMIN/FRONT_DESK users) returning AppointmentRef"
  - "Gateway row (/api/visits -> visit); visit in HARNESS_SERVICES and every isolation invariant"
affects: [02-11, 02-12]

actuals:
  tokens: 120000
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Row-level state machine: UPDATE ... WHERE status = <only predecessor> RETURNING id + history insert in one transaction; zero rows -> re-read -> noop (current/passed) or skipped (409)"
    - "Second data-layer guard UNIQUE(visit_id, to_status), valid because the lifecycle never re-enters a state"
    - "Idempotent create via INSERT ... ON CONFLICT (appointment_id) DO NOTHING RETURNING; losers read the winner back"
    - "Provider ownership resolved through Provider's by-user lookup BEFORE the visit is read; any mismatch is the identical 404"

key-files:
  created:
    - services/visit/__init__.py
    - services/visit/app.py
    - services/visit/config.py
    - services/visit/db.py
    - services/visit/Dockerfile
    - services/visit/references.py
    - services/visit/api/__init__.py
    - services/visit/api/deps.py
    - services/visit/api/visits.py
    - services/visit/domain/__init__.py
    - services/visit/domain/schemas.py
    - services/visit/domain/states.py
    - services/visit/repository/__init__.py
    - services/visit/repository/models.py
    - services/visit/repository/visit_repo.py
    - services/visit/migrations/alembic.ini
    - services/visit/migrations/env.py
    - services/visit/migrations/script.py.mako
    - services/visit/migrations/versions/visit_0001_visits.py
    - services/scheduling/api/internal.py
    - tests/visit/__init__.py
    - tests/visit/conftest.py
    - tests/visit/test_check_in.py
    - tests/visit/test_transitions.py
    - tests/visit/test_visit_list.py
    - tests/scheduling/test_internal_appointments.py
  modified:
    - services/scheduling/domain/schemas.py
    - services/scheduling/app.py
    - services/gateway/routing.py
    - tests/conftest.py
    - tests/gateway/test_routing.py
    - tests/libs/test_service_isolation.py

key-decisions:
  - "Transitions are sub-resources POST /api/visits/{id}/start and /complete (as the plan chose); responses are VisitOut, the detail GET is VisitDetailOut with history (from/to/created_at; the actor is never returned)"
  - "Check-in returns an EXISTING visit for the appointment (200) before asking Scheduling, so a repeat check-in is idempotent even if Scheduling is down or the appointment changed since; only a NEW visit needs a confirmed appointment"
  - "Applied transitions audit visit.start / visit.complete after the commit with dedupe_key visit-{id}:{action} (belt and braces on top of 'only applied writes audit'); check-in audits visit.check_in the same way"
  - "uq_visit_history_visit_to IntegrityError is caught in the ROUTE (rollback whole transaction, log visit.duplicate_history_suppressed, answer as noop); the repository never commits or rolls back"
  - "Scheduling's internal route also accepts ADMIN / FRONT_DESK user tokens (plan); user callers get clinic_scope_permits, and clinic_id must equal the appointment's clinic for everyone"
  - "Check-in is not restricted to the appointment's calendar day (documented limitation, as planned)"

patterns-established:
  - "Visit test helpers (tests/visit/conftest.py): VisitWorld(world, engine, provider_ids), fixtures visit_world / other_world (independent clinic), confirmed_appointment(vw, *, status=, provider_id=) (SQL insert on a fresh slot 7+ days beyond the generated ones), add_provider(client, vw) -> (provider_id, tokens), check_in, checked_in_visit, get_visit, audit_count(entity_id, action), visit_rows(appointment_id), history_rows(visit_id), cleanup_visit_rows(provider_ids), _visit_world(client, mesh) for fixtures in other folders"

requirements-completed: [VISIT-01, VISIT-02, VISIT-03, VISIT-05]

coverage:
  - id: D1
    description: "Tracer: front-desk checks in a confirmed appointment through the gateway; history [null -> checked_in]; one visit.check_in audit"
    requirement: VISIT-01
    verification:
      - kind: integration
        ref: "tests/visit/test_check_in.py#test_front_desk_checks_in_confirmed_appointment"
        status: pass
    human_judgment: false
  - id: D2
    description: "Check-in refusals (pending/cancelled/failed 409, other clinic identical 404, admin without clinic 422, provider/patient 403), Scheduling down 503 with zero rows, repeat 200, 10 concurrent -> one 201 + nine 200, one row/history/audit"
    requirement: VISIT-01, VISIT-03
    verification:
      - kind: integration
        ref: "tests/visit/test_check_in.py"
        status: pass
    human_judgment: false
  - id: D3
    description: "start/complete, repeats and passed targets 200 no-op, skip 409, provider own 200 / other provider identical 404, patient 403, other-clinic front-desk 404, admin, Provider down 503 unchanged, 10 concurrent start/complete change state once"
    requirement: VISIT-02, VISIT-03
    verification:
      - kind: integration
        ref: "tests/visit/test_transitions.py"
        status: pass
    human_judgment: false
  - id: D4
    description: "Internal appointment lookup: visit aud=scheduling 200 with exact keys, wrong clinic identical 404, clinic_id required 422, billing token 403, aud=provider 401, no token 401, provider user 403, gateway 404, absent from OpenAPI"
    requirement: VISIT-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_internal_appointments.py"
        status: pass
    human_judgment: false
  - id: D5
    description: "Clinic-scoped list: own clinic newest first, status/provider/date filters, limit cap/offset, front-desk other clinic 403, admin with/without clinic, provider/patient 403, provider detail own/other, polling reflects transitions"
    requirement: VISIT-05
    verification:
      - kind: integration
        ref: "tests/visit/test_visit_list.py"
        status: pass
    human_judgment: false
  - id: D6
    description: "Isolation invariants and gateway routing include Visit"
    requirement: VISIT-05
    verification:
      - kind: integration
        ref: "tests/libs/test_service_isolation.py"
        status: pass
      - kind: integration
        ref: "tests/gateway/test_routing.py"
        status: pass
    human_judgment: false
---

# Phase 2 Plan 10: Visit Service Summary

**Visit is now its own service on `visit_db`. Front-desk (own clinic) or admin checks a confirmed appointment in through the gateway. Visit confirms the appointment with Scheduling's new `/internal/appointments/{id}` over an aud=scheduling service token. The visit's provider, front-desk or admin then moves it `checked_in -> in_progress -> completed` with a conditional UPDATE plus a history row. Repeats are 200 no-ops, skips are 409, and ten concurrent duplicates change the state exactly once. Ops staff poll a clinic-scoped, filtered `GET /api/visits`.**

## Performance
- Tasks: 3 of 3
- Files: 26 created, 6 modified

## Accomplishments
- `visit_0001` round-trips: upgrade, downgrade base, upgrade. It creates both tables, `uq_visits_appointment`, the status CHECK, `uq_visit_history_visit_to`, the list index and the partial index `ix_visits_downstream_pending` for 02-11.
- Check-in:
  - An unconfirmed appointment is 409 "appointment not confirmed".
  - Another clinic's appointment, or a missing one, gets the identical 404.
  - If Scheduling or Identity is unreachable, the answer is 503 "scheduling service unavailable" and nothing is written.
  - Concurrent check-ins create one visit: one 201, the rest 200.
- Transitions:
  - PROVIDER callers are resolved through Provider's by-user lookup. A different provider gets the identical 404, and Provider being down is 503 "provider service unavailable".
  - Completion leaves `downstream_started_at` NULL.
- List: front-desk sees its own clinic, and naming another clinic is 403. Admin must name a clinic (422 without one). The list filters on status, provider and the check-in time window. `limit` is 1..200 and `offset` is supported. Items have VisitOut keys only.
- Scheduling `GET /internal/appointments/{id}?clinic_id=`:
  - Accepts a visit service token with aud=scheduling, or an ADMIN/FRONT_DESK user token. It is not in OpenAPI and not routed by the gateway.
  - `clinic_id` is required.
  - A billing token is 403, and a token for the wrong audience is 401.

## Task Commits
1. **Task 1: Tracer, check-in through the gateway** - `99a86ee` (feat)
2. **Task 2: Transitions, provider ownership, idempotency, concurrency** - `e0e7709` (feat; tests written first)
3. **Task 3: Clinic-scoped list, isolation, routing** - `3601eed` (feat; tests written first)

**Plan metadata:** see the `docs(02-10): complete plan summary` commit

## Decisions Made
See `key-decisions` in frontmatter.

## Deviations from Plan

**1. [Plan assumption obsolete] `seed_bookable_world(..., with_provider_user=True)` was not added.** Since 02-04, `seed_bookable_world` already creates a PROVIDER login and links it as the provider's `user_id` at creation. `World.provider` holds its tokens and `World.provider_user_id` its id. So `tests/scheduling/seed.py` is unchanged. A second provider in the same clinic comes from the new `tests/visit/conftest.add_provider`.

**2. [Real code] `create_visit` has an extra `created_by: uuid.UUID` argument.** `visits.created_by` is a UUID column, like `appointments.created_by`. The history `actor` stays the string `user:<sub>`.

**3. [Design within the plan] Check-in reads an existing visit first.** If the appointment already has a visit in the caller's clinic, it is returned with 200 and Scheduling is not called. A visit in another clinic gets the identical 404. Otherwise the plan's flow runs, and `ON CONFLICT` still arbitrates concurrent first check-ins (proven with 10 at once).

**4. [Commit hygiene] Some Task 3 edits moved into Task 1 so every commit stays green.**
- `visit` was added to `HARNESS_SERVICES` in Task 1.
- The exact-dict route-table assertion in `tests/gateway/test_routing.py` was also updated in Task 1, because the new gateway row would otherwise have broken it.
- Task 3 added the parametrised routing cases (`/api/visits`, `/api/visits/abc/start`, `/api/visitsX`). `/internal/appointments/...` was already covered as unrouted.

**5. [Interpretation] `date_from` / `date_to` are timezone-aware datetimes, not dates.** A naive value is 422. `date_from` is inclusive and `date_to` exclusive, both on `checked_in_at`. Visit does not know the clinic's timezone, so a bare date would be ambiguous. This matches Scheduling's `start`/`end` convention.

**6. [Belt and braces]**
- Audit writes carry `dedupe_key=visit-{id}:{action}`.
- The `uq_visit_history_visit_to` IntegrityError is handled in the route, not the repository, which keeps the "repository never commits or rolls back" convention.

**7. [Extra tests]**
- Admin check-in naming the wrong clinic is 404.
- Provider and patient cannot check in (403).
- Concurrent `complete` as well as concurrent `start`.
- The internal route refuses a provider user token (403) and is absent from Scheduling's OpenAPI.

**8. [Test infra]** `tests/scheduling/test_internal_appointments.py` reuses `tests.visit.conftest._visit_world` through a local `lookup_world` fixture, because fixtures do not cross sibling folders. Visit rows are cleaned by provider id (test data only).

**9. [Not in plan files]** No Compose service entry was added for `visit`, since the plan does not list `infra/docker-compose.yml`. Billing has none either. The Dockerfile exists and runs the migrations before gunicorn.

## Human checkpoints
- None in this plan. The tracer gate ran automatically: the tracer verify was re-run green before the expansion tasks.

## Verification
- Task 1 verify (alembic upgrade head, downgrade base, upgrade head, then `pytest tests/visit/test_check_in.py -x`): exit 0, 1 passed
- Task 2 verify (`pytest tests/visit tests/scheduling/test_internal_appointments.py -x -q`): 30 passed. The RED run before implementation had 12 failed and 18 passed (the check-in refusal tests and the internal-route tests passed against the Task 1 code).
- Task 3 verify (`pytest tests/visit tests/libs/test_service_isolation.py tests/gateway -x -q`): 118 passed. The RED run had 7 list tests failing.
- Acceptance greps: all pass
  - `smarthealth-visit` 1
  - gateway row 1
  - `"visit"` in tests/conftest.py 2
  - `audience="scheduling"` 1
  - `uq_visit_history_visit_to` in the migration 2
  - no cross-service imports in services/visit
  - `async def transition_visit` 1
  - `get_provider_by_user` in visits.py 2
  - no delete in visit_repo
  - test counts: transitions 11, internal appointments 9, list 8
  - isolation: `visit_state_history` 1, `VisitBase` 2
- Full suite `pytest tests -q -m "not e2e"`: **755 passed, 3 skipped** (was 713 + 3 skipped), 16 min 53 s
- Semgrep: not run (not installed in executor environment)

## Known limitations
- Check-in is not limited to the appointment's calendar day.
- Cancelling an appointment after check-in does not touch the visit. Visit does not listen for Scheduling changes in this phase.
- `downstream_started_at` is never set yet. Plan 02-11 adds the visit-completed workflow, the commit-then-start marker and the sweeper on `ix_visits_downstream_pending`.

## Next Phase Readiness
- 02-11 can start the downstream workflow right after the `complete` commit in `services/visit/api/visits._transition` (the `APPLIED` branch for `VisitStatus.COMPLETED`). Mark the visit with a conditional `UPDATE ... SET downstream_started_at = now() WHERE downstream_started_at IS NULL`. `VISIT_TASK_QUEUE = "visit-tq"` already exists in `libs/temporal_policies.py`. Identity already lets `visit` mint aud=`billing` and aud=`visit` tokens.
- Visit has no Temporal client or worker yet. Add `app.state.temporal_client` the way Scheduling does (lazy connect, and tests inject one).
