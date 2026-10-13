---
phase: 02-scheduling-service-workflow
plan: 04
subsystem: api
tags: [scheduling, slots, zoneinfo, dst, service-tokens, gateway, postgres]

# Dependency graph
requires:
  - phase: 02-01
    provides: ServiceTokenProvider, internal_request, write_audit, aud-scoped internal routes
  - phase: 02-02
    provides: provider_slots clinic_id/department_id columns, uq_provider_slots_provider_start, CHECK end>start
  - phase: 02-03
    provides: Provider availability/by-user lookups, Patient internal lookups, ClinicRef.timezone
provides:
  - Pure DST-safe slot generator (local_to_utc, expand_templates, CandidateSlot, AvailabilityTemplate)
  - Scheduling reference-read module over aud-scoped service tokens (provider, availability, department, clinic, patient, patient-by-user)
  - get_service_tokens / build_service_tokens dependency (lazy, fail-closed 503)
  - POST /api/slots/generate, GET /api/slots (with on-demand horizon extension), POST /api/slots/{id}/cancel
  - Gateway rows /api/slots, /api/appointments, /api/waitlist -> scheduling
  - tests/scheduling/seed.py seed_bookable_world + World for later Phase 2 plans
affects: [02-05, 02-06, 02-07, 02-08, 02-09, 02-10, booking, waitlist, visit]

actuals:
  tokens: 17071
  tasks: 3
  commits: 4

tech-stack:
  added: []
  patterns:
    - "Scheduling reads other services only through services/scheduling/references.py with ServiceTokenProvider.get(<aud>)"
    - "Generation core (generate_for_provider) raises GenerationRefused(status, detail) before any write; callers map or swallow"
    - "INSERT ... SELECT FROM unnest(...) ON CONFLICT DO NOTHING RETURNING id for idempotent bulk slot inserts"

key-files:
  created:
    - services/scheduling/domain/slot_generation.py
    - services/scheduling/domain/schemas.py
    - services/scheduling/references.py
    - services/scheduling/api/__init__.py
    - services/scheduling/api/deps.py
    - services/scheduling/api/slots.py
    - tests/scheduling/seed.py
    - tests/scheduling/test_slot_generation.py
    - tests/scheduling/test_slots_api.py
  modified:
    - services/scheduling/config.py
    - services/scheduling/app.py
    - services/scheduling/repository/slot_repo.py
    - services/gateway/routing.py
    - libs/internal_client.py
    - tests/gateway/test_routing.py
    - tests/libs/test_internal_client.py

key-decisions:
  - "Out-of-clinic scope is checked before the inactive-provider 409, so an inactive provider in another clinic is still the identical 404"
  - "A template window starting in a DST gap begins at the next existing grid point; one ending in a gap ends at the gap's start instant"
  - "On-demand extension starts at the latest slot's UTC date (not +1 day): one idempotent re-run day, never a skipped local day in negative-offset zones"
  - "Extension audits slots.generate only when it actually created rows (avoids an audit write on every search); POST /generate always audits"
  - "ServiceTokenUnavailable maps to 503 'identity service unavailable'"
  - "GET /api/slots has no clinic scope (ids/times only, patients search any provider); status=any returns every status"

patterns-established:
  - "build_service_tokens(app) rebuilds the provider when app.state.internal_http_client changes (test mesh swaps)"
  - "seed_bookable_world(client, mesh, days_ahead=2) + cleanup_scheduling_rows(engine, [world.provider_id])"

requirements-completed: [SCHED-01]

coverage:
  - id: D1
    description: "DST-safe pure slot expansion (gap, fold, effective window, inactive, no overlap)"
    requirement: SCHED-01
    verification:
      - kind: unit
        ref: "tests/scheduling/test_slot_generation.py"
        status: pass
    human_judgment: false
  - id: D2
    description: "Admin/front-desk generate slots through the gateway in clinic tz, stored UTC, idempotent, never touching held/booked/withdrawn slots"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_tracer_generated_slots_are_found_in_utc_through_the_gateway"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_second_identical_generation_creates_nothing"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_regeneration_never_touches_held_booked_or_withdrawn_slots"
        status: pass
    human_judgment: false
  - id: D3
    description: "Clinic scope (identical 404), RBAC 403, 31-day cap 422, Provider down 503 with no write, inactive provider 409"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py (scope/range/503/409 tests)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Search extends generation on demand within the 28-day horizon and stays available when Provider is down"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_search_beyond_the_generated_range_generates_missing_days_first"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_search_still_answers_when_extension_fails"
        status: pass
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py#test_search_entirely_beyond_the_horizon_generates_nothing"
        status: pass
    human_judgment: false
  - id: D5
    description: "Staff withdraw available slots only (200 / repeat 200 / held-booked 409 / out-of-clinic 404), audited"
    requirement: SCHED-01
    verification:
      - kind: integration
        ref: "tests/scheduling/test_slots_api.py (withdraw tests)"
        status: pass
    human_judgment: false
  - id: D6
    description: "Gateway routes /api/slots, /api/appointments, /api/waitlist to scheduling; /internal still 404"
    verification:
      - kind: unit
        ref: "tests/gateway/test_routing.py"
        status: pass
    human_judgment: false

duration: 29min
completed: 2026-10-13
status: complete
---

# Phase 2 Plan 04: Slot Supply Summary

**Scheduling now builds real slots from Provider's weekly templates in the clinic's IANA timezone and stores them as UTC. Generation is DST-safe and idempotent (`ON CONFLICT DO NOTHING`). Staff can generate and withdraw slots, and any role can search them through the gateway, with search filling in missing days on demand within the 28-day horizon.**

## Performance

- **Duration:** about 29 min
- **Started:** 2026-10-13T05:10:00Z
- **Completed:** 2026-10-13T05:39:00Z
- **Tasks:** 3
- **Files modified:** 16

## Accomplishments
- Pure generator `domain/slot_generation.py` (stdlib + uuid only). In America/New_York, the spring-forward 01:00-05:00 window gives 3 slots and the fall-back 00:00-04:00 window gives 5. No slot starts at a wall time that does not exist, and no two candidates overlap.
- `references.py` reads Provider and Organization (and Patient, for later plans) over aud-scoped service tokens. `api/deps.py` builds one `ServiceTokenProvider` the first time it is needed. If the secret can't be read, it fails closed with a 503.
- The `/api/slots` generate, search and withdraw endpoints, with clinic scope (D-18), audit, range bounds and fail-closed upstream handling.
- Three gateway route rows for Scheduling, and the routing docstring now matches them.
- `seed_bookable_world` creates a whole bookable world through the gateway for later plans: an Asia/Karachi clinic, a provider with a linked login and 7 templates (09:00-12:00, 30-minute slots), a front-desk user, a patient profile linked to a patient account, and 18 generated slots.

## Task Commits

1. **Task 1: Tracer (generate + search via gateway)** - `a9b692e` (feat)
2. **Task 2: DST, idempotency and scope proofs** - `66b235e` (test, includes a DST-gap end fix)
3. **Task 3: On-demand extension + withdrawal** - `ba86b38` (feat)

**Plan metadata:** see the `docs(02-04)` commit.

## Files Created/Modified
- `services/scheduling/domain/slot_generation.py`: `local_to_utc`, `expand_templates`, `CandidateSlot`, `AvailabilityTemplate`
- `services/scheduling/domain/schemas.py`: `SlotGenerateRequest/Response`, `SlotOut`
- `services/scheduling/references.py`: six service-token reference reads
- `services/scheduling/api/deps.py`: `build_service_tokens`, `get_service_tokens`
- `services/scheduling/api/slots.py`: router, `generate_for_provider`, `GenerationRefused`
- `services/scheduling/repository/slot_repo.py`: `insert_generated_slots`, `search_slots`, `latest_generated_start`, `get_slot`, `withdraw_slot`
- `services/scheduling/config.py`: `client_secret_path`, `slot_horizon_days=28`, `slot_max_range_days=31`
- `services/scheduling/app.py`: mounts the slots router
- `services/gateway/routing.py`: three Scheduling rows
- `libs/internal_client.py`: opt-in `allow_list` on `internal_request`
- tests: `seed.py`, `test_slot_generation.py` (12), `test_slots_api.py` (16), gateway and internal-client additions

## Decisions Made
See `key-decisions` in the frontmatter. Range semantics: a request is rejected (422) when `to_date - from_date > 31 days`, or `end - start > 31 days` for search. `from_date` and `to_date` are inclusive dates in the clinic's local time.

## Deviations from Plan

### Auto-fixed Issues

**1. [Plan gap] `internal_request` rejected list bodies**
- **Found during:** Task 1
- **Issue:** Provider's `/availability` returns a JSON array, but `internal_request` treats any non-object 200 body as `UpstreamServiceUnavailable`. That made `get_provider_availability` impossible to build "via internal_request" as the plan asked.
- **Fix:** Added a keyword `allow_list: bool = False` (default behaviour unchanged) and a unit test for it.
- **Files modified:** libs/internal_client.py, tests/libs/test_internal_client.py
- **Committed in:** a9b692e

**2. [Bug] A DST-gap window end shortened the window**
- **Found during:** Task 2 (test written first)
- **Issue:** A template ending at a nonexistent wall time (02:30 on the spring-forward day) used the earlier of the two readings. That dropped a real half hour before the gap.
- **Fix:** The window now ends at the instant the gap starts (the offset change, found to the minute).
- **Committed in:** 66b235e

**3. [Bug] Expired ORM instance read after rollback in the 409 withdraw path**
- **Found during:** Task 3
- **Issue:** `slot.clinic_id` was read after `db.rollback()`, which raised `MissingGreenlet` (500).
- **Fix:** `clinic_id` is captured before the rollback.
- **Committed in:** ba86b38

**4. [Plan gap] Search needed a way to return every status**
- **Found during:** Task 2
- **Issue:** The plan's `status` filter had no value meaning "all", but the non-resurrection proof needs to see held, booked and cancelled slots.
- **Fix:** `GET /api/slots?status=any` (the default stays `available`). Any other unknown value is a 422.
- **Committed in:** 66b235e

**5. [Design] Extension start date and audit**
- The plan says to extend from `latest + 1 day`. This uses `latest`'s UTC date instead, so no local day is skipped in negative-offset zones; re-running that one day is idempotent.
- Extension audits only when it actually created rows.
- The gateway test's exact route-table dict was updated in Task 1, not Task 2, to keep the Task 1 commit green.

---

**Total deviations:** 5 (2 bugs found by tests, 2 plan gaps, 1 design refinement)
**Impact on plan:** All were needed for correctness. No scope creep.

## Issues Encountered
- **Human-verify checkpoint:** none in this plan. The temporalio 1.34.0 approval belongs to 02-06 and was not touched.
- **Semgrep:** not run (not installed in the executor environment).
- **Known tradeoff:** `latest_generated_start` is the only marker of how far slots have been generated. A search whose `end` is past the latest slot (for example, days with no template) re-runs a bounded, idempotent extension: four upstream GETs and a 0-row insert. A per-provider "generated-through" marker would remove this and could be added later.

## Verification
- Plan verify commands: test_slots_api, then the Task 2 trio (67 passed), then `tests/scheduling` (64 passed).
- Full suite `pytest tests -q -m "not e2e"`: **517 passed** (481 before this plan).

## User Setup Required
None. Compose does not yet set `SCHEDULING_CLIENT_SECRET_PATH` for the scheduling container. The default is `infra/secrets/service-clients/scheduling.secret`, and a missing file gives a 503 on generate. Search still works and logs `slots.extend_failed`. Whichever plan wires the worker and compose secrets should set it.

## Next Phase Readiness
- The booking API can reuse `references.get_patient_by_user` and `get_patient_ref`, `get_service_tokens`, `slot_repo.get_slot` and `seed_bookable_world`.
- `/api/appointments` and `/api/waitlist` are already routed to Scheduling by the gateway. Until those routers exist, Scheduling answers them with a 404.

---
*Phase: 02-scheduling-service-workflow*
*Completed: 2026-10-13*
