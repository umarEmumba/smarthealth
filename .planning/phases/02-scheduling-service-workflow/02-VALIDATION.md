---
phase: "02"
slug: scheduling-service-workflow
status: draft
nyquist_compliant: false
wave_0_complete: false
created: "2026-10-12"
updated: "2026-10-13"
---

# Phase 02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 + pytest-asyncio 1.4.0 (`asyncio_mode = "auto"`) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (markers `integration`, `e2e`) |
| **Quick run command** | `.venv/bin/pytest tests/scheduling tests/workflows -q` |
| **Full suite command** | `.venv/bin/pytest tests -q -m "not e2e"` |
| **Real-stack command** | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/pytest tests/e2e -m e2e -q` (stack up with `GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000`) |
| **Estimated runtime** | ~210 seconds (full suite, baseline; growing) |

---

## Sampling Rate

- **After every task commit:** Run the task's own `<automated>` command (each is scoped to the touched service directory, e.g. `.venv/bin/pytest tests/scheduling tests/workflows -q`)
- **After every plan wave:** Run `.venv/bin/pytest tests -q -m "not e2e"`
- **Before `/gsd-verify-work`:** Full suite green plus `-m e2e` against the up stack (02-12 Tasks 2 and 4)
- **Max feedback latency:** 210 seconds

---

## Per-Task Verification Map

Every task below has an `<automated>` verify in its PLAN.md except the two blocking human checkpoints (02-06 Task 1, 02-12 Task 5). Those are why `nyquist_compliant` stays `false`; both are immediately followed or preceded by an automated task covering the same area (02-06 Task 2 asserts the installed `temporalio` version; 02-12 Task 4 runs the static dashboard gate), so no three consecutive tasks lack automated verification.

| Task | Wave | Requirement(s) | Test Type | Automated Command | Test File(s) | Status |
|------|------|----------------|-----------|-------------------|--------------|--------|
| 02-01 T1 | 1 | SCHED-01, SCHED-02 (internal auth) | integration | `.venv/bin/pytest tests/libs/test_service_tokens.py -x` | `tests/libs/test_service_tokens.py` | ⬜ |
| 02-01 T2 | 1 | SCHED-02, SCHED-08 (aud/exp/allowed callers) | integration | `.venv/bin/pytest tests/libs/test_service_tokens.py tests/libs/test_verification.py tests/identity tests/provider tests/organization tests/patient -x -q` | `tests/libs/test_service_tokens.py`, `tests/libs/test_verification.py`, `tests/identity/test_service_token.py`, `tests/identity/test_internal_users.py`, `tests/provider/test_internal_providers.py` | ⬜ |
| 02-01 T3 | 1 | SCHED-08 (idempotent audit, write client) | unit + integration | `.venv/bin/pytest tests/libs -x -q` | `tests/libs/test_internal_client.py`, `tests/libs/test_audit_dedupe.py`, `tests/libs/test_audit_indexes.py` | ⬜ |
| 02-02 T1 | 2 | SCHED-03 | integration (real Postgres) | alembic up/down/up `&& .venv/bin/pytest tests/scheduling/test_concurrent_booking.py -x` | `tests/scheduling/test_concurrent_booking.py` | ⬜ |
| 02-02 T2 | 2 | SCHED-03, SCHED-04 | integration | `.venv/bin/pytest tests/scheduling tests/libs/test_service_isolation.py -x -q` | `tests/scheduling/test_appointment_constraints.py`, `tests/scheduling/test_idempotency.py`, `tests/scheduling/test_slot_constraint.py` | ⬜ |
| 02-02 T3 | 2 | SCHED-08 | integration | `.venv/bin/pytest tests/scheduling/test_slot_state_steps.py -x` | `tests/scheduling/test_slot_state_steps.py` | ⬜ |
| 02-03 T1 | 3 | SCHED-01 | integration | `.venv/bin/pytest tests/patient/test_internal_patients.py -x` | `tests/patient/test_internal_patients.py` | ⬜ |
| 02-03 T2 | 3 | SCHED-01, VISIT-02 | integration | `.venv/bin/pytest tests/provider -x -q` | `tests/provider/test_internal_providers.py` | ⬜ |
| 02-03 T3 | 3 | SCHED-01 (clinic timezone) | integration | `.venv/bin/pytest tests/organization tests/identity/test_service_token.py -x -q` | `tests/organization/test_internal_lookups.py`, `tests/organization/test_clinics.py` | ⬜ |
| 02-04 T1 | 4 | SCHED-01 | integration | `.venv/bin/pytest tests/scheduling/test_slots_api.py -x` | `tests/scheduling/test_slots_api.py` | ⬜ |
| 02-04 T2 | 4 | SCHED-01 | unit + integration | `.venv/bin/pytest tests/scheduling/test_slot_generation.py tests/scheduling/test_slots_api.py tests/gateway/test_routing.py -x -q` | `tests/scheduling/test_slot_generation.py`, `tests/gateway/test_routing.py` | ⬜ |
| 02-04 T3 | 4 | SCHED-01 | integration | `.venv/bin/pytest tests/scheduling -x -q` | `tests/scheduling/test_slots_api.py` | ⬜ |
| 02-05 T1 | 5 | BILL-01 | integration | db-init + alembic up/down/up `&& .venv/bin/pytest tests/billing/test_precheck.py -x` | `tests/billing/test_precheck.py` | ⬜ |
| 02-05 T2 | 5 | BILL-01, BILL-02 | integration | `.venv/bin/pytest tests/billing -x -q` | `tests/billing/test_precheck.py`, `tests/billing/test_transitions.py` | ⬜ |
| 02-05 T3 | 5 | BILL-01 (isolation, routing) | unit | `.venv/bin/pytest tests/libs/test_service_isolation.py tests/gateway -x -q` | `tests/libs/test_service_isolation.py`, `tests/gateway/test_routing.py` | ⬜ |
| 02-06 T1 | 6 | — (package legitimacy) | **manual checkpoint** | none (blocking human verify of `temporalio==1.34.0`) | — | ⬜ |
| 02-06 T2 | 6 | SCHED-01, SCHED-02 | integration + workflow | `.venv/bin/python -c "import temporalio..." && .venv/bin/pytest tests/scheduling/test_booking_api.py -x` | `tests/scheduling/test_booking_api.py` | ⬜ |
| 02-06 T3 | 6 | SCHED-02, SCHED-04, BILL-01 | workflow + integration | `.venv/bin/pytest tests/workflows tests/scheduling/test_booking_api.py -x -q` | `tests/workflows/test_booking_workflow.py`, `tests/workflows/test_contracts.py`, `tests/workflows/test_trace.py` | ⬜ |
| 02-07 T1 | 7 | SCHED-08 | workflow + integration | `.venv/bin/pytest tests/workflows/test_booking_compensation.py -x` | `tests/workflows/test_booking_compensation.py` | ⬜ |
| 02-07 T2 | 7 | SCHED-08 (reaper, starter) | integration (Compose Temporal) | `docker compose ... up -d --wait temporal && .venv/bin/pytest tests/workflows/test_maintenance.py tests/workflows/test_schedules.py -x` | `tests/workflows/test_maintenance.py`, `tests/workflows/test_schedules.py` | ⬜ |
| 02-07 T3 | 7 | SCHED-03, SCHED-08 | integration + replay + e2e | `.venv/bin/pytest tests/scheduling/test_concurrent_booking.py tests/workflows/test_replay.py -x -q`; `.venv/bin/pytest tests/e2e/test_worker_crash.py -m e2e -x` | `tests/scheduling/test_concurrent_booking.py`, `tests/workflows/test_replay.py`, `tests/e2e/test_worker_crash.py` | ⬜ |
| 02-08 T1 | 8 | SCHED-06, BILL-02 | integration + workflow | `.venv/bin/pytest tests/scheduling/test_cancel.py -x` | `tests/scheduling/test_cancel.py` | ⬜ |
| 02-08 T2 | 8 | SCHED-05, BILL-02 | integration + workflow | `.venv/bin/pytest tests/scheduling/test_reschedule.py -x` | `tests/scheduling/test_reschedule.py` | ⬜ |
| 02-08 T3 | 8 | SCHED-05, SCHED-06 | workflow + replay | `.venv/bin/pytest tests/scheduling/test_cancel.py tests/scheduling/test_reschedule.py tests/workflows -x -q` | `tests/workflows/test_change_workflows.py`, `tests/workflows/test_replay.py` | ⬜ |
| 02-09 T1 | 9 | SCHED-07 | integration + workflow | alembic up/down/up `&& .venv/bin/pytest tests/scheduling/test_waitlist.py -x` | `tests/scheduling/test_waitlist.py` | ⬜ |
| 02-09 T2 | 9 | SCHED-07, SCHED-05 | integration + workflow | `.venv/bin/pytest tests/scheduling/test_waitlist.py tests/workflows/test_waitlist_workflow.py -x -q` | `tests/scheduling/test_waitlist.py`, `tests/workflows/test_waitlist_workflow.py` | ⬜ |
| 02-09 T3 | 9 | SCHED-07, SCHED-06 (patched replay) | integration (real Postgres) + replay | `.venv/bin/pytest tests/scheduling/test_waitlist_concurrency.py tests/workflows/test_replay.py tests/libs/test_service_isolation.py -x -q` | `tests/scheduling/test_waitlist_concurrency.py`, `tests/workflows/test_replay.py` | ⬜ |
| 02-10 T1 | 10 | VISIT-01 | integration | alembic up/down/up `&& .venv/bin/pytest tests/visit/test_check_in.py -x` | `tests/visit/test_check_in.py` | ⬜ |
| 02-10 T2 | 10 | VISIT-01, VISIT-02, VISIT-03 | integration (incl. 10-way concurrency) | `.venv/bin/pytest tests/visit tests/scheduling/test_internal_appointments.py -x -q` | `tests/visit/test_transitions.py`, `tests/visit/test_check_in.py`, `tests/scheduling/test_internal_appointments.py` | ⬜ |
| 02-10 T3 | 10 | VISIT-05 | integration | `.venv/bin/pytest tests/visit tests/libs/test_service_isolation.py tests/gateway -x -q` | `tests/visit/test_visit_list.py` | ⬜ |
| 02-11 T1 | 11 | VISIT-04, BILL-02 | integration + workflow | `.venv/bin/pytest tests/visit/test_downstream.py -x` | `tests/visit/test_downstream.py` | ⬜ |
| 02-11 T2 | 11 | VISIT-04 | integration (Compose Temporal) | `docker compose ... up -d --wait temporal && .venv/bin/pytest tests/visit/test_downstream.py tests/workflows/test_visit_schedules.py -x -q` | `tests/visit/test_downstream.py`, `tests/workflows/test_visit_schedules.py` | ⬜ |
| 02-11 T3 | 11 | VISIT-04 | workflow + replay | `.venv/bin/pytest tests/workflows -x -q` | `tests/workflows/test_visit_completed.py`, `tests/workflows/test_contracts.py`, `tests/workflows/test_replay.py` | ⬜ |
| 02-12 T1 | 12 | SCHED-01, SCHED-02 | e2e (Compose) | secrets + `docker compose ... up -d --build --wait && scripts/compose_healthcheck.sh && SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/pytest tests/e2e/test_phase2_flows.py -m e2e -x -k booking_confirmed` | `tests/e2e/test_phase2_flows.py` | ⬜ |
| 02-12 T2 | 12 | SCHED-03, SCHED-06, SCHED-07, SCHED-08, VISIT-01..04, BILL-01, BILL-02 | e2e (Compose) | `SMARTHEALTH_E2E_REQUIRE_STACK=1 .venv/bin/pytest tests/e2e -m e2e -x -q` | `tests/e2e/test_phase2_flows.py`, `tests/e2e/test_demo_concurrent_booking.py`, `tests/e2e/test_worker_crash.py` | ⬜ |
| 02-12 T3 | 12 | DASH (docs) | static grep | `grep -q ... README.md docs/erd.md docs/architecture.md ...` (see plan) | `README.md`, `docs/architecture.md`, `docs/erd.md` | ⬜ |
| 02-12 T4 | 12 | all 15 (traceability), DASH | static + full suite | `.venv/bin/pytest tests/docs -x -q && .venv/bin/pytest tests -q -m "not e2e"` | `tests/docs/test_dashboard.py`, `tests/docs/test_requirement_traceability.py` | ⬜ |
| 02-12 T5 | 12 | DASH (visual) | **manual checkpoint** | none (blocking human visual check) | — | ⬜ |

### Requirement → test map (source for `tests/docs/test_requirement_traceability.py`)

| Req ID | Behavior | Plans | Primary test files |
|--------|----------|-------|--------------------|
| SCHED-01 | Book → 202, `confirmed` after saga; slots exist to book | 02-03, 02-04, 02-06, 02-12 | `tests/scheduling/test_booking_api.py`, `tests/scheduling/test_slots_api.py`, `tests/e2e/test_phase2_flows.py` |
| SCHED-02 | Saga order; confirm only at end; notification best-effort | 02-01, 02-06 | `tests/workflows/test_booking_workflow.py`, `tests/workflows/test_trace.py` |
| SCHED-03 | N parallel bookings → one active appointment (DB, raw SQL, HTTP) | 02-02, 02-07, 02-12 | `tests/scheduling/test_concurrent_booking.py`, `tests/scheduling/test_appointment_constraints.py`, `tests/e2e/test_demo_concurrent_booking.py` |
| SCHED-04 | Idempotency key replay / mismatch / expiry | 02-02, 02-06 | `tests/scheduling/test_idempotency.py`, `tests/scheduling/test_booking_api.py` |
| SCHED-05 | Reschedule atomic reserve-then-swap | 02-08, 02-09 | `tests/scheduling/test_reschedule.py`, `tests/workflows/test_change_workflows.py` |
| SCHED-06 | Cancel releases slot, refund by cutoff | 02-08, 02-09, 02-12 | `tests/scheduling/test_cancel.py`, `tests/workflows/test_change_workflows.py` |
| SCHED-07 | Waitlist FIFO advance, failure skip, lost race, no double claim | 02-09, 02-12 | `tests/scheduling/test_waitlist.py`, `tests/scheduling/test_waitlist_concurrency.py`, `tests/workflows/test_waitlist_workflow.py` |
| SCHED-08 | Compensation, audit, reaper/starter, crash recovery, replay | 02-01, 02-02, 02-07 | `tests/workflows/test_booking_compensation.py`, `tests/workflows/test_maintenance.py`, `tests/scheduling/test_slot_state_steps.py`, `tests/e2e/test_worker_crash.py`, `tests/workflows/test_replay.py` |
| VISIT-01 | Check-in requires confirmed appointment | 02-10 | `tests/visit/test_check_in.py`, `tests/scheduling/test_internal_appointments.py` |
| VISIT-02 | Provider/front-desk advance states | 02-03, 02-10 | `tests/visit/test_transitions.py`, `tests/provider/test_internal_providers.py` |
| VISIT-03 | Idempotent, duplicate-safe transitions | 02-10 | `tests/visit/test_transitions.py` |
| VISIT-04 | Completion triggers downstream without blocking; sweeper | 02-11 | `tests/visit/test_downstream.py`, `tests/workflows/test_visit_completed.py`, `tests/workflows/test_visit_schedules.py` |
| VISIT-05 | Clinic-scoped visit status list | 02-10 | `tests/visit/test_visit_list.py` |
| BILL-01 | Pre-check pending → verified/failed, deterministic | 02-05, 02-06 | `tests/billing/test_precheck.py` |
| BILL-02 | Cancel/reschedule/finalize billing state | 02-05, 02-08, 02-11 | `tests/billing/test_transitions.py`, `tests/scheduling/test_cancel.py`, `tests/scheduling/test_reschedule.py`, `tests/visit/test_downstream.py` |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Folded into the plans that first need them (no separate Wave 0 plan):

- [ ] `tests/workflows/conftest.py` — time-skipping Temporal env, Worker factory with in-process mesh (02-06); shared to sibling test dirs via `tests/workflows/fixtures.py` + `pytest_plugins` (02-09 Task 1 if not already shared)
- [ ] `tests/scheduling/conftest.py` — committed-data helpers for concurrent transactions (02-02)
- [ ] `tests/conftest.py` — add `billing` (02-05) and `visit` (02-10) to `HARNESS_SERVICES`; service client secrets bootstrap (02-01)
- [ ] Seed helpers (clinic → department → provider → template → patient → slots): `tests/scheduling/seed.py` (02-04), provider-user option (02-10), e2e `tests/e2e/phase2_stack.py` (02-12)
- [ ] `tests/libs/test_service_isolation.py` — maps for new tables/services (02-02, 02-05, 02-09, 02-10)
- [ ] `temporalio==1.34.0` install (after human-verify checkpoint, 02-06)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `temporalio` package legitimacy before install | (supply chain) | Registry evidence judgment | 02-06 Task 1 checkpoint |
| Dashboard workflow/state diagrams render and read correctly | SCHED-02 (DASH) | Visual quality | 02-12 Task 5: open `docs/architecture-dashboard.html` offline and check the new booking/visit diagrams and Temporal rationale |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies (two human checkpoints excepted, see above)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (assigned to plans above)
- [x] No watch-mode flags
- [ ] Feedback latency < 210s (re-measure after 02-07; e2e excluded)
- [ ] `nyquist_compliant: true` set in frontmatter (blocked by the two human checkpoint tasks)

**Approval:** pending
