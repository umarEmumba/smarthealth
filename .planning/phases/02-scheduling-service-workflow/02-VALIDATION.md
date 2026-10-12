---
phase: "02"
slug: scheduling-service-workflow
status: draft
nyquist_compliant: false
wave_0_complete: false
created: "2026-10-12"
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
| **Estimated runtime** | ~210 seconds (full suite, baseline; growing) |

---

## Sampling Rate

- **After every task commit:** Run the service's own directory, e.g. `.venv/bin/pytest tests/scheduling tests/workflows -q`
- **After every plan wave:** Run `.venv/bin/pytest tests -q -m "not e2e"`
- **Before `/gsd-verify-work`:** Full suite green plus `-m e2e` against the up stack
- **Max feedback latency:** 210 seconds

---

## Per-Task Verification Map

Populated by the planner/checker once PLAN.md files exist. Requirement → test mapping (from RESEARCH.md):

| Req ID | Behavior | Test Type | Automated Command | File Exists |
|--------|----------|-----------|-------------------|-------------|
| SCHED-01 | Book → 202, `confirmed` after saga | integration | `.venv/bin/pytest tests/scheduling/test_booking_api.py -x` | ❌ W0 |
| SCHED-02 | Saga order; confirm only at end | workflow | `.venv/bin/pytest tests/workflows/test_booking_workflow.py -x` | ❌ W0 |
| SCHED-03 | N parallel bookings → one active appointment | integration | `.venv/bin/pytest tests/scheduling/test_concurrent_booking.py -x` | ❌ W0 |
| SCHED-04 | Idempotency key replay / conflict | integration | `.venv/bin/pytest tests/scheduling/test_idempotency.py -x` | ❌ W0 |
| SCHED-05 | Reschedule atomic swap | integration + workflow | `.venv/bin/pytest tests/scheduling/test_reschedule.py -x` | ❌ W0 |
| SCHED-06 | Cancel releases slot, refund by cutoff | integration + workflow | `.venv/bin/pytest tests/scheduling/test_cancel.py -x` | ❌ W0 |
| SCHED-07 | Waitlist FIFO advance | integration + workflow | `.venv/bin/pytest tests/scheduling/test_waitlist.py -x` | ❌ W0 |
| SCHED-08 | Compensation, audit, crash recovery | workflow + e2e | `.venv/bin/pytest tests/workflows/test_booking_compensation.py -x` | ❌ W0 |
| VISIT-01..05 | Visit state machine, downstream, list | integration | `.venv/bin/pytest tests/visit -x` | ❌ W0 |
| BILL-01/02 | Pre-check and refund states | integration | `.venv/bin/pytest tests/billing -x` | ❌ W0 |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/workflows/conftest.py` — time-skipping Temporal env, Worker factory with in-process mesh
- [ ] `tests/scheduling/conftest.py` — committed-data helpers for concurrent transactions
- [ ] `tests/conftest.py` — add `visit`, `billing` to `HARNESS_SERVICES`
- [ ] Seed helpers (clinic → department → provider → template → patient → slots)
- [ ] `tests/libs/test_service_isolation.py` — maps for new services
- [ ] `temporalio==1.34.0` install (after human-verify checkpoint)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Dashboard workflow/state diagrams render and read correctly | SCHED-02 (DASH) | Visual quality | Open `docs/architecture-dashboard.html` in a browser and check the new booking/visit diagrams and Temporal rationale |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 210s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
