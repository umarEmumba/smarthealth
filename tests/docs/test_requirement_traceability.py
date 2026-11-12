"""Every Phase 2 requirement maps to at least one existing test file (plan 02-12).

`PHASE2_TRACE` is copied from the 02-VALIDATION "Requirement -> test" map (test
files only). The test fails when a requirement is dropped or added, when a
mapped file disappears, or when a mapped file has no test function, so the
traceability table in the docs cannot silently rot. Whether those tests PASS
is the job of the suite itself (`pytest tests -m "not e2e"` and `-m e2e`).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

PHASE2_TRACE: dict[str, list[str]] = {
    "SCHED-01": [
        "tests/scheduling/test_booking_api.py",
        "tests/scheduling/test_slots_api.py",
        "tests/e2e/test_phase2_flows.py",
    ],
    "SCHED-02": [
        "tests/workflows/test_booking_workflow.py",
        "tests/workflows/test_trace.py",
    ],
    "SCHED-03": [
        "tests/scheduling/test_concurrent_booking.py",
        "tests/scheduling/test_appointment_constraints.py",
        "tests/e2e/test_demo_concurrent_booking.py",
    ],
    "SCHED-04": [
        "tests/scheduling/test_idempotency.py",
        "tests/scheduling/test_booking_api.py",
    ],
    "SCHED-05": [
        "tests/scheduling/test_reschedule.py",
        "tests/workflows/test_change_workflows.py",
    ],
    "SCHED-06": [
        "tests/scheduling/test_cancel.py",
        "tests/workflows/test_change_workflows.py",
        "tests/e2e/test_phase2_flows.py",
    ],
    "SCHED-07": [
        "tests/scheduling/test_waitlist.py",
        "tests/scheduling/test_waitlist_concurrency.py",
        "tests/workflows/test_waitlist_workflow.py",
        "tests/e2e/test_phase2_flows.py",
    ],
    "SCHED-08": [
        "tests/workflows/test_booking_compensation.py",
        "tests/workflows/test_maintenance.py",
        "tests/scheduling/test_slot_state_steps.py",
        "tests/e2e/test_worker_crash.py",
        "tests/workflows/test_replay.py",
    ],
    "VISIT-01": [
        "tests/visit/test_check_in.py",
        "tests/scheduling/test_internal_appointments.py",
    ],
    "VISIT-02": [
        "tests/visit/test_transitions.py",
        "tests/provider/test_internal_providers.py",
    ],
    "VISIT-03": [
        "tests/visit/test_transitions.py",
    ],
    "VISIT-04": [
        "tests/visit/test_downstream.py",
        "tests/workflows/test_visit_completed.py",
        "tests/workflows/test_visit_schedules.py",
        "tests/e2e/test_phase2_flows.py",
    ],
    "VISIT-05": [
        "tests/visit/test_visit_list.py",
    ],
    "BILL-01": [
        "tests/billing/test_precheck.py",
    ],
    "BILL-02": [
        "tests/billing/test_transitions.py",
        "tests/scheduling/test_cancel.py",
        "tests/scheduling/test_reschedule.py",
        "tests/visit/test_downstream.py",
    ],
}

EXPECTED_IDS = (
    {f"SCHED-0{i}" for i in range(1, 9)}
    | {f"VISIT-0{i}" for i in range(1, 6)}
    | {f"BILL-0{i}" for i in range(1, 3)}
)


def test_trace_covers_exactly_the_fifteen_phase2_requirements() -> None:
    assert set(PHASE2_TRACE) == EXPECTED_IDS
    assert len(PHASE2_TRACE) == 15
    assert all(PHASE2_TRACE[req] for req in PHASE2_TRACE), "a requirement maps to no test file"


@pytest.mark.parametrize("requirement", sorted(PHASE2_TRACE))
def test_every_mapped_test_file_exists_and_defines_tests(requirement: str) -> None:
    for relative in PHASE2_TRACE[requirement]:
        path = REPO_ROOT / relative
        assert path.is_file(), f"{requirement}: {relative} does not exist"
        assert re.search(r"^\s*(async\s+)?def test_", path.read_text(), re.MULTILINE), (
            f"{requirement}: {relative} defines no test function"
        )


def test_requirements_doc_lists_the_same_ids() -> None:
    text = (REPO_ROOT / ".planning" / "REQUIREMENTS.md").read_text()
    for requirement in EXPECTED_IDS:
        assert f"**{requirement}**" in text, requirement
