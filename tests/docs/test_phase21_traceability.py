"""Every Phase 2.1 mentor-review item maps to existing tests and to the docs (plan 02.1-13).

`PHASE21_TRACE` maps the fourteen review items of
`.planning/phases/02.1-week-1-2-mentor-review-remediation/FEEDBACK.md` (M1, M2,
S3..S7, C1..C5, D1, D2) to the test files that prove them. The tests below fail
when an item is dropped or added, when a mapped file disappears or defines no
test function, when an item id stops being named in the documentation, or when
the docs go back to claiming that no load test exists. Whether the mapped tests
PASS is the job of the suites themselves (`pytest tests -m "not e2e"`); this
module only keeps the mapping and the documentation from rotting. It mirrors
`test_requirement_traceability.py` (Phase 2).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

PHASE21_TRACE: dict[str, list[str]] = {
    # M1 appointment state machine, check-in window, cancel/reschedule refusal.
    "M1": [
        "tests/scheduling/test_appointment_states.py",
        "tests/visit/test_check_in_scheduling_state.py",
        "tests/visit/test_downstream.py",
    ],
    # M2 provider schedule change reaches slots and bookings.
    "M2": [
        "tests/provider/test_schedule_change_feed.py",
        "tests/scheduling/test_schedule_change.py",
        "tests/workflows/test_schedule_change_workflow.py",
    ],
    # S3 failures after commit: state-guarded compensations, non-fatal audits.
    "S3": [
        "tests/workflows/test_post_commit_safety.py",
    ],
    # S4 stuck states: reconcilers and the open-ended visit billing retry.
    "S4": [
        "tests/workflows/test_reconciler.py",
        "tests/visit/test_visit_reconciler.py",
        "tests/workflows/test_visit_billing_retry.py",
    ],
    # S5 a freed slot is offered to the waitlist again.
    "S5": [
        "tests/workflows/test_booking_workflow.py",
        "tests/workflows/test_reconciler.py",
    ],
    # S6 booking a slot in the past is rejected.
    "S6": [
        "tests/scheduling/test_past_slot_booking.py",
    ],
    # S7 the notification is requested only after confirm.
    "S7": [
        "tests/workflows/test_replay_booking_patches.py",
        "tests/workflows/test_booking_workflow.py",
    ],
    # C1 single-use, rotated refresh tokens with logout revocation.
    "C1": [
        "tests/identity/test_refresh_rotation.py",
    ],
    # C2 bcrypt off the event loop.
    "C2": [
        "tests/identity/test_password_executor.py",
    ],
    # C3 case-insensitive email and its migration.
    "C3": [
        "tests/identity/test_email_case_insensitive.py",
        "tests/identity/test_email_migration.py",
    ],
    # C4 a patient reads their own profile, a provider their own appointments.
    "C4": [
        "tests/patient/test_self_profile.py",
        "tests/scheduling/test_provider_appointments.py",
    ],
    # C5 paginated patient, provider and clinic lists.
    "C5": [
        "tests/patient/test_patient_pagination.py",
        "tests/organization/test_clinic_pagination.py",
        "tests/provider/test_provider_pagination.py",
    ],
    # D1 quantified NFRs, single-sourced.
    "D1": [
        "tests/loadtest/test_nfr_consistency.py",
    ],
    # D2 a runnable load test.
    "D2": [
        "tests/loadtest/test_loadtest_assets.py",
        "tests/loadtest/test_seed_plan.py",
    ],
}

EXPECTED_IDS = {"M1", "M2", "S3", "S4", "S5", "S6", "S7", "C1", "C2", "C3", "C4", "C5", "D1", "D2"}

README = REPO_ROOT / "README.md"
ARCHITECTURE = REPO_ROOT / "docs" / "architecture.md"
# The first Phase 2.1 section of the architecture doc; everything from here on
# (4d..4g, token and identity notes, limitations, decisions) is "the Phase 2.1 part".
PHASE21_HEADING = "## 4d. Appointment state machine (Phase 2.1)"

# Claims the documents must no longer make (Phase 2.1 delivered the NFR targets
# and a runnable load test; only the measurement is still missing).
NO_LOAD_TEST_CLAIMS = (
    r"no load test",
    r"there is no load test",
    r"without a load test",
    r"load test (does not|doesn't) exist",
    r"has no load test",
)


def _phase21_architecture() -> str:
    text = ARCHITECTURE.read_text(encoding="utf-8")
    assert PHASE21_HEADING in text, f"architecture.md lost its heading {PHASE21_HEADING!r}"
    return text[text.index(PHASE21_HEADING) :]


def test_trace_covers_exactly_the_fourteen_review_items() -> None:
    assert set(PHASE21_TRACE) == EXPECTED_IDS
    assert len(PHASE21_TRACE) == 14
    assert all(PHASE21_TRACE[item] for item in PHASE21_TRACE), "a review item maps to no test file"
    for item, files in PHASE21_TRACE.items():
        assert len(files) == len(set(files)), f"{item}: duplicate test file in its mapping"


@pytest.mark.parametrize("item", sorted(PHASE21_TRACE))
def test_every_mapped_test_file_exists_and_defines_tests(item: str) -> None:
    for relative in PHASE21_TRACE[item]:
        path = REPO_ROOT / relative
        assert path.is_file(), f"{item}: {relative} does not exist"
        assert re.search(r"^\s*(async\s+)?def test_", path.read_text(encoding="utf-8"), re.MULTILINE), (
            f"{item}: {relative} defines no test function"
        )


@pytest.mark.parametrize("item", sorted(PHASE21_TRACE))
def test_every_review_item_is_named_in_the_docs(item: str) -> None:
    docs = _phase21_architecture() + "\n" + README.read_text(encoding="utf-8")
    assert re.search(rf"(?<![A-Za-z0-9]){item}(?![A-Za-z0-9])", docs), (
        f"{item} is not named in the Phase 2.1 part of docs/architecture.md or in README.md"
    )


def test_architecture_review_map_has_one_row_per_item() -> None:
    section = _phase21_architecture()
    for item in EXPECTED_IDS:
        assert f"**{item}**" in section, f"the Phase 2.1 review map has no row for {item}"


@pytest.mark.parametrize("path", [README, ARCHITECTURE], ids=["README.md", "architecture.md"])
def test_docs_no_longer_claim_that_no_load_test_exists(path: Path) -> None:
    text = path.read_text(encoding="utf-8").lower()
    for claim in NO_LOAD_TEST_CLAIMS:
        assert not re.search(claim, text), f"{path.name} still claims: {claim!r}"


def test_docs_point_at_the_load_test_and_say_it_is_unmeasured() -> None:
    readme = README.read_text(encoding="utf-8")
    architecture = ARCHITECTURE.read_text(encoding="utf-8")
    for text in (readme, architecture):
        assert "loadtest/README.md" in text
        assert "docs/nfr.md" in text or "nfr.md" in text
        assert "not yet measured" in text
    # The files the docs point at exist.
    assert (REPO_ROOT / "loadtest" / "README.md").is_file()
    assert (REPO_ROOT / "docs" / "nfr.md").is_file()
