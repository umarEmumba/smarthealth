"""Every Phase 2 and Phase 3 requirement maps to at least one existing test file.

Phase 2 (plan 02-12) and Phase 3 (plan 03-20) each have a trace table below.

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
        "tests/e2e/test_phase2_flows.py",
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


# --- Phase 3 (plan 03-20) -------------------------------------------------------------
#
# Test files only, using the names that plans 03-01..03-19 actually produced. This map and
# its checks never edit or tick anything in REQUIREMENTS.md: the verifier and the user own
# that file.

PHASE3_TRACE: dict[str, list[str]] = {
    "EVENT-01": [
        "tests/events/test_schemas.py",
        "tests/e2e/test_phase3_outbox_cdc.py",
        "tests/e2e/test_phase3_booking_events.py",
        "tests/e2e/test_phase3_registry_events.py",
    ],
    "EVENT-02": [
        "tests/events/test_outbox_roundtrip.py",
        "tests/scheduling/test_outbox.py",
        "tests/visit/test_outbox.py",
        "tests/patient/test_outbox.py",
        "tests/provider/test_outbox.py",
        "tests/e2e/test_phase3_outbox_cdc.py",
        "tests/e2e/test_phase3_booking_events.py",
        "tests/e2e/test_phase3_visit_downstream.py",
    ],
    "EVENT-03": [
        "tests/events/test_consumer_idempotency.py",
        "tests/visit/test_visit_consumer.py",
        "tests/analytics/test_projections.py",
        "tests/e2e/test_phase3_redelivery.py",
    ],
    "EVENT-04": [
        "tests/notification/test_tasks.py",
        "tests/workflows/test_reminder_workflow.py",
        "tests/e2e/test_phase3_notifications.py",
    ],
    "EVENT-05": [
        "tests/events/test_consumer_dlq.py",
        "tests/notification/test_dlq.py",
        "tests/e2e/test_phase3_notifications.py",
    ],
    "NOTIF-01": [
        "tests/notification/test_triggers.py",
        "tests/e2e/test_phase3_notifications.py",
    ],
    "NOTIF-02": [
        "tests/workflows/test_reminder_workflow.py",
        "tests/workflows/test_replay_reminder.py",
        "tests/e2e/test_phase3_notifications.py",
    ],
    "NOTIF-03": [
        "tests/workflows/test_reminder_workflow.py",
        "tests/e2e/test_phase3_notifications.py",
    ],
    "NOTIF-04": [
        "tests/notification/test_dedupe.py",
        "tests/e2e/test_phase3_redelivery.py",
    ],
    **{
        f"ANLY-0{i}": [
            "tests/analytics/test_projections.py",
            "tests/analytics/test_api.py",
            "tests/e2e/test_phase3_analytics.py",
            "tests/e2e/test_phase3_metrics.py",
        ]
        for i in range(1, 6)
    },
    "ANLY-06": [
        "tests/analytics/test_replay_rebuild.py",
        "tests/libs/test_service_isolation.py",
    ],
    "OBS-01": [
        "tests/infra/test_grafana_provisioning.py",
        "tests/infra/test_prometheus_config.py",
        "tests/e2e/test_phase3_metrics.py",
    ],
    "OBS-02": [
        "tests/events/test_trace_chain.py",
        "tests/e2e/test_phase3_trace.py",
    ],
    "OBS-03": [
        "tests/libs/test_observability.py",
        "tests/e2e/test_phase3_logs.py",
    ],
    "DASH-03": [
        "tests/docs/test_dashboard.py",
        "tests/docs/test_phase3_docs.py",
    ],
    "DASH-04": [
        "tests/docs/test_dashboard.py",
    ],
}

PHASE3_TRACE_NOTES: dict[str, str] = {
    "EVENT-04": (
        "Deviation from the requirement text: projections run on the analytics-projection Kafka "
        "consumer (D-10), not as a Celery task, because the idempotency claim must share the "
        "effect's transaction; Celery covers notifications and follow-up sends. The deviation was "
        "ACCEPTED by the user at plan 03-01 Task 2 (reply: 'Accept all six (Recommended)', item 2 "
        "of the User-veto list); the requirement text in REQUIREMENTS.md was not edited."
    ),
    "NOTIF-04": (
        "One send per dedupe key holds at the delivery log and at the fake provider's idempotency "
        "store (the idempotency key is the dedupe key); a real SMS or email provider must accept "
        "an idempotency key for the same guarantee."
    ),
}

EXPECTED_PHASE3_IDS = (
    {f"EVENT-0{i}" for i in range(1, 6)}
    | {f"NOTIF-0{i}" for i in range(1, 5)}
    | {f"ANLY-0{i}" for i in range(1, 7)}
    | {f"OBS-0{i}" for i in range(1, 4)}
    | {"DASH-03", "DASH-04"}
)


def test_phase3_trace_covers_exactly_the_twenty_phase3_requirements() -> None:
    assert set(PHASE3_TRACE) == EXPECTED_PHASE3_IDS
    assert len(PHASE3_TRACE) == 20
    assert all(PHASE3_TRACE[req] for req in PHASE3_TRACE), "a requirement maps to no test file"


@pytest.mark.parametrize("requirement", sorted(PHASE3_TRACE))
def test_every_phase3_mapped_test_file_exists_and_defines_tests(requirement: str) -> None:
    for relative in PHASE3_TRACE[requirement]:
        path = REPO_ROOT / relative
        assert path.is_file(), f"{requirement}: {relative} does not exist"
        assert re.search(r"^\s*(async\s+)?def test_", path.read_text(), re.MULTILINE), (
            f"{requirement}: {relative} defines no test function"
        )


def test_phase3_requirements_exist_in_requirements_doc_and_are_not_edited_here() -> None:
    """Reads REQUIREMENTS.md only; ticking a requirement is the verifier's and the user's job."""
    text = (REPO_ROOT / ".planning" / "REQUIREMENTS.md").read_text()
    for requirement in EXPECTED_PHASE3_IDS:
        assert f"**{requirement}**" in text, requirement


def test_phase3_trace_notes_are_traced_and_record_the_deviations() -> None:
    assert set(PHASE3_TRACE_NOTES) <= set(PHASE3_TRACE)
    event04 = PHASE3_TRACE_NOTES["EVENT-04"]
    assert "analytics-projection" in event04
    assert "ACCEPTED" in event04, "the EVENT-04 note must carry the user's recorded answer"
    assert "Celery" in event04
    notif04 = PHASE3_TRACE_NOTES["NOTIF-04"]
    assert "idempotency key" in notif04
    assert "dedupe key" in notif04
