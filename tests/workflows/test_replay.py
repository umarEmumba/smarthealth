"""Recorded booking histories replay against the current workflow code
(plan 02-07 Task 3; D-19 versioning bar, research "Don't Hand-Roll").

`histories/booking_confirmed_v1.json` and `booking_billing_rejected_v1.json`
were captured from real sagas (real activities, in-process mesh, time-skipping
test server). Replaying them with `Replayer` re-runs `BookingWorkflow` against
the recorded events: any edit that changes the command sequence without a
`workflow.patched(...)` guard raises a nondeterminism error here, before it
can break bookings that are in flight in a real cluster.

The fixtures are ids and plain strings only: workflow input carries no
coverage reference or token (the coverage reference stays in the appointment
row), and the recorder strips stack traces (they hold local source lines and
paths, and replay does not need them). A test re-checks that on every run.

Plan 02-08 adds `cancel_refund_v1.json` (CancelWorkflow, refund before the
cutoff) and `reschedule_v1.json` (RescheduleWorkflow, successful swap), captured
the same way from the gateway tracer scenarios. Plan 02-09 replays them to
prove its `workflow.patched` change is backward compatible.

Plan 02-09 (`WAITLIST_PATCH_ID`): CancelWorkflow and RescheduleWorkflow now
start `WaitlistAdvanceWorkflow` as a child, but only behind
`workflow.patched(WAITLIST_PATCH_ID)`. Every v1 history (recorded before the
patch, no marker) is replayed against the PATCHED code with the full set of
Scheduling workflows, and the v2 histories (`cancel_waitlist_v2.json`,
`reschedule_waitlist_v2.json`, recorded with the patch: marker + child start)
replay too. `waitlist_advance_booked_v1.json` is the advance itself (claim,
child BookingWorkflow, resolve), for its own future changes.

Plan 02-11: `visit_completed_v1.json` is Visit's `VisitCompletedWorkflow`
(billing finalized, both intents recorded), captured from the gateway tracer
scenario (recorder `-k record_visit`). Plan 02.1-09 added a fourth, patched branch:
that fixture is the PRE-patch history and stays frozen (the recorder refuses to
overwrite it with a patched run); `visit_completed_p21.json` and the replay proofs
live in `test_replay_visit_completion.py`.

Re-record (only for a deliberate, versioned change; add `_v2` files and keep
the old ones replaying):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay.py -k record
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest
import httpx
from httpx import AsyncClient
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from services.scheduling.domain.contracts import (
    ACT_RESERVE,
    WAITLIST_PATCH_ID,
    WaitlistAdvanceResult,
    BOOKING_WORKFLOW,
    BookingInput,
    BookingResult,
    ChangeResult,
    booking_workflow_id,
    cancel_workflow_id,
    reschedule_workflow_id,
    waitlist_advance_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from services.scheduling.workflows.cancel import CancelWorkflow
from services.scheduling.workflows.reschedule import RescheduleWorkflow
from services.scheduling.workflows.waitlist_advance import WaitlistAdvanceWorkflow
from services.visit.domain.contracts import VisitCompletedResult, visit_completed_workflow_id
from services.visit.workflows.visit_completed import VisitCompletedWorkflow
from tests.scheduling.seed import World, auth

HISTORIES = Path(__file__).parent / "histories"
FIXTURES = {
    "booking_confirmed_v1.json": "confirmed",
    "booking_billing_rejected_v1.json": "failed",
}
# Plan 02-08: cancel / reschedule histories and the workflow each replays against.
CHANGE_FIXTURES = {
    "cancel_refund_v1.json": CancelWorkflow,
    "reschedule_v1.json": RescheduleWorkflow,
}
FORBIDDEN = ("coverage", "Bearer", "DENY-")
# Plan 02-09: every Scheduling workflow, as the production worker registers them.
ALL_WORKFLOWS = [BookingWorkflow, CancelWorkflow, RescheduleWorkflow, WaitlistAdvanceWorkflow]
PRE_PATCH = sorted([*FIXTURES, *CHANGE_FIXTURES])
WAITLIST_FIXTURES = {
    "cancel_waitlist_v2.json": "CancelWorkflow",
    "reschedule_waitlist_v2.json": "RescheduleWorkflow",
    "waitlist_advance_booked_v1.json": "WaitlistAdvanceWorkflow",
}


def _load(name: str) -> WorkflowHistory:
    return WorkflowHistory.from_json(f"replay-{name}", (HISTORIES / name).read_text())


@pytest.mark.parametrize("name", sorted(FIXTURES))
async def test_recorded_booking_history_replays_against_current_code(name: str) -> None:
    result = await Replayer(workflows=[BookingWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_history_fixtures_carry_no_coverage_token_or_failing_prefix(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


@pytest.mark.parametrize("name", sorted(CHANGE_FIXTURES))
async def test_recorded_change_history_replays_against_current_code(name: str) -> None:
    result = await Replayer(workflows=[CHANGE_FIXTURES[name]]).replay_workflow(_load(name))
    assert result.replay_failure is None


@pytest.mark.parametrize("name", sorted(CHANGE_FIXTURES))
def test_change_history_fixtures_carry_no_coverage_or_token(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


# --- plan 02-09: WAITLIST_PATCH_ID backward compatibility ----------------------


def _events(name: str) -> list[dict]:
    return json.loads((HISTORIES / name).read_text())["events"]


def _patch_ids(name: str) -> list[str]:
    """Patch ids recorded by `workflow.patched` (core_patch markers; the id is
    a base64 JSON payload)."""
    ids = []
    for event in _events(name):
        attrs = event.get("markerRecordedEventAttributes")
        if not attrs or attrs.get("markerName") != "core_patch":
            continue
        for payload in attrs["details"]["patch-data"]["payloads"]:
            ids.append(json.loads(base64.b64decode(payload["data"]))["id"])
    return ids


def _has_patch_marker(name: str) -> bool:
    return WAITLIST_PATCH_ID in _patch_ids(name)


def _child_types(name: str) -> list[str]:
    return [
        e["startChildWorkflowExecutionInitiatedEventAttributes"]["workflowType"]["name"]
        for e in _events(name)
        if e.get("eventType") == "EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED"
    ]


@pytest.mark.parametrize("name", PRE_PATCH)
async def test_pre_patch_history_replays_against_the_patched_workflows(name: str) -> None:
    """The backward-compatibility proof for `WAITLIST_PATCH_ID`: histories
    recorded before plan 02-09 carry no marker and no child start, and still
    replay against the patched CancelWorkflow/RescheduleWorkflow."""
    assert not _has_patch_marker(name)
    assert _child_types(name) == []
    result = await Replayer(workflows=ALL_WORKFLOWS).replay_workflow(_load(name))
    assert result.replay_failure is None


def _load_as_recorded(name: str) -> WorkflowHistory:
    """Load a history under the workflow id it ran with. The patched
    cancel/reschedule derive the advance's child id from their OWN workflow id
    (`waitlist-advance-{id}`), so replay must use the recorded id; it is
    recovered from that child start (the started event does not carry it)."""
    prefix = "waitlist-advance-"
    for event in _events(name):
        attrs = event.get("startChildWorkflowExecutionInitiatedEventAttributes")
        if attrs and attrs["workflowId"].startswith(prefix):
            workflow_id = attrs["workflowId"].removeprefix(prefix)
            return WorkflowHistory.from_json(workflow_id, (HISTORIES / name).read_text())
    return _load(name)


@pytest.mark.parametrize("name", sorted(WAITLIST_FIXTURES))
async def test_waitlist_history_replays_against_current_code(name: str) -> None:
    result = await Replayer(workflows=ALL_WORKFLOWS).replay_workflow(_load_as_recorded(name))
    assert result.replay_failure is None


@pytest.mark.parametrize("name", ["cancel_waitlist_v2.json", "reschedule_waitlist_v2.json"])
def test_v2_histories_took_the_patched_path(name: str) -> None:
    assert _has_patch_marker(name)
    assert _child_types(name) == ["WaitlistAdvanceWorkflow"]


def test_advance_history_ran_the_booking_saga_as_a_child() -> None:
    assert _child_types("waitlist_advance_booked_v1.json") == ["BookingWorkflow"]


@pytest.mark.parametrize("name", sorted(WAITLIST_FIXTURES))
def test_waitlist_history_fixtures_carry_no_coverage_or_token(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


@workflow.defn(name=BOOKING_WORKFLOW, sandboxed=False)
class _ReorderedBookingWorkflow:
    """An UNversioned edit (validate dropped): must not replay."""

    @workflow.run
    async def run(self, inp: BookingInput) -> BookingResult:
        from libs.temporal_policies import FORWARD_ACTIVITY  # noqa: PLC0415

        await workflow.execute_activity(ACT_RESERVE, inp, **FORWARD_ACTIVITY)
        return BookingResult(outcome="confirmed")


async def test_an_unversioned_edit_fails_replay() -> None:
    """The gate actually bites: the same history against a reordered workflow
    is a nondeterminism error."""
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[_ReorderedBookingWorkflow]).replay_workflow(
            _load("booking_confirmed_v1.json")
        )


# --- recorder (opt-in) ---------------------------------------------------------


def _scrub(node: Any) -> Any:
    if isinstance(node, dict):
        return {k: _scrub(v) for k, v in node.items() if k != "stackTrace"}
    if isinstance(node, list):
        return [_scrub(v) for v in node]
    return node


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_booking_histories(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    world = booking_world
    start, end = world.local_day_bounds(world.last_day)
    slots = (
        await client.get(
            "/api/slots",
            params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
            headers=auth(world.admin),
        )
    ).json()
    # Built at runtime so the failing prefix is never a literal in this module's
    # recording path; it travels only in the HTTP body, never in workflow input.
    denied = "-".join(["DENY", uuid.uuid4().hex[:6]])
    HISTORIES.mkdir(exist_ok=True)

    for (name, outcome), slot, coverage in zip(
        sorted(FIXTURES.items()), (slots[1], slots[0]), (denied, None), strict=True
    ):
        # sorted(): booking_billing_rejected_v1 first (denied), then confirmed.
        body = {"slot_id": slot["id"], "patient_id": str(world.patient_id)}
        if coverage is not None:
            body["coverage_ref"] = coverage
        resp = await client.post(
            "/api/appointments",
            json=body,
            headers=auth(world.front_desk) | {"Idempotency-Key": f"rec-{uuid.uuid4().hex}"},
        )
        assert resp.status_code == 202, resp.text
        handle = temporal_env.client.get_workflow_handle(
            booking_workflow_id(resp.json()["appointment_id"]), result_type=BookingResult
        )
        async with temporal_env.time_skipping_unlocked():
            assert (await handle.result()).outcome == outcome
        history = await handle.fetch_history()
        document = _scrub(history.to_json_dict())
        (HISTORIES / name).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_change_histories(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Plan 02-08: one cancel (refund, before the cutoff) and one successful
    reschedule, driven through the gateway like the tracer tests."""
    from tests.scheduling.test_cancel import book_confirmed, day_slots  # noqa: PLC0415

    world = booking_world
    slots = await day_slots(client, world)
    HISTORIES.mkdir(exist_ok=True)

    async def save(workflow_id: str, name: str, outcome: str) -> None:
        handle = temporal_env.client.get_workflow_handle(workflow_id, result_type=ChangeResult)
        async with temporal_env.time_skipping_unlocked():
            assert (await handle.result()).outcome == outcome
        history = await handle.fetch_history()
        document = _scrub(history.to_json_dict())
        (HISTORIES / name).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")

    cancelled = await book_confirmed(client, temporal_env, world, slots[0]["id"])
    resp = await client.post(
        f"/api/appointments/{cancelled['id']}/cancel", json={}, headers=auth(world.front_desk)
    )
    assert resp.status_code == 202, resp.text
    await save(cancel_workflow_id(cancelled["id"], cancelled["version"]), "cancel_refund_v1.json", "cancelled")

    moved = await book_confirmed(client, temporal_env, world, slots[2]["id"])
    resp = await client.post(
        f"/api/appointments/{moved['id']}/reschedule",
        json={"new_slot_id": slots[3]["id"]},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 202, resp.text
    await save(reschedule_workflow_id(moved["id"], moved["version"]), "reschedule_v1.json", "rescheduled")


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_waitlist_histories(
    client: AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    """Plan 02-09: the waitlist tracer (a cancel auto-books a waiting patient)
    and the reschedule scenario (the old slot goes to the waitlist), driven
    through the gateway with the patched workflows."""
    from tests.scheduling.test_cancel import day_slots  # noqa: PLC0415
    from tests.scheduling.test_waitlist import add_patient, book_for, joined  # noqa: PLC0415

    world = booking_world
    slots = await day_slots(client, world)
    patient_b, tokens_b = await add_patient(client, world)
    HISTORIES.mkdir(exist_ok=True)

    async def save(workflow_id: str, name: str, result_type: type, outcome: str) -> None:
        handle = temporal_env.client.get_workflow_handle(workflow_id, result_type=result_type)
        async with temporal_env.time_skipping_unlocked():
            assert (await handle.result()).outcome == outcome
        history = await handle.fetch_history()
        document = _scrub(history.to_json_dict())
        (HISTORIES / name).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")

    # Cancel: B's booking is cancelled, A (the world's patient) is waiting.
    cancelled = await book_for(client, temporal_env, world, slots[0]["id"], patient_b)
    await joined(client, world.patient, {"provider_id": str(world.provider_id)})
    resp = await client.post(
        f"/api/appointments/{cancelled['id']}/cancel", json={}, headers=auth(world.front_desk)
    )
    assert resp.status_code == 202, resp.text
    cancel_id = cancel_workflow_id(cancelled["id"], cancelled["version"])
    await save(cancel_id, "cancel_waitlist_v2.json", ChangeResult, "cancelled")
    await save(
        waitlist_advance_workflow_id(cancel_id),
        "waitlist_advance_booked_v1.json",
        WaitlistAdvanceResult,
        "booked",
    )

    # Reschedule: A (now booked on slot 0) moves to slot 2; B waits for slot 0.
    await joined(client, tokens_b, {"provider_id": str(world.provider_id)})
    moved = (
        await client.get(
            "/api/appointments",
            params={"patient_id": str(world.patient_id)},
            headers=auth(world.front_desk),
        )
    ).json()[0]
    resp = await client.post(
        f"/api/appointments/{moved['id']}/reschedule",
        json={"new_slot_id": slots[2]["id"]},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 202, resp.text
    reschedule_id = reschedule_workflow_id(moved["id"], moved["version"])
    await save(reschedule_id, "reschedule_waitlist_v2.json", ChangeResult, "rescheduled")
    handle = temporal_env.client.get_workflow_handle(
        waitlist_advance_workflow_id(reschedule_id), result_type=WaitlistAdvanceResult
    )
    async with temporal_env.time_skipping_unlocked():
        assert (await handle.result()).outcome == "booked"



# --- plan 02-11: Visit's downstream workflow ---------------------------------

VISIT_FIXTURES = ["visit_completed_v1.json"]


@pytest.mark.parametrize("name", VISIT_FIXTURES)
async def test_recorded_visit_history_replays_against_current_code(name: str) -> None:
    result = await Replayer(workflows=[VisitCompletedWorkflow]).replay_workflow(_load(name))
    assert result.replay_failure is None


@pytest.mark.parametrize("name", VISIT_FIXTURES)
def test_visit_history_fixtures_carry_no_coverage_or_token(name: str) -> None:
    raw = (HISTORIES / name).read_text()
    for forbidden in FORBIDDEN:
        assert forbidden not in raw, f"{name} contains {forbidden!r}"
    assert "stackTrace" not in raw


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("SMARTHEALTH_RECORD_HISTORIES") != "1",
    reason="re-records the committed history fixtures; opt in with SMARTHEALTH_RECORD_HISTORIES=1",
)
async def test_record_visit_history(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
) -> None:
    from tests.visit.conftest import _visit_world, widened_checkin_window  # noqa: PLC0415
    from tests.visit.test_downstream import _completed_visit  # noqa: PLC0415

    # Plan 02.1-09: check-in now needs the widened window (the world's slots are far out).
    with widened_checkin_window():
        async for vw in _visit_world(client, mesh):
            visit, _, completed = await _completed_visit(client, mesh, vw)
            assert completed.status_code == 200, completed.text
            handle = temporal_env.client.get_workflow_handle(
                visit_completed_workflow_id(visit["id"]), result_type=VisitCompletedResult
            )
            async with temporal_env.time_skipping_unlocked():
                assert (await handle.result()).billing == "finalized"
            history = await handle.fetch_history()
            document = _scrub(history.to_json_dict())
    if '"core_patch"' in json.dumps(document):
        # The workflow now records workflow.patched markers (plan 02.1-09);
        # `visit_completed_v1.json` must stay a marker-free, pre-patch history.
        pytest.skip(
            "VisitCompletedWorkflow records patch markers now; the patched history "
            "is recorded by tests/workflows/test_replay_visit_completion.py"
        )
    HISTORIES.mkdir(exist_ok=True)
    (HISTORIES / VISIT_FIXTURES[0]).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
