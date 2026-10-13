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

Re-record (only for a deliberate, versioned change; add `_v2` files and keep
the old ones replaying):
    SMARTHEALTH_RECORD_HISTORIES=1 pytest tests/workflows/test_replay.py -k record
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from services.scheduling.domain.contracts import (
    ACT_RESERVE,
    BOOKING_WORKFLOW,
    BookingInput,
    BookingResult,
    booking_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow
from tests.scheduling.seed import World, auth

HISTORIES = Path(__file__).parent / "histories"
FIXTURES = {
    "booking_confirmed_v1.json": "confirmed",
    "booking_billing_rejected_v1.json": "failed",
}
FORBIDDEN = ("coverage", "Bearer", "DENY-")


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
