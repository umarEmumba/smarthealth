"""WaitlistAdvanceWorkflow logic in isolation (plan 02-09; D-12, research Pattern 7).

Stub activities are registered under the real `waitlist.*` names and a stub
child workflow under the real `BookingWorkflow` name, so these tests exercise
only the advance loop: one child per claim, resolve after every child, stop on
a lost race, skip failed entries, and the attempt cap. The stub claim encodes
the child's scripted result in `patient_id` ("confirmed" or
"failed:<Reason>"), so the stub child is deterministic. Time-skipping server.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.domain.contracts import (
    ACT_WAITLIST_CLAIM,
    ACT_WAITLIST_RESOLVE,
    BOOKING_WORKFLOW,
    BookingInput,
    BookingResult,
    ClaimResult,
    WaitlistAdvanceInput,
    WaitlistAdvanceResult,
    WaitlistClaimInput,
    waitlist_advance_workflow_id,
)
from services.scheduling.workflows.waitlist_advance import WaitlistAdvanceWorkflow

CHILD_RUNS: list[str] = []


@workflow.defn(name=BOOKING_WORKFLOW, sandboxed=False)
class _StubBookingWorkflow:
    @workflow.run
    async def run(self, inp: BookingInput) -> BookingResult:
        if not workflow.unsafe.is_replaying():
            CHILD_RUNS.append(inp.appointment_id)
        if inp.patient_id == "confirmed":
            return BookingResult(outcome="confirmed")
        return BookingResult(outcome="failed", reason=inp.patient_id.split(":", 1)[1])


def claimed(child_result: str) -> ClaimResult:
    return ClaimResult(
        outcome="claimed",
        entry_id=str(uuid.uuid4()),
        patient_id=child_result,
        provider_id=str(uuid.uuid4()),
    )


async def run_advance(
    env: WorkflowEnvironment,
    claims: list[ClaimResult | Callable[[], Exception]],
    *,
    max_attempts: int = 5,
) -> tuple[WaitlistAdvanceResult, list[str], list[dict]]:
    """Run the advance with scripted claim results (the last one repeats).
    Returns (result, claimed appointment ids, resolve inputs)."""
    CHILD_RUNS.clear()
    claim_calls: list[str] = []
    resolves: list[dict] = []

    @activity.defn(name=ACT_WAITLIST_CLAIM)
    async def claim(inp: WaitlistClaimInput) -> ClaimResult:
        claim_calls.append(inp.appointment_id)
        scripted = claims[min(len(claim_calls), len(claims)) - 1]
        if callable(scripted):
            raise scripted()
        return scripted

    @activity.defn(name=ACT_WAITLIST_RESOLVE)
    async def resolve(inp: Any) -> str:
        resolves.append(dict(inp) if isinstance(inp, dict) else inp.__dict__)
        return "ok"

    source = f"test-{uuid.uuid4().hex}"
    async with Worker(
        env.client,
        task_queue=SCHEDULING_TASK_QUEUE,
        workflows=[WaitlistAdvanceWorkflow, _StubBookingWorkflow],
        activities=[claim, resolve],
    ):
        result = await env.client.execute_workflow(
            WaitlistAdvanceWorkflow.run,
            WaitlistAdvanceInput(
                slot_id=str(uuid.uuid4()),
                clinic_id=str(uuid.uuid4()),
                source_workflow_id=source,
                max_attempts=max_attempts,
            ),
            id=waitlist_advance_workflow_id(source),
            task_queue=SCHEDULING_TASK_QUEUE,
        )
    return result, claim_calls, resolves


async def test_confirmed_on_the_first_claim_is_one_claim_one_child_one_resolve(
    temporal_env: WorkflowEnvironment,
) -> None:
    first = claimed("confirmed")
    result, claims, resolves = await run_advance(temporal_env, [first])

    assert result == WaitlistAdvanceResult(outcome="booked", booked_entry_id=first.entry_id, attempts=1)
    assert len(claims) == 1
    assert CHILD_RUNS == claims
    assert [(r["entry_id"], r["appointment_id"], r["booking_outcome"]) for r in resolves] == [
        (first.entry_id, claims[0], "confirmed")
    ]


async def test_lost_slot_race_in_the_child_resolves_and_stops(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, claims, resolves = await run_advance(
        temporal_env, [claimed("failed:SlotUnavailable"), claimed("confirmed")]
    )

    assert result.outcome == "slot_gone" and result.attempts == 1
    assert len(claims) == 1 and len(CHILD_RUNS) == 1
    assert [(r["booking_outcome"], r["reason"]) for r in resolves] == [("failed", "SlotUnavailable")]


async def test_business_failure_moves_on_to_the_next_entry(
    temporal_env: WorkflowEnvironment,
) -> None:
    second = claimed("confirmed")
    result, claims, resolves = await run_advance(
        temporal_env, [claimed("failed:BillingRejected"), second]
    )

    assert result == WaitlistAdvanceResult(outcome="booked", booked_entry_id=second.entry_id, attempts=2)
    assert len(claims) == 2 and CHILD_RUNS == claims
    # A fresh appointment id per iteration.
    assert len(set(claims)) == 2
    assert [r["reason"] for r in resolves] == ["BillingRejected", None]


async def test_failed_entries_every_time_exhaust_after_exactly_max_attempts(
    temporal_env: WorkflowEnvironment,
) -> None:
    failed = ClaimResult(outcome="entry_failed", entry_id=str(uuid.uuid4()), reason="PatientOverlap")
    result, claims, resolves = await run_advance(temporal_env, [failed])

    assert result == WaitlistAdvanceResult(outcome="exhausted", booked_entry_id=None, attempts=5)
    assert len(claims) == 5
    assert CHILD_RUNS == [] and resolves == []


async def test_saga_failures_are_capped_too(temporal_env: WorkflowEnvironment) -> None:
    result, claims, resolves = await run_advance(
        temporal_env, [claimed("failed:BillingRejected")], max_attempts=3
    )

    assert result.outcome == "exhausted" and result.attempts == 3
    assert len(claims) == 3 and len(CHILD_RUNS) == 3 and len(resolves) == 3


async def test_no_match_slot_gone_and_slot_taken_end_the_advance_at_once(
    temporal_env: WorkflowEnvironment,
) -> None:
    for outcome in ("no_match", "slot_gone", "slot_taken"):
        result, claims, resolves = await run_advance(temporal_env, [ClaimResult(outcome=outcome)])
        assert result == WaitlistAdvanceResult(outcome=outcome, booked_entry_id=None, attempts=0)
        assert len(claims) == 1 and CHILD_RUNS == [] and resolves == []


async def test_claim_infrastructure_failure_is_bounded(temporal_env: WorkflowEnvironment) -> None:
    result, claims, resolves = await run_advance(
        temporal_env, [lambda: ApplicationError("transient", type="UpstreamServiceUnavailable")]
    )

    assert result.outcome == "claim_failed" and result.attempts == 0
    # FORWARD_ACTIVITY allows five attempts, all with the same appointment id.
    assert len(claims) == 5 and len(set(claims)) == 1
    assert CHILD_RUNS == [] and resolves == []
