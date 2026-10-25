"""BookingWorkflow logic in isolation (plan 02-06; D-09, D-11, research Pattern 3).

Stub activities are registered under the real `ACT_*` names, so these tests
exercise only the workflow: step order, best-effort notification, and
reverse-order compensation followed by one `mark_failed`. They run on a
time-skipping Temporal test server, so retry backoff costs no wall-clock time.

Plan 02.1-07: the notification is requested AFTER confirm (S7), and a
top-level booking that fails after holding the slot starts a
`WaitlistAdvanceWorkflow` child (S5). The child-start assertions read the
workflow's own history (`StartChildWorkflowExecutionInitiated` events).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from services.scheduling.domain.contracts import (
    ACT_BILLING,
    ACT_CONFIRM,
    ACT_MARK_FAILED,
    ACT_NOTIFY,
    ACT_RELEASE,
    ACT_RESERVE,
    ACT_VALIDATE,
    ACT_VOID,
    BOOKING_WORKFLOW,
    WAITLIST_ADVANCE_WORKFLOW,
    BookingInput,
    BookingResult,
    waitlist_advance_workflow_id,
)
from services.scheduling.workflows.booking import BookingWorkflow

ALL_ACTIVITIES = (
    ACT_VALIDATE,
    ACT_RESERVE,
    ACT_BILLING,
    ACT_NOTIFY,
    ACT_CONFIRM,
    ACT_RELEASE,
    ACT_VOID,
    ACT_MARK_FAILED,
)
TASK_QUEUE = "booking-workflow-test"


def _short(name: str) -> str:
    return name.removeprefix("booking.")


def business(kind: str) -> Callable[[], Exception]:
    return lambda: ApplicationError("business outcome", type=kind, non_retryable=True)


def retryable(kind: str) -> Callable[[], Exception]:
    return lambda: ApplicationError("transient", type=kind)


def make_stubs(calls: list[str], failures: dict[str, Callable[[], Exception]]) -> list[Any]:
    """One stub per activity name: records its short name (mark_failed with its
    reason) and raises the configured failure, if any."""

    def stub(name: str) -> Any:
        @activity.defn(name=name)
        async def _run(arg: Any) -> None:
            if name == ACT_MARK_FAILED:
                calls.append(f"mark_failed:{arg['reason']}")
            else:
                calls.append(_short(name))
            if name in failures:
                raise failures[name]()

        return _run

    return [stub(name) for name in ALL_ACTIVITIES]


def _new_input() -> BookingInput:
    return BookingInput(
        appointment_id=str(uuid.uuid4()),
        clinic_id=str(uuid.uuid4()),
        patient_id=str(uuid.uuid4()),
        provider_id=str(uuid.uuid4()),
        slot_id=str(uuid.uuid4()),
    )


async def run_saga(
    env: WorkflowEnvironment, failures: dict[str, Callable[[], Exception]] | None = None
) -> tuple[BookingResult, list[str]]:
    result, calls, _ = await run_saga_with_history(env, failures)
    return result, calls


def _child_starts(history: Any) -> list[tuple[str, str]]:
    """(workflow type, workflow id) of every child start in a history."""
    out = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
            attrs = event.start_child_workflow_execution_initiated_event_attributes
            out.append((attrs.workflow_type.name, attrs.workflow_id))
    return out


async def run_saga_with_history(
    env: WorkflowEnvironment,
    failures: dict[str, Callable[[], Exception]] | None = None,
    *,
    inp: BookingInput | None = None,
    parent: bool = False,
) -> tuple[BookingResult, list[str], Any]:
    """Run the saga (top-level, or as a child of `_ParentOfBooking`) and return
    its result, the activity call order and the BOOKING workflow's history."""
    calls: list[str] = []
    inp = inp or _new_input()
    workflow_id = f"booking-{inp.appointment_id}"
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[BookingWorkflow, _ParentOfBooking],
        activities=make_stubs(calls, failures or {}),
    ):
        if parent:
            await env.client.execute_workflow(
                _ParentOfBooking.run, inp, id=f"parent-{inp.appointment_id}", task_queue=TASK_QUEUE
            )
            handle = env.client.get_workflow_handle(workflow_id, result_type=BookingResult)
            result = await handle.result()
        else:
            handle = await env.client.start_workflow(
                BookingWorkflow.run, inp, id=workflow_id, task_queue=TASK_QUEUE
            )
            result = await handle.result()
        history = await handle.fetch_history()
    return result, calls, history


@workflow.defn(name="ParentOfBookingTest", sandboxed=False)
class _ParentOfBooking:
    """Runs BookingWorkflow as a CHILD (as a waitlist advance does)."""

    @workflow.run
    async def run(self, inp: BookingInput) -> None:
        await workflow.execute_child_workflow(
            BOOKING_WORKFLOW, inp, id=f"booking-{inp.appointment_id}", task_queue=TASK_QUEUE
        )


async def test_happy_path_runs_d09_order_and_confirms(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env)

    # S7: the notification intent follows confirm.
    assert calls == ["validate", "reserve_slot", "billing_precheck", "confirm", "schedule_notification"]
    assert result == BookingResult(outcome="confirmed", reason=None)


async def test_notification_failure_never_fails_or_compensates_the_booking(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_saga(temporal_env, {ACT_NOTIFY: retryable("NotifierDown")})

    assert result.outcome == "confirmed"
    # Bounded best-effort retries, then the saga moves on and confirms.
    assert calls.count("schedule_notification") == 3
    assert calls[:4] == ["validate", "reserve_slot", "billing_precheck", "confirm"]
    assert not {"release_slot", "void_billing"} & set(calls)
    assert not [c for c in calls if c.startswith("mark_failed")]


async def test_billing_rejection_compensates_in_reverse_order(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env, {ACT_BILLING: business("BillingRejected")})

    assert calls == [
        "validate",
        "reserve_slot",
        "billing_precheck",
        "void_billing",
        "release_slot",
        "mark_failed:BillingRejected",
    ]
    assert result == BookingResult(outcome="failed", reason="BillingRejected")


async def test_slot_unavailable_releases_and_never_calls_billing(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env, {ACT_RESERVE: business("SlotUnavailable")})

    assert calls == ["validate", "reserve_slot", "release_slot", "mark_failed:SlotUnavailable"]
    assert "billing_precheck" not in calls
    assert result == BookingResult(outcome="failed", reason="SlotUnavailable")


async def test_lost_hold_at_confirm_voids_then_releases(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env, {ACT_CONFIRM: business("ConfirmLostHold")})

    assert calls == [
        "validate",
        "reserve_slot",
        "billing_precheck",
        "confirm",
        "void_billing",
        "release_slot",
        "mark_failed:ConfirmLostHold",
    ]
    # S7: a booking that failed at confirm never requested a notification.
    assert "schedule_notification" not in calls
    assert result == BookingResult(outcome="failed", reason="ConfirmLostHold")


async def test_invalid_reference_needs_no_compensation(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env, {ACT_VALIDATE: business("ReferenceInvalid")})

    assert calls == ["validate", "mark_failed:ReferenceInvalid"]
    assert result == BookingResult(outcome="failed", reason="ReferenceInvalid")


async def test_a_failing_compensation_is_logged_and_the_rest_still_run(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_saga(
        temporal_env,
        {ACT_BILLING: business("BillingRejected"), ACT_VOID: business("VoidRefused")},
    )

    assert calls == [
        "validate",
        "reserve_slot",
        "billing_precheck",
        "void_billing",
        "release_slot",
        "mark_failed:BillingRejected",
    ]
    assert result == BookingResult(outcome="failed", reason="BillingRejected")


async def test_exhausted_transient_failure_compensates_with_generic_reason(
    temporal_env: WorkflowEnvironment,
) -> None:
    # A retryable error with no business type still reaches compensation once
    # the bounded forward policy (5 attempts) is exhausted; the reason is the
    # error's type.
    result, calls = await run_saga(temporal_env, {ACT_BILLING: retryable("BillingUnreachable")})

    assert calls.count("billing_precheck") == 5
    assert calls[-3:] == ["void_billing", "release_slot", "mark_failed:BillingUnreachable"]
    assert result == BookingResult(outcome="failed", reason="BillingUnreachable")


# --- S5: a freed slot is offered to the waitlist again (plan 02.1-07) ----------------


async def test_billing_rejection_of_a_top_level_booking_starts_one_waitlist_advance(
    temporal_env: WorkflowEnvironment,
) -> None:
    inp = _new_input()
    result, calls, history = await run_saga_with_history(
        temporal_env, {ACT_BILLING: business("BillingRejected")}, inp=inp
    )

    assert calls == [
        "validate",
        "reserve_slot",
        "billing_precheck",
        "void_billing",
        "release_slot",
        "mark_failed:BillingRejected",
    ]
    assert result == BookingResult(outcome="failed", reason="BillingRejected")
    assert _child_starts(history) == [
        (WAITLIST_ADVANCE_WORKFLOW, waitlist_advance_workflow_id(f"booking-{inp.appointment_id}"))
    ]
    assert waitlist_advance_workflow_id(f"booking-{inp.appointment_id}") == (
        f"waitlist-advance-booking-{inp.appointment_id}"
    )


async def test_an_exhausted_infrastructure_failure_after_the_hold_also_re_offers_the_slot(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, _, history = await run_saga_with_history(
        temporal_env, {ACT_BILLING: retryable("BillingUnreachable")}
    )

    assert result == BookingResult(outcome="failed", reason="BillingUnreachable")
    assert [t for t, _ in _child_starts(history)] == [WAITLIST_ADVANCE_WORKFLOW]


async def test_reference_invalid_starts_no_waitlist_advance(temporal_env: WorkflowEnvironment) -> None:
    result, calls, history = await run_saga_with_history(
        temporal_env, {ACT_VALIDATE: business("ReferenceInvalid")}
    )

    assert result == BookingResult(outcome="failed", reason="ReferenceInvalid")
    assert calls == ["validate", "mark_failed:ReferenceInvalid"]
    assert _child_starts(history) == []


async def test_slot_unavailable_starts_no_waitlist_advance(temporal_env: WorkflowEnvironment) -> None:
    result, _, history = await run_saga_with_history(
        temporal_env, {ACT_RESERVE: business("SlotUnavailable")}
    )

    assert result == BookingResult(outcome="failed", reason="SlotUnavailable")
    assert _child_starts(history) == []


async def test_confirm_lost_hold_starts_no_waitlist_advance(temporal_env: WorkflowEnvironment) -> None:
    result, _, history = await run_saga_with_history(
        temporal_env, {ACT_CONFIRM: business("ConfirmLostHold")}
    )

    assert result == BookingResult(outcome="failed", reason="ConfirmLostHold")
    assert _child_starts(history) == []


async def test_a_booking_started_as_a_child_starts_no_waitlist_advance(
    temporal_env: WorkflowEnvironment,
) -> None:
    """A booking run by a waitlist advance is decided by its parent loop."""
    result, _, history = await run_saga_with_history(
        temporal_env, {ACT_BILLING: business("BillingRejected")}, parent=True
    )

    assert result == BookingResult(outcome="failed", reason="BillingRejected")
    assert _child_starts(history) == []


async def test_a_failed_child_start_is_logged_and_the_result_is_unchanged(
    temporal_env: WorkflowEnvironment,
) -> None:
    """The advance id is already taken by a running workflow: the start fails
    (WorkflowAlreadyStartedError) and the booking still returns `failed`."""
    inp = _new_input()
    blocker_id = waitlist_advance_workflow_id(f"booking-{inp.appointment_id}")
    # No worker polls this queue: the squatter stays Running.
    await temporal_env.client.start_workflow(
        "Squatter", id=blocker_id, task_queue="nobody-polls-this-queue"
    )

    result, calls, history = await run_saga_with_history(
        temporal_env, {ACT_BILLING: business("BillingRejected")}, inp=inp
    )

    assert result == BookingResult(outcome="failed", reason="BillingRejected")
    assert calls[-1] == "mark_failed:BillingRejected"
    # The start was attempted (the initiated event is recorded) and refused.
    assert [t for t, _ in _child_starts(history)] == [WAITLIST_ADVANCE_WORKFLOW]
    failed_starts = [
        e
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
    ]
    assert len(failed_starts) == 1
