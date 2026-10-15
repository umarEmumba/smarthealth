"""BookingWorkflow logic in isolation (plan 02-06; D-09, D-11, research Pattern 3).

Stub activities are registered under the real `ACT_*` names, so these tests
exercise only the workflow: step order, best-effort notification, and
reverse-order compensation followed by one `mark_failed`. They run on a
time-skipping Temporal test server, so retry backoff costs no wall-clock time.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from temporalio import activity
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
    BookingInput,
    BookingResult,
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


async def run_saga(
    env: WorkflowEnvironment, failures: dict[str, Callable[[], Exception]] | None = None
) -> tuple[BookingResult, list[str]]:
    calls: list[str] = []
    inp = BookingInput(
        appointment_id=str(uuid.uuid4()),
        clinic_id=str(uuid.uuid4()),
        patient_id=str(uuid.uuid4()),
        provider_id=str(uuid.uuid4()),
        slot_id=str(uuid.uuid4()),
    )
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[BookingWorkflow],
        activities=make_stubs(calls, failures or {}),
    ):
        result = await env.client.execute_workflow(
            BookingWorkflow.run, inp, id=f"booking-{inp.appointment_id}", task_queue=TASK_QUEUE
        )
    return result, calls


async def test_happy_path_runs_d09_order_and_confirms(temporal_env: WorkflowEnvironment) -> None:
    result, calls = await run_saga(temporal_env)

    assert calls == ["validate", "reserve_slot", "billing_precheck", "schedule_notification", "confirm"]
    assert result == BookingResult(outcome="confirmed", reason=None)


async def test_notification_failure_never_fails_or_compensates_the_booking(
    temporal_env: WorkflowEnvironment,
) -> None:
    result, calls = await run_saga(temporal_env, {ACT_NOTIFY: retryable("NotifierDown")})

    assert result.outcome == "confirmed"
    # Bounded best-effort retries, then the saga moves on and confirms.
    assert calls.count("schedule_notification") == 3
    assert calls[-1] == "confirm"
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
        "schedule_notification",
        "confirm",
        "void_billing",
        "release_slot",
        "mark_failed:ConfirmLostHold",
    ]
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
