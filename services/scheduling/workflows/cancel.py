"""CancelWorkflow (plan 02-08; SCHED-06, BILL-02, D-13).

Started by `POST /api/appointments/{id}/cancel` as `cancel-{id}-{version}` on
`scheduling-tq`. The refund policy (`refund` / `no_refund`, cutoff and staff
waiver) was decided in the API request (research A11), so this workflow never
reads the clock and its history stays deterministic.

    1. cancel_appointment     appointment confirmed -> cancelled, slot -> available
                              (one local transaction)
    2. billing_cancel         refund_pending -> refunded, or no_refund
    3. schedule_notification  best effort (D-11), `booking_cancelled`

Cancellation is forward-only: once step 1 commits there is nothing to
compensate. Billing must then catch up, so step 2 runs on the long
compensation budget; if it still does not land, the workflow records
`cancel_incomplete:billing_pending` in the appointment history, audits
`billing.cancel_incomplete` for an operator, and returns
`ChangeResult("cancelled", "billing_pending")`. If step 1 is refused
(`NotCancellable`) the refusal is recorded and the result is `failed`.

Changing this code once histories exist (D-19): guard any change to the
command sequence with `workflow.patched(...)` and prove it with the replay test
(`tests/workflows/histories/cancel_refund_v1.json`).
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import COMPENSATION_ACTIVITY, FORWARD_ACTIVITY, NOTIFY_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_BILLING_CANCEL,
        ACT_CANCEL_APPOINTMENT,
        ACT_NOTIFY_CHANGE,
        ACT_RECORD_OUTCOME,
        BILLING_PENDING,
        CANCEL_WORKFLOW,
        NOTIFY_CANCELLED,
        OUTCOME_CANCELLED,
        OUTCOME_FAILED,
        CancelInput,
        ChangeOutcomeInput,
        ChangeResult,
        NotifyInput,
    )


def failure_reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    if isinstance(cause, TimeoutError):
        return "activity_timeout"
    return "activity_failed"


async def record_outcome(inp: ChangeOutcomeInput) -> None:
    """Best effort: a failure to write the note is logged, never raised."""
    try:
        await workflow.execute_activity(ACT_RECORD_OUTCOME, inp, **COMPENSATION_ACTIVITY)
    except ActivityError as err:
        workflow.logger.error("change.record_outcome_failed reason=%s", failure_reason(err))


async def notify(inp: NotifyInput) -> None:
    """Best effort (D-11): logged on failure, never fails the change."""
    try:
        await workflow.execute_activity(ACT_NOTIFY_CHANGE, inp, **NOTIFY_ACTIVITY)
    except ActivityError as err:
        workflow.logger.warning("change.notification_failed reason=%s", failure_reason(err))


@workflow.defn(name=CANCEL_WORKFLOW)
class CancelWorkflow:
    @workflow.run
    async def run(self, inp: CancelInput) -> ChangeResult:
        try:
            await workflow.execute_activity(ACT_CANCEL_APPOINTMENT, inp, **FORWARD_ACTIVITY)
        except ActivityError as err:
            reason = failure_reason(err)
            workflow.logger.info("cancel.refused reason=%s", reason)
            await record_outcome(
                ChangeOutcomeInput(
                    appointment_id=inp.appointment_id,
                    clinic_id=inp.clinic_id,
                    note=f"cancel_failed:{reason}",
                    audit_action="appointment.cancel_failed",
                )
            )
            return ChangeResult(outcome=OUTCOME_FAILED, reason=reason)

        billing_done = True
        try:
            await workflow.execute_activity(ACT_BILLING_CANCEL, inp, **COMPENSATION_ACTIVITY)
        except ActivityError as err:
            billing_done = False
            workflow.logger.error("cancel.billing_incomplete reason=%s", failure_reason(err))
            await record_outcome(
                ChangeOutcomeInput(
                    appointment_id=inp.appointment_id,
                    clinic_id=inp.clinic_id,
                    note=f"cancel_incomplete:{BILLING_PENDING}",
                    audit_action="billing.cancel_incomplete",
                )
            )

        await notify(
            NotifyInput(
                appointment_id=inp.appointment_id,
                clinic_id=inp.clinic_id,
                notification_type=NOTIFY_CANCELLED,
            )
        )
        if not billing_done:
            return ChangeResult(outcome=OUTCOME_CANCELLED, reason=BILLING_PENDING)
        return ChangeResult(outcome=OUTCOME_CANCELLED)
