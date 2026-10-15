"""CancelWorkflow (plan 02-08; SCHED-06, BILL-02, D-13).

Started by `POST /api/appointments/{id}/cancel` as `cancel-{id}-{version}` on
`scheduling-tq`. The refund policy (`refund` / `no_refund`, cutoff and staff
waiver) was decided in the API request (research A11), so this workflow never
reads the clock and its history stays deterministic.

    1. cancel_appointment     appointment confirmed -> cancelled, slot -> available
                              (one local transaction)
    2. billing_cancel         refund_pending -> refunded, or no_refund
    3. schedule_notification  best effort (D-11), `booking_cancelled`
    4. waitlist advance       (plan 02-09, D-12/D-13) start
                              `WaitlistAdvanceWorkflow` for the freed slot as a
                              child with `ParentClosePolicy.ABANDON`; not
                              awaited, the cancel result is unchanged

Cancellation is forward-only: once step 1 commits there is nothing to
compensate. Billing must then catch up, so step 2 runs on the long
compensation budget; if it still does not land, the workflow records
`cancel_incomplete:billing_pending` in the appointment history, audits
`billing.cancel_incomplete` for an operator, and returns
`ChangeResult("cancelled", "billing_pending")`. If step 1 is refused
(`NotCancellable`) the refusal is recorded and the result is `failed`.

Changing this code once histories exist (D-19): guard any change to the
command sequence with `workflow.patched(...)` and prove it with the replay test
(`tests/workflows/histories/cancel_refund_v1.json`). Step 4 is the first such
change: it runs only behind the `WAITLIST_PATCH_ID` patch marker, so a cancel
recorded before plan 02-09 (no marker in its history) replays without it, and
`cancel_waitlist_v2.json` proves the patched path.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import (
        COMPENSATION_ACTIVITY,
        FORWARD_ACTIVITY,
        NOTIFY_ACTIVITY,
        SCHEDULING_TASK_QUEUE,
    )
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
        WAITLIST_ADVANCE_WORKFLOW,
        WAITLIST_PATCH_ID,
        CancelInput,
        ChangeOutcomeInput,
        ChangeResult,
        NotifyInput,
        WaitlistAdvanceInput,
        waitlist_advance_workflow_id,
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


async def start_waitlist_advance(slot_id: str, clinic_id: str) -> None:
    """Start the waitlist advance for a freed slot (plan 02-09) as an
    ABANDONed child: it outlives this workflow and is never awaited. Its id is
    derived from this workflow's id, so a duplicate start is impossible."""
    source = workflow.info().workflow_id
    await workflow.start_child_workflow(
        WAITLIST_ADVANCE_WORKFLOW,
        WaitlistAdvanceInput(slot_id=slot_id, clinic_id=clinic_id, source_workflow_id=source),
        id=waitlist_advance_workflow_id(source),
        task_queue=SCHEDULING_TASK_QUEUE,
        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
    )


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
        # Reached only when step 1 reported "cancelled" or "already" (a refusal
        # returned above); "already" is a retried activity reporting the
        # cancellation it made itself.
        if workflow.patched(WAITLIST_PATCH_ID):
            await start_waitlist_advance(inp.slot_id, inp.clinic_id)
        if not billing_done:
            return ChangeResult(outcome=OUTCOME_CANCELLED, reason=BILLING_PENDING)
        return ChangeResult(outcome=OUTCOME_CANCELLED)
