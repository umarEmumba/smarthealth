"""RescheduleWorkflow: atomic reserve-then-swap (plan 02-08; SCHED-05, BILL-02, D-14).

Started by `POST /api/appointments/{id}/reschedule` as
`reschedule-{id}-{version}` on `scheduling-tq`. Never cancel-then-rebook:

    1. hold_new_slot          hold the new slot for this appointment
                              (compensation: release_new_hold)
    2. swap_slot              ONE local transaction: new slot booked, appointment
                              repointed (slot_id/start/end, version + 1), old slot
                              available, history `rescheduled`
    3. billing_relink         billing history re-link, no refund
    4. schedule_notification  best effort (D-11), `booking_rescheduled`
    5. waitlist advance       (plan 02-09, D-12/D-14) start
                              `WaitlistAdvanceWorkflow` for the OLD slot as an
                              ABANDONed child; not awaited. A failed reschedule
                              starts nothing (the old slot was never freed).

If step 1 or 2 fails (`SlotUnavailable`, `SwapRejected` from a lost state or a
database arbiter, or exhausted infrastructure retries) the new hold is released,
the original booking is untouched, and `reschedule_failed:<reason>` is written to
the appointment history (bumping its version so the next attempt gets a fresh
workflow id); the result is `failed`. Once the swap commits the move is final:
a billing re-link that does not land in its budget is recorded as
`reschedule_incomplete:billing_pending` and the result is
`ChangeResult("rescheduled", "billing_pending")`.

Changing this code once histories exist (D-19): guard any change to the
command sequence with `workflow.patched(...)` and prove it with the replay test
(`tests/workflows/histories/reschedule_v1.json`). Step 5 runs only behind
the `WAITLIST_PATCH_ID` patch marker (plan 02-09), so `reschedule_v1.json`
still replays and `reschedule_waitlist_v2.json` proves the patched path.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import COMPENSATION_ACTIVITY, FORWARD_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_BILLING_RELINK,
        ACT_HOLD_NEW,
        ACT_RELEASE_NEW,
        ACT_SWAP,
        BILLING_PENDING,
        NOTIFY_RESCHEDULED,
        OUTCOME_FAILED,
        OUTCOME_RESCHEDULED,
        RESCHEDULE_WORKFLOW,
        ChangeOutcomeInput,
        ChangeResult,
        NotifyInput,
        RescheduleInput,
        WAITLIST_PATCH_ID,
    )

# Sandbox-safe sibling (temporalio + the pure contracts only): imported normally.
from services.scheduling.workflows.cancel import (
    failure_reason,
    notify,
    record_outcome,
    start_waitlist_advance,
)


@workflow.defn(name=RESCHEDULE_WORKFLOW)
class RescheduleWorkflow:
    @workflow.run
    async def run(self, inp: RescheduleInput) -> ChangeResult:
        hold_registered = False
        try:
            # Registered BEFORE the hold runs: a hold that landed but reported
            # failure is still released (release tolerates "never held").
            hold_registered = True
            await workflow.execute_activity(ACT_HOLD_NEW, inp, **FORWARD_ACTIVITY)
            await workflow.execute_activity(ACT_SWAP, inp, **FORWARD_ACTIVITY)
        except ActivityError as err:
            reason = failure_reason(err)
            workflow.logger.info("reschedule.failed reason=%s", reason)
            if hold_registered:
                try:
                    await workflow.execute_activity(ACT_RELEASE_NEW, inp, **COMPENSATION_ACTIVITY)
                except ActivityError as comp_err:
                    workflow.logger.error(
                        "reschedule.release_failed reason=%s", failure_reason(comp_err)
                    )
            await record_outcome(
                ChangeOutcomeInput(
                    appointment_id=inp.appointment_id,
                    clinic_id=inp.clinic_id,
                    note=f"reschedule_failed:{reason}",
                    audit_action="appointment.reschedule_failed",
                    bump_version=1,
                )
            )
            return ChangeResult(outcome=OUTCOME_FAILED, reason=reason)

        billing_done = True
        try:
            await workflow.execute_activity(ACT_BILLING_RELINK, inp, **COMPENSATION_ACTIVITY)
        except ActivityError as err:
            billing_done = False
            workflow.logger.error("reschedule.billing_incomplete reason=%s", failure_reason(err))
            await record_outcome(
                ChangeOutcomeInput(
                    appointment_id=inp.appointment_id,
                    clinic_id=inp.clinic_id,
                    note=f"reschedule_incomplete:{BILLING_PENDING}",
                    audit_action="billing.relink_incomplete",
                )
            )

        await notify(
            NotifyInput(
                appointment_id=inp.appointment_id,
                clinic_id=inp.clinic_id,
                notification_type=NOTIFY_RESCHEDULED,
            )
        )
        # The swap committed, so the old slot is free: offer it to the waitlist.
        if workflow.patched(WAITLIST_PATCH_ID):
            await start_waitlist_advance(inp.old_slot_id, inp.clinic_id)
        if not billing_done:
            return ChangeResult(outcome=OUTCOME_RESCHEDULED, reason=BILLING_PENDING)
        return ChangeResult(outcome=OUTCOME_RESCHEDULED)
