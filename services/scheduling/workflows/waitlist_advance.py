"""WaitlistAdvanceWorkflow: give a freed slot to the waitlist (plan 02-09;
SCHED-07, D-12, research Pattern 7).

Started by CancelWorkflow (the cancelled appointment's slot) and
RescheduleWorkflow (the old slot of a successful swap) as a child with
`ParentClosePolicy.ABANDON`, id `waitlist-advance-{source_workflow_id}`, on
`scheduling-tq`. Loop, at most `max_attempts` times (D-12: no unbounded retry):

    1. claim_next      one transaction: claim the oldest waiting entry for the
                       slot's provider whose window contains the slot
                       (FOR UPDATE SKIP LOCKED) and create its `pending`
                       appointment; the appointment id comes from
                       `workflow.uuid4()`, so it is deterministic on replay and
                       a retried claim finds its own match
    2. BookingWorkflow the NORMAL booking saga as a child workflow
                       (`booking-{appointment_id}`): validation, hold, billing,
                       notification, confirm and compensation are all reused
    3. resolve_entry   confirmed -> entry `booked` (done); a lost slot race
                       (`LOST_RACE_REASONS`) -> entry back to `waiting` and stop,
                       the slot is no longer free; any other failure -> entry
                       `failed` with the reason, try the next entry

A claim that finds nothing (`no_match`), a slot that is no longer free
(`slot_gone`) or an appointment insert that lost the slot at the database
arbiter (`slot_taken`, entry left `waiting`) ends the advance with that
outcome. A claim refused for the entry's patient (`entry_failed`, e.g.
`PatientOverlap`) counts as an attempt and moves on. After `max_attempts`
the result is `exhausted`.

The child saga runs with `ParentClosePolicy.ABANDON`: terminating or
cancelling this workflow must never kill a booking saga before it has run its
own compensation (research Pattern 3/8). No run timeout is set for the same
reason; every activity here is bounded.

Documented limits (plan 02-09 discretion):
* only a cancel or a reschedule starts an advance; a hold released by saga
  compensation or by the reaper was never a booked slot, and joining a waitlist
  while a matching slot is already open does not auto-book (the patient can
  book it directly);
* if the claim activity itself exhausts its infrastructure retries the advance
  ends `claim_failed` (logged); a claim that committed just before that is
  left `matched` with its `pending` appointment for an operator.

Sandbox: temporalio, the stdlib and (passed through) the pure contracts and
policies only.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import (
        COMPENSATION_ACTIVITY,
        FORWARD_ACTIVITY,
        SCHEDULING_TASK_QUEUE,
    )
    from services.scheduling.domain.contracts import (
        ACT_WAITLIST_CLAIM,
        ACT_WAITLIST_RESOLVE,
        ADVANCE_BOOKED,
        ADVANCE_CLAIM_FAILED,
        ADVANCE_EXHAUSTED,
        ADVANCE_SLOT_GONE,
        BOOKING_WORKFLOW,
        CLAIM_CLAIMED,
        CLAIM_ENTRY_FAILED,
        LOST_RACE_REASONS,
        OUTCOME_CONFIRMED,
        OUTCOME_FAILED,
        WAITLIST_ADVANCE_WORKFLOW,
        BookingInput,
        BookingResult,
        ClaimResult,
        WaitlistAdvanceInput,
        WaitlistAdvanceResult,
        WaitlistClaimInput,
        WaitlistResolveInput,
        booking_workflow_id,
    )

# Reason recorded when the child saga itself errored instead of returning a result.
CHILD_ERROR = "booking_workflow_error"


def _reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    return "activity_failed"


@workflow.defn(name=WAITLIST_ADVANCE_WORKFLOW)
class WaitlistAdvanceWorkflow:
    @workflow.run
    async def run(self, inp: WaitlistAdvanceInput) -> WaitlistAdvanceResult:
        attempts = 0
        for _ in range(inp.max_attempts):
            appointment_id = str(workflow.uuid4())
            try:
                claim: ClaimResult = await workflow.execute_activity(
                    ACT_WAITLIST_CLAIM,
                    WaitlistClaimInput(
                        slot_id=inp.slot_id, clinic_id=inp.clinic_id, appointment_id=appointment_id
                    ),
                    result_type=ClaimResult,
                    **FORWARD_ACTIVITY,
                )
            except ActivityError as err:
                workflow.logger.error("waitlist.claim_failed reason=%s", _reason(err))
                return WaitlistAdvanceResult(outcome=ADVANCE_CLAIM_FAILED, attempts=attempts)

            if claim.outcome == CLAIM_ENTRY_FAILED:
                attempts += 1
                workflow.logger.info("waitlist.entry_failed reason=%s", claim.reason)
                continue
            if claim.outcome != CLAIM_CLAIMED:
                return WaitlistAdvanceResult(outcome=claim.outcome, attempts=attempts)
            attempts += 1

            try:
                booking: BookingResult = await workflow.execute_child_workflow(
                    BOOKING_WORKFLOW,
                    BookingInput(
                        appointment_id=appointment_id,
                        clinic_id=inp.clinic_id,
                        patient_id=claim.patient_id or "",
                        provider_id=claim.provider_id or "",
                        slot_id=inp.slot_id,
                    ),
                    id=booking_workflow_id(appointment_id),
                    task_queue=SCHEDULING_TASK_QUEUE,
                    parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                    result_type=BookingResult,
                )
            except ChildWorkflowError:
                workflow.logger.error("waitlist.booking_child_error")
                booking = BookingResult(outcome=OUTCOME_FAILED, reason=CHILD_ERROR)

            try:
                await workflow.execute_activity(
                    ACT_WAITLIST_RESOLVE,
                    WaitlistResolveInput(
                        entry_id=claim.entry_id or "",
                        appointment_id=appointment_id,
                        booking_outcome=booking.outcome,
                        reason=booking.reason,
                    ),
                    **COMPENSATION_ACTIVITY,
                )
            except ActivityError as err:
                # Long budget already spent: the entry stays `matched` for an
                # operator; the booking's own outcome still decides below.
                workflow.logger.error("waitlist.resolve_failed reason=%s", _reason(err))
            if booking.outcome == OUTCOME_CONFIRMED:
                return WaitlistAdvanceResult(
                    outcome=ADVANCE_BOOKED, booked_entry_id=claim.entry_id, attempts=attempts
                )
            if booking.reason in LOST_RACE_REASONS:
                return WaitlistAdvanceResult(outcome=ADVANCE_SLOT_GONE, attempts=attempts)
            workflow.logger.info("waitlist.booking_failed reason=%s", booking.reason)

        return WaitlistAdvanceResult(outcome=ADVANCE_EXHAUSTED, attempts=attempts)
