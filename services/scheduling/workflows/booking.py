"""BookingWorkflow: the booking saga (plan 02-06; SCHED-02, D-05, D-06, D-09, D-11).

The API has already committed the `pending` appointment (and its idempotency
claim) in one local transaction and returned 202 (D-07). This workflow, started
as `booking-{appointment_id}` on `scheduling-tq`, finishes the booking:

    1. validate                patient / provider / department / slot references
    2. reserve_slot            hold the slot          (compensation: release_slot)
    3. billing_precheck        Billing pre-check      (compensation: void_billing)
    4. confirm                 slot held -> booked, appointment pending -> confirmed
    5. schedule_notification   best effort (D-09, D-11): logged on failure, never
                               fails or compensates the booking

Order (D-09, changed by plan 02.1-07 S7): billing BEFORE confirm BEFORE the
notification. The notification intent is requested only after `confirm`
succeeds, so a booking that fails at confirm (`ConfirmLostHold`) never produces
a "booking confirmed" notification. This deviates from the SCHED-02 text (which
lists the notification earlier) on purpose. "Prevent conflicts" is the reserve
step's database guards (`uq_appointments_active_slot`,
`ex_appointments_patient_no_overlap`, the conditional hold). Histories recorded
before the change ran the notification between billing and confirm; they replay
that order because the move is behind `workflow.patched(NOTIFY_AFTER_CONFIRM_PATCH_ID)`.

Waitlist re-offer (plan 02.1-07 S5): a booking that is the top-level workflow
(not a waitlist advance's child), reached the slot hold and failed for a reason
other than a lost race frees the slot with `release_slot`; nothing would then
offer it to the waitlist, so the failure path starts `WaitlistAdvanceWorkflow`
for the slot (an ABANDONed child, id `waitlist-advance-booking-{appointment_id}`),
behind `workflow.patched(BOOKING_FAILURE_WAITLIST_PATCH_ID)`. A booking started
by a waitlist advance is decided by its parent loop, and a lost race means
someone else owns the slot, so neither re-offers. A failure to start the child
is logged and never changes the booking result.

Saga rules (research Pattern 3):

* each compensation is registered BEFORE its forward step runs, so a step that
  failed half-way is still compensated; compensations tolerate "the forward
  step never happened";
* on a forward failure the registered compensations run in REVERSE order as
  forward transitions (never deletes), a failing compensation is logged and the
  rest still run, and finally `mark_failed` records a machine-readable reason;
* a business failure returns a normal `BookingResult("failed", reason)`, so the
  run closes Completed and `REJECT_DUPLICATE` blocks a re-run.

No run/execution timeout is set when this saga is started (`libs.temporal`):
a run timeout terminates the workflow WITHOUT running compensation, while every
activity here is bounded (`libs.temporal_policies`), so the saga reaches
compensation on its own (research Pattern 8).

Changing this code once histories exist (D-19): any change to the sequence of
commands must be guarded with `workflow.patched("<change-id>")` (later
`workflow.deprecate_patch`) and proven with a replay test, or in-flight
bookings fail on replay. The two patches above are proven by
`tests/workflows/test_replay_booking_patches.py` (the `_v1` histories replay,
the `_p21` histories carry the markers).

Sandbox: this module imports only `temporalio`, the stdlib and (passed through)
the pure contracts and policies. It has no top-level side effects.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    TimeoutError,
    WorkflowAlreadyStartedError,
)

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import COMPENSATION_ACTIVITY, FORWARD_ACTIVITY, NOTIFY_ACTIVITY
    from services.scheduling.domain.contracts import (
        ACT_BILLING,
        ACT_CONFIRM,
        ACT_MARK_FAILED,
        ACT_NOTIFY,
        ACT_RELEASE,
        ACT_RESERVE,
        ACT_VALIDATE,
        ACT_VOID,
        BOOKING_FAILURE_WAITLIST_PATCH_ID,
        BOOKING_WORKFLOW,
        LOST_RACE_REASONS,
        NOTIFY_AFTER_CONFIRM_PATCH_ID,
        OUTCOME_CONFIRMED,
        OUTCOME_FAILED,
        BookingInput,
        BookingResult,
        MarkFailedInput,
    )

# Sandbox-safe sibling (temporalio + the pure contracts only): imported normally,
# exactly as reschedule.py does.
from services.scheduling.workflows.cancel import start_waitlist_advance


def _failure_reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    if isinstance(cause, TimeoutError):
        return "activity_timeout"
    return "activity_failed"


async def _notify_best_effort(inp: BookingInput) -> None:
    """Best effort (D-09, D-11): never fails or compensates the booking. One
    body for both positions (before confirm in pre-patch histories, after
    confirm otherwise)."""
    try:
        await workflow.execute_activity(ACT_NOTIFY, inp, **NOTIFY_ACTIVITY)
    except ActivityError as err:
        workflow.logger.warning("booking.notification_failed reason=%s", _failure_reason(err))


@workflow.defn(name=BOOKING_WORKFLOW)
class BookingWorkflow:
    @workflow.run
    async def run(self, inp: BookingInput) -> BookingResult:
        compensations: list[str] = []
        try:
            await workflow.execute_activity(ACT_VALIDATE, inp, **FORWARD_ACTIVITY)

            compensations.append(ACT_RELEASE)
            await workflow.execute_activity(ACT_RESERVE, inp, **FORWARD_ACTIVITY)

            compensations.append(ACT_VOID)
            await workflow.execute_activity(ACT_BILLING, inp, **FORWARD_ACTIVITY)

            # S7 (patched): evaluated once. A history recorded before this
            # change has no marker, so it replays the old order (notify, then
            # confirm); a new run confirms first and notifies only afterwards.
            notify_after = workflow.patched(NOTIFY_AFTER_CONFIRM_PATCH_ID)
            if not notify_after:
                await _notify_best_effort(inp)

            await workflow.execute_activity(ACT_CONFIRM, inp, **FORWARD_ACTIVITY)

            if notify_after:
                await _notify_best_effort(inp)
            return BookingResult(outcome=OUTCOME_CONFIRMED)
        except ActivityError as err:
            reason = _failure_reason(err)
            workflow.logger.info("booking.compensating reason=%s", reason)
            for name in reversed(compensations):
                try:
                    await workflow.execute_activity(name, inp, **COMPENSATION_ACTIVITY)
                except ActivityError as comp_err:
                    workflow.logger.error(
                        "booking.compensation_failed step=%s reason=%s",
                        name,
                        _failure_reason(comp_err),
                    )
            await workflow.execute_activity(
                ACT_MARK_FAILED,
                MarkFailedInput(appointment_id=inp.appointment_id, reason=reason),
                **COMPENSATION_ACTIVITY,
            )
            # S5: offer the slot this booking freed back to the waitlist. Only
            # for a top-level booking whose hold was attempted (ACT_RELEASE is
            # registered) and that did not lose the race for the slot; the
            # cheap deterministic conditions come first so the patch marker is
            # recorded only when the re-offer is actually in play.
            if (
                ACT_RELEASE in compensations
                and reason not in LOST_RACE_REASONS
                and workflow.info().parent is None
                and workflow.patched(BOOKING_FAILURE_WAITLIST_PATCH_ID)
            ):
                try:
                    await start_waitlist_advance(inp.slot_id, inp.clinic_id)
                except (WorkflowAlreadyStartedError, ChildWorkflowError) as exc:
                    # Already started (a duplicate) or the start was refused:
                    # the booking result is unchanged either way.
                    workflow.logger.warning(
                        "booking.waitlist_offer_failed error=%s", type(exc).__name__
                    )
            return BookingResult(outcome=OUTCOME_FAILED, reason=reason)
