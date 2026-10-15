"""BookingWorkflow: the booking saga (plan 02-06; SCHED-02, D-05, D-06, D-09, D-11).

The API has already committed the `pending` appointment (and its idempotency
claim) in one local transaction and returned 202 (D-07). This workflow, started
as `booking-{appointment_id}` on `scheduling-tq`, finishes the booking:

    1. validate                patient / provider / department / slot references
    2. reserve_slot            hold the slot          (compensation: release_slot)
    3. billing_precheck        Billing pre-check      (compensation: void_billing)
    4. schedule_notification   best effort (D-09, D-11): logged on failure, never
                               fails or compensates the booking
    5. confirm                 slot held -> booked, appointment pending -> confirmed

Order follows D-09: billing BEFORE notification. This deviates from the SCHED-02
text (which lists notification first) on purpose: the notification is
best-effort, so it fires only after the last fallible step that could still
undo the booking. "Prevent conflicts" is the reserve step's database guards
(`uq_appointments_active_slot`, `ex_appointments_patient_no_overlap`, the
conditional hold).

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
bookings fail on replay.

Sandbox: this module imports only `temporalio`, the stdlib and (passed through)
the pure contracts and policies. It has no top-level side effects.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

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
        BOOKING_WORKFLOW,
        OUTCOME_CONFIRMED,
        OUTCOME_FAILED,
        BookingInput,
        BookingResult,
        MarkFailedInput,
    )


def _failure_reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    if isinstance(cause, TimeoutError):
        return "activity_timeout"
    return "activity_failed"


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

            try:
                await workflow.execute_activity(ACT_NOTIFY, inp, **NOTIFY_ACTIVITY)
            except ActivityError as err:
                # Best effort (D-09, D-11): never fails or compensates the booking.
                workflow.logger.warning(
                    "booking.notification_failed reason=%s", _failure_reason(err)
                )

            await workflow.execute_activity(ACT_CONFIRM, inp, **FORWARD_ACTIVITY)
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
            return BookingResult(outcome=OUTCOME_FAILED, reason=reason)
