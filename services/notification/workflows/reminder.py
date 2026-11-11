"""Reminder and follow-up workflows (plan 03-06; NOTIF-02, NOTIF-03, D-02, D-09).

`ReminderWorkflow`, id `reminder-{appointment_id}` on `notification-tq`, walks the
configured leads (largest first). For each lead it waits, on a durable timer, until
`slot - lead`; when the timer fires it executes the `notification.enqueue` activity,
which publishes the SAME Celery `notifications.send` task the event-driven
notifications use (delivery stays on Celery workers; Temporal only owns the
schedule). Signals:

* `cancel`                 - stops the reminder (`cancelled`), nothing more is sent
* `reschedule(new_slot)`   - replaces the slot and restarts the walk for it; leads
                             sent for the old slot are not repeated for the new one
                             (the dedupe key includes the slot epoch, so the new slot
                             gets its own reminders)

A lead whose time has already passed is skipped. If every lead was skipped the
result is `skipped_past`, otherwise `completed`. The leads are inputs, never read
from settings inside the workflow.

`FollowUpReminderWorkflow`, id `followup-{visit_id}`, sleeps until
`completed_at + delay`, then enqueues one `visit.followup` send.

A reminder whose enqueue activity ultimately fails (the broker unreachable past the
retry budget) is logged and the walk goes on: one lost lead must not cancel the
others. The follow-up has only one send, so its failure fails the workflow, which
stays visible in the Temporal UI.

Changing this code once histories exist: any change to either workflow's command
sequence needs a `workflow.patched` id and a new recorded history
(`tests/workflows/test_replay_reminder.py`), exactly like the booking patches.
Activity, signal and workflow names are stable forever.

Sandbox: imports only `temporalio`, the stdlib and (passed through) the pure
contracts and policies. No top-level side effects.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from libs.temporal_policies import FORWARD_ACTIVITY
    from services.notification.contracts import (
        ACT_ENQUEUE,
        FOLLOWUP_WORKFLOW,
        REMINDER_WORKFLOW,
        SIGNAL_CANCEL,
        SIGNAL_RESCHEDULE,
        STATUS_CANCELLED,
        STATUS_COMPLETED,
        STATUS_SKIPPED_PAST,
        TYPE_FOLLOWUP,
        TYPE_REMINDER,
        EnqueueRequest,
        FollowUpInput,
        ReminderInput,
        ReminderResult,
        followup_dedupe_key,
        parse_instant,
        reminder_dedupe_key,
    )


@workflow.defn(name=REMINDER_WORKFLOW)
class ReminderWorkflow:
    def __init__(self) -> None:
        self._cancelled = False
        self._new_slot_iso: str | None = None

    @workflow.signal(name=SIGNAL_CANCEL)
    def cancel(self) -> None:
        self._cancelled = True

    @workflow.signal(name=SIGNAL_RESCHEDULE)
    def reschedule(self, new_slot_iso: str) -> None:
        self._new_slot_iso = new_slot_iso

    def _interrupted(self) -> bool:
        return self._cancelled or self._new_slot_iso is not None

    @workflow.run
    async def run(self, inp: ReminderInput) -> ReminderResult:
        leads = sorted(set(inp.lead_minutes), reverse=True)
        slot_iso = inp.slot_start_iso
        sent: list[int] = []
        while True:  # one pass per slot; a reschedule signal starts the next pass
            slot = parse_instant(slot_iso)
            moved = False
            for lead in leads:
                if self._cancelled:
                    return ReminderResult(STATUS_CANCELLED, sent)
                if self._new_slot_iso is not None:
                    moved = True
                    break
                fire_at = slot - timedelta(minutes=lead)
                remaining = fire_at - workflow.now()
                if remaining <= timedelta(0):
                    continue  # this lead is already in the past
                try:
                    await workflow.wait_condition(self._interrupted, timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # the timer fired: send
                if self._cancelled:
                    return ReminderResult(STATUS_CANCELLED, sent)
                if self._new_slot_iso is not None:
                    moved = True
                    break
                request = EnqueueRequest(
                    notification_type=TYPE_REMINDER,
                    dedupe_key=reminder_dedupe_key(inp.appointment_id, slot_iso, lead),
                    patient_id=inp.patient_id,
                    appointment_id=inp.appointment_id,
                    visit_id=None,
                    requested_at_iso=workflow.now().isoformat(),
                )
                try:
                    await workflow.execute_activity(ACT_ENQUEUE, request, **FORWARD_ACTIVITY)
                except ActivityError as err:
                    workflow.logger.warning(
                        "reminder.enqueue_failed lead=%s cause=%s", lead, type(err.cause).__name__
                    )
                    continue
                sent.append(lead)
            if moved:
                slot_iso, self._new_slot_iso = self._new_slot_iso, None
                sent = []  # leads of the old slot do not count for the new one
                continue
            break
        if self._cancelled:
            return ReminderResult(STATUS_CANCELLED, sent)
        return ReminderResult(STATUS_COMPLETED if sent else STATUS_SKIPPED_PAST, sent)


@workflow.defn(name=FOLLOWUP_WORKFLOW)
class FollowUpReminderWorkflow:
    @workflow.run
    async def run(self, inp: FollowUpInput) -> ReminderResult:
        fire_at = parse_instant(inp.completed_at_iso) + timedelta(minutes=inp.delay_minutes)
        remaining = fire_at - workflow.now()
        if remaining > timedelta(0):
            await workflow.sleep(remaining)
        request = EnqueueRequest(
            notification_type=TYPE_FOLLOWUP,
            dedupe_key=followup_dedupe_key(inp.visit_id),
            patient_id=inp.patient_id,
            appointment_id=inp.appointment_id,
            visit_id=inp.visit_id,
            requested_at_iso=workflow.now().isoformat(),
        )
        await workflow.execute_activity(ACT_ENQUEUE, request, **FORWARD_ACTIVITY)
        return ReminderResult(STATUS_COMPLETED, [inp.delay_minutes])
