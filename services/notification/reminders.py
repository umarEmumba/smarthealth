"""Client helpers that start, move and stop reminder workflows (plan 03-06).

Plan 03-14's notification consumer calls these from events. Workflow ids are
deterministic (`reminder-{appointment_id}`, `followup-{visit_id}`), so a repeated
event cannot create a second timer:

* `start_reminder`      - `USE_EXISTING` + `REJECT_DUPLICATE`: a start while the
                          reminder runs attaches to it, a start after it closed is
                          refused and reported as `False` (never raised)
* `reschedule_reminder` - signal-with-start (`start_signal="reschedule"`) with
                          `ALLOW_DUPLICATE`: signals the running reminder, or starts
                          a fresh run for the new slot when the old one closed; robust
                          to event ordering (research Pattern 7)
* `cancel_reminder`     - signals `cancel`; an unknown or already closed reminder is
                          logged and ignored
* `start_followup`      - like `start_reminder`, id `followup-{visit_id}`

RPC and connection errors other than "not found" propagate: the consumer decides
whether to retry (the Kafka offset is not committed, so the event is redelivered).
Workflow ids are only unique while the namespace retains a closed run (research
Pitfall 3); the send task's dedupe key is the real double-send guard.

This module reads `libs.temporal`, so workflow modules must never import it.
"""

from __future__ import annotations

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from libs.observability.logging import get_logger
from libs.temporal import START_RPC_TIMEOUT
from libs.temporal_policies import NOTIFICATION_TASK_QUEUE
from services.notification.contracts import (
    SIGNAL_CANCEL,
    SIGNAL_RESCHEDULE,
    FollowUpInput,
    ReminderInput,
    followup_workflow_id,
    reminder_workflow_id,
)
from services.notification.workflows.reminder import FollowUpReminderWorkflow, ReminderWorkflow

logger = get_logger()

__all__ = ["cancel_reminder", "reschedule_reminder", "start_followup", "start_reminder"]


async def start_reminder(
    client: Client,
    *,
    appointment_id: str,
    patient_id: str,
    slot_start_iso: str,
    lead_minutes: list[int],
) -> bool:
    """True when this call started the reminder (or attached to the running one),
    False when a reminder for this appointment already ran and closed."""
    try:
        await client.start_workflow(
            ReminderWorkflow.run,
            ReminderInput(appointment_id, patient_id, slot_start_iso, list(lead_minutes)),
            id=reminder_workflow_id(appointment_id),
            task_queue=NOTIFICATION_TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            rpc_timeout=START_RPC_TIMEOUT,
        )
    except WorkflowAlreadyStartedError:
        logger.info("reminder.already_started", appointment_id=appointment_id)
        return False
    return True


async def reschedule_reminder(
    client: Client,
    *,
    appointment_id: str,
    patient_id: str,
    new_slot_iso: str,
    lead_minutes: list[int],
) -> None:
    """Move the reminder to `new_slot_iso`: signal the running one, or start a new
    run for the new slot when the previous run already closed."""
    await client.start_workflow(
        ReminderWorkflow.run,
        ReminderInput(appointment_id, patient_id, new_slot_iso, list(lead_minutes)),
        id=reminder_workflow_id(appointment_id),
        task_queue=NOTIFICATION_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        start_signal=SIGNAL_RESCHEDULE,
        start_signal_args=[new_slot_iso],
        rpc_timeout=START_RPC_TIMEOUT,
    )


async def cancel_reminder(client: Client, appointment_id: str) -> None:
    """Stop the reminder; unknown or already closed ids are ignored."""
    handle = client.get_workflow_handle(reminder_workflow_id(appointment_id))
    try:
        await handle.signal(SIGNAL_CANCEL, rpc_timeout=START_RPC_TIMEOUT)
    except RPCError as err:
        if err.status != RPCStatusCode.NOT_FOUND:
            raise
        logger.info("reminder.cancel_nothing_to_cancel", appointment_id=appointment_id)


async def start_followup(
    client: Client,
    *,
    visit_id: str,
    appointment_id: str,
    patient_id: str,
    completed_at_iso: str,
    delay_minutes: int,
) -> bool:
    """True when this call started the follow-up (or attached to the running one),
    False when one for this visit already ran and closed."""
    try:
        await client.start_workflow(
            FollowUpReminderWorkflow.run,
            FollowUpInput(visit_id, appointment_id, patient_id, completed_at_iso, delay_minutes),
            id=followup_workflow_id(visit_id),
            task_queue=NOTIFICATION_TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            rpc_timeout=START_RPC_TIMEOUT,
        )
    except WorkflowAlreadyStartedError:
        logger.info("followup.already_started", visit_id=visit_id)
        return False
    return True
