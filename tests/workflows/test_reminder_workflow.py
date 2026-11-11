"""Reminder and follow-up workflows (plan 03-06; NOTIF-02, NOTIF-03, D-02, D-09).

Time-skipping Temporal environment, default sandboxed worker. The tracer test runs
the REAL `notification.enqueue` activity against a throwaway RabbitMQ vhost with an
in-process Celery worker and the live Mongo; every other test registers a recording
fake activity under the same name, so no broker is needed.

Pitfall 11: the time-skipping server advances virtual time for everyone while a
test awaits a result, so every slot is computed from `env.get_current_time()` and
each test has its own environment.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.temporal_policies import NOTIFICATION_TASK_QUEUE
from services.notification.activities import NotificationActivities
from services.notification.contracts import (
    ACT_ENQUEUE,
    EnqueueRequest,
    FollowUpInput,
    ReminderInput,
    ReminderResult,
    followup_dedupe_key,
    reminder_dedupe_key,
    reminder_workflow_id,
)
from services.notification.workflows.reminder import FollowUpReminderWorkflow, ReminderWorkflow

# Fixtures of the notification suite (throwaway vhost, in-process Celery worker, Mongo).
from tests.notification.conftest import (  # noqa: F401
    _span_exporter,
    app,
    broker,
    delivery,
    make_worker,
    mongo_db,
    wait_for,
)

WORKFLOWS = [ReminderWorkflow, FollowUpReminderWorkflow]


def _ids() -> tuple[str, str]:
    return str(uuid.uuid4()), str(uuid.uuid4())


@pytest.mark.integration
async def test_tracer_reminder_timer_fires_and_celery_send_is_delivered(
    temporal_env: WorkflowEnvironment, app, make_worker, mongo_db
) -> None:
    appointment_id, patient_id = _ids()
    make_worker(app, ("notifications.reminder",))
    slot = await temporal_env.get_current_time() + timedelta(hours=3)
    inp = ReminderInput(appointment_id, patient_id, slot.isoformat(), [120])
    async with Worker(
        temporal_env.client,
        task_queue=NOTIFICATION_TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=NotificationActivities(app).all(),
    ):
        result = await temporal_env.client.execute_workflow(
            ReminderWorkflow.run,
            inp,
            id=reminder_workflow_id(appointment_id),
            task_queue=NOTIFICATION_TASK_QUEUE,
        )
    assert result == ReminderResult("completed", [120])
    key = f"appointment.reminder:{appointment_id}:{int(slot.timestamp())}:120"
    assert key == reminder_dedupe_key(appointment_id, slot.isoformat(), 120)
    row = await asyncio.to_thread(
        wait_for, lambda: (d := delivery(mongo_db, key)) and d["status"] == "sent" and d
    )
    assert row, "the reminder was not delivered through the Celery send task"
    assert row["notification_type"] == "appointment.reminder"
