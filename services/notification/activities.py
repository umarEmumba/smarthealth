"""The `notification.enqueue` activity (plan 03-06; D-02, D-09).

A fired reminder or follow-up timer publishes the same Celery `notifications.send`
task the event consumer uses (`enqueue_send`), so there is one send task, one
delivery log and one dedupe rule. Publishing is a blocking kombu call, so it runs
in a worker thread (`asyncio.to_thread` copies the context, which keeps the
activity span as the parent of the publish span). A broker error propagates: the
activity fails and Temporal retries it; the send task's dedupe key makes a retry
after an unacknowledged publish harmless.

The activity name `notification.enqueue` is stable forever (it lives in histories).
"""

from __future__ import annotations

import asyncio

from celery import Celery
from temporalio import activity

from services.notification.contracts import ACT_ENQUEUE, EnqueueRequest
from services.notification.tasks import enqueue_send


class NotificationActivities:
    def __init__(self, celery_app: Celery) -> None:
        self._app = celery_app

    @activity.defn(name=ACT_ENQUEUE)
    async def enqueue(self, req: EnqueueRequest) -> None:
        await asyncio.to_thread(
            enqueue_send,
            self._app,
            notification_type=req.notification_type,
            dedupe_key=req.dedupe_key,
            patient_id=req.patient_id,
            appointment_id=req.appointment_id,
            visit_id=req.visit_id,
            requested_at=req.requested_at_iso,
        )

    def all(self) -> list:
        return [self.enqueue]
