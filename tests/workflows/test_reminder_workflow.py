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
import os
import signal
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from pydantic import ValidationError
from temporalio import activity
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
    followup_workflow_id,
    reminder_dedupe_key,
    reminder_workflow_id,
)
from services.notification.config import NotificationSettings
from services.notification.reminders import (
    cancel_reminder,
    reschedule_reminder,
    start_followup,
    start_reminder,
)
from services.notification import temporal_worker
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


# --- recording fake: the same activity name, no broker -----------------------------


class Recorder:
    """A stand-in for `notification.enqueue` that records the requests it receives."""

    def __init__(self) -> None:
        self.calls: list[EnqueueRequest] = []

    @activity.defn(name=ACT_ENQUEUE)
    async def enqueue(self, req: EnqueueRequest) -> None:
        self.calls.append(req)


@asynccontextmanager
async def running(env: WorkflowEnvironment) -> AsyncIterator[Recorder]:
    recorder = Recorder()
    async with Worker(
        env.client,
        task_queue=NOTIFICATION_TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=[recorder.enqueue],
    ):
        yield recorder


async def _result(env: WorkflowEnvironment, workflow_id: str) -> ReminderResult:
    """The latest run's result, with virtual time unlocked while waiting. Handles
    from `get_workflow_handle` (the helpers discard the start handle) do not unlock
    time skipping themselves."""
    async with env.time_skipping_unlocked():
        return await env.client.get_workflow_handle(workflow_id, result_type=ReminderResult).result()


async def _settle() -> None:
    """Real time, not virtual: lets the workflow reach its timer before a signal."""
    await asyncio.sleep(0.5)


# --- ReminderWorkflow ---------------------------------------------------------------


async def test_two_leads_fire_largest_first_with_distinct_keys(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(days=2)
    async with running(temporal_env) as recorder:
        started = await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[120, 1440],  # given out of order on purpose
        )
        assert started is True
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("completed", [1440, 120])
    assert [c.dedupe_key for c in recorder.calls] == [
        reminder_dedupe_key(appointment_id, slot.isoformat(), 1440),
        reminder_dedupe_key(appointment_id, slot.isoformat(), 120),
    ]
    assert {c.notification_type for c in recorder.calls} == {"appointment.reminder"}
    assert all(c.patient_id == patient_id and c.appointment_id == appointment_id for c in recorder.calls)


async def test_cancel_before_the_first_lead_fires_nothing(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(days=2)
    async with running(temporal_env) as recorder:
        await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[1440, 120],
        )
        await _settle()
        await cancel_reminder(temporal_env.client, appointment_id)
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
        assert result == ReminderResult("cancelled", [])
        assert recorder.calls == []
        # A second cancel (the run is closed) and a cancel of an unknown id are harmless.
        await cancel_reminder(temporal_env.client, appointment_id)
        await cancel_reminder(temporal_env.client, str(uuid.uuid4()))


async def test_reschedule_fires_for_the_new_slot_only(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = _ids()
    now = await temporal_env.get_current_time()
    old_slot, new_slot = now + timedelta(hours=3), now + timedelta(hours=8)
    async with running(temporal_env) as recorder:
        await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=old_slot.isoformat(),
            lead_minutes=[120],
        )
        await _settle()
        await reschedule_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            new_slot_iso=new_slot.isoformat(),
            lead_minutes=[120],
        )
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("completed", [120])
    assert [c.dedupe_key for c in recorder.calls] == [
        reminder_dedupe_key(appointment_id, new_slot.isoformat(), 120)
    ]
    assert str(int(old_slot.timestamp())) not in recorder.calls[0].dedupe_key


async def test_a_lead_already_in_the_past_is_skipped(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(minutes=30)
    async with running(temporal_env) as recorder:
        await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[120],
        )
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("skipped_past", [])
    assert recorder.calls == []


async def test_only_the_leads_still_ahead_fire(temporal_env: WorkflowEnvironment) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(hours=5)
    async with running(temporal_env) as recorder:
        await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[1440, 120],  # 24 h is past, 2 h is ahead
        )
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("completed", [120])
    assert len(recorder.calls) == 1


async def test_duplicate_start_attaches_to_the_running_run_then_is_refused_after_close(
    temporal_env: WorkflowEnvironment,
) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(days=2)
    kwargs = {
        "appointment_id": appointment_id,
        "patient_id": patient_id,
        "slot_start_iso": slot.isoformat(),
        "lead_minutes": [120],
    }
    async with running(temporal_env):
        client = temporal_env.client
        assert await start_reminder(client, **kwargs) is True
        handle = client.get_workflow_handle(reminder_workflow_id(appointment_id))
        first_run = (await handle.describe()).run_id
        assert await start_reminder(client, **kwargs) is True  # attaches
        assert (await handle.describe()).run_id == first_run
        await cancel_reminder(client, appointment_id)
        await _result(temporal_env, reminder_workflow_id(appointment_id))
        # The run closed: a second start is refused and reported, never raised.
        assert await start_reminder(client, **kwargs) is False


async def test_reschedule_after_the_reminder_closed_starts_a_new_run(
    temporal_env: WorkflowEnvironment,
) -> None:
    appointment_id, patient_id = _ids()
    client = temporal_env.client
    slot = await temporal_env.get_current_time() + timedelta(hours=3)
    async with running(temporal_env) as recorder:
        await start_reminder(
            client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[120],
        )
        handle = client.get_workflow_handle(reminder_workflow_id(appointment_id))
        assert await _result(temporal_env, reminder_workflow_id(appointment_id)) == ReminderResult(
            "completed", [120]
        )
        first_run = (await handle.describe()).run_id

        new_slot = await temporal_env.get_current_time() + timedelta(hours=6)
        await reschedule_reminder(
            client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            new_slot_iso=new_slot.isoformat(),
            lead_minutes=[120],
        )
        assert await _result(temporal_env, reminder_workflow_id(appointment_id)) == ReminderResult(
            "completed", [120]
        )
        assert (await handle.describe()).run_id != first_run
    assert [c.dedupe_key for c in recorder.calls] == [
        reminder_dedupe_key(appointment_id, slot.isoformat(), 120),
        reminder_dedupe_key(appointment_id, new_slot.isoformat(), 120),
    ]


# --- FollowUpReminderWorkflow ---------------------------------------------------------


async def test_followup_fires_once_after_the_delay_and_a_second_start_is_one_run(
    temporal_env: WorkflowEnvironment,
) -> None:
    visit_id, appointment_id = _ids()
    patient_id = str(uuid.uuid4())
    completed_at = await temporal_env.get_current_time()
    kwargs = {
        "visit_id": visit_id,
        "appointment_id": appointment_id,
        "patient_id": patient_id,
        "completed_at_iso": completed_at.isoformat(),
        "delay_minutes": 4320,
    }
    client = temporal_env.client
    async with running(temporal_env) as recorder:
        assert await start_followup(client, **kwargs) is True
        handle = client.get_workflow_handle(followup_workflow_id(visit_id))
        first_run = (await handle.describe()).run_id
        assert await start_followup(client, **kwargs) is True  # attaches while it sleeps
        assert (await handle.describe()).run_id == first_run
        assert await _result(temporal_env, followup_workflow_id(visit_id)) == ReminderResult(
            "completed", [4320]
        )
        assert await start_followup(client, **kwargs) is False  # closed: refused
    assert len(recorder.calls) == 1
    call = recorder.calls[0]
    assert call.notification_type == "visit.followup"
    assert call.dedupe_key == followup_dedupe_key(visit_id) == f"visit.followup:{visit_id}"
    assert call.visit_id == visit_id and call.appointment_id == appointment_id


# --- settings -------------------------------------------------------------------------


def test_reminder_leads_are_parsed_largest_first_and_must_be_positive() -> None:
    default = NotificationSettings()
    assert default.reminder_leads == [1440, 120]
    assert default.followup_delay_minutes == 4320
    parsed = NotificationSettings(NOTIFICATION_REMINDER_LEADS_MINUTES="30, 120,30,1440")
    assert parsed.reminder_leads == [1440, 120, 30]
    for bad in ("", "0,120", "-5", "abc", "120,,x"):
        with pytest.raises(ValidationError):
            NotificationSettings(NOTIFICATION_REMINDER_LEADS_MINUTES=bad)
    with pytest.raises(ValidationError):
        NotificationSettings(NOTIFICATION_FOLLOWUP_DELAY_MINUTES=0)


# --- the worker entrypoint ----------------------------------------------------------------


@pytest.mark.integration
async def test_build_worker_runs_a_reminder_and_publishes_one_send(
    temporal_env: WorkflowEnvironment, app
) -> None:
    appointment_id, patient_id = _ids()
    slot = await temporal_env.get_current_time() + timedelta(hours=3)
    async with temporal_worker.build_worker(temporal_env.client, app):
        started = await start_reminder(
            temporal_env.client,
            appointment_id=appointment_id,
            patient_id=patient_id,
            slot_start_iso=slot.isoformat(),
            lead_minutes=[120],
        )
        assert started is True
        result = await _result(temporal_env, reminder_workflow_id(appointment_id))
    assert result == ReminderResult("completed", [120])
    # No Celery worker runs here, so the one published send waits in its queue.
    with app.connection_for_read() as connection:
        depth = connection.default_channel.queue_declare("notifications.reminder", passive=True).message_count
    assert depth == 1


async def test_main_instruments_publishing_touches_the_heartbeat_and_stops_on_sigterm(
    temporal_env: WorkflowEnvironment, tmp_path, monkeypatch
) -> None:
    heartbeat = tmp_path / "notification-temporal-worker.alive"
    instrumented: list[bool] = []

    async def _connect() -> object:
        return temporal_env.client

    monkeypatch.setattr(temporal_worker.settings, "temporal_heartbeat_path", str(heartbeat))
    monkeypatch.setattr(temporal_worker, "_connect_with_retry", _connect)
    monkeypatch.setattr(temporal_worker, "instrument_publisher", lambda: instrumented.append(True))
    monkeypatch.setattr(temporal_worker, "configure_tracer_provider", lambda name: None)
    # The SIGTERM handler is installed by main() before it connects, so this signal
    # is handled by main's stop event and never reaches the default disposition.
    asyncio.get_running_loop().call_later(1.5, os.kill, os.getpid(), signal.SIGTERM)
    assert await asyncio.wait_for(temporal_worker.main(), timeout=30) == 0
    assert instrumented == [True]
    assert heartbeat.exists()
