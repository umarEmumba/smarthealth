"""Tracer: one send travels publisher span -> RabbitMQ -> threads worker -> fake provider -> Mongo (plan 03-02)."""

from __future__ import annotations

import time
import uuid
from typing import Any
from unittest import mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import ValidationError

from services.notification.celery_app import create_celery_app
from services.notification.config import NotificationSettings
from services.notification.tasks import QUEUE_FOR_TYPE, SendRequest, enqueue_send
from tests.notification.conftest import delivery, new_request, provider_calls, wait_for

pytestmark = pytest.mark.integration


def _queue_depth(app: Any, name: str) -> int:
    with app.connection_for_read() as connection:
        return connection.default_channel.queue_declare(name, passive=True).message_count


def test_send_is_delivered_once_and_recorded_in_one_trace(
    app: Any, make_worker: Any, mongo_db: Any, in_memory_spans: InMemorySpanExporter
) -> None:
    make_worker(app)
    request = new_request()
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("test.publish") as publish:
        enqueue_send(app, **request)
    publisher_trace = publish.get_span_context().trace_id

    doc = wait_for(lambda: (d := delivery(mongo_db, request["dedupe_key"])) and d["status"] == "sent" and d)
    assert doc, "delivery was never recorded as sent"
    assert doc["attempts"] == 1
    assert doc["patient_id"] == request["patient_id"]
    assert provider_calls(mongo_db, request["dedupe_key"]) == 1

    def task_span() -> Any:
        return [s for s in in_memory_spans.get_finished_spans() if s.name == "run/notifications.send"]

    spans = wait_for(task_span)
    assert spans, [s.name for s in in_memory_spans.get_finished_spans()]
    run = spans[-1]
    assert run.context.trace_id == publisher_trace
    by_id = {s.context.span_id: s for s in in_memory_spans.get_finished_spans()}
    assert run.parent is not None
    parent = by_id[run.parent.span_id]
    assert parent.name == "apply_async/notifications.send"
    assert parent.context.trace_id == publisher_trace
    assert parent.parent is not None
    assert parent.parent.span_id == publish.get_span_context().span_id


def test_worker_consumes_only_its_listed_queues(app: Any, make_worker: Any, mongo_db: Any) -> None:
    make_worker(app, queues=("notifications.booking",))
    reminder = new_request("appointment.reminder")
    booked = new_request("appointment.booked")
    enqueue_send(app, **reminder)
    enqueue_send(app, **booked)

    assert wait_for(lambda: (d := delivery(mongo_db, booked["dedupe_key"])) and d["status"] == "sent")
    time.sleep(1.5)
    assert delivery(mongo_db, reminder["dedupe_key"]) is None
    assert _queue_depth(app, "notifications.reminder") == 1
    assert _queue_depth(app, "notifications.booking") == 0


def test_queue_for_each_type() -> None:
    assert QUEUE_FOR_TYPE == {
        "appointment.booked": "notifications.booking",
        "appointment.cancelled": "notifications.booking",
        "appointment.rescheduled": "notifications.booking",
        "appointment.reminder": "notifications.reminder",
        "visit.followup": "notifications.followup",
    }


def test_task_kwargs_are_plain_strings_and_unknown_type_is_refused() -> None:
    app = create_celery_app("amqp://u:p@localhost:5672/none")
    request = new_request("visit.followup", visit_id=str(uuid.uuid4()))
    with mock.patch.object(app, "send_task") as send_task:
        enqueue_send(app, **request)
    kwargs = send_task.call_args.kwargs["kwargs"]
    assert send_task.call_args.kwargs["queue"] == "notifications.followup"
    assert all(type(v) is str for v in kwargs.values()), kwargs

    with pytest.raises(ValidationError):
        SendRequest(**new_request("appointment.exploded"))
    with pytest.raises(ValidationError):
        SendRequest(**new_request(patient_id="not-a-uuid"))
    with pytest.raises(ValidationError):
        SendRequest(**new_request(dedupe_key="x" * 201))
    with pytest.raises(ValidationError):
        SendRequest(**new_request(unexpected="field"))
    with mock.patch.object(app, "send_task") as send_task, pytest.raises(ValidationError):
        enqueue_send(app, **new_request("appointment.exploded"))
    send_task.assert_not_called()


def test_celery_app_is_configured_for_rabbitmq_43() -> None:
    app = create_celery_app("amqp://u:p@localhost:5672/none")
    conf = app.conf
    assert conf.worker_enable_remote_control is False
    assert conf.worker_send_task_events is False
    assert conf.task_send_sent_event is False
    assert conf.accept_content == ["json"]
    assert conf.task_serializer == "json"
    assert conf.task_acks_late is True
    assert conf.task_reject_on_worker_lost is True
    assert conf.worker_prefetch_multiplier == 1
    queues = {q.name: q for q in conf.task_queues}
    assert set(queues) == {
        "notifications.booking",
        "notifications.reminder",
        "notifications.followup",
        "notifications.dlq",
    }
    for name in ("notifications.booking", "notifications.reminder", "notifications.followup"):
        assert queues[name].queue_arguments["x-dead-letter-exchange"] == "smarthealth.dlx"
        assert queues[name].queue_arguments["x-dead-letter-routing-key"] == "notifications.dlq"
    assert queues["notifications.dlq"].exchange.name == "smarthealth.dlx"


def test_broker_url_is_lazy_and_never_shows_the_password(tmp_path: Any) -> None:
    secret = tmp_path / "broker.secret"
    secret.write_text("s3cr3t/pass word\n")
    s = NotificationSettings(NOTIFICATION_BROKER_PASSWORD_FILE=str(secret))
    assert "s3cr3t" not in repr(s)
    assert "s3cr3t" not in str(s.model_dump())
    url = s.broker_url
    assert url == "amqp://smarthealth_notification:s3cr3t%2Fpass%20word@localhost:5672/smarthealth"

    missing = NotificationSettings(NOTIFICATION_BROKER_PASSWORD_FILE=str(tmp_path / "nope"))
    with pytest.raises(RuntimeError, match="NOTIFICATION_BROKER_PASSWORD_FILE") as err:
        _ = missing.broker_url
    assert str(tmp_path) not in str(err.value)
