"""Dead-letter path through smarthealth.dlx into notifications.dlq (plan 03-02)."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any
from unittest import mock

import pytest
from prometheus_client import REGISTRY

from services.notification.config import settings
from services.notification.tasks import enqueue_send
from tests.notification.broker import Broker, rabbitmqctl
from tests.notification.conftest import delivery, new_request, provider_calls, wait_for

pytestmark = pytest.mark.integration


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _dlq_messages(app: Any, want: int = 1, timeout: float = 30.0) -> list[Any]:
    """Read (and ack) up to `want` messages from notifications.dlq."""
    found: list[Any] = []
    deadline = time.monotonic() + timeout
    with app.connection_for_read() as connection:
        channel = connection.default_channel
        while len(found) < want and time.monotonic() < deadline:
            message = channel.basic_get("notifications.dlq", no_ack=True)
            if message is None:
                time.sleep(0.2)
                continue
            found.append(message)
    return found


def _kwargs_of(message: Any) -> dict[str, Any]:
    return json.loads(message.body)[1]


def test_exhausted_retries_are_dead_lettered_with_their_original_kwargs(
    app: Any, make_worker: Any, mongo_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "retry_backoff_base_seconds", 0.05)
    monkeypatch.setattr(settings, "max_retries", 2)
    request = new_request(dedupe_key=f"FAIL-PERMANENT-{uuid.uuid4()}")
    key = request["dedupe_key"]
    before = _sample(
        "notifications_dead_lettered_total",
        notification_type="appointment.booked",
        error_type="TransientDeliveryError",
    )
    retried_before = _sample("notifications_retried_total", notification_type="appointment.booked")
    make_worker(app)
    enqueue_send(app, **request)

    messages = _dlq_messages(app)
    assert len(messages) == 1, "the exhausted send never reached notifications.dlq"
    assert _kwargs_of(messages[0]) == request
    doc = wait_for(lambda: (d := delivery(mongo_db, key)) and d["status"] == "failed" and d)
    assert doc and doc["attempts"] == 3
    assert doc["last_error_type"] == "TransientDeliveryError"
    assert provider_calls(mongo_db, key) == 0
    assert (
        _sample(
            "notifications_dead_lettered_total",
            notification_type="appointment.booked",
            error_type="TransientDeliveryError",
        )
        == before + 1
    )
    assert _sample("notifications_retried_total", notification_type="appointment.booked") == retried_before + 2


def test_permanent_provider_error_is_dead_lettered_without_retry(
    app: Any, make_worker: Any, mongo_db: Any
) -> None:
    request = new_request(dedupe_key=f"FAIL-INVALID-{uuid.uuid4()}")
    key = request["dedupe_key"]
    retried_before = _sample("notifications_retried_total", notification_type="appointment.booked")
    before = _sample(
        "notifications_dead_lettered_total",
        notification_type="appointment.booked",
        error_type="PermanentDeliveryError",
    )
    make_worker(app)
    enqueue_send(app, **request)

    messages = _dlq_messages(app)
    assert len(messages) == 1
    assert _kwargs_of(messages[0]) == request
    doc = delivery(mongo_db, key)
    assert doc["status"] == "failed"
    assert doc["attempts"] == 1
    assert provider_calls(mongo_db, key) == 0
    assert _sample("notifications_retried_total", notification_type="appointment.booked") == retried_before
    assert (
        _sample(
            "notifications_dead_lettered_total",
            notification_type="appointment.booked",
            error_type="PermanentDeliveryError",
        )
        == before + 1
    )


def test_message_with_invalid_kwargs_is_dead_lettered_on_the_first_attempt(
    app: Any, make_worker: Any, mongo_db: Any
) -> None:
    bad = {"notification_type": "appointment.exploded", "dedupe_key": f"bad-{uuid.uuid4()}"}
    before = _sample(
        "notifications_dead_lettered_total", notification_type="unknown", error_type="ValidationError"
    )
    retried_before = _sample("notifications_retried_total", notification_type="unknown")
    make_worker(app)
    app.send_task("notifications.send", kwargs=bad, queue="notifications.booking")

    messages = _dlq_messages(app)
    assert len(messages) == 1
    assert _kwargs_of(messages[0]) == bad
    assert delivery(mongo_db, bad["dedupe_key"]) is None
    assert (
        _sample("notifications_dead_lettered_total", notification_type="unknown", error_type="ValidationError")
        == before + 1
    )
    assert _sample("notifications_retried_total", notification_type="unknown") == retried_before


def test_unexpected_error_is_dead_lettered_not_lost(app: Any, make_worker: Any, mongo_db: Any) -> None:
    request = new_request()
    key = request["dedupe_key"]
    make_worker(app)
    with mock.patch("services.notification.tasks.FakeProvider.deliver", side_effect=RuntimeError("boom")):
        enqueue_send(app, **request)
        messages = _dlq_messages(app)
    assert len(messages) == 1
    assert delivery(mongo_db, key)["status"] == "failed"
    assert provider_calls(mongo_db, key) == 0


def test_declared_queues_carry_the_dead_letter_exchange(app: Any, broker: Broker) -> None:
    out = rabbitmqctl("list_queues", "-p", broker.vhost, "name", "arguments").stdout
    rows = {line.split("\t")[0]: line for line in out.splitlines() if line.startswith("notifications.")}
    assert set(rows) == {
        "notifications.booking",
        "notifications.reminder",
        "notifications.followup",
        "notifications.dlq",
    }
    for name in ("notifications.booking", "notifications.reminder", "notifications.followup"):
        assert "x-dead-letter-exchange" in rows[name] and "smarthealth.dlx" in rows[name], rows[name]
        assert "x-dead-letter-routing-key" in rows[name] and "notifications.dlq" in rows[name], rows[name]
    assert "x-dead-letter-exchange" not in rows["notifications.dlq"]
