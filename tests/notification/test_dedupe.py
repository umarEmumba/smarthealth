"""Exactly-once delivery per dedupe key, retry with backoff, closed-appointment guard (plan 03-02).

The crash test kills a REAL worker process: a Celery worker started as a
subprocess takes a `FAIL-CRASH-AFTER-PROVIDER-` send, the provider records the
call, and the process ends with `os._exit(70)` before `mark_sent` and before the
ack. RabbitMQ returns the unacknowledged message to the queue when the
connection drops, and an in-process worker finishes it.
"""

from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from prometheus_client import REGISTRY

from libs.config import shared_settings
from services.notification import deliveries
from services.notification.config import settings
from services.notification.provider import FakeProvider
from services.notification.deliveries import Claim, claim_delivery, mark_appointment_closed
from services.notification.tasks import SendRequest, enqueue_send
from tests.notification.broker import REPO_ROOT, Broker
from tests.notification.conftest import delivery, new_request, provider_calls, wait_for

pytestmark = pytest.mark.integration

ALL_WORK = ("notifications.booking", "notifications.reminder", "notifications.followup")


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _is(db: Any, key: str, status: str) -> Any:
    return lambda: (d := delivery(db, key)) and d["status"] == status and d


def test_same_send_enqueued_twice_reaches_the_provider_once(app: Any, make_worker: Any, mongo_db: Any) -> None:
    make_worker(app)
    request = new_request()
    enqueue_send(app, **request)
    enqueue_send(app, **request)
    doc = wait_for(_is(mongo_db, request["dedupe_key"], "sent"))
    assert doc
    time.sleep(2)  # let the second message drain
    assert provider_calls(mongo_db, request["dedupe_key"]) == 1
    assert mongo_db[deliveries.DELIVERIES].count_documents({"dedupe_key": request["dedupe_key"]}) == 1
    assert delivery(mongo_db, request["dedupe_key"])["status"] == "sent"


def test_claim_delivery_state_machine(mongo_db: Any) -> None:
    request = SendRequest(**new_request())
    first = claim_delivery(mongo_db, request)
    assert (first.state, first.attempts) == (Claim.NEW, 1)
    again = claim_delivery(mongo_db, request)  # still `sending`: a previous attempt died
    assert (again.state, again.attempts) == (Claim.RETRY, 2)
    deliveries.mark_failed(mongo_db, request.dedupe_key, "TransientDeliveryError")
    third = claim_delivery(mongo_db, request)
    assert (third.state, third.attempts) == (Claim.RETRY, 3)
    deliveries.mark_sent(mongo_db, request.dedupe_key, "m-1")
    assert claim_delivery(mongo_db, request).state is Claim.DONE


def test_window_between_provider_call_and_mark_sent_replays_the_recorded_message(
    app: Any, make_worker: Any, mongo_db: Any
) -> None:
    """Deterministic variant: the exact state a kill leaves (delivery `sending`, provider record present)."""
    request = new_request()
    key = request["dedupe_key"]
    stored_message_id = str(uuid.uuid4())
    mongo_db[deliveries.DELIVERIES].insert_one(
        {**request, "status": "sending", "attempts": 1, "visit_id": None, "event_id": request["event_id"]}
    )
    mongo_db[deliveries.PROVIDER_CALLS].insert_one(
        {"dedupe_key": key, "message_id": stored_message_id, "attempt": 1, "notification_type": request["notification_type"]}
    )
    replayed_before = _sample("notifications_provider_replayed_total", notification_type="appointment.booked")
    make_worker(app)
    enqueue_send(app, **request)

    doc = wait_for(_is(mongo_db, key, "sent"))
    assert doc
    assert provider_calls(mongo_db, key) == 1
    assert doc["message_id"] == stored_message_id
    assert doc["provider_replayed"] is True
    assert doc["attempts"] == 2
    assert _sample("notifications_provider_replayed_total", notification_type="appointment.booked") == replayed_before + 1


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _run_worker_subprocess(
    broker: Broker, db_name: str, tmp_path: Path, log: Path, *, crash: bool = True
) -> subprocess.Popen[bytes]:
    password_file = tmp_path / "broker.secret"
    password_file.write_text(broker.password)
    password_file.chmod(0o600)
    env = {
        **os.environ,
        "NOTIFICATION_BROKER_HOST": broker.host,
        "NOTIFICATION_BROKER_PORT": str(broker.port),
        "NOTIFICATION_BROKER_VHOST": broker.vhost,
        "NOTIFICATION_BROKER_USER": broker.user,
        "NOTIFICATION_BROKER_PASSWORD_FILE": str(password_file),
        "NOTIFICATION_FAKE_CRASH_ENABLED": "true" if crash else "false",
        "NOTIFICATION_METRICS_PORT": str(_free_port()),
        "MONGO_DB_NAME": db_name,
    }
    argv = [
        str(REPO_ROOT / ".venv" / "bin" / "celery"),
        "-A",
        "services.notification.celery_app",
        "worker",
        "-P",
        "threads",
        "-c",
        "1",
        "-Q",
        "notifications.booking",
        "--without-mingle",
        "--without-gossip",
        "--without-heartbeat",
        "-l",
        "INFO",
    ]
    out = log.open("wb")
    return subprocess.Popen(argv, env=env, cwd=REPO_ROOT, stdout=out, stderr=subprocess.STDOUT)  # noqa: S603


def test_worker_process_killed_between_provider_call_and_mark_sent_sends_once(
    app: Any, make_worker: Any, mongo_db: Any, broker: Broker, tmp_path: Path
) -> None:
    request = new_request(dedupe_key=f"FAIL-CRASH-AFTER-PROVIDER-{uuid.uuid4()}")
    key = request["dedupe_key"]
    enqueue_send(app, **request)  # declares/uses the queue before the worker starts

    replayed_before = _sample("notifications_provider_replayed_total", notification_type="appointment.booked")
    log = tmp_path / "worker.log"
    proc = _run_worker_subprocess(broker, shared_settings.mongo_db_name, tmp_path, log)
    try:
        deadline = time.monotonic() + 60
        while proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.5)
        exit_code = proc.poll()
        if exit_code is None:
            proc.terminate()
            proc.wait(timeout=15)
            pytest.fail(f"subprocess worker did not crash within 60 s; log tail: {log.read_text(errors='replace')[-1500:]}")
    finally:
        if proc.poll() is None:
            proc.kill()
    assert exit_code == 70, log.read_text(errors="replace")[-1500:]

    # State the kill leaves: the provider recorded the call, the delivery is still `sending`.
    assert provider_calls(mongo_db, key) == 1
    killed = delivery(mongo_db, key)
    assert killed["status"] == "sending"
    assert killed["attempts"] == 1
    original_message_id = mongo_db[deliveries.PROVIDER_CALLS].find_one({"dedupe_key": key})["message_id"]

    # The unacknowledged message returned to the queue; an in-process worker (crash switch off) takes it.
    assert settings.fake_crash_enabled is False
    make_worker(app)
    doc = wait_for(_is(mongo_db, key, "sent"))
    assert doc, delivery(mongo_db, key)
    assert doc["attempts"] == 2
    assert doc["message_id"] == original_message_id
    assert provider_calls(mongo_db, key) == 1
    assert _sample("notifications_provider_replayed_total", notification_type="appointment.booked") == replayed_before + 1


def test_transient_failure_is_retried_with_exponential_backoff(
    app: Any, make_worker: Any, mongo_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "retry_backoff_base_seconds", 1.0)
    request = new_request(dedupe_key=f"FAIL-TRANSIENT-{uuid.uuid4()}")
    key = request["dedupe_key"]
    retried_before = _sample("notifications_retried_total", notification_type="appointment.booked")
    countdowns: list[float] = []
    task = app.tasks["notifications.send"]
    real_retry = task.retry

    def recording_retry(*args: Any, **kwargs: Any) -> Any:
        countdowns.append(kwargs["countdown"])
        return real_retry(*args, **kwargs)

    with mock.patch.object(task, "retry", side_effect=recording_retry):
        make_worker(app)
        started = time.monotonic()
        enqueue_send(app, **request)
        doc = wait_for(_is(mongo_db, key, "sent"), timeout=40)
        elapsed = time.monotonic() - started
    assert doc, delivery(mongo_db, key)
    assert countdowns == [1.0, 2.0]
    assert elapsed >= 3.0
    assert doc["attempts"] == 3
    assert provider_calls(mongo_db, key) == 1
    assert mongo_db[deliveries.PROVIDER_CALLS].find_one({"dedupe_key": key})["attempt"] == 3
    assert _sample("notifications_retried_total", notification_type="appointment.booked") == retried_before + 2


def test_reminder_for_a_closed_appointment_is_skipped_but_booking_is_not(
    app: Any, make_worker: Any, mongo_db: Any
) -> None:
    appointment_id = str(uuid.uuid4())
    mark_appointment_closed(mongo_db, appointment_id, "cancelled")
    reminder = new_request("appointment.reminder", appointment_id=appointment_id)
    booked = new_request("appointment.booked", appointment_id=appointment_id)
    skipped_before = _sample("notifications_skipped_total", notification_type="appointment.reminder", reason="closed")
    make_worker(app, queues=ALL_WORK)
    enqueue_send(app, **reminder)
    enqueue_send(app, **booked)

    assert wait_for(_is(mongo_db, reminder["dedupe_key"], "skipped_closed"))
    assert wait_for(_is(mongo_db, booked["dedupe_key"], "sent"))
    assert provider_calls(mongo_db, reminder["dedupe_key"]) == 0
    assert provider_calls(mongo_db, booked["dedupe_key"]) == 1
    assert (
        _sample("notifications_skipped_total", notification_type="appointment.reminder", reason="closed")
        == skipped_before + 1
    )


def test_concurrent_claims_of_one_key_give_one_provider_record(mongo_db: Any) -> None:
    """Eight threads race claim + provider call on one key: the unique indexes allow one delivery."""
    request = SendRequest(**new_request())
    barrier = threading.Barrier(8)
    results: list[Any] = []

    def attempt() -> None:
        barrier.wait()
        claim = claim_delivery(mongo_db, request)
        if claim.state is not Claim.DONE:
            results.append(FakeProvider(mongo_db).deliver(request, claim.attempts))

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert len(results) >= 1
    assert sum(1 for r in results if not r.replayed) == 1
    assert len({r.message_id for r in results}) == 1
    assert provider_calls(mongo_db, request.dedupe_key) == 1
    assert mongo_db[deliveries.DELIVERIES].count_documents({"dedupe_key": request.dedupe_key}) == 1


def test_concurrent_duplicates_on_two_workers_give_one_provider_record(
    app: Any, broker: Broker, make_worker: Any, mongo_db: Any, tmp_path: Path
) -> None:
    """One worker is a real subprocess, one is in-process (two in-process workers share
    Celery's global shutdown flag, so only one can be stopped cleanly in a single process)."""
    log = tmp_path / "second-worker.log"
    proc = _run_worker_subprocess(broker, shared_settings.mongo_db_name, tmp_path, log, crash=False)
    try:
        assert wait_for(lambda: b" ready." in log.read_bytes(), timeout=60), log.read_text(errors="replace")[-1500:]
        make_worker(app)
        request = new_request()
        key = request["dedupe_key"]
        for _ in range(8):
            enqueue_send(app, **request)
        assert wait_for(_is(mongo_db, key, "sent"))
        time.sleep(3)
        assert provider_calls(mongo_db, key) == 1
        assert mongo_db[deliveries.DELIVERIES].count_documents({"dedupe_key": key}) == 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
