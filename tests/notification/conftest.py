"""Fixtures for the notification tests (plan 03-02).

Everything runs against the live RabbitMQ (one throwaway vhost per session) and
the live Mongo (one throwaway database per session, dropped afterwards).
"""

from __future__ import annotations

import datetime
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pymongo.database import Database

from libs.config import shared_settings
from libs.observability.otel import configure_tracer_provider
from services.notification import deliveries
from services.notification.celery_app import (
    DEAD_LETTER_QUEUE,
    WORK_QUEUES,
    create_celery_app,
    declare_topology,
    instrument_publisher,
)
from services.notification.config import settings
from services.notification import tasks as _tasks  # noqa: F401  (registers notifications.send)
from tests.notification.broker import Broker, throwaway_broker

pytestmark = pytest.mark.integration

ALL_QUEUES = (*WORK_QUEUES, DEAD_LETTER_QUEUE)


@pytest.fixture(scope="session")
def broker() -> Iterator[Broker]:
    with throwaway_broker() as b:
        yield b


@pytest.fixture(scope="session")
def mongo_db() -> Iterator[Database]:
    name = f"p3test_notif_{uuid.uuid4().hex[:10]}"
    patch = pytest.MonkeyPatch()
    patch.setattr(shared_settings, "mongo_db_name", name)
    # Workers started in-process run the worker_init hook; keep it off fixed ports.
    patch.setattr(settings, "metrics_port", 0)
    db = deliveries.get_db()
    deliveries.ensure_notification_indexes(db)
    try:
        yield db
    finally:
        deliveries.get_client().drop_database(name)
        patch.undo()


@pytest.fixture(scope="session")
def _span_exporter() -> InMemorySpanExporter:
    configure_tracer_provider("smarthealth-tests")
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider), "SDK tracer provider is not installed"
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instrument_publisher()
    return exporter


@pytest.fixture
def in_memory_spans(_span_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    _span_exporter.clear()
    yield _span_exporter
    _span_exporter.clear()


@pytest.fixture
def app(broker: Broker, mongo_db: Database) -> Celery:
    """A Celery app bound to the throwaway vhost, with the topology declared and
    every queue empty at the start of the test."""
    celery_app = create_celery_app(broker.url)
    declare_topology(celery_app)
    with celery_app.connection_for_write() as connection:
        channel = connection.default_channel
        for name in ALL_QUEUES:
            channel.queue_purge(name)
    return celery_app


@pytest.fixture
def make_worker() -> Iterator[Callable[..., Any]]:
    """`make_worker(app, queues=...)` starts an in-process threads-pool worker; all are stopped at teardown."""
    stack: list[Any] = []

    def _make(celery_app: Celery, queues: tuple[str, ...] = ("notifications.booking",)) -> Any:
        cm = start_worker(
            celery_app,
            pool="threads",
            concurrency=2,
            perform_ping_check=False,
            queues=list(queues),
            loglevel="WARNING",
        )
        worker = cm.__enter__()
        stack.append(cm)
        return worker

    try:
        yield _make
    finally:
        for cm in reversed(stack):
            cm.__exit__(None, None, None)


def new_request(notification_type: str = "appointment.booked", dedupe_key: str | None = None, **over: Any) -> dict[str, str]:
    request = {
        "notification_type": notification_type,
        "dedupe_key": dedupe_key or f"{notification_type}:{uuid.uuid4()}",
        "patient_id": str(uuid.uuid4()),
        "appointment_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "requested_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    request.update(over)
    return request  # type: ignore[return-value]


def wait_for(predicate: Callable[[], Any], timeout: float = 30.0, interval: float = 0.2) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def delivery(db: Database, dedupe_key: str) -> dict[str, Any] | None:
    return db[deliveries.DELIVERIES].find_one({"dedupe_key": dedupe_key})


def provider_calls(db: Database, dedupe_key: str) -> int:
    return db[deliveries.PROVIDER_CALLS].count_documents({"dedupe_key": dedupe_key})
