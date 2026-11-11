"""Fixtures for the event library tests (plan 03-01).

`mock_registry` points the registry client at an in-memory `mock://` registry
and registers every catalogue entry, so event types added by later plans are
covered without changing this fixture.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient

from libs.config import shared_settings
from libs.events.registry import (
    get_registry_client,
    register_all,
    reset_registry_for_tests,
)


@pytest.fixture
def mock_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[SchemaRegistryClient]:
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "mock://p3-tests")
    # The settings singleton is built at import time, so the env var alone
    # would not reach it; patch the attribute the registry module reads.
    monkeypatch.setattr(shared_settings, "schema_registry_url", "mock://p3-tests")
    reset_registry_for_tests()
    client = get_registry_client()
    register_all(client)
    yield client
    reset_registry_for_tests()


# --------------------------------------------------------------------------
# Live Kafka fixtures (plan 03-04). Every test gets unique topics and groups
# (`p3test.<uuid>`), created and deleted by the fixtures, so nothing reaches
# the real event topics.
# --------------------------------------------------------------------------

import datetime  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from collections.abc import Callable  # noqa: E402
from typing import Any  # noqa: E402

from confluent_kafka import Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402
from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)

from libs.events.catalog import topic_for  # noqa: E402
from libs.events.registry import serializer_for  # noqa: E402
from libs.observability.otel import configure_tracer_provider  # noqa: E402

KAFKA_BOOTSTRAP = "localhost:9092"


@pytest.fixture
def kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


@pytest.fixture
def topic_factory(kafka_admin: AdminClient) -> Iterator[Callable[[str], str]]:
    created: list[str] = []

    def make(prefix: str = "p3test") -> str:
        name = f"{prefix}.{uuid.uuid4().hex}"
        futures = kafka_admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])
        futures[name].result(timeout=30)
        created.append(name)
        # Wait until metadata shows the topic so a producer does not race it.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and name not in kafka_admin.list_topics(timeout=5).topics:
            time.sleep(0.2)
        return name

    yield make
    if created:
        for future in kafka_admin.delete_topics(created).values():
            try:
                future.result(timeout=30)
            except Exception:  # noqa: BLE001, S110 - cleanup is best effort
                pass


@pytest.fixture
def unique_topic(topic_factory: Callable[[str], str]) -> str:
    return topic_factory("p3test")


@pytest.fixture
def dlq_topic(topic_factory: Callable[[str], str]) -> str:
    return topic_factory("p3test.dlq")


@pytest.fixture
def unique_group() -> str:
    return f"p3test-group-{uuid.uuid4().hex}"


def booked_payload(**overrides: Any) -> dict[str, Any]:
    now = datetime.datetime(2026, 11, 10, 9, 0, tzinfo=datetime.UTC)
    base: dict[str, Any] = {
        "appointment_id": str(uuid.uuid4()),
        "clinic_id": str(uuid.uuid4()),
        "patient_id": str(uuid.uuid4()),
        "provider_id": str(uuid.uuid4()),
        "slot_id": str(uuid.uuid4()),
        "slot_start": now + datetime.timedelta(days=1),
        "slot_end": now + datetime.timedelta(days=1, minutes=30),
        "booked_at": now,
    }
    base.update(overrides)
    return base


@pytest.fixture
def produce_event(mock_registry: SchemaRegistryClient) -> Callable[..., dict[str, Any]]:
    """Serialize an event with the mock registry and produce it; returns the record."""
    del mock_registry
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def produce(
        topic: str,
        event_type: str = "appointment.booked",
        payload: dict[str, Any] | None = None,
        headers: list[tuple[str, bytes | None]] | None = None,
        *,
        raw_value: bytes | None = None,
        key: str | None = None,
        occurred_at: datetime.datetime | None = None,
    ) -> dict[str, Any]:
        body = dict(payload) if payload is not None else booked_payload()
        aggregate_id = str(body.get("appointment_id", uuid.uuid4()))
        record = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "schema_version": 1,
            "occurred_at": occurred_at or datetime.datetime.now(datetime.UTC),
            "aggregate_id": aggregate_id,
            **body,
        }
        value = raw_value
        if value is None:
            value = serializer_for(event_type)(
                record, SerializationContext(topic_for(event_type), MessageField.VALUE)
            )
        producer.produce(
            topic,
            key=(key or aggregate_id).encode(),
            value=value,
            headers=headers,
        )
        assert producer.flush(15) == 0
        return record

    return produce


@pytest.fixture(scope="session")
def _span_exporter() -> InMemorySpanExporter:
    configure_tracer_provider("smarthealth-tests")
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider), "SDK tracer provider is not installed"
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def in_memory_spans(_span_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    _span_exporter.clear()
    yield _span_exporter
    _span_exporter.clear()


def poll_until(consumer: Any, *, timeout: float = 30.0, want: int = 1) -> list[Any]:
    """Poll `consumer` until it has produced `want` non-None outcomes (or timeout)."""
    outcomes: list[Any] = []
    deadline = time.monotonic() + timeout
    while len(outcomes) < want and time.monotonic() < deadline:
        outcome = consumer.poll_once()
        if outcome is not None:
            outcomes.append(outcome)
    return outcomes


def poll_for(consumer: Any, seconds: float) -> list[Any]:
    """Poll for a fixed window and return every non-None outcome seen."""
    outcomes: list[Any] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        outcome = consumer.poll_once()
        if outcome is not None:
            outcomes.append(outcome)
    return outcomes
