"""Where a NEW analytics consumer group starts (plan 03-13, D-13).

`ANALYTICS_CONSUMER_START=latest` (Compose) makes the live metrics count from go-live.
`pin_start_offsets` records that position explicitly before the consumer subscribes, so
an event produced right after startup is never skipped by the "latest is resolved at the
first assignment" race, while the events already on the topic are left out.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition

from services.analytics.config import AnalyticsSettings
from services.analytics.consumer import build_consumer, pin_start_offsets
from tests.analytics.conftest import (  # noqa: F401 - fixtures
    clinic_id,
    claimed_event_ids,
    dlq_topic,
    kafka_admin,
    mock_registry,
    produce_event,
    query,
    running_consumer,
    topic_factory,
    unique_group,
    unique_topic,
    wait_for,
)
from tests.events.conftest import KAFKA_BOOTSTRAP
from tests.analytics.test_projections import T0, _active, _registered

pytestmark = pytest.mark.integration


def _committed(group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        return int(probe.committed([TopicPartition(topic, 0)], timeout=10)[0].offset)
    finally:
        probe.close()


def test_settings_default_to_earliest_and_map_to_auto_offset_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    assert AnalyticsSettings().consumer_start == "earliest"  # tests and replays read history
    monkeypatch.setenv("ANALYTICS_CONSUMER_START", "latest")
    assert AnalyticsSettings().consumer_start == "latest"
    monkeypatch.setenv("ANALYTICS_CONSUMER_START", "yesterday")
    with pytest.raises(ValueError):
        AnalyticsSettings()


def test_build_consumer_passes_the_start_policy(monkeypatch: pytest.MonkeyPatch, unique_topic: str, unique_group: str, dlq_topic: str) -> None:
    from services.analytics import consumer as module  # noqa: PLC0415

    monkeypatch.setattr(module.settings, "consumer_start", "latest")
    captured: dict[str, Any] = {}
    monkeypatch.setattr(module, "EventConsumer", lambda **kwargs: captured.update(kwargs))
    build_consumer(group_id=unique_group, topics=[unique_topic], dlq_topic=dlq_topic)
    assert captured["consumer_overrides"] == {"auto.offset.reset": "latest"}


def test_pinned_group_skips_history_but_never_the_first_event_after_start(
    clinic_id: uuid.UUID,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
    claimed_event_ids: list[str],
) -> None:
    def produce(patient: uuid.UUID) -> None:
        record = produce_event(unique_topic, "patient.registered", _registered(clinic_id, patient), occurred_at=T0)
        claimed_event_ids.append(record["event_id"])

    produce(uuid.uuid4())  # history: before go-live
    produce(uuid.uuid4())

    assert pin_start_offsets(unique_group, [unique_topic], "earliest") == {}
    assert _committed(unique_group, unique_topic) < 0  # earliest pins nothing
    pinned = pin_start_offsets(unique_group, [unique_topic], "latest")
    assert pinned == {(unique_topic, 0): 2}
    assert _committed(unique_group, unique_topic) == 2
    # A second start resumes: an existing position is never moved.
    assert pin_start_offsets(unique_group, [unique_topic], "latest") == {}

    produce(uuid.uuid4())  # the first event after go-live
    with running_consumer(unique_topic, unique_group, dlq_topic):
        assert wait_for(lambda: _active(clinic_id), 1) == 1
        assert wait_for(lambda: _active(clinic_id), 1, timeout=2) == 1  # history stayed out
