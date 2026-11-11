"""Pin the go-live offset of a NEW consumer group (D-13; moved here from the analytics
consumer in plan 03-14 so notification-trigger shares it without importing another
service).

With `start == "latest"`, `pin_start_offsets` commits the current end of every partition
the group has no committed offset for, BEFORE the consumer subscribes. The go-live
position is therefore recorded deterministically at container start instead of being
left to the group's first assignment, so events produced after the call are always
consumed while the retained history (throw-away test events) is not.
"""

from __future__ import annotations

import structlog
from confluent_kafka import Consumer, KafkaException, TopicPartition

from libs.config import shared_settings

__all__ = ["pin_start_offsets"]

log = structlog.get_logger(__name__)


def pin_start_offsets(
    group_id: str,
    topics: list[str],
    start: str,
    *,
    bootstrap_servers: str | None = None,
    timeout: float = 15.0,
) -> dict[tuple[str, int], int]:
    """With `start == "latest"`, commit the current end of every partition the group
    has NOT committed yet; return the (topic, partition) -> offset pairs it pinned.

    This records the go-live position deterministically instead of leaving it to the
    group's first assignment, so events produced after this call are always consumed.
    A group that already has a committed offset keeps it (a restart resumes). Any
    broker error is logged and ignored: the consumer then falls back to
    `auto.offset.reset`, which is the same `latest` behaviour minus the race guard.
    """
    if start != "latest":
        return {}
    probe = Consumer(
        {
            "bootstrap.servers": bootstrap_servers or shared_settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    pinned: dict[tuple[str, int], int] = {}
    try:
        metadata = probe.list_topics(timeout=timeout)
        wanted = [
            TopicPartition(topic, partition)
            for topic in topics
            if topic in metadata.topics and metadata.topics[topic].error is None
            for partition in metadata.topics[topic].partitions
        ]
        if not wanted:
            return {}
        to_commit: list[TopicPartition] = []
        for tp in probe.committed(wanted, timeout=timeout):
            if tp.offset >= 0:
                continue  # already has a position: keep it
            _low, high = probe.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=timeout
            )
            to_commit.append(TopicPartition(tp.topic, tp.partition, high))
            pinned[(tp.topic, tp.partition)] = high
        if to_commit:
            probe.commit(offsets=to_commit, asynchronous=False)
    except KafkaException as exc:
        log.warning("consumer.pin_start_failed", group=group_id, error=type(exc).__name__)
        return {}
    finally:
        probe.close()
    return pinned
