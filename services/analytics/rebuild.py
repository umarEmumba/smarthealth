"""Replay rebuild of the analytics projections (plan 03-13; D-10, T-3-59).

The projections in analytics_db are derived data: every row comes from a Kafka event,
and the topics retain the events (the outbox rows are insert-only and Debezium relays
them, so the topics are the replay source). This tool empties the projections and
rewinds the consumer group to the start of the topics; the consumer then re-applies
every retained event and the views come out the same as before (idempotent,
order-tolerant upserts, plan 03-10).

Operator procedure (all three steps matter, in this order):

    1. docker compose -f infra/docker-compose.yml stop analytics-consumer
    2. docker compose -f infra/docker-compose.yml run --rm --no-deps analytics \\
           python -m services.analytics.rebuild            # dry run: prints the plan
       ... then the same command with --apply
    3. docker compose -f infra/docker-compose.yml start analytics-consumer

`python -m services.analytics.rebuild [--apply] [--group G] [--topics T ...]`

* Without `--apply` nothing changes: it prints the group's member count, the row counts
  it would delete and the offsets it would reset.
* With `--apply` it first REFUSES (exit 1, nothing touched) while the group has an
  active member: a live consumer would keep writing into the tables being emptied.
* Then, in ONE transaction, it truncates `patients`, `appointments` and
  `visits_completed` and deletes this projection's claims from `processed_events`
  (the claims are keyed by the consumer name `analytics-projection`, whatever the
  group is called; without deleting them a replay would skip every event as already
  processed). Only after that transaction commits does it set the group's committed
  offsets to each partition's earliest (low watermark) offset. If the offset reset
  fails, the tables are already empty and the command can simply be run again
  (it is idempotent).
* The replay reads from the earliest RETAINED offset. Events that Kafka already
  removed by retention, and events from before the Phase 3 cutover (D-13), are not
  recoverable this way; so a rebuild after the live metrics started at go-live also
  replays any throw-away events still sitting in the topics.

Output carries counts and names only: no event content, no connection strings.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from libs.config import shared_settings
from libs.events.catalog import topic_for
from services.analytics.config import settings
from services.analytics.consumer import CONSUMER_NAME, GROUP_ID
from services.analytics.projections import PROJECTED_EVENT_TYPES

__all__ = ["main", "run"]

_TIMEOUT = 30.0
_COUNTS = {
    "patients": "SELECT count(*) FROM patients",
    "appointments": "SELECT count(*) FROM appointments",
    "visits_completed": "SELECT count(*) FROM visits_completed",
}
_TRUNCATE = "TRUNCATE TABLE patients, appointments, visits_completed"
_DELETE_CLAIMS = "DELETE FROM processed_events WHERE consumer = :consumer"
_COUNT_CLAIMS = "SELECT count(*) FROM processed_events WHERE consumer = :consumer"


def default_topics() -> list[str]:
    return sorted({topic_for(event_type) for event_type in PROJECTED_EVENT_TYPES})


def group_member_count(admin: AdminClient, group: str) -> int:
    """Number of live members of `group` (0 for an empty or never-created group)."""
    future = admin.describe_consumer_groups([group], request_timeout=_TIMEOUT)[group]
    try:
        return len(future.result(timeout=_TIMEOUT).members)
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
            return 0
        raise


def earliest_offsets(admin: AdminClient, topics: Sequence[str]) -> dict[TopicPartition, int]:
    """Low watermark of every partition of every topic."""
    metadata = admin.list_topics(timeout=_TIMEOUT)
    requests: dict[TopicPartition, OffsetSpec] = {}
    for topic in topics:
        entry = metadata.topics.get(topic)
        if entry is None or entry.error is not None:
            raise RuntimeError(f"topic {topic} does not exist")
        for partition in entry.partitions:
            requests[TopicPartition(topic, partition)] = OffsetSpec.earliest()
    return {
        tp: future.result(timeout=_TIMEOUT).offset
        for tp, future in admin.list_offsets(requests, request_timeout=_TIMEOUT).items()
    }


async def _counts() -> tuple[dict[str, int], int]:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            tables = {name: int((await conn.execute(text(sql))).scalar_one()) for name, sql in _COUNTS.items()}
            claims = int(
                (await conn.execute(text(_COUNT_CLAIMS), {"consumer": CONSUMER_NAME})).scalar_one()
            )
        return tables, claims
    finally:
        await engine.dispose()


async def _truncate() -> None:
    """One transaction: the three projection tables and this projection's claims."""
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(_TRUNCATE))
            await conn.execute(text(_DELETE_CLAIMS), {"consumer": CONSUMER_NAME})
    finally:
        await engine.dispose()


def _reset_offsets(admin: AdminClient, group: str, offsets: dict[TopicPartition, int]) -> None:
    request = ConsumerGroupTopicPartitions(
        group, [TopicPartition(tp.topic, tp.partition, offset) for tp, offset in offsets.items()]
    )
    admin.alter_consumer_group_offsets([request], request_timeout=_TIMEOUT)[group].result(
        timeout=_TIMEOUT
    )


def run(*, group: str, topics: Sequence[str], apply: bool) -> int:
    admin = AdminClient({"bootstrap.servers": shared_settings.kafka_bootstrap_servers})
    members = group_member_count(admin, group)
    print(f"group {group}: {members} active member(s)")
    if apply and members:
        print(
            "refusing to rebuild: the consumer group has active members; stop the "
            "analytics-consumer first. Nothing was changed.",
            file=sys.stderr,
        )
        return 1

    tables, claims = asyncio.run(_counts())
    offsets = earliest_offsets(admin, topics)
    for name, count in tables.items():
        print(f"{'would delete' if not apply else 'deleting'} {count} row(s) from {name}")
    print(f"{'would delete' if not apply else 'deleting'} {claims} claim(s) of {CONSUMER_NAME}")
    for tp, offset in sorted(offsets.items(), key=lambda item: (item[0].topic, item[0].partition)):
        print(
            f"{'would reset' if not apply else 'resetting'} {group} on {tp.topic}[{tp.partition}] "
            f"to offset {offset}"
        )
    if not apply:
        print("dry run: nothing changed (pass --apply to rebuild)")
        return 0

    asyncio.run(_truncate())
    _reset_offsets(admin, group, offsets)
    print("rebuild applied: start the analytics-consumer to replay the topics")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m services.analytics.rebuild",
        description=(
            "Empty the analytics projections and rewind the consumer group to the start of "
            "the topics so the consumer replays them. Stop analytics-consumer first. "
            "Without --apply it only prints what it would do."
        ),
    )
    parser.add_argument("--apply", action="store_true", help="actually truncate and reset offsets")
    parser.add_argument("--group", default=GROUP_ID, help=f"consumer group (default {GROUP_ID})")
    parser.add_argument(
        "--topics",
        nargs="+",
        default=default_topics(),
        help="topics whose group offsets are reset (default: the patient, appointment and visit topics)",
    )
    args = parser.parse_args(argv)
    try:
        return run(group=args.group, topics=args.topics, apply=args.apply)
    except (KafkaException, RuntimeError, OSError, TimeoutError) as exc:
        print(f"rebuild failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
