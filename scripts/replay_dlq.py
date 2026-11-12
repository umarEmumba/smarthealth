"""Replay dead-lettered events to their original topic (plan 03-22, W4).

Why this exists: a consumer group dead-letters a record it cannot process (a poison record,
a handler failure that kept failing, or a transient failure that hit the consumer base's
`TransientExhausted` cap, plan 03-21) by writing its ORIGINAL key, value bytes and headers
to `smarthealth.dlq.<group>` plus `dlq.*` headers (`dlq.error_type`, `dlq.consumer_group`,
`dlq.original_topic`, `dlq.original_partition`, `dlq.original_offset`, `dlq.attempts`), and
then moves its offset on. Nothing reads a DLQ topic back, so a dead letter whose cause has
been fixed (the database is back, a bug is deployed) needs this tool.

What a replay does: it re-produces the record's original key, value bytes and original
headers (every `dlq.*` header removed, so the event id header and `traceparent` are the
original ones) to the record's `dlq.original_topic`, which EVERY consumer group on that
topic then reads (for smarthealth.visit.events: visit-downstream, analytics-projection and
notification-trigger). A group that already recorded the event id absorbs the duplicate
through its idempotency key (D-08: processed-event claims for analytics-projection, dedupe
keys and deterministic workflow ids for notification-trigger, `start_once` plus the
conditional marker for visit-downstream). A group that never handled it (it dead-lettered
the event, or the event predates the group's pinned start offset) processes it as NEW.
notification-trigger's visit.completed handler has no staleness guard (first-review WR-06,
still open), so replaying an old visit.completed can send a follow-up for an old visit.
Select narrowly, ideally by `--event-id`. A record that fails again is dead-lettered again.

Recovery map per group:
  visit-downstream      the visit-reconciler's lost-trigger pass restarts the visit after
                        the reconcile grace (automatic); `scripts/backfill_visit_completed_events.py`
                        is the manual route; this script re-delivers the event itself.
  analytics-projection  this script, or `services.analytics.rebuild`.
  notification-trigger  this script.

Safety: dry run by default (it reads and prints, produces nothing). `--apply` needs a
selector: `--event-id`, `--key`, `--error-type`, or `--all` with an explicit `--limit`; an
unfiltered `--apply` exits 2 before any Kafka client is opened. Records whose
`dlq.error_type` is NonRetryableEventError or DeserializationError (poison: replaying them
only dead-letters them again) are skipped by default and counted per type on
`skipped_poison error_type=<type> count=<n>` lines; `--include-poison`, or an `--error-type`
that names the type, selects them. It reads with partitions
`assign`ed explicitly and never commits, so no consumer group's offsets change, and it stops
at the high watermark captured at start. A record is only replayed to a catalogue topic
(`libs.events.catalog.AGGREGATE_TOPICS`) or to a topic named with `--allow-topic`; a forged
`dlq.original_topic` header is counted `refused` and nothing is produced for it. `--limit`
(default 100) bounds a run (the poison records skipped do not count against it). The producer is idempotent with `acks=all`.

Output is DLQ coordinates, the error type, the event id and counts only: never a key, a
value or any other header value (a header that is not plain text is shown as `?`).

Usage (repo root; the broker comes from the shared settings, `KAFKA_BOOTSTRAP_SERVERS`):

    .venv/bin/python scripts/replay_dlq.py --group visit-downstream             # dry run
    .venv/bin/python scripts/replay_dlq.py --dlq-topic smarthealth.dlq.analytics-projection
        [--error-type TransientExhausted] [--key <record key>] [--event-id <uuid>]
        [--all] [--include-poison] [--limit N] [--allow-topic <topic>] [--apply]
    .venv/bin/python scripts/replay_dlq.py --group analytics-projection --all --limit 20 --apply

One line per selected record, `dlq_partition=P dlq_offset=O original=<topic>:<p>:<o>
error_type=<type> event_id=<uuid or ->` (a refused record ends with ` refused`), then
`mode=dry-run|apply scanned=N selected=M replayed=R refused=F` (the skipped_poison lines come
just before it). Exit codes: 0 on success (including a dry run), 1 on a Kafka or registry
error (the error name only is printed), 2 on bad arguments (including an unfiltered
`--apply`).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any

# Repo root on sys.path so this works as `python scripts/replay_dlq.py` from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from libs.config import shared_settings  # noqa: E402
from libs.events.catalog import AGGREGATE_TOPICS, DLQ_TOPICS  # noqa: E402

DEFAULT_LIMIT = 100
# dlq.error_type values of records a replay cannot fix (libs.events.consumer: the base's
# DESERIALIZATION_ERROR and NonRetryableEventError): skipped unless asked for.
POISON_ERROR_TYPES = frozenset({"NonRetryableEventError", "DeserializationError"})
DLQ_PREFIX = "dlq."
IDLE_SECONDS = 10.0
FLUSH_SECONDS = 30.0
_SAFE_TEXT = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_DIGITS = re.compile(r"^\d{1,12}$")


@dataclass
class Record:
    """One dead-lettered record, as read. Never printed except through `describe()`."""

    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]

    def header(self, name: str) -> str | None:
        for key, value in self.headers:
            if key == name and value is not None:
                return value.decode("utf-8", errors="replace")
        return None

    @property
    def original_topic(self) -> str | None:
        return self.header("dlq.original_topic")

    def replay_headers(self) -> list[tuple[str, bytes | None]]:
        """The original headers, in order, without any `dlq.` header."""
        return [(k, v) for k, v in self.headers if not k.startswith(DLQ_PREFIX)]

    def event_id(self, *, decode_value: bool = False) -> str | None:
        """The event id: the `id` header the outbox router sets; only when it is absent and
        `decode_value` is set, `event_id` from the Avro value. None when unknown."""
        header = self.header("id")
        if header is not None:
            return _as_uuid(header)
        if decode_value and self.value is not None:
            try:
                from libs.events.registry import deserializer  # noqa: PLC0415

                record = deserializer()(
                    self.value, SerializationContext(self.original_topic or "", MessageField.VALUE)
                )
            except Exception:  # noqa: BLE001 - an undecodable value simply has no known id
                return None
            if isinstance(record, dict):
                return _as_uuid(str(record.get("event_id", "")))
        return None


def _as_uuid(text: str) -> str | None:
    try:
        return str(uuid.UUID(text))
    except ValueError:
        return None


def _safe(text: str | None, pattern: re.Pattern[str] = _SAFE_TEXT) -> str:
    """Header text for output: only plain, bounded text; anything else is `?`."""
    return text if text is not None and pattern.match(text) else "?"


@dataclass
class Counts:
    scanned: int = 0
    selected: int = 0
    replayed: int = 0
    refused: int = 0
    skipped_poison: dict[str, int] = field(default_factory=dict)
    lines: list[str] = field(default_factory=list)

    def final(self, *, apply: bool) -> str:
        mode = "apply" if apply else "dry-run"
        return (
            f"mode={mode} scanned={self.scanned} selected={self.selected} "
            f"replayed={self.replayed} refused={self.refused}"
        )


@dataclass(frozen=True)
class Selection:
    group: str | None
    error_type: str | None
    key: bytes | None
    event_id: str | None
    limit: int
    include_poison: bool = False


def matches(record: Record, sel: Selection) -> bool:
    if record.original_topic is None:
        return False
    if (
        sel.error_type is None
        and not sel.include_poison
        and record.header("dlq.error_type") in POISON_ERROR_TYPES
    ):
        return False
    if sel.group is not None and record.header("dlq.consumer_group") != sel.group:
        return False
    if sel.error_type is not None and record.header("dlq.error_type") != sel.error_type:
        return False
    if sel.key is not None and record.key != sel.key:
        return False
    if sel.event_id is not None and record.event_id(decode_value=True) != sel.event_id:
        return False
    return True


def read_dlq(
    bootstrap: str, topic: str, sel: Selection, counts: Counts
) -> list[Record]:
    """Scan the DLQ topic from the low to the high watermark captured now (partitions
    assigned explicitly, nothing committed) and return the selected records, at most
    `sel.limit`. `counts.scanned` counts every record examined."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"replay-dlq-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
        }
    )
    selected: list[Record] = []
    try:
        metadata = consumer.list_topics(topic, timeout=15).topics.get(topic)
        if metadata is None or metadata.error is not None or not metadata.partitions:
            return selected  # a DLQ that was never written to is empty
        ends: dict[int, int] = {}
        starts: list[TopicPartition] = []
        for partition in sorted(metadata.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=15
            )
            if high > low:
                ends[partition] = high
                starts.append(TopicPartition(topic, partition, low))
        if not starts:
            return selected
        consumer.assign(starts)
        pending = set(ends)
        last_progress = time.monotonic()
        while pending and len(selected) < sel.limit:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_progress > IDLE_SECONDS:
                    break  # offset gaps (markers, compaction): nothing more to read
                continue
            if message.error():
                if message.error().code() == -191:  # _PARTITION_EOF
                    continue
                raise KafkaException(message.error())
            last_progress = time.monotonic()
            partition, offset = message.partition(), message.offset()
            if partition not in pending:
                continue
            if offset >= ends[partition]:
                pending.discard(partition)
                continue
            if offset >= ends[partition] - 1:
                pending.discard(partition)
            counts.scanned += 1
            record = Record(
                partition=partition,
                offset=offset,
                key=message.key(),
                value=message.value(),
                headers=list(message.headers() or []),
            )
            if matches(record, sel):
                selected.append(record)
            elif matches(record, replace(sel, include_poison=True)):
                error_type = _safe(record.header("dlq.error_type"))
                counts.skipped_poison[error_type] = counts.skipped_poison.get(error_type, 0) + 1
    finally:
        consumer.close()
    return selected


def describe(record: Record, *, refused: bool) -> str:
    original = (
        f"{_safe(record.original_topic)}:"
        f"{_safe(record.header('dlq.original_partition'), _DIGITS)}:"
        f"{_safe(record.header('dlq.original_offset'), _DIGITS)}"
    )
    line = (
        f"dlq_partition={record.partition} dlq_offset={record.offset} original={original} "
        f"error_type={_safe(record.header('dlq.error_type'))} "
        f"event_id={record.event_id() or '-'}"
    )
    return line + (" refused" if refused else "")


def replay(bootstrap: str, records: list[Record]) -> int:
    """Produce each record's original key, value and non-dlq headers to its original topic
    with an idempotent producer; returns how many deliveries were confirmed. Raises
    KafkaException on any delivery error."""
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            # Java's default partitioner: the same key lands on the same partition as the
            # original (Debezium) produce, keeping per-key order.
            "partitioner": "murmur2_random",
        }
    )
    delivered = 0
    errors: list[Any] = []

    def on_delivery(err: Any, _message: Any) -> None:
        nonlocal delivered
        if err is not None:
            errors.append(err)
        else:
            delivered += 1

    for record in records:
        producer.produce(
            record.original_topic,
            key=record.key,
            value=record.value,
            headers=record.replay_headers(),
            on_delivery=on_delivery,
        )
    remaining = producer.flush(FLUSH_SECONDS)
    if errors:
        raise KafkaException(errors[0])
    if remaining > 0:
        raise KafkaException(_Undelivered())
    return delivered


class _Undelivered:
    """Stands in for a KafkaError when a flush times out with messages still queued."""

    def name(self) -> str:
        return "_MSG_TIMED_OUT"

    def __str__(self) -> str:
        return "_MSG_TIMED_OUT"


def run(
    *,
    bootstrap: str,
    dlq_topic: str,
    selection: Selection,
    allowed: set[str],
    apply: bool,
) -> Counts:
    counts = Counts()
    records = read_dlq(bootstrap, dlq_topic, selection, counts)
    to_replay: list[Record] = []
    for record in records:
        counts.selected += 1
        refused = record.original_topic not in allowed
        if refused:
            counts.refused += 1
        else:
            to_replay.append(record)
        counts.lines.append(describe(record, refused=refused))
    if apply and to_replay:
        counts.replayed = replay(bootstrap, to_replay)
    return counts


def _kafka_name(exc: KafkaException) -> str:
    arg = exc.args[0] if exc.args else None
    name = getattr(arg, "name", None)
    return name() if callable(name) else type(exc).__name__


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--group", choices=sorted(DLQ_TOPICS), help="a consumer group (its DLQ topic)")
    where.add_argument("--dlq-topic", help="a DLQ topic by name")
    parser.add_argument("--error-type", default=None, help="only records with this dlq.error_type")
    parser.add_argument("--key", default=None, help="only records with this key")
    parser.add_argument("--event-id", default=None, help="only the record of this event id")
    parser.add_argument(
        "--limit", type=int, default=None, help=f"most records per run (default {DEFAULT_LIMIT})"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="no further filter; required with --apply when no --event-id/--key/--error-type "
        "is given; needs an explicit --limit",
    )
    parser.add_argument(
        "--include-poison",
        action="store_true",
        help="also select NonRetryableEventError and DeserializationError records",
    )
    parser.add_argument(
        "--allow-topic",
        action="append",
        default=[],
        help="also allow replaying to this topic (repeatable); catalogue topics are always allowed",
    )
    parser.add_argument("--apply", action="store_true", help="produce the records (default: dry run)")
    parser.add_argument("--bootstrap-servers", default=None, help="override the shared setting")
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.all and args.limit is None:
        parser.error("--all needs an explicit --limit")
    if args.apply and not (args.event_id or args.key is not None or args.error_type or args.all):
        parser.error(
            "--apply needs a selector: --event-id, --key, --error-type, or --all with an "
            "explicit --limit (run without --apply first to see what would be replayed)"
        )
    event_id = None
    if args.event_id is not None:
        event_id = _as_uuid(args.event_id)
        if event_id is None:
            parser.error("--event-id must be a UUID")

    selection = Selection(
        group=args.group,
        error_type=args.error_type,
        key=args.key.encode() if args.key is not None else None,
        event_id=event_id,
        limit=args.limit if args.limit is not None else DEFAULT_LIMIT,
        include_poison=args.include_poison,
    )
    dlq_topic = DLQ_TOPICS[args.group] if args.group else args.dlq_topic
    allowed = set(AGGREGATE_TOPICS.values()) | set(args.allow_topic)
    bootstrap = args.bootstrap_servers or shared_settings.kafka_bootstrap_servers

    try:
        counts = run(
            bootstrap=bootstrap,
            dlq_topic=dlq_topic,
            selection=selection,
            allowed=allowed,
            apply=args.apply,
        )
    except KafkaException as exc:
        print(f"replay failed: {_kafka_name(exc)}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - class name only: messages can carry URLs
        print(f"replay failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    for line in counts.lines:
        print(line)
    for error_type, count in sorted(counts.skipped_poison.items()):
        print(f"skipped_poison error_type={error_type} count={count}")
    print(counts.final(apply=args.apply))
    return 0


if __name__ == "__main__":
    sys.exit(main())
