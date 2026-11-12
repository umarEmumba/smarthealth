"""`scripts/replay_dlq.py` against the live broker (plan 03-22, W4).

Throw-away topics (`p3test.*`, deleted by the fixtures) stand in for a dead-letter topic and
for the original topic; the script is imported in-process and driven through `main(argv)`
with stdout captured. Records are shaped like the consumer base's DLQ records: the original
headers (`id`, `eventType`, `traceparent`) followed by the `dlq.*` headers.
"""

from __future__ import annotations

import importlib.util
import sys
import time
import uuid
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from libs.events.catalog import DLQ_TOPICS
from tests.events.conftest import KAFKA_BOOTSTRAP

pytestmark = pytest.mark.integration

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "replay_dlq.py"
MARKER = "PAYLOAD-MARKER-9f3c"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("replay_dlq", SCRIPT)
    assert spec and spec.loader
    module = sys.modules.get(spec.name)
    if module is None:  # the script's dataclasses resolve their module through sys.modules
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


replay_dlq = _load()


class Dead:
    """One dead-lettered record written to the throw-away DLQ topic."""

    def __init__(self, source: str, *, error_type: str, group: str = "visit-downstream") -> None:
        self.event_id = str(uuid.uuid4())
        self.key = f"key-{uuid.uuid4().hex}".encode()
        self.value = f"{MARKER}-{uuid.uuid4().hex}".encode()
        self.original_headers: list[tuple[str, bytes]] = [
            ("id", self.event_id.encode()),
            ("eventType", b"appointment.booked"),
            ("traceparent", b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        ]
        self.dlq_headers: list[tuple[str, bytes]] = [
            ("dlq.error_type", error_type.encode()),
            ("dlq.consumer_group", group.encode()),
            ("dlq.original_topic", source.encode()),
            ("dlq.original_partition", b"0"),
            ("dlq.original_offset", b"7"),
            ("dlq.attempts", b"10"),
        ]


def _produce(topic: str, records: list[Dead]) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
    for record in records:
        producer.produce(
            topic,
            key=record.key,
            value=record.value,
            headers=record.original_headers + record.dlq_headers,
        )
    assert producer.flush(30) == 0


def _read_all(topic: str) -> list[Any]:
    """Every record of the (single-partition) topic, read without a group (assign)."""
    reader = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"p3test-reader-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        low, high = reader.get_watermark_offsets(TopicPartition(topic, 0), timeout=15)
        if high <= low:
            return []
        reader.assign([TopicPartition(topic, 0, low)])
        out: list[Any] = []
        deadline = time.monotonic() + 20
        while len(out) < high - low and time.monotonic() < deadline:
            message = reader.poll(0.5)
            if message is not None and message.error() is None:
                out.append(message)
        return out
    finally:
        reader.close()


@pytest.fixture
def setup(
    unique_topic: str, dlq_topic: str
) -> tuple[str, str, list[Dead]]:
    """(source topic, dlq topic, [transient 1, transient 2, handler error])."""
    records = [
        Dead(unique_topic, error_type="TransientExhausted"),
        Dead(unique_topic, error_type="TransientExhausted"),
        Dead(unique_topic, error_type="HandlerError"),
    ]
    _produce(dlq_topic, records)
    return unique_topic, dlq_topic, records


def _run(
    capsys: pytest.CaptureFixture[str], dlq_topic: str, *args: str
) -> tuple[int, list[str], str]:
    capsys.readouterr()
    code = replay_dlq.main(
        ["--dlq-topic", dlq_topic, "--bootstrap-servers", KAFKA_BOOTSTRAP, *args]
    )
    captured = capsys.readouterr()
    return code, captured.out.strip().splitlines(), captured.out + captured.err


def test_the_default_run_is_a_dry_run_that_produces_nothing(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup

    code, lines, _ = _run(
        capsys, dlq, "--error-type", "TransientExhausted", "--allow-topic", source
    )

    assert code == 0
    assert lines[-1] == "mode=dry-run scanned=3 selected=2 replayed=0 refused=0"
    assert len(lines) == 3  # two selected records and the counts line
    for line, record in zip(lines[:2], records[:2], strict=True):
        assert f"event_id={record.event_id}" in line
        assert f"original={source}:0:7 error_type=TransientExhausted" in line
    assert _read_all(source) == []  # nothing was produced to the original topic


def test_apply_replays_the_original_key_value_and_headers_without_dlq_headers(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup
    first = records[0]

    code, lines, _ = _run(
        capsys,
        dlq,
        "--apply",
        "--error-type",
        "TransientExhausted",
        "--event-id",
        first.event_id,
        "--allow-topic",
        source,
    )

    assert code == 0
    assert lines[-1] == "mode=apply scanned=3 selected=1 replayed=1 refused=0"
    (replayed,) = _read_all(source)
    assert replayed.key() == first.key
    assert replayed.value() == first.value
    assert replayed.headers() == first.original_headers  # same names, values and order
    assert not any(name.startswith("dlq.") for name, _ in replayed.headers())
    # The event id and traceparent are the original ones.
    assert dict(replayed.headers())["id"] == first.event_id.encode()
    assert dict(replayed.headers())["traceparent"] == dict(first.original_headers)["traceparent"]


def test_key_selects_the_record(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup
    target = records[2]

    code, lines, _ = _run(
        capsys, dlq, "--apply", "--key", target.key.decode(), "--allow-topic", source
    )

    assert code == 0
    assert lines[-1] == "mode=apply scanned=3 selected=1 replayed=1 refused=0"
    (replayed,) = _read_all(source)
    assert replayed.key() == target.key and replayed.value() == target.value


def test_limit_bounds_the_replay(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup

    code, lines, _ = _run(
        capsys,
        dlq,
        "--apply",
        "--error-type",
        "TransientExhausted",
        "--limit",
        "1",
        "--allow-topic",
        source,
    )

    assert code == 0
    assert lines[-1].startswith("mode=apply ") and "selected=1 replayed=1 refused=0" in lines[-1]
    (replayed,) = _read_all(source)
    assert replayed.key() == records[0].key  # the oldest match, in DLQ order


def test_a_target_that_is_neither_catalogue_nor_allowed_is_refused(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup

    code, lines, _ = _run(
        capsys, dlq, "--apply", "--event-id", records[0].event_id  # no --allow-topic
    )

    assert code == 0
    assert lines[-1] == "mode=apply scanned=3 selected=1 replayed=0 refused=1"
    assert lines[0].endswith(" refused")
    assert _read_all(source) == []  # nothing produced for a refused target


def test_a_catalogue_topic_is_an_allowed_target_without_the_flag(
    dlq_topic: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The dry run reports a forged-but-catalogue target as not refused (a catalogue topic
    is always allowed) and an arbitrary one as refused: only the dry run is exercised, so
    no real event topic is ever written to."""
    catalogue = Dead("smarthealth.visit.events", error_type="TransientExhausted")
    forged = Dead("not-a-catalogue-topic", error_type="TransientExhausted")
    _produce(dlq_topic, [catalogue, forged])

    code, lines, _ = _run(capsys, dlq_topic)

    assert code == 0
    assert lines[-1] == "mode=dry-run scanned=2 selected=2 replayed=0 refused=1"
    assert not lines[0].endswith(" refused") and lines[1].endswith(" refused")


def test_the_script_never_changes_a_consumer_groups_offsets(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str], unique_group: str
) -> None:
    source, dlq, records = setup
    group = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": unique_group,
            "enable.auto.commit": False,
        }
    )
    try:
        group.commit(offsets=[TopicPartition(dlq, 0, 1)], asynchronous=False)

        code, _, _ = _run(
            capsys, dlq, "--apply", "--error-type", "TransientExhausted", "--allow-topic", source
        )

        assert code == 0
        (committed,) = group.committed([TopicPartition(dlq, 0)], timeout=15)
        assert committed.offset == 1  # exactly what the test committed
    finally:
        group.close()


def test_neither_a_payload_nor_a_key_reaches_the_output(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = setup

    for extra in ((), ("--apply", "--all", "--limit", "10", "--allow-topic", source)):
        code, _, everything = _run(capsys, dlq, *extra)
        assert code == 0
        assert MARKER not in everything
        for record in records:
            assert record.key.decode() not in everything
            assert record.value.decode() not in everything
            assert "00-0af7651916cd43dd8448eb211c80319c" not in everything  # traceparent


def test_group_and_missing_original_topic_filters() -> None:
    """`--group` requires the record's dlq.consumer_group to match; a record without
    dlq.original_topic is never selected."""
    record = Dead("smarthealth.visit.events", error_type="HandlerError", group="visit-downstream")

    def as_record(dead: Dead, headers: list[tuple[str, bytes | None]]) -> Any:
        return replay_dlq.Record(0, 0, dead.key, dead.value, headers)

    full = as_record(record, record.original_headers + record.dlq_headers)  # type: ignore[arg-type]
    no_topic = as_record(
        record,
        [h for h in record.original_headers + record.dlq_headers if h[0] != "dlq.original_topic"],  # type: ignore[misc]
    )
    sel = replay_dlq.Selection(None, None, None, None, 100)
    assert replay_dlq.matches(full, sel)
    assert not replay_dlq.matches(no_topic, sel)
    assert replay_dlq.matches(full, replay_dlq.Selection("visit-downstream", None, None, None, 100))
    assert not replay_dlq.matches(full, replay_dlq.Selection("analytics-projection", None, None, None, 100))


def test_arguments_are_validated(capsys: pytest.CaptureFixture[str]) -> None:
    for argv in (
        [],  # one of --group / --dlq-topic is required
        ["--group", "visit-downstream", "--dlq-topic", "x"],
        ["--group", "no-such-group"],
        ["--dlq-topic", "x", "--limit", "0"],
        ["--dlq-topic", "x", "--event-id", "not-a-uuid"],
        ["--dlq-topic", "x", "--apply"],
        ["--group", "visit-downstream", "--apply"],
        ["--dlq-topic", "x", "--apply", "--all"],  # --all needs an explicit --limit
        ["--dlq-topic", "x", "--all"],
        ["--dlq-topic", "x", "--apply", "--include-poison"],  # not a selector
    ):
        with pytest.raises(SystemExit) as exc_info:
            replay_dlq.main(argv)
        assert exc_info.value.code == 2, argv
    assert set(DLQ_TOPICS) == {"visit-downstream", "notification-trigger", "analytics-projection"}
    capsys.readouterr()


def test_a_missing_dlq_topic_is_an_empty_one(capsys: pytest.CaptureFixture[str]) -> None:
    code, lines, _ = _run(capsys, f"p3test.dlq.never-created.{uuid.uuid4().hex}")

    assert code == 0
    assert lines == ["mode=dry-run scanned=0 selected=0 replayed=0 refused=0"]


def test_apply_without_a_selector_is_refused_before_anything_is_produced(
    setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, _ = setup
    capsys.readouterr()

    with pytest.raises(SystemExit) as exc_info:
        replay_dlq.main(
            ["--dlq-topic", dlq, "--bootstrap-servers", KAFKA_BOOTSTRAP, "--apply", "--allow-topic", source]
        )

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    for name in ("--event-id", "--key", "--error-type", "--all"):
        assert name in err
    assert _read_all(source) == []


@pytest.fixture
def poison_setup(unique_topic: str, dlq_topic: str) -> tuple[str, str, list[Dead]]:
    """The live head-of-queue shape: poison first, then a replayable record."""
    records = [
        Dead(unique_topic, error_type="NonRetryableEventError"),
        Dead(unique_topic, error_type="NonRetryableEventError"),
        Dead(unique_topic, error_type="DeserializationError"),
        Dead(unique_topic, error_type="TransientExhausted"),
    ]
    _produce(dlq_topic, records)
    return unique_topic, dlq_topic, records


def test_poison_is_skipped_by_default_and_counted(
    poison_setup: tuple[str, str, list[Dead]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, dlq, records = poison_setup

    code, lines, _ = _run(capsys, dlq, "--limit", "1", "--allow-topic", source)

    assert code == 0
    assert f"event_id={records[3].event_id}" in lines[0]
    assert "skipped_poison error_type=NonRetryableEventError count=2" in lines
    assert "skipped_poison error_type=DeserializationError count=1" in lines
    assert lines[-1] == "mode=dry-run scanned=4 selected=1 replayed=0 refused=0"

    _, lines, _ = _run(capsys, dlq, "--include-poison", "--allow-topic", source)
    assert lines[-1] == "mode=dry-run scanned=4 selected=4 replayed=0 refused=0"
    _, lines, _ = _run(capsys, dlq, "--error-type", "NonRetryableEventError", "--allow-topic", source)
    assert lines[-1] == "mode=dry-run scanned=4 selected=2 replayed=0 refused=0"


def test_all_with_an_explicit_limit_applies_every_non_poison_record(
    unique_topic: str, dlq_topic: str, capsys: pytest.CaptureFixture[str]
) -> None:
    records = [
        Dead(unique_topic, error_type="NonRetryableEventError"),
        Dead(unique_topic, error_type="TransientExhausted"),
        Dead(unique_topic, error_type="HandlerError"),
    ]
    _produce(dlq_topic, records)

    code, lines, _ = _run(capsys, dlq_topic, "--apply", "--all", "--limit", "10", "--allow-topic", unique_topic)

    assert code == 0
    assert lines[-1] == "mode=apply scanned=3 selected=2 replayed=2 refused=0"
    assert [m.key() for m in _read_all(unique_topic)] == [records[1].key, records[2].key]


def test_the_poison_types_match_the_consumer_base() -> None:
    from libs.events import consumer  # noqa: PLC0415

    assert replay_dlq.POISON_ERROR_TYPES == {
        consumer.DESERIALIZATION_ERROR,
        consumer.NonRetryableEventError.__name__,
    }
    doc = replay_dlq.__doc__ or ""
    assert "WR-06" in doc and "notification-trigger" in doc
    assert "is unaffected" not in doc
