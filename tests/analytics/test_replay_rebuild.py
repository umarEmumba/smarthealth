"""Replay rebuild of the analytics projections (plan 03-13 Task 3; D-10, T-3-59).

`python -m services.analytics.rebuild` truncates the projections and rewinds a consumer
group, so these tests never point it at the shared dev analytics_db (that would wipe the
live container's projections). Each test creates a THROW-AWAY database
(`analytics_rebuild_<hex>`, owned by `analytics_svc`) through the Postgres superuser in
the Compose container, runs the real Alembic migration on it, and drives the real
`EventConsumer` + `handle_event` and the real rebuild CLI (a subprocess with
`ANALYTICS_DATABASE_URL` pointing at that database) against it. Kafka topics and the
consumer group are unique per test, and the database is dropped at teardown.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from libs.db.postgres import get_session_factory
from libs.events.consumer import EventEnvelope
from services.analytics.config import settings
from services.analytics.consumer import CONSUMER_NAME, build_consumer, handle_event
from services.analytics.projections import PROJECTED_EVENT_TYPES
from services.analytics.rebuild import group_member_count
from tests.analytics.conftest import (  # noqa: F401 - re-exported fixtures
    Projector,
    dlq_topic,
    kafka_admin,
    mock_registry,
    produce_event,
    topic_factory,
    unique_group,
    unique_topic,
    wait_for,
)
from tests.analytics.test_projections import fixed_sequence
from tests.e2e.phase3_stack import REPO_ROOT, psql_superuser

pytestmark = pytest.mark.integration

KAFKA = "localhost:9092"
VIEWS = (
    ("v_active_patients", "clinic_id"),
    ("v_appointments_booked_daily", "clinic_id, day"),
    ("v_completed_visits_daily", "clinic_id, day"),
    ("v_cancellation", "clinic_id, day"),
    ("v_visit_wait", "clinic_id, completed_at, wait_seconds"),
)
TABLES = (
    ("patients", "patient_id"),
    ("appointments", "appointment_id"),
    ("visits_completed", "visit_id"),
)


# --- a throw-away analytics database ---------------------------------------------------


@pytest.fixture
def rebuild_db() -> Iterator[str]:
    """URL of a freshly migrated, throw-away database that `analytics_svc` owns."""
    name = f"analytics_rebuild_{uuid.uuid4().hex[:12]}"
    psql_superuser(f"CREATE DATABASE {name} OWNER analytics_svc")
    url = make_url(settings.database_url).set(database=name).render_as_string(hide_password=False)
    try:
        subprocess.run(  # noqa: S603 - fixed argv, no shell
            [sys.executable, "-m", "alembic", "-c", "services/analytics/migrations/alembic.ini",
             "upgrade", "head"],
            cwd=REPO_ROOT,
            env={**os.environ, "ANALYTICS_DATABASE_URL": url},
            check=True,
            capture_output=True,
            timeout=120,
        )
        yield url
    finally:
        psql_superuser(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


async def _sql(url: str, sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params or {})
            return list(result.all()) if result.returns_rows else []
    finally:
        await engine.dispose()


def query(url: str, sql: str, **params: Any) -> list[Any]:
    return asyncio.run(_sql(url, sql, params))


def claims(url: str) -> int:
    return int(query(url, "SELECT count(*) FROM processed_events")[0][0])


def snapshot(url: str) -> dict[str, list[tuple[Any, ...]]]:
    """Every view and every projection table, fully ordered, as plain tuples."""
    out: dict[str, list[tuple[Any, ...]]] = {}
    for name, order in (*VIEWS, *TABLES):
        out[name] = [tuple(row) for row in query(url, f"SELECT * FROM {name} ORDER BY {order}")]
    return out


# --- consumer and rebuild drivers --------------------------------------------------------


@contextmanager
def replay_consumer(url: str, topic: str, group: str, dlq: str) -> Iterator[threading.Thread]:
    """The real EventConsumer and handle_event, writing to the throw-away database."""
    stop = threading.Event()
    errors: list[BaseException] = []

    async def handler(envelope: EventEnvelope) -> None:
        await handle_event(envelope, session_factory=get_session_factory(url))

    def target() -> None:
        try:
            consumer = build_consumer(
                group_id=group,
                topics=[topic],
                dlq_topic=dlq,
                handlers={event_type: handler for event_type in PROJECTED_EVENT_TYPES},
                poll_timeout=0.5,
                backoff_seconds=0.05,
                heartbeat_path=None,
                metrics_port=None,
            )
            consumer.run(stop)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the test
            errors.append(exc)

    thread = threading.Thread(target=target, name="analytics-replay-consumer", daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        stop.set()
        thread.join(timeout=30)
    assert not thread.is_alive(), "analytics consumer did not stop"
    assert not errors, errors


def run_rebuild(url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, "-m", "services.analytics.rebuild", *args],
        cwd=REPO_ROOT,
        env={**os.environ, "ANALYTICS_DATABASE_URL": url},
        capture_output=True,
        text=True,
        timeout=120,
    )


def wait_until(check: Callable[[], bool], *, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.3)
    return check()


def members(group: str) -> int:
    return group_member_count(AdminClient({"bootstrap.servers": KAFKA}), group)


def committed(group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": KAFKA, "group.id": group, "enable.auto.commit": False})
    try:
        return int(probe.committed([TopicPartition(topic, 0)], timeout=10)[0].offset)
    finally:
        probe.close()


def produce_all(
    produce_event: Callable[..., dict[str, Any]], topic: str, envelopes: list[EventEnvelope]
) -> None:
    for envelope in envelopes:
        produce_event(
            topic,
            envelope.event_type,
            envelope.payload,
            key=envelope.aggregate_id,
            occurred_at=envelope.occurred_at,
        )


# --- tests -----------------------------------------------------------------------------


def test_rebuild_by_replay_reproduces_every_view(
    rebuild_db: str,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    clinic = uuid.uuid4()
    envelopes = fixed_sequence(Projector(clinic))
    produce_all(produce_event, unique_topic, envelopes)

    with replay_consumer(rebuild_db, unique_topic, unique_group, dlq_topic):
        assert wait_for(lambda: claims(rebuild_db), len(envelopes), timeout=60) == len(envelopes)
    before = snapshot(rebuild_db)
    assert before["v_active_patients"] and before["v_visit_wait"], "the sequence produced no metrics"
    assert wait_until(lambda: members(unique_group) == 0), "consumer did not leave the group"

    dry = run_rebuild(rebuild_db, "--group", unique_group, "--topics", unique_topic)
    assert dry.returncode == 0, dry.stderr
    assert "would delete" in dry.stdout and "dry run" in dry.stdout
    assert snapshot(rebuild_db) == before and claims(rebuild_db) == len(envelopes)

    applied = run_rebuild(rebuild_db, "--apply", "--group", unique_group, "--topics", unique_topic)
    assert applied.returncode == 0, applied.stderr
    assert all(not rows for name, rows in snapshot(rebuild_db).items() if name in dict(TABLES))
    assert claims(rebuild_db) == 0
    assert committed(unique_group, unique_topic) == 0  # the low watermark

    with replay_consumer(rebuild_db, unique_topic, unique_group, dlq_topic):
        assert wait_for(lambda: claims(rebuild_db), len(envelopes), timeout=60) == len(envelopes)
    assert snapshot(rebuild_db) == before
    assert str(clinic) in {str(row[0]) for row in before["v_active_patients"]}


def test_rebuild_refuses_while_the_group_has_an_active_member(
    rebuild_db: str, unique_topic: str, dlq_topic: str, unique_group: str
) -> None:
    marker = uuid.uuid4()
    query(
        rebuild_db,
        "INSERT INTO patients (patient_id, clinic_id, registered_at) VALUES (:p, :c, now())",
        p=marker,
        c=uuid.uuid4(),
    )
    with replay_consumer(rebuild_db, unique_topic, unique_group, dlq_topic):
        assert wait_until(lambda: members(unique_group) >= 1, timeout=45), "consumer never joined"
        refused = run_rebuild(
            rebuild_db, "--apply", "--group", unique_group, "--topics", unique_topic
        )
        assert refused.returncode == 1
        assert "refusing" in refused.stderr
    # Nothing was truncated and no offset was set.
    assert query(rebuild_db, "SELECT count(*) FROM patients WHERE patient_id = :p", p=marker)[0][0] == 1
    assert committed(unique_group, unique_topic) < 0  # no offset was reset to the low watermark


def test_rebuild_without_apply_changes_nothing(rebuild_db: str, unique_topic: str, unique_group: str) -> None:
    query(
        rebuild_db,
        "INSERT INTO patients (patient_id, clinic_id, registered_at) VALUES (:p, :c, now())",
        p=uuid.uuid4(),
        c=uuid.uuid4(),
    )
    query(
        rebuild_db,
        "INSERT INTO processed_events (event_id, consumer) VALUES (:e, :c)",
        e=uuid.uuid4(),
        c=CONSUMER_NAME,
    )
    before = snapshot(rebuild_db)
    result = run_rebuild(rebuild_db, "--group", unique_group, "--topics", unique_topic)
    assert result.returncode == 0, result.stderr
    assert "would delete 1 row(s) from patients" in result.stdout
    assert "would delete 1 claim(s)" in result.stdout
    assert snapshot(rebuild_db) == before and claims(rebuild_db) == 1


def test_rebuild_help_exits_zero() -> None:
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, "-m", "services.analytics.rebuild", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0
    assert "--apply" in result.stdout and "--group" in result.stdout and "--topics" in result.stdout
