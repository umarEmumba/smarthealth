"""Fixtures for the analytics projection tests (plan 03-10).

The live Kafka fixtures (`unique_topic`, `dlq_topic`, `unique_group`, `produce_event`,
`mock_registry`) come from `tests/events/conftest.py`; they are imported here so the
analytics tests see them without a `pytest_plugins` entry. Every test asserts on a
UNIQUE clinic id (research Pitfall 12), so rows other tests or the live stack leave in
analytics_db can never change a number, and nothing a test writes needs deleting for
another test to be correct (each test still removes its own rows at teardown).
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from services.analytics.config import settings
from services.analytics.consumer import build_consumer
from tests.events.conftest import (  # noqa: F401 - re-exported fixtures
    dlq_topic,
    kafka_admin,
    mock_registry,
    produce_event,
    topic_factory,
    unique_group,
    unique_topic,
)


async def _run_sql(sql: str, params: dict[str, Any], *, write: bool = False) -> Any:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params)
            return None if write else result.all()
    finally:
        await engine.dispose()


def query(sql: str, **params: Any) -> list[Any]:
    """Run a read-only statement on analytics_db from a SYNC test (own loop and engine)."""
    return asyncio.run(_run_sql(sql, params))


@pytest.fixture
def clinic_id() -> Iterator[uuid.UUID]:
    """A unique clinic id; the projection rows written for it are removed afterwards
    (test-data hygiene only: no other test's numbers depend on them)."""
    clinic = uuid.uuid4()
    yield clinic
    for table in ("patients", "appointments", "visits_completed"):
        asyncio.run(_run_sql(f"DELETE FROM {table} WHERE clinic_id = :c", {"c": clinic}, write=True))


@contextmanager
def running_consumer(topic: str, group: str, dlq: str) -> Iterator[threading.Thread]:
    """Run the REAL analytics consumer (real `handle_event`) on one test topic in a thread."""
    stop = threading.Event()
    errors: list[BaseException] = []

    def target() -> None:
        try:
            consumer = build_consumer(
                group_id=group,
                topics=[topic],
                dlq_topic=dlq,
                poll_timeout=0.5,
                backoff_seconds=0.05,
                heartbeat_path=None,
                metrics_port=None,
            )
            consumer.run(stop)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the test
            errors.append(exc)

    thread = threading.Thread(target=target, name="analytics-test-consumer", daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        stop.set()
        thread.join(timeout=20)
    assert not thread.is_alive(), "analytics consumer did not stop"
    assert not errors, errors


def wait_for(read: Callable[[], Any], expected: Any, *, timeout: float = 30.0) -> Any:
    """Poll `read()` until it equals `expected` (or the timeout); returns the last value."""
    import time

    deadline = time.monotonic() + timeout
    value = read()
    while value != expected and time.monotonic() < deadline:
        time.sleep(0.3)
        value = read()
    return value
