"""The async engine registry releases a closed loop's connections (plan 03-23, WR-05).

`libs/db/postgres.py` keeps one engine per (database URL, event loop) because asyncpg
binds pooled connections to the loop that opened them. Before plan 03-23 a changed loop
replaced the entry without disposing the old engine, so every per-test loop left a pool of
live server connections behind and the test process crept toward the dev Postgres limit
(304 of 300 at the phase gate). These tests pin the repaired behaviour against the live
`visit_db` (read-only `SELECT`s) and, where no connection is needed, against a URL nothing
listens on (engine creation is lazy).

The tests are plain `def` tests: each drives its own event loops, which is the point.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Coroutine, Iterator
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from libs.db import postgres
from services.visit.config import VisitSettings

VISIT_URL = VisitSettings().database_url
# Nothing listens here: engines are created lazily, so registry bookkeeping can be
# asserted without a connection.
OFFLINE_URL = "postgresql+asyncpg://nobody:nothing@127.0.0.1:1/nowhere"


@pytest.fixture
def isolated_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[Any, Any]]:
    """An empty registry for the test; the module's own registry is restored afterwards."""
    fresh: dict[Any, Any] = {}
    monkeypatch.setattr(postgres, "_registry", fresh)
    yield fresh


@pytest.fixture
def counted_engines(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record the URL of every engine the registry builds."""
    created: list[str] = []
    real = postgres.create_async_engine

    def counting(url: str, **kwargs: Any) -> Any:
        created.append(url)
        return real(url, **kwargs)

    monkeypatch.setattr(postgres, "create_async_engine", counting)
    return created


async def _backend_pid_through_registry(url: str) -> int:
    async with postgres.get_session_factory(url)() as session:
        return int((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())


async def _alive_pids(pids: list[int]) -> set[int]:
    """Which of `pids` are still backends, asked over a throw-away NullPool engine."""
    engine = create_async_engine(VISIT_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text("SELECT pid FROM pg_stat_activity WHERE pid = ANY(:pids)"),
                {"pids": pids},
            )
            return {int(row[0]) for row in rows}
    finally:
        await engine.dispose()


def _alive_after_grace(pids: list[int], *, at_most: int, seconds: float = 5.0) -> set[int]:
    """Poll until at most `at_most` of `pids` remain (a closed socket ends a backend a moment later)."""
    deadline = time.monotonic() + seconds
    alive = asyncio.run(_alive_pids(pids))
    while len(alive) > at_most and time.monotonic() < deadline:
        time.sleep(0.2)
        alive = asyncio.run(_alive_pids(pids))
    return alive


class _LoopThread:
    """A long-lived event loop in its own thread, like the consumer's persistent Runner."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def call(self, coro: Coroutine[Any, Any, Any]) -> Any:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout=30)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=10)
        self.loop.close()


def test_sequential_loops_leave_at_most_one_backend_alive(
    isolated_registry: dict[Any, Any],
) -> None:
    pids: list[int] = []
    for _ in range(8):
        pids.append(asyncio.run(_backend_pid_through_registry(VISIT_URL)))
    assert len(set(pids)) == len(pids), "each loop should have used its own connection"

    alive = _alive_after_grace(pids, at_most=1)

    assert len(alive) <= 1, f"{len(alive)} of {len(pids)} backends of closed loops survive: {sorted(alive)}"
    # the survivor can only be the last loop's: its entry is retired by the next call
    assert alive <= {pids[-1]}


def test_two_live_loops_alternate_on_one_engine_each(
    isolated_registry: dict[Any, Any], counted_engines: list[str]
) -> None:
    other = _LoopThread()
    try:
        with asyncio.Runner() as runner:
            for _ in range(3):
                assert other.call(_backend_pid_through_registry(VISIT_URL)) > 0
                assert runner.run(_backend_pid_through_registry(VISIT_URL)) > 0
            assert counted_engines.count(VISIT_URL) <= 2, counted_engines
            # both live loops still own an entry; neither was torn down from the other
            assert len(isolated_registry) == 2
    finally:
        other.stop()


def test_closed_loop_entries_are_retired_on_the_next_call_for_any_url(
    isolated_registry: dict[Any, Any],
) -> None:
    async def build(url: str) -> asyncio.AbstractEventLoop:
        postgres.get_session_factory(url)
        return asyncio.get_running_loop()

    closed_loop = asyncio.run(build(OFFLINE_URL))
    assert closed_loop.is_closed()
    assert len(isolated_registry) == 1

    other = _LoopThread()
    try:
        other.call(build(VISIT_URL))  # a different URL, a different (live) loop
        assert [entry.loop for entry in isolated_registry.values()] == [other.loop]

        # an entry whose loop is alive survives calls made from any other loop
        asyncio.run(build(OFFLINE_URL))
        assert other.loop in [entry.loop for entry in isolated_registry.values()]
    finally:
        other.stop()


def test_one_long_lived_loop_gets_the_same_engine(isolated_registry: dict[Any, Any]) -> None:
    async def twice() -> tuple[Any, Any, Any]:
        return (
            postgres.get_engine(OFFLINE_URL),
            postgres.get_engine(OFFLINE_URL),
            postgres.get_session_factory(OFFLINE_URL),
        )

    first, second, factory = asyncio.run(twice())
    assert first is second
    assert factory.kw["bind"] is first
    assert len(isolated_registry) == 1
