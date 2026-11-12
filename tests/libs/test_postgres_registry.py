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
import re
import threading
import time
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
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


# --- Pool settings ----------------------------------------------------------------------


def test_pool_size_and_overflow_come_from_settings(
    isolated_registry: dict[Any, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from libs.config import shared_settings

    monkeypatch.setattr(shared_settings, "db_pool_size", 2)
    monkeypatch.setattr(shared_settings, "db_max_overflow", 1)

    async def build() -> Any:
        return postgres.get_engine(OFFLINE_URL)

    engine = asyncio.run(build())
    pool = engine.sync_engine.pool
    assert pool.size() == 2
    assert pool._max_overflow == 1  # QueuePool keeps no public accessor for the bound


def test_shared_settings_code_defaults_are_sqlalchemys(monkeypatch: pytest.MonkeyPatch) -> None:
    from libs.config import SharedSettings

    monkeypatch.delenv("DB_POOL_SIZE", raising=False)
    monkeypatch.delenv("DB_MAX_OVERFLOW", raising=False)
    fresh = SharedSettings(_env_file=None)
    assert (fresh.db_pool_size, fresh.db_max_overflow) == (5, 10)
    monkeypatch.setenv("DB_POOL_SIZE", "3")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "4")
    from_env = SharedSettings(_env_file=None)
    assert (from_env.db_pool_size, from_env.db_max_overflow) == (3, 4)


# --- Compose connection budget (W8: every term derived from the repository) -------------

REPO = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO / "infra" / "docker-compose.yml"
CONNECTORS_DIR = REPO / "infra" / "connect" / "connectors"
GRAFANA_DATASOURCES = REPO / "infra" / "grafana" / "provisioning" / "datasources" / "datasources.yml"

# Debezium holds a replication stream plus the connector's regular JDBC connection:
# 2 per connector file. Assumed, not measured per connector; the phase-gate sample
# showed exactly 2 `Debezium General` backends per source database.
DEBEZIUM_CONNECTIONS_PER_CONNECTOR = 2
# Temporal: the auto-setup config template (/etc/temporal/config/config_template.yaml,
# read from the running image in plan 03-23) caps the default store at
# SQL_MAX_CONNS (default 20) and the visibility store at SQL_VIS_MAX_CONNS (default 10)
# per pool. Compose sets neither, so the figure is NOT derivable from the repository;
# 40 is the idle total observed at the phase gate (29 observed in plan 03-23), which
# already exceeds one 20 + 10 pool pair, i.e. Temporal's services pool separately.
TEMPORAL_TEMPLATE_DEFAULT_STORE = 20
TEMPORAL_TEMPLATE_VISIBILITY_STORE = 10
TEMPORAL_OBSERVED_FALLBACK = 40
# Postgres default (`SHOW superuser_reserved_connections` = 3 on the dev server) unless
# the postgres service's command sets it.
POSTGRES_DEFAULT_RESERVED = 3

_GUNICORN_WORKERS = re.compile(r"-w\s+\$\{(?P<var>[A-Za-z_][A-Za-z0-9_]*):-(?P<default>\d+)\}")
_BUDGET_COMMENT = re.compile(r"Worst-case connection total:\s*(\d+) of (\d+)")


def _env_of(service: dict[str, Any]) -> dict[str, str]:
    env = service.get("environment") or {}
    if isinstance(env, list):
        pairs = (item.partition("=") for item in env)
        return {key: value for key, _, value in pairs}
    return {str(key): str(value) for key, value in env.items()}


def gunicorn_workers_for(service: dict[str, Any], dockerfile_text: str) -> int:
    """Gunicorn worker count of an API service: Compose override, else the Dockerfile CMD default."""
    match = _GUNICORN_WORKERS.search(dockerfile_text)
    if match is None:
        raise AssertionError("no `-w ${VAR:-N}` worker default in the Dockerfile CMD")
    override = _env_of(service).get(match.group("var"))
    return int(override) if override is not None else int(match.group("default"))


def pool_connections_for(service_name: str, service: dict[str, Any]) -> int:
    env = _env_of(service)
    for key in ("DB_POOL_SIZE", "DB_MAX_OVERFLOW"):
        if not env.get(key, "").isdigit():
            raise AssertionError(f"{service_name}: {key} is missing or not a plain integer")
    return int(env["DB_POOL_SIZE"]) + int(env["DB_MAX_OVERFLOW"])


def test_gunicorn_worker_default_is_parsed_from_the_dockerfile_cmd() -> None:
    cmd = 'CMD ["sh", "-c", "exec gunicorn -k uvicorn.workers.UvicornWorker -w ${X:-3} -b 0.0.0.0:8000 a:b"]'
    assert gunicorn_workers_for({"environment": {}}, cmd) == 3
    assert gunicorn_workers_for({"environment": {"X": "4"}}, cmd) == 4
    assert gunicorn_workers_for({"environment": ["X=5"]}, cmd) == 5
    with pytest.raises(AssertionError, match="worker default"):
        gunicorn_workers_for({}, 'CMD ["python", "-m", "svc"]')


def test_a_missing_or_templated_pool_value_fails_the_derivation() -> None:
    assert pool_connections_for("a", {"environment": {"DB_POOL_SIZE": "2", "DB_MAX_OVERFLOW": "3"}}) == 5
    with pytest.raises(AssertionError, match="DB_MAX_OVERFLOW"):
        pool_connections_for("a", {"environment": {"DB_POOL_SIZE": "2"}})
    with pytest.raises(AssertionError, match="DB_POOL_SIZE"):
        pool_connections_for("a", {"environment": {"DB_POOL_SIZE": "${X:-2}", "DB_MAX_OVERFLOW": "3"}})


def compose_connection_budget() -> dict[str, Any]:
    """Every term of the worst-case Postgres connection budget, derived from the repository."""
    compose = yaml.safe_load(COMPOSE_FILE.read_text())  # merge keys (<<) are resolved
    services: dict[str, dict[str, Any]] = compose["services"]

    # (1) DB-using services: a `*_DATABASE_URL` in the resolved environment, minus the
    # one-shot jobs other services wait on with `service_completed_successfully`.
    one_shot = {
        dependency
        for service in services.values()
        for dependency, spec in (service.get("depends_on") or {}).items()
        if isinstance(spec, dict) and spec.get("condition") == "service_completed_successfully"
    }
    assert "db-init" in one_shot, "one-shot job detection no longer finds db-init"
    db_services = {
        name: service
        for name, service in services.items()
        if name not in one_shot and any(key.endswith("_DATABASE_URL") for key in _env_of(service))
    }
    assert db_services, "no DB-using service found in Compose"

    terms: dict[str, int] = {}
    detail: dict[str, str] = {}
    for name, service in sorted(db_services.items()):
        pool = pool_connections_for(name, service)  # (2)
        if "command" in service:  # worker / consumer: one process
            processes, kind = 1, "process"
        else:  # (3) API: gunicorn workers from the Dockerfile CMD default
            build = service["build"]
            dockerfile = (COMPOSE_FILE.parent / build["context"] / build["dockerfile"]).resolve()
            processes, kind = gunicorn_workers_for(service, dockerfile.read_text()), "workers"
        terms[name] = processes * pool
        detail[name] = f"{processes} {kind} x pool {pool}"

    # (4) Debezium
    connectors = sorted(CONNECTORS_DIR.glob("*.json"))
    assert connectors, "no Debezium connector files found"
    terms["debezium"] = DEBEZIUM_CONNECTIONS_PER_CONNECTOR * len(connectors)
    detail["debezium"] = f"{len(connectors)} connectors x {DEBEZIUM_CONNECTIONS_PER_CONNECTOR}"

    # (5) Temporal
    temporal_env = _env_of(services["temporal"])
    if "SQL_MAX_CONNS" in temporal_env:
        visibility = int(temporal_env.get("SQL_VIS_MAX_CONNS", TEMPORAL_TEMPLATE_VISIBILITY_STORE))
        terms["temporal"] = int(temporal_env["SQL_MAX_CONNS"]) + visibility
        detail["temporal"] = "SQL_MAX_CONNS + SQL_VIS_MAX_CONNS from Compose"
    else:
        terms["temporal"] = TEMPORAL_OBSERVED_FALLBACK
        detail["temporal"] = (
            f"observed fallback (template caps {TEMPORAL_TEMPLATE_DEFAULT_STORE} + "
            f"{TEMPORAL_TEMPLATE_VISIBILITY_STORE} per pool, none set in Compose)"
        )

    # (6) Grafana
    datasources = yaml.safe_load(GRAFANA_DATASOURCES.read_text())["datasources"]
    postgres_sources = [d for d in datasources if d.get("type") == "grafana-postgresql-datasource"]
    assert postgres_sources, "no Grafana Postgres datasource found"
    for source in postgres_sources:
        assert "maxOpenConns" in source.get("jsonData", {}), f"{source['name']}: no maxOpenConns"
    terms["grafana"] = sum(int(d["jsonData"]["maxOpenConns"]) for d in postgres_sources)
    detail["grafana"] = f"{len(postgres_sources)} datasource(s) x maxOpenConns"

    # (7) the ceiling
    command = [str(part) for part in services["postgres"]["command"]]
    settings = dict(part.split("=", 1) for part in command if "=" in part and "_" in part.split("=", 1)[0])
    max_connections = int(settings["max_connections"])
    reserved = int(settings.get("superuser_reserved_connections", POSTGRES_DEFAULT_RESERVED))
    return {
        "terms": terms,
        "detail": detail,
        "total": sum(terms.values()),
        "ceiling": max_connections - reserved,
        "max_connections": max_connections,
        "reserved": reserved,
    }


def test_compose_connection_budget(capsys: pytest.CaptureFixture[str]) -> None:
    budget = compose_connection_budget()
    with capsys.disabled():
        print("\nPostgres connection budget (worst case, derived):")
        for name, value in budget["terms"].items():
            print(f"  {name:<22} {value:>4}   ({budget['detail'][name]})")
        print(
            f"  {'TOTAL':<22} {budget['total']:>4}   of {budget['ceiling']} "
            f"(max_connections {budget['max_connections']} - {budget['reserved']} reserved)"
        )

    assert budget["total"] <= budget["ceiling"], (
        f"worst-case connections {budget['total']} exceed {budget['ceiling']}"
    )
    stated = _BUDGET_COMMENT.search(COMPOSE_FILE.read_text())
    assert stated is not None, "the Compose budget comment ('Worst-case connection total: N of M') is missing"
    assert (int(stated.group(1)), int(stated.group(2))) == (budget["total"], budget["ceiling"]), (
        f"Compose comment states {stated.group(0)!r}, derived {budget['total']} of {budget['ceiling']}"
    )
