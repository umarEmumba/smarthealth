"""Helpers for the Phase 3 real-stack suites (plan 03-05, reused by later plans).

Everything here drives the deployed Compose stack the way an operator would:
`docker compose -f infra/docker-compose.yml ...` from the repository root (project
`smarthealth`), the Connect REST API through `compose exec connect curl` (its port
is deliberately not published to the host), Kafka through the published broker
port and the Schema Registry through its published port.

Safety rules baked into the helpers:

* every Compose call passes the same fixed `-f` file and a fixed argv (never a
  shell string), so only the `smarthealth` project can be touched;
* nothing here ever removes a volume or stops anything but a named Compose service;
* secrets are never printed: callers read the Debezium password through
  `debezium_password()` and hand it straight to the driver.

Public helpers:

* `compose(*args)`, `psql_superuser(sql, db)` - subprocess wrappers.
* `connector_names()`, `connector_config(name)` - from the JSON files present, so
  tests parametrized over them automatically cover connectors later plans add.
* `connector_status(name)`, `wait_connectors_running(names=None)`,
  `register_connector(name)`, `put_connector(name, config)`, `delete_connector(name)`.
* `kill_service`, `stop_service`, `start_service`, `wait_service_healthy`.
* `live_registry()`, `consume_matching(topic, predicate, ...)`.
* `insert_probe_rows(db_url, rows)`, `make_probe_rows(aggregate_id, n)`.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
CONNECTORS_DIR = REPO_ROOT / "infra" / "connect" / "connectors"
DEBEZIUM_SECRET_FILE = REPO_ROOT / "infra" / "secrets" / "infra" / "debezium.properties"

CONNECT_SERVICE = "connect"
CONNECT_LOCAL_URL = "http://localhost:8083"  # inside the connect container only


def kafka_bootstrap() -> str:
    return os.environ.get("SMARTHEALTH_KAFKA_BOOTSTRAP", "localhost:9092")


def registry_url() -> str:
    return os.environ.get("SMARTHEALTH_SCHEMA_REGISTRY_URL", "http://localhost:8081")


# --- subprocess wrappers ------------------------------------------------------------


def compose(
    *args: str, check: bool = True, input_text: str | None = None, timeout: float = 180
) -> subprocess.CompletedProcess[str]:
    """Run `docker compose -f infra/docker-compose.yml <args>` from the repo root."""
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        cwd=REPO_ROOT,
        check=check,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=timeout,
    )


def psql_superuser(sql: str, db: str = "postgres") -> str:
    """Run one SQL statement as the Postgres superuser inside the postgres
    container (its own POSTGRES_USER over the local socket; no password handled)."""
    out = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d "$1" -Atc "$2"',
        "_",
        db,
        sql,
    )
    return out.stdout.strip()


def debezium_password() -> str:
    """The Debezium role password from the Compose secret file. Never print it."""
    for line in DEBEZIUM_SECRET_FILE.read_text().splitlines():
        if line.startswith("password="):
            return line[len("password=") :]
    raise RuntimeError("debezium.properties has no password= line")


# --- connectors ---------------------------------------------------------------------


def connector_names() -> list[str]:
    """Basenames of the connector JSON files present (a pytest parametrize source)."""
    return sorted(path.stem for path in CONNECTORS_DIR.glob("*.json"))


def connector_config(name: str) -> dict[str, str]:
    return json.loads((CONNECTORS_DIR / f"{name}.json").read_text())


def connector_status(name: str) -> dict[str, Any]:
    """`GET /connectors/<name>/status` from inside the connect container."""
    out = compose(
        "exec", "-T", CONNECT_SERVICE, "curl", "-s", f"{CONNECT_LOCAL_URL}/connectors/{name}/status"
    )
    return json.loads(out.stdout)


def connector_states(name: str) -> tuple[str, list[str]]:
    """(connector state, [task states]); ("UNKNOWN", []) when the status is unavailable."""
    try:
        status = connector_status(name)
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError):
        return "UNREACHABLE", []
    if "connector" not in status:
        return "UNKNOWN", []
    return status["connector"]["state"], [task["state"] for task in status.get("tasks", [])]


def connector_running(name: str) -> bool:
    """True only when the connector AND at least one task, and every task, is RUNNING."""
    state, tasks = connector_states(name)
    return state == "RUNNING" and bool(tasks) and all(task == "RUNNING" for task in tasks)


def wait_connectors_running(names: Sequence[str] | None = None, timeout: float = 120) -> None:
    """Block until every named connector (default: every connector file present) has
    its connector and all tasks RUNNING; fail the test with the states otherwise."""
    wanted = list(names) if names is not None else connector_names()
    deadline = time.monotonic() + timeout
    states: dict[str, tuple[str, list[str]]] = {}
    while time.monotonic() < deadline:
        states = {name: connector_states(name) for name in wanted}
        if all(
            state == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks)
            for state, tasks in states.values()
        ):
            return
        time.sleep(2)
    pytest.fail(f"connectors not RUNNING within {timeout}s: {states}")


def register_connector(name: str) -> str:
    """Run the registration script for one connector name inside `connect`
    (idempotent PUT, then waits for connector and task RUNNING)."""
    out = compose(
        "exec", "-T", CONNECT_SERVICE, "bash", "/connect-init/register-connectors.sh", name
    )
    return out.stdout


def put_connector(name: str, config: dict[str, str]) -> None:
    """Idempotent `PUT /connectors/<name>/config` with an in-memory config (the config
    travels on stdin, so no file is left behind and no secret is expanded)."""
    compose(
        "exec",
        "-T",
        CONNECT_SERVICE,
        "curl",
        "-fs",
        "-o",
        "/dev/null",
        "-X",
        "PUT",
        "-H",
        "Content-Type: application/json",
        "--data-binary",
        "@-",
        f"{CONNECT_LOCAL_URL}/connectors/{name}/config",
        input_text=json.dumps(config),
    )


def delete_connector(name: str) -> None:
    compose(
        "exec", "-T", CONNECT_SERVICE, "curl", "-s", "-o", "/dev/null", "-X", "DELETE",
        f"{CONNECT_LOCAL_URL}/connectors/{name}",
        check=False,
    )


def drop_replication_slot(slot: str, timeout: float = 60) -> None:
    """Drop a replication slot, retrying while the (just deleted) connector still holds it."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = psql_superuser(
            f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        )
        if remaining == "0":
            return
        active = psql_superuser(
            f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}' AND active"
        )
        if active == "0":
            try:
                psql_superuser(f"SELECT pg_drop_replication_slot('{slot}')")
            except subprocess.CalledProcessError:
                pass  # raced with a reconnect; retry
        time.sleep(2)
    raise AssertionError(f"replication slot {slot} could not be dropped within {timeout}s")


def delete_topics(*topics: str) -> None:
    """Delete (disposable, probe-only) topics through the broker's own CLI."""
    for topic in topics:
        compose(
            "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092",
            "--delete", "--if-exists", "--topic", topic,
            check=False,
        )


# --- service control (named Compose services only) ----------------------------------


def stop_service(service: str) -> None:
    compose("stop", service)


def start_service(service: str) -> None:
    compose("start", service)


def kill_service(service: str) -> None:
    """SIGKILL one Compose service (`docker compose kill`): a hard crash."""
    compose("kill", service)


def service_health(service: str) -> str:
    out = compose("ps", "-a", "--format", "json", service, check=False)
    raw = out.stdout.strip()
    if not raw:
        return "missing"
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    row = rows[0]
    return row.get("Health") or row.get("State") or "unknown"


def wait_service_healthy(service: str, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    last = "unknown"
    while time.monotonic() < deadline:
        last = service_health(service)
        if last == "healthy":
            return
        time.sleep(3)
    pytest.fail(f"service {service} not healthy within {timeout}s (last: {last})")


# --- stack gating -------------------------------------------------------------------


def require_phase3_stack() -> None:
    """Skip (or, with SMARTHEALTH_E2E_REQUIRE_STACK=1, fail) when Connect is not up."""
    reachable = False
    try:
        compose(
            "exec", "-T", CONNECT_SERVICE, "curl", "-fs", f"{CONNECT_LOCAL_URL}/connectors",
            timeout=30,
        )
        reachable = True
    except (subprocess.SubprocessError, OSError):
        reachable = False
    if not reachable:
        message = "Kafka Connect is not reachable through Compose; start the stack first"
        if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
            pytest.fail(message)
        pytest.skip(message)


# --- registry and Kafka -------------------------------------------------------------


def live_registry() -> Any:
    from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: PLC0415

    return SchemaRegistryClient({"url": registry_url()})


def _header_text(value: bytes | None) -> str | None:
    return None if value is None else value.decode("utf-8", errors="replace")


def consume_matching(
    topic: str,
    predicate: Callable[[dict[str, Any]], bool],
    timeout: float = 60,
    min_count: int = 1,
    *,
    done: Callable[[list[dict[str, Any]]], bool] | None = None,
    decode: bool = True,
) -> list[dict[str, Any]]:
    """Read `topic` from the beginning on every partition and return the messages
    the `predicate` accepts, as dicts with `key`, `headers`, `partition`, `offset`,
    `topic` and (when `decode`) the Avro-decoded `value`.

    The predicate sees `key`, `headers`, `partition` and `offset` only (the value is
    decoded for matches afterwards). Returns as soon as `done(matches)` is true
    (default: `len(matches) >= min_count`), or after `timeout` seconds with whatever
    matched, so a "nothing arrives" check is `consume_matching(..., timeout=10) == []`.
    A unique consumer group and no commits mean this never disturbs a real consumer.
    """
    from confluent_kafka import Consumer, TopicPartition  # noqa: PLC0415
    from confluent_kafka.serialization import (  # noqa: PLC0415
        MessageField,
        SerializationContext,
    )

    from libs.events.registry import deserializer  # noqa: PLC0415

    def finished(found: list[dict[str, Any]]) -> bool:
        return done(found) if done is not None else len(found) >= min_count

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap(),
            "group.id": f"phase3-e2e-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    matches: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    assigned = False
    try:
        while time.monotonic() < deadline:
            if not assigned:
                metadata = consumer.list_topics(topic, timeout=5)
                partitions = metadata.topics.get(topic)
                if partitions is None or partitions.error is not None or not partitions.partitions:
                    time.sleep(1)
                    continue
                consumer.assign([TopicPartition(topic, p, 0) for p in partitions.partitions])
                assigned = True
            message = consumer.poll(0.5)
            if message is None or message.error():
                if finished(matches):
                    break
                continue
            headers = {k: _header_text(v) for k, v in (message.headers() or [])}
            key = message.key().decode("utf-8") if message.key() is not None else None
            entry: dict[str, Any] = {
                "topic": message.topic(),
                "key": key,
                "headers": headers,
                "partition": message.partition(),
                "offset": message.offset(),
                "raw_value": message.value(),
            }
            if predicate(entry):
                matches.append(entry)
                if finished(matches):
                    break
    finally:
        consumer.close()

    if decode:
        decoder = deserializer()
        for entry in matches:
            entry["value"] = decoder(
                entry["raw_value"], SerializationContext(entry["topic"], MessageField.VALUE)
            )
    return matches


# --- probe rows ---------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeRow:
    """One synthetic AppointmentBooked event destined for the `cdcprobe` topic."""

    event_id: uuid.UUID
    aggregate_id: str
    appointment_id: uuid.UUID
    traceparent: str
    occurred_at: datetime.datetime


def make_probe_rows(aggregate_id: str | None = None, count: int = 1) -> list[ProbeRow]:
    """`count` rows for one aggregate (default: a fresh random aggregate id)."""
    aggregate = aggregate_id or str(uuid.uuid4())
    base = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    return [
        ProbeRow(
            event_id=uuid.uuid4(),
            aggregate_id=aggregate,
            appointment_id=uuid.UUID(aggregate) if _is_uuid(aggregate) else uuid.uuid4(),
            traceparent=f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01",
            occurred_at=base + datetime.timedelta(seconds=index),
        )
        for index in range(count)
    ]


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _probe_record(row: ProbeRow) -> dict[str, Any]:
    nil = str(uuid.UUID(int=0))
    return {
        "event_id": str(row.event_id),
        "event_type": "appointment.booked",
        "schema_version": 1,
        "occurred_at": row.occurred_at,
        "aggregate_id": row.aggregate_id,
        "appointment_id": str(row.appointment_id),
        "clinic_id": nil,
        "patient_id": nil,
        "provider_id": nil,
        "slot_id": nil,
        "slot_start": row.occurred_at,
        "slot_end": row.occurred_at + datetime.timedelta(minutes=30),
        "booked_at": row.occurred_at,
    }


def serialize_probe(row: ProbeRow) -> bytes:
    """Registry-framed Avro for the probe row, through the STRICT serializer (the
    schema must already be registered by `schema-init`)."""
    from confluent_kafka.serialization import (  # noqa: PLC0415
        MessageField,
        SerializationContext,
    )

    from libs.events.catalog import CDC_PROBE_TOPIC  # noqa: PLC0415
    from libs.events.registry import serializer_for  # noqa: PLC0415

    body = serializer_for("appointment.booked")(
        _probe_record(row), SerializationContext(CDC_PROBE_TOPIC, MessageField.VALUE)
    )
    assert body is not None
    return body


async def _insert_probe_rows(db_url: str, rows: Sequence[ProbeRow], aggregatetype: str) -> None:
    from sqlalchemy import text  # noqa: PLC0415
    from sqlalchemy.ext.asyncio import create_async_engine  # noqa: PLC0415

    statement = text(
        """
        INSERT INTO outbox
            (id, aggregatetype, aggregateid, type, payload, traceparent, schema_version,
             occurred_at)
        VALUES
            (:id, :aggregatetype, :aggregateid, 'appointment.booked', :payload, :traceparent,
             1, :occurred_at)
        """
    )
    bodies = [serialize_probe(row) for row in rows]  # blocking registry call, before the loop work
    engine = create_async_engine(db_url)
    try:
        for row, body in zip(rows, bodies, strict=True):
            async with engine.begin() as connection:  # one transaction (commit) per row
                await connection.execute(
                    statement,
                    {
                        "id": row.event_id,
                        "aggregatetype": aggregatetype,
                        "aggregateid": row.aggregate_id,
                        "payload": body,
                        "traceparent": row.traceparent,
                        "occurred_at": row.occurred_at,
                    },
                )
    finally:
        await engine.dispose()


def insert_probe_rows(
    db_url: str, rows: Sequence[ProbeRow], *, aggregatetype: str = "cdcprobe"
) -> None:
    """Insert each probe row into the database's real `outbox` in its OWN committed
    transaction, in order. `aggregatetype='cdcprobe'` routes the event to
    `smarthealth.cdcprobe.events`, which no consumer reads."""
    asyncio.run(_insert_probe_rows(db_url, rows, aggregatetype))
