"""CDC relay proofs against the live stack (plan 03-05; D-01, EVENT-02).

A row committed to a service's `outbox` reaches Kafka through Debezium with
key = aggregate id, headers `id`, `eventType`, `traceparent`, `schemaVersion`, and
a registry-framed Avro value; delivery survives Connect being stopped or hard
killed. Tests that name a connector are parametrized over the connector JSON files
present, so the patient, provider and visit connectors that later plans add are
covered without editing this file (select one with `-k <name>`).

Probe rows use `aggregatetype='cdcprobe'` and land on `smarthealth.cdcprobe.events`,
which no consumer reads. They are synthetic and are left in the outbox (outbox
tables are insert-only; test hygiene never deletes application rows).
"""

from __future__ import annotations

import asyncio
import json
from urllib.parse import urlparse

import pytest

from libs.events.catalog import CDC_PROBE_TOPIC
from tests.e2e import phase3_stack as stack

pytestmark = pytest.mark.e2e

# database -> a table the debezium role must NOT be able to read (least privilege,
# T-3-07). An entry is exercised only when its connector file exists.
NON_OUTBOX_PROBE_TABLE = {
    "scheduling_db": "appointments",
    "patient_db": "patients",
    "provider_db": "providers",
    "visit_db": "visits",
}


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()


def _scheduling_db_url() -> str:
    from services.scheduling.config import settings  # noqa: PLC0415

    return settings.database_url


# --- Task 1: tracer ------------------------------------------------------------------


def test_tracer_probe_row_reaches_kafka_through_debezium() -> None:
    """One committed outbox row -> one Kafka message with the agreed key, headers
    and an Avro value that decodes against the LIVE registry."""
    stack.wait_connectors_running(["scheduling-outbox"])
    (row,) = stack.make_probe_rows(count=1)
    stack.insert_probe_rows(_scheduling_db_url(), [row])

    messages = stack.consume_matching(
        CDC_PROBE_TOPIC, lambda m: m["key"] == row.aggregate_id, timeout=60
    )
    assert len(messages) == 1, f"expected one probe message, got {len(messages)}"
    message = messages[0]

    assert message["key"] == row.aggregate_id
    headers = message["headers"]
    assert headers["id"] == str(row.event_id)
    assert headers["eventType"] == "appointment.booked"
    assert headers["traceparent"] == row.traceparent
    assert headers["schemaVersion"] == "1"

    value = message["value"]
    # The `uuid` logical type decodes to uuid.UUID.
    assert str(value["event_id"]) == str(row.event_id)
    assert value["event_type"] == "appointment.booked"
    assert value["aggregate_id"] == row.aggregate_id
    assert str(value["appointment_id"]) == str(row.appointment_id)
    assert value["schema_version"] == 1


# --- Task 2: per-connector task state, slot, publication, least privilege --------------


@pytest.mark.parametrize("name", stack.connector_names())
def test_connector_tasks_running(name: str) -> None:
    """The connector AND every task is RUNNING; the connector state alone is not
    accepted (a missing publication leaves it RUNNING with a FAILED task)."""
    status = stack.connector_status(name)
    assert status["connector"]["state"] == "RUNNING", status["connector"]
    tasks = status.get("tasks", [])
    assert tasks, f"connector {name} has no tasks"
    assert all(task["state"] == "RUNNING" for task in tasks), [t["state"] for t in tasks]


@pytest.mark.parametrize("name", stack.connector_names())
def test_slots_and_publications(name: str) -> None:
    config = stack.connector_config(name)
    slot, database = config["slot.name"], config["database.dbname"]

    active = stack.psql_superuser(
        f"SELECT active FROM pg_replication_slots WHERE slot_name = '{slot}'"
    )
    assert active == "t", f"replication slot {slot} is missing or inactive (active={active!r})"

    published = stack.psql_superuser(
        "SELECT count(*) FROM pg_publication_tables "
        f"WHERE pubname = '{config['publication.name']}' AND tablename = 'outbox'",
        db=database,
    )
    assert published == "1", f"outbox is not in publication {config['publication.name']}"


@pytest.mark.parametrize("name", stack.connector_names())
def test_debezium_role_least_privilege(name: str) -> None:
    """`debezium` can read `outbox` and nothing else (the password comes from the
    secret file and is never printed)."""
    import asyncpg  # noqa: PLC0415

    database = stack.connector_config(name)["database.dbname"]
    non_outbox = NON_OUTBOX_PROBE_TABLE[database]
    parsed = urlparse(_scheduling_db_url().replace("+asyncpg", ""))

    async def check() -> tuple[int, bool]:
        connection = await asyncpg.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user="debezium",
            password=stack.debezium_password(),
            database=database,
        )
        try:
            count = await connection.fetchval("SELECT count(*) FROM outbox")
            try:
                await connection.fetchval(f"SELECT 1 FROM {non_outbox} LIMIT 1")
            except asyncpg.InsufficientPrivilegeError:
                return int(count), True
            return int(count), False
        finally:
            await connection.close()

    _count, denied = asyncio.run(check())
    assert denied, f"debezium could read {non_outbox} in {database} (permission denied expected)"


def test_connect_rest_port_is_not_published() -> None:
    """T-3-05: the REST API is reachable only on the Compose network."""
    out = stack.compose("ps", "--format", "json", "connect")
    rows = [json.loads(line) for line in out.stdout.splitlines() if line.strip()]
    assert rows, "connect container not found"
    published = [
        publisher
        for row in rows
        for publisher in row.get("Publishers") or []
        if publisher.get("PublishedPort")
    ]
    assert published == [], f"connect must not publish a host port: {published}"
