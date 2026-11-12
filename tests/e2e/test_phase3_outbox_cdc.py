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
import time
import uuid
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


def _ids(messages: list[dict]) -> list[str]:
    return [m["headers"]["id"] for m in messages]


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


# --- Task 3: durability --------------------------------------------------------------


def test_connect_stopped_delivers_in_commit_order_after_start() -> None:
    """With Connect stopped, three committed events for one aggregate produce no
    message; after it starts they arrive on one partition in commit order."""
    stack.wait_connectors_running()
    rows = stack.make_probe_rows(count=3)
    try:
        stack.stop_service("connect")
        stack.insert_probe_rows(_scheduling_db_url(), rows)

        while_stopped = stack.consume_matching(
            CDC_PROBE_TOPIC, lambda m: m["key"] == rows[0].aggregate_id, timeout=10, decode=False
        )
        assert while_stopped == [], "a message was delivered while Connect was stopped"
    finally:
        stack.start_service("connect")
        stack.wait_service_healthy("connect")
        stack.wait_connectors_running()

    expected = [str(row.event_id) for row in rows]
    delivered = stack.consume_matching(
        CDC_PROBE_TOPIC,
        lambda m: m["key"] == rows[0].aggregate_id,
        timeout=90,
        done=lambda found: _ids(found) == expected or set(_ids(found)) >= set(expected),
    )
    first_seen: list[str] = []
    for entry in delivered:
        if entry["headers"]["id"] not in first_seen:
            first_seen.append(entry["headers"]["id"])
    assert first_seen == expected, f"delivery order {first_seen} != commit order {expected}"
    assert len({entry["partition"] for entry in delivered}) == 1, "one aggregate, one partition"
    offsets = [entry["offset"] for entry in delivered]
    assert offsets == sorted(offsets)


def test_connect_hard_kill_loses_nothing() -> None:
    """Five events delivered, Connect SIGKILLed within seconds, restarted, one more
    event: all six event ids are on the topic. Extra copies are expected (at-least-once
    on the wire, research F4) and are counted and logged, not asserted to be zero."""
    stack.wait_connectors_running()
    aggregate = str(uuid.uuid4())
    first = stack.make_probe_rows(aggregate, count=5)
    stack.insert_probe_rows(_scheduling_db_url(), first)
    first_ids = {str(row.event_id) for row in first}

    delivered_before = stack.consume_matching(
        CDC_PROBE_TOPIC,
        lambda m: m["key"] == aggregate,
        timeout=60,
        done=lambda found: set(_ids(found)) >= first_ids,
        decode=False,
    )
    assert set(_ids(delivered_before)) >= first_ids, "first five were not delivered"

    killed_at = time.monotonic()
    try:
        stack.kill_service("connect")
        assert time.monotonic() - killed_at < 5
        (extra,) = stack.make_probe_rows(aggregate, count=1)
        # Committed while Connect is down (or being restarted by its restart policy).
        stack.insert_probe_rows(_scheduling_db_url(), [extra])
    finally:
        # `restart: unless-stopped` normally restarts a killed container; `start` is a
        # no-op when it already runs and revives it otherwise.
        stack.start_service("connect")
        stack.wait_service_healthy("connect")
        stack.wait_connectors_running()

    all_ids = first_ids | {str(extra.event_id)}
    after = stack.consume_matching(
        CDC_PROBE_TOPIC,
        lambda m: m["key"] == aggregate,
        timeout=90,
        done=lambda found: set(_ids(found)) >= all_ids,
        decode=False,
    )
    seen = _ids(after)
    assert set(seen) >= all_ids, f"lost events: {sorted(all_ids - set(seen))}"
    duplicates = len(seen) - len(set(seen))
    print(f"HARD-KILL DUPLICATE COUNT: {duplicates} (messages {len(seen)}, distinct {len(set(seen))})")
    # The aggregate is one partition: per-aggregate order of first occurrences holds.
    order = [event_id for index, event_id in enumerate(seen) if event_id not in seen[:index]]
    assert order[-1] == str(extra.event_id), "the post-kill event must come last"


def test_snapshot_delivers_row_committed_before_connector_first_start() -> None:
    """`snapshot.mode=initial`: a row committed BEFORE a connector first registers is
    delivered by its initial snapshot. Uses a disposable connector with its own slot,
    prefix and topic, restricted to the one probe row, and removes the connector, its
    slot and its topics afterwards (an orphaned slot would retain WAL, Pitfall 4).

    The connector name carries a per-run suffix: Kafka Connect keeps a deleted connector's
    stored offsets, so a second run under the same name resumed from a stale LSN of the
    already dropped slot ("Last recorded offset is no longer available") and the task
    FAILED. A fresh name has no stored offset, so the test passes on every run."""
    name, slot = f"scheduling-outbox-snapshot-probe-{uuid.uuid4().hex[:8]}", "dbz_snapshot_probe"
    prefix = "smarthealth-snapshot-probe"
    topic = "smarthealth.snapshotprobe.events"
    (row,) = stack.make_probe_rows(count=1)
    stack.insert_probe_rows(_scheduling_db_url(), [row])  # committed first

    config = dict(stack.connector_config("scheduling-outbox"))
    config.update(
        {
            "slot.name": slot,
            "topic.prefix": prefix,
            "snapshot.mode": "initial",
            "snapshot.select.statement.overrides": "public.outbox",
            "snapshot.select.statement.overrides.public.outbox": (
                "SELECT * FROM public.outbox "
                f"WHERE aggregatetype = 'cdcprobe' AND aggregateid = '{row.aggregate_id}'"
            ),
            # Whatever the snapshot reads goes to the disposable topic, never a real one.
            "transforms.outbox.route.topic.replacement": topic,
        }
    )
    try:
        stack.put_connector(name, config)
        stack.wait_connectors_running([name])
        delivered = stack.consume_matching(
            topic, lambda m: m["key"] == row.aggregate_id, timeout=60
        )
        assert [m["headers"]["id"] for m in delivered] == [str(row.event_id)]
        assert str(delivered[0]["value"]["event_id"]) == str(row.event_id)
    finally:
        stack.delete_connector(name)
        stack.drop_replication_slot(slot)
        stack.delete_topics(topic, f"__debezium-heartbeat.{prefix}")

    left = stack.psql_superuser(
        f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
    )
    assert left == "0"
    stack.wait_connectors_running()


def test_everything_running_after_durability_tests() -> None:
    """Runs last (file order): every registered connector is RUNNING again."""
    stack.wait_connectors_running()
    for name in stack.connector_names():
        assert stack.connector_running(name), name


def test_probe_run_leaves_no_orphan_slot() -> None:
    left = stack.psql_superuser(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'dbz_snapshot_probe'"
    )
    assert left == "0"


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
