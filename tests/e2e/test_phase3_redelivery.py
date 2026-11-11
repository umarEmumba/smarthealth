"""Forced redelivery leaves every effect at exactly one (plan 03-17; EVENT-02, EVENT-03, D-08;
roadmap success criterion 1; research F4 and Pitfalls 3 and 12).

Debezium is at-least-once on the wire, so "never lost or duplicated" means NO LOSS on the
wire plus EFFECT-LEVEL exactly-once through idempotent consumers. These tests force the
redelivery instead of waiting for it:

* `test_offset_rewind_...`: a fresh clinic is driven through three bookings (one stays booked,
  one is cancelled, one has a completed visit). Then, for each consumer service and group
  (`visit-consumer`/`visit-downstream`, `analytics-consumer`/`analytics-projection`,
  `notification-consumer`/`notification-trigger`), the service is stopped, the group's offsets
  on its topics are reset to a point before the test started
  (`kafka-consumer-groups --reset-offsets --to-datetime ... --execute`), the service is started
  and the group drains to lag 0. The service's own `event.consumed` log lines name every event
  id it handled again (the evidence that the rewind happened). Afterwards the analytics summary,
  the delivery log, the provider call count and the Temporal executions are exactly what they
  were before.
* `test_connect_hard_kill_...`: a booking is made; the moment its `appointment.booked` event is on
  Kafka, Connect is SIGKILLed (so its last flushed source offset is older than the event) and
  started again. The connector re-reads the outbox row from its slot, so the event may appear on
  the topic more than once (counted and printed); the booking still has one delivery, one provider
  call, one reminder execution and exactly one more `appointments_booked` in the analytics summary.

Every assertion is scoped to the test's own clinic and ids, never to a whole topic (the live
topics hold throw-away test records). Every stopped or killed service is restarted in `finally`
and its health awaited; Connect also waits for all four connector tasks RUNNING.

Run with the stack up and nothing else running against it:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_redelivery.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import pytest_asyncio
from temporalio.client import Client

from libs.events.catalog import EventType, topic_for
from services.analytics.projections import PROJECTED_EVENT_TYPES
from services.notification import deliveries
from services.notification.consumer import TOPICS as NOTIFICATION_TOPICS
from services.notification.contracts import followup_workflow_id, reminder_workflow_id
from services.visit.domain.contracts import visit_completed_workflow_id
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
    book_slot_inside_checkin_window,
    day_slots,
    gateway_url,
    poll,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

TEMPORAL_LOCAL_ADDRESS = "localhost:7233"
EFFECT_DEADLINE_SECONDS = 120
# The Kafka message timestamp comes from Connect's clock, the reset time from the host's:
# rewind generously so the test's own events are certainly included. Older events are
# redelivered too, which only strengthens the proof (every one is a no-op).
REWIND_MARGIN_SECONDS = 120
SETTLE_SECONDS = 15
CONNECT_DUPLICATE_WINDOW_SECONDS = 25

APPOINTMENT_TOPIC = topic_for(EventType.APPOINTMENT_BOOKED)
VISIT_TOPIC = topic_for(EventType.VISIT_COMPLETED)

# service -> (consumer group, topics the group reads)
CONSUMER_GROUPS: dict[str, tuple[str, list[str]]] = {
    "visit-consumer": ("visit-downstream", [VISIT_TOPIC]),
    "analytics-consumer": (
        "analytics-projection",
        sorted({topic_for(event_type) for event_type in PROJECTED_EVENT_TYPES}),
    ),
    "notification-consumer": ("notification-trigger", list(NOTIFICATION_TOPICS)),
}


# --- fixtures ------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_connectors_running()
    for service in (
        "analytics",
        "visit-consumer",
        "analytics-consumer",
        "notification-consumer",
        "notification-celery",
        "notification-temporal-worker",
    ):
        stack.wait_service_healthy(service)


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def temporal() -> Client:
    return await Client.connect(TEMPORAL_LOCAL_ADDRESS)


@contextmanager
def _restored(service: str, *, connect: bool = False) -> Iterator[None]:
    """Whatever the body does to `service` (stop, kill), start it again and wait for health;
    for Connect also until every connector task is RUNNING."""
    try:
        yield
    finally:
        stack.start_service(service)
        stack.wait_service_healthy(service)
        if connect:
            stack.wait_connectors_running()


# --- Kafka helpers -------------------------------------------------------------------


def _utc_stamp(moment: datetime.datetime) -> str:
    moment = moment.astimezone(datetime.UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _reset_offsets(group: str, topics: list[str], to: datetime.datetime) -> str:
    """`kafka-consumer-groups --reset-offsets --to-datetime ... --execute` inside the broker
    container; retried while the (just stopped) group is still being expired."""
    argv = [
        "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:29092",
        "--group", group,
        *[arg for topic in topics for arg in ("--topic", topic)],
        "--reset-offsets", "--to-datetime", _utc_stamp(to), "--execute",
    ]  # fmt: skip
    deadline = time.monotonic() + 90
    last = ""
    while time.monotonic() < deadline:
        result = stack.compose(*argv, check=False, timeout=60)
        last = (result.stdout + result.stderr).strip()
        if result.returncode == 0 and "Error" not in last:
            return last
        time.sleep(3)
    raise AssertionError(f"could not reset offsets of group {group}: {last[-300:]}")


def _group_drained(group: str) -> bool:
    """True when the group has an active member on every row and lag 0 everywhere."""
    result = stack.compose(
        "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:29092",
        "--describe", "--group", group,
        check=False, timeout=60,
    )  # fmt: skip
    rows = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 7 and parts[0] == group and parts[2].isdigit():
            rows.append(parts)
    if not rows:
        return False
    return all(row[5] == "0" and row[6] != "-" for row in rows)


def _events_since(
    topic: str, since: datetime.datetime, keys: set[str], *, stop_after_key: str | None = None,
    timeout: float = 30,
) -> list[dict[str, Any]]:
    """Events of `topic` with Kafka timestamp >= `since` whose key is in `keys`, decoded.

    Reads up to the end of each partition as it was at the call (so the call returns). With
    `stop_after_key` it instead returns as soon as one record with that key has been seen, for
    reacting to an event the moment it lands (`timeout` bounds the wait).
    """
    from confluent_kafka import Consumer, TopicPartition  # noqa: PLC0415
    from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: PLC0415

    from libs.events.registry import deserializer  # noqa: PLC0415

    consumer = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap(),
            "group.id": f"phase3-e2e-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    found: list[dict[str, Any]] = []
    try:
        partitions = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        since_ms = int(since.timestamp() * 1000)
        offsets = consumer.offsets_for_times(
            [TopicPartition(topic, p, since_ms) for p in partitions], timeout=15
        )
        assignment = []
        remaining: dict[int, int] = {}
        for tp in offsets:
            high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)[1]
            start = tp.offset if tp.offset >= 0 else high  # -1: nothing at/after `since`
            assignment.append(TopicPartition(topic, tp.partition, start))
            if start < high:
                remaining[tp.partition] = high
        consumer.assign(assignment)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if stop_after_key is None and not remaining:
                break
            message = consumer.poll(0.5)
            if message is None or message.error():
                continue
            key = message.key().decode("utf-8") if message.key() is not None else None
            if message.offset() + 1 >= remaining.get(message.partition(), 0):
                remaining.pop(message.partition(), None)
            if key in keys:
                value = deserializer()(
                    message.value(), SerializationContext(message.topic(), MessageField.VALUE)
                )
                found.append(
                    {
                        "key": key,
                        "event_id": str(value["event_id"]),
                        "event_type": value["event_type"],
                        "partition": message.partition(),
                        "offset": message.offset(),
                    }
                )
                if stop_after_key is not None and key == stop_after_key:
                    break
    finally:
        consumer.close()
    return found


def _consumed_event_ids(service: str, since: datetime.datetime) -> set[str]:
    """Event ids the service logged as consumed (`event.consumed`) since `since`."""
    out = stack.compose(
        "logs", "--no-color", "--since", since.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        service, timeout=120,
    ).stdout  # fmt: skip
    ids: set[str] = set()
    for line in out.splitlines():
        _, _, payload = line.partition("| ")
        try:
            record = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if record.get("event") == "event.consumed" and record.get("event_id"):
            ids.add(str(record["event_id"]))
    return ids


def _consumed_line_count(service: str, since: datetime.datetime) -> int:
    out = stack.compose(
        "logs", "--no-color", "--since", since.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        service, timeout=120,
    ).stdout  # fmt: skip
    return out.count('"event.consumed"')


# --- effect readers ------------------------------------------------------------------


@dataclass
class Scenario:
    ew: E2EWorld
    kept: str = ""
    cancelled: str = ""
    visited: str = ""
    visit_id: str = ""
    ids: set[str] = field(default_factory=set)

    @property
    def appointment_ids(self) -> list[str]:
        return [self.kept, self.cancelled, self.visited]


def _window() -> dict[str, str]:
    today = datetime.datetime.now(datetime.UTC).date()
    return {
        "from": (today - datetime.timedelta(days=1)).isoformat(),
        "to": (today + datetime.timedelta(days=1)).isoformat(),
    }


async def _summary(gateway: httpx.AsyncClient, ew: E2EWorld) -> dict[str, Any]:
    response = await gateway.get(
        "/api/analytics/summary",
        params={"clinic_id": ew.clinic_id, **_window()},
        headers=ew.admin,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _delivery_state(appointment_id: str, notification_type: str) -> list[tuple[str, str, int, int]]:
    """(dedupe key, status, attempts, provider calls) for every delivery of the type."""
    db = deliveries.get_db()
    documents = db[deliveries.DELIVERIES].find(
        {"appointment_id": appointment_id, "notification_type": notification_type}
    )
    state = []
    for document in documents:
        key = document["dedupe_key"]
        calls = db[deliveries.PROVIDER_CALLS].count_documents({"dedupe_key": key})
        state.append((key, document["status"], document["attempts"], calls))
    return sorted(state)


async def _executions(temporal: Client, workflow_id: str) -> int:
    """Number of Temporal executions (any status) with this deterministic workflow id."""
    query = f"WorkflowId = '{workflow_id}'"
    return len([wf async for wf in temporal.list_workflows(query)])


async def _snapshot(
    gateway: httpx.AsyncClient, temporal: Client, sc: Scenario
) -> dict[str, Any]:
    workflows = {
        "reminder_kept": reminder_workflow_id(sc.kept),
        "reminder_cancelled": reminder_workflow_id(sc.cancelled),
        "reminder_visited": reminder_workflow_id(sc.visited),
        "visit_completed": visit_completed_workflow_id(sc.visit_id),
        "followup": followup_workflow_id(sc.visit_id),
    }
    return {
        "summary": await _summary(gateway, sc.ew),
        "booked": {a: _delivery_state(a, "appointment.booked") for a in sc.appointment_ids},
        "cancelled": _delivery_state(sc.cancelled, "appointment.cancelled"),
        "executions": {
            name: await _executions(temporal, workflow_id) for name, workflow_id in workflows.items()
        },
    }


def _assert_one_effect_each(snapshot: dict[str, Any], sc: Scenario, *, where: str) -> None:
    for appointment_id, state in snapshot["booked"].items():
        assert len(state) == 1, f"{where}: {appointment_id} booked deliveries {state}"
        _, status, attempts, calls = state[0]
        assert (status, attempts, calls) == ("sent", 1, 1), f"{where}: {appointment_id} {state}"
    assert len(snapshot["cancelled"]) == 1, f"{where}: cancelled deliveries {snapshot['cancelled']}"
    assert snapshot["cancelled"][0][1:] == ("sent", 1, 1), f"{where}: {snapshot['cancelled']}"
    assert snapshot["executions"] == {
        "reminder_kept": 1,
        "reminder_cancelled": 1,
        "reminder_visited": 1,
        "visit_completed": 1,
        "followup": 1,
    }, f"{where}: {snapshot['executions']}"
    summary = snapshot["summary"]
    assert summary["appointments_booked"] == 3, f"{where}: {summary}"
    assert summary["completed_visits"] == 1, f"{where}: {summary}"
    assert summary["total_patients"] == 3, f"{where}: {summary}"


async def _drive_scenario(gateway: httpx.AsyncClient, temporal: Client) -> Scenario:
    """Fresh clinic: three patients, three confirmed bookings, one cancelled, one visit
    checked in, started and completed; wait until every effect exists."""
    ew = await seed_phase2_world(gateway, patients=3)
    sc = Scenario(ew=ew)
    slots = [slot["id"] for slot in (await day_slots(gateway, ew, ew.world.last_day))[:2]]
    assert len(slots) == 2
    window_slot, _provider = await book_slot_inside_checkin_window(gateway, ew)

    sc.kept = await book(gateway, ew, slot_id=slots[0], patient_id=ew.patient_ids[0])
    sc.cancelled = await book(gateway, ew, slot_id=slots[1], patient_id=ew.patient_ids[1])
    sc.visited = await book(gateway, ew, slot_id=window_slot, patient_id=ew.patient_ids[2])
    for appointment_id in sc.appointment_ids:
        confirmed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
        assert confirmed["status"] == "confirmed", confirmed

    cancel = await gateway.post(
        f"/api/appointments/{sc.cancelled}/cancel", json={}, headers=ew.front_desk
    )
    assert cancel.status_code == 202, cancel.text
    await appointment_in(gateway, ew, sc.cancelled, {"cancelled"})

    checked_in = await gateway.post(
        "/api/visits", json={"appointment_id": sc.visited}, headers=ew.front_desk
    )
    assert checked_in.status_code == 201, checked_in.text
    sc.visit_id = checked_in.json()["id"]
    assert (await gateway.post(f"/api/visits/{sc.visit_id}/start", headers=ew.front_desk)).status_code == 200
    done = await gateway.post(f"/api/visits/{sc.visit_id}/complete", headers=ew.front_desk)
    assert done.status_code == 200, done.text

    async def all_effects() -> dict[str, Any] | None:
        snapshot = await _snapshot(gateway, temporal, sc)
        try:
            _assert_one_effect_each(snapshot, sc, where="setup")
        except AssertionError:
            return None
        return snapshot

    await poll(all_effects, timeout=EFFECT_DEADLINE_SECONDS, interval=2.0)
    sc.ids = {sc.kept, sc.cancelled, sc.visited, sc.visit_id, *ew.patient_ids}
    return sc


def _test_event_ids(topic: str, keys: set[str], since: datetime.datetime) -> set[str]:
    return {event["event_id"] for event in _events_since(topic, since, keys)}


# --- offset rewind -------------------------------------------------------------------


async def test_offset_rewind_of_every_consumer_group_leaves_each_effect_at_one(
    gateway: httpx.AsyncClient, temporal: Client
) -> None:
    test_start = datetime.datetime.now(datetime.UTC)
    sc = await _drive_scenario(gateway, temporal)
    before = await _snapshot(gateway, temporal, sc)
    _assert_one_effect_each(before, sc, where="before the rewind")

    rewind_to = test_start - datetime.timedelta(seconds=REWIND_MARGIN_SECONDS)
    reprocessed: dict[str, tuple[int, int]] = {}
    for service, (group, topics) in CONSUMER_GROUPS.items():
        # The test's own events on this group's topics (known before the rewind).
        own: set[str] = set()
        for topic in topics:
            own |= _test_event_ids(topic, sc.ids, rewind_to)
        assert own, f"no event of this test on {topics}: the scenario produced nothing for {group}"

        with _restored(service):
            await asyncio.to_thread(stack.stop_service, service)
            await asyncio.to_thread(_reset_offsets, group, topics, rewind_to)
            restart_at = datetime.datetime.now(datetime.UTC)
            await asyncio.to_thread(stack.start_service, service)
            await asyncio.to_thread(stack.wait_service_healthy, service)

            async def drained(group: str = group) -> bool:
                return await asyncio.to_thread(_group_drained, group)

            await poll(drained, timeout=240, interval=3.0)

            # Evidence that the rewind happened: the service logged every one of the
            # test's own events as consumed again after its restart.
            async def reprocessed_all(
                service: str = service, own: set[str] = own, restart_at: datetime.datetime = restart_at
            ) -> bool:
                seen = await asyncio.to_thread(_consumed_event_ids, service, restart_at)
                return own <= seen

            await poll(reprocessed_all, timeout=90, interval=3.0)
            total = await asyncio.to_thread(_consumed_line_count, service, restart_at)
            reprocessed[group] = (len(own), total)

    await asyncio.sleep(SETTLE_SECONDS)  # let the re-enqueued Celery sends and signals drain
    after = await _snapshot(gateway, temporal, sc)
    _assert_one_effect_each(after, sc, where="after the rewind")
    assert after == before, f"an effect changed after the redelivery:\nbefore={before}\nafter={after}"

    print(  # noqa: T201 - evidence quoted by the docs (plan 03-18) and the SUMMARY
        "\nREDELIVERY_EVIDENCE rewind "
        + " ".join(
            f"{group}: own_events_reprocessed={own} total_event_consumed_lines={total}"
            for group, (own, total) in reprocessed.items()
        )
    )


# --- Connect hard kill ---------------------------------------------------------------


async def test_connect_hard_kill_after_delivery_changes_no_effect(
    gateway: httpx.AsyncClient, temporal: Client
) -> None:
    ew = await seed_phase2_world(gateway, patients=1)
    slots = [slot["id"] for slot in (await day_slots(gateway, ew, ew.world.last_day))[:1]]
    assert slots

    baseline = (await _summary(gateway, ew))["appointments_booked"]
    since = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=30)
    appointment_id = await book(gateway, ew, slot_id=slots[0], patient_id=ew.patient_ids[0])

    with _restored(stack.CONNECT_SERVICE, connect=True):
        landed = await asyncio.to_thread(
            _events_since, APPOINTMENT_TOPIC, since, {appointment_id},
            stop_after_key=appointment_id, timeout=60,
        )  # fmt: skip
        assert landed, "the booked event never reached Kafka"
        await asyncio.to_thread(stack.kill_service, stack.CONNECT_SERVICE)
        killed_at = time.monotonic()
        await asyncio.to_thread(stack.start_service, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_service_healthy, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_connectors_running)
        down_seconds = time.monotonic() - killed_at

    await asyncio.sleep(CONNECT_DUPLICATE_WINDOW_SECONDS)
    copies = [
        event
        for event in await asyncio.to_thread(_events_since, APPOINTMENT_TOPIC, since, {appointment_id})
        if event["event_type"] == EventType.APPOINTMENT_BOOKED
    ]
    ids = {event["event_id"] for event in copies}
    assert len(ids) == 1, f"one booking must have one event id, saw {ids}"  # no loss, no phantom
    assert len(copies) >= 1, "the booked event vanished from the topic (loss)"

    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    async def sent() -> list[tuple[str, str, int, int]] | None:
        state = _delivery_state(appointment_id, "appointment.booked")
        return state if state and state[0][1] == "sent" else None

    await poll(sent, timeout=EFFECT_DEADLINE_SECONDS, interval=2.0)

    async def counted() -> bool:
        return (await _summary(gateway, ew))["appointments_booked"] == baseline + 1

    await poll(counted, timeout=EFFECT_DEADLINE_SECONDS, interval=2.0)

    async def reminder_started() -> bool:
        return await _executions(temporal, reminder_workflow_id(appointment_id)) >= 1

    await poll(reminder_started, timeout=EFFECT_DEADLINE_SECONDS, interval=2.0)
    await asyncio.sleep(SETTLE_SECONDS)

    state = _delivery_state(appointment_id, "appointment.booked")
    assert len(state) == 1 and state[0][1:] == ("sent", 1, 1), state
    assert await _executions(temporal, reminder_workflow_id(appointment_id)) == 1
    assert (await _summary(gateway, ew))["appointments_booked"] == baseline + 1

    print(  # noqa: T201 - evidence quoted by the docs (plan 03-18) and the SUMMARY
        f"\nREDELIVERY_EVIDENCE connect_hard_kill appointment_id={appointment_id} "
        f"copies_of_booked_event_on_topic={len(copies)} distinct_event_ids={len(ids)} "
        f"connect_restart_seconds={down_seconds:.1f}"
    )
