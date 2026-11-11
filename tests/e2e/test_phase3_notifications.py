"""Notifications, reminders and follow-ups on the deployed stack (plan 03-14;
NOTIF-01..04, EVENT-03..05, D-02, D-09 revised, D-13).

A booking, cancellation or reschedule is made through the published gateway. The event
travels outbox -> Debezium -> Kafka -> the `notification-consumer` container -> RabbitMQ
(vhost `smarthealth`, the dedicated non-admin user) -> the `notification-celery`
container -> the fake provider, and the delivery log in Mongo records exactly one `sent`
delivery and the provider's idempotency store exactly one call per dedupe key. The
`notification-temporal-worker` container runs the reminder and follow-up workflows on
Temporal.

Every test uses a fresh clinic (the seeded world). Nothing is deleted. The notification
consumer group starts at the LATEST offset on its first start (D-13), pinned at container
start, so events produced by these tests after the container is healthy are consumed.

Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_notifications.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import pytest_asyncio
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from libs.events.catalog import DLQ_TOPICS, topic_for
from services.notification import deliveries
from services.notification.celery_app import DEAD_LETTER_QUEUE, create_celery_app
from services.notification.config import settings as notification_settings
from services.notification.contracts import followup_workflow_id, reminder_workflow_id
from services.notification.tasks import enqueue_send
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

TRACER_DEADLINE_SECONDS = 60
TEMPORAL_LOCAL_ADDRESS = "localhost:7233"
NOTIFICATION_SERVICES = ("notification-celery", "notification-consumer", "notification-temporal-worker")


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    for service in NOTIFICATION_SERVICES:
        stack.wait_service_healthy(service)


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway, patients=1)


@pytest_asyncio.fixture
async def temporal() -> Client:
    return await Client.connect(TEMPORAL_LOCAL_ADDRESS)


def _deliveries_for(appointment_id: str, notification_type: str) -> list[dict[str, Any]]:
    db = deliveries.get_db()
    return list(
        db[deliveries.DELIVERIES].find(
            {"appointment_id": appointment_id, "notification_type": notification_type}
        )
    )


def _provider_calls(dedupe_key: str) -> int:
    return deliveries.get_db()[deliveries.PROVIDER_CALLS].count_documents({"dedupe_key": dedupe_key})


async def _sent_delivery(appointment_id: str, notification_type: str, *, timeout: float = TRACER_DEADLINE_SECONDS) -> dict[str, Any]:
    """The single `sent` delivery of this type for the appointment (waits for it)."""

    async def check() -> dict[str, Any] | None:
        found = [d for d in _deliveries_for(appointment_id, notification_type) if d["status"] == "sent"]
        return found[0] if found else None

    return await poll(check, timeout=timeout, interval=1.0)


def _assert_exactly_one(appointment_id: str, notification_type: str) -> dict[str, Any]:
    """Exactly one delivery document of the type, `sent`, key `{type}:{appointment}:{event}`,
    and exactly one provider call record for that key."""
    documents = _deliveries_for(appointment_id, notification_type)
    assert len(documents) == 1, documents
    delivery = documents[0]
    assert delivery["status"] == "sent", delivery
    assert delivery["dedupe_key"] == f"{notification_type}:{appointment_id}:{delivery['event_id']}"
    assert _provider_calls(delivery["dedupe_key"]) == 1
    return delivery


async def _wait_workflow_status(
    temporal: Client, workflow_id: str, wanted: WorkflowExecutionStatus, *, timeout: float = 30.0
) -> Any:
    async def check() -> Any:
        try:
            description = await temporal.get_workflow_handle(workflow_id).describe()
        except RPCError as err:
            if err.status == RPCStatusCode.NOT_FOUND:
                return None
            raise
        return description if description.status == wanted else None

    return await poll(check, timeout=timeout, interval=1.0)


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_booking_reaches_the_fake_provider_once_and_its_reminder_runs(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    slots = [slot["id"] for slot in await _day_slots(gateway, ew)]
    appointment_id = await book(gateway, ew, slot_id=slots[0], patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))["status"] == "confirmed"

    delivery = await _sent_delivery(appointment_id, "appointment.booked")
    key = delivery["dedupe_key"]
    assert key == f"appointment.booked:{appointment_id}:{delivery['event_id']}"
    assert _provider_calls(key) == 1
    assert len(_deliveries_for(appointment_id, "appointment.booked")) == 1

    description = await _wait_workflow_status(
        temporal, reminder_workflow_id(appointment_id), WorkflowExecutionStatus.RUNNING
    )
    assert description.status == WorkflowExecutionStatus.RUNNING


async def _day_slots(gateway: httpx.AsyncClient, ew: E2EWorld) -> list[dict[str, Any]]:
    slots = await day_slots(gateway, ew, ew.world.last_day)
    assert len(slots) >= 2, slots
    return slots


# --- Task 3: cancel, reschedule, follow-up, dead letters ------------------------------


async def _booked_and_notified(
    gateway: httpx.AsyncClient, ew: E2EWorld, slot_id: str
) -> str:
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))["status"] == "confirmed"
    await _sent_delivery(appointment_id, "appointment.booked")
    return appointment_id


async def test_cancel_sends_one_notification_and_closes_the_reminder(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    slots = await _day_slots(gateway, ew)
    appointment_id = await _booked_and_notified(gateway, ew, slots[0]["id"])
    await _wait_workflow_status(temporal, reminder_workflow_id(appointment_id), WorkflowExecutionStatus.RUNNING)

    cancel = await gateway.post(f"/api/appointments/{appointment_id}/cancel", json={}, headers=ew.front_desk)
    assert cancel.status_code == 202, cancel.text
    await appointment_in(gateway, ew, appointment_id, {"cancelled"})

    await _sent_delivery(appointment_id, "appointment.cancelled")
    _assert_exactly_one(appointment_id, "appointment.cancelled")
    closed = await _wait_workflow_status(
        temporal, reminder_workflow_id(appointment_id), WorkflowExecutionStatus.COMPLETED
    )
    assert closed.status == WorkflowExecutionStatus.COMPLETED
    result = await temporal.get_workflow_handle(reminder_workflow_id(appointment_id)).result()
    assert result["status"] == "cancelled"
    assert deliveries.is_appointment_closed(deliveries.get_db(), appointment_id)
    # the booking notice is still exactly one
    _assert_exactly_one(appointment_id, "appointment.booked")


async def _signal_inputs(temporal: Client, workflow_id: str, signal_name: str) -> list[Any]:
    """Decoded JSON arguments of every signal of this name in the workflow's history."""
    values: list[Any] = []
    async for event in temporal.get_workflow_handle(workflow_id).fetch_history_events():
        if not event.HasField("workflow_execution_signaled_event_attributes"):
            continue
        attributes = event.workflow_execution_signaled_event_attributes
        if attributes.signal_name == signal_name:
            values.append(json.loads(attributes.input.payloads[0].data))
    return values


async def test_reschedule_sends_one_notification_and_moves_the_reminder(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    slots = await _day_slots(gateway, ew)
    old_slot, new_slot = slots[0], slots[1]
    appointment_id = await _booked_and_notified(gateway, ew, old_slot["id"])
    await _wait_workflow_status(temporal, reminder_workflow_id(appointment_id), WorkflowExecutionStatus.RUNNING)

    resp = await gateway.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": new_slot["id"]},
        headers=ew.front_desk,
    )
    assert resp.status_code == 202, resp.text

    await _sent_delivery(appointment_id, "appointment.rescheduled")
    _assert_exactly_one(appointment_id, "appointment.rescheduled")

    async def signalled() -> list[Any] | None:
        return await _signal_inputs(temporal, reminder_workflow_id(appointment_id), "reschedule") or None

    inputs = await poll(signalled, timeout=30, interval=1.0)
    assert datetime.fromisoformat(inputs[-1]) == datetime.fromisoformat(new_slot["start_time"])
    running = await temporal.get_workflow_handle(reminder_workflow_id(appointment_id)).describe()
    assert running.status == WorkflowExecutionStatus.RUNNING


async def test_a_completed_visit_starts_the_followup_workflow(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    slot_id, _ = await book_slot_inside_checkin_window(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))["status"] == "confirmed"
    checked_in = await gateway.post("/api/visits", json={"appointment_id": appointment_id}, headers=ew.front_desk)
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)
    assert started.status_code == 200, started.text
    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)
    assert completed.status_code == 200, completed.text

    description = await _wait_workflow_status(
        temporal, followup_workflow_id(visit_id), WorkflowExecutionStatus.RUNNING
    )
    assert description.status == WorkflowExecutionStatus.RUNNING
    # the check-in already stopped the appointment's reminder (event-driven, not polled)
    await _wait_workflow_status(temporal, reminder_workflow_id(appointment_id), WorkflowExecutionStatus.COMPLETED)


def _queue_depths() -> dict[str, int]:
    out = stack.compose(
        "exec", "-T", "rabbitmq", "rabbitmqctl", "list_queues", "-p", notification_settings.broker_vhost,
        "name", "messages", "--quiet", timeout=60,
    )
    depths: dict[str, int] = {}
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            depths[parts[0]] = int(parts[1])
    return depths


async def test_a_poison_celery_message_lands_in_the_dead_letter_queue_on_the_live_broker() -> None:
    before = _queue_depths()[DEAD_LETTER_QUEUE]
    key = f"FAIL-INVALID-{uuid.uuid4().hex}"
    app = create_celery_app()  # broker URL from the secret file, resolved lazily, never printed
    enqueue_send(
        app,
        notification_type="appointment.booked",
        dedupe_key=key,
        patient_id=str(uuid.uuid4()),
        appointment_id=str(uuid.uuid4()),
        event_id=str(uuid.uuid4()),
        requested_at=datetime.now(UTC).isoformat(),
    )

    async def check() -> bool:
        return _queue_depths()[DEAD_LETTER_QUEUE] >= before + 1

    await poll(check, timeout=60, interval=1.0)
    assert _queue_depths()[DEAD_LETTER_QUEUE] == before + 1
    failed = deliveries.get_db()[deliveries.DELIVERIES].find_one({"dedupe_key": key})
    assert failed is not None and failed["status"] == "failed"
    assert deliveries.get_db()[deliveries.PROVIDER_CALLS].count_documents({"dedupe_key": key}) == 0


async def test_a_poison_kafka_message_is_dead_lettered_and_notifications_continue(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    from confluent_kafka import Producer  # noqa: PLC0415

    poison_key = f"poison-{uuid.uuid4().hex}"
    producer = Producer({"bootstrap.servers": stack.kafka_bootstrap()})
    producer.produce(topic_for("appointment.booked"), key=poison_key, value=b"\x00not-avro-at-all")
    assert producer.flush(15) == 0

    dlq_topic = DLQ_TOPICS["notification-trigger"]
    matches: list[dict[str, Any]] = []
    deadline = time.monotonic() + 60.0
    while not matches and time.monotonic() < deadline:
        matches = stack.consume_matching(
            dlq_topic, lambda entry: entry["key"] == poison_key, timeout=10, decode=False
        )
    assert len(matches) == 1, matches
    assert matches[0]["headers"]["dlq.error_type"]
    assert matches[0]["headers"]["dlq.consumer_group"] == "notification-trigger"

    # the partition moved on: a booking made after the poison is still notified
    slots = await _day_slots(gateway, ew)
    appointment_id = await _booked_and_notified(gateway, ew, slots[0]["id"])
    _assert_exactly_one(appointment_id, "appointment.booked")
    assert stack.service_health("notification-consumer") == "healthy"


# --- redelivery and retry on the live stack (T-3-41, EVENT-03, NOTIF-04) ---------------


def _committed_offset(partition: int) -> int:
    from confluent_kafka import Consumer, TopicPartition  # noqa: PLC0415

    probe = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap(),
            "group.id": "notification-trigger",
            "enable.auto.commit": False,
        }
    )
    try:  # `committed` only reads the group's offset; this probe never joins the group
        return int(probe.committed([TopicPartition(topic_for("appointment.booked"), partition)], timeout=10)[0].offset)
    finally:
        probe.close()


async def test_a_redelivered_event_sends_once_and_starts_no_second_reminder(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    from confluent_kafka import Producer  # noqa: PLC0415

    slots = await _day_slots(gateway, ew)
    appointment_id = await _booked_and_notified(gateway, ew, slots[0]["id"])
    delivery = _assert_exactly_one(appointment_id, "appointment.booked")
    run_id = (await temporal.get_workflow_handle(reminder_workflow_id(appointment_id)).describe()).run_id

    topic = topic_for("appointment.booked")
    original = stack.consume_matching(
        topic,
        lambda entry: entry["headers"].get("eventId") == delivery["event_id"] or entry["key"] == appointment_id,
        timeout=60,
        decode=False,
    )
    assert original, "the booked event is not on the topic"
    message = original[0]
    delivered: list[Any] = []
    producer = Producer({"bootstrap.servers": stack.kafka_bootstrap()})
    producer.produce(
        topic,
        key=message["key"],
        value=message["raw_value"],
        headers=[(k, v.encode()) for k, v in message["headers"].items() if v is not None],
        partition=message["partition"],
        on_delivery=lambda err, msg: delivered.append((err, msg.offset())),
    )
    assert producer.flush(15) == 0 and delivered[0][0] is None
    duplicate_offset = delivered[0][1]

    async def consumed() -> bool:
        return _committed_offset(message["partition"]) > duplicate_offset

    await poll(consumed, timeout=60, interval=1.0)  # the consumer handled the duplicate

    assert _assert_exactly_one(appointment_id, "appointment.booked")["attempts"] == 1
    description = await temporal.get_workflow_handle(reminder_workflow_id(appointment_id)).describe()
    assert description.run_id == run_id and description.status == WorkflowExecutionStatus.RUNNING


async def test_a_transient_provider_failure_is_retried_and_reaches_the_provider_once() -> None:
    key = f"FAIL-TRANSIENT-{uuid.uuid4().hex}"
    enqueue_send(
        create_celery_app(),
        notification_type="appointment.booked",
        dedupe_key=key,
        patient_id=str(uuid.uuid4()),
        appointment_id=str(uuid.uuid4()),
        event_id=str(uuid.uuid4()),
        requested_at=datetime.now(UTC).isoformat(),
    )

    async def sent() -> dict[str, Any] | None:
        document = deliveries.get_db()[deliveries.DELIVERIES].find_one({"dedupe_key": key})
        return document if document and document["status"] == "sent" else None

    delivery = await poll(sent, timeout=90, interval=1.0)
    assert delivery["attempts"] == 3  # two injected failures, retried with backoff, then sent
    assert _provider_calls(key) == 1
