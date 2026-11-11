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

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from services.notification import deliveries
from services.notification.contracts import reminder_workflow_id
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
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

