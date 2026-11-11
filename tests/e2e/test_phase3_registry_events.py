"""Patient and Provider events on the deployed stack (plan 03-08; EVENT-01, EVENT-02,
ANLY-01; D-06, D-07).

A patient registered through the gateway commits a `patient.registered` outbox row
with the patient in patient_db; Debezium relays it to `smarthealth.patient.events`
keyed by the patient id. An availability-template update commits a
`provider.schedule_changed` row with its `schedule_change_events` row in provider_db;
Debezium relays it to `smarthealth.provider.events` keyed by the provider id.

The patient test submits four distinctive synthetic PHI strings and then searches the
RAW Kafka message value bytes (and every header) for each of them: Patient is the only
PHI holder, so this is the byte-level proof that no PHI reaches the event stream
(T-3-24).

Each read uses a fresh consumer group (nothing here disturbs a real consumer). Nothing
in this file stops, kills or restarts a container. Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_registry_events.py -m e2e
"""

from __future__ import annotations

import asyncio
import datetime
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio

from libs.events.catalog import topic_for
from libs.events.outbox import event_id_for
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import E2EWorld, gateway_url, require_stack, seed_phase2_world

pytestmark = pytest.mark.e2e

PATIENT_TOPIC = topic_for("patient.registered")

# Distinctive synthetic PHI strings: none may appear in any byte of the event.
PHI_FULL_NAME = "Zzyzx Tracerpatient Phibyte"
PHI_DATE_OF_BIRTH = "1987-03-14"
PHI_PHONE = "+1-555-010-4242"
PHI_ADDRESS = "4242 Synthetic Boulevard Apt 9"


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_connectors_running(["patient-outbox"])


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway, patients=1)


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)


async def _messages(
    topic: str, key: str, event_type: str, *, timeout: float = 60
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(
        stack.consume_matching,
        topic,
        lambda m: m["key"] == key and m["headers"].get("eventType") == event_type,
        timeout,
        1,
    )


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_registered_patient_publishes_phi_free_event(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> None:
    """POST /api/patients through the gateway -> one patient.registered message keyed by
    the patient id; ids and a timestamp only; none of the submitted PHI in its bytes."""
    created = await gateway.post(
        "/api/patients",
        json={
            "clinic_id": ew.clinic_id,
            "full_name": PHI_FULL_NAME,
            "date_of_birth": PHI_DATE_OF_BIRTH,
            "phone": PHI_PHONE,
            "address": PHI_ADDRESS,
        },
        headers=ew.front_desk,
    )
    assert created.status_code == 201, created.text
    patient_id = created.json()["id"]

    found = await _messages(PATIENT_TOPIC, patient_id, "patient.registered")
    assert found, f"no patient.registered message for {patient_id} within 60 s"
    # Debezium is at-least-once: count distinct event ids, not messages.
    assert {m["headers"]["id"] for m in found} == {
        str(event_id_for(patient_id, "patient.registered", "registered"))
    }
    message = found[0]

    assert message["key"] == patient_id
    assert message["headers"]["eventType"] == "patient.registered"

    value = message["value"]
    assert str(value["patient_id"]) == patient_id
    assert str(value["clinic_id"]) == ew.clinic_id
    assert str(value["aggregate_id"]) == patient_id
    assert value["event_type"] == "patient.registered"
    assert isinstance(value["registered_at"], datetime.datetime)
    assert abs(
        _aware(value["registered_at"]) - datetime.datetime.now(datetime.UTC)
    ) < datetime.timedelta(minutes=5)
    # The record has exactly the PHI-free fields and no demographic field name.
    assert set(value) == {
        "event_id",
        "event_type",
        "schema_version",
        "occurred_at",
        "aggregate_id",
        "patient_id",
        "clinic_id",
        "registered_at",
    }

    # Byte-level PHI proof: the raw value and every header, not just the decoded dict.
    raw = bytes(message["raw_value"])
    header_bytes = b"".join(
        (v or "").encode("utf-8") for v in message["headers"].values()
    )
    for phi in (PHI_FULL_NAME, PHI_DATE_OF_BIRTH, PHI_PHONE, PHI_ADDRESS):
        assert phi.encode("utf-8") not in raw, f"PHI string leaked into the message value: {phi!r}"
        assert phi.encode("utf-8") not in header_bytes, f"PHI string leaked into a header: {phi!r}"
    # Not even the distinctive fragments of the strings.
    for fragment in (b"Tracerpatient", b"Phibyte", b"Boulevard", b"555-010", b"1987"):
        assert fragment not in raw, fragment

    # The id is not random noise: it is the uuid5 of the patient id and the type.
    assert uuid.UUID(message["headers"]["id"]).version == 5
