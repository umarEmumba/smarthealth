"""Billing pre-check (BILL-01, D-10): deterministic, single-transaction,
idempotent, readable by clinic staff through the gateway.
"""

from __future__ import annotations

import httpx
import pytest
from httpx import AsyncClient

from tests.billing.conftest import BillingWorld, auth, precheck, service_token

pytestmark = pytest.mark.integration


async def test_tracer_scheduling_precheck_verifies_and_front_desk_reads_it_via_gateway(
    client: AsyncClient, mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "scheduling")

    created = await precheck(mesh, token, billing_world.clinic_id, coverage_ref=None)

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "verified"
    assert body["coverage_kind"] == "self_pay"
    appointment_id = body["appointment_id"]

    read = await client.get(
        "/api/billing/records",
        params={"appointment_id": appointment_id},
        headers=auth(billing_world.front_desk),
    )

    assert read.status_code == 200, read.text
    record = read.json()
    assert record["status"] == "verified"
    assert record["clinic_id"] == billing_world.clinic_id
    assert [(h["from_status"], h["to_status"]) for h in record["history"]] == [
        (None, "pending"),
        ("pending", "verified"),
    ]
    # No keys, actors or coverage reference ever leave Billing.
    assert set(record) == {
        "id",
        "appointment_id",
        "clinic_id",
        "status",
        "coverage_kind",
        "reason_code",
        "created_at",
        "updated_at",
        "history",
    }
    assert set(record["history"][0]) == {"from_status", "to_status", "reason_code", "created_at"}

    by_id = await client.get(
        f"/api/billing/records/{record['id']}", headers=auth(billing_world.front_desk)
    )
    assert by_id.status_code == 200, by_id.text
    assert by_id.json() == record
