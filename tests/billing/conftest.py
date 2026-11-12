"""Shared helpers for the Billing tests (plan 02-05).

Billing is called DIRECTLY over the in-process mesh with service tokens minted
by Identity for the calling service (`ServiceTokenProvider`, as Scheduling and
Visit activities do), and read by staff through the gateway.

Appointment ids are random UUIDs: Billing does not validate them against
Scheduling (its callers are trusted, authenticated services). Rows are left in
place (no hard deletes; uuid-keyed, so tests never collide).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import httpx
import pytest_asyncio
from httpx import AsyncClient

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def auth(tokens: dict) -> dict[str, str]:
    return bearer(tokens["access_token"])


def billing_url(path: str) -> str:
    return f"{service_base_url('billing')}{path}"


async def service_token(mesh: httpx.AsyncClient, service: str, audience: str = "billing") -> str:
    secret = load_client_secret(os.environ[f"{service.upper()}_CLIENT_SECRET_PATH"])
    tokens = ServiceTokenProvider(service, secret, service_base_url("identity"), http=mesh)
    return await tokens.get(audience)


def new_key() -> str:
    return f"test-{uuid.uuid4().hex}"


async def precheck(
    mesh: httpx.AsyncClient,
    token: str,
    clinic_id: str,
    *,
    appointment_id: str | None = None,
    coverage_ref: str | None = None,
    key: str | None = None,
) -> httpx.Response:
    body: dict = {
        "appointment_id": appointment_id or str(uuid.uuid4()),
        "clinic_id": clinic_id,
    }
    if coverage_ref is not None:
        body["coverage_ref"] = coverage_ref
    return await mesh.post(
        billing_url("/internal/billing/prechecks"),
        json=body,
        headers={**bearer(token), "Idempotency-Key": key or new_key()},
    )


async def transition(
    mesh: httpx.AsyncClient,
    token: str,
    appointment_id: str,
    target: str,
    *,
    key: str | None = None,
    reason_code: str | None = None,
) -> httpx.Response:
    body: dict = {"target": target}
    if reason_code is not None:
        body["reason_code"] = reason_code
    return await mesh.post(
        billing_url(f"/internal/billing/records/{appointment_id}/transitions"),
        json=body,
        headers={**bearer(token), "Idempotency-Key": key or new_key()},
    )


async def relink(
    mesh: httpx.AsyncClient, token: str, appointment_id: str, *, key: str | None = None
) -> httpx.Response:
    return await mesh.post(
        billing_url(f"/internal/billing/records/{appointment_id}/relink"),
        headers={**bearer(token), "Idempotency-Key": key or new_key()},
    )


@dataclass(frozen=True)
class BillingWorld:
    admin: dict
    clinic_id: str
    front_desk: dict
    other_clinic_id: str
    other_front_desk: dict


async def _clinic(client: AsyncClient, admin: dict, name: str) -> str:
    resp = await client.post("/api/clinics", json={"name": name, "timezone": "UTC"}, headers=auth(admin))
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _front_desk(client: AsyncClient, admin: dict, clinic_id: str) -> dict:
    email = unique_email("billing-front-desk")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk", "clinic_id": clinic_id},
        headers=auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


@pytest_asyncio.fixture
async def billing_world(client: AsyncClient) -> BillingWorld:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _clinic(client, admin, "Billing Clinic")
    other_clinic_id = await _clinic(client, admin, "Billing Other Clinic")
    return BillingWorld(
        admin=admin,
        clinic_id=clinic_id,
        front_desk=await _front_desk(client, admin, clinic_id),
        other_clinic_id=other_clinic_id,
        other_front_desk=await _front_desk(client, admin, other_clinic_id),
    )
