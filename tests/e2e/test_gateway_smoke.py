"""Real-stack smoke test: the Phase 1 flow through the PUBLISHED gateway port.

Unlike every other suite, nothing runs in-process here: the six application
services are separate containers started by `docker compose up`, and this test
talks to them only through the gateway's host port with a plain HTTP client —
exactly what an external client would do (OBS-04, roadmap SC1 and SC5).

Flow: seed an admin (straight into identity_db through the published Postgres
port), then through the gateway: health, JWKS, login, admin ping, clinic ->
department -> provider (one specialty) -> availability template, a clinic-bound
front-desk account, a patient created and listed by that account, and a
negative check that `/internal/*` has no route at the edge. Finally it asserts
that audit documents written by three DIFFERENT containers (Organization,
Provider, Patient) exist in the shared Mongo audit sink (IAM-04).

Run (stack up): `pytest tests/e2e/test_gateway_smoke.py -m e2e`.
Environment: `SMARTHEALTH_GATEWAY_URL` (default http://localhost:8000). Without a
reachable gateway the test is skipped, unless `SMARTHEALTH_E2E_REQUIRE_STACK=1`
is set, in which case it fails (use that in CI so a missing stack is not green).
All data is synthetic.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from pymongo import AsyncMongoClient

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import insert_admin_user, unique_email

pytestmark = pytest.mark.e2e

GATEWAY_URL = os.environ.get("SMARTHEALTH_GATEWAY_URL", "http://localhost:8000").rstrip("/")
PASSWORD = "correct-horse-battery"


def _auth(tokens: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=GATEWAY_URL, timeout=15.0) as http:
        try:
            await http.get("/health")
        except httpx.TransportError as exc:
            message = f"gateway not reachable at {GATEWAY_URL} ({type(exc).__name__}); start the stack first"
            if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
                pytest.fail(message)
            pytest.skip(message)
        yield http


async def _login(gateway: httpx.AsyncClient, email: str) -> dict:
    resp = await gateway.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _audit_doc(collection, action: str, entity_id: str) -> dict | None:
    """The audit write is synchronous with the request in the owning service, but
    allow a short grace period before declaring it missing."""
    for _ in range(10):
        doc = await collection.find_one({"action": action, "entity_id": entity_id})
        if doc is not None:
            return doc
        await asyncio.sleep(0.5)
    return None


async def test_phase1_flow_through_the_published_gateway(gateway: httpx.AsyncClient) -> None:
    # --- edge basics -----------------------------------------------------------
    health = await gateway.get("/health")
    assert health.status_code == 200, health.text
    assert health.json() == {"status": "ok", "service": "gateway"}

    jwks = await gateway.get("/.well-known/jwks.json")
    assert jwks.status_code == 200, jwks.text
    assert len(jwks.json()["keys"]) == 1

    # --- admin ------------------------------------------------------------------
    admin_email = unique_email("e2e-admin")
    await insert_admin_user(admin_email, PASSWORD)
    admin = await _login(gateway, admin_email)

    ping = await gateway.get("/api/admin/ping", headers=_auth(admin))
    assert ping.status_code == 200, ping.text

    # --- clinic -> department -> provider -> availability ------------------------
    clinic = await gateway.post(
        "/api/clinics",
        json={"name": f"E2E Clinic {uuid.uuid4().hex[:8]}", "timezone": "UTC"},
        headers=_auth(admin),
    )
    assert clinic.status_code == 201, clinic.text
    clinic_id = clinic.json()["id"]

    department = await gateway.post(
        f"/api/clinics/{clinic_id}/departments",
        json={"name": "E2E General"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    department_id = department.json()["id"]

    provider = await gateway.post(
        "/api/providers",
        json={
            "department_id": department_id,
            "full_name": "Dr. E2E Synthetic",
            "specialties": ["E2E General Medicine"],
        },
        headers=_auth(admin),
    )
    assert provider.status_code == 201, provider.text
    provider_id = provider.json()["id"]
    assert [s["name"] for s in provider.json()["specialties"]] == ["E2E General Medicine"]

    availability = await gateway.post(
        f"/api/providers/{provider_id}/availability",
        json={
            "day_of_week": 2,
            "start_time": "09:00:00",
            "end_time": "12:00:00",
            "slot_duration_minutes": 30,
            "effective_from": "2026-01-05",
            "effective_to": "2026-12-31",
        },
        headers=_auth(admin),
    )
    assert availability.status_code == 201, availability.text
    assert availability.json()["provider_id"] == provider_id

    # --- clinic-bound front desk -> patient --------------------------------------
    front_desk_email = unique_email("e2e-front-desk")
    account = await gateway.post(
        "/api/auth/admin/users",
        json={
            "email": front_desk_email,
            "password": PASSWORD,
            "role": "front_desk",
            "clinic_id": clinic_id,
        },
        headers=_auth(admin),
    )
    assert account.status_code == 201, account.text
    front_desk = await _login(gateway, front_desk_email)

    patient = await gateway.post(
        "/api/patients",
        json={
            "clinic_id": clinic_id,
            "full_name": "Zzyzx E2E Synthetic",
            "date_of_birth": "1990-01-02",
            "phone": "+1-555-010-0000",
            "address": "1 Synthetic Road",
        },
        headers=_auth(front_desk),
    )
    assert patient.status_code == 201, patient.text
    patient_id = patient.json()["id"]

    listed = await gateway.get("/api/patients", params={"clinic_id": clinic_id}, headers=_auth(front_desk))
    assert listed.status_code == 200, listed.text
    assert patient_id in [p["id"] for p in listed.json()]

    # --- /internal has no route at the edge ---------------------------------------
    internal = await gateway.get(f"/internal/departments/{department_id}", headers=_auth(admin))
    assert internal.status_code == 404, internal.text
    internal_provider = await gateway.get(f"/internal/providers/{provider_id}", headers=_auth(admin))
    assert internal_provider.status_code == 404, internal_provider.text

    # --- audit trail written by three different containers ------------------------
    mongo: AsyncMongoClient = AsyncMongoClient(shared_settings.mongo_url)
    try:
        audit = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
        for action, entity_id in (
            ("clinic.create", clinic_id),
            ("provider.create", provider_id),
            ("patient.create", patient_id),
        ):
            doc = await _audit_doc(audit, action, entity_id)
            assert doc is not None, f"no audit document for {action} {entity_id}"
            assert doc.get("outcome") == "success"
    finally:
        await mongo.close()
