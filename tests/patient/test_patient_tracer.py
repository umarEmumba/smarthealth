"""Tracer for plan 01.1-06: client -> gateway -> Patient (-> Organization).

One thin vertical slice proving the Patient service works before any expansion:
a clinic-bound front-desk account registers a patient through the gateway,
Patient validates the clinic with Organization (D-05, forwarding the caller's own
token), writes only to its OWN database (patient_db) and audits the mutation
with field NAMES only. A clinic Organization does not know is a 422 and leaves
no row behind.

Service-module imports are done inside the tests so a missing module fails the
target test, not collection of the whole file. All PHI values are synthetic.
"""

from __future__ import annotations

import json
import uuid

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login, unique_email

PASSWORD = "correct-horse-battery"

SYNTHETIC_PHI_NAME = "Zzyzx Tracerpatient Synthetic"
SYNTHETIC_PHI_DOB = "1991-02-03"
SYNTHETIC_PHI_PHONE = "+1-555-010-7777"
SYNTHETIC_PHI_ADDRESS = "77 Synthetic Tracer Road"

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _payload(clinic_id: str) -> dict:
    return {
        "clinic_id": clinic_id,
        "full_name": SYNTHETIC_PHI_NAME,
        "date_of_birth": SYNTHETIC_PHI_DOB,
        "phone": SYNTHETIC_PHI_PHONE,
        "address": SYNTHETIC_PHI_ADDRESS,
    }


async def _clinic_and_front_desk(client: AsyncClient) -> tuple[dict, str, dict]:
    """(admin tokens, clinic id, tokens of a front_desk account bound to that clinic)."""
    admin = await bootstrap_admin_and_login(client)
    created = await client.post(
        "/api/clinics",
        json={"name": "Patient Tracer Clinic", "timezone": "UTC"},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    clinic_id = created.json()["id"]

    email = unique_email("front-desk")
    account = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk", "clinic_id": clinic_id},
        headers=_auth(admin),
    )
    assert account.status_code == 201, account.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return admin, clinic_id, login.json()


async def _patient_rows(where: str, params: dict) -> int:
    """Committed-state count straight against patient_db (own short-lived engine)."""
    from services.patient.config import settings as patient_settings  # noqa: PLC0415

    engine = create_async_engine(patient_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(text(f"SELECT count(*) FROM patients WHERE {where}"), params)
    finally:
        await engine.dispose()


async def test_front_desk_registers_and_reads_patient_through_gateway(client: AsyncClient) -> None:
    _admin, clinic_id, front_desk = await _clinic_and_front_desk(client)

    created = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(front_desk))
    assert created.status_code == 201, created.text
    patient = created.json()
    assert patient["clinic_id"] == clinic_id
    assert patient["user_id"] is None  # a profile never implies a login account (D-04)
    assert patient["full_name"] == SYNTHETIC_PHI_NAME

    fetched = await client.get(
        f"/api/patients/{patient['id']}",
        params={"clinic_id": clinic_id},
        headers=_auth(front_desk),
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == patient

    # The row lives in patient_db (checked with an independent engine).
    assert await _patient_rows("id = :id", {"id": patient["id"]}) == 1


async def test_patient_create_audit_is_phi_safe(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    _admin, clinic_id, front_desk = await _clinic_and_front_desk(client)

    created = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(front_desk))
    assert created.status_code == 201, created.text
    patient_id = created.json()["id"]

    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    try:
        doc = await audit.find_one({"action": "patient.create", "entity_id": patient_id})
        assert doc is not None
        assert doc["outcome"] == "success"
        assert doc["entity_type"] == "patient"
        assert doc["clinic_id"] == clinic_id
        # PHI boundary: field NAMES only, never values.
        assert doc["changes"] == {"fields": ["full_name", "date_of_birth", "phone", "address"]}
        serialized = json.dumps(doc, default=str)
        for value in (
            SYNTHETIC_PHI_NAME,
            SYNTHETIC_PHI_DOB,
            SYNTHETIC_PHI_PHONE,
            SYNTHETIC_PHI_ADDRESS,
        ):
            assert value not in serialized
    finally:
        await audit.delete_many({"action": "patient.create", "entity_id": patient_id})


async def test_patient_create_with_unknown_clinic_rejected_and_not_written(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    unknown_clinic = str(uuid.uuid4())

    resp = await client.post("/api/patients", json=_payload(unknown_clinic), headers=_auth(admin))
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "clinic not found"

    # No half-created profile: nothing was written for that clinic.
    assert await _patient_rows("clinic_id = :c", {"c": unknown_clinic}) == 0

    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    await audit.delete_many({"action": "patient.create", "clinic_id": unknown_clinic})
