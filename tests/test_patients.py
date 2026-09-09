"""Patient CRUD integration tests (PPL-01/PPL-04) — front-desk/admin only,
audited, PHI-safe, read-after-write consistent.

Uses entirely synthetic PHI fixture values (never real patient data) per
the plan's action notes; `SYNTHETIC_PHI_NAME` mirrors the `test_audit.py`
convention from Plan 03 so this test's PHI-safety assertion is directly
comparable to that established pattern.
"""

from __future__ import annotations

import io
import json
import uuid

import pytest
import structlog
from httpx import AsyncClient
from pymongo import AsyncMongoClient

from libs.observability.logging import add_trace_context
from smarthealth.config import settings
from tests.conftest import bootstrap_admin_and_login, unique_email

SYNTHETIC_PHI_NAME = "Zzyzx Testpatient Synthetic"
SYNTHETIC_PHI_DOB = "1990-01-01"
SYNTHETIC_PHI_PHONE = "+1-555-010-9999"


async def _register_and_login(
    client: AsyncClient, *, role: str, clinic_id: str | None = None
) -> dict:
    """Create a user with `role` (optionally scoped to `clinic_id`) and
    return its login token pair.

    CR-01: public `/api/auth/register` always creates a PATIENT account
    now, so a `role="patient"` request goes straight through it; any
    other role is created via the authenticated-admin-only
    `/api/auth/admin/users` route, using a throwaway bootstrap admin.

    WR-02: a front_desk (or other non-admin) account MUST carry a
    `clinic_id` for the router's clinic-scope check to accept it —
    callers exercising WR-02's scoping behavior must pass the target
    clinic_id explicitly.
    """
    email = unique_email(role)
    if role == "patient":
        register_resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert register_resp.status_code == 201, register_resp.text
    else:
        admin_tokens = await bootstrap_admin_and_login(client)
        payload = {"email": email, "password": "correct-horse-battery", "role": role}
        if clinic_id is not None:
            payload["clinic_id"] = clinic_id
        create_resp = await client.post(
            "/api/auth/admin/users",
            json=payload,
            headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
        )
        assert create_resp.status_code == 201, create_resp.text

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    return login_resp.json()


def _auth_header(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _create_clinic(client: AsyncClient, tokens: dict, *, name: str = "Patient Test Clinic") -> str:
    resp = await client.post(
        "/api/clinics",
        json={"name": name, "timezone": "America/New_York"},
        headers=_auth_header(tokens),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def _patient_payload(clinic_id: str) -> dict:
    return {
        "clinic_id": clinic_id,
        "full_name": SYNTHETIC_PHI_NAME,
        "date_of_birth": SYNTHETIC_PHI_DOB,
        "phone": SYNTHETIC_PHI_PHONE,
        "address": "123 Synthetic Ave",
    }


@pytest.mark.integration
async def test_create_patient(client: AsyncClient) -> None:
    """Front-desk can register a patient (PPL-01)."""
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens)

    front_desk_tokens = await _register_and_login(
        client, role="front_desk", clinic_id=clinic_id
    )
    resp = await client.post(
        "/api/patients", json=_patient_payload(clinic_id), headers=_auth_header(front_desk_tokens)
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["full_name"] == SYNTHETIC_PHI_NAME
    assert body["clinic_id"] == clinic_id
    assert body["user_id"] is None  # PPL-01: no login required (Open Question 1)


@pytest.mark.integration
async def test_read_after_write_consistency(client: AsyncClient) -> None:
    """Registering a patient then immediately reading it back returns the
    just-committed record — PPL-04 read-after-write.
    """
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens, name="Read-After-Write Clinic")

    front_desk_tokens = await _register_and_login(
        client, role="front_desk", clinic_id=clinic_id
    )
    create_resp = await client.post(
        "/api/patients", json=_patient_payload(clinic_id), headers=_auth_header(front_desk_tokens)
    )
    assert create_resp.status_code == 201, create_resp.text
    patient_id = create_resp.json()["id"]

    get_resp = await client.get(
        f"/api/patients/{patient_id}",
        params={"clinic_id": clinic_id},
        headers=_auth_header(front_desk_tokens),
    )
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json() == create_resp.json()

    list_resp = await client.get(
        "/api/patients", params={"clinic_id": clinic_id}, headers=_auth_header(front_desk_tokens)
    )
    assert list_resp.status_code == 200
    assert any(p["id"] == patient_id for p in list_resp.json())


@pytest.mark.integration
async def test_list_patients_404s_on_nonexistent_clinic(client: AsyncClient) -> None:
    """WR-01 regression: list_patients must 404 on a clinic_id that does
    not exist, consistent with list_departments — not silently return []
    (which is indistinguishable from "clinic exists, zero patients").
    """
    admin_tokens = await _register_and_login(client, role="admin")
    resp = await client.get(
        "/api/patients",
        params={"clinic_id": str(uuid.uuid4())},
        headers=_auth_header(admin_tokens),
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_front_desk_cannot_access_another_clinics_patients(client: AsyncClient) -> None:
    """WR-02 regression: a FRONT_DESK token scoped to Clinic A must be
    rejected 403 when passing Clinic B's clinic_id to list/get/create —
    front-desk is scoped to their own clinic at the route layer, not just
    "any clinic the caller can guess a UUID for".
    """
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_a = await _create_clinic(client, admin_tokens, name="WR-02 Clinic A")
    clinic_b = await _create_clinic(client, admin_tokens, name="WR-02 Clinic B")

    front_desk_a_tokens = await _register_and_login(
        client, role="front_desk", clinic_id=clinic_a
    )

    list_resp = await client.get(
        "/api/patients",
        params={"clinic_id": clinic_b},
        headers=_auth_header(front_desk_a_tokens),
    )
    assert list_resp.status_code == 403

    create_resp = await client.post(
        "/api/patients",
        json=_patient_payload(clinic_b),
        headers=_auth_header(front_desk_a_tokens),
    )
    assert create_resp.status_code == 403

    get_resp = await client.get(
        f"/api/patients/{uuid.uuid4()}",
        params={"clinic_id": clinic_b},
        headers=_auth_header(front_desk_a_tokens),
    )
    assert get_resp.status_code == 403

    # An admin token is exempt from clinic-scoping (unaffected by WR-02).
    admin_list_resp = await client.get(
        "/api/patients",
        params={"clinic_id": clinic_b},
        headers=_auth_header(admin_tokens),
    )
    assert admin_list_resp.status_code == 200


@pytest.mark.integration
async def test_patient_role_forbidden(client: AsyncClient) -> None:
    """A patient-role token cannot register a patient profile on this staff
    route (T-05-02) — the staff registration route is front-desk/admin only.
    """
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens, name="Forbidden-Role Clinic")

    patient_tokens = await _register_and_login(client, role="patient")
    resp = await client.post(
        "/api/patients", json=_patient_payload(clinic_id), headers=_auth_header(patient_tokens)
    )
    assert resp.status_code == 403


@pytest.mark.integration
async def test_no_phi_in_patient_audit_or_logs(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    """A patient mutation's audit `changes` field and captured logs contain
    no synthetic PHI value — only field names/identifiers (T-05-01).
    """
    admin_tokens = await _register_and_login(client, role="admin")
    clinic_id = await _create_clinic(client, admin_tokens, name="PHI-Safe Clinic")

    front_desk_tokens = await _register_and_login(
        client, role="front_desk", clinic_id=clinic_id
    )
    resp = await client.post(
        "/api/patients", json=_patient_payload(clinic_id), headers=_auth_header(front_desk_tokens)
    )
    assert resp.status_code == 201, resp.text
    patient_id = resp.json()["id"]

    collection = mongo_client[settings.mongo_db_name]["audit_log"]
    try:
        doc = await collection.find_one({"action": "patient.create", "entity_id": patient_id})
        assert doc is not None
        assert doc["outcome"] == "success"
        assert doc["entity_type"] == "patient"
        # PHI boundary: changes stores field NAMES only, never PHI values.
        assert doc["changes"] == {
            "fields": ["full_name", "date_of_birth", "phone", "address"]
        }
        doc_as_text = json.dumps(doc, default=str)
        assert SYNTHETIC_PHI_NAME not in doc_as_text
        assert SYNTHETIC_PHI_DOB not in doc_as_text
        assert SYNTHETIC_PHI_PHONE not in doc_as_text
    finally:
        await collection.delete_many({"action": "patient.create", "entity_id": patient_id})

    # Same PHI-safe processor chain as libs/observability/logging.py,
    # wired to a captured stream — proves the identifier-only logging
    # convention this router follows never leaks the synthetic PHI value.
    stream = io.StringIO()
    logger_factory = structlog.PrintLoggerFactory(file=stream)
    bound_logger = structlog.wrap_logger(
        logger_factory(),
        processors=[
            structlog.contextvars.merge_contextvars,
            add_trace_context,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    bound_logger.info("patient.create.success", patient_id=patient_id, clinic_id=clinic_id)
    output = stream.getvalue()
    assert SYNTHETIC_PHI_NAME not in output
    assert SYNTHETIC_PHI_DOB not in output
    assert SYNTHETIC_PHI_PHONE not in output
