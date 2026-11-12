"""Patient's two cross-service references and the PHI boundary (plan 01.1-06, Task 2).

`patients.clinic_id` is Organization's and `patients.user_id` is Identity's. Both
are plain UUID columns, so Patient validates them at write time through the shared
reference client (D-05): a missing, inactive or out-of-scope target is a 422, an
owner that cannot answer cleanly is a 503, and in EVERY refusal no `patients` row
exists (no half-created profile). Reads return this service's own rows only and
never cascade from a deactivated clinic (D-06, D-07).

The PHI boundary (T-01.1-25, T-01.1-28): the internal calls made while creating a
patient carry ids only, and no other service's schema has a PHI column. All PHI
values below are synthetic.
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
from tests.conftest import RequestRecorder, bootstrap_admin_and_login, degraded_mesh, unique_email

PASSWORD = "correct-horse-battery"

SYNTHETIC_PHI_NAME = "Zzyzx Referencepatient Synthetic"
SYNTHETIC_PHI_DOB = "1987-06-05"
SYNTHETIC_PHI_PHONE = "+1-555-010-4242"
SYNTHETIC_PHI_ADDRESS = "42 Synthetic Reference Lane"
_PHI_VALUES = (SYNTHETIC_PHI_NAME, SYNTHETIC_PHI_DOB, SYNTHETIC_PHI_PHONE, SYNTHETIC_PHI_ADDRESS)

def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _payload(clinic_id: str, user_id: str | None = None) -> dict:
    body = {
        "clinic_id": clinic_id,
        "full_name": SYNTHETIC_PHI_NAME,
        "date_of_birth": SYNTHETIC_PHI_DOB,
        "phone": SYNTHETIC_PHI_PHONE,
        "address": SYNTHETIC_PHI_ADDRESS,
    }
    if user_id is not None:
        body["user_id"] = user_id
    return body


async def _create_clinic(client: AsyncClient, admin: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _create_account(
    client: AsyncClient, admin: dict, *, role: str, clinic_id: str | None
) -> tuple[str, dict]:
    """(user id, login tokens) of a staff account created by the admin."""
    email = unique_email(role)
    body = {"email": email, "password": PASSWORD, "role": role}
    if clinic_id is not None:
        body["clinic_id"] = clinic_id
    created = await client.post("/api/auth/admin/users", json=body, headers=_auth(admin))
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return created.json()["id"], login.json()


async def _register_patient_account(client: AsyncClient) -> str:
    """A clinic-less patient-role login account (public registration); returns its user id."""
    resp = await client.post(
        "/api/auth/register", json={"email": unique_email("patient"), "password": PASSWORD}
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _rows(where: str, params: dict) -> int:
    """Committed-state count straight against patient_db (own short-lived engine)."""
    from services.patient.config import settings as patient_settings  # noqa: PLC0415

    engine = create_async_engine(patient_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(text(f"SELECT count(*) FROM patients WHERE {where}"), params)
    finally:
        await engine.dispose()


async def _cleanup_audit(mongo: AsyncMongoClient, *clinic_ids: str) -> None:
    audit = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    await audit.delete_many({"action": "patient.create", "clinic_id": {"$in": list(clinic_ids)}})


@pytest.mark.integration
async def test_inactive_clinic_rejected_and_not_written(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Soon Inactive Clinic")
    deactivated = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200, deactivated.text

    resp = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(admin))
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "clinic is inactive"
    assert await _rows("clinic_id = :c", {"c": clinic_id}) == 0

    # The refusal is audited as a failure, and the audit document holds no PHI.
    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    try:
        doc = await audit.find_one(
            {"action": "patient.create", "outcome": "failure", "clinic_id": clinic_id}
        )
        assert doc is not None
        assert doc["entity_id"] == "unknown"
        serialized = json.dumps(doc, default=str)
        assert not [v for v in _PHI_VALUES if v in serialized]
    finally:
        await _cleanup_audit(mongo_client, clinic_id)


@pytest.mark.integration
async def test_unknown_user_id_rejected_and_not_written(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Unknown User Clinic")
    ghost = str(uuid.uuid4())

    resp = await client.post(
        "/api/patients", json=_payload(clinic_id, user_id=ghost), headers=_auth(admin)
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "user not found"
    assert await _rows("user_id = :u OR clinic_id = :c", {"u": ghost, "c": clinic_id}) == 0


@pytest.mark.integration
async def test_front_desk_cannot_link_account_of_another_clinic(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_a = await _create_clinic(client, admin, "Link Clinic A")
    clinic_b = await _create_clinic(client, admin, "Link Clinic B")
    _fd_id, front_desk_a = await _create_account(
        client, admin, role="front_desk", clinic_id=clinic_a
    )
    account_b, _tokens = await _create_account(client, admin, role="front_desk", clinic_id=clinic_b)

    resp = await client.post(
        "/api/patients",
        json=_payload(clinic_a, user_id=account_b),
        headers=_auth(front_desk_a),
    )
    # Identity answers 404 for an out-of-scope account, so it reads as "not found".
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "user not found"
    assert await _rows("clinic_id = :c", {"c": clinic_a}) == 0


@pytest.mark.integration
async def test_front_desk_links_clinic_less_patient_account(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Linkable Clinic")
    _fd_id, front_desk = await _create_account(
        client, admin, role="front_desk", clinic_id=clinic_id
    )
    account = await _register_patient_account(client)

    resp = await client.post(
        "/api/patients", json=_payload(clinic_id, user_id=account), headers=_auth(front_desk)
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["user_id"] == account
    assert await _rows("user_id = :u", {"u": account}) == 1


@pytest.mark.integration
async def test_second_profile_for_same_account_is_409_and_not_written(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Duplicate Link Clinic")
    account = await _register_patient_account(client)

    first = await client.post(
        "/api/patients", json=_payload(clinic_id, user_id=account), headers=_auth(admin)
    )
    assert first.status_code == 201, first.text

    second = await client.post(
        "/api/patients", json=_payload(clinic_id, user_id=account), headers=_auth(admin)
    )
    assert second.status_code == 409, second.text
    assert second.json()["detail"] == "account is already linked to a patient profile"
    assert await _rows("user_id = :u", {"u": account}) == 1

    await _cleanup_audit(mongo_client, clinic_id)


@pytest.mark.integration
async def test_create_when_organization_unreachable_is_503_and_not_written(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Org Down Create Clinic")

    async with degraded_mesh("organization"):
        resp = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(admin))

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "organization service unavailable"
    assert await _rows("clinic_id = :c", {"c": clinic_id}) == 0


@pytest.mark.integration
async def test_list_when_organization_unreachable_is_503(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Org Down List Clinic")

    async with degraded_mesh("organization"):
        resp = await client.get(
            "/api/patients", params={"clinic_id": clinic_id}, headers=_auth(admin)
        )

    assert resp.status_code == 503, resp.text


@pytest.mark.integration
async def test_user_link_when_identity_unreachable_is_503_and_not_written(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Identity Down Clinic")
    account = await _register_patient_account(client)

    # Warm Patient's JWKS cache so token verification does not need Identity
    # while it is down; only the account-link check should be affected.
    warm = await client.get("/api/patients", params={"clinic_id": clinic_id}, headers=_auth(admin))
    assert warm.status_code == 200, warm.text

    async with degraded_mesh("identity"):
        resp = await client.post(
            "/api/patients", json=_payload(clinic_id, user_id=account), headers=_auth(admin)
        )

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "identity service unavailable"
    assert await _rows("clinic_id = :c", {"c": clinic_id}) == 0


@pytest.mark.integration
async def test_deactivated_clinics_patients_are_still_listed(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Deactivated Listing Clinic")
    created = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(admin))
    assert created.status_code == 201, created.text
    patient_id = created.json()["id"]

    deactivated = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200, deactivated.text

    # D-06: deactivating a clinic never cascades, and reads of its patients still work.
    listing = await client.get("/api/patients", params={"clinic_id": clinic_id}, headers=_auth(admin))
    assert listing.status_code == 200, listing.text
    assert [p["id"] for p in listing.json()] == [patient_id]
    assert listing.json()[0]["is_active"] is True


@pytest.mark.integration
async def test_list_response_holds_only_patient_fields(client: AsyncClient) -> None:
    """D-07: the clinic lookup only validates the scope parameter; nothing from
    Organization is merged into the response."""
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Id Only Clinic")
    created = await client.post("/api/patients", json=_payload(clinic_id), headers=_auth(admin))
    assert created.status_code == 201, created.text

    listing = await client.get("/api/patients", params={"clinic_id": clinic_id}, headers=_auth(admin))
    assert listing.status_code == 200, listing.text
    assert set(listing.json()[0]) == {
        "id",
        "clinic_id",
        "full_name",
        "date_of_birth",
        "phone",
        "address",
        "user_id",
        "is_active",
        "deactivated_at",
    }


@pytest.mark.integration
async def test_internal_calls_carry_no_phi(
    client: AsyncClient, request_recorder: RequestRecorder, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "No PHI On The Wire Clinic")
    account = await _register_patient_account(client)

    created = await client.post(
        "/api/patients", json=_payload(clinic_id, user_id=account), headers=_auth(admin)
    )
    assert created.status_code == 201, created.text

    internal = [r for r in request_recorder.requests if r.url.path.startswith("/internal")]
    # Both reference checks were made (otherwise this test proves nothing).
    paths = [r.url.path for r in internal]
    assert f"/internal/clinics/{clinic_id}" in paths
    assert f"/internal/users/{account}" in paths

    for request in internal:
        wire = " ".join(
            [str(request.url)]
            + [f"{name}: {value}" for name, value in request.headers.items()]
            + [request.content.decode("utf-8", errors="replace")]
        )
        leaked = [v for v in _PHI_VALUES if v in wire]
        assert not leaked, f"PHI value present in an internal call to {request.url.path}"
        assert request.method == "GET"
        assert request.content == b""

    await _cleanup_audit(mongo_client, clinic_id)


def test_no_other_service_schema_holds_patient_phi() -> None:
    from services.identity.repository.models import IdentityBase  # noqa: PLC0415
    from services.organization.repository.models import OrganizationBase  # noqa: PLC0415
    from services.patient.repository.models import PatientBase  # noqa: PLC0415

    phi_columns = {"date_of_birth", "phone", "address"}
    for base in (IdentityBase, OrganizationBase):
        for table in base.metadata.tables.values():
            assert not (phi_columns & {c.name for c in table.columns}), table.name

    assert phi_columns <= {c.name for c in PatientBase.metadata.tables["patients"].columns}
