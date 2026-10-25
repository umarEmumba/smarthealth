"""`GET /api/patients/me` (plan 02.1-12, C4): a patient reads their own profile.

Everything goes through the gateway with synthetic data. The route has no id
parameter, so the only thing that decides whose profile is returned is the
verified token (its `sub` and clinic claim).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from services.patient.config import settings as patient_settings
from tests.conftest import bootstrap_admin_and_login, unique_email

PASSWORD = "correct-horse-battery"
SELF_NAME = "Zzyzx Selfview Synthetic"
SELF_PHONE = "+1-555-010-7777"
SELF_ADDRESS = "1 Synthetic Way"
OTHER_NAME = "Qqplex Otherview Synthetic"
OTHER_PHONE = "+1-555-010-8888"


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _ok(response, expected: int = 201) -> dict:
    assert response.status_code == expected, response.text
    return response.json()


@dataclass
class Account:
    user_id: str
    tokens: dict
    patient_id: str | None = None


@dataclass
class Setup:
    admin: dict
    clinic_id: str
    other_clinic_id: str
    front_desk: dict


async def _patient_db(statement: str, **params) -> None:
    """Committed write straight into patient_db for a row this test created,
    to build states the API refuses to create (own short-lived engine)."""
    engine = create_async_engine(patient_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(statement), params)
    finally:
        await engine.dispose()


async def _login(client: AsyncClient, email: str) -> dict:
    return _ok(await client.post("/api/auth/login", json={"email": email, "password": PASSWORD}), 200)


async def _setup(client: AsyncClient) -> Setup:
    admin = await bootstrap_admin_and_login(client)
    clinic = _ok(
        await client.post(
            "/api/clinics",
            json={"name": f"Self Clinic {uuid.uuid4().hex[:6]}", "timezone": "America/New_York"},
            headers=_auth(admin),
        )
    )
    other = _ok(
        await client.post(
            "/api/clinics",
            json={"name": f"Other Clinic {uuid.uuid4().hex[:6]}", "timezone": "America/New_York"},
            headers=_auth(admin),
        )
    )
    email = unique_email("front-desk")
    _ok(
        await client.post(
            "/api/auth/admin/users",
            json={
                "email": email,
                "password": PASSWORD,
                "role": "front_desk",
                "clinic_id": clinic["id"],
            },
            headers=_auth(admin),
        )
    )
    return Setup(
        admin=admin,
        clinic_id=clinic["id"],
        other_clinic_id=other["id"],
        front_desk=await _login(client, email),
    )


async def _patient_account(
    client: AsyncClient,
    setup: Setup,
    *,
    name: str | None = SELF_NAME,
    phone: str | None = SELF_PHONE,
    clinic_id: str | None = None,
) -> Account:
    """A self-registered PATIENT login, linked to a profile when `name` is given."""
    clinic_id = clinic_id or setup.clinic_id
    email = unique_email("patient")
    created = _ok(
        await client.post(
            "/api/auth/register",
            json={"email": email, "password": PASSWORD, "clinic_id": clinic_id},
        )
    )
    account = Account(user_id=created["id"], tokens=await _login(client, email))
    if name is not None:
        patient = _ok(
            await client.post(
                "/api/patients",
                json={
                    "clinic_id": clinic_id,
                    "full_name": name,
                    "date_of_birth": "1990-01-01",
                    "phone": phone,
                    "address": SELF_ADDRESS,
                    "user_id": account.user_id,
                },
                headers=_auth(setup.front_desk if clinic_id == setup.clinic_id else setup.admin),
            )
        )
        account.patient_id = patient["id"]
    return account


@pytest.mark.integration
async def test_patient_reads_their_own_profile(client: AsyncClient) -> None:
    setup = await _setup(client)
    me = await _patient_account(client, setup)

    resp = await client.get("/api/patients/me", headers=_auth(me.tokens))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == me.patient_id
    assert body["clinic_id"] == setup.clinic_id
    assert body["user_id"] == me.user_id
    assert body["full_name"] == SELF_NAME
    assert body["date_of_birth"] == "1990-01-01"
    assert body["phone"] == SELF_PHONE
    assert body["address"] == SELF_ADDRESS
    assert body["is_active"] is True


@pytest.mark.integration
async def test_response_carries_exactly_the_staff_read_fields(client: AsyncClient) -> None:
    setup = await _setup(client)
    me = await _patient_account(client, setup)

    own = (await client.get("/api/patients/me", headers=_auth(me.tokens))).json()
    staff = (
        await client.get(
            f"/api/patients/{me.patient_id}",
            params={"clinic_id": setup.clinic_id},
            headers=_auth(setup.front_desk),
        )
    ).json()

    assert own == staff


@pytest.mark.integration
async def test_account_without_a_linked_profile_is_404(client: AsyncClient) -> None:
    setup = await _setup(client)
    unlinked = await _patient_account(client, setup, name=None)

    resp = await client.get("/api/patients/me", headers=_auth(unlinked.tokens))

    assert resp.status_code == 404
    assert resp.json() == {"detail": "patient profile not found"}


@pytest.mark.integration
async def test_deactivated_profile_is_404(client: AsyncClient) -> None:
    setup = await _setup(client)
    me = await _patient_account(client, setup)
    assert (await client.get("/api/patients/me", headers=_auth(me.tokens))).status_code == 200

    deactivated = await client.post(
        f"/api/patients/{me.patient_id}/deactivate",
        params={"clinic_id": setup.clinic_id},
        headers=_auth(setup.front_desk),
    )
    assert deactivated.status_code == 200, deactivated.text

    resp = await client.get("/api/patients/me", headers=_auth(me.tokens))
    assert resp.status_code == 404
    assert resp.json() == {"detail": "patient profile not found"}


@pytest.mark.integration
async def test_two_patients_each_read_only_their_own_profile(client: AsyncClient) -> None:
    setup = await _setup(client)
    first = await _patient_account(client, setup)
    second = await _patient_account(client, setup, name=OTHER_NAME, phone=OTHER_PHONE)

    first_body = (await client.get("/api/patients/me", headers=_auth(first.tokens))).json()
    second_body = (await client.get("/api/patients/me", headers=_auth(second.tokens))).json()

    assert first_body["id"] == first.patient_id and first_body["full_name"] == SELF_NAME
    assert second_body["id"] == second.patient_id and second_body["full_name"] == OTHER_NAME
    assert first_body["id"] != second_body["id"]
    assert OTHER_NAME not in str(first_body) and SELF_NAME not in str(second_body)


@pytest.mark.integration
async def test_the_route_takes_no_identifier(client: AsyncClient) -> None:
    """Query parameters cannot redirect the read to someone else's profile."""
    setup = await _setup(client)
    first = await _patient_account(client, setup)
    second = await _patient_account(client, setup, name=OTHER_NAME, phone=OTHER_PHONE)

    resp = await client.get(
        "/api/patients/me",
        params={
            "patient_id": second.patient_id,
            "user_id": second.user_id,
            "clinic_id": setup.other_clinic_id,
        },
        headers=_auth(first.tokens),
    )

    assert resp.status_code == 200
    assert resp.json()["id"] == first.patient_id


@pytest.mark.integration
async def test_token_clinic_that_differs_from_the_profile_clinic_is_404(client: AsyncClient) -> None:
    """The profile row sits in clinic B while the token's clinic claim says A.

    The API cannot link an account to a profile of another clinic, so the
    mismatch is built by moving this test's own profile row in patient_db.
    """
    setup = await _setup(client)
    me = await _patient_account(client, setup)
    assert (await client.get("/api/patients/me", headers=_auth(me.tokens))).status_code == 200
    await _patient_db(
        "UPDATE patients SET clinic_id = :c WHERE id = :p",
        c=uuid.UUID(setup.other_clinic_id),
        p=uuid.UUID(me.patient_id),
    )

    resp = await client.get("/api/patients/me", headers=_auth(me.tokens))

    assert resp.status_code == 404
    assert resp.json() == {"detail": "patient profile not found"}


@pytest.mark.integration
async def test_patient_without_a_clinic_claim_is_404(client: AsyncClient) -> None:
    """An account registered without a clinic has no clinic claim; even a
    profile linked to it (inserted directly, the API would refuse) is not found."""
    setup = await _setup(client)
    email = unique_email("patient")
    created = _ok(
        await client.post("/api/auth/register", json={"email": email, "password": PASSWORD})
    )
    tokens = await _login(client, email)
    await _patient_db(
        "INSERT INTO patients (id, clinic_id, full_name, date_of_birth, user_id, is_active) "
        "VALUES (:id, :c, :n, '1990-01-01', :u, true)",
        id=uuid.uuid4(),
        c=uuid.UUID(setup.clinic_id),
        n=SELF_NAME,
        u=uuid.UUID(created["id"]),
    )

    resp = await client.get("/api/patients/me", headers=_auth(tokens))

    assert resp.status_code == 404
    assert resp.json() == {"detail": "patient profile not found"}


@pytest.mark.integration
@pytest.mark.parametrize("role", ["front_desk", "provider", "admin"])
async def test_staff_roles_cannot_use_the_self_route(client: AsyncClient, role: str) -> None:
    setup = await _setup(client)
    if role == "admin":
        tokens = setup.admin
    elif role == "front_desk":
        tokens = setup.front_desk
    else:
        email = unique_email("provider")
        _ok(
            await client.post(
                "/api/auth/admin/users",
                json={
                    "email": email,
                    "password": PASSWORD,
                    "role": "provider",
                    "clinic_id": setup.clinic_id,
                },
                headers=_auth(setup.admin),
            )
        )
        tokens = await _login(client, email)

    resp = await client.get("/api/patients/me", headers=_auth(tokens))

    assert resp.status_code == 403


@pytest.mark.integration
async def test_no_token_is_401(client: AsyncClient) -> None:
    resp = await client.get("/api/patients/me")

    assert resp.status_code == 401


@pytest.mark.integration
async def test_a_patient_still_cannot_use_the_staff_routes(client: AsyncClient) -> None:
    """The self route did not open anything else to the PATIENT role."""
    setup = await _setup(client)
    me = await _patient_account(client, setup)

    by_id = await client.get(
        f"/api/patients/{me.patient_id}",
        params={"clinic_id": setup.clinic_id},
        headers=_auth(me.tokens),
    )
    listing = await client.get(
        "/api/patients", params={"clinic_id": setup.clinic_id}, headers=_auth(me.tokens)
    )

    assert by_id.status_code == 403
    assert listing.status_code == 403


@pytest.mark.integration
async def test_self_read_logs_ids_only(client: AsyncClient, capsys: pytest.CaptureFixture[str]) -> None:
    setup = await _setup(client)
    me = await _patient_account(client, setup)
    capsys.readouterr()

    resp = await client.get("/api/patients/me", headers=_auth(me.tokens))
    assert resp.status_code == 200
    output = capsys.readouterr().out

    assert "patient.me" in output
    assert me.patient_id in output
    for value in (SELF_NAME, "Selfview", SELF_PHONE, SELF_ADDRESS, "1990-01-01"):
        assert value not in output
