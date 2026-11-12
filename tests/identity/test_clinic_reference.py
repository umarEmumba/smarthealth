"""D-05 tracer: Identity validates `users.clinic_id` against Organization before
writing (plan 01.1-05, Task 1).

The path under test: an admin creates a clinic-scoped staff account through the
gateway; Identity makes a synchronous reference check to Organization's
`/internal/clinics/{id}` (forwarding the admin's own token) BEFORE inserting the
row. Contract: a missing or inactive clinic is a 422, an unreachable Organization
is a 503, and in every refusal nothing is written (no users row) while the
attempt is still audited as a failure. Without a `clinic_id` no cross-service call
is made at all.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import RequestRecorder, bootstrap_admin_and_login, degraded_mesh, unique_email

PASSWORD = "correct-horse-battery"

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _create_clinic(client: AsyncClient, admin: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _user_row_exists(email: str) -> bool:
    """Committed-state check straight against identity_db (own short-lived engine)."""
    from services.identity.config import settings as identity_settings  # noqa: PLC0415
    from services.identity.repository.models import User  # noqa: PLC0415

    engine = create_async_engine(identity_settings.database_url)
    try:
        factory = async_sessionmaker(bind=engine, expire_on_commit=False)
        async with factory() as session:
            found = await session.execute(select(User.id).where(User.email == email))
            return found.first() is not None
    finally:
        await engine.dispose()


async def _failure_audit_count(mongo: AsyncMongoClient, clinic_id: str) -> int:
    collection = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    return await collection.count_documents(
        {"action": "user.create", "outcome": "failure", "clinic_id": clinic_id}
    )


async def _cleanup_audit(mongo: AsyncMongoClient, clinic_id: str) -> None:
    collection = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    await collection.delete_many({"action": "user.create", "clinic_id": clinic_id})


async def test_staff_user_with_existing_clinic_created(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Reference Clinic")

    resp = await client.post(
        "/api/auth/admin/users",
        json={
            "email": unique_email("staff-ok"),
            "password": PASSWORD,
            "role": "front_desk",
            "clinic_id": clinic_id,
        },
        headers=_auth(admin),
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["clinic_id"] == clinic_id


async def test_staff_user_with_missing_clinic_rejected_422(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    email = unique_email("staff-missing")
    missing_clinic_id = str(uuid.uuid4())

    try:
        resp = await client.post(
            "/api/auth/admin/users",
            json={
                "email": email,
                "password": PASSWORD,
                "role": "front_desk",
                "clinic_id": missing_clinic_id,
            },
            headers=_auth(admin),
        )

        assert resp.status_code == 422, resp.text
        assert resp.json() == {"detail": "clinic not found"}
        assert not await _user_row_exists(email)
        assert await _failure_audit_count(mongo_client, missing_clinic_id) == 1
    finally:
        await _cleanup_audit(mongo_client, missing_clinic_id)


async def test_staff_user_with_inactive_clinic_rejected_422(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Soon Inactive Reference Clinic")
    deactivated = await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(admin))
    assert deactivated.status_code == 200, deactivated.text
    email = unique_email("staff-inactive")

    try:
        resp = await client.post(
            "/api/auth/admin/users",
            json={
                "email": email,
                "password": PASSWORD,
                "role": "front_desk",
                "clinic_id": clinic_id,
            },
            headers=_auth(admin),
        )

        assert resp.status_code == 422, resp.text
        assert resp.json() == {"detail": "clinic is inactive"}
        assert not await _user_row_exists(email)
        assert await _failure_audit_count(mongo_client, clinic_id) == 1
    finally:
        await _cleanup_audit(mongo_client, clinic_id)


async def test_staff_user_when_organization_unreachable_503(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Unreachable Owner Clinic")
    email = unique_email("staff-degraded")

    try:
        async with degraded_mesh("organization"):
            resp = await client.post(
                "/api/auth/admin/users",
                json={
                    "email": email,
                    "password": PASSWORD,
                    "role": "front_desk",
                    "clinic_id": clinic_id,
                },
                headers=_auth(admin),
            )

        assert resp.status_code == 503, resp.text
        assert resp.json() == {"detail": "organization service unavailable"}
        assert not await _user_row_exists(email)
        assert await _failure_audit_count(mongo_client, clinic_id) == 1
    finally:
        await _cleanup_audit(mongo_client, clinic_id)


async def test_staff_user_without_clinic_makes_no_reference_call(
    client: AsyncClient, request_recorder: RequestRecorder
) -> None:
    admin = await bootstrap_admin_and_login(client)
    request_recorder.requests.clear()

    resp = await client.post(
        "/api/auth/admin/users",
        json={"email": unique_email("staff-no-clinic"), "password": PASSWORD, "role": "admin"},
        headers=_auth(admin),
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["clinic_id"] is None
    assert request_recorder.paths("/internal") == []
