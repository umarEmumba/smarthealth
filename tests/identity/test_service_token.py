"""Service tokens for calls with no user context, and the validated anonymous
registration that needs them (plan 01.1-05, Task 2; STRIDE T-01.1-22).

Anonymous self-registration has no user token to forward to Organization, so
Identity signs a short-lived (60 s) service token of its own:

    type=service, sub=service:identity, aud=organization, jti, iat, exp (RS256, kid header)

It is never handed to a client. It is accepted ONLY by `require_internal_caller`
(on `/internal/*`), and every public route still rejects it with 401 because
`get_current_user` accepts access tokens only. Malformed and expired service
tokens are built here with the real signing key and kid, so exactly one claim is
wrong in each test.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from jose import jwt
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.auth.denylist import revoke_token
from libs.config import shared_settings
from tests.conftest import (
    RequestRecorder,
    bootstrap_admin_and_login,
    degraded_mesh,
    service_base_url,
    unique_email,
    verify_identity_token,
)

PASSWORD = "correct-horse-battery"

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _signed(claims: dict[str, Any]) -> str:
    """Sign arbitrary claims with Identity's real private key and kid."""
    from services.identity.security.keys import get_signing_key  # noqa: PLC0415

    key = get_signing_key()
    return jwt.encode(claims, key.private_pem, algorithm="RS256", headers={"kid": key.kid})


def _service_claims(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    claims: dict[str, Any] = {
        "sub": "service:identity",
        "aud": "organization",
        "type": "service",
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + timedelta(seconds=60),
    }
    claims.update(overrides)
    return claims


async def _create_clinic(client: AsyncClient, admin: dict, name: str) -> str:
    resp = await client.post(
        "/api/clinics", json={"name": name, "timezone": "UTC"}, headers=_auth(admin)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def _org_clinic_url(clinic_id: str) -> str:
    return f"{service_base_url('organization')}/internal/clinics/{clinic_id}"


async def _user_row_exists(email: str) -> bool:
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


# --- create_service_token ------------------------------------------------------


def test_create_service_token_claims_and_short_ttl() -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    claims = verify_identity_token(create_service_token("identity", audience="organization"))

    assert set(claims) == {"sub", "aud", "type", "jti", "iat", "exp"}
    assert claims["sub"] == "service:identity"
    assert claims["aud"] == "organization"
    assert claims["type"] == "service"
    assert claims["exp"] - claims["iat"] == 60
    # No user context of any kind rides in a service token.
    assert "role" not in claims and "clinic_id" not in claims


def test_service_token_ttl_follows_the_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.identity.config import settings as identity_settings  # noqa: PLC0415
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    assert identity_settings.service_token_expire_seconds == 60
    monkeypatch.setattr(identity_settings, "service_token_expire_seconds", 15)

    claims = verify_identity_token(create_service_token("identity", audience="organization"))

    assert claims["exp"] - claims["iat"] == 15


# --- anonymous registration validates the clinic --------------------------------


async def test_anonymous_register_with_existing_clinic_stores_it(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Self Registration Clinic")

    resp = await client.post(
        "/api/auth/register",
        json={"email": unique_email("self-reg"), "password": PASSWORD, "clinic_id": clinic_id},
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["clinic_id"] == clinic_id
    assert resp.json()["role"] == "patient"


async def test_anonymous_register_with_missing_clinic_is_422_and_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    missing_clinic_id = str(uuid.uuid4())
    email = unique_email("self-reg-missing")
    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]

    try:
        resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": PASSWORD, "clinic_id": missing_clinic_id},
        )

        assert resp.status_code == 422, resp.text
        assert resp.json() == {"detail": "clinic not found"}
        assert not await _user_row_exists(email)
        doc = await collection.find_one(
            {"action": "user.create", "outcome": "failure", "clinic_id": missing_clinic_id}
        )
        assert doc is not None
        assert doc["actor_id"] == "anonymous"
        assert doc["actor_role"] == "anonymous"
    finally:
        await collection.delete_many({"clinic_id": missing_clinic_id})


async def test_anonymous_register_with_inactive_clinic_is_422(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Inactive Self Registration Clinic")
    assert (
        await client.post(f"/api/clinics/{clinic_id}/deactivate", headers=_auth(admin))
    ).status_code == 200
    email = unique_email("self-reg-inactive")

    resp = await client.post(
        "/api/auth/register",
        json={"email": email, "password": PASSWORD, "clinic_id": clinic_id},
    )

    assert resp.status_code == 422, resp.text
    assert resp.json() == {"detail": "clinic is inactive"}
    assert not await _user_row_exists(email)


async def test_anonymous_register_when_organization_unreachable_is_503(
    client: AsyncClient,
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Degraded Self Registration Clinic")
    email = unique_email("self-reg-degraded")

    async with degraded_mesh("organization"):
        resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": PASSWORD, "clinic_id": clinic_id},
        )

    assert resp.status_code == 503, resp.text
    assert resp.json() == {"detail": "organization service unavailable"}
    assert not await _user_row_exists(email)


async def test_anonymous_register_without_clinic_makes_no_reference_call(
    client: AsyncClient, request_recorder: RequestRecorder
) -> None:
    request_recorder.requests.clear()

    resp = await client.post(
        "/api/auth/register", json={"email": unique_email("self-reg-none"), "password": PASSWORD}
    )

    assert resp.status_code == 201, resp.text
    assert request_recorder.paths("/internal") == []


async def test_anonymous_register_sends_a_short_lived_identity_service_token(
    client: AsyncClient, request_recorder: RequestRecorder
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Service Token Clinic")
    request_recorder.requests.clear()

    resp = await client.post(
        "/api/auth/register",
        json={"email": unique_email("self-reg-token"), "password": PASSWORD, "clinic_id": clinic_id},
    )
    assert resp.status_code == 201, resp.text

    (lookup,) = [r for r in request_recorder.requests if r.url.path.startswith("/internal")]
    assert lookup.method == "GET"
    token = lookup.headers["authorization"].removeprefix("Bearer ")
    claims = verify_identity_token(token)
    assert (claims["type"], claims["sub"]) == ("service", "service:identity")
    assert claims["aud"] == "organization"
    assert claims["exp"] - claims["iat"] == 60
    # The service token is an internal credential: it never reaches the client.
    assert token not in resp.text


# --- who accepts a service token ---------------------------------------------------


async def test_service_token_is_rejected_by_public_routes(client: AsyncClient) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    token = create_service_token("identity", audience="organization")

    ping = await client.get("/api/admin/ping", headers=_bearer(token))
    logout = await client.post("/api/auth/logout", headers=_bearer(token))

    assert ping.status_code == 401
    assert logout.status_code == 401


async def test_service_token_is_accepted_on_an_internal_endpoint(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Internal Service Token Clinic")

    resp = await mesh.get(
        _org_clinic_url(clinic_id), headers=_bearer(create_service_token("identity", audience="organization"))
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": clinic_id, "is_active": True}


async def test_service_token_without_the_service_prefix_is_rejected(
    mesh: httpx.AsyncClient,
) -> None:
    token = _signed(_service_claims(sub="identity"))

    resp = await mesh.get(_org_clinic_url(str(uuid.uuid4())), headers=_bearer(token))

    assert resp.status_code == 401


async def test_user_subject_in_a_service_type_token_is_rejected(mesh: httpx.AsyncClient) -> None:
    """A type=service token cannot borrow a user's subject."""
    token = _signed(_service_claims(sub=str(uuid.uuid4())))

    resp = await mesh.get(_org_clinic_url(str(uuid.uuid4())), headers=_bearer(token))

    assert resp.status_code == 401


async def test_expired_service_token_is_rejected(mesh: httpx.AsyncClient) -> None:
    now = datetime.now(timezone.utc)
    token = _signed(_service_claims(iat=now - timedelta(seconds=120), exp=now - timedelta(seconds=60)))

    resp = await mesh.get(_org_clinic_url(str(uuid.uuid4())), headers=_bearer(token))

    assert resp.status_code == 401


async def test_revoked_service_token_is_rejected(
    mesh: httpx.AsyncClient, redis_client: Redis
) -> None:
    claims = _service_claims()
    token = _signed(claims)
    await revoke_token(claims["jti"], 60, redis_client)

    resp = await mesh.get(_org_clinic_url(str(uuid.uuid4())), headers=_bearer(token))

    assert resp.status_code == 401


async def test_refresh_token_is_not_accepted_as_an_internal_caller(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)

    resp = await mesh.get(
        _org_clinic_url(str(uuid.uuid4())), headers=_bearer(admin["refresh_token"])
    )

    assert resp.status_code == 401


async def test_user_access_token_still_works_on_internal_endpoints(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic_id = await _create_clinic(client, admin, "Forwarded Admin Token Clinic")

    resp = await mesh.get(_org_clinic_url(clinic_id), headers=_auth(admin))

    assert resp.status_code == 200, resp.text
