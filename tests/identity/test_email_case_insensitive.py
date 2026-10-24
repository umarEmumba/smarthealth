"""Email identity is case-insensitive end to end (plan 02.1-02, carry-over C3).

Input is normalized (strip + lowercase), lookups use lower(email), and the unique
index on lower(email) is the database-level guard. A legacy row stored in mixed
case (written before the normalizer existed) is still matched by the lowercase
address. All emails are synthetic and uuid-suffixed.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.identity.config import settings as identity_settings
from services.identity.domain.schemas import LoginRequest, RegisterRequest, StaffRegisterRequest
from services.identity.security.passwords import hash_password
from tests.conftest import bootstrap_admin_and_login, unique_email

pytestmark = pytest.mark.integration

PASSWORD = "correct-horse-battery"


def _mixed(prefix: str) -> str:
    return unique_email(prefix).replace(prefix, f"Case.{prefix.title()}", 1).replace("@example.com", "@Example.com")


async def _register(client: AsyncClient, email: str):
    return await client.post("/api/auth/register", json={"email": email, "password": PASSWORD})


async def _login(client: AsyncClient, email: str):
    return await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})


def test_request_schemas_normalize_the_email() -> None:
    assert RegisterRequest(email="  Mixed.Case@Example.COM ", password=PASSWORD).email == "mixed.case@example.com"
    assert (
        StaffRegisterRequest(email="STAFF@Example.com", password=PASSWORD, role="provider").email
        == "staff@example.com"
    )
    assert LoginRequest(email=" LOGIN@Example.com", password=PASSWORD).email == "login@example.com"


async def test_second_registration_with_a_different_casing_is_409(client: AsyncClient) -> None:
    email = _mixed("dup")
    first = await _register(client, email)
    assert first.status_code == 201, first.text
    assert first.json()["email"] == email.lower()

    second = await _register(client, email.lower().replace("@example.com", "@EXAMPLE.COM").upper())
    assert second.status_code == 409
    assert second.json()["detail"] == "email already registered"

    third = await _register(client, email.swapcase())
    assert third.status_code == 409


async def test_login_works_with_any_casing_of_the_registered_email(client: AsyncClient) -> None:
    email = _mixed("login")
    assert (await _register(client, email)).status_code == 201

    for variant in (email, email.lower(), email.upper(), email.swapcase(), f"  {email} "):
        resp = await _login(client, variant)
        assert resp.status_code == 200, (variant, resp.text)


async def test_staff_creation_is_case_insensitive_too(client: AsyncClient) -> None:
    admin = await bootstrap_admin_and_login(client)
    headers = {"Authorization": f"Bearer {admin['access_token']}"}
    email = _mixed("staff")
    first = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "provider"},
        headers=headers,
    )
    assert first.status_code == 201, first.text
    second = await client.post(
        "/api/auth/admin/users",
        json={"email": email.upper(), "password": PASSWORD, "role": "front_desk"},
        headers=headers,
    )
    assert second.status_code == 409
    # Public registration cannot take the same address either.
    assert (await _register(client, email.swapcase())).status_code == 409


async def test_concurrent_registrations_differing_only_in_case_make_one_account(
    client: AsyncClient,
) -> None:
    base = unique_email("race")
    variants = [base, base.upper(), base.title(), base.swapcase()] * 2
    responses = await asyncio.gather(*[_register(client, v) for v in variants])
    codes = sorted(r.status_code for r in responses)
    assert codes == [201] + [409] * (len(variants) - 1), codes


async def test_a_legacy_mixed_case_row_is_matched_by_the_lowercase_address(
    client: AsyncClient,
) -> None:
    legacy_email = f"Legacy.{uuid.uuid4().hex[:12]}@Example.com"
    engine = create_async_engine(identity_settings.database_url)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await session.execute(
                text(
                    "INSERT INTO users (email, hashed_password, role) "
                    "VALUES (:email, :hashed, 'patient')"
                ),
                {"email": legacy_email, "hashed": hash_password(PASSWORD)},
            )
            await session.commit()

        # Login with the lowercase address finds the legacy row ...
        assert (await _login(client, legacy_email.lower())).status_code == 200
        # ... and registering any other casing of it is refused.
        assert (await _register(client, legacy_email.lower())).status_code == 409
        assert (await _register(client, legacy_email.upper())).status_code == 409
    finally:
        async with factory() as session:
            await session.execute(
                text("DELETE FROM users WHERE lower(email) = :email"), {"email": legacy_email.lower()}
            )
            await session.commit()
        await engine.dispose()
