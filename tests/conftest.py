"""Shared pytest harness for the SmartHealth microservice test suite.

Multi-app, in-process harness (RESEARCH Pattern 3): every service's ASGI app
is mounted into ONE `httpx.AsyncClient` (`mesh`) keyed by that service's
internal base URL, so a request that crosses service boundaries (gateway ->
service, service -> service reference checks, JWKS fetches) runs fully
in-process with no real sockets and no Docker beyond the data stores.

Extension contract (later plans): add a service name to `HARNESS_SERVICES`
and, once a gateway exists, point `ENTRY_SERVICE` at it. Nothing else here
needs to change — `harness_apps()` imports `services.<name>.app:app` by name.

Key bootstrap: settings objects are built at import time, so a throwaway RSA
test key is generated and `JWT_PRIVATE_KEY_PATH` is set BEFORE anything from
`libs` or `services` is imported. A key supplied by the environment wins, so
CI can pin its own. No key material is ever committed or printed.

No PHI or real credentials appear in any fixture; connection info comes from
`libs.config.shared_settings` / `services.<name>.config.settings`, which hold
local-dev placeholder values only (see `.env.example`).
"""

from __future__ import annotations

import importlib
import os
import tempfile
import uuid
from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Any


def _ensure_test_signing_key() -> None:
    """Point JWT_PRIVATE_KEY_PATH at a freshly generated RSA-2048 PKCS8 PEM
    unless the environment already supplies one. Must run before any `libs` /
    `services` import (Identity builds its signing key when its app is built).
    """
    if os.environ.get("JWT_PRIVATE_KEY_PATH"):
        return

    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_dir = tempfile.mkdtemp(prefix="smarthealth-test-keys-")
    key_path = os.path.join(key_dir, "jwt_private.pem")
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(pem)
    os.environ["JWT_PRIVATE_KEY_PATH"] = key_path


_ensure_test_signing_key()

import httpx  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from pymongo import AsyncMongoClient  # noqa: E402
from redis.asyncio import Redis  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

from libs.config import shared_settings  # noqa: E402

# Phase 1's monolith tests are parked here until the plan that owns each
# service relocates them to tests/<service>/ (excluded from collection).
collect_ignore = ["legacy_monolith"]

# Services whose ASGI apps are mounted into the in-process mesh, and the one
# the `client` fixture talks to (the gateway, once it exists).
HARNESS_SERVICES: tuple[str, ...] = ("identity", "organization", "gateway")
ENTRY_SERVICE = "gateway"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: marks tests that require live Postgres/Mongo/Redis containers"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests that require the full docker compose stack"
    )


def service_base_url(name: str) -> str:
    """Internal base URL the mesh mounts a service's ASGI app at."""
    if name == "gateway":
        return "http://gateway"
    return getattr(shared_settings, f"{name}_service_url")


@lru_cache(maxsize=1)
def harness_apps() -> dict[str, FastAPI]:
    """Import each harness service's module-level `app` (cached: building an
    app is import-time work and the apps themselves are stateless).
    """
    apps: dict[str, FastAPI] = {}
    for name in HARNESS_SERVICES:
        module = importlib.import_module(f"services.{name}.app")
        apps[name] = module.app
    return apps


@pytest_asyncio.fixture
async def mesh() -> AsyncIterator[httpx.AsyncClient]:
    """One `httpx.AsyncClient` whose `mounts` map every harness service's
    base URL to that service's in-process ASGI app. Injected into every app's
    `state.internal_http_client`, so internal calls (JWKS fetches, reference
    validation) and the entry client share the same in-process network.
    """
    apps = harness_apps()
    mounts: dict[str, httpx.AsyncBaseTransport] = {
        service_base_url(name): ASGITransport(app=app) for name, app in apps.items()
    }
    async with httpx.AsyncClient(mounts=mounts) as http:
        for app in apps.values():
            app.state.internal_http_client = http
        try:
            yield http
        finally:
            for app in apps.values():
                app.state.internal_http_client = None


@pytest_asyncio.fixture
async def client(mesh: httpx.AsyncClient) -> AsyncIterator[AsyncClient]:
    """Entry-point client: talks to `ENTRY_SERVICE` over ASGI with the mesh
    wired behind it."""
    transport = ASGITransport(app=harness_apps()[ENTRY_SERVICE])
    async with AsyncClient(transport=transport, base_url=service_base_url(ENTRY_SERVICE)) as ac:
        yield ac


@pytest_asyncio.fixture
async def mongo_client() -> AsyncIterator[AsyncMongoClient]:
    """PyMongo native async client against a dedicated test database.

    Requires a live MongoDB container (integration-marked tests only). The
    test database is dropped at teardown so audit tests never leak documents.
    """
    mongo: AsyncMongoClient = AsyncMongoClient(shared_settings.mongo_url)
    test_db_name = f"{shared_settings.mongo_db_name}_test"

    try:
        yield mongo
    finally:
        await mongo.drop_database(test_db_name)
        await mongo.close()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """redis.asyncio client, flushed per test. Requires a live Redis container."""
    redis: Redis = Redis.from_url(shared_settings.redis_url, decode_responses=True)

    try:
        await redis.flushdb()
        yield redis
    finally:
        await redis.flushdb()
        await redis.aclose()


def unique_email(prefix: str = "user") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}@example.com"


async def insert_admin_user(email: str, password: str) -> None:
    """Committed insert of an ADMIN account straight into identity_db.

    Public `/api/auth/register` only ever creates PATIENT accounts (CR-01), so
    tests that need an admin seed one directly, hashing with the SAME helper
    the real login path verifies against. Uses its own short-lived engine so
    the running app's own connection can see the committed row. Emails are
    uuid-suffixed and never cleaned up (established suite convention).
    """
    from services.identity.config import settings as identity_settings  # noqa: PLC0415
    from services.identity.repository.models import User  # noqa: PLC0415
    from services.identity.security.passwords import hash_password  # noqa: PLC0415

    engine = create_async_engine(identity_settings.database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            existing = await session.execute(select(User).where(User.email == email))
            if existing.scalar_one_or_none() is None:
                session.add(User(email=email, hashed_password=hash_password(password), role="admin"))
                await session.commit()
    finally:
        await engine.dispose()


async def bootstrap_admin_and_login(client: AsyncClient) -> dict[str, Any]:
    """Create a throwaway ADMIN account and return its login token pair."""
    email = unique_email("bootstrap-admin")
    password = "correct-horse-battery"

    await insert_admin_user(email, password)

    login_resp = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert login_resp.status_code == 200, login_resp.text
    return login_resp.json()


def verify_identity_token(token: str) -> dict[str, Any]:
    """Decode a token against Identity's PUBLIC JWK with RS256 pinned — claim
    assertions in tests never need (or see) a shared secret.
    """
    from jose import jwt  # noqa: PLC0415

    from services.identity.security.keys import get_signing_key  # noqa: PLC0415

    return jwt.decode(token, get_signing_key().public_jwk, algorithms=["RS256"])
