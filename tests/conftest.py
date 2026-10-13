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

import asyncio
import contextvars
import importlib
import os
import tempfile
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any, NamedTuple


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


SERVICE_CLIENTS: tuple[str, ...] = ("scheduling", "visit", "billing")


def _ensure_test_service_client_secrets() -> None:
    """Give each non-Identity service client a throwaway secret file (plan 02-01)
    unless the environment already supplies `SERVICE_CLIENT_SECRETS_DIR`.

    Sets `SERVICE_CLIENT_SECRETS_DIR` (read by Identity's mint endpoint) and
    `<SERVICE>_CLIENT_SECRET_PATH` (read by each calling service) before any
    settings object is built. Secrets are random per run and never printed.
    """
    import secrets as _secrets  # noqa: PLC0415

    if os.environ.get("SERVICE_CLIENT_SECRETS_DIR"):
        return
    secrets_dir = tempfile.mkdtemp(prefix="smarthealth-test-service-clients-")
    for client in SERVICE_CLIENTS:
        path = os.path.join(secrets_dir, f"{client}.secret")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(_secrets.token_urlsafe(32))
        os.environ[f"{client.upper()}_CLIENT_SECRET_PATH"] = path
    os.environ["SERVICE_CLIENT_SECRETS_DIR"] = secrets_dir


_ensure_test_service_client_secrets()

# The gateway rate-limits per client IP. The functional suite fires far more
# requests than production budgets allow from one address, so lift both budgets
# BEFORE any settings object is built; tests/gateway/test_ratelimit.py passes its
# own small budgets explicitly.
os.environ.setdefault("GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE", "100000")
os.environ.setdefault("GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE", "100000")

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

# Services whose ASGI apps are mounted into the in-process mesh, and the one
# the `client` fixture talks to (the gateway, once it exists).
HARNESS_SERVICES: tuple[str, ...] = (
    "identity",
    "organization",
    "patient",
    "provider",
    "scheduling",
    "gateway",
)
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


class _NetworkHopTransport(httpx.AsyncBaseTransport):
    """Wrap an in-process ASGI transport so each call behaves like a real
    network hop with respect to context propagation.

    Over a real network the callee is a different process: it inherits nothing
    from the caller's `contextvars` (in particular not the active OpenTelemetry
    span), and learns the caller's trace ONLY from the `traceparent` header.
    Awaiting the callee's ASGI app directly would share the caller's context
    and make a trace look connected even when no header was ever sent, so each
    call runs in its own task with an EMPTY context (plan 01.1-04, roadmap SC4).
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        task = asyncio.get_running_loop().create_task(
            self._inner.handle_async_request(request), context=contextvars.Context()
        )
        return await task

    async def aclose(self) -> None:
        await self._inner.aclose()


class _UnreachableTransport(httpx.AsyncBaseTransport):
    """Stands in for a service that is down: every call fails with the transport
    error a refused connection produces, with no real socket and no DNS lookup
    (an unmounted internal hostname would otherwise try to resolve for real)."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused (service excluded from the mesh)", request=request)


def _mesh_mounts(excluded: tuple[str, ...] = ()) -> dict[str, httpx.AsyncBaseTransport]:
    """Base URL -> transport for every harness service; services named in
    `excluded` are mapped to an unreachable transport instead of their app."""
    unknown = set(excluded) - set(HARNESS_SERVICES)
    assert not unknown, f"not harness services: {sorted(unknown)}"
    mounts: dict[str, httpx.AsyncBaseTransport] = {}
    for name, app in harness_apps().items():
        if name in excluded:
            mounts[service_base_url(name)] = _UnreachableTransport()
        else:
            mounts[service_base_url(name)] = _NetworkHopTransport(ASGITransport(app=app))
    return mounts


@pytest_asyncio.fixture
async def mesh() -> AsyncIterator[httpx.AsyncClient]:
    """One `httpx.AsyncClient` whose `mounts` map every harness service's
    base URL to that service's in-process ASGI app. Injected into every app's
    `state.internal_http_client`, so internal calls (JWKS fetches, reference
    validation) and the entry client share the same in-process network.
    """
    apps = harness_apps()
    async with httpx.AsyncClient(mounts=_mesh_mounts()) as http:
        for app in apps.values():
            app.state.internal_http_client = http
        try:
            yield http
        finally:
            for app in apps.values():
                app.state.internal_http_client = None


@asynccontextmanager
async def degraded_mesh(
    *excluded: str, event_hooks: dict[str, list[Any]] | None = None
) -> AsyncIterator[httpx.AsyncClient]:
    """A mesh in which the named services are unreachable (plan 01.1-05).

    Mounts every harness app except `excluded` (those hosts refuse connections),
    assigns it to every app's `state.internal_http_client` for the block, and
    restores the regular mesh afterwards. Used by owner-unreachable tests to
    prove a cross-service reference check fails closed (503, nothing written).
    Requires the `mesh` fixture to be active (the entry client depends on it).
    """
    apps = harness_apps()
    previous = {name: app.state.internal_http_client for name, app in apps.items()}
    async with httpx.AsyncClient(mounts=_mesh_mounts(excluded), event_hooks=event_hooks) as degraded:
        for app in apps.values():
            app.state.internal_http_client = degraded
        try:
            yield degraded
        finally:
            for name, app in apps.items():
                app.state.internal_http_client = previous[name]


class RequestRecorder(NamedTuple):
    """Every request sent over the mesh, plus the `event_hooks` dict that records
    them (pass it to `degraded_mesh(..., event_hooks=...)` to record there too)."""

    requests: list[httpx.Request]
    event_hooks: dict[str, list[Any]]

    def paths(self, prefix: str = "") -> list[str]:
        return [r.url.path for r in self.requests if r.url.path.startswith(prefix)]


@pytest_asyncio.fixture
async def request_recorder(mesh: httpx.AsyncClient) -> RequestRecorder:
    """Records every request the in-process mesh carries (method, URL, headers).

    The hook is appended to the live mesh client, so everything an app sends
    through `state.internal_http_client` shows up in `requests`.
    """
    requests: list[httpx.Request] = []

    async def _record(request: httpx.Request) -> None:
        requests.append(request)

    event_hooks: dict[str, list[Any]] = {"request": [_record], "response": []}
    mesh.event_hooks["request"].append(_record)
    return RequestRecorder(requests=requests, event_hooks=event_hooks)


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


def verify_identity_token(token: str, *, audience: str | None = None) -> dict[str, Any]:
    """Decode a token against Identity's PUBLIC JWK with RS256 pinned — claim
    assertions in tests never need (or see) a shared secret.

    Service tokens carry `aud` (plan 02-01). With `audience` given it must match;
    without it the audience is not checked, so a test can read the claims and
    assert on `aud` itself.
    """
    from jose import jwt  # noqa: PLC0415

    from services.identity.security.keys import get_signing_key  # noqa: PLC0415

    return jwt.decode(
        token,
        get_signing_key().public_jwk,
        algorithms=["RS256"],
        audience=audience,
        options={"verify_aud": audience is not None},
    )
