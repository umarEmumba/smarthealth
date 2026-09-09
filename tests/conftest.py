"""Shared pytest fixtures for the SmartHealth test suite.

Exposes the four fixtures every later plan's tests depend on by name:
`db_session`, `mongo_client`, `redis_client`, `client`.

Fixtures that require live Postgres/Mongo/Redis containers are marked
`integration` so `pytest tests/ -x -k "not integration"` can run without the
Docker Compose stack up. No PHI or real credentials appear in any fixture —
all connection info comes from `smarthealth.config.settings`, which itself
only ever holds local-dev placeholder values (see `.env.example`).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from smarthealth.config import settings


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: marks tests that require live Postgres/Mongo/Redis containers"
    )


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    """Async SQLAlchemy session, rolled back at teardown for test isolation.

    Requires a live Postgres container (integration-marked tests only) —
    see infra/docker-compose.yml. The outer transaction is never committed,
    so each test sees a clean slate regardless of what it writes.
    """
    engine = create_async_engine(settings.database_url)
    connection = await engine.connect()
    transaction = await connection.begin()
    session_factory = async_sessionmaker(bind=connection, expire_on_commit=False)
    session = session_factory()

    try:
        yield session
    finally:
        await session.close()
        await transaction.rollback()
        await connection.close()
        await engine.dispose()


@pytest_asyncio.fixture
async def mongo_client() -> AsyncIterator[AsyncMongoClient]:
    """PyMongo native async client against a dedicated test database.

    Requires a live MongoDB container (integration-marked tests only).
    The test database is dropped at teardown so audit-trail tests never
    leak documents between runs.
    """
    client: AsyncMongoClient = AsyncMongoClient(settings.mongo_url)
    test_db_name = f"{settings.mongo_db_name}_test"

    try:
        yield client
    finally:
        await client.drop_database(test_db_name)
        await client.close()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """redis.asyncio client against a dedicated test db index, flushed per test.

    Requires a live Redis container (integration-marked tests only).
    """
    client: Redis = Redis.from_url(settings.redis_url, decode_responses=True)

    try:
        await client.flushdb()
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """httpx.AsyncClient wired to the real FastAPI app for integration tests.

    `smarthealth.main:app` (added in Plan 01-03, the auth/RBAC/audit
    tracer) is the app every later plan's routers mount onto.
    """
    from smarthealth.main import app  # noqa: PLC0415

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


def unique_email(prefix: str = "user") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}@example.com"


async def bootstrap_admin_and_login(client: AsyncClient) -> dict:
    """Create a throwaway ADMIN account and return its login token pair.

    CR-01 fix: public `/api/auth/register` always creates a PATIENT
    account now (no caller-supplied `role`), so there is no API path for
    an unauthenticated caller to mint the very first admin. Tests need a
    seeded admin to exercise the admin-only `/api/auth/admin/users` route
    (and every other admin-guarded route) — this helper seeds one directly
    via a committed DB insert, exactly like `test_models.py`'s established
    direct-model-insertion pattern, using the SAME password-hashing helper
    the real register/login code path uses so the seeded row round-trips
    through the app's own bcrypt verification.

    The row is committed against its own short-lived engine (distinct
    from `db_session`'s rolled-back transaction) so the running app --
    which opens its own connection via `get_db()` -- can actually see it.
    Test emails are uuid-suffixed and never cleaned up, matching this
    suite's existing established convention (test_auth.py's docstring:
    "no cleanup fixture exists for the live `users` table across test
    runs").
    """
    from libs.auth.passwords import hash_password  # noqa: PLC0415
    from services.identity.repository.models import User  # noqa: PLC0415

    email = unique_email("bootstrap-admin")
    password = "correct-horse-battery"

    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        existing = await session.execute(select(User).where(User.email == email))
        if existing.scalar_one_or_none() is None:
            session.add(User(email=email, hashed_password=hash_password(password), role="admin"))
            await session.commit()
    await engine.dispose()

    login_resp = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert login_resp.status_code == 200, login_resp.text
    return login_resp.json()
