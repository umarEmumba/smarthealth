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

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
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
    """httpx.AsyncClient wired to the FastAPI app for integration tests.

    The FastAPI app itself (`smarthealth.main:app`) is added in a later plan
    (01-03, the auth/RBAC/audit tracer). Until then this fixture targets a
    minimal placeholder ASGI app so the fixture is importable and later
    plans only need to swap the app import, not the fixture shape.
    """
    try:
        from smarthealth.main import app  # noqa: PLC0415
    except ImportError:
        from fastapi import FastAPI

        app = FastAPI()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
