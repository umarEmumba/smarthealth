"""Idempotent audit writes (plan 02-01, Task 3; research G5, Pitfall 7).

Temporal activities retry, and the design wants "an audit entry per step", so
`write_audit(dedupe_key=...)` upserts with `$setOnInsert` behind the partial
unique index `ix_audit_dedupe_key`: a retried (or concurrently duplicated) step
leaves exactly one document. Without a dedupe_key, every call still inserts.

Requires the live MongoDB container. `write_audit` targets the shared
`MONGO_DB_NAME` database, so each test removes only the documents it wrote.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from pymongo import AsyncMongoClient

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.audit.writer import AUDIT_COLLECTION_NAME, write_audit
from libs.config import shared_settings

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def audit(mongo_client: AsyncMongoClient) -> AsyncIterator[tuple[AsyncMongoClient, str]]:
    """(client, unique entity_id); indexes ensured; this test's documents removed after."""
    await ensure_audit_indexes(mongo_client)
    entity_id = f"dedupe-test-{uuid.uuid4()}"
    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    try:
        yield mongo_client, entity_id
    finally:
        await collection.delete_many({"entity_id": entity_id})


async def _write(mongo: AsyncMongoClient, entity_id: str, dedupe_key: str | None) -> None:
    await write_audit(
        mongo,
        actor_id="system",
        actor_role="system",
        action="appointment.reserve",
        entity_type="appointment",
        entity_id=entity_id,
        clinic_id=None,
        outcome="success",
        dedupe_key=dedupe_key,
    )


async def _count(mongo: AsyncMongoClient, query: dict) -> int:
    collection = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    return await collection.count_documents(query)


async def test_same_dedupe_key_twice_writes_one_document(audit) -> None:
    mongo, entity_id = audit
    key = f"wf-{uuid.uuid4()}:reserve"

    await _write(mongo, entity_id, key)
    await _write(mongo, entity_id, key)

    assert await _count(mongo, {"dedupe_key": key}) == 1
    doc = await mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME].find_one(
        {"dedupe_key": key}
    )
    assert doc["action"] == "appointment.reserve"
    assert doc["entity_id"] == entity_id
    assert doc["outcome"] == "success"


async def test_concurrent_writes_with_same_key_leave_one_document(audit) -> None:
    mongo, entity_id = audit
    key = f"wf-{uuid.uuid4()}:reserve"

    await asyncio.gather(*(_write(mongo, entity_id, key) for _ in range(8)))

    assert await _count(mongo, {"dedupe_key": key}) == 1


async def test_different_keys_write_separate_documents(audit) -> None:
    mongo, entity_id = audit
    workflow = f"wf-{uuid.uuid4()}"

    await _write(mongo, entity_id, f"{workflow}:reserve")
    await _write(mongo, entity_id, f"{workflow}:confirm")

    assert await _count(mongo, {"entity_id": entity_id}) == 2


async def test_without_dedupe_key_every_call_inserts(audit) -> None:
    mongo, entity_id = audit

    await _write(mongo, entity_id, None)
    await _write(mongo, entity_id, None)

    assert await _count(mongo, {"entity_id": entity_id}) == 2
    # The legacy path stores no dedupe_key field at all (the partial index skips it).
    assert await _count(mongo, {"entity_id": entity_id, "dedupe_key": {"$exists": True}}) == 0
