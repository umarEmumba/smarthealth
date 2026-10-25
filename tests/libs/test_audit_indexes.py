"""Integration test for the shared audit_log index setup (IAM-04).

Extracted from Phase 1's `test_models.py` by plan 01.1-02 — the audit sink is
shared cross-cutting infrastructure (`libs.audit`), so its test lives with
`libs`, not with any one service's models. Requires the live MongoDB container.
"""

from __future__ import annotations

import pytest
from pymongo import AsyncMongoClient

from libs.audit.mongo_indexes import AUDIT_COLLECTION_NAME, ensure_audit_indexes
from libs.config import shared_settings


@pytest.mark.integration
async def test_audit_indexes_created(mongo_client: AsyncMongoClient) -> None:
    """ensure_audit_indexes creates the three query indexes plus the partial
    unique `ix_audit_dedupe_key` (plan 02-01) idempotently.

    ensure_audit_indexes writes to shared_settings.mongo_db_name (the app's real
    configured database, not a hardcoded test override) — so this test reads
    the indexes back from that same database and cleans up the collection
    itself afterwards (the mongo_client fixture's teardown only drops
    `{mongo_db_name}_test`, a different database).
    """
    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]

    try:
        # First call creates the indexes.
        names_first = await ensure_audit_indexes(mongo_client)
        assert set(names_first) == {
            "ix_audit_entity_type_entity_id_timestamp",
            "ix_audit_actor_id_timestamp",
            "ix_audit_clinic_id_timestamp",
            "ix_audit_dedupe_key",
        }

        # Second call must not raise (idempotent by index name).
        names_second = await ensure_audit_indexes(mongo_client)
        assert set(names_second) == set(names_first)

        index_cursor = await collection.list_indexes()
        existing = {doc["name"]: doc async for doc in index_cursor}
        assert "ix_audit_entity_type_entity_id_timestamp" in existing
        assert "ix_audit_actor_id_timestamp" in existing
        assert "ix_audit_clinic_id_timestamp" in existing

        # Retried activity steps dedupe on this key; documents without one
        # (every pre-Phase-2 call site) are outside the index entirely.
        dedupe = existing["ix_audit_dedupe_key"]
        assert dict(dedupe["key"]) == {"dedupe_key": 1}
        assert dedupe["unique"] is True
        assert dedupe["partialFilterExpression"] == {"dedupe_key": {"$exists": True}}
        # The three query indexes are not unique.
        assert not existing["ix_audit_actor_id_timestamp"].get("unique", False)
    finally:
        await collection.drop()
        # Dropping the collection also drops its indexes, including the partial unique
        # `ix_audit_dedupe_key`. Services create them only at start-up, so restore them,
        # or every later test (and every running service) in this database loses the
        # audit dedupe guarantee (plan 02.1-13 found a visit-reconciler race test failing
        # in a full-suite run because of exactly this).
        await ensure_audit_indexes(mongo_client)
