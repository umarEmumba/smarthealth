"""Idempotent index setup for the `audit_log` MongoDB collection.

Per RESEARCH's MongoDB Audit-Trail Schema Design section: three indexes
support the query patterns the audit trail needs — "an entity's full
history," "a user's actions," and "a clinic's audit view."

A fourth, `ix_audit_dedupe_key` (plan 02-01), is a PARTIAL unique index on
`dedupe_key`: it backs `write_audit(dedupe_key=...)` so a retried workflow step
writes one entry, and it ignores every document without a `dedupe_key` (all
pre-Phase-2 call sites), so those keep inserting freely.

Deliberately no auto-expiring retention index (RESEARCH assumption A2 — no
audit retention policy has been decided yet). Adding one here would silently
start deleting audit documents contrary to that recorded assumption.

`create_index` is itself idempotent by name in MongoDB — calling
`ensure_audit_indexes` multiple times (e.g. on every app startup) is safe
and a no-op after the first call.
"""

from __future__ import annotations

from typing import Any

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient

from libs.config import shared_settings

AUDIT_COLLECTION_NAME = "audit_log"


async def ensure_audit_indexes(mongo_client: AsyncMongoClient) -> list[str]:
    """Create the audit_log indexes (three query indexes plus the partial
    unique dedupe index) if they don't already exist.

    Returns the list of index names that exist after this call (as reported
    by `create_indexes`), so callers/tests can assert on it.
    """
    db = mongo_client[shared_settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]

    # (keys, name, extra create_index options)
    index_specs: list[tuple[list[tuple[str, int]], str, dict[str, Any]]] = [
        (
            [("entity_type", ASCENDING), ("entity_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_entity_type_entity_id_timestamp",
            {},
        ),
        (
            [("actor_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_actor_id_timestamp",
            {},
        ),
        (
            [("clinic_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_clinic_id_timestamp",
            {},
        ),
        (
            [("dedupe_key", ASCENDING)],
            "ix_audit_dedupe_key",
            {"unique": True, "partialFilterExpression": {"dedupe_key": {"$exists": True}}},
        ),
    ]

    created = []
    for keys, name, options in index_specs:
        result_name = await collection.create_index(keys, name=name, **options)
        created.append(result_name)

    return created
