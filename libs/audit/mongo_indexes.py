"""Idempotent index setup for the `audit_log` MongoDB collection.

Per RESEARCH's MongoDB Audit-Trail Schema Design section: three indexes
support the query patterns the audit trail needs — "an entity's full
history," "a user's actions," and "a clinic's audit view."

Deliberately no auto-expiring retention index (RESEARCH assumption A2 — no
audit retention policy has been decided yet). Adding one here would silently
start deleting audit documents contrary to that recorded assumption.

`create_index` is itself idempotent by name in MongoDB — calling
`ensure_audit_indexes` multiple times (e.g. on every app startup) is safe
and a no-op after the first call.
"""

from __future__ import annotations

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient

from smarthealth.config import settings

AUDIT_COLLECTION_NAME = "audit_log"


async def ensure_audit_indexes(mongo_client: AsyncMongoClient) -> list[str]:
    """Create the three audit_log query indexes if they don't already exist.

    Returns the list of index names that exist after this call (as reported
    by `create_indexes`), so callers/tests can assert on it.
    """
    db = mongo_client[settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]

    index_specs = [
        (
            [("entity_type", ASCENDING), ("entity_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_entity_type_entity_id_timestamp",
        ),
        (
            [("actor_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_actor_id_timestamp",
        ),
        (
            [("clinic_id", ASCENDING), ("timestamp", DESCENDING)],
            "ix_audit_clinic_id_timestamp",
        ),
    ]

    created = []
    for keys, name in index_specs:
        result_name = await collection.create_index(keys, name=name)
        created.append(result_name)

    return created
