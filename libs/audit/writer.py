"""The single shared audit-write helper — every mutating route in this
codebase calls `write_audit`, never a per-router Mongo insert copy (RESEARCH
anti-patterns: a copy-pasted audit write is how an endpoint silently ends up
unaudited).

Document shape (locked contract, checkpoint-resolved — see RESEARCH's
MongoDB Audit-Trail Schema Design section):

    {
        actor_id: str,        # opaque user UUID string, or "system" for
                               # non-human-initiated actions (future phases)
        actor_role: str,
        action: str,           # dot-namespaced "<entity>.<verb>", e.g. "user.create"
        entity_type: str,
        entity_id: str,
        clinic_id: str | None,
        timestamp: datetime,   # UTC, set here — never trust a caller-supplied clock
        trace_id: str | None,  # correlates to the request's OTel trace in Jaeger
        outcome: "success" | "failure",
        changes: {"fields": [str, ...]} | None,  # field NAMES only, never PHI values
    }

PHI boundary: `changes` records which fields changed, never their values.
Passing a PHI value (patient name/dob/phone/address) into `changes` is a
caller bug this helper does not attempt to auto-redact — callers must only
ever pass field names.

Idempotent writes (plan 02-01, research Pitfall 7): Temporal activities retry,
and the design wants exactly one audit entry per workflow step. Passing
`dedupe_key` (e.g. `f"{workflow_id}:{step}"`, never PHI) stores it on the
document and upserts with `$setOnInsert` behind the partial unique index
`ix_audit_dedupe_key`, so a retried or concurrently duplicated step leaves one
document. Without `dedupe_key` the writer inserts exactly as before.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

from libs.config import shared_settings
from libs.observability.otel import current_trace_id

AUDIT_COLLECTION_NAME = "audit_log"

Outcome = Literal["success", "failure"]


async def write_audit(
    mongo_client: AsyncMongoClient,
    *,
    actor_id: str,
    actor_role: str,
    action: str,
    entity_type: str,
    entity_id: str,
    clinic_id: str | None,
    outcome: Outcome,
    trace_id: str | None = None,
    changed_fields: list[str] | None = None,
    dedupe_key: str | None = None,
) -> None:
    """Write one audit_log document. `trace_id` defaults to the active OTel
    span's trace_id if not explicitly supplied (so most call sites don't
    need to thread it through manually).

    `changed_fields` must be field NAMES ONLY — never pass PHI values here.

    `dedupe_key`: when set, the write is idempotent — the first write for a key
    wins and later writes with the same key are no-ops (a concurrent upsert
    race surfaces as `DuplicateKeyError`, treated as success). The audit write
    is NOT atomic with the caller's Postgres commit (review WR-04), so an
    activity writes its audit entry as its LAST step, after the idempotent
    business write has committed.
    """
    # Every service writes to the ONE shared `audit_log` collection in the
    # shared Mongo database (`MONGO_DB_NAME`): the audit trail is cross-cutting
    # infrastructure owned by the future Audit service (D-02), not a service
    # database, and actor/clinic queries span services.
    db = mongo_client[shared_settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]

    document = {
        "actor_id": actor_id,
        "actor_role": actor_role,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "clinic_id": clinic_id,
        "timestamp": datetime.now(timezone.utc),
        "trace_id": trace_id if trace_id is not None else current_trace_id(),
        "outcome": outcome,
        "changes": {"fields": changed_fields} if changed_fields else None,
    }

    if dedupe_key is None:
        await collection.insert_one(document)
        return

    document["dedupe_key"] = dedupe_key
    try:
        await collection.update_one(
            {"dedupe_key": dedupe_key}, {"$setOnInsert": document}, upsert=True
        )
    except DuplicateKeyError:
        # A concurrent writer inserted the same dedupe_key first: the entry exists.
        pass
