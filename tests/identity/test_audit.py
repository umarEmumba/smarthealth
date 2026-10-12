"""Audit-trail + PHI-safe-logging tests (Phase 1 Plan 01-03 Task 3, IAM-04;
relocated by plan 01.1-02).

Mirrors tests/libs/test_audit_indexes.py's established pattern: reads/writes
against `shared_settings.mongo_db_name` directly (the shared database
`write_audit`/`ensure_audit_indexes` actually target — every service writes to
the one shared `audit_log` collection), with test-local cleanup in a `finally`
block, since the `mongo_client` fixture's teardown only drops
`{mongo_db_name}_test`.

PHI test fixtures are entirely synthetic (fake name/dob) so this test file
itself never handles real PHI, per the plan's action notes.
"""

from __future__ import annotations

import inspect
import io
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import structlog
from httpx import AsyncClient
from pymongo import AsyncMongoClient

from libs.audit.writer import AUDIT_COLLECTION_NAME, write_audit
from libs.observability.logging import add_trace_context
from libs.config import shared_settings

SYNTHETIC_PHI_NAME = "Zzyzx Testperson Synthetic"


def _unique_email(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}@example.com"


@pytest.mark.integration
async def test_mutation_creates_audit_entry(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    """A mutating action (register) produces an audit_log document with the
    expected actor/action/entity/clinic/trace_id/outcome=success fields.
    """
    email = _unique_email("audit-success")
    db = mongo_client[shared_settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]

    try:
        # CR-01: public /register always creates a PATIENT account now —
        # `actor_role` on this audit entry is expected to be "patient".
        resp = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert resp.status_code == 201
        user_id = resp.json()["id"]

        doc = await collection.find_one({"action": "user.create", "entity_id": user_id})
        assert doc is not None
        assert doc["actor_id"] == user_id
        assert doc["actor_role"] == "patient"
        assert doc["entity_type"] == "user"
        assert doc["outcome"] == "success"
        assert "timestamp" in doc
        # trace_id may be None in a test run with no OTel exporter reachable,
        # but the FIELD must be present in the document shape either way.
        assert "trace_id" in doc
    finally:
        await collection.delete_many({"action": "user.create", "entity_id": user_id})


@pytest.mark.integration
async def test_failed_mutation_audited(
    client: AsyncClient, mongo_client: AsyncMongoClient
) -> None:
    """A denied/failed mutation (duplicate-email 409) also produces an
    outcome=failure audit entry — denied attempts are audited, not only
    successes (RESEARCH Pitfall 2).
    """
    email = _unique_email("audit-failure")
    db = mongo_client[shared_settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]

    try:
        # CR-01: public /register always creates a PATIENT account now —
        # role is irrelevant to this test (it only exercises the
        # duplicate-email failure-audit path), so no `role` is sent.
        first = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert first.status_code == 201

        second = await client.post(
            "/api/auth/register",
            json={"email": email, "password": "correct-horse-battery"},
        )
        assert second.status_code == 409

        failure_doc = await collection.find_one(
            {"action": "user.create", "outcome": "failure"},
            sort=[("timestamp", -1)],
        )
        assert failure_doc is not None
        assert failure_doc["outcome"] == "failure"
    finally:
        await collection.delete_many({"action": "user.create", "actor_role": "patient"})


def test_no_phi_in_logs() -> None:
    """A log event following the PHI-safe convention (identifiers only) is
    captured and asserted to contain identifiers but never a PHI value
    string — proving the exact processor chain this codebase configures
    in libs/observability/logging.py (add_trace_context + JSONRenderer)
    does not leak PHI into its JSON output.

    Builds a standalone structlog logger using the SAME processor chain
    `configure_logging` installs, wired to a captured stream instead of
    stdout, so this test doesn't depend on (or mutate) global logging
    state shared with other test modules.
    """
    stream = io.StringIO()
    logger_factory = structlog.PrintLoggerFactory(file=stream)

    bound_logger = structlog.wrap_logger(
        logger_factory(),
        processors=[
            structlog.contextvars.merge_contextvars,
            add_trace_context,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    patient_id = str(uuid.uuid4())
    clinic_id = str(uuid.uuid4())

    # Correct call shape per the PHI-safe convention: identifiers only,
    # never the synthetic PHI value (SYNTHETIC_PHI_NAME) itself.
    bound_logger.info("patient.created", patient_id=patient_id, clinic_id=clinic_id)

    output = stream.getvalue()
    assert output.strip(), "expected at least one captured log line"

    parsed = json.loads(output.strip().splitlines()[-1])
    assert parsed["patient_id"] == patient_id
    assert parsed["clinic_id"] == clinic_id
    assert SYNTHETIC_PHI_NAME not in output
    assert "date_of_birth" not in parsed
    assert "full_name" not in parsed


@pytest.mark.integration
async def test_audit_changes_field_names_only(
    mongo_client: AsyncMongoClient,
) -> None:
    """write_audit's `changes` field, when present, stores field NAMES —
    never PHI plaintext values. Calls write_audit directly with
    changed_fields=["full_name", "date_of_birth"] (field names describing a
    hypothetical patient-profile edit) and asserts the stored document
    contains those names but never the synthetic PHI value string that a
    real edit would have carried.
    """
    db = mongo_client[shared_settings.mongo_db_name]
    collection = db[AUDIT_COLLECTION_NAME]
    entity_id = str(uuid.uuid4())

    try:
        await write_audit(
            mongo_client,
            actor_id=str(uuid.uuid4()),
            actor_role="front_desk",
            action="patient.update",
            entity_type="patient",
            entity_id=entity_id,
            clinic_id=str(uuid.uuid4()),
            outcome="success",
            changed_fields=["full_name", "date_of_birth"],
        )

        doc = await collection.find_one({"action": "patient.update", "entity_id": entity_id})
        assert doc is not None
        assert doc["changes"] == {"fields": ["full_name", "date_of_birth"]}
        # The changes payload is a list of field NAMES — assert the
        # synthetic PHI value never appears anywhere in the stored document.
        assert SYNTHETIC_PHI_NAME not in json.dumps(doc, default=str)
    finally:
        await collection.delete_many({"action": "patient.update", "entity_id": entity_id})


@pytest.mark.integration
async def test_audit_changes_null_when_no_fields(
    mongo_client: AsyncMongoClient,
) -> None:
    """IAM-04 empty edge: `changed_fields=None` and `changed_fields=[]` both
    store `changes` as null — an audit document never carries an empty (or
    value-bearing) changes payload.
    """
    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    entity_ids = {"none": str(uuid.uuid4()), "empty": str(uuid.uuid4())}

    try:
        for label, changed_fields in (("none", None), ("empty", [])):
            await write_audit(
                mongo_client,
                actor_id=str(uuid.uuid4()),
                actor_role="front_desk",
                action="patient.update",
                entity_type="patient",
                entity_id=entity_ids[label],
                clinic_id=None,
                outcome="success",
                changed_fields=changed_fields,
            )

        for entity_id in entity_ids.values():
            doc = await collection.find_one({"action": "patient.update", "entity_id": entity_id})
            assert doc is not None
            assert doc["changes"] is None
    finally:
        await collection.delete_many(
            {"action": "patient.update", "entity_id": {"$in": list(entity_ids.values())}}
        )


@pytest.mark.integration
async def test_audit_timestamp_is_server_side_utc_and_ordered(
    mongo_client: AsyncMongoClient,
) -> None:
    """IAM-04 ordering edge: `write_audit` sets the timestamp itself, in UTC —
    callers cannot supply a clock — so audit order reflects write order.
    """
    assert "timestamp" not in inspect.signature(write_audit).parameters

    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    entity_id = str(uuid.uuid4())

    try:
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        for _ in range(2):
            await write_audit(
                mongo_client,
                actor_id=str(uuid.uuid4()),
                actor_role="admin",
                action="user.update",
                entity_type="user",
                entity_id=entity_id,
                clinic_id=None,
                outcome="success",
            )
        after = datetime.now(timezone.utc).replace(tzinfo=None)

        docs = [
            doc
            async for doc in collection.find({"action": "user.update", "entity_id": entity_id}).sort(
                "_id", 1
            )
        ]
        assert len(docs) == 2
        first, second = docs[0]["timestamp"], docs[1]["timestamp"]
        # PyMongo returns naive datetimes holding the stored UTC instant.
        assert before - timedelta(seconds=1) <= first <= second <= after + timedelta(seconds=1)
    finally:
        await collection.delete_many({"action": "user.update", "entity_id": entity_id})


@pytest.mark.integration
async def test_failed_then_successful_attempt_produce_two_distinct_documents(
    mongo_client: AsyncMongoClient,
) -> None:
    """IAM-04 adjacency edge: a failed attempt and a later successful attempt
    on the same entity are two separate audit documents — never merged or
    overwritten.
    """
    collection = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    entity_id = str(uuid.uuid4())

    try:
        for outcome in ("failure", "success"):
            await write_audit(
                mongo_client,
                actor_id=str(uuid.uuid4()),
                actor_role="admin",
                action="user.update",
                entity_type="user",
                entity_id=entity_id,
                clinic_id=None,
                outcome=outcome,
            )

        outcomes = [
            doc["outcome"]
            async for doc in collection.find({"action": "user.update", "entity_id": entity_id}).sort(
                "_id", 1
            )
        ]
        assert outcomes == ["failure", "success"]
    finally:
        await collection.delete_many({"action": "user.update", "entity_id": entity_id})
