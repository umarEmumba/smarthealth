"""Mongo delivery log for notifications (plan 03-02, NOTIFICATION NOTIF-04).

Three collections in the shared Mongo database (`shared_settings.mongo_db_name`):

- `notification_deliveries`: one document per dedupe key with a status machine
  (`sending` -> `sent` | `failed` | `skipped_closed` | `skipped_stale`).
- `notification_provider_calls`: the fake provider's idempotency store (see
  `provider.py`), unique on `dedupe_key`.
- `notification_appointment_state`: appointments recorded as closed (cancelled or
  no-show), read by the send-time guard for reminders.

Sync `pymongo` on purpose: the Celery threads pool runs sync task bodies. Only
ids, types, statuses and timestamps are stored; the recipient is a patient id.
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from libs.config import shared_settings

if TYPE_CHECKING:
    from services.notification.tasks import SendRequest

DELIVERIES = "notification_deliveries"
PROVIDER_CALLS = "notification_provider_calls"
APPOINTMENT_STATE = "notification_appointment_state"

STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"
STATUS_SKIPPED_CLOSED = "skipped_closed"
STATUS_SKIPPED_STALE = "skipped_stale"
_TERMINAL = (STATUS_SENT, STATUS_SKIPPED_CLOSED, STATUS_SKIPPED_STALE)
_CLAIMABLE = (STATUS_SENDING, STATUS_FAILED)

_client: MongoClient | None = None
_client_lock = threading.Lock()


def get_client() -> MongoClient:
    global _client  # noqa: PLW0603
    with _client_lock:
        if _client is None:
            _client = MongoClient(shared_settings.mongo_url, tz_aware=True)
        return _client


def get_db() -> Database:
    """The notification database, resolved at call time (tests point it elsewhere)."""
    return get_client()[shared_settings.mongo_db_name]


def _now() -> datetime:
    return datetime.now(UTC)


def ensure_notification_indexes(db: Database | None = None) -> None:
    db = db if db is not None else get_db()
    db[DELIVERIES].create_index([("dedupe_key", ASCENDING)], name="ux_delivery_dedupe_key", unique=True)
    db[PROVIDER_CALLS].create_index(
        [("dedupe_key", ASCENDING)], name="ux_provider_call_dedupe_key", unique=True
    )
    db[APPOINTMENT_STATE].create_index(
        [("appointment_id", ASCENDING)], name="ux_appointment_state_appointment_id", unique=True
    )


class Claim(enum.Enum):
    NEW = "new"  # first attempt for this key
    RETRY = "retry"  # an earlier attempt died or failed; go again
    DONE = "done"  # already sent or skipped; do nothing


@dataclass(frozen=True)
class ClaimResult:
    state: Claim
    attempts: int


def claim_delivery(db: Database, request: SendRequest) -> ClaimResult:
    """Claim the dedupe key for this attempt.

    `NEW` on a first insert, `RETRY` (with the incremented attempt number) when a
    previous attempt left `sending` or `failed`, `DONE` when the key is already
    `sent` or skipped. The provider is never called for `DONE`.
    """
    now = _now()
    document = {
        "dedupe_key": request.dedupe_key,
        "notification_type": request.notification_type,
        "patient_id": request.patient_id,
        "appointment_id": request.appointment_id,
        "visit_id": request.visit_id,
        "event_id": request.event_id,
        "status": STATUS_SENDING,
        "attempts": 1,
        "created_at": now,
        "updated_at": now,
    }
    try:
        db[DELIVERIES].insert_one(document)
    except DuplicateKeyError:
        pass
    else:
        return ClaimResult(Claim.NEW, 1)

    updated = db[DELIVERIES].find_one_and_update(
        {"dedupe_key": request.dedupe_key, "status": {"$in": list(_CLAIMABLE)}},
        {"$inc": {"attempts": 1}, "$set": {"status": STATUS_SENDING, "updated_at": now}},
        return_document=True,
    )
    if updated is not None:
        return ClaimResult(Claim.RETRY, int(updated["attempts"]))
    # Not claimable: it reached a terminal status (possibly concurrently).
    existing = db[DELIVERIES].find_one({"dedupe_key": request.dedupe_key})
    return ClaimResult(Claim.DONE, int(existing["attempts"]) if existing else 0)


def mark_sent(db: Database, dedupe_key: str, message_id: str, *, replayed: bool = False) -> None:
    now = _now()
    db[DELIVERIES].update_one(
        {"dedupe_key": dedupe_key},
        {
            "$set": {
                "status": STATUS_SENT,
                "message_id": message_id,
                "provider_replayed": replayed,
                "sent_at": now,
                "updated_at": now,
            }
        },
    )


def mark_failed(db: Database, dedupe_key: str, error_type: str) -> None:
    """Record a failed attempt; the key stays claimable. Error TYPE only, never text."""
    db[DELIVERIES].update_one(
        {"dedupe_key": dedupe_key, "status": {"$in": list(_CLAIMABLE)}},
        {"$set": {"status": STATUS_FAILED, "last_error_type": error_type, "updated_at": _now()}},
    )


def mark_skipped(db: Database, request: SendRequest, reason: str) -> None:
    """Record a skipped send (`reason` is `closed` or `stale`) without ever overwriting `sent`."""
    status = STATUS_SKIPPED_CLOSED if reason == "closed" else STATUS_SKIPPED_STALE
    now = _now()
    try:
        db[DELIVERIES].insert_one(
            {
                "dedupe_key": request.dedupe_key,
                "notification_type": request.notification_type,
                "patient_id": request.patient_id,
                "appointment_id": request.appointment_id,
                "visit_id": request.visit_id,
                "event_id": request.event_id,
                "status": status,
                "attempts": 0,
                "created_at": now,
                "updated_at": now,
            }
        )
    except DuplicateKeyError:
        db[DELIVERIES].update_one(
            {"dedupe_key": request.dedupe_key, "status": {"$in": list(_CLAIMABLE)}},
            {"$set": {"status": status, "updated_at": now}},
        )


def record_skipped(db: Database, request: SendRequest, reason: str) -> None:
    """Record a send that was never published (`reason` is `stale` or `closed`), without a
    provider call. Insert-only: a key that already has a document is left as it is, so a
    redelivered event can never overwrite a `sent` delivery."""
    status = STATUS_SKIPPED_CLOSED if reason == "closed" else STATUS_SKIPPED_STALE
    now = _now()
    try:
        db[DELIVERIES].insert_one(
            {
                "dedupe_key": request.dedupe_key,
                "notification_type": request.notification_type,
                "patient_id": request.patient_id,
                "appointment_id": request.appointment_id,
                "visit_id": request.visit_id,
                "event_id": request.event_id,
                "status": status,
                "attempts": 0,
                "created_at": now,
                "updated_at": now,
            }
        )
    except DuplicateKeyError:
        pass


def mark_appointment_closed(db: Database, appointment_id: str, reason: str) -> None:
    """Record that an appointment is cancelled or a no-show (idempotent)."""
    db[APPOINTMENT_STATE].update_one(
        {"appointment_id": appointment_id},
        {
            "$set": {"closed_reason": reason},
            "$setOnInsert": {"closed_at": _now()},
        },
        upsert=True,
    )


def is_appointment_closed(db: Database, appointment_id: str) -> bool:
    return db[APPOINTMENT_STATE].find_one({"appointment_id": appointment_id}, {"_id": 1}) is not None
