"""Fake notification provider that honours an idempotency key (plan 03-02).

The provider's call record IS the delivered message. `deliver` inserts one
document into `notification_provider_calls` under a unique `dedupe_key` index, in
one atomic operation: a successful insert is the delivery (so there is no window
inside the fake between "delivered" and "recorded"); a `DuplicateKeyError` means
the key was already delivered, and the stored `message_id` is returned with
`replayed=True` without delivering again.

That is the guarantee which lets a worker killed between the provider call and
`mark_sent` be retried safely. A REAL SMS or email provider gives the same
guarantee only if it accepts an idempotency key for the request; this fake
demonstrates the pattern. Without such a key each crash at exactly that point
could cause one duplicate (stated in the docs, plan 03-18).

Failure injection by dedupe-key prefix (mirrors BILLING_PRECHECK_FAIL_PREFIX).
These model requests that never reached the provider, so they record nothing:

- `<fail_prefix>TRANSIENT-`  TransientDeliveryError on attempts 1 and 2
- `<fail_prefix>PERMANENT-`  TransientDeliveryError on every attempt (retries exhaust)
- `<fail_prefix>INVALID-`    PermanentDeliveryError

Real keys start with the notification type, so they never match a prefix.
The recipient is a patient id only (no contact data).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from prometheus_client import Counter
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from services.notification.config import settings
from services.notification.deliveries import PROVIDER_CALLS, get_db

if TYPE_CHECKING:
    from services.notification.tasks import SendRequest

PROVIDER_REPLAYED = Counter(
    "notifications_provider_replayed_total",
    "Provider calls answered from the idempotency store (a repeat of an already delivered key)",
    ["notification_type"],
)

TRANSIENT_ATTEMPTS = 2  # `TRANSIENT-` keys fail on attempts 1..TRANSIENT_ATTEMPTS


class TransientDeliveryError(Exception):
    """The provider could not take the request now; retry with backoff."""


class PermanentDeliveryError(Exception):
    """The provider refused the request for good; dead-letter it."""


@dataclass(frozen=True)
class ProviderResult:
    message_id: str
    replayed: bool


class FakeProvider:
    def __init__(self, db: Database | None = None) -> None:
        self._db = db

    @property
    def db(self) -> Database:
        return self._db if self._db is not None else get_db()

    def _inject_failure(self, dedupe_key: str, attempt: int) -> None:
        prefix = settings.fake_fail_prefix
        if not prefix or not dedupe_key.startswith(prefix):
            return
        marker = dedupe_key[len(prefix) :]
        if marker.startswith("TRANSIENT-") and attempt <= TRANSIENT_ATTEMPTS:
            raise TransientDeliveryError("injected transient provider failure")
        if marker.startswith("PERMANENT-"):
            raise TransientDeliveryError("injected provider failure that never clears")
        if marker.startswith("INVALID-"):
            raise PermanentDeliveryError("injected permanent provider rejection")

    def deliver(self, request: SendRequest, attempt: int) -> ProviderResult:
        self._inject_failure(request.dedupe_key, attempt)
        calls = self.db[PROVIDER_CALLS]
        message_id = str(uuid.uuid4())
        try:
            calls.insert_one(
                {
                    "dedupe_key": request.dedupe_key,
                    "message_id": message_id,
                    "notification_type": request.notification_type,
                    "patient_id": request.patient_id,
                    "attempt": attempt,
                    "delivered_at": datetime.now(UTC),
                }
            )
        except DuplicateKeyError:
            stored = calls.find_one({"dedupe_key": request.dedupe_key})
            PROVIDER_REPLAYED.labels(request.notification_type).inc()
            return ProviderResult(message_id=str(stored["message_id"]), replayed=True)
        return ProviderResult(message_id=message_id, replayed=False)
