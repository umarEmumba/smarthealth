"""Billing request/response models.

`PrecheckRequest.coverage_ref` is the only place the raw coverage reference
exists: it is evaluated in memory and never stored, logged, audited or echoed
back (D-18, T-02-24). Responses expose statuses, coarse coverage kind, reason
codes and timestamps only — never idempotency keys or caller identities.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# Every billing state is a nameable target; the state machine (not this schema)
# decides which moves are legal, so an illegal move is a 409, not a 422.
TransitionTarget = Literal[
    "pending",
    "verified",
    "failed",
    "voided",
    "refund_pending",
    "refunded",
    "no_refund",
    "finalized",
]

REASON_CODE_PATTERN = r"^[a-z0-9_]+$"


class PrecheckRequest(BaseModel):
    appointment_id: uuid.UUID
    clinic_id: uuid.UUID
    coverage_ref: str | None = Field(default=None, max_length=64)


class TransitionRequest(BaseModel):
    target: TransitionTarget
    # Machine-readable code only (e.g. "booking_compensated", "visit_completed").
    reason_code: str | None = Field(
        default=None, min_length=1, max_length=64, pattern=REASON_CODE_PATTERN
    )


class BillingHistoryOut(BaseModel):
    from_status: str | None
    to_status: str
    reason_code: str | None
    created_at: datetime


class BillingRecordOut(BaseModel):
    id: uuid.UUID
    appointment_id: uuid.UUID
    clinic_id: uuid.UUID
    status: str
    coverage_kind: str
    reason_code: str | None
    created_at: datetime
    updated_at: datetime
    history: list[BillingHistoryOut]
