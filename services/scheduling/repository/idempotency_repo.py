"""Idempotency-key arbitration for booking requests (D-08, SCHED-04).

The `(caller_user_id, key)` primary key of `idempotency_keys` is the arbiter
for duplicate requests. The claim is written FIRST in the accept transaction,
before the appointment row, so two concurrent requests with the same key are
serialized by Postgres: the second `INSERT ... ON CONFLICT` waits for the first
transaction, then either sees its committed row (replay) or, if the first
rolled back, claims the key itself.

Expiry is lazy (24 hours): an expired row is overwritten in place by the next
claim with the same key, never deleted (project rule: no hard deletes).

Functions flush through the caller's session and never commit; the caller owns
the transaction. No request body or other PHI is stored, only its SHA-256.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

KEY_TTL_HOURS = 24

_CLAIM_SQL = text(
    f"""
    INSERT INTO idempotency_keys
        (caller_user_id, key, request_hash, appointment_id, expires_at, created_at)
    VALUES
        (:caller_user_id, :key, :request_hash, :appointment_id,
         now() + interval '{KEY_TTL_HOURS} hours', now())
    ON CONFLICT (caller_user_id, key) DO UPDATE
        SET request_hash = EXCLUDED.request_hash,
            appointment_id = EXCLUDED.appointment_id,
            expires_at = EXCLUDED.expires_at,
            created_at = now()
        WHERE idempotency_keys.expires_at < now()
    RETURNING appointment_id
    """
)


async def claim_idempotency_key(
    db: AsyncSession,
    *,
    caller_user_id: uuid.UUID,
    key: str,
    request_hash: str,
    appointment_id: uuid.UUID,
) -> uuid.UUID | None:
    """Try to make this request the owner of `(caller_user_id, key)`.

    Returns `appointment_id` when this request owns the key (a fresh key, or an
    expired one that was overwritten), or None when a live claim already exists
    (the caller then reads it with `get_idempotency_record`).
    """
    return await db.scalar(
        _CLAIM_SQL,
        {
            "caller_user_id": caller_user_id,
            "key": key,
            "request_hash": request_hash,
            "appointment_id": appointment_id,
        },
    )


async def get_idempotency_record(
    db: AsyncSession, *, caller_user_id: uuid.UUID, key: str
) -> tuple[str, uuid.UUID] | None:
    """`(request_hash, appointment_id)` stored for the key, or None."""
    row = (
        await db.execute(
            text(
                "SELECT request_hash, appointment_id FROM idempotency_keys "
                "WHERE caller_user_id = :caller_user_id AND key = :key"
            ),
            {"caller_user_id": caller_user_id, "key": key},
        )
    ).one_or_none()
    if row is None:
        return None
    return row.request_hash.strip(), row.appointment_id
