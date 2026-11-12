"""Billing persistence: pre-check creation, state-targeted transitions, re-link.

Every function here flushes but NEVER commits: the API route owns the
transaction. The contract for the two state-changing helpers is:

  * `APPLIED`  -> the caller commits, then writes audit;
  * `NOOP`     -> the caller rolls back (nothing of value is pending) and
                  answers 200 with the current state;
  * `CONFLICT` -> the caller rolls back and answers 409.

Idempotency (D-06) is by STATE and by KEY:
  * requesting the state the record is already in is a no-op;
  * a request whose `Idempotency-Key` already appears in this record's history
    is a no-op (checked up front, and enforced by
    `uq_billing_history_record_key` for the concurrent case);
  * the move itself is a conditional `UPDATE ... WHERE status = :from`, so two
    racing requests cannot both apply — the loser re-reads and becomes a no-op
    (same target) or a 409 (different target).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from services.billing.domain.rules import PrecheckDecision
from services.billing.domain.states import (
    RELINK_REASON,
    RELINKABLE,
    BillingStatus,
    can_transition,
)
from services.billing.repository.models import BillingRecord, BillingStatusHistory

# The pre-check writes two history rows in one transaction; the UNIQUE
# (record_id, idempotency_key) constraint forbids them sharing a key, so the
# outcome row carries the request key plus this suffix.
PRECHECK_OUTCOME_KEY_SUFFIX = ":outcome"
ACTOR_MAX_LEN = 64


class TransitionOutcome:
    APPLIED = "applied"
    NOOP = "noop"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class TransitionResult:
    outcome: str
    current_status: str


def _actor(actor: str) -> str:
    return actor[:ACTOR_MAX_LEN]


async def get_by_appointment(db: AsyncSession, appointment_id: uuid.UUID) -> BillingRecord | None:
    result = await db.execute(
        select(BillingRecord)
        .where(BillingRecord.appointment_id == appointment_id)
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def get_record(db: AsyncSession, record_id: uuid.UUID) -> BillingRecord | None:
    result = await db.execute(
        select(BillingRecord)
        .where(BillingRecord.id == record_id)
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def list_history(db: AsyncSession, record_id: uuid.UUID) -> list[BillingStatusHistory]:
    result = await db.execute(
        select(BillingStatusHistory)
        .where(BillingStatusHistory.record_id == record_id)
        .order_by(BillingStatusHistory.created_at, BillingStatusHistory.id)
    )
    return list(result.scalars().all())


async def create_precheck(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID,
    decision: PrecheckDecision,
    idempotency_key: str,
    actor: str,
) -> BillingRecord:
    """Insert the record as `pending`, then apply the decision, with one history
    row per step — all in the caller's transaction, so `pending` is never
    observable on its own. A duplicate `appointment_id` raises IntegrityError
    from the first flush (the caller rolls back and returns the winner)."""
    record = BillingRecord(
        appointment_id=appointment_id,
        clinic_id=clinic_id,
        status=BillingStatus.PENDING,
        coverage_kind=decision.coverage_kind,
    )
    db.add(record)
    await db.flush()

    await db.execute(
        insert(BillingStatusHistory).values(
            record_id=record.id,
            from_status=None,
            to_status=BillingStatus.PENDING,
            idempotency_key=idempotency_key,
            actor=_actor(actor),
        )
    )
    await db.execute(
        update(BillingRecord)
        .where(BillingRecord.id == record.id, BillingRecord.status == BillingStatus.PENDING)
        .values(status=decision.status, reason_code=decision.reason_code, updated_at=func.now())
    )
    await db.execute(
        insert(BillingStatusHistory).values(
            record_id=record.id,
            from_status=BillingStatus.PENDING,
            to_status=decision.status,
            reason_code=decision.reason_code,
            idempotency_key=f"{idempotency_key}{PRECHECK_OUTCOME_KEY_SUFFIX}",
            actor=_actor(actor),
        )
    )
    await db.flush()
    return record


async def _key_used(db: AsyncSession, record_id: uuid.UUID, idempotency_key: str) -> bool:
    result = await db.execute(
        select(BillingStatusHistory.id).where(
            BillingStatusHistory.record_id == record_id,
            BillingStatusHistory.idempotency_key == idempotency_key,
        )
    )
    return result.first() is not None


async def _current_status(db: AsyncSession, record_id: uuid.UUID, *, lock: bool = False) -> str:
    stmt = select(BillingRecord.status).where(BillingRecord.id == record_id)
    if lock:
        stmt = stmt.with_for_update()
    return (await db.execute(stmt)).scalar_one()


async def _append_history(
    db: AsyncSession,
    *,
    record_id: uuid.UUID,
    from_status: str,
    to_status: str,
    reason_code: str | None,
    idempotency_key: str,
    actor: str,
) -> bool:
    """Insert one history row inside a savepoint; False when the key was taken
    concurrently (the outer transaction stays usable for the caller's rollback)."""
    try:
        async with db.begin_nested():
            await db.execute(
                insert(BillingStatusHistory).values(
                    record_id=record_id,
                    from_status=from_status,
                    to_status=to_status,
                    reason_code=reason_code,
                    idempotency_key=idempotency_key,
                    actor=_actor(actor),
                )
            )
    except IntegrityError:
        return False
    return True


async def transition_record(
    db: AsyncSession,
    record_id: uuid.UUID,
    target: str,
    *,
    idempotency_key: str,
    reason_code: str | None,
    actor: str,
) -> TransitionResult:
    """Move the record to `target` if the state machine allows it (see module doc)."""
    current = await _current_status(db, record_id)
    if await _key_used(db, record_id, idempotency_key) or current == target:
        return TransitionResult(TransitionOutcome.NOOP, current)
    if not can_transition(current, target):
        return TransitionResult(TransitionOutcome.CONFLICT, current)

    values: dict[str, object] = {"status": target, "updated_at": func.now()}
    if reason_code is not None:
        values["reason_code"] = reason_code
    moved = await db.execute(
        update(BillingRecord)
        .where(BillingRecord.id == record_id, BillingRecord.status == current)
        .values(**values)
        .returning(BillingRecord.id)
        .execution_options(synchronize_session=False)
    )
    if moved.first() is None:
        # Lost a race: someone else moved the record first.
        now = await _current_status(db, record_id)
        outcome = TransitionOutcome.NOOP if now == target else TransitionOutcome.CONFLICT
        return TransitionResult(outcome, now)

    inserted = await _append_history(
        db,
        record_id=record_id,
        from_status=current,
        to_status=target,
        reason_code=reason_code,
        idempotency_key=idempotency_key,
        actor=actor,
    )
    if not inserted:
        return TransitionResult(TransitionOutcome.NOOP, current)
    return TransitionResult(TransitionOutcome.APPLIED, target)


async def append_relink(
    db: AsyncSession,
    record_id: uuid.UUID,
    *,
    idempotency_key: str,
    actor: str,
) -> TransitionResult:
    """Record a reschedule re-link (D-14): a history row with
    `from_status == to_status` and reason `rescheduled`; the state is unchanged."""
    if await _key_used(db, record_id, idempotency_key):
        return TransitionResult(TransitionOutcome.NOOP, await _current_status(db, record_id))
    # Row lock so the state cannot move between this check and the history row.
    current = await _current_status(db, record_id, lock=True)
    if current not in RELINKABLE:
        return TransitionResult(TransitionOutcome.CONFLICT, current)

    inserted = await _append_history(
        db,
        record_id=record_id,
        from_status=current,
        to_status=current,
        reason_code=RELINK_REASON,
        idempotency_key=idempotency_key,
        actor=actor,
    )
    if not inserted:
        return TransitionResult(TransitionOutcome.NOOP, current)
    await db.execute(
        update(BillingRecord)
        .where(BillingRecord.id == record_id)
        .values(updated_at=func.now())
        .execution_options(synchronize_session=False)
    )
    return TransitionResult(TransitionOutcome.APPLIED, current)
