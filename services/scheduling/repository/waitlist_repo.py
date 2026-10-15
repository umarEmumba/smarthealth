"""Waitlist writes and reads (plan 02-09; SCHED-07, D-12, research Pattern 7).

Every function takes the caller's `AsyncSession`, flushes, and never commits:
the caller (API route or Temporal activity) owns the transaction. Nothing here
deletes a row; every state change is a conditional `UPDATE ... WHERE <expected
state>`, so retries and races are arbitrated by Postgres.

Entry lifecycle (`WaitlistStatus`):

    waiting --claim--> matched --resolve--> booked
       ^                  |      \\--------> failed   (reason; never retried)
       |                  \\--lost race----> waiting  (slot taken by someone else)
       \\--cancel--> cancelled

The claim is the concurrency guard: `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`
picks the oldest matching waiting entry and moves it to `matched` in the same
transaction that creates the appointment, so two concurrent slot frees for the
same provider never claim the same entry (a locked row is skipped, a committed
`matched` row no longer matches).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.domain.enums import WaitlistStatus
from services.scheduling.repository.models import WaitlistEntry

ACTIVE_ENTRY_CONSTRAINT = "uq_waitlist_active_patient_provider"

CancelOutcome = Literal["cancelled", "already", "not_cancellable"]
ResolveOutcome = Literal["booked", "returned", "failed"]


async def create_entry(
    db: AsyncSession,
    *,
    clinic_id: uuid.UUID,
    provider_id: uuid.UUID,
    patient_id: uuid.UUID,
    window_start: datetime | None,
    window_end: datetime | None,
    coverage_ref: str | None,
    created_by: uuid.UUID,
) -> WaitlistEntry:
    """Insert a `waiting` entry. A second active entry for the same patient and
    provider raises `IntegrityError` naming `uq_waitlist_active_patient_provider`
    (the caller maps it to 409 and rolls back)."""
    entry = WaitlistEntry(
        clinic_id=clinic_id,
        provider_id=provider_id,
        patient_id=patient_id,
        window_start=window_start,
        window_end=window_end,
        coverage_ref=coverage_ref,
        status=WaitlistStatus.WAITING,
        created_by=created_by,
    )
    db.add(entry)
    await db.flush()
    await db.refresh(entry)
    return entry


async def get_entry(db: AsyncSession, entry_id: uuid.UUID) -> WaitlistEntry | None:
    return await db.get(WaitlistEntry, entry_id)


async def list_entries(
    db: AsyncSession,
    *,
    clinic_id: uuid.UUID | None = None,
    provider_id: uuid.UUID | None = None,
    patient_id: uuid.UUID | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[WaitlistEntry]:
    """Entries matching every given filter, oldest first (FIFO order). Scope
    filters are applied by the caller from the user's role."""
    query = select(WaitlistEntry)
    if clinic_id is not None:
        query = query.where(WaitlistEntry.clinic_id == clinic_id)
    if provider_id is not None:
        query = query.where(WaitlistEntry.provider_id == provider_id)
    if patient_id is not None:
        query = query.where(WaitlistEntry.patient_id == patient_id)
    if status is not None:
        query = query.where(WaitlistEntry.status == status)
    query = query.order_by(WaitlistEntry.created_at, WaitlistEntry.id).limit(limit)
    return list((await db.scalars(query)).all())


async def cancel_entry(db: AsyncSession, *, entry_id: uuid.UUID, actor: str) -> CancelOutcome:
    """`waiting -> cancelled` (conditional). "already" for a cancelled entry
    (a repeat), "not_cancellable" for matched/booked/failed or a missing row.
    `actor` is recorded by the caller's audit entry (ids only)."""
    del actor  # the history of an entry is its audit trail (waitlist.cancel)
    cancelled = await db.scalar(
        update(WaitlistEntry)
        .where(WaitlistEntry.id == entry_id, WaitlistEntry.status == WaitlistStatus.WAITING)
        .values(status=WaitlistStatus.CANCELLED, updated_at=func.now())
        .returning(WaitlistEntry.id)
    )
    if cancelled is not None:
        return "cancelled"
    current = await db.scalar(select(WaitlistEntry.status).where(WaitlistEntry.id == entry_id))
    return "already" if current == WaitlistStatus.CANCELLED else "not_cancellable"


async def find_matched_entry(
    db: AsyncSession, *, appointment_id: uuid.UUID
) -> WaitlistEntry | None:
    """The entry already matched to `appointment_id` (a retried claim)."""
    return await db.scalar(
        select(WaitlistEntry).where(
            WaitlistEntry.matched_appointment_id == appointment_id,
            WaitlistEntry.status == WaitlistStatus.MATCHED,
        )
    )


async def claim_next_entry(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    slot_start: datetime,
    slot_end: datetime,
    appointment_id: uuid.UUID,
) -> WaitlistEntry | None:
    """Claim the oldest waiting entry for `provider_id` whose window contains
    `[slot_start, slot_end]` and mark it `matched` to `appointment_id`.

    A retry first finds the entry already matched to the same appointment id
    and returns it unchanged. Otherwise `FOR UPDATE SKIP LOCKED LIMIT 1`
    ordered by `(created_at, id)` (served by `ix_waitlist_fifo`): a row locked
    by a concurrent claim is skipped, never waited on, so concurrent frees
    claim different entries. None when nothing matches.
    """
    existing = await find_matched_entry(db, appointment_id=appointment_id)
    if existing is not None:
        return existing

    candidate = await db.scalar(
        select(WaitlistEntry)
        .where(
            WaitlistEntry.provider_id == provider_id,
            WaitlistEntry.status == WaitlistStatus.WAITING,
            or_(WaitlistEntry.window_start.is_(None), WaitlistEntry.window_start <= slot_start),
            or_(WaitlistEntry.window_end.is_(None), WaitlistEntry.window_end >= slot_end),
        )
        .order_by(WaitlistEntry.created_at, WaitlistEntry.id)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if candidate is None:
        return None
    await db.execute(
        update(WaitlistEntry)
        .where(WaitlistEntry.id == candidate.id, WaitlistEntry.status == WaitlistStatus.WAITING)
        .values(
            status=WaitlistStatus.MATCHED,
            matched_appointment_id=appointment_id,
            attempts=WaitlistEntry.attempts + 1,
            updated_at=func.now(),
        )
        .execution_options(synchronize_session=False)
    )
    await db.refresh(candidate)
    return candidate


async def fail_waiting_entry(db: AsyncSession, *, entry_id: uuid.UUID, reason: str) -> bool:
    """`waiting -> failed` with a machine-readable reason, for a claim whose
    appointment insert was refused for this patient (e.g. `PatientOverlap`)
    and rolled back. Counts the attempt. True when applied now."""
    failed = await db.scalar(
        update(WaitlistEntry)
        .where(WaitlistEntry.id == entry_id, WaitlistEntry.status == WaitlistStatus.WAITING)
        .values(
            status=WaitlistStatus.FAILED,
            failure_reason=reason[:64],
            attempts=WaitlistEntry.attempts + 1,
            updated_at=func.now(),
        )
        .returning(WaitlistEntry.id)
    )
    return failed is not None


async def resolve_entry(
    db: AsyncSession,
    *,
    entry_id: uuid.UUID,
    appointment_id: uuid.UUID,
    outcome: ResolveOutcome,
    reason: str | None,
) -> bool:
    """Settle a `matched` entry after its booking saga finished. Conditional on
    `status = 'matched' AND matched_appointment_id = :a`, so a retry (or a
    stale call) changes nothing and returns False.

    * "booked"   -> `booked`, `booked_appointment_id = :a`;
    * "returned" -> back to `waiting` (lost slot race), match cleared;
    * "failed"   -> `failed` with `reason` (never retried).
    """
    values: dict = {"updated_at": func.now()}
    if outcome == "booked":
        values |= {"status": WaitlistStatus.BOOKED, "booked_appointment_id": appointment_id}
    elif outcome == "returned":
        values |= {"status": WaitlistStatus.WAITING, "matched_appointment_id": None}
    else:
        values |= {"status": WaitlistStatus.FAILED, "failure_reason": (reason or "unknown")[:64]}
    resolved = await db.scalar(
        update(WaitlistEntry)
        .where(
            WaitlistEntry.id == entry_id,
            WaitlistEntry.status == WaitlistStatus.MATCHED,
            WaitlistEntry.matched_appointment_id == appointment_id,
        )
        .values(**values)
        .returning(WaitlistEntry.id)
    )
    return resolved is not None
