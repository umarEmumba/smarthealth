"""Visit persistence: idempotent check-in, forward-only transitions, clinic list.

Every function here flushes but NEVER commits: the API route owns the
transaction (the Billing/Scheduling convention). Nothing here deletes.

Duplicate safety (D-15, research Pattern 9):
  * `create_visit` is `INSERT ... ON CONFLICT (appointment_id) DO NOTHING
    RETURNING id`: of N concurrent check-ins for one appointment exactly one
    inserts (and writes the `null -> checked_in` history row); the others read
    the winner's visit back.
  * `transition_visit` is a conditional `UPDATE ... WHERE id = :v AND
    status = <the target's only predecessor>`. A concurrent loser blocks on
    the winner's row lock, re-evaluates the predicate after the winner
    commits, matches zero rows, re-reads and lands in `noop`. The history
    insert is additionally guarded by `uq_visit_history_visit_to`; if that
    ever raises, the `IntegrityError` propagates and the route rolls the whole
    transaction back and answers as a no-op.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from services.visit.domain.states import ORDER, PREVIOUS, TransitionOutcome, VisitStatus
from services.visit.repository.models import Visit, VisitStateHistory

ACTOR_MAX_LEN = 64


def _actor(actor: str) -> str:
    return actor[:ACTOR_MAX_LEN]


async def get_visit(db: AsyncSession, visit_id: uuid.UUID) -> Visit | None:
    result = await db.execute(
        select(Visit).where(Visit.id == visit_id).execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def get_by_appointment(db: AsyncSession, appointment_id: uuid.UUID) -> Visit | None:
    result = await db.execute(
        select(Visit)
        .where(Visit.appointment_id == appointment_id)
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def get_history(db: AsyncSession, visit_id: uuid.UUID) -> list[VisitStateHistory]:
    result = await db.execute(
        select(VisitStateHistory)
        .where(VisitStateHistory.visit_id == visit_id)
        .order_by(VisitStateHistory.created_at, VisitStateHistory.id)
    )
    return list(result.scalars().all())


async def create_visit(
    db: AsyncSession,
    *,
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID,
    patient_id: uuid.UUID,
    provider_id: uuid.UUID,
    created_by: uuid.UUID,
    actor: str,
) -> tuple[Visit, bool]:
    """(visit, created). Inserts the visit in `checked_in` plus its first
    history row, or returns the appointment's existing visit unchanged."""
    inserted = await db.execute(
        pg_insert(Visit)
        .values(
            appointment_id=appointment_id,
            clinic_id=clinic_id,
            patient_id=patient_id,
            provider_id=provider_id,
            status=VisitStatus.CHECKED_IN,
            created_by=created_by,
        )
        .on_conflict_do_nothing(index_elements=[Visit.appointment_id])
        .returning(Visit.id)
    )
    visit_id = inserted.scalar_one_or_none()
    if visit_id is None:
        existing = await get_by_appointment(db, appointment_id)
        assert existing is not None  # the conflicting row is committed and visible
        return existing, False

    await db.execute(
        insert(VisitStateHistory).values(
            visit_id=visit_id,
            from_status=None,
            to_status=VisitStatus.CHECKED_IN,
            actor=_actor(actor),
        )
    )
    await db.flush()
    visit = await get_visit(db, visit_id)
    assert visit is not None
    return visit, True


async def _current_status(db: AsyncSession, visit_id: uuid.UUID) -> str:
    return (await db.execute(select(Visit.status).where(Visit.id == visit_id))).scalar_one()


async def transition_visit(
    db: AsyncSession, *, visit_id: uuid.UUID, target: str, actor: str
) -> str:
    """Move the visit to `target` (see module doc). Returns
    `"applied"`, `"noop"` (target is current or already passed) or
    `"skipped"` (target is more than one step ahead)."""
    if target not in PREVIOUS:
        raise ValueError(f"not a transition target: {target}")
    expected = PREVIOUS[target]

    values: dict[str, object] = {"status": target, "updated_at": func.now()}
    if target == VisitStatus.IN_PROGRESS:
        values["started_at"] = func.now()
    elif target == VisitStatus.COMPLETED:
        # downstream_started_at deliberately stays NULL (plan 02-11 sweeper).
        values["completed_at"] = func.now()

    moved = await db.execute(
        update(Visit)
        .where(Visit.id == visit_id, Visit.status == expected)
        .values(**values)
        .returning(Visit.id)
        .execution_options(synchronize_session=False)
    )
    if moved.first() is None:
        current = await _current_status(db, visit_id)
        if ORDER[current] >= ORDER[target]:
            return TransitionOutcome.NOOP
        return TransitionOutcome.SKIPPED

    await db.execute(
        insert(VisitStateHistory).values(
            visit_id=visit_id, from_status=expected, to_status=target, actor=_actor(actor)
        )
    )
    await db.flush()
    return TransitionOutcome.APPLIED


async def list_visits(
    db: AsyncSession,
    *,
    clinic_id: uuid.UUID,
    status: str | None = None,
    provider_id: uuid.UUID | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Visit]:
    """One clinic's visits, newest first (`ix_visits_clinic_status_created`).
    `date_from` (inclusive) / `date_to` (exclusive) bound `checked_in_at`."""
    query = select(Visit).where(Visit.clinic_id == clinic_id)
    if status is not None:
        query = query.where(Visit.status == status)
    if provider_id is not None:
        query = query.where(Visit.provider_id == provider_id)
    if date_from is not None:
        query = query.where(Visit.checked_in_at >= date_from)
    if date_to is not None:
        query = query.where(Visit.checked_in_at < date_to)
    query = query.order_by(Visit.created_at.desc(), Visit.id).limit(limit).offset(offset)
    return list((await db.scalars(query)).all())


async def mark_downstream_started(db: AsyncSession, *, visit_id: uuid.UUID) -> bool:
    """Record that the visit's downstream workflow has been started (plan 02-11,
    D-16). Conditional (`WHERE downstream_started_at IS NULL`), so a repeat or
    a race with the sweeper is harmless: True when this call set the marker.
    Flushes only; the caller commits."""
    marked = await db.execute(
        update(Visit)
        .where(Visit.id == visit_id, Visit.downstream_started_at.is_(None))
        .values(downstream_started_at=func.now())
        .returning(Visit.id)
        .execution_options(synchronize_session=False)
    )
    return marked.first() is not None


async def list_downstream_unstarted(
    db: AsyncSession, *, older_than_seconds: int, limit: int = 100
) -> list[Visit]:
    """Completed visits whose downstream workflow was never started (plan
    02-11): `status = 'completed' AND downstream_started_at IS NULL AND
    completed_at < now() - older_than_seconds`, oldest first, at most `limit`.
    Served by the partial index `ix_visits_downstream_pending`. No row lock:
    the sweeper calls Temporal per row, and a duplicate start is a no-op."""
    query = (
        select(Visit)
        .where(
            Visit.status == VisitStatus.COMPLETED,
            Visit.downstream_started_at.is_(None),
            Visit.completed_at
            < text("now() - make_interval(secs => :grace)").bindparams(
                grace=float(older_than_seconds)
            ),
        )
        .order_by(Visit.completed_at)
        .limit(limit)
    )
    return list((await db.scalars(query)).all())
