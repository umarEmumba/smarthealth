"""Exclusion-constraint tests for Scheduling's `provider_slots` (D-03, ORG-03).

Relocated from the Phase 1 monolith's `tests/test_models.py` and rewritten for
scheduling_db: `provider_slots.provider_id` is a plain UUID (no foreign key), so
no clinic/department/provider rows are needed — a random id stands in for the
provider. The GiST exclusion constraint `ex_provider_slots_no_overlap` is the
load-bearing double-booking backstop that Phase 2's booking saga relies on, so
it is proven against the live database, not merely declared.

Integration tests require the Postgres container with the scheduling migration
applied.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from services.scheduling.repository.models import ProviderSlot

pytestmark = pytest.mark.integration

BASE = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)


def _slot(
    provider_id: uuid.UUID, start: datetime, minutes: int = 30, status: str = "available"
) -> ProviderSlot:
    return ProviderSlot(
        provider_id=provider_id,
        start_time=start,
        end_time=start + timedelta(minutes=minutes),
        status=status,
    )


async def test_exclusion_constraint_live(scheduling_db_session: AsyncSession) -> None:
    """Overlapping non-cancelled slots for the same provider raise IntegrityError
    — enforced by Postgres itself, not just declared in the migration file."""
    provider_id = uuid.uuid4()
    scheduling_db_session.add(_slot(provider_id, BASE))
    await scheduling_db_session.flush()

    # Overlaps the first slot by 15 minutes for the SAME provider. Flushed inside
    # its own SAVEPOINT so the expected error only undoes this one statement.
    with pytest.raises(IntegrityError) as excinfo:
        async with scheduling_db_session.begin_nested():
            scheduling_db_session.add(_slot(provider_id, BASE + timedelta(minutes=15)))
            await scheduling_db_session.flush()
    assert "ex_provider_slots_no_overlap" in str(excinfo.value)


async def test_overlapping_cancelled_slot_is_accepted(scheduling_db_session: AsyncSession) -> None:
    """The constraint's WHERE clause ignores cancelled slots, so a window freed
    by a cancellation can be re-offered."""
    provider_id = uuid.uuid4()
    scheduling_db_session.add(_slot(provider_id, BASE))
    await scheduling_db_session.flush()

    scheduling_db_session.add(_slot(provider_id, BASE + timedelta(minutes=15), status="cancelled"))
    await scheduling_db_session.flush()

    count = await scheduling_db_session.scalar(
        select(func.count()).select_from(ProviderSlot).where(ProviderSlot.provider_id == provider_id)
    )
    assert count == 2


async def test_same_window_for_different_provider_is_accepted(
    scheduling_db_session: AsyncSession,
) -> None:
    scheduling_db_session.add(_slot(uuid.uuid4(), BASE))
    scheduling_db_session.add(_slot(uuid.uuid4(), BASE))
    await scheduling_db_session.flush()


async def test_back_to_back_slots_are_accepted(scheduling_db_session: AsyncSession) -> None:
    """tstzrange is half-open, so a slot ending exactly when the next starts
    does not overlap it."""
    provider_id = uuid.uuid4()
    first = _slot(provider_id, BASE)
    scheduling_db_session.add(first)
    scheduling_db_session.add(_slot(provider_id, first.end_time))
    await scheduling_db_session.flush()


async def test_non_cancelled_statuses_all_conflict(scheduling_db_session: AsyncSession) -> None:
    """held and booked slots block an overlap just like available ones."""
    provider_id = uuid.uuid4()
    scheduling_db_session.add(_slot(provider_id, BASE, status="held"))
    await scheduling_db_session.flush()

    with pytest.raises(IntegrityError):
        async with scheduling_db_session.begin_nested():
            scheduling_db_session.add(_slot(provider_id, BASE, status="booked"))
            await scheduling_db_session.flush()
