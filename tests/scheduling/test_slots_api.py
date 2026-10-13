"""Slot supply through the gateway (plan 02-04, D-01, D-18, research Pattern 6).

`/api/slots` is Scheduling's first public surface: an admin or front-desk user
materializes slots from Provider's templates in the clinic's timezone (resolved
over service tokens from Provider and Organization), and any authenticated user
searches them. Everything here goes through the gateway over the in-process
mesh; slot rows are committed and removed per test by provider id.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, time, timezone

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.scheduling.conftest import cleanup_scheduling_rows
from tests.scheduling.seed import World, auth, seed_bookable_world

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def world(
    client: AsyncClient, mesh: httpx.AsyncClient, committed_scheduling_engine: AsyncEngine
) -> AsyncIterator[World]:
    seeded = await seed_bookable_world(client, mesh)
    try:
        yield seeded
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, [seeded.provider_id])


async def _search(client: AsyncClient, world: World, tokens: dict, start: datetime, end: datetime, **params):
    return await client.get(
        "/api/slots",
        params={
            "provider_id": str(world.provider_id),
            "start": start.isoformat(),
            "end": end.isoformat(),
            **params,
        },
        headers=auth(tokens),
    )


async def test_tracer_generated_slots_are_found_in_utc_through_the_gateway(
    client: AsyncClient, world: World
) -> None:
    # 3 local days x 6 half-hour slots (09:00-12:00 Asia/Karachi).
    assert world.generated == {"created": 18, "considered": 18}
    day = world.first_day
    start, end = world.local_day_bounds(day)

    resp = await _search(client, world, world.patient, start, end)

    assert resp.status_code == 200, resp.text
    slots = resp.json()
    assert len(slots) == 6
    first = slots[0]
    assert first["status"] == "available"
    assert first["provider_id"] == str(world.provider_id)
    assert first["clinic_id"] == str(world.clinic_id)
    assert first["department_id"] == str(world.department_id)
    first_start = datetime.fromisoformat(first["start_time"])
    assert first_start.utcoffset() is not None and first_start.utcoffset().total_seconds() == 0
    # 09:00 Asia/Karachi (UTC+5) is 04:00Z on the same date.
    assert first_start == datetime.combine(day, time(4, 0), tzinfo=timezone.utc)
    assert first_start.astimezone(world.tz).time() == time(9, 0)
    assert [s["start_time"] for s in slots] == sorted(s["start_time"] for s in slots)
