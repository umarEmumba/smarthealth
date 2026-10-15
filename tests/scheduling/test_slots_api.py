"""Slot supply through the gateway (plan 02-04, D-01, D-18, research Pattern 6).

`/api/slots` is Scheduling's first public surface: an admin or front-desk user
materializes slots from Provider's templates in the clinic's timezone (resolved
over service tokens from Provider and Organization), and any authenticated user
searches them. Everything here goes through the gateway over the in-process
mesh; slot rows are committed and removed per test by provider id.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import date, datetime, time, timedelta, timezone

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import degraded_mesh, unique_email
from tests.scheduling.conftest import cleanup_scheduling_rows
from tests.scheduling.seed import PASSWORD, World, auth, seed_bookable_world

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


# --- plan 02-04 Task 2: idempotency, non-resurrection, scope, bounds ---------


async def _count(engine: AsyncEngine, world: World) -> int:
    async with engine.connect() as conn:
        return await conn.scalar(
            text("SELECT count(*) FROM provider_slots WHERE provider_id = :p"), {"p": world.provider_id}
        )


async def _generate(client: AsyncClient, tokens: dict, provider_id, first: date, last: date):
    return await client.post(
        "/api/slots/generate",
        json={"provider_id": str(provider_id), "from_date": first.isoformat(), "to_date": last.isoformat()},
        headers=auth(tokens),
    )


async def _other_clinic_front_desk(client: AsyncClient, admin: dict) -> dict:
    clinic = await client.post(
        "/api/clinics", json={"name": "Other Slot Clinic", "timezone": "UTC"}, headers=auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    email = unique_email("front-desk-b")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk", "clinic_id": clinic.json()["id"]},
        headers=auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def test_second_identical_generation_creates_nothing(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    before = await _count(committed_scheduling_engine, world)

    again = await _generate(client, world.admin, world.provider_id, world.first_day, world.last_day)

    assert again.status_code == 200, again.text
    assert again.json() == {"created": 0, "considered": 18}
    assert await _count(committed_scheduling_engine, world) == before == 18


async def test_regeneration_never_touches_held_booked_or_withdrawn_slots(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    start, end = world.local_day_bounds(world.first_day)
    slots = (await _search(client, world, world.admin, start, end)).json()
    held, booked, withdrawn = (uuid.UUID(s["id"]) for s in slots[:3])
    async with committed_scheduling_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = 'held', held_by_appointment_id = :a, "
                "held_until = now() + interval '10 minutes' WHERE id = :s"
            ),
            {"s": held, "a": uuid.uuid4()},
        )
        await conn.execute(
            text("UPDATE provider_slots SET status = 'booked', held_by_appointment_id = :a WHERE id = :s"),
            {"s": booked, "a": uuid.uuid4()},
        )
        await conn.execute(
            text("UPDATE provider_slots SET status = 'cancelled' WHERE id = :s"), {"s": withdrawn}
        )

    again = await _generate(client, world.front_desk, world.provider_id, world.first_day, world.last_day)

    assert again.status_code == 200, again.text
    assert again.json()["created"] == 0
    everything = (await _search(client, world, world.admin, start, end, status="any")).json()
    by_id = {uuid.UUID(s["id"]): s["status"] for s in everything}
    assert by_id[held] == "held"
    assert by_id[booked] == "booked"
    assert by_id[withdrawn] == "cancelled"
    assert len(everything) == 6  # no available twin was inserted beside the withdrawn slot


async def test_front_desk_of_another_clinic_gets_the_same_404_as_an_unknown_provider(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    outsider = await _other_clinic_front_desk(client, world.admin)
    before = await _count(committed_scheduling_engine, world)
    later = world.last_day + timedelta(days=5)

    out_of_clinic = await _generate(client, outsider, world.provider_id, later, later)
    unknown = await _generate(client, world.admin, uuid.uuid4(), later, later)

    assert out_of_clinic.status_code == 404
    assert unknown.status_code == 404
    assert out_of_clinic.json() == unknown.json() == {"detail": "not found"}
    assert await _count(committed_scheduling_engine, world) == before


async def test_patient_cannot_generate_slots(client: AsyncClient, world: World) -> None:
    resp = await _generate(client, world.patient, world.provider_id, world.first_day, world.first_day)

    assert resp.status_code == 403


async def test_generation_range_wider_than_the_cap_is_422(client: AsyncClient, world: World) -> None:
    resp = await _generate(
        client, world.admin, world.provider_id, world.first_day, world.first_day + timedelta(days=32)
    )
    backwards = await _generate(
        client, world.admin, world.provider_id, world.first_day, world.first_day - timedelta(days=1)
    )

    assert resp.status_code == 422
    assert backwards.status_code == 422


async def test_search_range_wider_than_the_cap_is_422(client: AsyncClient, world: World) -> None:
    start, _end = world.local_day_bounds(world.first_day)

    resp = await _search(client, world, world.patient, start, start + timedelta(days=32))

    assert resp.status_code == 422


async def test_provider_unreachable_is_503_and_writes_nothing(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    before = await _count(committed_scheduling_engine, world)
    later = world.last_day + timedelta(days=3)

    async with degraded_mesh("provider"):
        resp = await _generate(client, world.admin, world.provider_id, later, later + timedelta(days=2))

    assert resp.status_code == 503
    assert resp.json() == {"detail": "provider service unavailable"}
    assert await _count(committed_scheduling_engine, world) == before


async def test_inactive_provider_is_409(client: AsyncClient, world: World) -> None:
    deactivated = await client.post(
        f"/api/providers/{world.provider_id}/deactivate", headers=auth(world.admin)
    )
    assert deactivated.status_code == 200, deactivated.text
    later = world.last_day + timedelta(days=1)

    resp = await _generate(client, world.admin, world.provider_id, later, later)

    assert resp.status_code == 409
    assert resp.json() == {"detail": "provider inactive"}


# --- plan 02-04 Task 3: on-demand extension and withdrawal -------------------


async def _audit_actions(entity_id: str, action: str) -> list[dict]:
    mongo: AsyncMongoClient = AsyncMongoClient(shared_settings.mongo_url)
    try:
        audit = mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
        return await audit.find({"entity_id": entity_id, "action": action}).to_list(None)
    finally:
        await mongo.close()


async def test_search_beyond_the_generated_range_generates_missing_days_first(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    first = world.first_day + timedelta(days=5)
    start, _ = world.local_day_bounds(first)
    _, end = world.local_day_bounds(first + timedelta(days=2))

    resp = await _search(client, world, world.patient, start, end)

    assert resp.status_code == 200, resp.text
    slots = resp.json()
    assert len(slots) == 18  # three newly generated local days x six slots
    assert {datetime.fromisoformat(s["start_time"]).astimezone(world.tz).date() for s in slots} == {
        first,
        first + timedelta(days=1),
        first + timedelta(days=2),
    }
    assert all(s["status"] == "available" for s in slots)
    # The days in between were filled too (no hole between ranges).
    assert await _count(committed_scheduling_engine, world) == 8 * 6
    audits = await _audit_actions(str(world.provider_id), "slots.generate")
    assert {a["actor_role"] for a in audits} >= {"admin", "patient"}
    assert all(a["changes"] == {"fields": ["provider_slots"]} for a in audits)


async def test_search_entirely_beyond_the_horizon_generates_nothing(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine
) -> None:
    start = datetime.now(timezone.utc) + timedelta(days=29)

    resp = await _search(client, world, world.patient, start, start + timedelta(days=3))

    assert resp.status_code == 200, resp.text
    assert resp.json() == []
    assert await _count(committed_scheduling_engine, world) == 18


async def test_search_still_answers_when_extension_fails(
    client: AsyncClient,
    world: World,
    committed_scheduling_engine: AsyncEngine,
    capsys: pytest.CaptureFixture[str],
) -> None:
    start, _ = world.local_day_bounds(world.first_day)
    _, end = world.local_day_bounds(world.first_day + timedelta(days=9))
    capsys.readouterr()

    async with degraded_mesh("provider"):
        resp = await _search(client, world, world.patient, start, end)

    assert resp.status_code == 200, resp.text
    assert len(resp.json()) == 18  # the existing slots, nothing new
    assert await _count(committed_scheduling_engine, world) == 18
    assert "slots.extend_failed" in capsys.readouterr().out


async def test_withdraw_available_slot_then_repeat_is_a_noop(
    client: AsyncClient, world: World
) -> None:
    start, end = world.local_day_bounds(world.first_day)
    slot_id = (await _search(client, world, world.front_desk, start, end)).json()[0]["id"]

    first = await client.post(f"/api/slots/{slot_id}/cancel", headers=auth(world.front_desk))
    repeat = await client.post(f"/api/slots/{slot_id}/cancel", headers=auth(world.admin))

    assert first.status_code == 200, first.text
    assert first.json()["status"] == "cancelled"
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["status"] == "cancelled"
    remaining = (await _search(client, world, world.patient, start, end)).json()
    assert slot_id not in {s["id"] for s in remaining}
    audits = await _audit_actions(slot_id, "slot.withdraw")
    assert len(audits) == 1  # the repeat changed nothing and wrote nothing
    assert audits[0]["outcome"] == "success"
    assert audits[0]["clinic_id"] == str(world.clinic_id)


@pytest.mark.parametrize("held_status", ["held", "booked"])
async def test_withdrawing_a_held_or_booked_slot_is_409(
    client: AsyncClient, world: World, committed_scheduling_engine: AsyncEngine, held_status: str
) -> None:
    start, end = world.local_day_bounds(world.first_day)
    slot_id = (await _search(client, world, world.admin, start, end)).json()[0]["id"]
    async with committed_scheduling_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE provider_slots SET status = CAST(:st AS varchar), held_by_appointment_id = :a, "
                "held_until = CASE WHEN CAST(:st AS varchar) = 'held' "
                "THEN now() + interval '10 minutes' END "
                "WHERE id = :s"
            ),
            {"s": uuid.UUID(slot_id), "a": uuid.uuid4(), "st": held_status},
        )

    resp = await client.post(f"/api/slots/{slot_id}/cancel", headers=auth(world.admin))

    assert resp.status_code == 409
    async with committed_scheduling_engine.connect() as conn:
        status_now = await conn.scalar(
            text("SELECT status FROM provider_slots WHERE id = :s"), {"s": uuid.UUID(slot_id)}
        )
    assert status_now == held_status


async def test_withdraw_out_of_clinic_or_unknown_slot_is_404_and_patient_is_403(
    client: AsyncClient, world: World
) -> None:
    start, end = world.local_day_bounds(world.first_day)
    slot_id = (await _search(client, world, world.admin, start, end)).json()[0]["id"]
    outsider = await _other_clinic_front_desk(client, world.admin)

    out_of_clinic = await client.post(f"/api/slots/{slot_id}/cancel", headers=auth(outsider))
    unknown = await client.post(f"/api/slots/{uuid.uuid4()}/cancel", headers=auth(world.admin))
    patient = await client.post(f"/api/slots/{slot_id}/cancel", headers=auth(world.patient))

    assert out_of_clinic.status_code == 404
    assert unknown.status_code == 404
    assert out_of_clinic.json() == unknown.json() == {"detail": "not found"}
    assert patient.status_code == 403
    still = (await _search(client, world, world.admin, start, end)).json()
    assert slot_id in {s["id"] for s in still}
