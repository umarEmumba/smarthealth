"""Visit transitions (plan 02-10, VISIT-02, VISIT-03, D-15, D-18).

checked_in -> in_progress -> completed, forward only, through the gateway:
  * the visit's provider, or front-desk/admin within the clinic, may advance it;
  * a repeat of the current or an already-passed target is a 200 no-op (no new
    history row, no new audit entry); skipping a state is 409;
  * another provider gets the identical 404 a missing visit gets;
  * concurrent identical requests change the state exactly once.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import degraded_mesh
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    add_provider,
    audit_count,
    checked_in_visit,
    get_visit,
    history_rows,
)

pytestmark = pytest.mark.integration

NOT_FOUND = {"detail": "not found"}


async def _move(client: AsyncClient, tokens: dict, visit_id: str, step: str) -> httpx.Response:
    return await client.post(f"/api/visits/{visit_id}/{step}", headers=auth(tokens))


async def _downstream_started_at(visit_id: str) -> object:
    from services.visit.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            return (
                await conn.execute(
                    text("SELECT downstream_started_at, status FROM visits WHERE id = :v"),
                    {"v": visit_id},
                )
            ).one()
    finally:
        await engine.dispose()


async def test_front_desk_starts_and_completes(client: AsyncClient, visit_world: VisitWorld) -> None:
    front_desk = visit_world.world.front_desk
    visit = await checked_in_visit(client, visit_world)

    started = await _move(client, front_desk, visit["id"], "start")
    assert started.status_code == 200, started.text
    assert started.json()["status"] == "in_progress"
    assert started.json()["started_at"] is not None
    assert started.json()["completed_at"] is None

    completed = await _move(client, front_desk, visit["id"], "complete")
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    assert completed.json()["completed_at"] is not None

    downstream, status = await _downstream_started_at(visit["id"])
    assert status == "completed" and downstream is None

    detail = await get_visit(client, front_desk, visit["id"])
    assert [(h["from_status"], h["to_status"]) for h in detail.json()["history"]] == [
        (None, "checked_in"),
        ("checked_in", "in_progress"),
        ("in_progress", "completed"),
    ]
    assert await audit_count(visit["id"], "visit.start") == 1
    assert await audit_count(visit["id"], "visit.complete") == 1


async def test_repeats_and_passed_targets_are_noops(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    front_desk = visit_world.world.front_desk
    visit = await checked_in_visit(client, visit_world)
    assert (await _move(client, front_desk, visit["id"], "start")).status_code == 200

    repeat_start = await _move(client, front_desk, visit["id"], "start")
    assert repeat_start.status_code == 200
    assert repeat_start.json()["status"] == "in_progress"
    assert len(await history_rows(visit["id"])) == 2

    assert (await _move(client, front_desk, visit["id"], "complete")).status_code == 200
    for step in ("start", "complete"):
        resp = await _move(client, front_desk, visit["id"], step)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "completed"

    assert len(await history_rows(visit["id"])) == 3
    assert await audit_count(visit["id"], "visit.start") == 1
    assert await audit_count(visit["id"], "visit.complete") == 1


async def test_skipping_in_progress_is_409(client: AsyncClient, visit_world: VisitWorld) -> None:
    visit = await checked_in_visit(client, visit_world)

    resp = await _move(client, visit_world.world.front_desk, visit["id"], "complete")

    assert resp.status_code == 409
    assert resp.json() == {"detail": "visit must be in_progress before completed"}
    assert await history_rows(visit["id"]) == [(None, "checked_in")]
    assert await audit_count(visit["id"], "visit.complete") == 0


async def test_visits_provider_can_advance_and_read(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    provider = visit_world.world.provider
    visit = await checked_in_visit(client, visit_world)

    started = await _move(client, provider, visit["id"], "start")
    assert started.status_code == 200, started.text
    completed = await _move(client, provider, visit["id"], "complete")
    assert completed.status_code == 200, completed.text
    detail = await get_visit(client, provider, visit["id"])
    assert detail.status_code == 200
    assert detail.json()["status"] == "completed"


async def test_another_provider_gets_identical_404(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    _other_id, other_provider = await add_provider(client, visit_world)
    visit = await checked_in_visit(client, visit_world)

    foreign = await _move(client, other_provider, visit["id"], "start")
    missing = await _move(client, other_provider, str(uuid.uuid4()), "start")
    foreign_read = await get_visit(client, other_provider, visit["id"])

    assert foreign.status_code == missing.status_code == foreign_read.status_code == 404
    assert foreign.json() == missing.json() == foreign_read.json() == NOT_FOUND
    assert await history_rows(visit["id"]) == [(None, "checked_in")]


async def test_patient_is_forbidden(client: AsyncClient, visit_world: VisitWorld) -> None:
    visit = await checked_in_visit(client, visit_world)

    for step in ("start", "complete"):
        resp = await _move(client, visit_world.world.patient, visit["id"], step)
        assert resp.status_code == 403
    assert (await get_visit(client, visit_world.world.patient, visit["id"])).status_code == 403


async def test_other_clinic_front_desk_gets_404(
    client: AsyncClient, visit_world: VisitWorld, other_world: VisitWorld
) -> None:
    visit = await checked_in_visit(client, visit_world)
    outsider = other_world.world.front_desk

    resp = await _move(client, outsider, visit["id"], "start")
    missing = await _move(client, outsider, str(uuid.uuid4()), "start")

    assert resp.status_code == missing.status_code == 404
    assert resp.json() == missing.json() == NOT_FOUND
    assert (await get_visit(client, outsider, visit["id"])).status_code == 404


async def test_admin_can_advance(client: AsyncClient, visit_world: VisitWorld) -> None:
    visit = await checked_in_visit(client, visit_world)

    resp = await _move(client, visit_world.world.admin, visit["id"], "start")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "in_progress"


async def test_provider_service_unreachable_is_503_and_unchanged(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    visit = await checked_in_visit(client, visit_world)

    async with degraded_mesh("provider"):
        resp = await _move(client, visit_world.world.provider, visit["id"], "start")

    assert resp.status_code == 503, resp.text
    assert resp.json() == {"detail": "provider service unavailable"}
    detail = await get_visit(client, visit_world.world.front_desk, visit["id"])
    assert detail.json()["status"] == "checked_in"


async def test_concurrent_starts_change_state_once(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    front_desk = visit_world.world.front_desk
    visit = await checked_in_visit(client, visit_world)

    responses = await asyncio.gather(
        *(_move(client, front_desk, visit["id"], "start") for _ in range(10))
    )

    assert [r.status_code for r in responses] == [200] * 10, [r.text for r in responses]
    assert {r.json()["status"] for r in responses} == {"in_progress"}
    assert await history_rows(visit["id"]) == [(None, "checked_in"), ("checked_in", "in_progress")]
    assert await audit_count(visit["id"], "visit.start") == 1


async def test_concurrent_completes_change_state_once(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    provider = visit_world.world.provider
    visit = await checked_in_visit(client, visit_world)
    assert (await _move(client, provider, visit["id"], "start")).status_code == 200

    responses = await asyncio.gather(
        *(_move(client, provider, visit["id"], "complete") for _ in range(10))
    )

    assert [r.status_code for r in responses] == [200] * 10, [r.text for r in responses]
    assert len(await history_rows(visit["id"])) == 3
    assert await audit_count(visit["id"], "visit.complete") == 1
