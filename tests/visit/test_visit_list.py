"""Clinic-scoped visit list (plan 02-10, VISIT-05, D-17, D-18).

`GET /api/visits` is the ops view, polled through the gateway (no cache, no
push): front-desk sees its own clinic only, admin names the clinic; filters on
status, provider and check-in time; ids/statuses/timestamps only.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    add_provider,
    check_in,
    checked_in_visit,
    confirmed_appointment,
    get_visit,
)

pytestmark = pytest.mark.integration

VISIT_OUT_KEYS = {
    "id",
    "appointment_id",
    "clinic_id",
    "patient_id",
    "provider_id",
    "status",
    "checked_in_at",
    "started_at",
    "completed_at",
}


async def _list(client: AsyncClient, tokens: dict, **params: Any) -> httpx.Response:
    query = {k: str(v) for k, v in params.items() if v is not None}
    return await client.get("/api/visits", params=query, headers=auth(tokens))


def _ids(resp: httpx.Response) -> list[str]:
    assert resp.status_code == 200, resp.text
    return [v["id"] for v in resp.json()]


async def test_front_desk_lists_own_clinic_newest_first(
    client: AsyncClient, visit_world: VisitWorld, other_world: VisitWorld
) -> None:
    created = [await checked_in_visit(client, visit_world) for _ in range(3)]
    foreign = await checked_in_visit(client, other_world)

    resp = await _list(client, visit_world.world.front_desk)

    assert _ids(resp) == [v["id"] for v in reversed(created)]
    assert foreign["id"] not in _ids(resp)
    for item in resp.json():
        assert set(item) == VISIT_OUT_KEYS
        assert item["clinic_id"] == str(visit_world.clinic_id)


async def test_status_and_provider_filters(client: AsyncClient, visit_world: VisitWorld) -> None:
    front_desk = visit_world.world.front_desk
    waiting = await checked_in_visit(client, visit_world)
    seen = await checked_in_visit(client, visit_world)
    started = await client.post(f"/api/visits/{seen['id']}/start", headers=auth(front_desk))
    assert started.status_code == 200
    second_provider_id, _tokens = await add_provider(client, visit_world)
    other_appointment = await confirmed_appointment(
        visit_world, provider_id=uuid.UUID(second_provider_id)
    )
    other = await check_in(client, front_desk, other_appointment)
    assert other.status_code == 201, other.text

    assert _ids(await _list(client, front_desk, status="in_progress")) == [seen["id"]]
    assert set(_ids(await _list(client, front_desk, status="checked_in"))) == {
        waiting["id"],
        other.json()["id"],
    }
    assert _ids(await _list(client, front_desk, provider_id=second_provider_id)) == [
        other.json()["id"]
    ]
    assert set(_ids(await _list(client, front_desk, provider_id=visit_world.world.provider_id))) == {
        waiting["id"],
        seen["id"],
    }
    assert (await _list(client, front_desk, status="bogus")).status_code == 422


async def test_date_filters_bound_checked_in_at(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    front_desk = visit_world.world.front_desk
    visit = await checked_in_visit(client, visit_world)
    now = datetime.now(timezone.utc)
    hour = timedelta(hours=1)

    assert _ids(
        await _list(client, front_desk, date_from=(now - hour).isoformat(), date_to=(now + hour).isoformat())
    ) == [visit["id"]]
    assert _ids(await _list(client, front_desk, date_from=(now + hour).isoformat())) == []
    assert _ids(await _list(client, front_desk, date_to=(now - hour).isoformat())) == []
    naive = (now - hour).replace(tzinfo=None).isoformat()
    assert (await _list(client, front_desk, date_from=naive)).status_code == 422


async def test_limit_is_capped_and_offset_pages(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    front_desk = visit_world.world.front_desk
    created = [await checked_in_visit(client, visit_world) for _ in range(3)]
    newest_first = [v["id"] for v in reversed(created)]

    assert (await _list(client, front_desk, limit=201)).status_code == 422
    assert (await _list(client, front_desk, limit=0)).status_code == 422
    assert (await _list(client, front_desk, limit=200)).status_code == 200
    assert _ids(await _list(client, front_desk, limit=2)) == newest_first[:2]
    assert _ids(await _list(client, front_desk, limit=2, offset=2)) == newest_first[2:]


async def test_clinic_scope_for_front_desk_and_admin(
    client: AsyncClient, visit_world: VisitWorld, other_world: VisitWorld
) -> None:
    own = await checked_in_visit(client, visit_world)
    foreign = await checked_in_visit(client, other_world)
    world = visit_world.world

    assert (await _list(client, world.front_desk, clinic_id=other_world.clinic_id)).status_code == 403
    assert _ids(await _list(client, world.front_desk, clinic_id=world.clinic_id)) == [own["id"]]

    assert _ids(await _list(client, world.admin, clinic_id=other_world.clinic_id)) == [foreign["id"]]
    assert _ids(await _list(client, world.admin, clinic_id=world.clinic_id)) == [own["id"]]
    assert (await _list(client, world.admin)).status_code == 422


async def test_provider_and_patient_cannot_list(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    await checked_in_visit(client, visit_world)

    assert (await _list(client, visit_world.world.provider)).status_code == 403
    assert (await _list(client, visit_world.world.patient)).status_code == 403


async def test_provider_reads_own_visit_but_not_anothers(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    second_provider_id, second_tokens = await add_provider(client, visit_world)
    mine = await checked_in_visit(client, visit_world)

    own = await get_visit(client, visit_world.world.provider, mine["id"])
    assert own.status_code == 200, own.text
    assert own.json()["id"] == mine["id"]

    foreign = await get_visit(client, second_tokens, mine["id"])
    assert foreign.status_code == 404
    assert foreign.json() == {"detail": "not found"}
    assert second_provider_id != mine["provider_id"]


async def test_polling_reflects_a_transition_immediately(
    client: AsyncClient, visit_world: VisitWorld
) -> None:
    front_desk = visit_world.world.front_desk
    visit = await checked_in_visit(client, visit_world)
    before = await _list(client, front_desk)
    assert [v["status"] for v in before.json()] == ["checked_in"]

    started = await client.post(f"/api/visits/{visit['id']}/start", headers=auth(front_desk))
    assert started.status_code == 200

    after = await _list(client, front_desk)
    assert [v["status"] for v in after.json()] == ["in_progress"]
    assert after.json()[0]["started_at"] is not None
    assert (await get_visit(client, front_desk, visit["id"])).json()["status"] == "in_progress"
