"""Provider's schedule-change feed (plan 02.1-05, mentor item M2).

Every committed change that can make already-generated slots wrong writes ONE event
in the same transaction (template edit that changes a schedule-affecting field,
template deactivation, provider deactivation); Scheduling drains the events through
`GET /internal/schedule-changes` and acknowledges them idempotently. The feed is read
here over the in-process mesh at Provider's internal base URL (the gateway has no
`/internal` route). Other tests leave events in the shared database, so every
assertion is scoped to the provider id the test created.
"""

from __future__ import annotations

import os
import uuid
from datetime import time
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import func, select

from tests.conftest import bootstrap_admin_and_login, service_base_url, unique_email

PASSWORD = "correct-horse-battery"

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(autouse=True)
async def _drain_feed(mesh: httpx.AsyncClient) -> None:
    """Acknowledge every pending event before each test.

    The feed is global, oldest first, capped at 200 per read, and most other
    tests (templates, providers) leave pending events behind in the shared
    database. Without draining, a fresh provider's events would eventually sit
    beyond the first 200 and no test could see them. Draining is safe here: this
    is the test database and no consumer runs.
    """
    headers = await _service_headers(mesh, "scheduling")
    for _ in range(1000):
        resp = await mesh.get(_feed(), params={"limit": 200}, headers=headers)
        assert resp.status_code == 200, resp.text
        pending = resp.json()
        if not pending:
            return
        for event in pending:
            ack = await mesh.post(_feed(f"/internal/schedule-changes/{event['id']}/ack"), headers=headers)
            assert ack.status_code == 200, ack.text
    raise AssertionError("feed did not drain")


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _feed(path: str = "/internal/schedule-changes") -> str:
    return f"{service_base_url('provider')}{path}"


async def _service_headers(mesh: httpx.AsyncClient, service: str, audience: str = "provider") -> dict:
    """A REAL service token minted through Identity's client-credentials endpoint."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415

    secret_path = os.environ[f"{service.upper()}_CLIENT_SECRET_PATH"]
    provider = ServiceTokenProvider(
        service,
        load_client_secret(secret_path),
        service_base_url("identity"),
        http=mesh,
    )
    return {"Authorization": f"Bearer {await provider.get(audience)}"}


async def _front_desk(client: AsyncClient, admin: dict) -> dict:
    email = unique_email("front-desk")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "front_desk"},
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


async def _provider(client: AsyncClient, admin: dict) -> str:
    """Provider id of a fresh provider created through the gateway."""
    clinic = await client.post(
        "/api/clinics", json={"name": "Schedule Feed Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments",
        json={"name": "Feed"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    created = await client.post(
        "/api/providers",
        json={
            "department_id": department.json()["id"],
            "full_name": "Dr. Schedule Feed",
            "specialties": ["General"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


def _template_payload(**overrides) -> dict:
    payload = {
        "day_of_week": 0,
        "start_time": "09:00:00",
        "end_time": "17:00:00",
        "slot_duration_minutes": 30,
        "effective_from": "2026-01-05",
        "effective_to": "2026-12-31",
    }
    payload.update(overrides)
    return payload


async def _template(client: AsyncClient, admin: dict, provider_id: str, **overrides) -> str:
    created = await client.post(
        f"/api/providers/{provider_id}/availability",
        json=_template_payload(**overrides),
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


async def _patch(client: AsyncClient, admin: dict, provider_id: str, template_id: str, body: dict):
    resp = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}", json=body, headers=_auth(admin)
    )
    assert resp.status_code == 200, resp.text
    return resp


async def _deactivate_template(client: AsyncClient, admin: dict, provider_id: str, template_id: str):
    resp = await client.post(
        f"/api/providers/{provider_id}/availability/{template_id}/deactivate", headers=_auth(admin)
    )
    assert resp.status_code == 200, resp.text
    return resp


async def _events(mesh: httpx.AsyncClient, headers: dict, provider_id: str) -> list[dict]:
    """The pending events of `provider_id` (the feed is global; scope by provider)."""
    resp = await mesh.get(_feed(), params={"limit": 200}, headers=headers)
    assert resp.status_code == 200, resp.text
    return [e for e in resp.json() if e["provider_id"] == provider_id]


async def test_tracer_deactivation_event_is_readable_and_ackable(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id = await _provider(client, admin)
    template_id = await _template(client, admin, provider_id)
    await _deactivate_template(client, admin, provider_id, template_id)
    scheduling = await _service_headers(mesh, "scheduling")

    events = await _events(mesh, scheduling, provider_id)

    assert len(events) == 1, events
    event = events[0]
    assert event["change_type"] == "template_deactivated"
    assert event["template_id"] == template_id
    assert set(event) == {"id", "provider_id", "change_type", "template_id", "created_at"}

    ack = await mesh.post(_feed(f"/internal/schedule-changes/{event['id']}/ack"), headers=scheduling)
    assert ack.status_code == 200, ack.text
    assert ack.json() == {"id": event["id"], "delivered": True}
    assert await _events(mesh, scheduling, provider_id) == []

    # Only a Scheduling service token or an ADMIN user may use the feed.
    front_desk = _auth(await _front_desk(client, admin))
    visit = await _service_headers(mesh, "visit")
    ack_path = f"/internal/schedule-changes/{uuid.uuid4()}/ack"
    for headers in (front_desk, visit):
        assert (await mesh.get(_feed(), headers=headers)).status_code == 403
        assert (await mesh.post(_feed(ack_path), headers=headers)).status_code == 403
    assert (await mesh.get(_feed())).status_code == 401
    assert (await mesh.post(_feed(ack_path))).status_code == 401


# --- task 2: hooks, no-op suppression, atomicity and feed behaviour ---

TEMPLATE_FIELD_CHANGES = [
    ("day_of_week", 2),
    ("start_time", "08:00:00"),
    ("end_time", "18:00:00"),
    ("slot_duration_minutes", 20),
    ("effective_from", "2026-01-12"),
    ("effective_to", "2026-11-30"),
]


async def _setup(client: AsyncClient, mesh: httpx.AsyncClient):
    admin = await bootstrap_admin_and_login(client)
    provider_id = await _provider(client, admin)
    return admin, provider_id, await _service_headers(mesh, "scheduling")


@pytest.mark.parametrize(("field", "value"), TEMPLATE_FIELD_CHANGES, ids=[f for f, _ in TEMPLATE_FIELD_CHANGES])
async def test_each_schedule_affecting_field_change_writes_one_template_updated_event(
    client: AsyncClient, mesh: httpx.AsyncClient, field: str, value: object
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)
    assert await _events(mesh, scheduling, provider_id) == []  # creating writes nothing

    await _patch(client, admin, provider_id, template_id, {field: value})

    events = await _events(mesh, scheduling, provider_id)
    assert [(e["change_type"], e["template_id"]) for e in events] == [("template_updated", template_id)]


async def test_patch_with_identical_values_writes_no_event(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)

    # Every field supplied, all equal to the stored values.
    await _patch(client, admin, provider_id, template_id, _template_payload())
    # An empty PATCH is rejected by validation and writes nothing either.
    empty = await client.patch(
        f"/api/providers/{provider_id}/availability/{template_id}", json={}, headers=_auth(admin)
    )
    assert empty.status_code == 422

    assert await _events(mesh, scheduling, provider_id) == []


async def test_deactivating_a_provider_writes_one_event_and_repeating_writes_none(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)

    for _ in range(2):
        resp = await client.post(f"/api/providers/{provider_id}/deactivate", headers=_auth(admin))
        assert resp.status_code == 200, resp.text

    events = await _events(mesh, scheduling, provider_id)
    assert [(e["change_type"], e["template_id"]) for e in events] == [("provider_deactivated", None)]


async def test_deactivating_a_template_twice_writes_one_event(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)

    await _deactivate_template(client, admin, provider_id, template_id)
    await _deactivate_template(client, admin, provider_id, template_id)

    events = await _events(mesh, scheduling, provider_id)
    assert [(e["change_type"], e["template_id"]) for e in events] == [("template_deactivated", template_id)]


async def test_three_separate_edits_produce_three_events_in_order(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)

    await _patch(client, admin, provider_id, template_id, {"start_time": "08:00:00"})
    await _patch(client, admin, provider_id, template_id, {"slot_duration_minutes": 15})
    await _deactivate_template(client, admin, provider_id, template_id)

    events = await _events(mesh, scheduling, provider_id)
    assert [e["change_type"] for e in events] == ["template_updated", "template_updated", "template_deactivated"]
    assert len({e["id"] for e in events}) == 3


async def _fresh_session():
    from libs.db.postgres import get_session_factory  # noqa: PLC0415
    from services.provider.config import settings  # noqa: PLC0415

    return get_session_factory(settings.database_url)()


async def _event_count(provider_id: str) -> int:
    from services.provider.repository.models import ScheduleChangeEvent  # noqa: PLC0415

    async with await _fresh_session() as session:
        result = await session.execute(
            select(func.count())
            .select_from(ScheduleChangeEvent)
            .where(ScheduleChangeEvent.provider_id == uuid.UUID(provider_id))
        )
        return int(result.scalar_one())


@pytest.mark.parametrize("operation", ["update_template", "deactivate_template", "deactivate_provider"])
async def test_a_failed_commit_leaves_neither_the_change_nor_an_event(
    client: AsyncClient, mesh: httpx.AsyncClient, operation: str
) -> None:
    """The event is added to the SAME session/transaction as the change, so when the
    commit fails nothing is persisted (no change without its event, no event alone)."""
    from services.provider.repository import provider_repo  # noqa: PLC0415

    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)
    provider_uuid, template_uuid = uuid.UUID(provider_id), uuid.UUID(template_id)

    async with await _fresh_session() as session:
        session.commit = AsyncMock(side_effect=RuntimeError("commit failed"))  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="commit failed"):
            if operation == "update_template":
                await provider_repo.update_availability_template(
                    session, template_uuid, provider_uuid, start_time=time(7, 0)
                )
            elif operation == "deactivate_template":
                await provider_repo.deactivate_availability_template(session, template_uuid, provider_uuid)
            else:
                await provider_repo.deactivate_provider(session, provider_uuid)
        await session.rollback()
        session.commit.assert_awaited_once()

    # A fresh session sees the original rows and no event.
    assert await _event_count(provider_id) == 0
    assert await _events(mesh, scheduling, provider_id) == []
    template = (await client.get(f"/api/providers/{provider_id}/availability", headers=_auth(admin))).json()
    assert [(t["id"], t["start_time"], t["is_active"]) for t in template] == [(template_id, "09:00:00", True)]
    provider = await client.get(f"/api/providers/{provider_id}", headers=_auth(admin))
    assert provider.json()["is_active"] is True


async def test_event_and_change_commit_together_on_success(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)

    await _patch(client, admin, provider_id, template_id, {"day_of_week": 3})

    assert await _event_count(provider_id) == 1
    listed = (await client.get(f"/api/providers/{provider_id}/availability", headers=_auth(admin))).json()
    assert listed[0]["day_of_week"] == 3


async def test_feed_is_oldest_first_and_excludes_acknowledged_events(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    first = await _template(client, admin, provider_id)
    second = await _template(client, admin, provider_id, day_of_week=1)
    await _deactivate_template(client, admin, provider_id, first)
    await _deactivate_template(client, admin, provider_id, second)

    listed = (await mesh.get(_feed(), params={"limit": 200}, headers=scheduling)).json()
    assert [e["template_id"] for e in listed] == [first, second]
    assert [(e["created_at"], e["id"]) for e in listed] == sorted((e["created_at"], e["id"]) for e in listed)

    ack = await mesh.post(_feed(f"/internal/schedule-changes/{listed[0]['id']}/ack"), headers=scheduling)
    assert ack.status_code == 200, ack.text

    remaining = (await mesh.get(_feed(), params={"limit": 200}, headers=scheduling)).json()
    assert [e["template_id"] for e in remaining] == [second]


async def test_limit_is_honoured_and_out_of_range_is_422(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)
    await _patch(client, admin, provider_id, template_id, {"start_time": "08:00:00"})
    await _patch(client, admin, provider_id, template_id, {"start_time": "07:00:00"})
    await _deactivate_template(client, admin, provider_id, template_id)

    everything = (await mesh.get(_feed(), params={"limit": 200}, headers=scheduling)).json()
    assert len(everything) == 3
    one = (await mesh.get(_feed(), params={"limit": 1}, headers=scheduling)).json()
    two = (await mesh.get(_feed(), params={"limit": 2}, headers=scheduling)).json()
    assert one == everything[:1]
    assert two == everything[:2]
    # The default (100) returns them all too.
    assert (await mesh.get(_feed(), headers=scheduling)).json() == everything

    for bad in (0, 201, -1, "abc"):
        resp = await mesh.get(_feed(), params={"limit": bad}, headers=scheduling)
        assert resp.status_code == 422, (bad, resp.text)
    assert (await mesh.get(_feed(), params={"limit": 200}, headers=scheduling)).status_code == 200


async def test_acknowledging_twice_is_200_and_unknown_id_is_the_identical_404(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin, provider_id, scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)
    await _deactivate_template(client, admin, provider_id, template_id)
    (event,) = await _events(mesh, scheduling, provider_id)

    for _ in range(2):
        resp = await mesh.post(_feed(f"/internal/schedule-changes/{event['id']}/ack"), headers=scheduling)
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"id": event["id"], "delivered": True}

    unknown = await mesh.post(_feed(f"/internal/schedule-changes/{uuid.uuid4()}/ack"), headers=scheduling)
    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "not found"}
    # An admin user token is accepted too, with the same answers.
    as_admin = await mesh.post(_feed(f"/internal/schedule-changes/{uuid.uuid4()}/ack"), headers=_auth(admin))
    assert as_admin.status_code == 404
    assert as_admin.json() == {"detail": "not found"}


async def test_admin_user_token_can_read_the_feed(client: AsyncClient, mesh: httpx.AsyncClient) -> None:
    admin, provider_id, _scheduling = await _setup(client, mesh)
    template_id = await _template(client, admin, provider_id)
    await _deactivate_template(client, admin, provider_id, template_id)

    events = await _events(mesh, _auth(admin), provider_id)

    assert [e["change_type"] for e in events] == ["template_deactivated"]


async def test_other_services_and_audiences_are_refused(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    admin, _provider_id, _scheduling = await _setup(client, mesh)
    front_desk = await _front_desk(client, admin)
    billing = {"Authorization": f"Bearer {create_service_token('billing', audience='provider')}"}
    wrong_audience = {"Authorization": f"Bearer {create_service_token('scheduling', audience='patient')}"}

    assert (await mesh.get(_feed(), headers=billing)).status_code == 403
    assert (await mesh.get(_feed(), headers=wrong_audience)).status_code == 401
    assert (await mesh.get(_feed(), headers=_auth(front_desk))).status_code == 403


async def test_feed_is_not_documented_and_not_reachable_through_the_gateway(
    client: AsyncClient,
) -> None:
    from services.provider.app import app  # noqa: PLC0415

    assert not [p for p in app.openapi()["paths"] if "schedule-changes" in p]
    admin = await bootstrap_admin_and_login(client)
    assert (await client.get("/internal/schedule-changes", headers=_auth(admin))).status_code == 404
