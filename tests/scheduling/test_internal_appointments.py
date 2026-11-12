"""Scheduling's internal appointment lookup for Visit (plan 02-10, T-02-53).

`GET /internal/appointments/{id}?clinic_id=` is called DIRECTLY over the
in-process mesh at Scheduling's internal base URL; the gateway has no route to
`/internal` at all. Contract: Visit service tokens with `aud=scheduling` (and
admin/front-desk user tokens) only; `clinic_id` required; missing and
out-of-clinic are the identical 404; ids/times/status only.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from tests.conftest import service_base_url
from tests.scheduling.seed import auth
from tests.visit.conftest import VisitWorld, _visit_world, confirmed_appointment

pytestmark = pytest.mark.integration

NOT_FOUND = {"detail": "not found"}
REF_KEYS = {
    "id",
    "clinic_id",
    "patient_id",
    "provider_id",
    "slot_id",
    "slot_start",
    "slot_end",
    "status",
}


@pytest_asyncio.fixture
async def lookup_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    async for vw in _visit_world(client, mesh):
        yield vw


def _url(appointment_id: str, clinic_id: uuid.UUID | str | None) -> str:
    base = f"{service_base_url('scheduling')}/internal/appointments/{appointment_id}"
    return base if clinic_id is None else f"{base}?clinic_id={clinic_id}"


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _visit_token(mesh: httpx.AsyncClient, audience: str = "scheduling") -> str:
    secret = load_client_secret(os.environ["VISIT_CLIENT_SECRET_PATH"])
    tokens = ServiceTokenProvider("visit", secret, service_base_url("identity"), http=mesh)
    return await tokens.get(audience)


async def test_visit_service_token_reads_appointment_ref(
    mesh: httpx.AsyncClient, lookup_world: VisitWorld
) -> None:
    world = lookup_world.world
    appointment_id = await confirmed_appointment(lookup_world)

    resp = await mesh.get(_url(appointment_id, world.clinic_id), headers=_bearer(await _visit_token(mesh)))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == REF_KEYS
    assert body["id"] == appointment_id
    assert body["status"] == "confirmed"
    assert body["clinic_id"] == str(world.clinic_id)
    assert body["patient_id"] == str(world.patient_id)
    assert body["provider_id"] == str(world.provider_id)


async def test_wrong_clinic_is_identical_404(
    mesh: httpx.AsyncClient, lookup_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(lookup_world)
    token = await _visit_token(mesh)

    other_clinic = await mesh.get(_url(appointment_id, uuid.uuid4()), headers=_bearer(token))
    missing = await mesh.get(_url(str(uuid.uuid4()), lookup_world.clinic_id), headers=_bearer(token))

    assert other_clinic.status_code == missing.status_code == 404
    assert other_clinic.json() == missing.json() == NOT_FOUND


async def test_clinic_id_is_required(mesh: httpx.AsyncClient, lookup_world: VisitWorld) -> None:
    appointment_id = await confirmed_appointment(lookup_world)

    resp = await mesh.get(_url(appointment_id, None), headers=_bearer(await _visit_token(mesh)))

    assert resp.status_code == 422


async def test_billing_service_token_is_forbidden(
    mesh: httpx.AsyncClient, lookup_world: VisitWorld
) -> None:
    """Identity refuses to mint billing->scheduling, so the token is hand-signed:
    correctly audienced, but billing is not in the route's allowed services."""
    from services.identity.security.signing import create_service_token  # noqa: PLC0415

    appointment_id = await confirmed_appointment(lookup_world)
    token = create_service_token("billing", audience="scheduling")

    resp = await mesh.get(_url(appointment_id, lookup_world.clinic_id), headers=_bearer(token))

    assert resp.status_code == 403, resp.text


async def test_token_for_another_audience_is_401(
    mesh: httpx.AsyncClient, lookup_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(lookup_world)
    token = await _visit_token(mesh, audience="provider")

    resp = await mesh.get(_url(appointment_id, lookup_world.clinic_id), headers=_bearer(token))

    assert resp.status_code == 401, resp.text


async def test_no_token_is_401(mesh: httpx.AsyncClient, lookup_world: VisitWorld) -> None:
    appointment_id = await confirmed_appointment(lookup_world)

    resp = await mesh.get(_url(appointment_id, lookup_world.clinic_id))

    assert resp.status_code == 401


async def test_front_desk_user_is_scoped_to_own_clinic(
    mesh: httpx.AsyncClient, lookup_world: VisitWorld
) -> None:
    world = lookup_world.world
    appointment_id = await confirmed_appointment(lookup_world)

    own = await mesh.get(_url(appointment_id, world.clinic_id), headers=auth(world.front_desk))
    assert own.status_code == 200, own.text

    # A provider (role not allowed on this route) is refused outright.
    provider = await mesh.get(_url(appointment_id, world.clinic_id), headers=auth(world.provider))
    assert provider.status_code == 403


async def test_gateway_does_not_route_internal_appointments(
    client: AsyncClient, lookup_world: VisitWorld
) -> None:
    appointment_id = await confirmed_appointment(lookup_world)

    resp = await client.get(
        f"/internal/appointments/{appointment_id}?clinic_id={lookup_world.clinic_id}",
        headers=auth(lookup_world.world.admin),
    )

    assert resp.status_code == 404


async def test_route_is_absent_from_scheduling_openapi(mesh: httpx.AsyncClient) -> None:
    resp = await mesh.get(f"{service_base_url('scheduling')}/openapi.json")

    assert resp.status_code == 200
    assert not [path for path in resp.json()["paths"] if path.startswith("/internal")]
