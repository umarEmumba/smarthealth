"""Shared helpers for the Visit tests (plan 02-10).

A `visit_world` is a `seed_bookable_world` (02-04: clinic, department, a
provider with a linked PROVIDER login, front-desk, patient) plus a committed
scheduling engine. Appointments in any status are inserted with SQL on fresh
slots far beyond the generated ones (`confirmed_appointment`), because the
booking saga is not what these tests exercise: Visit only reads the
appointment back through Scheduling's internal lookup.

Clean-up removes only rows the test created: Scheduling rows by the world's
provider id(s) (`cleanup_scheduling_rows`) and Visit rows by the same
provider ids (test-data hygiene; application code never deletes).
"""

from __future__ import annotations

import itertools
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any

import httpx
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from libs.config import shared_settings
from services.scheduling.domain.enums import AppointmentStatus
from tests.conftest import unique_email
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import PASSWORD, World, auth, seed_bookable_world


@dataclass
class VisitWorld:
    world: World
    engine: AsyncEngine
    # Every provider id whose rows the fixture must clean up.
    provider_ids: list[uuid.UUID] = field(default_factory=list)
    _slot_counter: itertools.count = field(default_factory=itertools.count)

    @property
    def clinic_id(self) -> uuid.UUID:
        return self.world.clinic_id

    def next_slot_start(self) -> datetime:
        """A 30-minute slot start well beyond the generated slots (which cover
        local today .. today+2), unique within this world."""
        base = datetime.combine(self.world.last_day + timedelta(days=7), time(0), tzinfo=timezone.utc)
        return base + timedelta(minutes=30 * next(self._slot_counter))


async def confirmed_appointment(
    vw: VisitWorld,
    *,
    status: str = "confirmed",
    provider_id: uuid.UUID | None = None,
    with_billing: bool = False,
    mesh: httpx.AsyncClient | None = None,
    start: datetime | None = None,
) -> str:
    """Insert (and COMMIT) one appointment in `status` on a fresh slot of the
    world's provider (or `provider_id`, which must be in the world's clinic);
    return its id. Appointments in any slot-holding status
    (`AppointmentStatus.ACTIVE`: pending, confirmed, checked_in, completed,
    no_show) get a `booked` slot, mirroring reality.

    `start` (plan 02.1-01) overrides the slot start (default: `next_slot_start()`,
    far beyond the generated slots) so window tests can seed a slot starting
    "soon" or already over; the caller keeps it unique per provider (a start
    near now may overlap the world's generated slots, so such tests use a
    provider id of their own).

    `with_billing=True` (plan 02-11) also creates its `verified` (self-pay)
    billing record through Billing's internal pre-check over `mesh`, with a
    service token minted for Scheduling (the only allowed caller)."""
    world = vw.world
    provider_id = provider_id or world.provider_id
    start = start if start is not None else vw.next_slot_start()
    slot_status = "booked" if status in AppointmentStatus.ACTIVE else "available"
    slot_id = await seed_slot(
        vw.engine,
        provider_id=provider_id,
        start=start,
        clinic_id=world.clinic_id,
        department_id=world.department_id,
        status=slot_status,
    )
    appointment_id = uuid.uuid4()
    async with vw.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
                "slot_start, slot_end, status, version, created_by) "
                "VALUES (:id, :c, :pt, :pr, :s, :st, :en, :status, 1, :by)"
            ),
            {
                "id": appointment_id,
                "c": world.clinic_id,
                "pt": world.patient_id,
                "pr": provider_id,
                "s": slot_id,
                "st": start,
                "en": start + timedelta(minutes=30),
                "status": status,
                "by": uuid.uuid4(),
            },
        )
        if slot_status == "booked":
            await conn.execute(
                text("UPDATE provider_slots SET held_by_appointment_id = :a WHERE id = :s"),
                {"a": appointment_id, "s": slot_id},
            )
    if with_billing:
        from tests.billing.conftest import precheck, service_token  # noqa: PLC0415

        assert mesh is not None, "with_billing needs the mesh"
        token = await service_token(mesh, "scheduling")
        resp = await precheck(mesh, token, str(world.clinic_id), appointment_id=str(appointment_id))
        assert resp.status_code == 201, resp.text
        assert resp.json()["status"] == "verified"
    return str(appointment_id)


async def add_provider(client: AsyncClient, vw: VisitWorld) -> tuple[str, dict[str, Any]]:
    """A second provider (with its own PROVIDER login) in the world's clinic
    and department: (provider id, login tokens)."""
    world = vw.world
    email = unique_email("provider")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "provider", "clinic_id": str(world.clinic_id)},
        headers=auth(world.admin),
    )
    assert created.status_code == 201, created.text
    provider = await client.post(
        "/api/providers",
        json={
            "department_id": str(world.department_id),
            "full_name": "Dr. Second Synthetic",
            "specialties": ["General"],
            "user_id": created.json()["id"],
        },
        headers=auth(world.admin),
    )
    assert provider.status_code == 201, provider.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    vw.provider_ids.append(uuid.UUID(provider.json()["id"]))
    return provider.json()["id"], login.json()


async def check_in(
    client: AsyncClient, tokens: dict, appointment_id: str, clinic_id: uuid.UUID | str | None = None
) -> httpx.Response:
    body: dict[str, Any] = {"appointment_id": appointment_id}
    if clinic_id is not None:
        body["clinic_id"] = str(clinic_id)
    return await client.post("/api/visits", json=body, headers=auth(tokens))


async def checked_in_visit(client: AsyncClient, vw: VisitWorld) -> dict[str, Any]:
    """A fresh confirmed appointment checked in by the world's front-desk."""
    appointment_id = await confirmed_appointment(vw)
    resp = await check_in(client, vw.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def get_visit(client: AsyncClient, tokens: dict, visit_id: str) -> httpx.Response:
    return await client.get(f"/api/visits/{visit_id}", headers=auth(tokens))


async def audit_count(entity_id: str, action: str) -> int:
    mongo: AsyncMongoClient = AsyncMongoClient(shared_settings.mongo_url)
    try:
        collection = mongo[shared_settings.mongo_db_name]["audit_log"]
        return await collection.count_documents({"entity_id": entity_id, "action": action})
    finally:
        await mongo.close()


async def visit_rows(appointment_id: str) -> int:
    """How many visits rows exist for `appointment_id` (read straight from visit_db)."""
    from services.visit.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT count(*) FROM visits WHERE appointment_id = :a"), {"a": appointment_id}
            )
    finally:
        await engine.dispose()


async def history_rows(visit_id: str) -> list[tuple[str | None, str]]:
    from services.visit.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT from_status, to_status FROM visit_state_history "
                    "WHERE visit_id = :v ORDER BY created_at, id"
                ),
                {"v": visit_id},
            )
            return [(r.from_status, r.to_status) for r in rows]
    finally:
        await engine.dispose()


async def cleanup_visit_rows(provider_ids: Iterable[uuid.UUID]) -> None:
    """Remove ONLY the visit rows a test created, scoped by its provider ids."""
    from services.visit.config import settings  # noqa: PLC0415

    ids = list(provider_ids)
    if not ids:
        return
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.begin() as conn:
            params = {"ids": ids}
            await conn.execute(
                text(
                    "DELETE FROM visit_state_history WHERE visit_id IN "
                    "(SELECT id FROM visits WHERE provider_id = ANY(:ids))"
                ),
                params,
            )
            await conn.execute(text("DELETE FROM visits WHERE provider_id = ANY(:ids)"), params)
    finally:
        await engine.dispose()


async def _visit_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    from services.scheduling.config import settings  # noqa: PLC0415

    engine = create_async_engine(settings.database_url, pool_size=10, max_overflow=10)
    world = await seed_bookable_world(client, mesh)
    vw = VisitWorld(world=world, engine=engine, provider_ids=[world.provider_id])
    try:
        yield vw
    finally:
        await cleanup_visit_rows(vw.provider_ids)
        await cleanup_scheduling_rows(engine, vw.provider_ids)
        await engine.dispose()


class _DownTemporal:
    """Stands in for an unreachable Temporal server: every start fails."""

    async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
        from temporalio.service import RPCError, RPCStatusCode  # noqa: PLC0415

        raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")


@pytest_asyncio.fixture(autouse=True)
async def _visit_temporal_down() -> AsyncIterator[None]:
    """By default the Visit app's Temporal is "down" (plan 02-11), so a
    completion in a test never lazily connects to the Compose Temporal and
    leaves a stray workflow there; the downstream start is deferred (marker
    NULL). `visit_worker` swaps in the test server's client for its block."""
    from tests.conftest import harness_apps  # noqa: PLC0415

    app = harness_apps()["visit"]
    previous = getattr(app.state, "temporal_client", None)
    app.state.temporal_client = _DownTemporal()
    try:
        yield
    finally:
        app.state.temporal_client = previous


@pytest_asyncio.fixture
async def visit_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    async for vw in _visit_world(client, mesh):
        yield vw


@pytest_asyncio.fixture
async def other_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    """A second, independent clinic (for cross-clinic scope tests)."""
    async for vw in _visit_world(client, mesh):
        yield vw
