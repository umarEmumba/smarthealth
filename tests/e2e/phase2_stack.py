"""Helpers for the Phase 2 real-stack suites (plan 02-12).

Everything here talks to the deployed Compose stack through the PUBLISHED
gateway port with a plain httpx client, exactly as an external client would:
nothing runs in-process except the admin seed, which (like the Phase 1 smoke
test) is inserted straight into identity_db through the published Postgres
port because public registration only creates patients (CR-01).

* `gateway_url()` - `SMARTHEALTH_GATEWAY_URL`, default http://localhost:8000.
* `require_stack(http)` - skips the test when the gateway is unreachable,
  unless `SMARTHEALTH_E2E_REQUIRE_STACK=1`, in which case it fails.
* `seed_phase2_world(http, patients=...)` - the 02-04 bookable world (clinic
  in Asia/Karachi, department, provider with a linked login and 7 templates
  09:00-12:00 / 30 min, front desk, one patient with a login, generated
  slots) built through the gateway, plus extra staff-created patients.
* `poll(fn, *, timeout=90)` - await `fn()` until it returns a truthy value.

All data is synthetic and is left in place (no hard deletes; the Phase 1
smoke-test convention).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, TypeVar

import httpx
import pytest

from tests.scheduling.seed import World, auth, seed_bookable_world

T = TypeVar("T")


def gateway_url() -> str:
    return os.environ.get("SMARTHEALTH_GATEWAY_URL", "http://localhost:8000").rstrip("/")


async def require_stack(http: httpx.AsyncClient) -> None:
    try:
        await http.get("/health")
    except httpx.TransportError as exc:
        message = f"gateway not reachable at {gateway_url()} ({type(exc).__name__}); start the stack first"
        if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
            pytest.fail(message)
        pytest.skip(message)


@dataclass
class E2EWorld:
    world: World
    extra_patient_ids: list[str] = field(default_factory=list)

    @property
    def front_desk(self) -> dict[str, str]:
        return auth(self.world.front_desk)

    @property
    def provider(self) -> dict[str, str]:
        return auth(self.world.provider)

    @property
    def admin(self) -> dict[str, str]:
        return auth(self.world.admin)

    @property
    def clinic_id(self) -> str:
        return str(self.world.clinic_id)

    @property
    def provider_id(self) -> str:
        return str(self.world.provider_id)

    @property
    def patient_ids(self) -> list[str]:
        return [str(self.world.patient_id), *self.extra_patient_ids]


async def seed_phase2_world(
    http: httpx.AsyncClient, *, patients: int = 4, days_ahead: int = 3
) -> E2EWorld:
    """`patients` is the total number of patients (the seeded one included)."""
    world = await seed_bookable_world(http, None, days_ahead=days_ahead)  # type: ignore[arg-type]
    extra: list[str] = []
    for index in range(max(0, patients - 1)):
        created = await http.post(
            "/api/patients",
            json={
                "clinic_id": str(world.clinic_id),
                "full_name": f"Zzyzx E2E Synthetic {index} {uuid.uuid4().hex[:6]}",
                "date_of_birth": "1990-01-01",
            },
            headers=auth(world.front_desk),
        )
        assert created.status_code == 201, created.text
        extra.append(created.json()["id"])
    return E2EWorld(world=world, extra_patient_ids=extra)


async def day_slots(http: httpx.AsyncClient, ew: E2EWorld, day: date, *, status: str = "available") -> list[dict[str, Any]]:
    start, end = ew.world.local_day_bounds(day)
    resp = await http.get(
        "/api/slots",
        params={
            "provider_id": ew.provider_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "status": status,
        },
        headers=ew.front_desk,
    )
    assert resp.status_code == 200, resp.text
    return sorted(resp.json(), key=lambda s: s["start_time"])


async def book(
    http: httpx.AsyncClient,
    ew: E2EWorld,
    *,
    slot_id: str,
    patient_id: str,
    coverage_ref: str | None = None,
) -> str:
    body: dict[str, Any] = {"slot_id": slot_id, "patient_id": patient_id}
    if coverage_ref is not None:
        body["coverage_ref"] = coverage_ref
    resp = await http.post(
        "/api/appointments",
        json=body,
        headers={**ew.front_desk, "Idempotency-Key": f"e2e-{uuid.uuid4().hex}"},
    )
    assert resp.status_code == 202, resp.text
    return resp.json()["appointment_id"]


async def get_appointment(http: httpx.AsyncClient, ew: E2EWorld, appointment_id: str) -> dict[str, Any]:
    resp = await http.get(f"/api/appointments/{appointment_id}", headers=ew.front_desk)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def billing_record(http: httpx.AsyncClient, ew: E2EWorld, appointment_id: str) -> dict[str, Any] | None:
    resp = await http.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=ew.front_desk
    )
    if resp.status_code == 404:
        return None
    assert resp.status_code == 200, resp.text
    return resp.json()


async def poll(fn: Callable[[], Awaitable[T]], *, timeout: float = 90, interval: float = 0.5) -> T:
    """Await `fn()` until it returns something truthy; fail after `timeout` s."""
    deadline = time.monotonic() + timeout
    last: T | None = None
    while time.monotonic() < deadline:
        last = await fn()
        if last:
            return last
        await asyncio.sleep(interval)
    pytest.fail(f"condition not met within {timeout}s (last value: {last!r})")


async def appointment_in(
    http: httpx.AsyncClient, ew: E2EWorld, appointment_id: str, statuses: set[str], *, timeout: float = 90
) -> dict[str, Any]:
    async def check() -> dict[str, Any] | None:
        got = await get_appointment(http, ew, appointment_id)
        return got if got["status"] in statuses else None

    return await poll(check, timeout=timeout)


async def billing_in(
    http: httpx.AsyncClient, ew: E2EWorld, appointment_id: str, status: str, *, timeout: float = 90
) -> dict[str, Any]:
    async def check() -> dict[str, Any] | None:
        record = await billing_record(http, ew, appointment_id)
        return record if record is not None and record["status"] == status else None

    return await poll(check, timeout=timeout)
