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
* `book_slot_inside_checkin_window(http, ew)` - plan 02.1-06: a slot that starts
  within Scheduling's check-in opening window (today, clinic-local), on a second
  provider, for the flow that checks in; the day-ahead slots are outside it.

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
from datetime import date, datetime, timedelta
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


# The smallest lead time (minutes) a slot may start from now, and the room that
# must remain before clinic-local midnight: a weekly template cannot cross
# midnight, so the window slot has to start and end on the same local day.
CHECKIN_SLOT_LEAD_MINUTES = 5
CHECKIN_SLOT_MIN_MINUTES_BEFORE_MIDNIGHT = 40
CHECKIN_SLOT_MINUTES = 30


async def book_slot_inside_checkin_window(http: httpx.AsyncClient, ew: E2EWorld) -> tuple[str, str]:
    """(slot id, provider id) of a fresh 30-minute slot that starts inside
    Scheduling's check-in opening window (it starts at least 5 minutes from now,
    on the next half-hour grid instant, so it is bookable and well within the
    default 60-minute window).

    The world's own templates are 09:00-12:00 and its slots are generated for
    today .. today+2, which are days away or already over, so this creates a
    second provider in the world's department, one weekly template for today's
    weekday whose window is exactly that slot (`effective_from` today), and
    generates today's slots for it. Everything goes through the gateway.

    Skips (an explicit reason) when fewer than 40 minutes remain before
    clinic-local midnight, because a template cannot cross midnight."""
    world = ew.world
    now_local = datetime.now(world.tz)
    midnight = datetime.combine(now_local.date() + timedelta(days=1), datetime.min.time(), tzinfo=world.tz)
    if midnight - now_local < timedelta(minutes=CHECKIN_SLOT_MIN_MINUTES_BEFORE_MIDNIGHT):
        pytest.skip(
            f"less than {CHECKIN_SLOT_MIN_MINUTES_BEFORE_MIDNIGHT} minutes before clinic-local "
            f"midnight ({world.timezone}): a weekly template cannot cross midnight, so no "
            "slot inside the check-in window can be built right now"
        )

    earliest = (now_local + timedelta(minutes=CHECKIN_SLOT_LEAD_MINUTES)).replace(second=0, microsecond=0)
    start = earliest.replace(minute=0) + timedelta(
        minutes=CHECKIN_SLOT_MINUTES * -(-earliest.minute // CHECKIN_SLOT_MINUTES)
    )
    end = start + timedelta(minutes=CHECKIN_SLOT_MINUTES)
    if end >= midnight:  # inside the 40-minute margin yet still crossing: rounding can add 30
        pytest.skip("the next half-hour slot would reach clinic-local midnight")

    provider = await http.post(
        "/api/providers",
        json={
            "department_id": str(world.department_id),
            "full_name": "Dr. Window Synthetic",
            "specialties": ["General"],
        },
        headers=ew.admin,
    )
    assert provider.status_code == 201, provider.text
    provider_id = provider.json()["id"]

    template = await http.post(
        f"/api/providers/{provider_id}/availability",
        json={
            "day_of_week": now_local.weekday(),
            "start_time": start.time().isoformat(),
            "end_time": end.time().isoformat(),
            "slot_duration_minutes": CHECKIN_SLOT_MINUTES,
            "effective_from": now_local.date().isoformat(),
        },
        headers=ew.admin,
    )
    assert template.status_code == 201, template.text

    generated = await http.post(
        "/api/slots/generate",
        json={
            "provider_id": provider_id,
            "from_date": now_local.date().isoformat(),
            "to_date": now_local.date().isoformat(),
        },
        headers=ew.admin,
    )
    assert generated.status_code == 200, generated.text

    day_start, day_end = world.local_day_bounds(now_local.date())
    slots = await http.get(
        "/api/slots",
        params={
            "provider_id": provider_id,
            "start": day_start.isoformat(),
            "end": day_end.isoformat(),
            "status": "available",
        },
        headers=ew.front_desk,
    )
    assert slots.status_code == 200, slots.text
    found = [s for s in slots.json() if datetime.fromisoformat(s["start_time"]) == start]
    assert len(found) == 1, (start, slots.json())
    return found[0]["id"], provider_id
