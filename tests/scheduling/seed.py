"""`seed_bookable_world`: one bookable world built through the gateway (plan 02-04).

Later Phase 2 plans (booking, cancel, reschedule, waitlist, visits) reuse it, so
everything is created the way a real client would create it, through the
gateway with an admin bootstrapped by `bootstrap_admin_and_login`:

* a clinic in `Asia/Karachi` (UTC+5, no DST) and one department;
* a provider with a linked PROVIDER login, and one availability template per
  day of the week, 09:00-12:00 local with 30-minute slots (six slots a day,
  the first at 04:00Z);
* a FRONT_DESK user scoped to the clinic;
* a patient profile linked to a self-registered PATIENT account in the clinic;
* slots generated with `POST /api/slots/generate` for clinic-local
  today .. today + `days_ahead`.

Every value is synthetic. The returned `World` carries ids and token pairs.
Slot rows are committed in scheduling_db; callers remove them with
`tests.scheduling.conftest.cleanup_scheduling_rows(engine, [world.provider_id])`.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from httpx import AsyncClient

from tests.conftest import bootstrap_admin_and_login, unique_email

PASSWORD = "correct-horse-battery"
CLINIC_TIMEZONE = "Asia/Karachi"
TEMPLATE_START = time(9, 0)
TEMPLATE_END = time(12, 0)
SLOT_MINUTES = 30


def auth(tokens: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


@dataclass
class World:
    clinic_id: uuid.UUID
    department_id: uuid.UUID
    provider_id: uuid.UUID
    provider_user_id: uuid.UUID
    patient_id: uuid.UUID
    patient_user_id: uuid.UUID
    template_ids: list[uuid.UUID]
    admin: dict[str, Any]
    front_desk: dict[str, Any]
    provider: dict[str, Any]
    patient: dict[str, Any]
    timezone: str
    first_day: date
    last_day: date
    generated: dict[str, Any] = field(default_factory=dict)

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def local_day_bounds(self, day: date) -> tuple[datetime, datetime]:
        """[start, end) of clinic-local `day` as tz-aware datetimes."""
        start = datetime.combine(day, time(0), tzinfo=self.tz)
        return start, start + timedelta(days=1)


def _ok(response: httpx.Response, expected: int = 201) -> dict[str, Any]:
    assert response.status_code == expected, response.text
    return response.json()


async def _login(client: AsyncClient, email: str) -> dict[str, Any]:
    return _ok(
        await client.post("/api/auth/login", json={"email": email, "password": PASSWORD}), 200
    )


async def _staff(client: AsyncClient, admin: dict, role: str, clinic_id: str) -> tuple[str, dict]:
    email = unique_email(role.replace("_", "-"))
    created = _ok(
        await client.post(
            "/api/auth/admin/users",
            json={"email": email, "password": PASSWORD, "role": role, "clinic_id": clinic_id},
            headers=auth(admin),
        )
    )
    return created["id"], await _login(client, email)


async def seed_bookable_world(
    client: AsyncClient, mesh: httpx.AsyncClient, *, days_ahead: int = 2
) -> World:
    """Create the world described in the module docstring and return it.

    `mesh` is taken so callers hold the in-process network open for the whole
    seeding (the gateway and every service call run over it).
    """
    del mesh  # the `client` fixture already routes through it
    admin = await bootstrap_admin_and_login(client)

    clinic = _ok(
        await client.post(
            "/api/clinics",
            json={"name": f"Seed Clinic {uuid.uuid4().hex[:6]}", "timezone": CLINIC_TIMEZONE},
            headers=auth(admin),
        )
    )
    clinic_id = clinic["id"]
    department = _ok(
        await client.post(
            f"/api/clinics/{clinic_id}/departments", json={"name": "General"}, headers=auth(admin)
        )
    )

    provider_user_id, provider_tokens = await _staff(client, admin, "provider", clinic_id)
    provider = _ok(
        await client.post(
            "/api/providers",
            json={
                "department_id": department["id"],
                "full_name": "Dr. Seed Synthetic",
                "specialties": ["General"],
                "user_id": provider_user_id,
            },
            headers=auth(admin),
        )
    )

    template_ids: list[uuid.UUID] = []
    for day_of_week in range(7):
        template = _ok(
            await client.post(
                f"/api/providers/{provider['id']}/availability",
                json={
                    "day_of_week": day_of_week,
                    "start_time": TEMPLATE_START.isoformat(),
                    "end_time": TEMPLATE_END.isoformat(),
                    "slot_duration_minutes": SLOT_MINUTES,
                    "effective_from": "2020-01-01",
                },
                headers=auth(admin),
            )
        )
        template_ids.append(uuid.UUID(template["id"]))

    _front_desk_user_id, front_desk = await _staff(client, admin, "front_desk", clinic_id)

    patient_email = unique_email("patient")
    patient_account = _ok(
        await client.post(
            "/api/auth/register",
            json={"email": patient_email, "password": PASSWORD, "clinic_id": clinic_id},
        )
    )
    patient_tokens = await _login(client, patient_email)
    patient = _ok(
        await client.post(
            "/api/patients",
            json={
                "clinic_id": clinic_id,
                "full_name": "Zzyzx Seed Synthetic",
                "date_of_birth": "1990-01-01",
                "user_id": patient_account["id"],
            },
            headers=auth(front_desk),
        )
    )

    first_day = datetime.now(ZoneInfo(CLINIC_TIMEZONE)).date()
    last_day = first_day + timedelta(days=days_ahead)
    generated = _ok(
        await client.post(
            "/api/slots/generate",
            json={
                "provider_id": provider["id"],
                "from_date": first_day.isoformat(),
                "to_date": last_day.isoformat(),
            },
            headers=auth(admin),
        ),
        200,
    )

    return World(
        clinic_id=uuid.UUID(clinic_id),
        department_id=uuid.UUID(department["id"]),
        provider_id=uuid.UUID(provider["id"]),
        provider_user_id=uuid.UUID(provider_user_id),
        patient_id=uuid.UUID(patient["id"]),
        patient_user_id=uuid.UUID(patient_account["id"]),
        template_ids=template_ids,
        admin=admin,
        front_desk=front_desk,
        provider=provider_tokens,
        patient=patient_tokens,
        timezone=CLINIC_TIMEZONE,
        first_day=first_day,
        last_day=last_day,
        generated=generated,
    )
