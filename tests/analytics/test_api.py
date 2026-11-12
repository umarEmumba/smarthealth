"""The analytics read API through the in-process gateway mesh (plan 03-13 Task 2;
ANLY-01..05, T-3-33, T-3-37).

Projection rows are seeded straight into analytics_db for two real clinics (created
through the gateway so the front-desk tokens carry a real clinic id), then read back
through the gateway with admin, front-desk, provider and patient tokens and no token.
Fixed historical days keep the numbers independent of the wall clock. Each test class
of numbers is asserted on its own clinics; the all-clinics read is `>=` because the live
analytics_db may hold other clinics' rows. Rows are removed at teardown.
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from services.analytics.config import settings
from tests.conftest import bootstrap_admin_and_login, unique_email

pytestmark = pytest.mark.integration

PASSWORD = "correct-horse-battery"
D1 = datetime.datetime(2026, 3, 10, 12, 0, tzinfo=datetime.UTC)
D2 = datetime.datetime(2026, 3, 11, 12, 0, tzinfo=datetime.UTC)
WINDOW = {"from": "2026-03-01", "to": "2026-03-31"}


def bearer(tokens: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


@dataclass
class ApiWorld:
    clinic_a: str
    clinic_b: str
    admin: dict[str, Any]
    front_desk_a: dict[str, Any]
    provider_a: dict[str, Any]
    patient_a: dict[str, Any]


async def _login(client: AsyncClient, email: str) -> dict[str, Any]:
    resp = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _user(client: AsyncClient, admin: dict[str, Any], role: str, clinic: str) -> dict[str, Any]:
    email = unique_email(f"analytics-{role}")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": role, "clinic_id": clinic},
        headers=bearer(admin),
    )
    assert created.status_code == 201, created.text
    return await _login(client, email)


async def _sql(statements: list[tuple[str, dict[str, Any]]]) -> None:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            for sql, params in statements:
                await conn.execute(text(sql), params)
    finally:
        await engine.dispose()


async def _seed(clinic_a: str, clinic_b: str) -> None:
    a, b = uuid.UUID(clinic_a), uuid.UUID(clinic_b)
    statements: list[tuple[str, dict[str, Any]]] = []
    # Clinic A: 3 registered, 1 deactivated -> total 2.
    for index in range(3):
        statements.append(
            (
                "INSERT INTO patients (patient_id, clinic_id, registered_at, deactivated_at) "
                "VALUES (:p, :c, :r, :d)",
                {"p": uuid.uuid4(), "c": a, "r": D1, "d": D2 if index == 0 else None},
            )
        )
    # Clinic A: 4 booked (2 on D1, 2 on D2), 1 cancelled -> rate 0.25.
    for index, (booked, cancelled) in enumerate([(D1, None), (D1, D2), (D2, None), (D2, None)]):
        statements.append(
            (
                "INSERT INTO appointments (appointment_id, clinic_id, booked_at, cancelled_at) "
                "VALUES (:p, :c, :b, :x)",
                {"p": uuid.uuid4(), "c": a, "b": booked, "x": cancelled},
            )
        )
    # Clinic A: 3 completed visits on D2; waits 600 s and 300 s, one without a start.
    start = D2 - datetime.timedelta(minutes=30)
    for checked_in, started in [
        (start, start + datetime.timedelta(seconds=600)),
        (start, start + datetime.timedelta(seconds=300)),
        (start, None),
    ]:
        statements.append(
            (
                "INSERT INTO visits_completed (visit_id, clinic_id, checked_in_at, started_at, "
                "completed_at) VALUES (:v, :c, :i, :s, :d)",
                {"v": uuid.uuid4(), "c": a, "i": checked_in, "s": started, "d": D2},
            )
        )
    # Clinic B: 1 patient, 1 booked appointment.
    statements.append(
        (
            "INSERT INTO patients (patient_id, clinic_id, registered_at) VALUES (:p, :c, :r)",
            {"p": uuid.uuid4(), "c": b, "r": D1},
        )
    )
    statements.append(
        (
            "INSERT INTO appointments (appointment_id, clinic_id, booked_at) VALUES (:p, :c, :b)",
            {"p": uuid.uuid4(), "c": b, "b": D1},
        )
    )
    await _sql(statements)


async def _clean(*clinics: str) -> None:
    await _sql(
        [
            (f"DELETE FROM {table} WHERE clinic_id = :c", {"c": uuid.UUID(clinic)})
            for table in ("patients", "appointments", "visits_completed")
            for clinic in clinics
        ]
    )


@pytest_asyncio.fixture
async def world(client: AsyncClient) -> AsyncIterator[ApiWorld]:
    admin = await bootstrap_admin_and_login(client)
    clinics: list[str] = []
    for name in ("Analytics Clinic A", "Analytics Clinic B"):
        resp = await client.post(
            "/api/clinics", json={"name": f"{name} {uuid.uuid4().hex[:6]}", "timezone": "UTC"},
            headers=bearer(admin),
        )
        assert resp.status_code == 201, resp.text
        clinics.append(resp.json()["id"])
    clinic_a, clinic_b = clinics
    await _seed(clinic_a, clinic_b)
    patient_email = unique_email("analytics-patient")
    registered = await client.post(
        "/api/auth/register",
        json={"email": patient_email, "password": PASSWORD, "clinic_id": clinic_a},
    )
    assert registered.status_code == 201, registered.text
    try:
        yield ApiWorld(
            clinic_a=clinic_a,
            clinic_b=clinic_b,
            admin=admin,
            front_desk_a=await _user(client, admin, "front_desk", clinic_a),
            provider_a=await _user(client, admin, "provider", clinic_a),
            patient_a=await _login(client, patient_email),
        )
    finally:
        await _clean(clinic_a, clinic_b)


async def test_admin_summary_has_all_five_metrics(client: AsyncClient, world: ApiWorld) -> None:
    resp = await client.get(
        "/api/analytics/summary",
        params={"clinic_id": world.clinic_a, **WINDOW},
        headers=bearer(world.admin),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "clinic_id": world.clinic_a,
        "from": "2026-03-01",
        "to": "2026-03-31",
        "total_patients": 2,
        "appointments_booked": 4,
        "completed_visits": 3,
        "cancellation_rate": 0.25,
        "average_wait_seconds": 450.0,
    }


async def test_window_filters_the_numbers(client: AsyncClient, world: ApiWorld) -> None:
    resp = await client.get(
        "/api/analytics/summary",
        params={"clinic_id": world.clinic_a, "from": "2026-03-10", "to": "2026-03-10"},
        headers=bearer(world.admin),
    )
    body = resp.json()
    # Only D1: two bookings, one of them cancelled; no visits completed that day.
    assert (body["appointments_booked"], body["cancellation_rate"]) == (2, 0.5)
    assert (body["completed_visits"], body["average_wait_seconds"]) == (0, None)
    assert body["total_patients"] == 2  # a current count, not windowed


async def test_empty_window_has_null_rate_and_wait(client: AsyncClient, world: ApiWorld) -> None:
    resp = await client.get(
        "/api/analytics/summary",
        params={"clinic_id": world.clinic_b, "from": "2025-01-01", "to": "2025-01-31"},
        headers=bearer(world.admin),
    )
    body = resp.json()
    assert body["appointments_booked"] == 0
    assert body["cancellation_rate"] is None
    assert body["average_wait_seconds"] is None


async def test_admin_all_clinics_includes_every_seeded_clinic(
    client: AsyncClient, world: ApiWorld
) -> None:
    resp = await client.get("/api/analytics/summary", params=WINDOW, headers=bearer(world.admin))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["clinic_id"] is None
    assert body["total_patients"] >= 3 and body["appointments_booked"] >= 5


async def test_timeseries_is_zero_filled_per_day(client: AsyncClient, world: ApiWorld) -> None:
    resp = await client.get(
        "/api/analytics/timeseries",
        params={
            "metric": "appointments_booked",
            "clinic_id": world.clinic_a,
            "from": "2026-03-09",
            "to": "2026-03-12",
        },
        headers=bearer(world.admin),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == [
        {"day": "2026-03-09", "value": 0},
        {"day": "2026-03-10", "value": 2},
        {"day": "2026-03-11", "value": 2},
        {"day": "2026-03-12", "value": 0},
    ]
    visits = await client.get(
        "/api/analytics/timeseries",
        params={"metric": "completed_visits", "clinic_id": world.clinic_a, **WINDOW},
        headers=bearer(world.admin),
    )
    assert {p["day"]: p["value"] for p in visits.json() if p["value"]} == {"2026-03-11": 3}
    assert len(visits.json()) == 31


async def test_front_desk_reads_only_its_own_clinic(client: AsyncClient, world: ApiWorld) -> None:
    headers = bearer(world.front_desk_a)
    own = await client.get("/api/analytics/summary", params=WINDOW, headers=headers)  # omitted
    assert own.status_code == 200, own.text
    assert own.json()["clinic_id"] == world.clinic_a
    assert own.json()["appointments_booked"] == 4

    explicit = await client.get(
        "/api/analytics/summary", params={"clinic_id": world.clinic_a, **WINDOW}, headers=headers
    )
    assert explicit.status_code == 200

    other = await client.get(
        "/api/analytics/summary", params={"clinic_id": world.clinic_b, **WINDOW}, headers=headers
    )
    assert other.status_code == 403, other.text
    other_series = await client.get(
        "/api/analytics/timeseries",
        params={"metric": "completed_visits", "clinic_id": world.clinic_b, **WINDOW},
        headers=headers,
    )
    assert other_series.status_code == 403
    # Not an existence oracle: a clinic that has no data answers the same 403.
    assert (
        await client.get(
            "/api/analytics/summary", params={"clinic_id": str(uuid.uuid4())}, headers=headers
        )
    ).status_code == 403


@pytest.mark.parametrize("path", ["/api/analytics/summary", "/api/analytics/timeseries"])
async def test_provider_and_patient_tokens_are_forbidden_and_no_token_is_401(
    client: AsyncClient, world: ApiWorld, path: str
) -> None:
    params = {"metric": "appointments_booked", "clinic_id": world.clinic_a, **WINDOW}
    for tokens in (world.provider_a, world.patient_a):
        resp = await client.get(path, params=params, headers=bearer(tokens))
        assert resp.status_code == 403, (path, resp.text)
    assert (await client.get(path, params=params)).status_code == 401
    assert (
        await client.get(path, params=params, headers={"Authorization": "Bearer not-a-token"})
    ).status_code == 401


async def test_invalid_parameters_are_422(client: AsyncClient, world: ApiWorld) -> None:
    headers = bearer(world.admin)
    summary = "/api/analytics/summary"
    series = "/api/analytics/timeseries"
    reversed_window = await client.get(
        summary, params={"from": "2026-03-31", "to": "2026-03-01"}, headers=headers
    )
    assert reversed_window.status_code == 422
    too_long = await client.get(
        summary, params={"from": "2025-01-01", "to": "2026-03-01"}, headers=headers
    )
    assert too_long.status_code == 422
    # Exactly 366 days is allowed, 367 is not.
    assert (
        await client.get(
            summary, params={"from": "2025-03-01", "to": "2026-03-01"}, headers=headers
        )
    ).status_code == 200
    assert (
        await client.get(
            summary, params={"from": "2025-02-28", "to": "2026-03-01"}, headers=headers
        )
    ).status_code == 422
    unknown_metric = await client.get(series, params={"metric": "revenue", **WINDOW}, headers=headers)
    assert unknown_metric.status_code == 422
    missing_metric = await client.get(series, params=WINDOW, headers=headers)
    assert missing_metric.status_code == 422
    bad_clinic = await client.get(summary, params={"clinic_id": "not-a-uuid"}, headers=headers)
    assert bad_clinic.status_code == 422


async def test_default_window_is_the_last_configured_days(
    client: AsyncClient, world: ApiWorld
) -> None:
    resp = await client.get("/api/analytics/summary", headers=bearer(world.admin))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    start = datetime.date.fromisoformat(body["from"])
    end = datetime.date.fromisoformat(body["to"])
    assert (end - start).days + 1 == settings.default_window_days
    assert end <= datetime.datetime.now(datetime.UTC).date()


async def test_responses_carry_aggregates_only(client: AsyncClient, world: ApiWorld) -> None:
    resp = await client.get(
        "/api/analytics/summary",
        params={"clinic_id": world.clinic_a, **WINDOW},
        headers=bearer(world.admin),
    )
    keys = set(resp.json())
    assert not {k for k in keys if "patient_id" in k or "appointment_id" in k or "visit_id" in k}
    assert keys == {
        "clinic_id", "from", "to", "total_patients", "appointments_booked", "completed_visits",
        "cancellation_rate", "average_wait_seconds",
    }

