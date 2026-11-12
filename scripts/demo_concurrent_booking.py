"""One-command live illustration of "one slot, many bookers, one winner" (plan 02-12).

Fires N parallel `POST /api/appointments` for the SAME slot through the
published gateway, each for a different patient and with its own
`Idempotency-Key`, and shows that exactly one is accepted (202), the other
N-1 are refused (409 "slot not available"), the winner's booking saga ends
`confirmed`, and the slot carries exactly one active appointment.

The graded evidence for double-booking prevention is the pytest concurrency
suite (plans 02-02 and 02-07: the partial unique index
`uq_appointments_active_slot` arbitrates inside Postgres); this script is the
live illustration against the deployed stack.

What it does, all through the gateway with synthetic data:
  1. logs in an existing admin (BOOKING_DEMO_ADMIN_EMAIL / _PASSWORD, falling
     back to BOOTSTRAP_ADMIN_EMAIL / _PASSWORD - see scripts/bootstrap_admin.py);
  2. creates a clinic, department, provider and weekly availability, a
     front-desk account and N patients, and generates slots;
  3. picks the first available slot two days out and fires the N bookings
     concurrently with asyncio.gather;
  4. polls the winner until confirmed (60 s) and counts the slot's active
     appointments.

Rate limit: the gateway allows 300 requests/minute per client by default and
20/minute on the auth routes (login, account creation); a run makes roughly
2N + 25 requests, 3 of them auth. For large N, or right after other e2e runs
from the same host, restart the gateway with
    GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 \
        docker compose -f infra/docker-compose.yml up -d gateway
Any 429 makes the script exit non-zero with that hint.

Usage:
    python scripts/demo_concurrent_booking.py [--gateway-url URL] [--parallel N] [--json]

Never prints tokens or passwords. Exit 0 only for exactly one 202, N-1 409s,
a confirmed winner and one active appointment on the slot.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from collections import Counter
from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

CLINIC_TIMEZONE = "Asia/Karachi"
STAFF_PASSWORD = "demo-" + uuid.uuid4().hex  # throwaway, never printed
RATE_LIMIT_HINT = (
    "the gateway rate-limited the demo (429). Restart it with higher limits, e.g. "
    "GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 "
    "docker compose -f infra/docker-compose.yml up -d gateway"
)


class DemoError(RuntimeError):
    pass


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _expect(resp: httpx.Response, status: int, what: str) -> dict[str, Any]:
    if resp.status_code == 429:
        raise DemoError(RATE_LIMIT_HINT)
    if resp.status_code != status:
        raise DemoError(f"{what}: expected {status}, got {resp.status_code}: {resp.text[:200]}")
    return resp.json()


async def _login(http: httpx.AsyncClient, email: str, password: str) -> str:
    body = _expect(
        await http.post("/api/auth/login", json={"email": email, "password": password}), 200, "login"
    )
    return body["access_token"]


async def _setup(http: httpx.AsyncClient, admin: str, parallel: int) -> dict[str, Any]:
    tag = uuid.uuid4().hex[:6]
    clinic = _expect(
        await http.post(
            "/api/clinics",
            json={"name": f"Demo Clinic {tag}", "timezone": CLINIC_TIMEZONE},
            headers=_auth(admin),
        ),
        201,
        "create clinic",
    )
    department = _expect(
        await http.post(
            f"/api/clinics/{clinic['id']}/departments", json={"name": "General"}, headers=_auth(admin)
        ),
        201,
        "create department",
    )
    provider = _expect(
        await http.post(
            "/api/providers",
            json={
                "department_id": department["id"],
                "full_name": "Dr. Demo Synthetic",
                "specialties": ["General"],
            },
            headers=_auth(admin),
        ),
        201,
        "create provider",
    )
    for day_of_week in range(7):
        _expect(
            await http.post(
                f"/api/providers/{provider['id']}/availability",
                json={
                    "day_of_week": day_of_week,
                    "start_time": "09:00:00",
                    "end_time": "12:00:00",
                    "slot_duration_minutes": 30,
                    "effective_from": "2020-01-01",
                },
                headers=_auth(admin),
            ),
            201,
            "create availability",
        )

    desk_email = f"demo-front-desk-{uuid.uuid4().hex[:12]}@example.com"
    _expect(
        await http.post(
            "/api/auth/admin/users",
            json={
                "email": desk_email,
                "password": STAFF_PASSWORD,
                "role": "front_desk",
                "clinic_id": clinic["id"],
            },
            headers=_auth(admin),
        ),
        201,
        "create front desk",
    )
    desk = await _login(http, desk_email, STAFF_PASSWORD)

    async def make_patient(index: int) -> str:
        body = _expect(
            await http.post(
                "/api/patients",
                json={
                    "clinic_id": clinic["id"],
                    "full_name": f"Zzyzx Demo Patient {index} {tag}",
                    "date_of_birth": "1990-01-01",
                },
                headers=_auth(desk),
            ),
            201,
            "create patient",
        )
        return body["id"]

    patients: list[str] = []
    for start in range(0, parallel, 10):  # small batches keep the setup gentle
        patients += await asyncio.gather(
            *(make_patient(i) for i in range(start, min(parallel, start + 10)))
        )

    today = datetime.now(ZoneInfo(CLINIC_TIMEZONE)).date()
    day = today + timedelta(days=2)
    _expect(
        await http.post(
            "/api/slots/generate",
            json={"provider_id": provider["id"], "from_date": today.isoformat(), "to_date": day.isoformat()},
            headers=_auth(admin),
        ),
        200,
        "generate slots",
    )
    return {"provider_id": provider["id"], "desk": desk, "patients": patients, "day": day}


def _day_bounds(day: date) -> tuple[str, str]:
    start = datetime.combine(day, dtime(0), tzinfo=ZoneInfo(CLINIC_TIMEZONE))
    return start.isoformat(), (start + timedelta(days=1)).isoformat()


async def run(gateway_url: str, parallel: int, admin_email: str, admin_password: str) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=gateway_url, timeout=30.0) as http:
        admin = await _login(http, admin_email, admin_password)
        world = await _setup(http, admin, parallel)
        desk, provider_id = world["desk"], world["provider_id"]
        start, end = _day_bounds(world["day"])

        slots = _expect(
            await http.get(
                "/api/slots",
                params={"provider_id": provider_id, "start": start, "end": end},
                headers=_auth(desk),
            ),
            200,
            "list slots",
        )
        if not slots:
            raise DemoError("no available slot was generated")
        slot = sorted(slots, key=lambda s: s["start_time"])[0]

        async def attempt(patient_id: str) -> httpx.Response:
            return await http.post(
                "/api/appointments",
                json={"slot_id": slot["id"], "patient_id": patient_id},
                headers={**_auth(desk), "Idempotency-Key": f"demo-{uuid.uuid4().hex}"},
            )

        began = time.monotonic()
        responses = await asyncio.gather(*(attempt(p) for p in world["patients"]))
        burst_ms = int((time.monotonic() - began) * 1000)

        codes = Counter(r.status_code for r in responses)
        if codes.get(429):
            raise DemoError(RATE_LIMIT_HINT)
        winners = [r.json()["appointment_id"] for r in responses if r.status_code == 202]

        winner_status = None
        if len(winners) == 1:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                got = _expect(
                    await http.get(f"/api/appointments/{winners[0]}", headers=_auth(desk)),
                    200,
                    "get winner",
                )
                winner_status = got["status"]
                if winner_status in {"confirmed", "failed"}:
                    break
                await asyncio.sleep(0.5)

        listed = _expect(
            await http.get(
                "/api/appointments",
                params={"provider_id": provider_id, "start": start, "end": end},
                headers=_auth(desk),
            ),
            200,
            "list appointments",
        )
        active = [
            a for a in listed if a["slot_id"] == slot["id"] and a["status"] in {"pending", "confirmed"}
        ]
        return {
            "parallel": parallel,
            "accepted": codes.get(202, 0),
            "conflicts": codes.get(409, 0),
            "other": sum(n for code, n in codes.items() if code not in (202, 409)),
            "winner_status": winner_status,
            "active_for_slot": len(active),
            "slot_start": slot["start_time"],
            "burst_ms": burst_ms,
        }


def _succeeded(summary: dict[str, Any]) -> bool:
    return (
        summary["accepted"] == 1
        and summary["conflicts"] == summary["parallel"] - 1
        and summary["other"] == 0
        and summary["winner_status"] == "confirmed"
        and summary["active_for_slot"] == 1
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Concurrent-booking demo: N bookers, one slot, one winner.")
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("SMARTHEALTH_GATEWAY_URL", "http://localhost:8000"),
        help="gateway base URL (env SMARTHEALTH_GATEWAY_URL; default http://localhost:8000)",
    )
    parser.add_argument("--parallel", type=int, default=50, help="concurrent bookings, 2-200 (default 50)")
    parser.add_argument("--json", action="store_true", help="print a JSON summary instead of a table")
    args = parser.parse_args(argv)
    if not 2 <= args.parallel <= 200:
        parser.error("--parallel must be between 2 and 200")

    email = os.environ.get("BOOKING_DEMO_ADMIN_EMAIL") or os.environ.get("BOOTSTRAP_ADMIN_EMAIL")
    password = os.environ.get("BOOKING_DEMO_ADMIN_PASSWORD") or os.environ.get("BOOTSTRAP_ADMIN_PASSWORD")
    if not email or not password:
        print(
            "ERROR: set BOOKING_DEMO_ADMIN_EMAIL/BOOKING_DEMO_ADMIN_PASSWORD "
            "(or BOOTSTRAP_ADMIN_EMAIL/BOOTSTRAP_ADMIN_PASSWORD) for an existing admin",
            file=sys.stderr,
        )
        return 2

    try:
        summary = asyncio.run(run(args.gateway_url.rstrip("/"), args.parallel, email, password))
    except DemoError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except httpx.TransportError as exc:
        print(f"ERROR: gateway not reachable at {args.gateway_url} ({type(exc).__name__})", file=sys.stderr)
        return 1

    ok = _succeeded(summary)
    if args.json:
        print(json.dumps(summary))
    else:
        print(f"Concurrent booking demo: {summary['parallel']} parallel bookings for one slot ({summary['slot_start']})")
        print(f"  202 accepted        : {summary['accepted']}")
        print(f"  409 slot not avail. : {summary['conflicts']}")
        print(f"  other responses     : {summary['other']}")
        print(f"  winner final status : {summary['winner_status']}")
        print(f"  active appts on slot: {summary['active_for_slot']}")
        print(f"  burst duration      : {summary['burst_ms']} ms")
        print("RESULT: " + ("one winner, no double booking" if ok else "UNEXPECTED OUTCOME"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
