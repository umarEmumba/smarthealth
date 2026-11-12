"""Build the load-test world through the gateway and write a booking plan (plan 02.1-04, D2).

The k6 script (`loadtest/booking.js`) books against real slots for real patients,
so something has to create them first. This script does that through the PUBLISHED
gateway, exactly as an external client would, with synthetic data only:

  1. logs in an existing admin (env LOADTEST_ADMIN_EMAIL / LOADTEST_ADMIN_PASSWORD;
     create one with scripts/bootstrap_admin.py);
  2. creates a clinic (Asia/Karachi), one department, N providers each with a
     weekly 06:00-22:00 template for every day of the week, a FRONT_DESK user for
     the clinic (random password), and M + C synthetic patient profiles
     (M for the booking plan, C = 200 reserved for the single-slot contention run);
  3. generates slots for every provider and lists every available future slot;
  4. reserves ONE slot (the farthest one) plus the C patients for the contention
     profile, and builds the booking plan for the rest;
  5. writes `loadtest/data/plan.json` (mode 0600, gitignored; it holds the
     front-desk credentials the k6 run logs in with).

The plan assigns every slot exactly once, never gives a patient two overlapping
slots (the database would refuse that with a 409) and spreads slots that start at
the same instant across different patients.

Usage:
    LOADTEST_ADMIN_EMAIL=... LOADTEST_ADMIN_PASSWORD=... \\
        python scripts/loadtest_seed.py [--gateway-url URL] [--providers N] \\
            [--patients M] [--days D] [--slot-minutes S] [--out PATH]

Never prints a token or a password; stdout carries counts only. Exit codes: 0 on
success, 1 on a failure talking to the gateway, 2 on a missing admin variable.

LOCAL STACK ONLY. The run makes about M + C + 60 requests; raise the gateway
budget first (loadtest/README.md).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

CLINIC_TIMEZONE = "Asia/Karachi"
DAY_START = "06:00:00"
DAY_END = "22:00:00"
PATIENT_BATCH = 20
GENERATE_WINDOW_DAYS = 28  # the gateway refuses a generate range wider than 31
LIST_WINDOW_DAYS = 7
CONTENTION_PATIENTS = 200
MIN_LEAD_MINUTES = 60
TOKEN_REFRESH_SECONDS = 10 * 60  # access tokens live 15 minutes
ADMIN_ENV = ("LOADTEST_ADMIN_EMAIL", "LOADTEST_ADMIN_PASSWORD")
DEFAULT_OUT = "loadtest/data/plan.json"

RATE_LIMIT_HINT = (
    "the gateway rate-limited the seed (429). Raise GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE "
    "(and GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE) in infra/.env and restart the gateway "
    "(see loadtest/README.md)"
)


class SeedError(RuntimeError):
    """A failure the operator can act on; the message never carries a secret."""


class PlanError(ValueError):
    """The booking plan cannot be built from the given slots and patients."""


# --- the booking plan (pure) ----------------------------------------------------


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def build_booking_plan(slots: list[dict[str, Any]], patient_ids: list[str]) -> list[dict[str, str]]:
    """Assign every slot to exactly one patient, in booking order.

    `slots` are dicts with `id`, `provider_id`, `start_time` and `end_time` (the
    shape `GET /api/slots` returns). Slots are sorted by start then provider id.
    Patients are taken in rotation, skipping any patient still busy with an
    earlier slot, so:

    * every slot appears once;
    * a patient never holds two overlapping slots (a slot starts at or after the
      end of that patient's previous one);
    * slots that start at the same instant, one per provider, get different
      patients;
    * the result is deterministic for the same input.

    Raises `PlanError` when there are fewer patients than providers (slots that
    start together could not all get a distinct patient) or when no patient is
    free for some slot.
    """
    providers = {str(slot["provider_id"]) for slot in slots}
    if slots and len(patient_ids) < len(providers):
        raise PlanError(
            f"need at least one patient per provider ({len(patient_ids)} patients, "
            f"{len(providers)} providers): simultaneous slots could not get distinct patients"
        )

    ordered = sorted(slots, key=lambda s: (_instant(s["start_time"]), str(s["provider_id"]), str(s["id"])))
    busy_until: dict[str, datetime] = {}
    plan: list[dict[str, str]] = []
    cursor = 0
    count = len(patient_ids)
    for slot in ordered:
        start, end = _instant(slot["start_time"]), _instant(slot["end_time"])
        for step in range(count):
            index = (cursor + step) % count
            patient = patient_ids[index]
            free_at = busy_until.get(patient)
            if free_at is None or free_at <= start:
                busy_until[patient] = end
                cursor = (index + 1) % count
                plan.append({"slot_id": str(slot["id"]), "patient_id": str(patient)})
                break
        else:
            raise PlanError(f"no free patient for slot {slot['id']}: add patients")
    return plan


# --- the world (through the gateway) --------------------------------------------


@dataclass
class World:
    clinic_id: str
    front_desk_email: str
    front_desk_password: str
    plan: list[dict[str, str]]
    contention_slot: dict[str, str]
    contention_patient_ids: list[str]
    providers: int = 0
    patients: int = 0
    slots_created: int = 0
    slots_listed: int = 0
    gateway_url: str = ""


class _Token:
    """An access token that is renewed before the 15-minute lifetime runs out."""

    def __init__(self, client: httpx.AsyncClient, email: str, password: str) -> None:
        self._client, self._email, self._password = client, email, password
        self._value = ""
        self._obtained = 0.0

    async def headers(self) -> dict[str, str]:
        if not self._value or time.monotonic() - self._obtained > TOKEN_REFRESH_SECONDS:
            response = await self._client.post(
                "/api/auth/login", json={"email": self._email, "password": self._password}
            )
            body = _expect(response, 200, "login")
            self._value, self._obtained = body["access_token"], time.monotonic()
        return {"Authorization": f"Bearer {self._value}"}


def _expect(response: httpx.Response, status: int, what: str) -> Any:
    if response.status_code == 429:
        raise SeedError(RATE_LIMIT_HINT)
    if response.status_code != status:
        # Status and a short body only; request bodies (credentials) are never echoed.
        raise SeedError(f"{what}: expected {status}, got {response.status_code}: {response.text[:200]}")
    return response.json()


async def _create_patients(
    client: httpx.AsyncClient, token: _Token, clinic_id: str, count: int, label: str, tag: str
) -> list[str]:
    async def one(index: int) -> str:
        response = await client.post(
            "/api/patients",
            json={
                "clinic_id": clinic_id,
                "full_name": f"Zzyzx Load {label} {index:04d} {tag}",
                "date_of_birth": "1990-01-01",
            },
            headers=await token.headers(),
        )
        return str(_expect(response, 201, "create patient")["id"])

    ids: list[str] = []
    for start in range(0, count, PATIENT_BATCH):
        ids += await asyncio.gather(*(one(i) for i in range(start, min(count, start + PATIENT_BATCH))))
    return ids


def _local_midnight(day: date, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, dtime(0), tzinfo=tz)


async def build_world(
    client: httpx.AsyncClient,
    *,
    admin_email: str,
    admin_password: str,
    providers: int,
    patients: int,
    days: int,
    slot_minutes: int,
    contention_patients: int = CONTENTION_PATIENTS,
    min_lead_minutes: int = MIN_LEAD_MINUTES,
) -> World:
    """Create the load-test world through `client` (a client onto the gateway).

    Slots starting sooner than `min_lead_minutes` from now are left out of the plan
    so a past-slot rule can never turn a planned booking into a failure.
    """
    if providers < 1 or patients < providers or days < 1 or slot_minutes < 1:
        raise SeedError("need providers >= 1, patients >= providers, days >= 1, slot minutes >= 1")
    tag = uuid.uuid4().hex[:6]
    admin = _Token(client, admin_email, admin_password)

    clinic = _expect(
        await client.post(
            "/api/clinics",
            json={"name": f"Load Clinic {tag}", "timezone": CLINIC_TIMEZONE},
            headers=await admin.headers(),
        ),
        201,
        "create clinic",
    )
    clinic_id = str(clinic["id"])
    department = _expect(
        await client.post(
            f"/api/clinics/{clinic_id}/departments",
            json={"name": "General"},
            headers=await admin.headers(),
        ),
        201,
        "create department",
    )

    desk_email = f"loadtest-front-desk-{uuid.uuid4().hex[:12]}@example.com"
    desk_password = secrets.token_urlsafe(24)
    _expect(
        await client.post(
            "/api/auth/admin/users",
            json={"email": desk_email, "password": desk_password, "role": "front_desk", "clinic_id": clinic_id},
            headers=await admin.headers(),
        ),
        201,
        "create front desk",
    )
    desk = _Token(client, desk_email, desk_password)

    today = datetime.now(ZoneInfo(CLINIC_TIMEZONE)).date()
    provider_ids: list[str] = []
    for number in range(1, providers + 1):
        provider = _expect(
            await client.post(
                "/api/providers",
                json={
                    "department_id": str(department["id"]),
                    "full_name": f"Load Provider {number:02d}",
                    "specialties": ["General"],
                },
                headers=await admin.headers(),
            ),
            201,
            "create provider",
        )
        provider_ids.append(str(provider["id"]))
        for day_of_week in range(7):
            _expect(
                await client.post(
                    f"/api/providers/{provider['id']}/availability",
                    json={
                        "day_of_week": day_of_week,
                        "start_time": DAY_START,
                        "end_time": DAY_END,
                        "slot_duration_minutes": slot_minutes,
                        "effective_from": today.isoformat(),
                    },
                    headers=await admin.headers(),
                ),
                201,
                "create availability",
            )

    booking_patients = await _create_patients(client, desk, clinic_id, patients, "Patient", tag)
    reserved_patients = await _create_patients(client, desk, clinic_id, contention_patients, "Contender", tag)

    # Generate in windows no wider than the gateway allows, then list the slots.
    created = 0
    last_day = today + timedelta(days=days - 1)
    for provider_id in provider_ids:
        window_start = today
        while window_start <= last_day:
            window_end = min(window_start + timedelta(days=GENERATE_WINDOW_DAYS - 1), last_day)
            result = _expect(
                await client.post(
                    "/api/slots/generate",
                    json={
                        "provider_id": provider_id,
                        "from_date": window_start.isoformat(),
                        "to_date": window_end.isoformat(),
                    },
                    headers=await admin.headers(),
                ),
                200,
                "generate slots",
            )
            created += int(result["created"])
            window_start = window_end + timedelta(days=1)

    tz = ZoneInfo(CLINIC_TIMEZONE)
    earliest = datetime.now(timezone.utc) + timedelta(minutes=min_lead_minutes)
    listed: list[dict[str, Any]] = []
    for provider_id in provider_ids:
        window_start = today
        while window_start <= last_day:
            window_end = min(window_start + timedelta(days=LIST_WINDOW_DAYS), last_day + timedelta(days=1))
            found = _expect(
                await client.get(
                    "/api/slots",
                    params={
                        "provider_id": provider_id,
                        "start": _local_midnight(window_start, tz).isoformat(),
                        "end": _local_midnight(window_end, tz).isoformat(),
                        "status": "available",
                    },
                    headers=await desk.headers(),
                ),
                200,
                "list slots",
            )
            listed += found
            window_start = window_end
    future = [slot for slot in listed if _instant(slot["start_time"]) >= earliest]
    if len(future) < 2:
        raise SeedError("fewer than two bookable future slots were generated; raise --days")

    ordered = sorted(future, key=lambda s: (_instant(s["start_time"]), str(s["provider_id"]), str(s["id"])))
    contention = ordered.pop()  # the farthest slot, kept out of the booking plan
    plan = build_booking_plan(ordered, booking_patients)

    return World(
        clinic_id=clinic_id,
        front_desk_email=desk_email,
        front_desk_password=desk_password,
        plan=plan,
        contention_slot={
            "slot_id": str(contention["id"]),
            "provider_id": str(contention["provider_id"]),
            "slot_start": str(contention["start_time"]),
            "slot_end": str(contention["end_time"]),
        },
        contention_patient_ids=reserved_patients,
        providers=len(provider_ids),
        patients=len(booking_patients),
        slots_created=created,
        slots_listed=len(listed),
        gateway_url=str(client.base_url).rstrip("/"),
    )


# --- plan.json and the CLI ------------------------------------------------------


def world_to_plan_json(world: World) -> dict[str, Any]:
    return {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "clinic_id": world.clinic_id,
        "front_desk": {"email": world.front_desk_email, "password": world.front_desk_password},
        "plan": world.plan,
        "contention": {
            **world.contention_slot,
            "patient_ids": world.contention_patient_ids,
        },
    }


def write_plan(world: World, out: Path) -> None:
    """Write plan.json with mode 0600 (it holds the front-desk password)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(world_to_plan_json(world), handle)
    os.chmod(out, 0o600)  # an existing file keeps its old mode through O_CREAT


def _required_entries() -> int | None:
    """Plan entries the smoke + sustained + burst sequence consumes, when known."""
    path = Path(__file__).resolve().parent.parent / "loadtest" / "nfr.json"
    try:
        targets = json.loads(path.read_text(encoding="utf-8"))
        return int(
            5 * 30  # smoke: 5 requests per second for 30 s
            + targets["sustained_rps"] * targets["sustained_seconds"]
            + targets["burst_rps"] * targets["burst_seconds"]
        )
    except (OSError, ValueError, KeyError):
        return None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed a load-test world through the gateway and write loadtest/data/plan.json. "
        f"Admin credentials come only from {ADMIN_ENV[0]} and {ADMIN_ENV[1]}."
    )
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("SMARTHEALTH_GATEWAY_URL", "http://localhost:8000"),
        help="gateway base URL (env SMARTHEALTH_GATEWAY_URL; default http://localhost:8000)",
    )
    parser.add_argument("--providers", type=int, default=5, help="providers to create (default 5)")
    parser.add_argument("--patients", type=int, default=600, help="patients for the booking plan (default 600)")
    parser.add_argument("--days", type=int, default=28, help="days of slots from today, 1-60 (default 28)")
    parser.add_argument("--slot-minutes", type=int, default=5, help="slot length in minutes (default 5)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"plan file to write (default {DEFAULT_OUT})")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not 1 <= args.providers <= 50:
        parser.error("--providers must be between 1 and 50")
    if args.patients < args.providers:
        parser.error("--patients must be at least --providers")
    if not 1 <= args.days <= 60:
        parser.error("--days must be between 1 and 60")
    if not 1 <= args.slot_minutes <= 240:
        parser.error("--slot-minutes must be between 1 and 240")

    missing = [name for name in ADMIN_ENV if not os.environ.get(name)]
    if missing:
        print(f"ERROR: set {' and '.join(missing)} (an existing admin; see scripts/bootstrap_admin.py)", file=sys.stderr)
        return 2

    async def run() -> World:
        async with httpx.AsyncClient(base_url=args.gateway_url.rstrip("/"), timeout=60.0) as client:
            return await build_world(
                client,
                admin_email=os.environ[ADMIN_ENV[0]],
                admin_password=os.environ[ADMIN_ENV[1]],
                providers=args.providers,
                patients=args.patients,
                days=args.days,
                slot_minutes=args.slot_minutes,
            )

    try:
        world = asyncio.run(run())
        write_plan(world, Path(args.out))
    except (SeedError, PlanError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except httpx.TransportError as exc:
        print(f"ERROR: gateway not reachable at {args.gateway_url} ({type(exc).__name__})", file=sys.stderr)
        return 1

    print(f"clinic            : {world.clinic_id}")
    print(f"providers         : {world.providers}")
    print(f"patients (plan)   : {world.patients}")
    print(f"patients (contend): {len(world.contention_patient_ids)}")
    print(f"slots created     : {world.slots_created}")
    print(f"booking plan      : {len(world.plan)} bookings")
    print(f"plan written to   : {args.out} (mode 0600)")
    needed = _required_entries()
    if needed is not None and len(world.plan) < needed:
        print(
            f"NOTE: the smoke + sustained + burst sequence consumes {needed} plan entries and this plan "
            f"holds {len(world.plan)}; add --providers or --days, or reseed between profiles."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
