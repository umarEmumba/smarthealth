#!/usr/bin/env python3
"""Build a demo world for the Phase 2.1 validation UI through the gateway.

Creates, with an existing admin account: a clinic, a department, a provider
with a linked login and weekly templates, a front-desk user, a patient with a
linked login and profile, a few extra staff-created patient profiles (so the
patients list pages at small limits), generated slots (two days back and three
days ahead, so past slots exist for the past-slot check), and - unless
--no-window-slot is given - a SECOND provider with one slot inside the check-in
window (starts 5-35 minutes from now) for the check-in flow.

All data is synthetic (example.com emails, "Synthetic" names) and is left in
place, the project convention. Standard library only.

The admin account is never created here: pass an existing one through the
environment (see scripts/bootstrap_admin.py). The admin password is never
printed. The demo-user password is generated per run (or taken from
DEMO_PASSWORD), printed once to stdout and never written to disk.

This script creates accounts, so it refuses a gateway that is not on
localhost/127.0.0.1/::1 unless --allow-non-local is given.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TextIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_GATEWAY_URL = "http://localhost:8000"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
SLOT_MINUTES = 30
LEAD_MINUTES = 5
MIN_MINUTES_BEFORE_MIDNIGHT = 40
HTTP_TIMEOUT = 30

RATE_LIMIT_HINT = (
    "the gateway rate-limited the seeder (429). Raise the limits and restart the gateway: "
    "GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE=100000 GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000 "
    "docker compose -f infra/docker-compose.yml up -d gateway"
)

EPILOG = """\
environment:
  DEMO_ADMIN_EMAIL / DEMO_ADMIN_PASSWORD   existing admin (falls back to
                                           BOOTSTRAP_ADMIN_EMAIL / BOOTSTRAP_ADMIN_PASSWORD;
                                           create one with scripts/bootstrap_admin.py)
  DEMO_PASSWORD                            password for the demo users (>= 8 chars);
                                           default: generated per run and printed once
  GATEWAY_URL                              gateway base URL (default http://localhost:8000)
"""


class SeedError(Exception):
    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


class Gateway:
    """Tiny urllib client: HTTP errors are returned, not raised."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None

    def call(
        self,
        method: str,
        path: str,
        *,
        step: str,
        body: Any = None,
        query: Mapping[str, str] | None = None,
        expect: tuple[int, ...] = (200, 201),
    ) -> Any:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as resp:  # noqa: S310 - local gateway
                status, raw = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read()
        except (urllib.error.URLError, OSError) as exc:
            raise SeedError(f"step '{step}': cannot reach the gateway at {self.base_url} ({type(exc).__name__})") from exc
        if status == 429:
            raise SeedError(RATE_LIMIT_HINT)
        if status not in expect:
            raise SeedError(f"step '{step}' failed with HTTP {status}")
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed a synthetic demo world for the Phase 2.1 validation UI (local gateway only).",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--gateway-url", help="gateway base URL (default env GATEWAY_URL else http://localhost:8000)")
    parser.add_argument("--timezone", default="Asia/Karachi", help="clinic IANA timezone (default Asia/Karachi)")
    parser.add_argument("--extra-patients", type=int, default=6, help="staff-created patient profiles without a login (default 6)")
    parser.add_argument("--no-window-slot", action="store_true", help="skip the second provider and the check-in-window slot")
    parser.add_argument("--allow-non-local", action="store_true", help="allow a gateway host other than localhost/127.0.0.1/::1")
    return parser


def _window(now_local: datetime) -> tuple[datetime, datetime] | None:
    """(start, end) of the next half-hour grid slot at least 5 minutes away, or
    None when a weekly template could not hold it (it cannot cross midnight)."""
    midnight = datetime.combine(now_local.date() + timedelta(days=1), datetime.min.time(), tzinfo=now_local.tzinfo)
    if midnight - now_local < timedelta(minutes=MIN_MINUTES_BEFORE_MIDNIGHT):
        return None
    earliest = (now_local + timedelta(minutes=LEAD_MINUTES)).replace(second=0, microsecond=0)
    start = earliest.replace(minute=0) + timedelta(minutes=SLOT_MINUTES * -(-earliest.minute // SLOT_MINUTES))
    end = start + timedelta(minutes=SLOT_MINUTES)
    if end >= midnight:
        return None
    return start, end


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(
    argv: list[str] | None = None,
    *,
    now: datetime | None = None,
    out: TextIO | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    out = out if out is not None else sys.stdout
    env = env if env is not None else os.environ
    args = _parser().parse_args(argv)

    def say(text: str = "") -> None:
        print(text, file=out)

    try:
        return _seed(args, say, now, env)
    except SeedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.code


def _seed(args: argparse.Namespace, say: Any, now: datetime | None, env: Mapping[str, str]) -> int:
    admin_email = env.get("DEMO_ADMIN_EMAIL") or env.get("BOOTSTRAP_ADMIN_EMAIL")
    admin_password = env.get("DEMO_ADMIN_PASSWORD") or env.get("BOOTSTRAP_ADMIN_PASSWORD")
    if not admin_email or not admin_password:
        raise SeedError(
            "set DEMO_ADMIN_EMAIL and DEMO_ADMIN_PASSWORD (or BOOTSTRAP_ADMIN_EMAIL / BOOTSTRAP_ADMIN_PASSWORD) "
            "for an existing admin; create one with scripts/bootstrap_admin.py"
        )
    gateway_url = args.gateway_url or env.get("GATEWAY_URL") or DEFAULT_GATEWAY_URL
    host = urllib.parse.urlparse(gateway_url).hostname or ""
    if host not in LOCAL_HOSTS and not args.allow_non_local:
        raise SeedError(
            f"refusing to seed gateway host '{host}': this script creates accounts and must not run against a "
            "shared or production gateway by accident. Pass --allow-non-local to override.",
            code=2,
        )
    try:
        tz = ZoneInfo(args.timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise SeedError(f"unknown timezone '{args.timezone}'", code=2) from exc
    password = env.get("DEMO_PASSWORD") or secrets.token_urlsafe(12)
    if len(password) < 8:
        raise SeedError("DEMO_PASSWORD must be at least 8 characters")

    now_local = (now or datetime.now(timezone.utc)).astimezone(tz)
    today = now_local.date()
    suffix = secrets.token_hex(3)
    gw = Gateway(gateway_url)

    login = gw.call("POST", "/api/auth/login", step="admin login", body={"email": admin_email, "password": admin_password}, expect=(200,))
    gw.token = login["access_token"]

    clinic = gw.call("POST", "/api/clinics", step="create clinic", body={"name": f"Validation UI Clinic {suffix}", "timezone": args.timezone})
    clinic_id = clinic["id"]
    department = gw.call("POST", f"/api/clinics/{clinic_id}/departments", step="create department", body={"name": "General"})
    department_id = department["id"]

    def staff(role: str) -> tuple[str, str]:
        email = f"{role.replace('_', '-')}-{suffix}@example.com"
        created = gw.call(
            "POST", "/api/auth/admin/users", step=f"create {role} user",
            body={"email": email, "password": password, "role": role, "clinic_id": clinic_id},
        )
        return email, created["id"]

    provider_email, provider_user_id = staff("provider")
    provider = gw.call(
        "POST", "/api/providers", step="create provider profile",
        body={"department_id": department_id, "full_name": "Dr. Validation Synthetic", "specialties": ["General"], "user_id": provider_user_id},
    )
    provider_id = provider["id"]
    for day_of_week in range(7):
        gw.call(
            "POST", f"/api/providers/{provider_id}/availability", step="create availability template",
            body={
                "day_of_week": day_of_week, "start_time": "09:00:00", "end_time": "12:00:00",
                "slot_duration_minutes": SLOT_MINUTES, "effective_from": "2020-01-01",
            },
        )

    front_desk_email, _ = staff("front_desk")

    patient_email = f"patient-{suffix}@example.com"
    account = gw.call(
        "POST", "/api/auth/register", step="register patient account",
        body={"email": patient_email, "password": password, "clinic_id": clinic_id},
    )
    patient = gw.call(
        "POST", "/api/patients", step="create patient profile",
        body={"clinic_id": clinic_id, "full_name": "Zzyzx Validation Synthetic", "date_of_birth": "1990-01-01", "user_id": account["id"]},
    )
    for index in range(max(0, args.extra_patients)):
        gw.call(
            "POST", "/api/patients", step="create extra patient profile",
            body={"clinic_id": clinic_id, "full_name": f"Zzyzx Extra Synthetic {index + 1} {suffix}", "date_of_birth": "1990-01-01"},
        )

    gw.call(
        "POST", "/api/slots/generate", step="generate slots",
        body={"provider_id": provider_id, "from_date": (today - timedelta(days=2)).isoformat(), "to_date": (today + timedelta(days=3)).isoformat()},
        expect=(200,),
    )

    window: dict[str, str] = {}
    window_warning = ""
    if not args.no_window_slot:
        span = _window(now_local)
        if span is None:
            window_warning = (
                "WARNING: fewer than 40 minutes remain before clinic-local midnight, so no check-in-window slot "
                "was built (a weekly template cannot cross midnight). Re-run this script after midnight."
            )
        else:
            start, end = span
            second = gw.call(
                "POST", "/api/providers", step="create window provider",
                body={"department_id": department_id, "full_name": "Dr. Window Synthetic", "specialties": ["General"]},
            )
            window_provider_id = second["id"]
            gw.call(
                "POST", f"/api/providers/{window_provider_id}/availability", step="create window template",
                body={
                    "day_of_week": now_local.weekday(), "start_time": start.time().isoformat(), "end_time": end.time().isoformat(),
                    "slot_duration_minutes": SLOT_MINUTES, "effective_from": today.isoformat(),
                },
            )
            gw.call(
                "POST", "/api/slots/generate", step="generate window slots",
                body={"provider_id": window_provider_id, "from_date": today.isoformat(), "to_date": today.isoformat()},
                expect=(200,),
            )
            found = gw.call(
                "GET", "/api/slots", step="find window slot",
                query={
                    "provider_id": window_provider_id,
                    "start": _utc_iso(start - timedelta(minutes=1)),
                    "end": _utc_iso(end + timedelta(minutes=1)),
                    "status": "available",
                },
                expect=(200,),
            )
            if not found:
                raise SeedError("step 'find window slot' returned no slot")
            slot = sorted(found, key=lambda s: s["start_time"])[0]
            window = {
                "slot_id": slot["id"], "window_provider_id": window_provider_id,
                "window_slot_start": _utc_iso(start), "window_slot_end": _utc_iso(end),
            }
            window["_local"] = f"{start.strftime('%Y-%m-%d %H:%M')} - {end.strftime('%H:%M')} {args.timezone}"

    say("Demo world created (synthetic data, left in place).")
    say()
    say(f"{'role':<12} {'email':<44} password")
    for role, email in (("provider", provider_email), ("front_desk", front_desk_email), ("patient", patient_email)):
        say(f"{role:<12} {email:<44} {password}")
    say()
    say(f"clinic_id:     {clinic_id}")
    say(f"department_id: {department_id}")
    say(f"provider_id:   {provider_id}")
    say(f"patient_id:    {patient['id']}")
    if window:
        say()
        say(f"window slot (UTC):   {window['window_slot_start']} - {window['window_slot_end']}")
        say(f"window slot (local): {window['_local']}")
        say("check-in opens 60 minutes before the start and closes at the end; re-run this script for a fresh window slot")
    if window_warning:
        say()
        say(window_warning)
    context: dict[str, str] = {
        "clinic_id": clinic_id, "department_id": department_id, "provider_id": provider_id, "patient_id": patient["id"],
    }
    context.update({k: v for k, v in window.items() if not k.startswith("_")})
    say()
    say("CONTEXT_JSON: " + json.dumps(context))
    say()
    say("Log into the UI as the admin account you used here for the admin-only panels (clinics, providers, templates).")
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
