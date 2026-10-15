"""The concurrent-booking demo script works end to end (plan 02-12, roadmap SC2 illustration).

Runs `scripts/demo_concurrent_booking.py --parallel 20 --json` as a subprocess
against the published gateway, with a freshly seeded admin, and checks the
single-winner summary. 20 parallel bookings stay inside the gateway's
default limit; after other e2e suites the auth budget (20/min) needs the
GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE override described in the script.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from tests.conftest import insert_admin_user, unique_email
from tests.e2e.phase2_stack import gateway_url, require_stack

pytestmark = pytest.mark.e2e

REPO_ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "correct-horse-battery"


async def test_demo_prints_one_winner() -> None:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=10.0) as http:
        await require_stack(http)

    email = unique_email("demo-admin")
    await insert_admin_user(email, PASSWORD)
    env = {
        **os.environ,
        "SMARTHEALTH_GATEWAY_URL": gateway_url(),
        "BOOKING_DEMO_ADMIN_EMAIL": email,
        "BOOKING_DEMO_ADMIN_PASSWORD": PASSWORD,
    }
    done = subprocess.run(
        [sys.executable, "scripts/demo_concurrent_booking.py", "--parallel", "20", "--json"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    summary = json.loads(done.stdout.strip().splitlines()[-1])
    assert {k: summary[k] for k in ("parallel", "accepted", "conflicts", "other", "winner_status", "active_for_slot")} == {
        "parallel": 20,
        "accepted": 1,
        "conflicts": 19,
        "other": 0,
        "winner_status": "confirmed",
        "active_for_slot": 1,
    }
