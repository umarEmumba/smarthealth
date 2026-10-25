"""The k6 smoke profile runs against the real stack and passes its thresholds (plan 02.1-04, D2).

Needs Docker (the k6 image runs in a container) and the full Compose stack with
the gateway budget raised (see loadtest/README.md). It SKIPS, never fails, when
either is missing, unless `SMARTHEALTH_E2E_REQUIRE_STACK=1` is set. It seeds a
small world through the published gateway with the seed script's own functions,
copies `loadtest/` into a temporary directory (so a developer's real
`loadtest/data/plan.json` is never touched) and runs `run.sh smoke`.

Informational only: the smoke profile is plumbing plus the NFR thresholds on a
small stack. It is not a claim that NFR-01/NFR-02 are met (measurement is Phase 3).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from tests.conftest import insert_admin_user, unique_email
from tests.e2e.phase2_stack import gateway_url, require_stack

pytestmark = pytest.mark.e2e

REPO_ROOT = Path(__file__).resolve().parents[2]
LOADTEST = REPO_ROOT / "loadtest"
PASSWORD = "correct-horse-battery"
DOCKER_FAILURE_CODES = {125, 126, 127}  # the docker client itself failed (daemon, pull), not k6


def _require(condition: bool, reason: str) -> None:
    if condition:
        return
    if os.environ.get("SMARTHEALTH_E2E_REQUIRE_STACK") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


def _docker_ready() -> bool:
    if shutil.which("docker") is None:
        return False
    done = subprocess.run(  # noqa: S603, S607 - fixed argv
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        timeout=30,
        check=False,
    )
    return done.returncode == 0


def _seed_module():
    spec = importlib.util.spec_from_file_location("loadtest_seed_e2e", REPO_ROOT / "scripts" / "loadtest_seed.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["loadtest_seed_e2e"] = module
    spec.loader.exec_module(module)
    return module


def _container_url(url: str) -> str:
    """The gateway as the k6 container reaches it (the host's localhost is host.docker.internal)."""
    for local in ("localhost", "127.0.0.1"):
        url = url.replace(f"//{local}", "//host.docker.internal")
    return url


async def test_k6_smoke_profile_passes_against_the_stack(tmp_path: Path) -> None:
    _require(_docker_ready(), "docker is not available; the k6 smoke test needs it")
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=10.0) as probe:
        await require_stack(probe)

    seed = _seed_module()
    email = unique_email("loadtest-admin")
    await insert_admin_user(email, PASSWORD)
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=60.0) as http:
        # Two providers over two days leave at least 190 future slots at any hour,
        # more than the 150 bookings the smoke profile makes.
        world = await seed.build_world(
            http,
            admin_email=email,
            admin_password=PASSWORD,
            providers=2,
            patients=20,
            days=2,
            slot_minutes=5,
            contention_patients=2,
        )
    assert len(world.plan) >= 150, f"plan too small for the smoke profile: {len(world.plan)}"

    sandbox = tmp_path / "loadtest"
    sandbox.mkdir()
    for name in ("booking.js", "nfr.json", "run.sh"):
        shutil.copy(LOADTEST / name, sandbox / name)
    (sandbox / "data").mkdir()
    seed.write_plan(world, sandbox / "data" / "plan.json")
    # The container user needs to read the 0600 plan file; run.sh passes --user for that.
    assert json.loads((sandbox / "data" / "plan.json").read_text())["plan"]

    env = {**os.environ, "GATEWAY_URL": _container_url(gateway_url())}
    done = await asyncio.to_thread(
        subprocess.run,
        [str(sandbox / "run.sh"), "smoke"],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = done.stdout + done.stderr
    if done.returncode in DOCKER_FAILURE_CODES:
        _require(False, f"docker could not run the k6 image (exit {done.returncode}): {output[-300:]}")
    assert done.returncode == 0, output[-3000:]
