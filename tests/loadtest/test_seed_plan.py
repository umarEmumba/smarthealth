"""`scripts/loadtest_seed.py`: the booking plan (pure) and the world (through the gateway).

The plan tests are unit/property tests over random slot grids. The world test runs
`build_world` against the in-process gateway (integration: live Postgres, Mongo and
Redis, no Temporal needed because nothing is booked). The CLI tests run the script
as a subprocess and assert the exit codes the plan names.
"""

from __future__ import annotations

import importlib.util
import json
import os
import random
import stat
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.conftest import insert_admin_user, unique_email

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "loadtest_seed.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("loadtest_seed", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["loadtest_seed"] = module
    spec.loader.exec_module(module)
    return module


seed = _load_script()

BASE = datetime(2030, 1, 1, 6, 0, tzinfo=timezone.utc)


def _grid(rng: random.Random, providers: int, instants: int, minutes: int = 5) -> list[dict]:
    """Random slot grid: each provider offers a random subset of `instants` consecutive starts."""
    provider_ids = [str(uuid.UUID(int=rng.getrandbits(128))) for _ in range(providers)]
    slots = []
    for provider_id in provider_ids:
        for index in range(instants):
            if rng.random() < 0.85:  # gaps are normal
                start = BASE + timedelta(minutes=minutes * index)
                slots.append(
                    {
                        "id": str(uuid.UUID(int=rng.getrandbits(128))),
                        "provider_id": provider_id,
                        "start_time": start.isoformat(),
                        "end_time": (start + timedelta(minutes=minutes)).isoformat(),
                    }
                )
    rng.shuffle(slots)
    return slots


def _patients(count: int) -> list[str]:
    return [str(uuid.UUID(int=index + 1)) for index in range(count)]


def _overlapping_pairs(slots: list[dict], plan: list[dict]) -> int:
    by_slot = {slot["id"]: slot for slot in slots}
    per_patient: dict[str, list[tuple[datetime, datetime]]] = {}
    for entry in plan:
        slot = by_slot[entry["slot_id"]]
        per_patient.setdefault(entry["patient_id"], []).append(
            (datetime.fromisoformat(slot["start_time"]), datetime.fromisoformat(slot["end_time"]))
        )
    overlaps = 0
    for intervals in per_patient.values():
        intervals.sort()
        for (_s1, e1), (s2, _e2) in zip(intervals, intervals[1:], strict=False):
            if s2 < e1:
                overlaps += 1
    return overlaps


@pytest.mark.parametrize("seed_value", range(25))
def test_random_grids_cover_every_slot_once_with_zero_patient_overlap(seed_value: int) -> None:
    rng = random.Random(seed_value)
    providers = rng.randint(1, 6)
    slots = _grid(rng, providers, instants=rng.randint(1, 40))
    if not slots:
        pytest.skip("empty random grid")
    patients = _patients(rng.randint(providers, providers * 4))

    plan = seed.build_booking_plan(slots, patients)

    assert sorted(e["slot_id"] for e in plan) == sorted(s["id"] for s in slots)  # each slot exactly once
    assert len({e["slot_id"] for e in plan}) == len(slots)
    assert _overlapping_pairs(slots, plan) == 0
    assert {e["patient_id"] for e in plan} <= set(patients)


@pytest.mark.parametrize("seed_value", range(10))
def test_simultaneous_slots_get_distinct_patients(seed_value: int) -> None:
    rng = random.Random(1000 + seed_value)
    providers = rng.randint(2, 6)
    slots = _grid(rng, providers, instants=rng.randint(2, 30))
    plan = seed.build_booking_plan(slots, _patients(providers + rng.randint(0, 5)))

    by_slot = {s["id"]: s for s in slots}
    by_instant: dict[str, list[str]] = {}
    for entry in plan:
        by_instant.setdefault(by_slot[entry["slot_id"]]["start_time"], []).append(entry["patient_id"])
    for instant, patient_ids in by_instant.items():
        assert len(patient_ids) == len(set(patient_ids)), f"patient repeated at {instant}"


def test_plan_is_deterministic_and_independent_of_input_order() -> None:
    rng = random.Random(7)
    slots = _grid(rng, 4, instants=30)
    patients = _patients(10)
    first = seed.build_booking_plan(slots, patients)
    shuffled = list(slots)
    random.Random(99).shuffle(shuffled)
    assert seed.build_booking_plan(slots, patients) == first
    assert seed.build_booking_plan(shuffled, patients) == first


def test_plan_follows_start_time_order() -> None:
    slots = _grid(random.Random(3), 3, instants=20)
    plan = seed.build_booking_plan(slots, _patients(6))
    by_slot = {s["id"]: s for s in slots}
    starts = [datetime.fromisoformat(by_slot[e["slot_id"]]["start_time"]) for e in plan]
    assert starts == sorted(starts)


def test_fewer_patients_than_providers_is_a_clear_error() -> None:
    slots = _grid(random.Random(5), 4, instants=10)
    with pytest.raises(seed.PlanError, match="at least one patient per provider"):
        seed.build_booking_plan(slots, _patients(3))


def test_mixed_slot_lengths_still_never_overlap_a_patient() -> None:
    """The overlap guard does not rely on a shared 5-minute grid."""
    rng = random.Random(11)
    slots = []
    for index in range(60):
        minutes = rng.choice([5, 10, 15])
        start = BASE + timedelta(minutes=5 * index)
        slots.append(
            {
                "id": str(uuid.UUID(int=index + 1)),
                "provider_id": str(uuid.UUID(int=1000 + index % 3)),
                "start_time": start.isoformat(),
                "end_time": (start + timedelta(minutes=minutes)).isoformat(),
            }
        )
    plan = seed.build_booking_plan(slots, _patients(12))
    assert _overlapping_pairs(slots, plan) == 0
    assert len(plan) == len(slots)


def test_no_free_patient_is_an_error_not_a_silent_overlap() -> None:
    start = BASE
    slots = [
        {
            "id": str(uuid.UUID(int=i + 1)),
            "provider_id": str(uuid.UUID(int=100 + i)),
            "start_time": start.isoformat(),
            "end_time": (start + timedelta(minutes=5)).isoformat(),
        }
        for i in range(2)
    ]
    # two providers share one instant but only one patient exists
    with pytest.raises(seed.PlanError):
        seed.build_booking_plan(slots, _patients(1))


def test_empty_slots_give_an_empty_plan() -> None:
    assert seed.build_booking_plan([], _patients(3)) == []


# --- the CLI ------------------------------------------------------------------


def _run_script(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    clean = {k: v for k, v in os.environ.items() if not k.startswith("LOADTEST_")}
    clean.update(env or {})
    return subprocess.run(  # noqa: S603 - fixed argv, repo script
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        env=clean,
        timeout=60,
        check=False,
    )


def test_cli_help_exits_zero_and_names_every_flag() -> None:
    result = _run_script("--help")
    assert result.returncode == 0
    for flag in ("--gateway-url", "--providers", "--patients", "--days", "--slot-minutes", "--out"):
        assert flag in result.stdout


@pytest.mark.parametrize(
    "env, missing",
    [
        ({}, "LOADTEST_ADMIN_EMAIL"),
        ({"LOADTEST_ADMIN_EMAIL": "a@example.com"}, "LOADTEST_ADMIN_PASSWORD"),
        ({"LOADTEST_ADMIN_PASSWORD": "x"}, "LOADTEST_ADMIN_EMAIL"),
    ],
)
def test_cli_refuses_without_admin_credentials_with_exit_2_and_no_traceback(env, missing) -> None:
    result = _run_script(env=env)
    assert result.returncode == 2
    assert missing in result.stderr
    assert "Traceback" not in result.stderr
    assert result.stdout == ""


def test_cli_rejects_nonsense_flags_with_exit_2() -> None:
    result = _run_script("--providers", "0", env={"LOADTEST_ADMIN_EMAIL": "a@example.com", "LOADTEST_ADMIN_PASSWORD": "x"})
    assert result.returncode == 2
    assert "Traceback" not in result.stderr


def test_unreachable_gateway_is_exit_1_and_never_prints_the_password(tmp_path: Path) -> None:
    password = "never-print-this-" + uuid.uuid4().hex
    result = _run_script(
        "--gateway-url", "http://127.0.0.1:9", "--out", str(tmp_path / "plan.json"),
        env={"LOADTEST_ADMIN_EMAIL": "a@example.com", "LOADTEST_ADMIN_PASSWORD": password},
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert password not in result.stdout + result.stderr
    assert not (tmp_path / "plan.json").exists()


def test_plan_file_is_written_with_mode_0600_even_over_an_existing_file(tmp_path: Path) -> None:
    out = tmp_path / "data" / "plan.json"
    out.parent.mkdir()
    out.write_text("{}")
    out.chmod(0o644)
    world = seed.World(
        clinic_id="c",
        front_desk_email="f@example.com",
        front_desk_password="pw",
        plan=[{"slot_id": "s", "patient_id": "p"}],
        contention_slot={"slot_id": "x", "provider_id": "y", "slot_start": "a", "slot_end": "b"},
        contention_patient_ids=["q"],
    )
    seed.write_plan(world, out)
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    body = json.loads(out.read_text())
    assert body["front_desk"] == {"email": "f@example.com", "password": "pw"}
    assert body["plan"] == [{"slot_id": "s", "patient_id": "p"}]
    assert body["contention"]["patient_ids"] == ["q"]


def test_script_creates_only_synthetic_names_and_reads_no_other_env() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert "Zzyzx Load" in source and "Load Provider" in source and "example.com" in source
    assert "os.environ" in source
    # credentials come from the two LOADTEST_ variables only
    assert source.count("LOADTEST_ADMIN_EMAIL") >= 1 and source.count("LOADTEST_ADMIN_PASSWORD") >= 1


# --- the world, through the in-process gateway --------------------------------


@pytest.mark.integration
async def test_build_world_creates_clinic_providers_patients_and_a_collision_free_plan(client) -> None:
    admin_email, admin_password = unique_email("loadtest-admin"), "correct-horse-battery"
    await insert_admin_user(admin_email, admin_password)

    world = await seed.build_world(
        client,
        admin_email=admin_email,
        admin_password=admin_password,
        providers=2,
        patients=6,
        days=2,
        slot_minutes=5,
        contention_patients=4,
        min_lead_minutes=0,
    )

    # 06:00-22:00 in 5-minute slots is 192 a day per provider; two days, two providers.
    assert world.slots_created == 2 * 2 * 192
    assert world.slots_listed == world.slots_created
    assert world.providers == 2 and world.patients == 6
    assert len(world.contention_patient_ids) == 4

    # Every listed future slot is in the plan once, except the single contention slot.
    planned = [entry["slot_id"] for entry in world.plan]
    assert len(planned) == len(set(planned))
    assert world.contention_slot["slot_id"] not in planned
    assert len(planned) + 1 <= world.slots_listed

    # The front-desk account in the plan really logs in, scoped to the new clinic.
    login = await client.post(
        "/api/auth/login",
        json={"email": world.front_desk_email, "password": world.front_desk_password},
    )
    assert login.status_code == 200, login.text
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    # The plan's patients exist in the clinic (staff can read them).
    some = world.plan[0]["patient_id"]
    patient = await client.get(
        f"/api/patients/{some}", params={"clinic_id": world.clinic_id}, headers=headers
    )
    assert patient.status_code == 200, patient.text
    assert patient.json()["clinic_id"] == world.clinic_id
    assert patient.json()["full_name"].startswith("Zzyzx Load")

    # The contention slot is a real, still-available slot.
    slot_start = datetime.fromisoformat(world.contention_slot["slot_start"])
    found = await client.get(
        "/api/slots",
        params={
            "provider_id": world.contention_slot["provider_id"],
            "start": (slot_start - timedelta(minutes=1)).isoformat(),
            "end": (slot_start + timedelta(minutes=1)).isoformat(),
        },
        headers=headers,
    )
    assert found.status_code == 200, found.text
    assert world.contention_slot["slot_id"] in {s["id"] for s in found.json()}
