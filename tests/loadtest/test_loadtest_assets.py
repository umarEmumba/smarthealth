"""Static checks on the load-test assets (plan 02.1-04, D2; no Docker needed).

They fail when the k6 script copies a number instead of reading `nfr.json`, when a
profile or endpoint tag goes missing, when the runner uses an unpinned image, or
when the README stops naming what an operator must do. `run.sh` is also executed
against a fake `docker` so its argument handling and exit-code pass-through are
proven without a daemon.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LOADTEST = REPO_ROOT / "loadtest"
BOOKING_JS = LOADTEST / "booking.js"
RUN_SH = LOADTEST / "run.sh"
README = LOADTEST / "README.md"
NFR_JSON = LOADTEST / "nfr.json"

PROFILES = ("smoke", "sustained", "burst", "contention")
ENDPOINT_TAGS = ("book", "read_appointment", "read_slots")


def _code(path: Path) -> str:
    """Source without `//` comments (a comment may quote a number; code must not)."""
    return "\n".join(re.sub(r"(?<!:)//.*$", "", line) for line in path.read_text(encoding="utf-8").splitlines())


def _function_body(code: str, name: str) -> str:
    start = code.index(f"function {name}(")
    depth, i = 0, code.index("{", start)
    begin = i
    while True:
        depth += {"{": 1, "}": -1}.get(code[i], 0)
        i += 1
        if depth == 0:
            return code[begin:i]


def test_booking_script_reads_the_single_source() -> None:
    code = _code(BOOKING_JS)
    assert re.search(r"JSON\.parse\(open\('\./nfr\.json'\)\)", code)
    assert "data/plan.json" in code and "SharedArray" in code


@pytest.mark.parametrize("profile", PROFILES)
def test_every_profile_is_defined_and_selected_by_env(profile: str) -> None:
    code = _code(BOOKING_JS)
    assert re.search(rf"\b{profile}\b\s*:", code), f"scenario {profile}"
    assert "__ENV.PROFILE" in code
    assert f"'{profile}'" in code


@pytest.mark.parametrize("tag", ENDPOINT_TAGS)
def test_every_endpoint_tag_is_used(tag: str) -> None:
    assert f"endpoint: '{tag}'" in _code(BOOKING_JS)


def test_thresholds_cover_each_key_family_from_nfr_json() -> None:
    code = _code(BOOKING_JS)
    booking = _function_body(code, "bookingThresholds")
    for needle, keys in (
        ("http_req_duration{endpoint:book}", ("book_p95_ms", "book_p99_ms")),
        ("http_req_duration{endpoint:read_appointment}", ("read_p95_ms", "read_p99_ms")),
        ("http_req_duration{endpoint:read_slots}", ("read_p95_ms", "read_p99_ms")),
        ("booking_confirm_seconds", ("confirm_p95_s", "confirm_p99_s")),
    ):
        assert needle in booking, needle
        for key in keys:
            assert f"NFR.{key}" in booking, f"{needle} must use NFR.{key}"
    # the failed-rate threshold lives in the shared constant and uses error_rate_max
    assert "http_req_failed{endpoint:book}" in code
    assert booking.count("NFR.error_rate_max") == 1
    assert "NFR.error_rate_max" in _function_body(code, "contentionThresholds")
    assert "contention_violations" in _function_body(code, "contentionThresholds")


def test_threshold_blocks_hold_no_numeric_literals() -> None:
    """Inside the threshold builders every number must come from NFR.*; the only digits
    allowed are the percentile names and `count==0`."""
    code = _code(BOOKING_JS)
    for name in ("bookingThresholds", "contentionThresholds"):
        body = _function_body(code, name)
        body = re.sub(r"\$\{[^}]*\}", "", body)  # template expressions are the NFR reads
        body = body.replace("p(95)", "").replace("p(99)", "").replace("count==0", "")
        assert not re.search(r"\d", body), f"{name} contains a hard-coded number: {body!r}"


def test_scenario_rates_and_durations_come_from_nfr_json() -> None:
    code = _code(BOOKING_JS)
    for key in ("sustained_rps", "sustained_seconds", "burst_rps", "burst_seconds", "contention_requests"):
        assert f"NFR.{key}" in code, key


def test_no_nfr_value_is_copied_into_the_script() -> None:
    targets = json.loads(NFR_JSON.read_text(encoding="utf-8"))
    code = re.sub(r"\$\{[^}]*\}", "", _code(BOOKING_JS))
    # values distinctive enough that a literal in the script can only be a copy
    for key in ("book_p95_ms", "book_p99_ms", "read_p99_ms", "sustained_seconds"):
        value = targets[key]
        assert not re.search(rf"(?<![\w.]){value}(?![\w.])", code), f"{key}={value} is hard-coded in booking.js"
    assert not re.search(r"(?<![\w.])99\.9(?![\w.])", code)


def test_script_never_logs_a_token_or_password() -> None:
    for line in _code(BOOKING_JS).splitlines():
        if "console." in line or "fail(" in line:
            lowered = line.lower()
            assert "token" not in lowered and "password" not in lowered and "authorization" not in lowered, line


def test_script_has_valid_syntax(tmp_path: Path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not installed; k6 syntax is checked when the runner executes")
    work = tmp_path / "booking.mjs"  # syntax only: the k6/* imports are not resolved by --check
    work.write_text(BOOKING_JS.read_text(encoding="utf-8"), encoding="utf-8")
    done = subprocess.run([node, "--check", str(work)], capture_output=True, text=True, timeout=30, check=False)  # noqa: S603
    assert done.returncode == 0, done.stderr


# --- run.sh -------------------------------------------------------------------


def test_runner_uses_docker_run_with_a_pinned_tag_and_host_gateway() -> None:
    text = RUN_SH.read_text(encoding="utf-8")
    assert "docker run" in text
    tags = re.findall(r"grafana/k6:([\w.\-]+)", text)
    assert tags, "an explicit k6 tag is required"
    for tag in tags:
        assert tag != "latest" and re.fullmatch(r"\d+\.\d+\.\d+", tag), tag
    assert "grafana/k6:latest" not in text
    assert "host.docker.internal" in text and "host-gateway" in text
    assert ":/loadtest:ro" in text  # the repo's loadtest dir is mounted read-only
    assert text.startswith("#!/bin/sh")
    assert os.access(RUN_SH, os.X_OK) and RUN_SH.stat().st_mode & stat.S_IXUSR


@pytest.fixture
def runner_sandbox(tmp_path: Path) -> dict[str, Path]:
    """A copy of loadtest/ plus a fake `docker` that records its arguments."""
    copy = tmp_path / "loadtest"
    copy.mkdir()
    shutil.copy(RUN_SH, copy / "run.sh")
    (copy / "data").mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    record = tmp_path / "docker-args.txt"
    fake = bin_dir / "docker"
    fake.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "{record}"\nexit "${{FAKE_DOCKER_EXIT:-0}}"\n')
    fake.chmod(0o755)
    return {"dir": copy, "bin": bin_dir, "record": record}


def _run(sandbox: dict[str, Path], *args: str, **env: str) -> subprocess.CompletedProcess[str]:
    base = {k: v for k, v in os.environ.items() if k not in ("GATEWAY_URL", "PLAN_OFFSET")}
    full_env = {**base, "PATH": f"{sandbox['bin']}:{os.environ['PATH']}", **env}
    return subprocess.run(  # noqa: S603
        ["/bin/sh", str(sandbox["dir"] / "run.sh"), *args],
        capture_output=True, text=True, env=full_env, timeout=30, check=False,
    )


@pytest.mark.parametrize("args", [(), ("nope",), ("smoke", "extra")])
def test_runner_rejects_bad_usage_with_exit_2(runner_sandbox, args) -> None:
    (runner_sandbox["dir"] / "data" / "plan.json").write_text("{}")
    done = _run(runner_sandbox, *args)
    assert done.returncode == 2
    assert "usage" in done.stderr
    assert not runner_sandbox["record"].exists()


def test_runner_refuses_without_a_plan_file(runner_sandbox) -> None:
    done = _run(runner_sandbox, "smoke")
    assert done.returncode == 2 and "plan.json" in done.stderr
    assert not runner_sandbox["record"].exists()


@pytest.mark.parametrize("profile", PROFILES)
def test_runner_passes_the_profile_and_gateway_url_and_mounts_the_dir(runner_sandbox, profile) -> None:
    (runner_sandbox["dir"] / "data" / "plan.json").write_text("{}")
    done = _run(runner_sandbox, profile)
    assert done.returncode == 0, done.stderr
    args = runner_sandbox["record"].read_text().splitlines()
    assert args[:2] == ["run", "--rm"]
    assert f"PROFILE={profile}" in args
    assert "GATEWAY_URL=http://host.docker.internal:8000" in args
    assert any(a.startswith("grafana/k6:") for a in args)
    assert args[-2:] == ["run", "/loadtest/booking.js"]
    mounts = [a for a in args if a.endswith(":/loadtest:ro")]
    assert len(mounts) == 1
    assert os.path.realpath(mounts[0].split(":/loadtest:ro")[0]) == os.path.realpath(runner_sandbox["dir"])
    assert not any(a.startswith("PLAN_OFFSET=") for a in args)


def test_runner_forwards_gateway_url_and_plan_offset(runner_sandbox) -> None:
    (runner_sandbox["dir"] / "data" / "plan.json").write_text("{}")
    done = _run(runner_sandbox, "burst", GATEWAY_URL="http://example.test:9000", PLAN_OFFSET="150")
    assert done.returncode == 0, done.stderr
    args = runner_sandbox["record"].read_text().splitlines()
    assert "GATEWAY_URL=http://example.test:9000" in args and "PLAN_OFFSET=150" in args


def test_runner_exits_with_k6s_exit_code(runner_sandbox) -> None:
    (runner_sandbox["dir"] / "data" / "plan.json").write_text("{}")
    assert _run(runner_sandbox, "smoke", FAKE_DOCKER_EXIT="99").returncode == 99  # thresholds crossed
    assert _run(runner_sandbox, "smoke", FAKE_DOCKER_EXIT="0").returncode == 0


# --- README -------------------------------------------------------------------


def test_readme_covers_profiles_budgets_and_the_pinned_image() -> None:
    text = README.read_text(encoding="utf-8")
    for profile in PROFILES:
        assert profile in text, profile
    for needle in (
        "GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE",
        "GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE",
        "scripts/bootstrap_admin.py",
        "scripts/loadtest_seed.py",
        "docs/nfr.md",
        "12 minutes",
        "docker manifest inspect",
        "local",
    ):
        assert needle in text, needle
    tag = re.search(r"grafana/k6:(\d+\.\d+\.\d+)", RUN_SH.read_text(encoding="utf-8")).group(1)
    assert f"grafana/k6:{tag}" in text, "the README must record the pinned tag run.sh uses"
    assert re.search(r"verified[^\n]*\d{4}-\d{2}-\d{2}", text), "the README records the verification date"
