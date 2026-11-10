"""Tests for scripts/generate_infra_secrets.py (plan 03-03, T-3-03)."""

from __future__ import annotations

import importlib.util
import stat
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "generate_infra_secrets.py"
FILES = ("debezium.properties", "grafana_ro.secret", "notification_broker.secret")


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("generate_infra_secrets", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def script() -> ModuleType:
    return _load()


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_writes_three_files_with_mode_0600_in_a_0700_directory(
    script: ModuleType, tmp_path: Path
) -> None:
    out = tmp_path / "infra"
    assert script.main(["--output-dir", str(out)]) == 0
    assert _mode(out) == 0o700
    for name in FILES:
        assert (out / name).is_file()
        assert _mode(out / name) == 0o600, name


def test_content_formats(script: ModuleType, tmp_path: Path) -> None:
    out = tmp_path / "infra"
    assert script.main(["--output-dir", str(out)]) == 0
    debezium = (out / "debezium.properties").read_text()
    assert debezium.count("\n") == 1 and debezium.endswith("\n")
    assert debezium.startswith("password=") and len(debezium) > len("password=") + 30
    values = {(out / name).read_text() for name in FILES[1:]}
    assert all(len(value) >= 32 and "\n" not in value for value in values)
    # Three independent credentials: no value is reused across files.
    assert len(values | {debezium.removeprefix("password=").strip()}) == 3


def test_stdout_prints_paths_and_never_a_secret_value(
    script: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "infra"
    assert script.main(["--output-dir", str(out)]) == 0
    captured = capsys.readouterr()
    text = captured.out + captured.err
    for name in FILES:
        assert str(out / name) in text
        value = (out / name).read_text().removeprefix("password=").strip()
        assert value not in text


def test_refuses_to_overwrite_without_force_and_writes_nothing(
    script: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "infra"
    out.mkdir(mode=0o700)
    existing = out / "grafana_ro.secret"
    existing.write_text("keep-me")
    assert script.main(["--output-dir", str(out)]) == 1
    assert existing.read_text() == "keep-me"
    assert not (out / "debezium.properties").exists()
    assert not (out / "notification_broker.secret").exists()
    assert "refusing to overwrite" in capsys.readouterr().err


def test_second_run_without_force_leaves_every_file_untouched(
    script: ModuleType, tmp_path: Path
) -> None:
    out = tmp_path / "infra"
    assert script.main(["--output-dir", str(out)]) == 0
    before = {name: (out / name).read_text() for name in FILES}
    assert script.main(["--output-dir", str(out)]) == 1
    assert {name: (out / name).read_text() for name in FILES} == before


def test_force_replaces_the_files_and_keeps_mode_0600(script: ModuleType, tmp_path: Path) -> None:
    out = tmp_path / "infra"
    assert script.main(["--output-dir", str(out)]) == 0
    before = {name: (out / name).read_text() for name in FILES}
    (out / "debezium.properties").chmod(0o644)  # a loosened file is tightened again
    assert script.main(["--output-dir", str(out), "--force"]) == 0
    for name in FILES:
        assert (out / name).read_text() != before[name], name
        assert _mode(out / name) == 0o600, name
