"""Prometheus multiprocess mode for the gunicorn APIs (plan 03-24, CR-02, D-14).

Hook behaviour of `libs.observability.gunicorn_conf`, the aggregation of the shared
exposition helper, and a static check that each API image's default command keeps two
workers, exports the directory variable (only there), and passes the gunicorn config.
Plan 03-25 extends `API_DOCKERFILES` to all nine APIs.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from prometheus_client import REGISTRY
from prometheus_client.mmap_dict import MmapedDict, mmap_key

from libs.observability import gunicorn_conf
from libs.observability.http_metrics import exposition_registry, render_latest

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV = "PROMETHEUS_MULTIPROC_DIR"

# service -> (has an Alembic step, app path, binds)
API_DOCKERFILES: dict[str, tuple[bool, str, tuple[str, ...]]] = {
    "scheduling": (True, "services.scheduling.app:app", ("0.0.0.0:8000",)),
    "gateway": (False, "services.gateway.app:app", ("0.0.0.0:8000", "0.0.0.0:9100")),
}


def test_on_starting_creates_and_empties_the_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "multiproc"
    directory.mkdir()
    (directory / "counter_123.db").write_bytes(b"stale")
    (directory / "histogram_456.db").write_bytes(b"stale")
    monkeypatch.setenv(ENV, str(directory))

    gunicorn_conf.on_starting(server=None)

    assert directory.is_dir()
    assert list(directory.iterdir()) == []


def test_on_starting_creates_a_missing_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "does" / "not" / "exist"
    monkeypatch.setenv(ENV, str(directory))

    gunicorn_conf.on_starting(server=None)

    assert directory.is_dir()


def test_on_starting_does_nothing_without_the_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    untouched = tmp_path / "counter_1.db"
    untouched.write_bytes(b"x")
    monkeypatch.chdir(tmp_path)

    gunicorn_conf.on_starting(server=None)

    assert untouched.exists()


def test_child_exit_marks_the_worker_dead_when_the_variable_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV, str(tmp_path))
    seen: list[int] = []
    monkeypatch.setattr(gunicorn_conf.multiprocess, "mark_process_dead", seen.append)

    gunicorn_conf.child_exit(server=None, worker=SimpleNamespace(pid=4242))

    assert seen == [4242]


def test_child_exit_does_nothing_without_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    seen: list[int] = []
    monkeypatch.setattr(gunicorn_conf.multiprocess, "mark_process_dead", seen.append)

    gunicorn_conf.child_exit(server=None, worker=SimpleNamespace(pid=4242))

    assert seen == []


def test_the_config_module_does_not_set_the_variable() -> None:
    source = Path(gunicorn_conf.__file__).read_text()
    assert "os.environ[" not in source
    assert "setdefault" not in source


def _default_command(service: str) -> str:
    lines = (REPO_ROOT / "services" / service / "Dockerfile").read_text().splitlines()
    commands = [line for line in lines if line.startswith("CMD ")]
    assert len(commands) == 1, commands
    return commands[0]


@pytest.mark.parametrize("service", sorted(API_DOCKERFILES))
def test_api_dockerfile_command_runs_two_workers_in_multiprocess_mode(service: str) -> None:
    has_alembic, app_path, binds = API_DOCKERFILES[service]
    cmd = _default_command(service)

    assert cmd.count("python:libs.observability.gunicorn_conf") == 1
    assert "-w ${WEB_CONCURRENCY:-2}" in cmd
    assert "export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus-multiproc" in cmd
    assert 'mkdir -p \\"$PROMETHEUS_MULTIPROC_DIR\\"' in cmd
    assert "exec gunicorn" in cmd and app_path in cmd
    for bind in binds:
        assert f"-b {bind}" in cmd
    # The directory variable is exported before gunicorn starts, after the migrations.
    assert cmd.index("export PROMETHEUS_MULTIPROC_DIR") < cmd.index("exec gunicorn")
    if has_alembic:
        assert "alembic" in cmd and "upgrade head" in cmd
        assert cmd.index("upgrade head") < cmd.index("export PROMETHEUS_MULTIPROC_DIR")
    else:
        assert "alembic" not in cmd


def test_the_variable_is_exported_only_by_image_commands_never_in_compose() -> None:
    """Workers and consumers reuse the API images with a command override; if Compose set
    the variable they would write mmap files nobody aggregates."""
    compose = (REPO_ROOT / "infra" / "docker-compose.yml").read_text()
    assert "PROMETHEUS_MULTIPROC_DIR" not in compose


def _write_counter_file(directory: Path, pid: int, value: float) -> None:
    key = mmap_key(
        "http_requests_total", "http_requests_total", ["handler", "method", "status"], ["/x", "GET", "2xx"], "help"
    )
    values = MmapedDict(str(directory / f"counter_{pid}.db"))
    try:
        values.write_value(key, value, 0.0)
    finally:
        values.close()


def test_render_latest_sums_the_per_process_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_counter_file(tmp_path, 101, 3.0)
    _write_counter_file(tmp_path, 202, 4.0)
    monkeypatch.setenv(ENV, str(tmp_path))

    text = render_latest().decode()

    assert 'http_requests_total{handler="/x",method="GET",status="2xx"} 7.0' in text


def test_exposition_registry_is_the_default_registry_without_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    assert exposition_registry() is REGISTRY
    assert isinstance(render_latest(), bytes)
