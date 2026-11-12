"""Broker provisioning unit tests (plan 03-14, EVENT-04, T-3-42): no live broker, the
management API is a `httpx.MockTransport`."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from services.notification import broker_setup
from services.notification.broker_setup import BrokerSetupError, ensure_broker_user
from services.notification.config import settings

PASSWORD = "s3cret-Pa55word-never-printed"


def _client(handler) -> httpx.Client:  # type: ignore[no-untyped-def]
    return httpx.Client(base_url="http://rabbitmq:15672", transport=httpx.MockTransport(handler))


def test_ensure_broker_user_issues_the_three_puts_without_user_tags() -> None:
    seen: list[tuple[str, str, dict | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, body))
        return httpx.Response(201)

    with _client(handler) as client:
        ensure_broker_user(client, vhost="smarthealth", user="smarthealth_notification", password=PASSWORD)

    assert [(m, p) for m, p, _ in seen] == [
        ("PUT", "/api/vhosts/smarthealth"),
        ("PUT", "/api/users/smarthealth_notification"),
        ("PUT", "/api/permissions/smarthealth/smarthealth_notification"),
    ]
    assert seen[1][2] == {"password": PASSWORD, "tags": ""}  # no administrator / management tag
    assert seen[2][2] == {"configure": ".*", "write": ".*", "read": ".*"}


def test_ensure_broker_user_is_idempotent_on_204() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    with _client(handler) as client:
        ensure_broker_user(client, vhost="smarthealth", user="u", password=PASSWORD)
        ensure_broker_user(client, vhost="smarthealth", user="u", password=PASSWORD)


def test_failure_reports_the_status_code_only(capsys: pytest.CaptureFixture[str]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "echo " + request.content.decode()})

    with _client(handler) as client, pytest.raises(BrokerSetupError) as info:
        ensure_broker_user(client, vhost="smarthealth", user="u", password=PASSWORD)
    assert "401" in str(info.value)
    assert PASSWORD not in str(info.value)
    captured = capsys.readouterr()
    assert PASSWORD not in captured.out + captured.err


def _password_file(tmp_path: Path) -> Path:
    path = tmp_path / "broker.secret"
    path.write_text(PASSWORD + "\n", encoding="utf-8")
    return path


def test_main_succeeds_and_never_prints_a_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("RABBITMQ_ADMIN_USER", "admin-user")
    monkeypatch.setenv("RABBITMQ_ADMIN_PASSWORD", "admin-pass-never-printed")
    monkeypatch.setattr(settings, "broker_password_file", str(_password_file(tmp_path)))
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(201)

    real_client = httpx.Client
    monkeypatch.setattr(
        broker_setup.httpx,
        "Client",
        lambda **kwargs: real_client(**{**kwargs, "transport": httpx.MockTransport(handler)}),
    )
    assert broker_setup.main() == 0
    out = capsys.readouterr()
    assert "broker user ready" in out.out
    assert PASSWORD not in out.out + out.err
    assert "admin-pass-never-printed" not in out.out + out.err
    assert len(calls) == 3


@pytest.mark.parametrize("missing", ["RABBITMQ_ADMIN_USER", "RABBITMQ_ADMIN_PASSWORD"])
def test_main_exits_one_without_admin_credentials(
    missing: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("RABBITMQ_ADMIN_USER", "admin-user")
    monkeypatch.setenv("RABBITMQ_ADMIN_PASSWORD", "admin-pass")
    monkeypatch.delenv(missing)
    monkeypatch.setattr(settings, "broker_password_file", str(_password_file(tmp_path)))
    assert broker_setup.main() == 1
    err = capsys.readouterr().err
    assert "RABBITMQ_ADMIN_USER" in err and "RABBITMQ_ADMIN_PASSWORD" in err


def test_main_exits_one_when_the_password_file_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("RABBITMQ_ADMIN_USER", "admin-user")
    monkeypatch.setenv("RABBITMQ_ADMIN_PASSWORD", "admin-pass")
    monkeypatch.setattr(settings, "broker_password_file", str(tmp_path / "absent.secret"))
    assert broker_setup.main() == 1
    assert "NOTIFICATION_BROKER_PASSWORD_FILE" in capsys.readouterr().err
