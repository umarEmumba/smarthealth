"""Compose wiring of the notification services (plan 03-14; T-3-42, EVENT-04).

Static checks on `infra/docker-compose.yml`: only the provisioning job holds broker
administrator credentials, the crash-injection switch of plan 03-02 is never set, no
notification service publishes a host port, and the broker password is a Compose secret.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "infra" / "docker-compose.yml"
NOTIFICATION_SERVICES = (
    "rabbitmq-init",
    "notification-celery",
    "notification-consumer",
    "notification-temporal-worker",
)
LONG_RUNNING = NOTIFICATION_SERVICES[1:]


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def test_the_crash_injection_switch_is_never_set_in_compose() -> None:
    assert "NOTIFICATION_FAKE_CRASH_ENABLED" not in COMPOSE.read_text(encoding="utf-8")


def test_only_the_provisioning_job_receives_broker_admin_credentials(compose: dict[str, Any]) -> None:
    holders = [
        name
        for name, service in compose["services"].items()
        if "RABBITMQ_ADMIN_PASSWORD" in (service.get("environment") or {})
    ]
    assert holders == ["rabbitmq-init"]
    assert COMPOSE.read_text(encoding="utf-8").count("RABBITMQ_ADMIN_PASSWORD") == 1


@pytest.mark.parametrize("name", NOTIFICATION_SERVICES)
def test_notification_services_publish_no_host_port(compose: dict[str, Any], name: str) -> None:
    assert "ports" not in compose["services"][name]


@pytest.mark.parametrize("name", NOTIFICATION_SERVICES)
def test_the_broker_password_is_a_mounted_secret_not_an_environment_value(
    compose: dict[str, Any], name: str
) -> None:
    service = compose["services"][name]
    assert {"source": "notification_broker_password", "target": "notification_broker.secret"} in service["secrets"]
    environment = service["environment"]
    assert environment["NOTIFICATION_BROKER_PASSWORD_FILE"] == "/run/secrets/notification_broker.secret"
    assert not any("BROKER_PASSWORD" in key and key != "NOTIFICATION_BROKER_PASSWORD_FILE" for key in environment)
    assert compose["secrets"]["notification_broker_password"]["file"] == "./secrets/infra/notification_broker.secret"


@pytest.mark.parametrize("name", NOTIFICATION_SERVICES)
def test_every_notification_service_builds_from_the_notification_image(
    compose: dict[str, Any], name: str
) -> None:
    assert compose["services"][name]["build"]["dockerfile"] == "services/notification/Dockerfile"


@pytest.mark.parametrize("name", LONG_RUNNING)
def test_long_running_processes_restart_and_have_a_healthcheck_and_wait_for_provisioning(
    compose: dict[str, Any], name: str
) -> None:
    service = compose["services"][name]
    assert service["restart"] == "unless-stopped"
    assert service["healthcheck"]["test"]
    assert service["depends_on"]["rabbitmq-init"]["condition"] == "service_completed_successfully"


def test_provisioning_job_depends_on_a_healthy_broker_and_runs_once(compose: dict[str, Any]) -> None:
    job = compose["services"]["rabbitmq-init"]
    assert job["depends_on"]["rabbitmq"]["condition"] == "service_healthy"
    assert job["restart"] == "no"
    assert job["command"] == ["python", "-m", "services.notification.broker_setup"]


def test_the_consumer_starts_a_new_group_at_latest_by_default(compose: dict[str, Any]) -> None:
    environment = compose["services"]["notification-consumer"]["environment"]
    assert environment["NOTIFICATION_CONSUMER_START"] == "${NOTIFICATION_CONSUMER_START:-latest}"
    assert compose["services"]["notification-consumer"]["command"] == [
        "python",
        "-m",
        "services.notification.consumer",
    ]
