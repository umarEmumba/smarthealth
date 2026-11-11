"""Static checks of the Prometheus scrape configuration (plan 03-15; OBS-01, T-3-45); no stack needed.

Every process is scraped over the Compose network, and no metrics port is published
to the host: a metrics endpoint is internal-only unless it was already published
before Phase 3 (`PRE_PHASE_3_PUBLISHED`, empty today: the gateway's metrics listener
:9100 has never been published, only its :8000 API).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
PROMETHEUS_FILE = REPO_ROOT / "infra" / "prometheus.yml"

# (service, container port) pairs that were published to the host before Phase 3 and
# are also scraped. None today.
PRE_PHASE_3_PUBLISHED: set[tuple[str, int]] = set()

EXPECTED_JOBS = {
    "smarthealth-gateway",
    "smarthealth-identity",
    "smarthealth-organization",
    "smarthealth-provider",
    "smarthealth-patient",
    "smarthealth-scheduling",
    "smarthealth-billing",
    "smarthealth-visit",
    "smarthealth-analytics",
    "smarthealth-scheduling-worker",
    "smarthealth-visit-worker",
    "smarthealth-notification-temporal-worker",
    "smarthealth-visit-consumer",
    "smarthealth-analytics-consumer",
    "smarthealth-notification-consumer",
    "smarthealth-notification-celery",
    "rabbitmq",
}

# Each Temporal worker serves application metrics (:9100) and SDK metrics (:9464).
TEMPORAL_WORKER_JOBS = {
    "smarthealth-scheduling-worker": "scheduling-worker",
    "smarthealth-visit-worker": "visit-worker",
    "smarthealth-notification-temporal-worker": "notification-temporal-worker",
}


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE_FILE.read_text())


@pytest.fixture(scope="module")
def scrape_configs() -> dict[str, list[str]]:
    config = yaml.safe_load(PROMETHEUS_FILE.read_text())
    jobs: dict[str, list[str]] = {}
    for job in config["scrape_configs"]:
        name = job["job_name"]
        assert name not in jobs, f"duplicate job {name}"
        jobs[name] = [t for sc in job["static_configs"] for t in sc["targets"]]
    return jobs


def _split(target: str) -> tuple[str, int]:
    host, _, port = target.rpartition(":")
    return host, int(port)


def _published_container_ports(service: dict[str, Any]) -> set[int]:
    """Container ports a Compose service publishes to the host (short or long syntax)."""
    ports: set[int] = set()
    for entry in service.get("ports", []):
        if isinstance(entry, dict):
            ports.add(int(entry["target"]))
        else:
            ports.add(int(str(entry).split("/")[0].rsplit(":", 1)[-1]))
    return ports


def test_every_expected_job_is_scraped(scrape_configs: dict[str, list[str]]) -> None:
    assert EXPECTED_JOBS <= set(scrape_configs), EXPECTED_JOBS - set(scrape_configs)
    assert all(scrape_configs[job] for job in EXPECTED_JOBS)


def test_every_target_host_is_a_compose_service(
    compose: dict[str, Any], scrape_configs: dict[str, list[str]]
) -> None:
    services = set(compose["services"])
    for job, targets in scrape_configs.items():
        for target in targets:
            host, _ = _split(target)
            assert host in services, f"{job}: {host} is not a Compose service"


def test_no_scraped_port_is_published_to_the_host(
    compose: dict[str, Any], scrape_configs: dict[str, list[str]]
) -> None:
    for job, targets in scrape_configs.items():
        for target in targets:
            host, port = _split(target)
            if (host, port) in PRE_PHASE_3_PUBLISHED:
                continue
            published = _published_container_ports(compose["services"][host])
            assert port not in published, f"{job}: {target} is published to the host"


def test_no_compose_service_publishes_a_metrics_port_added_in_phase_3(compose: dict[str, Any]) -> None:
    # The worker, consumer and Celery listeners (:9100) and the Temporal SDK listener
    # (:9464) are internal-only: nothing may map them.
    for name, service in compose["services"].items():
        leaked = _published_container_ports(service) & {9100, 9464}
        assert not leaked, f"{name} publishes {sorted(leaked)}"


@pytest.mark.parametrize(("job", "service"), sorted(TEMPORAL_WORKER_JOBS.items()))
def test_each_temporal_worker_is_scraped_on_both_metrics_ports(
    scrape_configs: dict[str, list[str]], job: str, service: str
) -> None:
    assert sorted(scrape_configs[job]) == sorted([f"{service}:9100", f"{service}:9464"])


def test_the_celery_worker_and_the_kafka_consumers_are_scraped_on_9100(
    scrape_configs: dict[str, list[str]],
) -> None:
    for job in (
        "smarthealth-visit-consumer",
        "smarthealth-analytics-consumer",
        "smarthealth-notification-consumer",
        "smarthealth-notification-celery",
    ):
        service = job.removeprefix("smarthealth-")
        assert scrape_configs[job] == [f"{service}:9100"]
    assert scrape_configs["rabbitmq"] == ["rabbitmq:15692"]
    assert scrape_configs["smarthealth-analytics"] == ["analytics:8000"]
