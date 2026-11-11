"""Static checks of Grafana's file provisioning (plan 03-15; D-11, D-04); no stack needed.

The dashboards are code: the datasource and the dashboards provider come from
`infra/grafana/provisioning`, the booking/NFR dashboard from
`infra/grafana/dashboards`, and its threshold lines must equal `loadtest/nfr.json`
(the single source of the NFR numbers).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GRAFANA = REPO_ROOT / "infra" / "grafana"
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
NFR = json.loads((REPO_ROOT / "loadtest" / "nfr.json").read_text())
DASHBOARD_PATH = GRAFANA / "dashboards" / "smarthealth-booking-nfr.json"


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    return json.loads(DASHBOARD_PATH.read_text())


def _panel(dashboard: dict[str, Any], title_part: str) -> dict[str, Any]:
    matches = [p for p in dashboard["panels"] if title_part in p["title"]]
    assert len(matches) == 1, (title_part, [p["title"] for p in dashboard["panels"]])
    return matches[0]


def _limit(panel: dict[str, Any]) -> float:
    steps = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
    assert len(steps) == 2 and steps[0]["value"] is None
    return float(steps[1]["value"])


def test_prometheus_is_a_provisioned_default_datasource_with_a_stable_uid() -> None:
    config = yaml.safe_load((GRAFANA / "provisioning" / "datasources" / "datasources.yml").read_text())
    assert config["apiVersion"] == 1
    prometheus = next(d for d in config["datasources"] if d["uid"] == "prometheus")
    assert prometheus["type"] == "prometheus"
    assert prometheus["url"] == "http://prometheus:9090"
    assert prometheus["isDefault"] is True


def test_the_dashboards_provider_is_file_based_and_read_only() -> None:
    config = yaml.safe_load((GRAFANA / "provisioning" / "dashboards" / "smarthealth.yml").read_text())
    provider = config["providers"][0]
    assert provider["type"] == "file"
    assert provider["allowUiUpdates"] is False
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"


def test_compose_mounts_the_provisioning_and_dashboards_read_only() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    volumes = compose["services"]["grafana"]["volumes"]
    assert "./grafana/provisioning:/etc/grafana/provisioning:ro" in volumes
    assert "./grafana/dashboards:/var/lib/grafana/dashboards:ro" in volumes


def test_grafana_finds_every_provisioning_directory() -> None:
    # Grafana logs an error for a missing provisioning directory; the empty ones carry
    # a valid placeholder file so a clean start has no provisioning error lines.
    for name in ("datasources", "dashboards", "plugins", "alerting"):
        files = list((GRAFANA / "provisioning" / name).glob("*.yml"))
        assert files, name


def test_the_dashboard_is_addressed_by_a_stable_uid_and_charts_the_saga_histogram(
    dashboard: dict[str, Any],
) -> None:
    assert dashboard["uid"] == "smarthealth-booking-nfr"
    expressions = [t["expr"] for p in dashboard["panels"] for t in p["targets"]]
    assert any("booking_saga_seconds_bucket" in e for e in expressions)
    # Every panel and query reads the provisioned Prometheus datasource by uid.
    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == "prometheus"
        assert all(t["datasource"]["uid"] == "prometheus" for t in panel["targets"])


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("NFR-03 booking accept latency p95", NFR["book_p95_ms"] / 1000),
        ("NFR-03 booking accept latency p99", NFR["book_p99_ms"] / 1000),
        ("NFR-04 read latency p95", NFR["read_p95_ms"] / 1000),
        ("NFR-04 read latency p99", NFR["read_p99_ms"] / 1000),
        ("NFR-05 booking saga completion p95", NFR["confirm_p95_s"]),
        ("NFR-05 booking saga completion p99", NFR["confirm_p99_s"]),
        ("NFR-02 gateway 5xx ratio", NFR["error_rate_max"]),
    ],
)
def test_threshold_lines_equal_loadtest_nfr_json(
    dashboard: dict[str, Any], title: str, expected: float
) -> None:
    assert _limit(_panel(dashboard, title)) == pytest.approx(expected)
