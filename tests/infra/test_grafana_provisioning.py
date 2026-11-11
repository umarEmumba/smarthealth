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


# --- Plan 03-16: the four dashboards, three datasources, read-only database role --------

import re

DASHBOARDS_DIR = GRAFANA / "dashboards"
DASHBOARD_FILES = sorted(DASHBOARDS_DIR.glob("*.json"))
EXPECTED_DASHBOARD_UIDS = {
    "smarthealth-booking-nfr",
    "smarthealth-events",
    "smarthealth-notifications",
    "smarthealth-operations",
}
OPERATIONS_VIEWS = {
    "ANLY-01": "v_active_patients",
    "ANLY-02": "v_appointments_booked_daily",
    "ANLY-03": "v_completed_visits_daily",
    "ANLY-04": "v_cancellation",
    "ANLY-05": "v_visit_wait",
}


def _datasources() -> list[dict[str, Any]]:
    config = yaml.safe_load((GRAFANA / "provisioning" / "datasources" / "datasources.yml").read_text())
    return config["datasources"]


def _all_panels(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    panels: list[dict[str, Any]] = []
    for panel in dashboard["panels"]:
        panels.append(panel)
        panels.extend(panel.get("panels", []))
    return panels


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def test_there_are_exactly_the_four_expected_dashboards() -> None:
    assert {_load(p)["uid"] for p in DASHBOARD_FILES} == EXPECTED_DASHBOARD_UIDS
    assert len(DASHBOARD_FILES) == 4


def test_dashboard_uids_and_titles_are_unique_and_the_files_are_read_only_dashboards() -> None:
    dashboards = [_load(p) for p in DASHBOARD_FILES]
    assert len({d["uid"] for d in dashboards}) == len(dashboards)
    assert len({d["title"] for d in dashboards}) == len(dashboards)
    for d in dashboards:
        assert d["editable"] is False, d["uid"]
        ids = [p["id"] for p in _all_panels(d)]
        assert len(ids) == len(set(ids)), d["uid"]


def test_the_datasources_are_prometheus_analytics_postgres_and_jaeger() -> None:
    by_uid = {d["uid"]: d for d in _datasources()}
    assert set(by_uid) == {"prometheus", "analytics-pg", "jaeger"}
    assert by_uid["jaeger"]["type"] == "jaeger"
    assert by_uid["jaeger"]["url"] == "http://jaeger:16686"


def test_the_analytics_datasource_is_read_only_with_a_file_provided_password() -> None:
    text = (GRAFANA / "provisioning" / "datasources" / "datasources.yml").read_text()
    pg = next(d for d in _datasources() if d["uid"] == "analytics-pg")
    assert pg["type"] == "grafana-postgresql-datasource"
    assert pg["user"] == "grafana_ro"  # the read-only role; no write role exists for Grafana
    assert pg["url"] == "postgres:5432"
    assert pg["jsonData"]["database"] == "analytics_db"
    assert pg["secureJsonData"]["password"] == "$__file{/run/secrets/grafana_ro.secret}"
    assert text.count("$__file{/run/secrets/grafana_ro.secret}") == 1
    # No literal password anywhere in the provisioning files.
    for path in (GRAFANA / "provisioning").rglob("*.yml"):
        for line in path.read_text().splitlines():
            if re.match(r"\s*(password|basicAuthPassword)\s*:", line):
                assert "$__file{" in line, (path.name, "literal password")
    assert not [d for d in _datasources() if d.get("user") not in (None, "grafana_ro")]


def test_compose_gives_grafana_only_the_read_only_secret_and_waits_for_analytics() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    grafana = compose["services"]["grafana"]
    assert grafana["secrets"] == [{"source": "grafana_ro_password", "target": "grafana_ro.secret"}]
    assert grafana["depends_on"]["analytics"]["condition"] == "service_healthy"
    assert "grafana_ro_password" in compose["secrets"]
    # The provider path is the in-container side of the dashboards mount.
    provider = yaml.safe_load(
        (GRAFANA / "provisioning" / "dashboards" / "smarthealth.yml").read_text()
    )["providers"][0]
    mounts = {v.split(":")[1] for v in grafana["volumes"] if v.startswith("./grafana/dashboards")}
    assert provider["options"]["path"] in mounts


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda p: p.stem)
def test_every_panel_and_variable_uses_a_provisioned_datasource_uid(path: Path) -> None:
    known = {d["uid"] for d in _datasources()}
    dashboard = _load(path)
    refs: list[tuple[str, str]] = []
    for panel in _all_panels(dashboard):
        if panel["type"] == "text":
            continue
        refs.append((panel["title"], panel["datasource"]["uid"]))
        refs.extend((panel["title"], t["datasource"]["uid"]) for t in panel["targets"])
    for variable in dashboard["templating"]["list"]:
        refs.append((variable["name"], variable["datasource"]["uid"]))
    assert refs
    for where, uid in refs:
        assert uid in known, (path.name, where, uid)


def _emitted_metric_names() -> set[str]:
    """Every metric name this codebase emits: prometheus_client declarations found in the
    source (histograms also expose _bucket/_count/_sum), the HTTP Instrumentator's names, and
    the Temporal SDK / RabbitMQ prefixes handled by `_is_known_metric`."""
    names: set[str] = set()
    declaration = re.compile(r"\b(Counter|Histogram|Gauge|Summary)\(\s*\"([a-z_:0-9]+)\"")
    for root in ("libs", "services"):
        for source in (REPO_ROOT / root).rglob("*.py"):
            for kind, name in declaration.findall(source.read_text()):
                if kind == "Histogram":
                    names.update({f"{name}_bucket", f"{name}_count", f"{name}_sum"})
                elif kind == "Counter":
                    names.add(name if name.endswith("_total") else f"{name}_total")
                else:
                    names.add(name)
    names.update(
        {
            "http_requests_total",
            "http_request_duration_seconds_bucket",
            "http_request_duration_seconds_count",
            "http_request_duration_seconds_sum",
        }
    )
    return names


def _is_known_metric(name: str, emitted: set[str]) -> bool:
    return name in emitted or name.startswith(("temporal_", "rabbitmq_"))


_PROMQL_WORDS = {
    "by", "without", "on", "ignoring", "group_left", "group_right", "bool", "offset",
    "and", "or", "unless",
}


def _promql_metrics(expression: str) -> set[str]:
    stripped = re.sub(r'"[^"]*"', "", expression)  # label values
    stripped = re.sub(r"\{[^}]*\}", "", stripped)  # label matchers
    stripped = re.sub(r"\b(by|without|on|ignoring)\s*\([^)]*\)", "", stripped)
    stripped = re.sub(r"\[[^\]]*\]", "", stripped)  # ranges, including $__range
    stripped = re.sub(r"\$\{?__?\w+\}?", "", stripped)  # Grafana variables
    stripped = re.sub(r"\b\d+(\.\d+)?(e[+-]?\d+)?\b", "", stripped)  # numbers, 1e-9
    found = set()
    for match in re.finditer(r"[a-zA-Z_:][a-zA-Z0-9_:]*", stripped):
        name = match.group(0)
        rest = stripped[match.end():].lstrip()
        if name in _PROMQL_WORDS or rest.startswith("("):  # keywords and functions
            continue
        found.add(name)
    return found


def test_the_metric_name_extractor_understands_promql() -> None:
    expression = (
        'histogram_quantile(0.95, sum by (le, group) (rate(event_processing_seconds_bucket'
        '{job="x"}[5m]))) / on(group) max(kafka_consumer_lag)'
    )
    assert _promql_metrics(expression) == {"event_processing_seconds_bucket", "kafka_consumer_lag"}
    assert _promql_metrics("sum by (a) (increase(dlq_events_total[$__range]))") == {"dlq_events_total"}


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda p: p.stem)
def test_every_prometheus_metric_in_a_panel_is_one_this_codebase_emits(path: Path) -> None:
    emitted = _emitted_metric_names()
    assert {"events_consumed_total", "outbox_insert_seconds_bucket", "booking_saga_seconds_bucket"} <= emitted
    used: set[str] = set()
    for panel in _all_panels(_load(path)):
        if panel["type"] == "text":
            continue
        for target in panel["targets"]:
            if target["datasource"]["uid"] == "prometheus":
                used |= _promql_metrics(target["expr"])
    unknown = {name for name in used if not _is_known_metric(name, emitted)}
    assert not unknown, (path.name, sorted(unknown))


def test_the_events_dashboard_covers_lag_dlq_outbox_and_replication_slots() -> None:
    dashboard = _load(DASHBOARDS_DIR / "smarthealth-events.json")
    expressions = " ".join(
        t.get("expr", "") for p in _all_panels(dashboard) for t in p["targets"]
    )
    for needle in (
        "kafka_consumer_lag",
        "event_end_to_end_lag_seconds_bucket",
        "event_processing_seconds_bucket",
        "dlq_events_total",
        "outbox_insert_seconds_bucket",
    ):
        assert needle in expressions, needle
    slot_panels = [p for p in _all_panels(dashboard) if p["datasource"]["uid"] == "analytics-pg"]
    assert len(slot_panels) == 1
    sql = slot_panels[0]["targets"][0]["rawSql"]
    assert "pg_replication_slots" in sql and "pg_wal_lsn_diff" in sql and "active" in sql


def test_the_notifications_dashboard_covers_delivery_queues_and_temporal() -> None:
    dashboard = _load(DASHBOARDS_DIR / "smarthealth-notifications.json")
    expressions = " ".join(t["expr"] for p in _all_panels(dashboard) for t in p["targets"])
    for needle in (
        "notifications_sent_total",
        "notifications_skipped_total",
        "notifications_retried_total",
        "notifications_dead_lettered_total",
        "notifications_provider_replayed_total",
        "notification_delivery_seconds_bucket",
        "rabbitmq_queue_messages_ready",
        "temporal_workflow_completed",
        "temporal_activity_execution_latency",
    ):
        assert needle in expressions, needle


def test_the_operations_dashboard_has_one_panel_per_metric_over_the_analytics_views_only() -> None:
    dashboard = _load(DASHBOARDS_DIR / "smarthealth-operations.json")
    assert dashboard["uid"] == "smarthealth-operations"
    assert [v["name"] for v in dashboard["templating"]["list"]] == ["clinic"]
    for marker, view in OPERATIONS_VIEWS.items():
        panels = [p for p in _all_panels(dashboard) if marker in p["title"]]
        assert len(panels) == 1, marker
        panel = panels[0]
        assert panel["datasource"]["uid"] == "analytics-pg"
        sql = panel["targets"][0]["rawSql"]
        assert re.findall(r"\bFROM\s+(\w+)", sql, flags=re.IGNORECASE) == [view], (marker, sql)
        assert "${clinic:sqlstring}" in sql  # always scoped to the selected clinic
        assert panel["description"], marker  # the definition lives in the description
    # Only the five metric panels query anything (the sixth panel is explanatory text).
    queried = [p for p in _all_panels(dashboard) if p["type"] != "text"]
    assert len(queried) == 5  # exactly one query panel per metric
    assert any("check-in" in p["description"] and "visit start" in p["description"] for p in queried)
    assert any("denominator" in p["description"] for p in queried)
