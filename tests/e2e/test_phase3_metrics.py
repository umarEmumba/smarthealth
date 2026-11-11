"""Metrics on the deployed stack (plan 03-15; OBS-01, D-04, D-11).

The tracer books through the published gateway and follows one booking's saga time
from the scheduling worker's `booking_saga_seconds` histogram into Prometheus
(published on localhost:9090) and onto the file-provisioned Grafana dashboard. The
later tests read Prometheus's own target list: every process the plan names must be
scraped and `up`, and each Temporal worker must expose Temporal SDK (`temporal_`)
series next to its application metrics.

Plan 03-16 adds the operations dashboard checks: the five panel queries of
`smarthealth-operations.json` are run as the read-only `grafana_ro` role (password read
from its Compose secret file, never printed) against analytics_db on the published
Postgres port, and compared with `/api/analytics/summary`.

Read-only against Prometheus and Grafana: nothing here restarts or stops a container.
Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_metrics.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio

from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
    book_slot_inside_checkin_window,
    day_slots,
    gateway_url,
    poll,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

SCRAPE_DEADLINE_SECONDS = 90
TRACER_DEADLINE_SECONDS = 60
FLOW_DEADLINE_SECONDS = 90

GRAFANA_DIR = stack.REPO_ROOT / "infra" / "grafana"
GRAFANA_RO_SECRET_FILE = stack.REPO_ROOT / "infra" / "secrets" / "infra" / "grafana_ro.secret"


def _published(service: str, container_port: int, env_var: str) -> str:
    """The host URL a service's port is published on: the environment override, else
    Compose's own answer (the host port is configurable in .env, so 9090/3000 are not
    assumed)."""
    override = os.environ.get(env_var)
    if override:
        return override
    out = stack.compose("port", service, str(container_port)).stdout.strip()
    host_port = out.rsplit(":", 1)[-1]
    return f"http://localhost:{host_port}"


def prometheus_url() -> str:
    return _published("prometheus", 9090, "SMARTHEALTH_PROMETHEUS_URL")


def grafana_url() -> str:
    return _published("grafana", 3000, "SMARTHEALTH_GRAFANA_URL")


def prom_query(expr: str) -> list[dict[str, Any]]:
    """Instant query against the published Prometheus; the result vector."""
    url = f"{prometheus_url()}/api/v1/query?{urllib.parse.urlencode({'query': expr})}"
    with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - fixed local URL
        body = json.loads(response.read())
    assert body["status"] == "success", body
    return body["data"]["result"]


def prom_targets() -> list[dict[str, Any]]:
    with urllib.request.urlopen(  # noqa: S310 - fixed local URL
        f"{prometheus_url()}/api/v1/targets", timeout=10
    ) as response:
        return json.loads(response.read())["data"]["activeTargets"]


def wait_until(check: Any, *, timeout: float, interval: float = 3.0) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(interval)
    pytest.fail(f"condition not met within {timeout}s (last value: {last!r})")


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_service_healthy("scheduling-worker")


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_saga_time_reaches_prometheus_and_grafana_is_provisioned(
    gateway: httpx.AsyncClient,
) -> None:
    ew: E2EWorld = await seed_phase2_world(gateway)
    slots = await day_slots(gateway, ew, ew.world.last_day)
    assert slots, "no available slot in the seeded world"
    appointment_id = await book(
        gateway, ew, slot_id=slots[0]["id"], patient_id=ew.patient_ids[0]
    )
    await appointment_in(gateway, ew, appointment_id, {"confirmed"}, timeout=TRACER_DEADLINE_SECONDS)

    # The scheduling worker's own metrics endpoint is scraped and carries the histogram.
    def saga_counted() -> bool:
        result = prom_query(
            'booking_saga_seconds_count{job="smarthealth-scheduling-worker",outcome="confirmed"}'
        )
        return bool(result) and float(result[0]["value"][1]) > 0

    await asyncio.to_thread(wait_until, saga_counted, timeout=TRACER_DEADLINE_SECONDS)

    # The Temporal SDK endpoint of the same worker is scraped too.
    def sdk_series() -> bool:
        return bool(prom_query('count({__name__=~"temporal_.+",job="smarthealth-scheduling-worker"})'))

    await asyncio.to_thread(wait_until, sdk_series, timeout=SCRAPE_DEADLINE_SECONDS)

    # Grafana is healthy and its file provisioning loaded without error.
    with urllib.request.urlopen(  # noqa: S310 - fixed local URL
        f"{grafana_url()}/api/health", timeout=10
    ) as response:
        assert response.status == 200
    logs = (await asyncio.to_thread(stack.compose, "logs", "--no-color", "grafana")).stdout
    provisioning = [line for line in logs.splitlines() if "provisioning" in line]
    assert any("provision dashboards" in line for line in provisioning), "dashboards provider not loaded"
    errors = [line for line in provisioning if "level=error" in line or "level=warn" in line]
    assert not errors, errors


# --- Task 2: every process scraped and up --------------------------------------------

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
TEMPORAL_WORKER_JOBS = (
    "smarthealth-scheduling-worker",
    "smarthealth-visit-worker",
    "smarthealth-notification-temporal-worker",
)


def test_every_job_and_target_is_up() -> None:
    def all_up() -> list[dict[str, Any]] | None:
        targets = prom_targets()
        jobs = {t["labels"]["job"] for t in targets}
        down = [t for t in targets if t["health"] != "up"]
        return targets if EXPECTED_JOBS <= jobs and not down else None

    targets = wait_until(all_up, timeout=SCRAPE_DEADLINE_SECONDS)
    assert len(targets) >= 20, len(targets)
    # Both ports of each Temporal worker are scraped (application + SDK metrics).
    for job in TEMPORAL_WORKER_JOBS:
        ports = {t["scrapeUrl"].split("//", 1)[1].split("/", 1)[0].rsplit(":", 1)[-1] for t in targets if t["labels"]["job"] == job}
        assert ports == {"9100", "9464"}, (job, ports)


@pytest.mark.parametrize("job", TEMPORAL_WORKER_JOBS)
def test_each_temporal_worker_exposes_temporal_sdk_series(job: str) -> None:
    def present() -> bool:
        return bool(prom_query(f'count({{__name__=~"temporal_.+",job="{job}"}})'))

    wait_until(present, timeout=SCRAPE_DEADLINE_SECONDS)


def test_no_worker_consumer_or_celery_series_is_labelled_by_an_entity_id() -> None:
    """Bounded cardinality (T-3-48): no series on the worker, consumer or Celery jobs
    carries an appointment, patient, visit, provider or clinic id as a label."""
    jobs = "smarthealth-(scheduling-worker|visit-worker|notification-.+|visit-consumer|analytics-consumer)"
    for label in ("appointment_id", "patient_id", "visit_id", "provider_id", "clinic_id"):
        assert prom_query(f'count({{job=~"{jobs}",{label}!=""}})') == [], label


# --- Plan 03-16 Task 1: the operations dashboard as grafana_ro -----------------------


def _postgres_port() -> int:
    override = os.environ.get("SMARTHEALTH_POSTGRES_PORT")
    if override:
        return int(override)
    return int(stack.compose("port", "postgres", "5432").stdout.strip().rsplit(":", 1)[-1])


async def _connect_as_grafana_ro(database: str = "analytics_db") -> asyncpg.Connection:
    # The password comes from the Compose secret file and is never printed or logged.
    password = GRAFANA_RO_SECRET_FILE.read_text().strip()
    return await asyncpg.connect(
        host="localhost", port=_postgres_port(), user="grafana_ro", password=password,
        database=database, timeout=15,
    )


def _operations_dashboard() -> dict[str, Any]:
    return json.loads((GRAFANA_DIR / "dashboards" / "smarthealth-operations.json").read_text())


def _panel_sql(dashboard: dict[str, Any], marker: str) -> str:
    matches = [p for p in dashboard["panels"] if marker in p.get("title", "")]
    assert len(matches) == 1, (marker, [p.get("title") for p in dashboard["panels"]])
    return matches[0]["targets"][0]["rawSql"]


def _render(sql: str, clinic_id: str, start: datetime.datetime, end: datetime.datetime) -> str:
    """Substitute what Grafana substitutes: the `clinic` variable (sqlstring) and the
    time-range macros (quoted ISO timestamps)."""
    return (
        sql.replace("${clinic:sqlstring}", f"'{clinic_id}'")
        .replace("$__timeFrom()", f"'{start.isoformat()}'")
        .replace("$__timeTo()", f"'{end.isoformat()}'")
    )


async def test_operations_panels_return_rows_as_grafana_ro(gateway: httpx.AsyncClient) -> None:
    """After a booking, a cancel and a visit completion in a fresh clinic, each of the five
    panel queries (run as grafana_ro, clinic variable and time range substituted) returns
    the clinic's number, equal to `/api/analytics/summary` for the same window."""
    ew = await seed_phase2_world(gateway, patients=3)
    day_slot_ids = [slot["id"] for slot in (await day_slots(gateway, ew, ew.world.last_day))[:2]]
    assert len(day_slot_ids) == 2
    window_slot_id, _provider = await book_slot_inside_checkin_window(gateway, ew)
    kept = await book(gateway, ew, slot_id=day_slot_ids[0], patient_id=ew.patient_ids[0])
    to_cancel = await book(gateway, ew, slot_id=day_slot_ids[1], patient_id=ew.patient_ids[1])
    visited = await book(gateway, ew, slot_id=window_slot_id, patient_id=ew.patient_ids[2])
    for appointment_id in (kept, to_cancel, visited):
        confirmed = await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"})
        assert confirmed["status"] == "confirmed", confirmed
    cancel = await gateway.post(f"/api/appointments/{to_cancel}/cancel", json={}, headers=ew.front_desk)
    assert cancel.status_code == 202, cancel.text
    await appointment_in(gateway, ew, to_cancel, {"cancelled"})
    checked_in = await gateway.post("/api/visits", json={"appointment_id": visited}, headers=ew.front_desk)
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    assert (await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)).status_code == 200
    assert (await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)).status_code == 200

    today = datetime.datetime.now(datetime.UTC).date()
    window_from, window_to = today - datetime.timedelta(days=1), today + datetime.timedelta(days=1)
    start = datetime.datetime.combine(window_from, datetime.time.min, tzinfo=datetime.UTC)
    end = datetime.datetime.combine(window_to, datetime.time(23, 59, 59), tzinfo=datetime.UTC)
    dashboard = _operations_dashboard()
    sql = {
        marker: _render(_panel_sql(dashboard, marker), ew.clinic_id, start, end)
        for marker in ("ANLY-01", "ANLY-02", "ANLY-03", "ANLY-04", "ANLY-05")
    }

    connection = await _connect_as_grafana_ro()
    try:
        async def reached() -> dict[str, Any] | None:
            summary = await gateway.get(
                "/api/analytics/summary",
                params={"clinic_id": ew.clinic_id, "from": window_from.isoformat(), "to": window_to.isoformat()},
                headers=ew.admin,
            )
            assert summary.status_code == 200, summary.text
            body = summary.json()
            if not (
                body["appointments_booked"] == 3
                and body["completed_visits"] == 1
                and body["cancellation_rate"] is not None
                and body["average_wait_seconds"] is not None
            ):
                return None
            rows = {marker: await connection.fetch(query) for marker, query in sql.items()}
            if any(not r or r[0][-1] is None for r in rows.values()):
                return None  # the projection has not caught up for some panel yet
            return {"summary": body, "rows": rows}

        result = await poll(reached, timeout=FLOW_DEADLINE_SECONDS, interval=2.0)
        summary, rows = result["summary"], result["rows"]

        def total(marker: str) -> float:
            return float(sum(row[-1] for row in rows[marker]))

        assert total("ANLY-01") == summary["total_patients"] == 3
        assert total("ANLY-02") == summary["appointments_booked"] == 3
        assert total("ANLY-03") == summary["completed_visits"] == 1
        assert total("ANLY-04") == pytest.approx(summary["cancellation_rate"], abs=1e-6)
        assert total("ANLY-04") == pytest.approx(1 / 3, abs=0.001)
        assert total("ANLY-05") == pytest.approx(summary["average_wait_seconds"], abs=1e-3)

        # The clinic variable's query lists the test clinic.
        variable_query = dashboard["templating"]["list"][0]["query"]
        clinics = {row[0] for row in await connection.fetch(variable_query)}
        assert ew.clinic_id in clinics
    finally:
        await connection.close()


async def test_grafana_ro_cannot_write() -> None:
    """The Grafana role has no write path: an INSERT and a CREATE are refused, and it cannot
    even connect to an OLTP database (T-3-46)."""
    connection = await _connect_as_grafana_ro()
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                "INSERT INTO patients (patient_id, clinic_id) VALUES (gen_random_uuid(), gen_random_uuid())"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("CREATE TABLE grafana_ro_should_not_exist (id int)")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM appointments")
    finally:
        await connection.close()
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _connect_as_grafana_ro("scheduling_db")


def test_grafana_provisioning_clean() -> None:
    """Grafana is healthy and the log since its last (re)creation has no provisioning
    error or warning, the dashboards provider loaded, and the analytics datasource did
    not fail to provision."""
    with urllib.request.urlopen(  # noqa: S310 - fixed local URL
        f"{grafana_url()}/api/health", timeout=10
    ) as response:
        assert response.status == 200
    logs = stack.compose("logs", "--no-color", "grafana").stdout.splitlines()
    provisioning = [line for line in logs if "provisioning" in line or "provision" in line]
    assert any("provision dashboards" in line for line in provisioning), "dashboards provider not loaded"
    assert any("datasources" in line for line in provisioning), "datasources were not provisioned"
    errors = [line for line in logs if ("level=error" in line or "level=warn" in line) and "provision" in line]
    assert not errors, errors
    assert not [line for line in logs if "grafana_ro.secret" in line and "level=" in line], "secret file problem"
