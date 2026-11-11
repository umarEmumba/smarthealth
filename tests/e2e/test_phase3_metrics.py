"""Metrics on the deployed stack (plan 03-15; OBS-01, D-04, D-11).

The tracer books through the published gateway and follows one booking's saga time
from the scheduling worker's `booking_saga_seconds` histogram into Prometheus
(published on localhost:9090) and onto the file-provisioned Grafana dashboard. The
later tests read Prometheus's own target list: every process the plan names must be
scraped and `up`, and each Temporal worker must expose Temporal SDK (`temporal_`)
series next to its application metrics.

Read-only against Prometheus and Grafana: nothing here restarts or stops a container.
Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_metrics.py -m e2e
The e2e needs the gateway auth budget raised (GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE=1000).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio

from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    book,
    day_slots,
    gateway_url,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

SCRAPE_DEADLINE_SECONDS = 90
TRACER_DEADLINE_SECONDS = 60


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
