"""One booking is one trace (plan 02-06; D-18, research A4).

The API-side Temporal client and the worker both carry OpenTelemetry's
`TracingInterceptor` (as `libs.temporal.connect_client` builds them in
production). `POST /api/appointments` starts the saga inside its request span,
so the gateway's server span, Scheduling's server span, the StartWorkflow span,
and the RunWorkflow/RunActivity spans on the worker all share one trace id.

The harness runs every service in one process, so the global SDK
TracerProvider configured by the first `create_app` is shared; an in-memory
exporter is added to it once per session (as in tests/e2e/test_trace_propagation.py).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import httpx
import pytest
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.testing import WorkflowEnvironment

from services.scheduling.domain.contracts import (
    ACT_BILLING,
    ACT_CONFIRM,
    ACT_RESERVE,
    ACT_VALIDATE,
    BookingResult,
    booking_workflow_id,
)
from tests.conftest import harness_apps
from tests.scheduling.seed import World, auth
from tests.temporal_fixtures import running_scheduling_worker

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def trace_exporter() -> InMemorySpanExporter:
    harness_apps()  # building the apps installs the SDK TracerProvider
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def spans(trace_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    trace_exporter.clear()
    yield trace_exporter
    trace_exporter.clear()


def traced(client: Client) -> Client:
    """The test server's client, rebuilt with the production interceptor."""
    config = client.config()
    config["interceptors"] = [TracingInterceptor()]
    return Client(**config)


def _named(finished: list[ReadableSpan], name: str) -> ReadableSpan:
    matches = [s for s in finished if s.name == name]
    assert matches, f"no span named {name!r}; got {sorted({s.name for s in finished})}"
    return matches[-1]


async def test_booking_request_workflow_and_activities_share_one_trace(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    booking_world: World,
    temporal_env: WorkflowEnvironment,
    spans: InMemorySpanExporter,
) -> None:
    world = booking_world
    start, end = world.local_day_bounds(world.last_day)
    found = await client.get(
        "/api/slots",
        params={"provider_id": str(world.provider_id), "start": start.isoformat(), "end": end.isoformat()},
        headers=auth(world.admin),
    )
    slot_id = found.json()[0]["id"]
    traced_client = traced(temporal_env.client)

    async with running_scheduling_worker(traced_client, mesh, mongo_client):
        spans.clear()
        resp = await client.post(
            "/api/appointments",
            json={"slot_id": slot_id, "patient_id": str(world.patient_id)},
            headers={**auth(world.front_desk), "Idempotency-Key": f"trace-{uuid.uuid4().hex}"},
        )
        assert resp.status_code == 202, resp.text
        appointment_id = resp.json()["appointment_id"]
        result = await traced_client.get_workflow_handle(
            booking_workflow_id(appointment_id), result_type=BookingResult
        ).result()
        assert result.outcome == "confirmed"

    finished = list(spans.get_finished_spans())
    api_spans = [
        s
        for s in finished
        if s.kind == SpanKind.SERVER and (s.attributes or {}).get("http.route") == "/api/appointments"
    ]
    assert api_spans, "no Scheduling server span for POST /api/appointments"
    api = api_spans[-1]
    start_span = _named(finished, "StartWorkflow:BookingWorkflow")
    run_workflow = _named(finished, "RunWorkflow:BookingWorkflow")
    activities = [
        _named(finished, f"RunActivity:{name}")
        for name in (ACT_VALIDATE, ACT_RESERVE, ACT_BILLING, ACT_CONFIRM)
    ]

    trace_ids = {s.context.trace_id for s in (api, start_span, run_workflow, *activities)}
    assert trace_ids == {api.context.trace_id}, "booking spans are split across traces"

    # Connected, not coincident: the workflow start hangs under the request
    # span, and the activities hang under the workflow run.
    assert _descends_from(start_span, api, finished)
    for span in activities:
        assert _descends_from(span, run_workflow, finished) or _descends_from(span, start_span, finished)


def _descends_from(span: ReadableSpan, ancestor: ReadableSpan, finished: list[ReadableSpan]) -> bool:
    by_id = {s.context.span_id: s for s in finished}
    parent = span.parent
    while parent is not None:
        if parent.span_id == ancestor.context.span_id:
            return True
        node = by_id.get(parent.span_id)
        parent = node.parent if node is not None else None
    return False
