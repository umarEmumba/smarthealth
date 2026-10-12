"""Tracer for plan 01.1-04: one gateway request is ONE connected trace.

client -> gateway (server span) -> Organization (server span) -> Identity JWKS
(server span, triggered by a cold key cache). All three share a single
trace_id because every internal hop injects W3C trace context through
`libs.http.outgoing_headers` (OpenTelemetry propagator API, never hand-written
header strings), and the audit document written by Organization carries that
same trace id (IAM-04).

The harness runs every service in one process, so the global SDK
TracerProvider that the first `create_app` configured is shared; the session
fixture adds an in-memory exporter to it and per-test fixtures clear it.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

import pytest
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from pymongo import AsyncMongoClient

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.config import shared_settings
from tests.conftest import bootstrap_admin_and_login, harness_apps

GATEWAY_ROUTE = "/{full_path:path}"
ORGANIZATION_ROUTE = "/api/clinics"
IDENTITY_JWKS_ROUTE = "/.well-known/jwks.json"


@pytest.fixture(scope="session")
def span_exporter() -> InMemorySpanExporter:
    # Building the apps configures the global provider (setup_tracing).
    harness_apps()
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider), (
        "the first create_app must have installed the SDK TracerProvider"
    )
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def spans(span_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    span_exporter.clear()
    yield span_exporter
    span_exporter.clear()


def _route_of(span: ReadableSpan) -> str:
    route = (span.attributes or {}).get("http.route")
    if isinstance(route, str) and route:
        return route
    return span.name


def _server_span_for(finished: list[ReadableSpan], route: str) -> ReadableSpan:
    matches = [
        s for s in finished if s.kind == SpanKind.SERVER and (_route_of(s) == route or route in s.name)
    ]
    assert matches, (
        f"no finished SERVER span for {route!r}; got "
        f"{[(s.name, s.kind.name) for s in finished if s.kind == SpanKind.SERVER]}"
    )
    return matches[-1]


@pytest.mark.integration
async def test_gateway_request_is_one_connected_trace(
    client: AsyncClient, spans: InMemorySpanExporter
) -> None:
    from libs.auth.jwks_client import JwksClient  # noqa: PLC0415

    org_app = harness_apps()["organization"]
    original_resolver = org_app.state.key_resolver
    org_app.state.key_resolver = JwksClient.from_settings(shared_settings)  # cold cache -> JWKS fetch
    try:
        tokens = await bootstrap_admin_and_login(client)
        spans.clear()

        created = await client.post(
            "/api/clinics",
            json={"name": "Trace Clinic", "timezone": "Europe/London"},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert created.status_code == 201, created.text
    finally:
        org_app.state.key_resolver = original_resolver

    finished = list(spans.get_finished_spans())
    gateway = _server_span_for(finished, GATEWAY_ROUTE)
    organization = _server_span_for(finished, ORGANIZATION_ROUTE)
    identity = _server_span_for(finished, IDENTITY_JWKS_ROUTE)

    trace_ids = {gateway.context.trace_id, organization.context.trace_id, identity.context.trace_id}
    assert len(trace_ids) == 1, f"expected one shared trace_id, got {len(trace_ids)}"

    # The trace is connected, not merely coincident: each hop is a descendant
    # of the span that made the call (organization under gateway).
    assert organization.parent is not None
    assert organization.parent.trace_id == gateway.context.trace_id


@pytest.mark.integration
async def test_audit_trace_id_matches_connected_trace(
    client: AsyncClient, spans: InMemorySpanExporter, mongo_client: AsyncMongoClient
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    spans.clear()

    created = await client.post(
        "/api/clinics",
        json={"name": "Audit Trace Clinic", "timezone": "Europe/London"},
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert created.status_code == 201, created.text
    clinic_id = created.json()["id"]

    finished = list(spans.get_finished_spans())
    organization = _server_span_for(finished, ORGANIZATION_ROUTE)
    gateway = _server_span_for(finished, GATEWAY_ROUTE)
    expected = format(organization.context.trace_id, "032x")
    assert format(gateway.context.trace_id, "032x") == expected

    audit = mongo_client[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME]
    doc = await audit.find_one({"action": "clinic.create", "entity_id": clinic_id})
    assert doc is not None
    assert re.fullmatch(r"[0-9a-f]{32}", doc["trace_id"])
    assert doc["trace_id"] == expected


def test_outgoing_headers_without_active_span_adds_nothing() -> None:
    from libs.http import outgoing_headers  # noqa: PLC0415

    assert outgoing_headers({"authorization": "Bearer x"}) == {"authorization": "Bearer x"}
    assert outgoing_headers() == {}


def test_outgoing_headers_inject_w3c_context_and_never_mutate_input(
    span_exporter: InMemorySpanExporter,
) -> None:
    from libs.http import outgoing_headers  # noqa: PLC0415

    extra = {"authorization": "Bearer x"}
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("probe") as span:
        headers = outgoing_headers(extra)
        expected_trace_id = format(span.get_span_context().trace_id, "032x")

    assert extra == {"authorization": "Bearer x"}
    assert headers["authorization"] == "Bearer x"
    assert re.fullmatch(rf"00-{expected_trace_id}-[0-9a-f]{{16}}-0[01]", headers["traceparent"])
    # Only W3C trace context travels: no baggage, nothing else.
    assert set(headers) <= {"authorization", "traceparent", "tracestate"}
