"""OpenTelemetry tracer + OTLP exporter setup (targets Jaeger's OTLP ingest).

PHI safety: FastAPI auto-instrumentation is configured WITHOUT request/response
body capture — `opentelemetry-instrumentation-fastapi`'s default behavior does
NOT capture bodies unless explicitly enabled via `http_capture_headers_*`/body
hooks, and this module never enables them. Span attributes set anywhere in this
codebase must carry identifiers only (ids, roles, jti) — never PHI field
values (patient name/dob/contact). See
.planning/phases/01-foundation-core-services/01-RESEARCH.md's PHI-Safe
Structured Logging section, which applies equally to span attributes.

Exports via OTLP (gRPC) to `shared_settings.otlp_endpoint` — never the legacy
`opentelemetry-exporter-jaeger` package (forbidden per CLAUDE.md; Jaeger
ingests OTLP natively).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from libs.config import shared_settings

if TYPE_CHECKING:
    from fastapi import FastAPI

_TRACER_PROVIDER_CONFIGURED = False


def configure_tracer_provider(service_name: str = "smarthealth-identity") -> TracerProvider:
    """Idempotently configure the global TracerProvider with an OTLP exporter.

    Safe to call more than once (e.g. across test runs importing the app
    factory repeatedly) — only the first call actually installs the
    provider; subsequent calls return the already-configured global
    provider without re-registering a span processor.
    """
    global _TRACER_PROVIDER_CONFIGURED  # noqa: PLW0603

    if _TRACER_PROVIDER_CONFIGURED:
        return trace.get_tracer_provider()  # type: ignore[return-value]

    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=shared_settings.otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    _TRACER_PROVIDER_CONFIGURED = True
    return provider


def setup_tracing(app: FastAPI, service_name: str = "smarthealth-identity") -> None:
    """Configure the tracer provider and auto-instrument the FastAPI app.

    Body capture is deliberately NOT enabled — only default span attributes
    (method, route, status code) are recorded, never request/response
    payloads, which is the PHI-leakage vector this function must avoid.
    """
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # noqa: PLC0415

    configure_tracer_provider(service_name)
    FastAPIInstrumentor.instrument_app(app)


def get_tracer(name: str = "smarthealth-identity") -> trace.Tracer:
    """Return a tracer from the globally configured provider."""
    return trace.get_tracer(name)


def current_trace_id() -> str | None:
    """Return the active span's trace_id as lowercase hex, or None if no
    span is currently active (e.g. code path invoked outside a request).
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if not ctx.is_valid:
        return None
    return format(ctx.trace_id, "032x")
