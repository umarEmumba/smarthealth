"""Trace-context extraction from Kafka message headers (plan 03-04, D-11).

The outbox row stores the producing span's W3C `traceparent`; Debezium forwards
it as a Kafka header. The consumer base extracts it here and starts its
processing span as a CHILD of the producer's span, so one booking stays one
trace across the HTTP request, the saga, the outbox, Kafka and the consumer.

This is deliberately hand-written instead of
`opentelemetry-instrumentation-confluent-kafka`: that package starts a new
"recv" span and only LINKS the producer context, which breaks the single-trace
criterion (research Pitfall 6).
"""

from __future__ import annotations

from collections.abc import Iterable

from opentelemetry import propagate
from opentelemetry.context import Context


def extract_context(headers: Iterable[tuple[str, bytes | str | None]] | None) -> Context:
    """OpenTelemetry context carried by a confluent-kafka header list.

    `Message.headers()` returns a list of `(key, value)` or None. A value is
    None when the producer wrote a null header: Debezium does that when the
    outbox `traceparent` column is NULL (research Pitfall 16), so None values
    are skipped, never decoded. Undecodable bytes are tolerated (UTF-8,
    errors ignored); a missing or malformed traceparent simply yields a
    context with no remote parent, so the span becomes a root span.
    """
    carrier: dict[str, str] = {}
    for key, value in headers or ():
        if value is None:
            continue
        carrier[key] = (
            value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
        )
    return propagate.extract(carrier)
