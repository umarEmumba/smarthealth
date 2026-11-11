"""One shared place that turns on the per-handler HTTP latency histogram (plan 03-16, D-04).

`prometheus-fastapi-instrumentator` exposes `http_request_duration_seconds{handler,method}`
with only three finite buckets by default (0.1, 0.5 and 1 s). `histogram_quantile`
interpolates inside a bucket, so with those buckets a p95 anywhere below 0.1 s reads as
at most 0.1 s and the NFR-03 / NFR-04 thresholds (0.2 s, 0.3 s, 0.5 s, 0.8 s) fall in the
middle of a wide bucket. These buckets put an edge on every NFR latency threshold in
`loadtest/nfr.json` (0.2, 0.3, 0.5, 0.8 s) and resolution below them.

Cost: the same one `observe()` per request as before (a bucket lookup is cheap), so the
booking accept path gains no work (D-04). The series count grows with the bucket count
only (handler and method labels are unchanged, so cardinality stays bounded, T-3-48).

Every service calls `instrument_http(app)` instead of building its own `Instrumentator()`.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

__all__ = ["HTTP_LATENCY_BUCKETS", "instrument_http"]

# Seconds. Edges on the NFR thresholds: 0.2 (read p95), 0.3 (book p95), 0.5 (read p99),
# 0.8 (book p99); finer below them for the fast paths; 1 to 5 s for the slow tail.
HTTP_LATENCY_BUCKETS: tuple[float, ...] = (0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 2.5, 5.0)


def instrument_http(app: FastAPI, *, registry: Any | None = None) -> Instrumentator:
    """Add the HTTP metrics middleware to `app` and return the Instrumentator, so a caller
    can still chain `.expose(app)`. `registry` is for tests (default: the process registry)."""
    instrumentator = Instrumentator(registry=registry) if registry is not None else Instrumentator()
    return instrumentator.instrument(app, latency_lowr_buckets=HTTP_LATENCY_BUCKETS)
