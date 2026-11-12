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

Multiprocess exposition (plan 03-24, CR-02, D-14): the gunicorn APIs run two workers, and
`prometheus_client` keeps one registry per process. When `PROMETHEUS_MULTIPROC_DIR` is set
(by the API image's default command only) each worker writes to memory-mapped files and a
scrape must sum them: `Instrumentator.expose()` does that by itself, and `render_latest()`
is the same thing for a hand-written `/metrics` route (the gateway's). With the variable
unset (tests, Temporal workers, consumers) both fall back to the process registry. Note a
multiprocess exposition carries only the metrics created through `prometheus_client`
(no default `process_*` / `python_*` collectors).
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest, multiprocess
from prometheus_fastapi_instrumentator import Instrumentator

__all__ = ["HTTP_LATENCY_BUCKETS", "exposition_registry", "instrument_http", "render_latest"]

# Seconds. Edges on the NFR thresholds: 0.2 (read p95), 0.3 (book p95), 0.5 (read p99),
# 0.8 (book p99); finer below them for the fast paths; 1 to 5 s for the slow tail.
HTTP_LATENCY_BUCKETS: tuple[float, ...] = (0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 2.5, 5.0)


def instrument_http(app: FastAPI, *, registry: Any | None = None) -> Instrumentator:
    """Add the HTTP metrics middleware to `app` and return the Instrumentator, so a caller
    can still chain `.expose(app)`. `registry` is for tests (default: the process registry)."""
    instrumentator = Instrumentator(registry=registry) if registry is not None else Instrumentator()
    return instrumentator.instrument(app, latency_lowr_buckets=HTTP_LATENCY_BUCKETS)


def exposition_registry() -> CollectorRegistry:
    """The registry a scrape must be rendered from: a fresh registry that sums the per-process
    files when `PROMETHEUS_MULTIPROC_DIR` is set, else the default process registry."""
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return registry
    return REGISTRY


def render_latest() -> bytes:
    """Prometheus text exposition of `exposition_registry()`."""
    return generate_latest(exposition_registry())
