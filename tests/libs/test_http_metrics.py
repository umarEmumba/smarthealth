"""The shared HTTP latency histogram has an edge on every NFR threshold (plan 03-16, D-04)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from libs.observability.http_metrics import HTTP_LATENCY_BUCKETS, instrument_http

NFR = json.loads((Path(__file__).resolve().parents[2] / "loadtest" / "nfr.json").read_text())


@pytest.mark.parametrize(
    "threshold_seconds",
    [NFR["book_p95_ms"] / 1000, NFR["book_p99_ms"] / 1000, NFR["read_p95_ms"] / 1000, NFR["read_p99_ms"] / 1000],
)
def test_every_latency_threshold_in_nfr_json_is_a_bucket_edge(threshold_seconds: float) -> None:
    assert threshold_seconds in HTTP_LATENCY_BUCKETS


def test_buckets_are_sorted_finite_and_few() -> None:
    assert list(HTTP_LATENCY_BUCKETS) == sorted(set(HTTP_LATENCY_BUCKETS))
    assert 8 <= len(HTTP_LATENCY_BUCKETS) <= 12  # cardinality stays bounded (T-3-48)


def test_the_per_handler_histogram_exposes_the_finer_buckets_and_keeps_its_labels() -> None:
    registry = CollectorRegistry()
    app = FastAPI()

    @app.post("/api/things/{thing_id}")
    async def create(thing_id: str) -> dict[str, str]:
        return {"id": thing_id}

    instrument_http(app, registry=registry).expose(app)
    client = TestClient(app)
    assert client.post("/api/things/abc").status_code == 200
    body = client.get("/metrics").text

    lines = [
        line for line in body.splitlines() if line.startswith("http_request_duration_seconds_bucket{")
    ]
    edges = {line.split('le="', 1)[1].split('"', 1)[0] for line in lines}
    assert {"0.025", "0.2", "0.3", "0.8", "5.0", "+Inf"} <= edges
    assert "0.1" in edges and "0.5" in edges and "1.0" in edges  # the old edges are kept
    # Labels are unchanged: the route template, never the concrete id.
    assert all('handler="/api/things/{thing_id}"' in line and 'method="POST"' in line for line in lines)
    assert "abc" not in body.split("http_request_duration_seconds_bucket", 1)[1].split("\n", 1)[0]
