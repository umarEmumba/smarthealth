"""httpx client-span instrumentation is installed exactly once per process.

Unit tests only: no container is needed. `setup_tracing` runs once per service
app build, and a process may build several apps (the test harness builds three),
so the httpx instrumentation must be idempotent — a second `instrument()` call
on an already-instrumented library only produces a noisy warning and no value.
"""

from __future__ import annotations

import logging

import pytest
from fastapi import FastAPI
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

from libs.observability import otel


def test_setup_tracing_instruments_httpx_once_across_apps(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        otel.setup_tracing(FastAPI(), service_name="smarthealth-test-a")
        otel.setup_tracing(FastAPI(), service_name="smarthealth-test-b")

    assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert otel._HTTPX_INSTRUMENTED is True
    assert not [r for r in caplog.records if "already instrumented" in r.getMessage().lower()]


def test_instrument_httpx_client_spans_is_idempotent() -> None:
    otel.instrument_httpx_client_spans()
    otel.instrument_httpx_client_spans()
    assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert otel._HTTPX_INSTRUMENTED is True
