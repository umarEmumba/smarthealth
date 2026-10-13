"""Unit tests (no infrastructure) for `libs.internal_client.internal_request`
(plan 02-01, Task 3; research G4, Pitfall 12).

The write-capable internal client is for Temporal activities: exactly ONE HTTP
attempt per call (Temporal owns retries), a bearer service token, an optional
deterministic Idempotency-Key, trace context on the request, and a typed error
contract — 404 / 409 / 422 are business outcomes, everything else that is not a
clean JSON success is one retryable `UpstreamServiceUnavailable`.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from libs.internal_client import (
    InternalConflict,
    InternalNotFound,
    InternalRejected,
    internal_request,
)
from libs.service_client import UpstreamServiceUnavailable

URL = "http://scheduling.internal/internal/appointments/abc/confirm?x=1"


class _Recorder:
    def __init__(self, respond: Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []
        self._respond = respond

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)


def _client(recorder: _Recorder) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(recorder))


async def test_post_returns_parsed_body_with_auth_idempotency_and_trace_headers() -> None:
    recorder = _Recorder(lambda r: httpx.Response(201, json={"id": "a1", "status": "reserved"}))
    tracer = TracerProvider().get_tracer(__name__)

    async with _client(recorder) as http:
        with tracer.start_as_current_span("activity"):
            body = await internal_request(
                http,
                "POST",
                URL,
                token="tok-123",
                service="scheduling",
                idempotency_key="wf-1:reserve",
                json={"slot_id": "s1"},
            )

    assert body == {"id": "a1", "status": "reserved"}
    (request,) = recorder.requests
    assert request.method == "POST"
    assert request.headers["authorization"] == "Bearer tok-123"
    assert request.headers["idempotency-key"] == "wf-1:reserve"
    assert "traceparent" in request.headers
    assert json.loads(request.content) == {"slot_id": "s1"}


async def test_no_idempotency_header_when_key_absent() -> None:
    recorder = _Recorder(lambda r: httpx.Response(200, json={"ok": True}))
    async with _client(recorder) as http:
        assert await internal_request(http, "GET", URL, token="t", service="scheduling") == {"ok": True}
    assert "idempotency-key" not in recorder.requests[0].headers


async def test_patch_204_returns_none() -> None:
    recorder = _Recorder(lambda r: httpx.Response(204))
    async with _client(recorder) as http:
        result = await internal_request(
            http, "PATCH", URL, token="t", service="scheduling", json={"status": "confirmed"}
        )
    assert result is None
    assert recorder.requests[0].method == "PATCH"


async def test_404_is_not_found() -> None:
    recorder = _Recorder(lambda r: httpx.Response(404, json={"detail": "not found"}))
    async with _client(recorder) as http:
        with pytest.raises(InternalNotFound):
            await internal_request(http, "GET", URL, token="t", service="scheduling")
    assert len(recorder.requests) == 1


async def test_409_is_conflict_with_detail() -> None:
    recorder = _Recorder(lambda r: httpx.Response(409, json={"detail": "slot already taken"}))
    async with _client(recorder) as http:
        with pytest.raises(InternalConflict) as excinfo:
            await internal_request(http, "POST", URL, token="t", service="scheduling", json={})
    assert excinfo.value.detail == "slot already taken"
    assert len(recorder.requests) == 1


async def test_422_is_rejected_with_detail() -> None:
    recorder = _Recorder(lambda r: httpx.Response(422, json={"detail": "provider is inactive"}))
    async with _client(recorder) as http:
        with pytest.raises(InternalRejected) as excinfo:
            await internal_request(http, "POST", URL, token="t", service="scheduling", json={})
    assert excinfo.value.detail == "provider is inactive"


@pytest.mark.parametrize(
    "body",
    [
        {"detail": "x" * 201},
        {"detail": [{"loc": ["body", "name"], "msg": "field required"}]},
        {"other": "shape"},
    ],
)
async def test_unusable_detail_is_replaced_by_a_fixed_string(body: dict) -> None:
    recorder = _Recorder(lambda r: httpx.Response(422, json=body))
    async with _client(recorder) as http:
        with pytest.raises(InternalRejected) as excinfo:
            await internal_request(http, "POST", URL, token="t", service="scheduling", json={})
    assert excinfo.value.detail == "request rejected"


async def test_non_json_409_uses_fixed_detail() -> None:
    recorder = _Recorder(lambda r: httpx.Response(409, text="<html>conflict</html>"))
    async with _client(recorder) as http:
        with pytest.raises(InternalConflict) as excinfo:
            await internal_request(http, "POST", URL, token="t", service="scheduling", json={})
    assert excinfo.value.detail == "conflict"


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(httpx.Response(401, json={"detail": "x"}), id="401"),
        pytest.param(httpx.Response(403, json={"detail": "x"}), id="403"),
        pytest.param(httpx.Response(500, text="boom"), id="500"),
        pytest.param(httpx.Response(503, json={"detail": "x"}), id="503"),
        pytest.param(httpx.Response(200, text="not json"), id="200-non-json"),
        pytest.param(httpx.Response(200, json=["a", "list"]), id="200-non-object"),
        pytest.param(httpx.Response(302, headers={"location": "/elsewhere"}), id="302"),
    ],
)
async def test_untrustworthy_answers_are_unavailable_after_one_attempt(
    response: httpx.Response,
) -> None:
    recorder = _Recorder(lambda r: response)
    async with _client(recorder) as http:
        with pytest.raises(UpstreamServiceUnavailable) as excinfo:
            await internal_request(http, "POST", URL, token="t", service="scheduling", json={})
    assert excinfo.value.service == "scheduling"
    assert len(recorder.requests) == 1


@pytest.mark.parametrize(
    "error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("bad")]
)
async def test_transport_errors_are_unavailable_after_one_attempt(error: Exception) -> None:
    def _raise(request: httpx.Request) -> httpx.Response:
        raise error

    recorder = _Recorder(_raise)
    async with _client(recorder) as http:
        with pytest.raises(UpstreamServiceUnavailable):
            await internal_request(http, "GET", URL, token="t", service="provider")
    assert len(recorder.requests) == 1


def test_module_imports_no_retry_library() -> None:
    import libs.internal_client as module  # noqa: PLC0415

    source = open(module.__file__, encoding="utf-8").read()
    assert "import tenacity" not in source and "from tenacity" not in source
