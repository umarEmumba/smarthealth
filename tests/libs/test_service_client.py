"""Unit tests for the shared cross-service reference client (D-05, plan 01.1-05).

No containers and no ASGI apps: every test talks to an `httpx.MockTransport`
that records the requests it was asked to serve. That makes the contract exact
and cheap to assert:

  * 200 -> body, 404 -> None / "missing", `is_active: false` -> "inactive";
  * 401/403/5xx or an unreadable body -> `UpstreamServiceUnavailable`, NEVER
    `ReferencedResourceInvalid` (an auth misconfiguration is not "not found");
  * transport errors are retried, at most 3 attempts in total; a business answer
    (404) or an error status is never retried; the client only ever sends GET;
  * the forwarded bearer token and the W3C trace context ride on every attempt.
"""

from __future__ import annotations

import json
import re
import uuid

import httpx
import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider

from libs import service_client
from libs.config import shared_settings
from libs.observability.logging import configure_logging
from libs.service_client import (
    REFERENCE_RETRY_ATTEMPTS,
    ReferencedResourceInvalid,
    UpstreamServiceUnavailable,
    lookup_clinic,
    lookup_reference,
    reference_error_detail,
    require_active_reference,
    validate_clinic,
    validate_department,
    validate_user,
)

BASE = "http://owner.test"
PATH = "/internal/things/1"
TOKEN = "header.payload.signature"


class Recorder:
    """MockTransport handler: serves scripted responses/errors in order and
    records every request."""

    def __init__(self, *script: httpx.Response | Exception) -> None:
        self._script = list(script)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        step = self._script[min(len(self.requests), len(self._script)) - 1]
        if isinstance(step, Exception):
            raise step
        return step

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


def _connect_error() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused")


async def _lookup(recorder: Recorder) -> dict | None:
    async with recorder.client() as http:
        return await lookup_reference(
            http, service="owner", base_url=BASE, path=PATH, bearer_token=TOKEN
        )


async def _require(recorder: Recorder) -> dict:
    async with recorder.client() as http:
        return await require_active_reference(
            http,
            service="owner",
            base_url=BASE,
            path=PATH,
            bearer_token=TOKEN,
            resource="thing",
        )


# --- answers -----------------------------------------------------------------


async def test_200_body_is_returned() -> None:
    recorder = Recorder(httpx.Response(200, json={"id": "1", "is_active": True}))

    assert await _lookup(recorder) == {"id": "1", "is_active": True}
    assert len(recorder.requests) == 1


async def test_404_is_none_and_require_reports_missing() -> None:
    assert await _lookup(Recorder(httpx.Response(404, json={"detail": "not found"}))) is None

    with pytest.raises(ReferencedResourceInvalid) as excinfo:
        await _require(Recorder(httpx.Response(404, json={"detail": "not found"})))
    assert excinfo.value.reason == "missing"
    assert excinfo.value.resource == "thing"


async def test_inactive_reference_reports_inactive() -> None:
    recorder = Recorder(httpx.Response(200, json={"id": "1", "is_active": False}))

    with pytest.raises(ReferencedResourceInvalid) as excinfo:
        await _require(recorder)
    assert excinfo.value.reason == "inactive"


async def test_reference_without_is_active_counts_as_active() -> None:
    """Identity's user reference carries no is_active; presence is the check."""
    assert await _require(Recorder(httpx.Response(200, json={"id": "1", "role": "patient"}))) == {
        "id": "1",
        "role": "patient",
    }


# --- fail closed ---------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403, 500, 502, 503])
async def test_error_status_is_unavailable_never_a_reference_error(status: int) -> None:
    recorder = Recorder(httpx.Response(status, json={"detail": "nope"}))

    with pytest.raises(UpstreamServiceUnavailable) as excinfo:
        await _require(recorder)

    assert excinfo.value.service == "owner"
    assert not isinstance(excinfo.value, ReferencedResourceInvalid)
    # A status answer is final: not retried.
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("body", [b"not json at all", b'["a", "list"]', b'"a string"'])
async def test_unreadable_or_non_object_body_is_unavailable(body: bytes) -> None:
    recorder = Recorder(httpx.Response(200, content=body))

    with pytest.raises(UpstreamServiceUnavailable):
        await _lookup(recorder)


async def test_unexpected_status_is_logged_with_service_and_status_only(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging()
    # A logger first used inside this test binds to THIS test's captured stdout.
    monkeypatch.setattr(service_client, "log", structlog.get_logger("test-service-client"))

    with pytest.raises(UpstreamServiceUnavailable):
        await _lookup(Recorder(httpx.Response(401, json={"detail": "secret-detail-text"})))

    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    flagged = [e for e in events if e.get("event") == "service_client.reference_check.unexpected_status"]
    assert len(flagged) == 1
    assert flagged[0]["level"] == "warning"
    assert flagged[0]["service"] == "owner"
    assert flagged[0]["status"] == 401
    dumped = json.dumps(flagged[0])
    assert TOKEN not in dumped
    assert "secret-detail-text" not in dumped


# --- retries ---------------------------------------------------------------------


def test_retry_budget_is_three_attempts() -> None:
    assert REFERENCE_RETRY_ATTEMPTS == 3


async def test_transport_error_twice_then_success_takes_exactly_three_requests() -> None:
    recorder = Recorder(
        _connect_error(),
        _connect_error(),
        httpx.Response(200, json={"id": "1", "is_active": True}),
    )

    assert await _lookup(recorder) == {"id": "1", "is_active": True}
    assert len(recorder.requests) == 3


async def test_transport_error_three_times_is_unavailable_after_exactly_three_requests() -> None:
    recorder = Recorder(_connect_error())

    with pytest.raises(UpstreamServiceUnavailable) as excinfo:
        await _lookup(recorder)

    assert excinfo.value.service == "owner"
    assert len(recorder.requests) == 3


async def test_timeout_counts_as_a_transport_error() -> None:
    recorder = Recorder(httpx.ReadTimeout("timed out"))

    with pytest.raises(UpstreamServiceUnavailable):
        await _lookup(recorder)
    assert len(recorder.requests) == 3


async def test_a_business_answer_is_never_retried() -> None:
    recorder = Recorder(httpx.Response(404, json={"detail": "not found"}))

    assert await _lookup(recorder) is None
    assert len(recorder.requests) == 1


# --- request shape -------------------------------------------------------------------


async def test_request_is_a_get_with_the_forwarded_bearer_token() -> None:
    recorder = Recorder(httpx.Response(200, json={"id": "1"}))

    await _lookup(recorder)

    (request,) = recorder.requests
    assert request.method == "GET"
    assert str(request.url) == f"{BASE}{PATH}"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.content == b""


async def test_request_inside_an_active_span_carries_traceparent() -> None:
    recorder = Recorder(httpx.Response(200, json={"id": "1"}))
    tracer = TracerProvider().get_tracer("test-service-client")

    with tracer.start_as_current_span("caller") as span:
        expected_trace_id = format(span.get_span_context().trace_id, "032x")
        await _lookup(recorder)

    (request,) = recorder.requests
    assert re.fullmatch(
        rf"00-{expected_trace_id}-[0-9a-f]{{16}}-[0-9a-f]{{2}}", request.headers["traceparent"]
    )
    assert "baggage" not in request.headers


async def test_every_retry_attempt_carries_the_token() -> None:
    recorder = Recorder(_connect_error(), httpx.Response(200, json={"id": "1"}))

    await _lookup(recorder)

    assert [r.headers["authorization"] for r in recorder.requests] == [f"Bearer {TOKEN}"] * 2


# --- typed helpers ---------------------------------------------------------------------


async def test_validate_clinic_targets_organization_and_requires_active() -> None:
    clinic_id = uuid.uuid4()
    recorder = Recorder(httpx.Response(200, json={"id": str(clinic_id), "is_active": True}))
    async with recorder.client() as http:
        body = await validate_clinic(http, clinic_id, TOKEN)

    assert body["id"] == str(clinic_id)
    (request,) = recorder.requests
    assert str(request.url) == f"{shared_settings.organization_service_url}/internal/clinics/{clinic_id}"

    inactive = Recorder(httpx.Response(200, json={"id": str(clinic_id), "is_active": False}))
    async with inactive.client() as http:
        with pytest.raises(ReferencedResourceInvalid) as excinfo:
            await validate_clinic(http, clinic_id, TOKEN)
    assert (excinfo.value.resource, excinfo.value.reason) == ("clinic", "inactive")


async def test_lookup_clinic_reports_inactive_clinics_without_raising() -> None:
    clinic_id = uuid.uuid4()
    recorder = Recorder(httpx.Response(200, json={"id": str(clinic_id), "is_active": False}))
    async with recorder.client() as http:
        assert await lookup_clinic(http, clinic_id, TOKEN) == {
            "id": str(clinic_id),
            "is_active": False,
        }

    missing = Recorder(httpx.Response(404, json={"detail": "not found"}))
    async with missing.client() as http:
        assert await lookup_clinic(http, clinic_id, TOKEN) is None


async def test_validate_department_returns_the_reference_with_its_clinic() -> None:
    department_id, clinic_id = uuid.uuid4(), uuid.uuid4()
    recorder = Recorder(
        httpx.Response(
            200,
            json={"id": str(department_id), "clinic_id": str(clinic_id), "is_active": True},
        )
    )
    async with recorder.client() as http:
        body = await validate_department(http, department_id, TOKEN)

    assert body["clinic_id"] == str(clinic_id)
    (request,) = recorder.requests
    assert request.url.path == f"/internal/departments/{department_id}"


async def test_validate_user_targets_identity_and_names_the_user_resource() -> None:
    user_id = uuid.uuid4()
    recorder = Recorder(httpx.Response(404, json={"detail": "not found"}))
    async with recorder.client() as http:
        with pytest.raises(ReferencedResourceInvalid) as excinfo:
            await validate_user(http, user_id, TOKEN)

    assert (excinfo.value.resource, excinfo.value.reason) == ("user", "missing")
    (request,) = recorder.requests
    assert str(request.url) == f"{shared_settings.identity_service_url}/internal/users/{user_id}"


def test_reference_error_detail_wording() -> None:
    assert reference_error_detail(ReferencedResourceInvalid("clinic", "missing")) == "clinic not found"
    assert reference_error_detail(ReferencedResourceInvalid("clinic", "inactive")) == "clinic is inactive"
    assert reference_error_detail(ReferencedResourceInvalid("user", "missing")) == "user not found"
