"""Gateway routing, isolation and header-hygiene tests (STRIDE T-01.1-11/12/13).

Pure unit tests: the gateway app is driven in-process through an
`httpx.ASGITransport` (with a fixed ASGI client address) and its upstream is an
`httpx.MockTransport`, so no container and no other service is needed. The
MockTransport handler records every upstream request, which is how "never
forwarded" is asserted: the recorder stays empty.

Encoded traversal paths are sent literally: httpx normalises literal dot
segments client-side but leaves percent-encoded ones alone.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable

import httpx
import pytest
import pytest_asyncio

from services.gateway.app import create_app
from services.gateway.config import GatewaySettings
from services.gateway.proxy import HOP_BY_HOP_HEADERS
from services.gateway.routing import ROUTE_TABLE, is_safe_path, resolve_upstream

CLIENT_HOST = "203.0.113.7"


class Upstream:
    """MockTransport handler that records requests and delegates to `respond`."""

    def __init__(self, respond: Callable[[httpx.Request], httpx.Response] | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._respond = respond or (lambda request: httpx.Response(200, json={"ok": True}))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)


async def _gateway_client(upstream: Upstream) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(GatewaySettings(_env_file=None))
    app.state.internal_http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    transport = httpx.ASGITransport(app=app, client=(CLIENT_HOST, 5000))
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://gateway") as client:
            yield client
    finally:
        await app.state.internal_http_client.aclose()


@pytest_asyncio.fixture
async def upstream() -> Upstream:
    return Upstream()


@pytest_asyncio.fixture
async def gateway(upstream: Upstream) -> AsyncIterator[httpx.AsyncClient]:
    async for client in _gateway_client(upstream):
        yield client


# --- route table / pure functions -------------------------------------------


def test_route_table_maps_owners_and_has_no_internal_prefix() -> None:
    table = dict(ROUTE_TABLE)
    assert table == {
        "/.well-known/jwks.json": "identity",
        "/api/auth": "identity",
        "/api/admin": "identity",
        "/api/clinics": "organization",
        "/api/providers": "provider",
        "/api/patients": "patient",
        "/api/slots": "scheduling",
        "/api/appointments": "scheduling",
        "/api/waitlist": "scheduling",
        "/api/billing": "billing",
    }
    assert not [prefix for prefix in table if prefix.startswith("/internal")]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/auth/login", "identity"),
        ("/api/admin/ping", "identity"),
        ("/.well-known/jwks.json", "identity"),
        ("/api/clinics", "organization"),
        ("/api/clinics/abc/departments", "organization"),
        ("/api/providers/abc", "provider"),
        ("/api/patients", "patient"),
        ("/api/slots", "scheduling"),
        ("/api/slots/generate", "scheduling"),
        ("/api/appointments/abc/cancel", "scheduling"),
        ("/api/waitlist", "scheduling"),
        ("/api/billing/records", "billing"),
        ("/api/billing/records/abc", "billing"),
        ("/api/billingX", None),
        ("/internal/billing/prechecks", None),
        ("/internal/billing/records/abc/transitions", None),
        ("/api/slotsX", None),
        ("/internal/appointments/abc", None),
        ("/internal/slots", None),
        ("/api/clinicsX", None),
        ("/api/clinics-x/abc", None),
        ("/api/unknown", None),
        ("/internal/clinics/abc", None),
        ("/", None),
    ],
)
def test_resolve_upstream_matches_on_segment_boundary(path: str, expected: str | None) -> None:
    resolved = resolve_upstream(path, GatewaySettings(_env_file=None))
    if expected is None:
        assert resolved is None
    else:
        name, base_url = resolved  # type: ignore[misc]
        assert name == expected
        assert base_url == getattr(GatewaySettings(_env_file=None), f"{expected}_service_url")


@pytest.mark.parametrize(
    ("path", "raw_path"),
    [
        ("/api/clinics/../x", b"/api/clinics/../x"),
        ("/api/clinics/./x", b"/api/clinics/./x"),
        ("/api//clinics", b"/api//clinics"),
        ("/api/clinics\\x", b"/api/clinics%5Cx"),
        ("/api/clinics/\x00", b"/api/clinics/%00"),
        ("/api/clinics/../x", b"/api/clinics/%2e%2e/x"),
        ("/api/clinics/a.b", b"/api/clinics/a%2Eb"),
        ("/api/clinics/a/b", b"/api/clinics/a%2fb"),
        ("/api/clinics/a/b", b"/api/clinics/a%2Fb"),
        ("/api/clinics/a\\b", b"/api/clinics/a%5cb"),
    ],
)
def test_is_safe_path_rejects_traversal_and_encoded_separators(path: str, raw_path: bytes) -> None:
    assert is_safe_path(path, raw_path) is False


def test_is_safe_path_accepts_ordinary_paths() -> None:
    assert is_safe_path("/api/clinics", b"/api/clinics")
    assert is_safe_path(
        "/api/clinics/0b6f7e0e-1c9d-4c4e-9c5e-3f1c2a7d8e11/departments",
        b"/api/clinics/0b6f7e0e-1c9d-4c4e-9c5e-3f1c2a7d8e11/departments",
    )
    assert is_safe_path("/api/clinics/a?b", b"/api/clinics/a%3Fb")


# --- isolation: nothing outside the table is ever forwarded ------------------


async def test_internal_paths_are_404_and_never_forwarded(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    for path in (
        "/internal/clinics/x",
        "/internal/departments/x",
        "/internal/appointments/x",
        "/internal/billing/prechecks",
        "/internal/billing/records/x/transitions",
        "/internal/billing/records/x/relink",
        "/internal",
    ):
        response = await gateway.get(path)
        assert response.status_code == 404, path
        assert response.json() == {"detail": "not found"}
    assert upstream.requests == []


async def test_unknown_prefix_is_404_and_never_forwarded(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    for path in ("/api/unknown", "/api/scheduling/x", "/openapi.json", "/docs", "/"):
        response = await gateway.get(path)
        assert response.status_code == 404, path
    assert upstream.requests == []


async def test_prefix_lookalike_is_404_and_never_forwarded(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    response = await gateway.get("/api/clinicsX")
    assert response.status_code == 404
    assert upstream.requests == []


async def test_encoded_traversal_to_internal_is_404_and_never_forwarded(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    paths = (
        "/api/clinics/%2e%2e/%2e%2e/internal/departments/x",
        "/api/clinics/%2E%2E/internal/clinics/x",
        "/api/clinics/..%2finternal/clinics/x",
        "/api/clinics/%5c..%5cinternal",
        "/api/clinics/%00",
    )
    for path in paths:
        response = await gateway.get(path)
        assert response.status_code == 404, path
    assert upstream.requests == []


async def test_empty_path_segment_is_404_and_never_forwarded(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    response = await gateway.get("/api//clinics")
    assert response.status_code == 404
    assert upstream.requests == []


# --- forwarding fidelity -----------------------------------------------------


async def test_forward_drops_hop_by_hop_headers_and_keeps_authorization(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    response = await gateway.get(
        "/api/clinics",
        headers={
            "Authorization": "Bearer synthetic-token",
            "Proxy-Authorization": "Basic c3ludGhldGlj",
            "Connection": "keep-alive, X-Custom-Hop",
            "X-Custom-Hop": "listed-in-connection",
            "X-App-Header": "kept",
        },
    )
    assert response.status_code == 200
    (forwarded,) = upstream.requests
    assert forwarded.headers["authorization"] == "Bearer synthetic-token"
    assert forwarded.headers["x-app-header"] == "kept"
    assert "proxy-authorization" not in forwarded.headers
    assert "x-custom-hop" not in forwarded.headers
    assert "proxy-authenticate" not in forwarded.headers
    # `host` is the upstream's, not the gateway's.
    assert forwarded.headers["host"] != "gateway"


async def test_x_forwarded_for_is_overwritten_with_the_asgi_client_host(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    await gateway.get(
        "/api/clinics",
        headers={"X-Forwarded-For": "6.6.6.6", "X-Forwarded-Proto": "gopher"},
    )
    (forwarded,) = upstream.requests
    assert forwarded.headers.get_list("x-forwarded-for") == [CLIENT_HOST]
    assert forwarded.headers.get_list("x-forwarded-proto") == ["http"]


async def test_method_body_and_query_are_forwarded_to_the_owning_service(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    response = await gateway.post(
        "/api/clinics/abc/departments?include_inactive=true",
        json={"name": "Cardiology"},
    )
    assert response.status_code == 200
    (forwarded,) = upstream.requests
    assert forwarded.method == "POST"
    assert forwarded.url.host == "organization"
    assert forwarded.url.raw_path == b"/api/clinics/abc/departments?include_inactive=true"
    assert forwarded.content == b'{"name":"Cardiology"}'


async def test_encoded_question_mark_never_becomes_a_query_string(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    await gateway.get("/api/clinics/a%3Fb")
    (forwarded,) = upstream.requests
    assert forwarded.url.raw_path == b"/api/clinics/a%3Fb"
    assert forwarded.url.query == b""


async def test_upstream_response_is_passed_through_unchanged() -> None:
    body = b'{"id": "abc", "name": "Zed"}'

    def respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            201,
            content=body,
            headers=[
                ("content-type", "application/json"),
                ("x-upstream-marker", "m1"),
                ("set-cookie", "a=1"),
                ("set-cookie", "b=2"),
                ("keep-alive", "timeout=5"),
                ("connection", "close"),
            ],
        )

    async for client in _gateway_client(Upstream(respond)):
        response = await client.post("/api/clinics", json={"name": "x", "timezone": "UTC"})

    assert response.status_code == 201
    assert response.content == body
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-upstream-marker"] == "m1"
    assert response.headers.get_list("set-cookie") == ["a=1", "b=2"]
    assert "keep-alive" not in response.headers
    assert "connection" not in response.headers


async def test_upstream_error_status_is_returned_as_is() -> None:
    def respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "insufficient role for this operation"})

    async for client in _gateway_client(Upstream(respond)):
        response = await client.get("/api/clinics")

    assert response.status_code == 403
    assert response.json() == {"detail": "insufficient role for this operation"}


# --- upstream failure mapping ------------------------------------------------


async def test_unreachable_upstream_yields_503_without_retry() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    upstream = Upstream(respond)
    async for client in _gateway_client(upstream):
        response = await client.post("/api/clinics", json={"name": "x", "timezone": "UTC"})

    assert response.status_code == 503
    assert response.json() == {"detail": "upstream service unavailable"}
    assert len(upstream.requests) == 1  # exactly once: a retried POST could double-apply


async def test_upstream_timeout_yields_504_without_retry() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    upstream = Upstream(respond)
    async for client in _gateway_client(upstream):
        response = await client.get("/api/clinics")

    assert response.status_code == 504
    assert response.json() == {"detail": "upstream service timed out"}
    assert len(upstream.requests) == 1


# --- gateway-owned endpoints -------------------------------------------------


async def test_health_is_answered_by_the_gateway_without_touching_the_upstream(
    gateway: httpx.AsyncClient, upstream: Upstream
) -> None:
    response = await gateway.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "gateway"}
    assert upstream.requests == []


def test_hop_by_hop_constant_covers_the_forwarding_headers() -> None:
    assert {"proxy-authorization", "x-forwarded-for", "x-forwarded-proto", "host"} <= HOP_BY_HOP_HEADERS
