"""Thin reverse proxy: forward one request to one upstream, unchanged.

Guarantees (D-07, STRIDE T-01.1-12/13):
  * One route -> one service. No fan-out, no aggregation, no enrichment, no
    rewriting of the upstream body: status, body bytes and headers (minus
    hop-by-hop ones) come back exactly as the owning service produced them.
  * The upstream URL is built from the RAW request path (`scope["raw_path"]`),
    so percent-encoding is preserved end to end — an encoded `?` can never turn
    into a query string and an encoded `/` can never become a path separator.
  * Hop-by-hop headers, and any header the client named in `Connection`, are
    dropped; `x-forwarded-for` / `x-forwarded-proto` are always OVERWRITTEN
    with values the gateway observed itself (a client cannot spoof its address).
  * The upstream call is made exactly ONCE with an explicit timeout — no retry,
    for any method (a retried POST could double-apply a write). Upstream
    failure maps to 503 (unreachable) / 504 (timed out), never a hang or a 500.

Logs carry the upstream name and request path only — never headers, tokens or
bodies (PHI / credential safety).
"""

from __future__ import annotations

import httpx
from fastapi import Request, Response
from fastapi.responses import JSONResponse

from libs.http import outgoing_headers
from libs.observability.logging import get_logger

log = get_logger(__name__)

# Hop-by-hop headers (RFC 9110 §7.6.1) plus the ones the gateway must own:
# `host` / `content-length` are recomputed for the upstream call, and the
# forwarding headers are overwritten below rather than trusted from the client.
HOP_BY_HOP_HEADERS: frozenset[str] = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "x-forwarded-for",
        "x-forwarded-proto",
    }
)

# Request headers the gateway never forwards as sent: `accept-encoding` is
# dropped so httpx advertises only the encodings it can itself decode (we
# return decoded bytes and strip content-encoding); the W3C trace headers are
# replaced by the gateway span's own context (see `_forward_headers`).
_REPLACED_REQUEST_HEADERS: frozenset[str] = frozenset(
    {"accept-encoding", "traceparent", "tracestate"}
)

# Response-only: httpx has already decoded the upstream body, so the encoding
# header no longer describes the bytes we hand back.
_RESPONSE_STRIP = HOP_BY_HOP_HEADERS | {"content-encoding"}


def _connection_listed(request: Request) -> set[str]:
    """Header names the client listed in `Connection` (they are hop-by-hop for
    this hop by definition, RFC 9110 §7.6.1)."""
    listed: set[str] = set()
    for value in request.headers.getlist("connection"):
        listed.update(token.strip().lower() for token in value.split(",") if token.strip())
    return listed


def _forward_headers(request: Request) -> list[tuple[str, str]]:
    """Upstream request headers: the client's (minus hop-by-hop), the gateway's
    own forwarding headers, and the gateway span's trace context.

    Any inbound `traceparent` / `tracestate` is dropped first and re-injected
    from the CURRENT span, so the gateway's server span is the parent of the
    upstream server span and a client cannot splice a foreign parent in."""
    dropped = HOP_BY_HOP_HEADERS | _connection_listed(request)
    headers: list[tuple[str, str]] = []
    for raw_name, raw_value in request.headers.raw:
        name = raw_name.decode("latin-1")
        lowered = name.lower()
        if lowered in dropped or lowered in _REPLACED_REQUEST_HEADERS:
            continue
        headers.append((name, raw_value.decode("latin-1")))

    if request.client is not None:
        headers.append(("x-forwarded-for", request.client.host))
    headers.append(("x-forwarded-proto", request.url.scheme))
    headers.extend(outgoing_headers().items())
    return headers


def _upstream_url(request: Request, base_url: str) -> str:
    raw_path: bytes = request.scope.get("raw_path") or request.url.path.encode("ascii", "ignore")
    url = base_url.rstrip("/") + raw_path.decode("ascii", "ignore")
    query: bytes = request.scope.get("query_string", b"")
    if query:
        url += "?" + query.decode("ascii", "ignore")
    return url


async def forward(
    request: Request,
    *,
    upstream_name: str,
    base_url: str,
    http: httpx.AsyncClient,
    timeout: float,
) -> Response:
    path = request.url.path
    try:
        upstream = await http.request(
            request.method,
            _upstream_url(request, base_url),
            headers=_forward_headers(request),
            content=await request.body(),
            timeout=timeout,
        )
    except httpx.TimeoutException:
        log.warning("gateway.upstream.timeout", upstream=upstream_name, path=path)
        return JSONResponse(status_code=504, content={"detail": "upstream service timed out"})
    except httpx.TransportError:
        log.warning("gateway.upstream.unavailable", upstream=upstream_name, path=path)
        return JSONResponse(status_code=503, content={"detail": "upstream service unavailable"})

    response = Response(content=upstream.content, status_code=upstream.status_code)
    for name, value in upstream.headers.multi_items():
        if name.lower() in _RESPONSE_STRIP:
            continue
        # append (not assign) so repeated headers such as Set-Cookie survive.
        response.headers.append(name, value)
    return response
