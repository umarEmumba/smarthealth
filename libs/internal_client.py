"""Write-capable internal client for Temporal activities (plan 02-01, research G4).

`libs/service_client.py` is the API-process client: GET-only reference checks
with a small bounded retry on transport errors. Activities need more — POST and
PATCH to another service's `/internal/*` route with a bearer SERVICE token and
a deterministic `Idempotency-Key` — and they must NOT retry internally: Temporal
owns the retry budget (activity retry policies, plans 02-06/02-07), so a hidden
retry loop here would multiply attempts and blur the timeout story. That is why
this is a separate module (research Pitfall 12) and why it pulls in no retry
library: exactly one HTTP attempt per call.

Outcome contract:

  * 200 / 201 with a JSON object body -> that dict; 204 -> None.
  * 404 -> `InternalNotFound` (a clean business answer).
  * 409 -> `InternalConflict(detail)`; 422 -> `InternalRejected(detail)`.
  * transport error, timeout, 401, 403, 5xx, any other status, or a success
    whose body is not a JSON object -> `UpstreamServiceUnavailable(service)`
    (reused from `libs.service_client`) — the one retryable outcome. An auth
    misconfiguration between services is never mistaken for a business answer.

`detail` is taken from the response's `detail` field only when it is a short
string (<= 200 chars); otherwise a fixed string, so a callee can never push an
arbitrary payload into workflow state. Trace context rides on every request via
`libs.http.outgoing_headers`. Logs carry method, path (no query string),
service and outcome only — never bodies, tokens or idempotency keys.
"""

from __future__ import annotations

from typing import Any, Literal

import httpx

from libs.http import outgoing_headers
from libs.observability.logging import get_logger
from libs.service_client import UpstreamServiceUnavailable

log = get_logger(__name__)

MAX_DETAIL_LENGTH = 200

Method = Literal["GET", "POST", "PATCH"]

__all__ = [
    "InternalConflict",
    "InternalNotFound",
    "InternalRejected",
    "UpstreamServiceUnavailable",
    "internal_request",
]


class InternalNotFound(Exception):
    """The callee answered 404: the target does not exist (not retryable)."""


class InternalConflict(Exception):
    """The callee answered 409 (e.g. slot already taken)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class InternalRejected(Exception):
    """The callee answered 422 (business-rule rejection)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def _detail(response: httpx.Response, fallback: str) -> str:
    try:
        body = response.json()
    except ValueError:
        return fallback
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, str) and detail and len(detail) <= MAX_DETAIL_LENGTH:
        return detail
    return fallback


async def internal_request(
    http: httpx.AsyncClient,
    method: Method,
    url: str,
    *,
    token: str,
    service: str,
    idempotency_key: str | None = None,
    json: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """One `method` request to `url` on `service` with a bearer service token.

    See the module docstring for the outcome contract. Exactly one attempt.
    """
    extra = {"authorization": f"Bearer {token}"}
    if idempotency_key:
        extra["idempotency-key"] = idempotency_key
    path = httpx.URL(url).path

    try:
        response = await http.request(method, url, headers=outgoing_headers(extra), json=json)
    except httpx.HTTPError as exc:
        log.warning(
            "internal.request",
            method=method,
            path=path,
            service=service,
            outcome="unreachable",
            error=type(exc).__name__,
        )
        raise UpstreamServiceUnavailable(service) from exc

    status = response.status_code
    log.info("internal.request", method=method, path=path, service=service, status=status)

    if status == 404:
        raise InternalNotFound(f"{service} resource not found")
    if status == 409:
        raise InternalConflict(_detail(response, "conflict"))
    if status == 422:
        raise InternalRejected(_detail(response, "request rejected"))
    if status == 204:
        return None
    if status in (200, 201):
        try:
            body = response.json()
        except ValueError as exc:
            log.warning("internal.request", method=method, path=path, service=service, outcome="malformed_body")
            raise UpstreamServiceUnavailable(service) from exc
        if isinstance(body, dict):
            return body
        log.warning("internal.request", method=method, path=path, service=service, outcome="malformed_body")
        raise UpstreamServiceUnavailable(service)

    raise UpstreamServiceUnavailable(service)
