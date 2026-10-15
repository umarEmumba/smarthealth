"""Synchronous cross-service reference validation (D-05).

A foreign key that crosses a service boundary (`users.clinic_id`,
`patients.user_id`, `providers.department_id`, ...) cannot be a database
constraint, so the WRITING service asks the OWNING service, over HTTP and at
write time, whether the target exists and is active. This module is that one
shared mechanism; every writing service uses it instead of rolling its own.

Contract (STRIDE T-01.1-21 / T-01.1-23):

  * 200            -> the owner's JSON body.
  * 404            -> the reference is missing (`ReferencedResourceInvalid`,
                      reason "missing", mapped to 422 by the caller).
  * `is_active` false -> reason "inactive" (also 422).
  * transport error, timeout, 401/403/5xx or an unreadable body
                   -> `UpstreamServiceUnavailable` (mapped to 503). It is NEVER a
                      422: an authentication misconfiguration between services
                      must not be mistaken for "that clinic does not exist".
  * The check NEVER fails open. If the owner cannot give a trustworthy answer,
    the caller refuses the write rather than persisting an unverified reference.

Retries are deliberately narrow: GET only (the client has no write method at
all), transport errors only (a business answer such as 404 is final), at most
`REFERENCE_RETRY_ATTEMPTS` attempts with exponential backoff capped at one
second, on top of the shared client's explicit per-request timeout.

Trace context rides on every attempt through `libs.http.outgoing_headers`, so a
reference check shows up as a child of the request that caused it. Logs carry
resource names, ids and outcomes only, never payloads.
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from libs.config import shared_settings
from libs.http import outgoing_headers
from libs.observability.logging import get_logger

log = get_logger(__name__)

# Total attempts per reference lookup (1 try + 2 retries), transport errors only.
REFERENCE_RETRY_ATTEMPTS = 3


class ReferencedResourceInvalid(Exception):
    """The referenced resource is missing or inactive. Callers map this to 422."""

    def __init__(self, resource: str, reason: str) -> None:
        super().__init__(f"{resource} {reason}")
        self.resource = resource
        self.reason = reason  # "missing" | "inactive"


class UpstreamServiceUnavailable(Exception):
    """The owning service could not give a trustworthy answer (unreachable,
    timed out, rejected the call, or misbehaved). Callers map this to 503."""

    def __init__(self, service: str) -> None:
        super().__init__(f"{service} service unavailable")
        self.service = service


def reference_error_detail(exc: ReferencedResourceInvalid) -> str:
    """Client-facing 422 detail: "<resource> not found" / "<resource> is inactive"."""
    if exc.reason == "inactive":
        return f"{exc.resource} is inactive"
    return f"{exc.resource} not found"


async def lookup_reference(
    http: httpx.AsyncClient,
    *,
    service: str,
    base_url: str,
    path: str,
    bearer_token: str,
) -> dict[str, Any] | None:
    """GET `{base_url}{path}` on the owning service and return its JSON body,
    or None when the owner says 404.

    Raises `UpstreamServiceUnavailable` for anything that is not a clean 200 or
    404, including a transport error that survives the bounded retries.
    """
    url = f"{base_url}{path}"

    async def _get_once() -> httpx.Response:
        # Headers are rebuilt per attempt so each carries the current span.
        return await http.get(
            url, headers=outgoing_headers({"authorization": f"Bearer {bearer_token}"})
        )

    retrying = AsyncRetrying(
        retry=retry_if_exception_type(httpx.TransportError),
        stop=stop_after_attempt(REFERENCE_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=0.1, max=1.0),
        reraise=True,
    )
    try:
        response = await retrying(_get_once)
    except httpx.TransportError as exc:
        log.warning(
            "service_client.reference_check.unreachable",
            service=service,
            error=type(exc).__name__,
        )
        raise UpstreamServiceUnavailable(service) from exc

    if response.status_code == 404:
        return None

    if response.status_code != 200:
        log.warning(
            "service_client.reference_check.unexpected_status",
            service=service,
            status=response.status_code,
        )
        raise UpstreamServiceUnavailable(service)

    try:
        body = response.json()
    except ValueError as exc:
        log.warning("service_client.reference_check.malformed_body", service=service)
        raise UpstreamServiceUnavailable(service) from exc
    if not isinstance(body, dict):
        log.warning("service_client.reference_check.malformed_body", service=service)
        raise UpstreamServiceUnavailable(service)
    return body


async def require_active_reference(
    http: httpx.AsyncClient,
    *,
    service: str,
    base_url: str,
    path: str,
    bearer_token: str,
    resource: str,
) -> dict[str, Any]:
    """Like `lookup_reference`, but the target must exist AND be active."""
    body = await lookup_reference(
        http, service=service, base_url=base_url, path=path, bearer_token=bearer_token
    )
    if body is None:
        log.info("service_client.reference_check.rejected", resource=resource, reason="missing")
        raise ReferencedResourceInvalid(resource, "missing")
    if body.get("is_active", True) is False:
        log.info("service_client.reference_check.rejected", resource=resource, reason="inactive")
        raise ReferencedResourceInvalid(resource, "inactive")
    return body


async def validate_clinic(
    http: httpx.AsyncClient, clinic_id: uuid.UUID, bearer_token: str
) -> dict[str, Any]:
    """The clinic must exist in Organization and be active (422 otherwise)."""
    return await require_active_reference(
        http,
        service="organization",
        base_url=shared_settings.organization_service_url,
        path=f"/internal/clinics/{clinic_id}",
        bearer_token=bearer_token,
        resource="clinic",
    )


async def lookup_clinic(
    http: httpx.AsyncClient, clinic_id: uuid.UUID, bearer_token: str
) -> dict[str, Any] | None:
    """The clinic's reference body, or None when missing. No active check: an
    inactive clinic is still a valid read scope (D-06)."""
    return await lookup_reference(
        http,
        service="organization",
        base_url=shared_settings.organization_service_url,
        path=f"/internal/clinics/{clinic_id}",
        bearer_token=bearer_token,
    )


async def validate_department(
    http: httpx.AsyncClient, department_id: uuid.UUID, bearer_token: str
) -> dict[str, Any]:
    """The department must exist in Organization and be active; the returned
    reference includes its `clinic_id`."""
    return await require_active_reference(
        http,
        service="organization",
        base_url=shared_settings.organization_service_url,
        path=f"/internal/departments/{department_id}",
        bearer_token=bearer_token,
        resource="department",
    )


async def validate_user(
    http: httpx.AsyncClient, user_id: uuid.UUID, bearer_token: str
) -> dict[str, Any]:
    """The account must exist in Identity (and be visible to the caller's clinic
    scope). Identity's reference carries no `is_active`, so presence is the check."""
    return await require_active_reference(
        http,
        service="identity",
        base_url=shared_settings.identity_service_url,
        path=f"/internal/users/{user_id}",
        bearer_token=bearer_token,
        resource="user",
    )
