"""Scheduling-side reference reads over aud-scoped service tokens (plan 02-04).

Scheduling needs other services' data to generate and book slots: the
provider and its availability templates (Provider), the provider's department
and clinic timezone (Organization), and the patient (Patient). Every read is a
GET to the owner's `/internal/*` route through `libs.internal_client.
internal_request`, authenticated with a SERVICE token whose `aud` is the owning
service (`ServiceTokenProvider.get(<aud>)`, D-06). The caller's own JWT is never
forwarded: these reads also run where no user exists (Temporal activities).

Outcomes (the `internal_request` contract):
  * the owner's JSON body on 200 (a list for the availability read);
  * None when the owner answers 404 (`InternalNotFound`): missing, or outside
    the scope the owner applies;
  * `UpstreamServiceUnavailable` (owner unreachable / misbehaving) and
    `ServiceTokenUnavailable` (Identity could not mint) propagate: the caller
    fails closed (503 in the API, a retryable error in an activity).

Requests carry ids only, never PHI. Exactly one attempt per call: the API maps
an outage to 503 and Temporal owns activity retries.
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx

from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalNotFound, internal_request
from services.scheduling.config import settings

__all__ = [
    "ack_schedule_change",
    "get_clinic_ref",
    "get_department_ref",
    "get_patient_by_user",
    "get_patient_ref",
    "get_provider_availability",
    "get_provider_by_user",
    "get_provider_ref",
    "list_schedule_changes",
]


async def _get(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    *,
    service: str,
    base_url: str,
    path: str,
    allow_list: bool = False,
) -> Any:
    token = await tokens.get(service)
    try:
        return await internal_request(
            http, "GET", f"{base_url}{path}", token=token, service=service, allow_list=allow_list
        )
    except InternalNotFound:
        return None


async def get_provider_ref(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, provider_id: uuid.UUID
) -> dict[str, Any] | None:
    """Provider's `{id, department_id, is_active}`, or None if unknown."""
    return await _get(
        http,
        tokens,
        service="provider",
        base_url=settings.provider_service_url,
        path=f"/internal/providers/{provider_id}",
    )


async def get_provider_by_user(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, user_id: uuid.UUID
) -> dict[str, Any] | None:
    """Provider's `{id, department_id, is_active}` for the provider profile linked
    to login account `user_id`, or None (plan 02.1-12). The same lookup Visit uses
    to decide provider ownership; the request carries the account id only."""
    return await _get(
        http,
        tokens,
        service="provider",
        base_url=settings.provider_service_url,
        path=f"/internal/providers/by-user/{user_id}",
    )


async def get_provider_availability(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, provider_id: uuid.UUID
) -> list[dict[str, Any]] | None:
    """ALL of the provider's templates (inactive included; the generator
    filters), `[]` if it has none, or None if the provider is unknown."""
    return await _get(
        http,
        tokens,
        service="provider",
        base_url=settings.provider_service_url,
        path=f"/internal/providers/{provider_id}/availability",
        allow_list=True,
    )


async def get_department_ref(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, department_id: uuid.UUID
) -> dict[str, Any] | None:
    """Organization's `{id, clinic_id, is_active}`, or None if unknown."""
    return await _get(
        http,
        tokens,
        service="organization",
        base_url=settings.organization_service_url,
        path=f"/internal/departments/{department_id}",
    )


async def get_clinic_ref(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, clinic_id: uuid.UUID
) -> dict[str, Any] | None:
    """Organization's `{id, is_active, timezone}`, or None if unknown. The
    timezone is returned as stored; callers guard `ZoneInfoNotFoundError`."""
    return await _get(
        http,
        tokens,
        service="organization",
        base_url=settings.organization_service_url,
        path=f"/internal/clinics/{clinic_id}",
    )


async def get_patient_ref(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    patient_id: uuid.UUID,
    clinic_id: uuid.UUID,
) -> dict[str, Any] | None:
    """Patient's `{id, clinic_id, user_id, is_active}` within `clinic_id`, or
    None (missing and out-of-clinic are the same answer)."""
    return await _get(
        http,
        tokens,
        service="patient",
        base_url=settings.patient_service_url,
        path=f"/internal/patients/{patient_id}?clinic_id={clinic_id}",
    )


async def get_patient_by_user(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    user_id: uuid.UUID,
    clinic_id: uuid.UUID,
) -> dict[str, Any] | None:
    """The patient profile linked to login account `user_id` within
    `clinic_id`, or None."""
    return await _get(
        http,
        tokens,
        service="patient",
        base_url=settings.patient_service_url,
        path=f"/internal/patients/by-user/{user_id}?clinic_id={clinic_id}",
    )


# --- plan 02.1-08 (M2): Provider's schedule-change feed ------------------------


async def list_schedule_changes(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, *, limit: int = 100
) -> list[dict[str, Any]]:
    """Provider's pending (undelivered) schedule-change events, oldest first, at
    most `limit`: `[{id, provider_id, change_type, template_id, created_at}]`.
    Read with Scheduling's own service token for `aud=provider`, the only
    service Provider's feed accepts. There is no cursor: an event stays in the
    feed until `ack_schedule_change`. An unreachable owner propagates as
    `UpstreamServiceUnavailable`."""
    events = await _get(
        http,
        tokens,
        service="provider",
        base_url=settings.provider_service_url,
        path=f"/internal/schedule-changes?limit={int(limit)}",
        allow_list=True,
    )
    return list(events or [])


async def ack_schedule_change(
    http: httpx.AsyncClient, tokens: ServiceTokenProvider, event_id: str
) -> bool:
    """Acknowledge one event AFTER it has been processed (at-least-once). Provider
    answers 200 for a first and for a repeated ack, so this is idempotent; False
    only when Provider does not know the id (404). Not retried here: Temporal
    owns the retry budget."""
    token = await tokens.get("provider")
    try:
        await internal_request(
            http,
            "POST",
            f"{settings.provider_service_url}/internal/schedule-changes/{event_id}/ack",
            token=token,
            service="provider",
        )
    except InternalNotFound:
        return False
    return True
