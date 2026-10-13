"""Visit-side reference reads over aud-scoped service tokens (plan 02-10, D-06).

  * `get_appointment_ref` — Scheduling's `GET /internal/appointments/{id}
    ?clinic_id=` (aud=scheduling): is the appointment confirmed, and whose is it?
  * `get_provider_by_user` — Provider's `GET /internal/providers/by-user/{id}`
    (aud=provider): which provider profile does this PROVIDER login own?

Every read goes through `libs.internal_client.internal_request` with a SERVICE
token minted for the owning service; the caller's own JWT is never forwarded.

Outcomes (the `internal_request` contract):
  * the owner's JSON body on 200;
  * None when the owner answers 404 (`InternalNotFound`): missing, or outside
    the clinic the request names (the owner gives the same answer);
  * `UpstreamServiceUnavailable` (owner unreachable / misbehaving) and
    `ServiceTokenUnavailable` (Identity could not mint) propagate: the route
    fails closed with 503 before writing anything.

Requests carry ids only, never PHI. Exactly one attempt per call.
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx

from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalNotFound, internal_request
from services.visit.config import settings

__all__ = ["get_appointment_ref", "get_provider_by_user"]


async def _get(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    *,
    service: str,
    base_url: str,
    path: str,
) -> dict[str, Any] | None:
    token = await tokens.get(service)
    try:
        body = await internal_request(http, "GET", f"{base_url}{path}", token=token, service=service)
    except InternalNotFound:
        return None
    return body  # type: ignore[return-value]  # a dict: allow_list is off


async def get_appointment_ref(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    *,
    appointment_id: uuid.UUID,
    clinic_id: uuid.UUID,
) -> dict[str, Any] | None:
    """Scheduling's AppointmentRef `{id, clinic_id, patient_id, provider_id,
    slot_id, slot_start, slot_end, status}` within `clinic_id`, or None."""
    return await _get(
        http,
        tokens,
        service="scheduling",
        base_url=settings.scheduling_service_url,
        path=f"/internal/appointments/{appointment_id}?clinic_id={clinic_id}",
    )


async def get_provider_by_user(
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    *,
    user_id: uuid.UUID,
) -> dict[str, Any] | None:
    """Provider's `{id, department_id, is_active}` linked to login `user_id`,
    or None if that login owns no provider profile."""
    return await _get(
        http,
        tokens,
        service="provider",
        base_url=settings.provider_service_url,
        path=f"/internal/providers/by-user/{user_id}",
    )
