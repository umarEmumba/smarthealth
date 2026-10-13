"""Provider's internal integrity lookup for other services (T-01.1-31).

Scheduling (Phase 2) will call this before accepting a `provider_slots.provider_id`,
since that column can no longer be a foreign key across the service boundary:

    GET /internal/providers/{provider_id} -> {id, department_id, is_active}

Design rules:
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated callers only: ADMIN user tokens, or a Scheduling/Visit service
    token whose `aud` is `provider` (`require_internal_caller`, D-06). Any other
    role or service is 403; no token, or a service token for another audience, is 401.
  * INACTIVE providers are REPORTED (`is_active: false`), never hidden: the
    caller decides what "inactive" means for it.
  * An unknown id is the same 404 for every caller. The answer carries ids and a
    flag only: no name, no specialties, no account link.
  * Reads are not audited (Phase 1 does not audit reads); the log line carries
    ids only. Provider makes no outbound call from here.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.provider.db import get_db
from services.provider.repository import provider_repo

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


class ProviderRef(BaseModel):
    id: uuid.UUID
    department_id: uuid.UUID
    is_active: bool


@router.get("/providers/{provider_id}", response_model=ProviderRef)
async def lookup_provider(
    provider_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, audience="provider", allowed_services=frozenset({"scheduling", "visit"}))
    ),
    db: AsyncSession = Depends(get_db),
) -> ProviderRef:
    provider = await provider_repo.get_provider(db, provider_id)
    if provider is None:
        log.info("internal.provider_lookup", provider_id=str(provider_id), outcome="not_found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")

    log.info("internal.provider_lookup", provider_id=str(provider.id), outcome="found")
    return ProviderRef(
        id=provider.id, department_id=provider.department_id, is_active=provider.is_active
    )
