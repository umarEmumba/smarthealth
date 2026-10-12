"""Identity's internal account lookup for other services (D-04, D-05).

Patient and Provider validate a profile -> account link (`patients.user_id`,
`providers.user_id`) by calling this at write time, forwarding the caller's token:

    GET /internal/users/{user_id} -> {id, role, clinic_id}

Design rules (STRIDE T-01.1-24):
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated callers only: ADMIN or FRONT_DESK user tokens, or an Identity
    service token (`require_internal_caller`).
  * A missing user and a user outside the caller's clinic scope return the
    IDENTICAL 404, so existence cannot be probed across clinics.
  * Minimal fields only. No email and no credential material ever leave Identity
    through this route. Reads are not audited (Phase 1 does not audit reads).
  * Identity makes no outbound call from here.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.identity.db import get_db
from services.identity.repository.models import User

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)


class UserRef(BaseModel):
    id: uuid.UUID
    role: Role
    clinic_id: uuid.UUID | None = None


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


@router.get("/users/{user_id}", response_model=UserRef)
async def lookup_user(
    user_id: uuid.UUID,
    caller: InternalCaller = Depends(require_internal_caller(Role.ADMIN, Role.FRONT_DESK)),
    db: AsyncSession = Depends(get_db),
) -> UserRef:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if user is None or not clinic_scope_permits(caller, user.clinic_id):
        log.info("internal.user_lookup", user_id=str(user_id), outcome="not_found")
        raise _not_found()

    log.info("internal.user_lookup", user_id=str(user.id), outcome="found")
    return UserRef(id=user.id, role=Role(user.role), clinic_id=user.clinic_id)
