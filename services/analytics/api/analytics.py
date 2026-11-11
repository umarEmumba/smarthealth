"""Analytics' read-only metrics API (plan 03-13; ANLY-01..06, D-10, D-13).

    GET /api/analytics/summary?clinic_id=&from=&to=

Aggregate numbers over the projections plan 03-10's consumer maintains from Kafka. The
service holds no OLTP database URL and imports no other service (ANLY-06). Responses
carry counts, rates, durations and clinic ids only; never a patient or appointment id.
Reads are not audited (existing convention); nothing is written.

Window: inclusive UTC calendar days `from`..`to`, default the last
`ANALYTICS_DEFAULT_WINDOW_DAYS` days ending today, at most 366 days (T-3-37).
Counts start at the Phase 3 cutover (D-13); `total_patients` is the current count.
"""

from __future__ import annotations

import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from services.analytics.config import settings
from services.analytics.db import get_db
from services.analytics.repository import queries

router = APIRouter(prefix="/api/analytics", tags=["analytics"])

MAX_WINDOW_DAYS = 366

OPERATOR_ROLES = (Role.ADMIN,)


class SummaryOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    clinic_id: uuid.UUID | None
    from_: datetime.date = Field(serialization_alias="from")
    to: datetime.date
    total_patients: int
    appointments_booked: int
    completed_visits: int
    cancellation_rate: float | None
    average_wait_seconds: float | None


class Window(BaseModel):
    start: datetime.date
    end: datetime.date


def window_params(
    from_: datetime.date | None = Query(default=None, alias="from"),
    to: datetime.date | None = Query(default=None),
) -> Window:
    """Validated window: `from <= to`, at most 366 days; defaults end today (UTC)."""
    end = to or datetime.datetime.now(datetime.UTC).date()
    start = from_ or end - datetime.timedelta(days=settings.default_window_days - 1)
    if start > end:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="from must not be after to")
    if (end - start).days + 1 > MAX_WINDOW_DAYS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"window must not exceed {MAX_WINDOW_DAYS} days",
        )
    return Window(start=start, end=end)


def resolve_clinic(user: AuthenticatedUser, requested: uuid.UUID | None) -> uuid.UUID | None:
    """The clinic a caller may read: admins any (None = all clinics); everyone else
    only the clinic in their token (a different one is 403)."""
    if user.role == Role.ADMIN:
        return requested
    if user.clinic_id is None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="no clinic in token")
    try:
        own = uuid.UUID(user.clinic_id)
    except ValueError as exc:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="no clinic in token") from exc
    if requested is not None and requested != own:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="clinic not permitted")
    return own


@router.get("/summary", response_model=SummaryOut, response_model_by_alias=True)
async def get_summary(
    user: AuthenticatedUser = Depends(require_role(*OPERATOR_ROLES)),
    window: Window = Depends(window_params),
    clinic_id: uuid.UUID | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> SummaryOut:
    clinic = resolve_clinic(user, clinic_id)
    numbers = await queries.summary(db, clinic, window.start, window.end)
    return SummaryOut(clinic_id=clinic, from_=window.start, to=window.end, **numbers)
