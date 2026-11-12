"""Billing's public, read-only routes (BILL-01/02 made demonstrable, D-18).

    GET /api/billing/records?appointment_id=   -> the appointment's record + history
    GET /api/billing/records/{record_id}       -> same shape

Front-desk and admin only, reached through the gateway. Front-desk is scoped
to its own clinic; a record in another clinic answers the IDENTICAL 404 as a
missing one (T-02-25). Responses carry statuses, coarse coverage kind, reason
codes and timestamps — never idempotency keys, actors or any coverage
reference. Reads are not audited (existing convention).
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from services.billing.db import get_db
from services.billing.domain.schemas import BillingHistoryOut, BillingRecordOut
from services.billing.repository import billing_repo
from services.billing.repository.models import BillingRecord

router = APIRouter(prefix="/api/billing", tags=["billing"])

STAFF_ROLES = (Role.FRONT_DESK, Role.ADMIN)


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _in_scope(user: AuthenticatedUser, clinic_id: uuid.UUID) -> bool:
    if user.role == Role.ADMIN:
        return True
    if user.clinic_id is None:
        return False
    try:
        return uuid.UUID(user.clinic_id) == clinic_id
    except ValueError:
        return False


async def build_record_out(db: AsyncSession, record: BillingRecord) -> BillingRecordOut:
    """The record plus its ordered history (shared with the internal routes)."""
    history = await billing_repo.list_history(db, record.id)
    return BillingRecordOut(
        id=record.id,
        appointment_id=record.appointment_id,
        clinic_id=record.clinic_id,
        status=record.status,
        coverage_kind=record.coverage_kind,
        reason_code=record.reason_code,
        created_at=record.created_at,
        updated_at=record.updated_at,
        history=[
            BillingHistoryOut(
                from_status=row.from_status,
                to_status=row.to_status,
                reason_code=row.reason_code,
                created_at=row.created_at,
            )
            for row in history
        ],
    )


@router.get("/records", response_model=BillingRecordOut)
async def get_record_by_appointment(
    appointment_id: uuid.UUID = Query(...),
    user: AuthenticatedUser = Depends(require_role(*STAFF_ROLES)),
    db: AsyncSession = Depends(get_db),
) -> BillingRecordOut:
    record = await billing_repo.get_by_appointment(db, appointment_id)
    if record is None or not _in_scope(user, record.clinic_id):
        raise _not_found()
    return await build_record_out(db, record)


@router.get("/records/{record_id}", response_model=BillingRecordOut)
async def get_record(
    record_id: uuid.UUID,
    user: AuthenticatedUser = Depends(require_role(*STAFF_ROLES)),
    db: AsyncSession = Depends(get_db),
) -> BillingRecordOut:
    record = await billing_repo.get_record(db, record_id)
    if record is None or not _in_scope(user, record.clinic_id):
        raise _not_found()
    return await build_record_out(db, record)
