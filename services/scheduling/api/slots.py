"""Slot supply routes (plan 02-04, D-01, D-18, research Pattern 6 / Pitfall 14).

    POST /api/slots/generate        ADMIN, FRONT_DESK  materialize slots from templates
    GET  /api/slots                 any role           search one provider's slots
    POST /api/slots/{slot_id}/cancel ADMIN, FRONT_DESK withdraw an available slot

Generation resolves, server-side and over aud-scoped service tokens
(`services.scheduling.references`): provider (Provider) -> department
(Organization) -> clinic timezone (Organization) -> templates (Provider). The
templates are expanded in the clinic's timezone by the pure, DST-safe
`domain.slot_generation` and inserted with `ON CONFLICT DO NOTHING`, so a repeat
creates nothing and a held, booked or withdrawn slot is never touched.

Scope (D-18, T-02-18): a front-desk user acts only on providers whose
department belongs to their own clinic; any other provider gets the SAME 404 as
an unknown one. Every upstream failure is a 503 raised before any write
(fail closed). A clinic timezone zoneinfo cannot load is a 409 (WR-13, T-02-19).

Bounds (T-02-17): no generate/search range may exceed `slot_max_range_days`
(422). A search reaching past the generated range, within now +
`slot_horizon_days`, first generates the missing days on demand (no scheduler
until Phase 3). That extension never fails the read: errors are logged as
`slots.extend_failed` and the existing slots are still returned.

Mutations are audited with field NAMES only (`provider_slots`); responses and
logs carry ids and UTC instants only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.current_user import AuthenticatedUser
from libs.auth.rbac import require_role
from libs.auth.roles import Role
from libs.auth.service_tokens import ServiceTokenProvider, ServiceTokenUnavailable
from libs.db.mongo import get_mongo
from libs.http import get_internal_http_client
from libs.observability.logging import get_logger
from libs.service_client import UpstreamServiceUnavailable
from services.scheduling import references
from services.scheduling.api.deps import get_service_tokens
from services.scheduling.config import settings
from services.scheduling.db import get_db
from services.scheduling.domain.enums import SlotStatus
from services.scheduling.domain.schemas import SlotGenerateRequest, SlotGenerateResponse, SlotOut
from services.scheduling.domain.slot_generation import expand_templates
from services.scheduling.repository import slot_repo

router = APIRouter(prefix="/api/slots", tags=["slots"])

log = get_logger(__name__)

ALL_ROLES = (Role.PATIENT, Role.PROVIDER, Role.FRONT_DESK, Role.ADMIN)
STAFF_ROLES = (Role.FRONT_DESK, Role.ADMIN)
CHANGED_FIELDS = ["provider_slots"]


class GenerationRefused(Exception):
    """Generation cannot proceed; carries the HTTP status/detail to answer with."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class GenerationResult:
    created: int
    considered: int
    clinic_id: uuid.UUID


def _not_found() -> GenerationRefused:
    return GenerationRefused(status.HTTP_404_NOT_FOUND, "not found")


def _in_scope(user: AuthenticatedUser, clinic_id: uuid.UUID) -> bool:
    """D-18: ADMIN acts anywhere; everyone else only in their own clinic."""
    if user.role == Role.ADMIN:
        return True
    if user.clinic_id is None:
        return False
    try:
        return uuid.UUID(user.clinic_id) == clinic_id
    except ValueError:
        return False


def _check_range_days(span: timedelta) -> None:
    if span < timedelta(0):
        raise HTTPException(status_code=422, detail="range end must not be before its start")
    if span > timedelta(days=settings.slot_max_range_days):
        raise HTTPException(
            status_code=422,
            detail=f"range must not exceed {settings.slot_max_range_days} days",
        )


async def generate_for_provider(
    *,
    http: httpx.AsyncClient,
    tokens: ServiceTokenProvider,
    db: AsyncSession,
    user: AuthenticatedUser,
    provider_id: uuid.UUID,
    from_date: date,
    to_date: date,
    enforce_scope: bool,
) -> GenerationResult:
    """Resolve provider -> department -> clinic -> templates, expand and insert.

    Raises `GenerationRefused` (404 / 409 / 503) BEFORE any write; flushes the
    inserts through `db` and leaves the commit to the caller.
    """
    try:
        provider = await references.get_provider_ref(http, tokens, provider_id)
        if provider is None:
            raise _not_found()
        department = await references.get_department_ref(
            http, tokens, uuid.UUID(provider["department_id"])
        )
        if department is None:
            raise _not_found()
        clinic_id = uuid.UUID(department["clinic_id"])
        if enforce_scope and not _in_scope(user, clinic_id):
            # Identical to an unknown provider: existence is not probeable
            # across clinics (T-02-18).
            raise _not_found()
        clinic = await references.get_clinic_ref(http, tokens, clinic_id)
        if clinic is None:
            raise _not_found()
        if not provider.get("is_active", False):
            raise GenerationRefused(status.HTTP_409_CONFLICT, "provider inactive")
        try:
            tz = ZoneInfo(str(clinic["timezone"]))
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            raise GenerationRefused(status.HTTP_409_CONFLICT, "clinic timezone invalid") from exc
        templates = await references.get_provider_availability(http, tokens, provider_id)
        if templates is None:
            raise _not_found()
    except UpstreamServiceUnavailable as exc:
        raise GenerationRefused(
            status.HTTP_503_SERVICE_UNAVAILABLE, f"{exc.service} service unavailable"
        ) from exc
    except ServiceTokenUnavailable as exc:
        raise GenerationRefused(
            status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable"
        ) from exc

    candidates = expand_templates(templates, tz, from_date, to_date)
    created = await slot_repo.insert_generated_slots(
        db, provider_id=provider_id, clinic_id=clinic_id, candidates=candidates
    )
    return GenerationResult(created=created, considered=len(candidates), clinic_id=clinic_id)


async def _audit_generate(
    mongo: AsyncMongoClient,
    user: AuthenticatedUser,
    *,
    provider_id: uuid.UUID,
    clinic_id: uuid.UUID | None,
    outcome: str,
) -> None:
    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="slots.generate",
        entity_type="provider",
        entity_id=str(provider_id),
        clinic_id=str(clinic_id) if clinic_id is not None else None,
        outcome=outcome,  # type: ignore[arg-type]
        changed_fields=CHANGED_FIELDS if outcome == "success" else None,
    )


def _to_out(slot) -> SlotOut:
    return SlotOut(
        id=slot.id,
        provider_id=slot.provider_id,
        clinic_id=slot.clinic_id,
        department_id=slot.department_id,
        start_time=slot.start_time,
        end_time=slot.end_time,
        status=slot.status,
    )


@router.post("/generate", response_model=SlotGenerateResponse)
async def generate_slots(
    payload: SlotGenerateRequest,
    user: AuthenticatedUser = Depends(require_role(*STAFF_ROLES)),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    http: httpx.AsyncClient = Depends(get_internal_http_client),
    tokens: ServiceTokenProvider = Depends(get_service_tokens),
) -> SlotGenerateResponse:
    _check_range_days(payload.to_date - payload.from_date)

    try:
        result = await generate_for_provider(
            http=http,
            tokens=tokens,
            db=db,
            user=user,
            provider_id=payload.provider_id,
            from_date=payload.from_date,
            to_date=payload.to_date,
            enforce_scope=True,
        )
    except GenerationRefused as exc:
        await db.rollback()
        log.info(
            "slots.generate.refused",
            provider_id=str(payload.provider_id),
            status=exc.status_code,
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    await db.commit()
    await _audit_generate(
        mongo, user, provider_id=payload.provider_id, clinic_id=result.clinic_id, outcome="success"
    )
    log.info(
        "slots.generate.success",
        provider_id=str(payload.provider_id),
        clinic_id=str(result.clinic_id),
        created=result.created,
        considered=result.considered,
    )
    return SlotGenerateResponse(created=result.created, considered=result.considered)


@router.get("", response_model=list[SlotOut])
async def search_slots(
    provider_id: uuid.UUID,
    start: datetime,
    end: datetime,
    slot_status: str | None = Query(default=SlotStatus.AVAILABLE.value, alias="status"),
    user: AuthenticatedUser = Depends(require_role(*ALL_ROLES)),
    db: AsyncSession = Depends(get_db),
) -> list[SlotOut]:
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(status_code=422, detail="start and end must include a UTC offset")
    if slot_status is not None and slot_status not in {s.value for s in SlotStatus}:
        raise HTTPException(status_code=422, detail="unknown slot status")
    _check_range_days(end - start)

    slots = await slot_repo.search_slots(
        db, provider_id=provider_id, start=start, end=end, status=slot_status
    )
    return [_to_out(s) for s in slots]
