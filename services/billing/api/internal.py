"""Billing's internal, state-targeted endpoints (BILL-01, BILL-02; D-06, D-10,
D-13, D-14, D-16).

    POST /internal/billing/prechecks                                   (scheduling)
    POST /internal/billing/records/{appointment_id}/transitions        (scheduling, visit)
    POST /internal/billing/records/{appointment_id}/relink             (scheduling)

Callers are Temporal activities, which retry. Every endpoint is therefore
idempotent by STATE and by `Idempotency-Key` (required header):

  * pre-check: if the appointment already has a record, it is returned with 200
    and never re-evaluated. Otherwise ONE transaction writes `pending`, applies
    the deterministic rule (`evaluate_precheck`, D-10) and records
    `verified|failed` — two history rows, no observable half-state — then 201.
    A concurrent duplicate that loses on the appointment UNIQUE returns the
    winner with 200.
  * transitions: name the TARGET state. Already there, or a key already
    recorded for this record -> 200 no-op; a move the state machine forbids ->
    409; otherwise a conditional UPDATE plus one history row -> 200.
    Settlement is SIMULATED (D-13): the cancel activity requests
    `refund_pending` and then `refunded` as two calls; no money moves anywhere.
  * relink (reschedule, D-14): one history row with `from_status == to_status`
    and reason `rescheduled`; no refund, no state change.

Security (T-02-22): `aud=billing` service tokens only from the listed services
(plus ADMIN user tokens for operations), never routed by the gateway, excluded
from OpenAPI. Visit may only finalize and Scheduling may not
(`SERVICE_TRANSITION_TARGETS`). Service tokens carry no clinic claim; a user
caller out of the record's clinic scope gets the same 404 as a missing record.

PHI/D-18: the raw `coverage_ref` is evaluated in memory and dropped — never
stored, logged, audited or returned. Logs carry ids and statuses only.
Audit is written AFTER commit, once per effective change, with a dedupe key.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pymongo import AsyncMongoClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.db.mongo import get_mongo
from libs.observability.logging import get_logger
from services.billing.api.records import build_record_out
from services.billing.config import BillingSettings, settings
from services.billing.db import get_db
from services.billing.domain.rules import evaluate_precheck
from services.billing.domain.schemas import BillingRecordOut, PrecheckRequest, TransitionRequest
from services.billing.domain.states import SERVICE_TRANSITION_TARGETS
from services.billing.repository import billing_repo
from services.billing.repository.billing_repo import TransitionOutcome
from services.billing.repository.models import BillingRecord

router = APIRouter(prefix="/internal/billing", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)

ENTITY_TYPE = "billing_record"
# 200 (column width) minus room for the pre-check's outcome-row suffix.
IDEMPOTENCY_KEY_MAX_LEN = 190

IdempotencyKey = Header(
    ..., alias="Idempotency-Key", min_length=1, max_length=IDEMPOTENCY_KEY_MAX_LEN
)


def get_billing_settings() -> BillingSettings:
    """Overridable in tests to exercise another failing prefix (D-10)."""
    return settings


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _actor_role(caller: InternalCaller) -> str:
    return "service" if caller.kind == "service" else (caller.role.value if caller.role else "unknown")


async def _audit(
    mongo: AsyncMongoClient,
    caller: InternalCaller,
    *,
    action: str,
    record: BillingRecord,
    changed_fields: list[str],
    dedupe_key: str,
) -> None:
    await write_audit(
        mongo,
        actor_id=caller.sub,
        actor_role=_actor_role(caller),
        action=action,
        entity_type=ENTITY_TYPE,
        entity_id=str(record.id),
        clinic_id=str(record.clinic_id),
        outcome="success",
        changed_fields=changed_fields,
        dedupe_key=dedupe_key,
    )


async def _record_in_scope(
    db: AsyncSession, caller: InternalCaller, appointment_id: uuid.UUID
) -> BillingRecord:
    record = await billing_repo.get_by_appointment(db, appointment_id)
    if record is None or not clinic_scope_permits(caller, record.clinic_id):
        raise _not_found()
    return record


@router.post(
    "/prechecks",
    response_model=BillingRecordOut,
    status_code=status.HTTP_201_CREATED,
)
async def run_precheck(
    payload: PrecheckRequest,
    response: Response,
    idempotency_key: str = IdempotencyKey,
    caller: InternalCaller = Depends(
        require_internal_caller(
            Role.ADMIN, audience="billing", allowed_services=frozenset({"scheduling"})
        )
    ),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
    cfg: BillingSettings = Depends(get_billing_settings),
) -> BillingRecordOut:
    appointment_id = payload.appointment_id

    existing = await billing_repo.get_by_appointment(db, appointment_id)
    if existing is None:
        # Evaluated in memory; only the decision (never the reference) goes on.
        decision = evaluate_precheck(payload.coverage_ref, cfg.precheck_fail_prefix)
        try:
            record = await billing_repo.create_precheck(
                db,
                appointment_id=appointment_id,
                clinic_id=payload.clinic_id,
                decision=decision,
                idempotency_key=idempotency_key,
                actor=caller.sub,
            )
            record_id = record.id
            await db.commit()
        except IntegrityError:
            await db.rollback()
            existing = await billing_repo.get_by_appointment(db, appointment_id)
            if existing is None:  # pragma: no cover - the UNIQUE winner must exist
                raise
        else:
            record = await billing_repo.get_record(db, record_id)
            assert record is not None
            await _audit(
                mongo,
                caller,
                action="billing.precheck",
                record=record,
                changed_fields=["status"],
                dedupe_key=f"billing:{appointment_id}:precheck",
            )
            log.info(
                "billing.precheck",
                appointment_id=str(appointment_id),
                record_id=str(record.id),
                status=record.status,
                outcome="created",
            )
            return await build_record_out(db, record)

    # Replay (or lost a concurrent race): the first evaluation stands.
    if existing.clinic_id != payload.clinic_id:
        log.info("billing.precheck", appointment_id=str(appointment_id), outcome="clinic_mismatch")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="billing record exists for another clinic",
        )
    if not clinic_scope_permits(caller, existing.clinic_id):
        raise _not_found()
    log.info(
        "billing.precheck",
        appointment_id=str(appointment_id),
        record_id=str(existing.id),
        status=existing.status,
        outcome="replayed",
    )
    response.status_code = status.HTTP_200_OK
    return await build_record_out(db, existing)


@router.post("/records/{appointment_id}/transitions", response_model=BillingRecordOut)
async def transition(
    appointment_id: uuid.UUID,
    payload: TransitionRequest,
    idempotency_key: str = IdempotencyKey,
    caller: InternalCaller = Depends(
        require_internal_caller(
            Role.ADMIN, audience="billing", allowed_services=frozenset({"scheduling", "visit"})
        )
    ),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> BillingRecordOut:
    target = payload.target
    if caller.kind == "service" and target not in SERVICE_TRANSITION_TARGETS.get(
        caller.service_name or "", frozenset()
    ):
        log.info("billing.transition_refused", service=caller.service_name, target=target)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="service not permitted for this transition",
        )

    record = await _record_in_scope(db, caller, appointment_id)
    record_id = record.id
    result = await billing_repo.transition_record(
        db,
        record_id,
        target,
        idempotency_key=idempotency_key,
        reason_code=payload.reason_code,
        actor=caller.sub,
    )

    if result.outcome != TransitionOutcome.APPLIED:
        await db.rollback()
        log.info(
            "billing.transition",
            appointment_id=str(appointment_id),
            record_id=str(record_id),
            target=target,
            current=result.current_status,
            outcome=result.outcome,
        )
        if result.outcome == TransitionOutcome.CONFLICT:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"cannot move billing record from {result.current_status} to {target}",
            )
        record = await billing_repo.get_record(db, record_id)
        assert record is not None
        return await build_record_out(db, record)

    await db.commit()
    record = await billing_repo.get_record(db, record_id)
    assert record is not None
    await _audit(
        mongo,
        caller,
        action=f"billing.{target}",
        record=record,
        changed_fields=["status"],
        dedupe_key=f"billing:{appointment_id}:{idempotency_key}",
    )
    log.info(
        "billing.transition",
        appointment_id=str(appointment_id),
        record_id=str(record_id),
        target=target,
        outcome="applied",
    )
    return await build_record_out(db, record)


@router.post("/records/{appointment_id}/relink", response_model=BillingRecordOut)
async def relink(
    appointment_id: uuid.UUID,
    idempotency_key: str = IdempotencyKey,
    caller: InternalCaller = Depends(
        require_internal_caller(
            Role.ADMIN, audience="billing", allowed_services=frozenset({"scheduling"})
        )
    ),
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> BillingRecordOut:
    record = await _record_in_scope(db, caller, appointment_id)
    record_id = record.id
    result = await billing_repo.append_relink(
        db, record_id, idempotency_key=idempotency_key, actor=caller.sub
    )

    if result.outcome != TransitionOutcome.APPLIED:
        await db.rollback()
        log.info(
            "billing.relink",
            appointment_id=str(appointment_id),
            record_id=str(record_id),
            current=result.current_status,
            outcome=result.outcome,
        )
        if result.outcome == TransitionOutcome.CONFLICT:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"cannot re-link a billing record in state {result.current_status}",
            )
        record = await billing_repo.get_record(db, record_id)
        assert record is not None
        return await build_record_out(db, record)

    await db.commit()
    record = await billing_repo.get_record(db, record_id)
    assert record is not None
    await _audit(
        mongo,
        caller,
        action="billing.relink",
        record=record,
        changed_fields=["billing_status_history"],
        dedupe_key=f"billing:{appointment_id}:{idempotency_key}",
    )
    log.info(
        "billing.relink", appointment_id=str(appointment_id), record_id=str(record_id), outcome="applied"
    )
    return await build_record_out(db, record)
