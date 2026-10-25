"""Scheduling reconciler activities (plan 02.1-11; S4, S5, M1).

Run by `ReconcilerWorkflow` on a Temporal Schedule (`{prefix}-reconciler`, every
60 s, overlap SKIP). Every activity is idempotent, bounded by a batch limit and
isolates per-row failures, so a run that overlaps a retry, or two workers, cannot
double-process anything. All state changes are conditional updates (`WHERE
status = ...`), so anything that finished meanwhile is left untouched.

* `reconcile_stuck_pending` - a `pending` appointment older than the threshold
  whose booking workflow `booking-{id}` is GONE (terminated, timed out, closed
  without writing a terminal state, or past retention) blocks its slot forever
  (`uq_appointments_active_slot` covers `pending`). Liveness is decided by
  `describe()`: RUNNING is skipped; NOT_FOUND and every closed status are gone; any
  other RPC error (Temporal unreachable) propagates and skips the whole step, so
  a booking is never failed because the control plane is down. For a gone
  workflow the order is: re-read the appointment AFTER the workflow was seen
  closed (anything the workflow committed happened-before its close, so a still
  `pending` row is really abandoned), void billing, then in ONE transaction
  release the hold, fail the appointment with `workflow_lost` and write the
  `billing_void_checked` note. If the void cannot be done the appointment stays
  `pending` and the next run retries, so the pair converges without a new
  column. After the commit: a best-effort audit entry and a best-effort start of
  `WaitlistAdvanceWorkflow` under `waitlist-advance-booking-{appointment_id}`
  (the id the booking workflow itself uses for its re-offer, so a duplicate is
  impossible).

  The same activity then runs the billing LOOKBACK: appointments failed with
  `hold_expired` (the hold reaper) or `workflow_lost` in the last 24 h that have
  no `billing_void_checked` note get their billing voided (a repeat is a Billing
  no-op) and the note written once the attempt resolved.

* `reconcile_stuck_waitlist` - waitlist entries stuck `matched` (the advance
  workflow documents that a lost claim result or an exhausted resolve leaves
  them for an operator) are resolved from the outcome of their matched
  appointment.

* `reconcile_no_shows` - confirmed appointments whose slot ended more than the
  grace ago: Billing `no_refund` first, then `mark_no_show`; either failure leaves
  the appointment confirmed and the next run repeats both idempotently.

Logs, history and audit carry ids, statuses and exception TYPES only; never a
name, a coverage reference, a token or a message that could carry data.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from libs.audit.writer import Outcome, write_audit
from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalConflict, InternalNotFound, InternalRejected, internal_request
from libs.observability.logging import get_logger
from libs.temporal import start_once
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_RECONCILE_PENDING,
    BILLING_VOID_CHECKED,
    RECONCILER_ACTOR,
    WAITLIST_ADVANCE_WORKFLOW,
    ReconcilePendingResult,
    WaitlistAdvanceInput,
    booking_workflow_id,
    waitlist_advance_workflow_id,
)
from services.scheduling.domain.enums import AppointmentStatus, FailureReason
from services.scheduling.repository import appointment_repo, slot_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = RECONCILER_ACTOR
AUDIT_ACTOR_ROLE = "service"
VOID_REASON_CODE = "booking_workflow_lost"

WorkflowStatus = Literal["running", "gone"]
RUNNING: WorkflowStatus = "running"
GONE: WorkflowStatus = "gone"

# Billing outcomes of one transition call: the state change applied (or was
# already there: Billing answers 200 to a repeat), there is no record, or Billing
# refused the move (a terminal answer, e.g. the record is already finalized).
BillingOutcome = Literal["applied", "no_record", "refused"]

# Failure reasons whose appointments the billing lookback covers.
LOOKBACK_REASONS = (FailureReason.HOLD_EXPIRED, FailureReason.WORKFLOW_LOST)


@dataclass(frozen=True, slots=True)
class _Pending:
    """What the reconciler needs of a stuck appointment, copied out of the
    session that read it (ids only)."""

    appointment_id: uuid.UUID
    clinic_id: uuid.UUID
    slot_id: uuid.UUID


class ReconcilerActivities:
    """The reconciler's steps, bound to injected clients. `client` is the
    worker's own Temporal client (liveness checks and workflow starts)."""

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        client: Client | None,
        mongo: AsyncMongoClient,
        http: httpx.AsyncClient,
        tokens: ServiceTokenProvider,
        *,
        pending_after_minutes: float | None = None,
        matched_after_minutes: float | None = None,
        no_show_grace_minutes: int | None = None,
        billing_lookback_hours: float | None = None,
        batch_limit: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._client = client
        self._mongo = mongo
        self._http = http
        self._tokens = tokens
        # Explicit None checks: 0 is a legitimate override (tests reconcile
        # freshly created rows without ageing them).
        self._pending_after = (
            settings.reconcile_pending_after_minutes
            if pending_after_minutes is None
            else pending_after_minutes
        )
        self._matched_after = (
            settings.reconcile_matched_after_minutes
            if matched_after_minutes is None
            else matched_after_minutes
        )
        self._no_show_grace = (
            settings.no_show_grace_minutes if no_show_grace_minutes is None else no_show_grace_minutes
        )
        self._lookback_hours = (
            settings.reconcile_billing_lookback_hours
            if billing_lookback_hours is None
            else billing_lookback_hours
        )
        self._limit = settings.reconcile_batch_limit if batch_limit is None else batch_limit

    def all(self) -> list[Callable[..., Any]]:
        """Every reconciler activity, for `Worker(activities=...)`."""
        return [self.reconcile_stuck_pending]

    # --- shared helpers -------------------------------------------------------

    async def _workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """The single liveness seam. `running` for a RUNNING execution; `gone`
        for NOT_FOUND (never existed or past retention) and for every closed
        status. Any other RPC error propagates: the caller skips the step."""
        if self._client is None:  # pragma: no cover - wiring error
            raise RuntimeError("ReconcilerActivities needs a Temporal client to check liveness")
        try:
            description = await self._client.get_workflow_handle(workflow_id).describe()
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                return GONE
            raise
        return RUNNING if description.status == WorkflowExecutionStatus.RUNNING else GONE

    async def _billing_transition(
        self, appointment_id: uuid.UUID, target: str, *, key: str, reason_code: str
    ) -> BillingOutcome:
        """POST a Billing transition by TARGET state with a deterministic
        Idempotency-Key. `UpstreamServiceUnavailable` (Billing unreachable or
        misbehaving) propagates: the caller leaves the row for the next run."""
        token = await self._tokens.get("billing")
        try:
            await internal_request(
                self._http,
                "POST",
                f"{settings.billing_service_url}/internal/billing/records/{appointment_id}/transitions",
                token=token,
                service="billing",
                idempotency_key=key,
                json={"target": target, "reason_code": reason_code},
            )
        except InternalNotFound:
            log.info(
                "reconcile.billing_transition",
                appointment_id=str(appointment_id),
                target=target,
                outcome="no_record",
            )
            return "no_record"
        except (InternalConflict, InternalRejected) as exc:
            log.warning(
                "reconcile.billing_transition",
                appointment_id=str(appointment_id),
                target=target,
                outcome="refused",
                error=type(exc).__name__,
            )
            return "refused"
        log.info(
            "reconcile.billing_transition",
            appointment_id=str(appointment_id),
            target=target,
            outcome="applied",
        )
        return "applied"

    async def _audit_best_effort(
        self,
        *,
        action: str,
        entity_type: str,
        entity_id: str,
        clinic_id: str | None,
        dedupe_key: str,
        changed_fields: list[str],
        outcome: Outcome = "success",
    ) -> None:
        """The state change is already committed (S3): a failed audit is logged
        (id and exception type only) and swallowed, never repaired here."""
        try:
            await write_audit(
                self._mongo,
                actor_id=AUDIT_ACTOR_ID,
                actor_role=AUDIT_ACTOR_ROLE,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                clinic_id=clinic_id,
                outcome=outcome,
                changed_fields=changed_fields,
                dedupe_key=dedupe_key,
            )
        except Exception as exc:  # noqa: BLE001 - deliberately non-fatal
            log.error(
                "audit.write_failed",
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                error=type(exc).__name__,
            )

    async def _offer_slot(
        self, *, slot_id: uuid.UUID, clinic_id: uuid.UUID, source_workflow_id: str
    ) -> None:
        """Best effort (S5): offer the slot this run freed to the waitlist by
        starting `WaitlistAdvanceWorkflow` under the id derived from
        `source_workflow_id`. Already started or closed is a no-op
        (`start_once`); a failure is logged with ids and the exception type
        and never raised: the slot is already free and bookable."""
        if self._client is None:
            return
        try:
            await start_once(
                self._client,
                WAITLIST_ADVANCE_WORKFLOW,
                WaitlistAdvanceInput(
                    slot_id=str(slot_id),
                    clinic_id=str(clinic_id),
                    source_workflow_id=source_workflow_id,
                ),
                workflow_id=waitlist_advance_workflow_id(source_workflow_id),
                task_queue=SCHEDULING_TASK_QUEUE,
            )
        except Exception as exc:  # noqa: BLE001 - best effort by design
            log.warning(
                "reconcile.waitlist_offer_failed",
                slot_id=str(slot_id),
                source=source_workflow_id,
                error=type(exc).__name__,
            )

    # --- stuck pending (S4) ---------------------------------------------------

    @activity.defn(name=ACT_RECONCILE_PENDING)
    async def reconcile_stuck_pending(self) -> ReconcilePendingResult:
        async with self._session_factory() as db:
            stuck = [
                _Pending(a.id, a.clinic_id, a.slot_id)
                for a in await appointment_repo.list_stuck_pending(
                    db, older_than_minutes=self._pending_after, limit=self._limit
                )
            ]

        failed = 0
        for row in stuck:
            try:
                failed += int(await self._fail_lost_booking(row))
            except RPCError:
                # Temporal itself is unhealthy: never fail a booking because the
                # control plane is unreachable. Skip the rest of the step.
                log.warning("reconcile.pending_skipped", reason="temporal_unavailable")
                raise
            except Exception as exc:  # noqa: BLE001 - per-row isolation
                log.warning(
                    "reconcile.pending_failed",
                    appointment_id=str(row.appointment_id),
                    error=type(exc).__name__,
                )
        log.info("reconcile.stuck_pending", scanned=len(stuck), failed=failed)
        return ReconcilePendingResult(failed=failed, billing_voids=0)

    async def _fail_lost_booking(self, row: _Pending) -> bool:
        """Fail one stuck pending appointment whose workflow is gone. True when
        THIS call failed it."""
        appointment_id = row.appointment_id
        wf_id = booking_workflow_id(str(appointment_id))
        if await self._workflow_status(wf_id) == RUNNING:
            log.info("reconcile.pending_skipped", appointment_id=str(appointment_id), reason="running")
            return False

        # The workflow was seen closed: whatever it committed happened before
        # that, so a fresh read now is final. Not pending any more -> done.
        async with self._session_factory() as db:
            fresh = await appointment_repo.get_appointment(db, appointment_id)
            current = fresh.status if fresh is not None else None
        if current != AppointmentStatus.PENDING:
            log.info(
                "reconcile.pending_skipped",
                appointment_id=str(appointment_id),
                reason="not_pending",
            )
            return False

        # Void FIRST: if it cannot be done the appointment stays pending and the
        # next run retries (UpstreamServiceUnavailable propagates to the caller).
        await self._billing_transition(
            appointment_id,
            "voided",
            key=f"reconciler:{appointment_id}:void",
            reason_code=VOID_REASON_CODE,
        )

        async with self._session_factory() as db:
            # Slot first, appointment second: the same lock order as the hold
            # reaper (slot, then appointment), so the two cannot deadlock.
            await slot_repo.release_hold(db, slot_id=row.slot_id, appointment_id=appointment_id)
            status = await appointment_repo.lock_appointment_status(
                db, appointment_id=appointment_id
            )
            if status != AppointmentStatus.PENDING:
                await db.rollback()
                log.info(
                    "reconcile.pending_skipped",
                    appointment_id=str(appointment_id),
                    reason="not_pending",
                )
                return False
            await appointment_repo.mark_appointment_failed(
                db,
                appointment_id=appointment_id,
                reason=FailureReason.WORKFLOW_LOST,
                actor=RECONCILER_ACTOR,
            )
            await appointment_repo.record_history_note(
                db,
                appointment_id=appointment_id,
                reason=BILLING_VOID_CHECKED,
                actor=RECONCILER_ACTOR,
            )
            await db.commit()

        log.info(
            "reconcile.pending_failed_lost",
            appointment_id=str(appointment_id),
            reason=FailureReason.WORKFLOW_LOST,
        )
        await self._audit_best_effort(
            action="appointment.fail",
            entity_type="appointment",
            entity_id=str(appointment_id),
            clinic_id=str(row.clinic_id),
            dedupe_key=f"reconciler:{appointment_id}:fail",
            changed_fields=["status", "failure_reason"],
        )
        await self._offer_slot(
            slot_id=row.slot_id, clinic_id=row.clinic_id, source_workflow_id=wf_id
        )
        return True
