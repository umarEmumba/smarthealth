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
from services.scheduling.activities.waitlist import offer_slot_to_waitlist
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_RECONCILE_NO_SHOWS,
    ACT_RECONCILE_PENDING,
    ACT_RECONCILE_WAITLIST,
    BILLING_VOID_CHECKED,
    LOST_RACE_REASONS,
    RECONCILER_ACTOR,
    MaintenanceResult,
    ReconcilePendingResult,
    booking_workflow_id,
)
from services.scheduling.domain.enums import AppointmentStatus, FailureReason
from services.scheduling.repository import appointment_repo, slot_repo, waitlist_repo

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
        return [self.reconcile_stuck_pending, self.reconcile_stuck_waitlist, self.reconcile_no_shows]

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
        voids = await self._billing_lookback()
        log.info(
            "reconcile.stuck_pending", scanned=len(stuck), failed=failed, billing_voids=voids
        )
        return ReconcilePendingResult(failed=failed, billing_voids=voids)

    async def _billing_lookback(self) -> int:
        """Void billing for appointments already failed by the hold reaper
        (`hold_expired`) or by this reconciler (`workflow_lost`) within the
        lookback window and not yet checked: the reaper fails the appointment
        but never reaches Billing, so a record the dead saga had verified would
        stay `verified` forever. The `billing_void_checked` note is written only
        once the attempt resolved (voided, no record, or a terminal refusal), so
        a Billing outage leaves the row for the next run. Returns how many
        records this call voided (a repeat void is a Billing no-op)."""
        async with self._session_factory() as db:
            orphans = [
                (a.id, str(a.clinic_id))
                for a in await appointment_repo.list_failed_unchecked_billing(
                    db,
                    reasons=LOOKBACK_REASONS,
                    within_hours=self._lookback_hours,
                    limit=self._limit,
                )
            ]
        voided = 0
        for appointment_id, clinic_id in orphans:
            try:
                outcome = await self._billing_transition(
                    appointment_id,
                    "voided",
                    key=f"reconciler:{appointment_id}:void",
                    reason_code=VOID_REASON_CODE,
                )
                async with self._session_factory() as db:
                    await appointment_repo.record_history_note(
                        db,
                        appointment_id=appointment_id,
                        reason=BILLING_VOID_CHECKED,
                        actor=RECONCILER_ACTOR,
                    )
                    await db.commit()
            except Exception as exc:  # noqa: BLE001 - per-row isolation
                log.warning(
                    "reconcile.billing_void_failed",
                    appointment_id=str(appointment_id),
                    error=type(exc).__name__,
                )
                continue
            if outcome == "applied":
                voided += 1
                await self._audit_best_effort(
                    action="billing.void",
                    entity_type="appointment",
                    entity_id=str(appointment_id),
                    clinic_id=clinic_id,
                    dedupe_key=f"reconciler:{appointment_id}:void",
                    changed_fields=["billing_status"],
                )
        return voided

    # --- stuck waitlist entries (S4) -------------------------------------------

    @activity.defn(name=ACT_RECONCILE_WAITLIST)
    async def reconcile_stuck_waitlist(self) -> MaintenanceResult:
        """Resolve entries stuck `matched` from the outcome of their matched
        appointment (the advance workflow can leave one for an operator: a claim
        result lost after commit, or a resolve that exhausted its budget):

        * confirmed, checked_in, completed, no_show or cancelled (it was booked)
          -> `booked`
        * failed with a lost-race reason (`SlotUnavailable`, `ConfirmLostHold`) or
          with `hold_expired` / `workflow_lost` -> back to `waiting`
        * failed with any other reason -> `failed` with that reason
        * pending -> untouched (its own workflow or the pending reconciler
          settles it first); a missing appointment -> back to `waiting`

        The resolve is conditional on `matched` and the appointment id, so a
        concurrent resolve by the advance workflow wins harmlessly."""
        async with self._session_factory() as db:
            stuck = [
                (e.id, e.matched_appointment_id, str(e.clinic_id))
                for e in await waitlist_repo.list_stuck_matched(
                    db, older_than_minutes=self._matched_after, limit=self._limit
                )
            ]
        resolved = 0
        for entry_id, appointment_id, clinic_id in stuck:
            if appointment_id is None:  # pragma: no cover - matched always carries one
                continue
            try:
                resolution = await self._resolve_matched(entry_id, appointment_id, clinic_id)
            except Exception as exc:  # noqa: BLE001 - per-row isolation
                log.warning(
                    "reconcile.waitlist_failed", entry_id=str(entry_id), error=type(exc).__name__
                )
                continue
            resolved += int(resolution is not None)
        log.info("reconcile.stuck_waitlist", scanned=len(stuck), resolved=resolved)
        return MaintenanceResult(processed=resolved)

    async def _resolve_matched(
        self, entry_id: uuid.UUID, appointment_id: uuid.UUID, clinic_id: str
    ) -> str | None:
        """Resolve one stuck entry; the resolution applied now, else None."""
        async with self._session_factory() as db:
            appointment = await appointment_repo.get_appointment(db, appointment_id)
            if appointment is None:
                resolution, reason = "returned", None
            elif appointment.status == AppointmentStatus.PENDING:
                return None
            elif appointment.status == AppointmentStatus.FAILED:
                reason = appointment.failure_reason
                if reason in LOST_RACE_REASONS or reason in LOOKBACK_REASONS:
                    resolution = "returned"
                else:
                    resolution = "failed"
            else:
                resolution, reason = "booked", None
            applied = await waitlist_repo.resolve_entry(
                db,
                entry_id=entry_id,
                appointment_id=appointment_id,
                outcome=resolution,  # type: ignore[arg-type]
                reason=reason,
            )
            await db.commit()
        if not applied:
            return None
        log.info(
            "reconcile.waitlist_resolved",
            entry_id=str(entry_id),
            appointment_id=str(appointment_id),
            resolution=resolution,
            reason=reason,
        )
        await self._audit_best_effort(
            action=f"waitlist.{resolution}",
            entity_type="waitlist_entry",
            entity_id=str(entry_id),
            clinic_id=clinic_id,
            dedupe_key=f"reconciler:{entry_id}:resolve",
            changed_fields=["status"],
            outcome="success" if resolution == "booked" else "failure",
        )
        return resolution

    # --- no-shows (M1) ---------------------------------------------------------

    @activity.defn(name=ACT_RECONCILE_NO_SHOWS)
    async def reconcile_no_shows(self) -> MaintenanceResult:
        """Confirmed appointments whose `slot_end + grace` has passed become
        `no_show`: Billing `no_refund` FIRST, then `mark_no_show`. If either
        fails the appointment stays confirmed and the next run repeats both
        (Billing answers a repeat 200). A checked-in patient is never selected
        (status is confirmed only); the slot stays booked."""
        async with self._session_factory() as db:
            candidates = [
                (a.id, str(a.clinic_id))
                for a in await appointment_repo.list_no_show_candidates(
                    db, grace_minutes=self._no_show_grace, limit=self._limit
                )
            ]
        marked = 0
        for appointment_id, clinic_id in candidates:
            try:
                marked += int(await self._mark_no_show(appointment_id, clinic_id))
            except Exception as exc:  # noqa: BLE001 - per-row isolation
                log.warning(
                    "reconcile.no_show_failed",
                    appointment_id=str(appointment_id),
                    error=type(exc).__name__,
                )
        log.info("reconcile.no_shows", scanned=len(candidates), marked=marked)
        return MaintenanceResult(processed=marked)

    async def _mark_no_show(self, appointment_id: uuid.UUID, clinic_id: str) -> bool:
        # A fresh read first: a patient who checked in (or a cancel) since the
        # list was read must never have their billing moved to no_refund.
        async with self._session_factory() as db:
            current = await appointment_repo.get_appointment(db, appointment_id)
            if current is None or current.status != AppointmentStatus.CONFIRMED:
                return False
        # Billing first. A 404 (no record) or a 409 refusal (already settled) is
        # logged by the helper and does not block the state change; an
        # unreachable Billing raises and leaves the appointment confirmed.
        await self._billing_transition(
            appointment_id,
            "no_refund",
            key=f"noshow:{appointment_id}",
            reason_code="appointment_no_show",
        )
        async with self._session_factory() as db:
            outcome = await appointment_repo.mark_no_show(
                db,
                appointment_id=appointment_id,
                actor=RECONCILER_ACTOR,
                grace_minutes=self._no_show_grace,
            )
            await db.commit()
        if outcome != "no_show":
            log.info(
                "reconcile.no_show_skipped", appointment_id=str(appointment_id), outcome=outcome
            )
            return False
        log.info("reconcile.no_show_marked", appointment_id=str(appointment_id))
        await self._audit_best_effort(
            action="appointment.no_show",
            entity_type="appointment",
            entity_id=str(appointment_id),
            clinic_id=clinic_id,
            dedupe_key=f"reconciler:{appointment_id}:no_show",
            changed_fields=["status"],
        )
        return True

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
        # S5: the slot this run freed goes back to the waitlist, under the id the
        # booking workflow's own re-offer uses (`waitlist-advance-booking-{id}`).
        await offer_slot_to_waitlist(
            self._client,
            slot_id=row.slot_id,
            clinic_id=row.clinic_id,
            source_workflow_id=wf_id,
            failure_event="reconcile.waitlist_offer_failed",
        )
        return True
