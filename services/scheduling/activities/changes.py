"""Cancel and reschedule activities (plan 02-08; D-13, D-14, research Pattern 5).

`AppointmentChangeActivities` follows the booking activities' rules
(`activities/booking.py`): constructor-injected clients, one idempotent,
state-targeted step per activity, business outcomes as NON-retryable
`ApplicationError`s whose `type` is the machine-readable reason (`NotCancellable`,
`SlotUnavailable`, `SwapRejected`, `BillingConflict`), infrastructure trouble
propagated for Temporal to retry, commit first and audit LAST with
`dedupe_key=f"{workflow_id}:{step}"`.

* Cancel: `cancel_appointment` (appointment `confirmed -> cancelled` and its
  slot back to `available`, one transaction) -> `billing_cancel` (the refund
  policy decided in the API request: `refund_pending` then `refunded`, or
  `no_refund`) -> `schedule_change_notification`.
* Reschedule: `hold_new_slot` -> `swap_slot` (one transaction) ->
  `billing_relink`; on failure `release_new_hold` and `record_change_outcome`.

Billing calls are keyed `{workflow_id}:<step>` so a retry, a duplicate delivery
or a worker crash never applies a transition twice. Ids and statuses only in
logs, audit and workflow I/O (no PHI).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import httpx
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.exceptions import ApplicationError

from libs.audit.writer import Outcome, write_audit
from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalConflict, InternalNotFound, InternalRejected, internal_request
from libs.observability.logging import get_logger
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_BILLING_CANCEL,
    ACT_BILLING_RELINK,
    ACT_CANCEL_APPOINTMENT,
    ACT_HOLD_NEW,
    ACT_NOTIFY_CHANGE,
    ACT_RECORD_OUTCOME,
    ACT_RELEASE_NEW,
    ACT_SWAP,
    REFUND,
    CancelInput,
    ChangeOutcomeInput,
    NotifyInput,
    RescheduleInput,
)
from services.scheduling.repository import appointment_repo, slot_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = "service:scheduling"
AUDIT_ACTOR_ROLE = "service"
CANCEL_REASON_CODE = "appointment_cancelled"


def _business_error(kind: str, message: str) -> ApplicationError:
    """A non-retryable business outcome; `message` must carry no values."""
    return ApplicationError(message, type=kind, non_retryable=True)


def _workflow_id() -> str:
    return activity.info().workflow_id or "unknown"


def _workflow_actor() -> str:
    """History actor for steps whose rows must be idempotent per workflow."""
    return f"workflow:{_workflow_id()}"[:64]


class AppointmentChangeActivities:
    """The cancel and reschedule workflows' steps, bound to injected clients."""

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        http: httpx.AsyncClient,
        tokens: ServiceTokenProvider,
        mongo: AsyncMongoClient,
        *,
        hold_ttl_minutes: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._http = http
        self._tokens = tokens
        self._mongo = mongo
        self._hold_ttl_minutes = hold_ttl_minutes or settings.hold_ttl_minutes

    def all(self) -> list[Callable[..., Any]]:
        """Every activity, for `Worker(activities=...)`."""
        return [
            self.cancel_appointment,
            self.billing_cancel,
            self.hold_new_slot,
            self.swap_slot,
            self.release_new_hold,
            self.billing_relink,
            self.record_change_outcome,
            self.schedule_change_notification,
        ]

    async def _audit(
        self,
        *,
        action: str,
        entity_type: str,
        entity_id: str,
        clinic_id: str | None,
        step: str,
        changed_fields: list[str] | None = None,
        outcome: Outcome = "success",
    ) -> None:
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
            dedupe_key=f"{_workflow_id()}:{step}",
        )

    async def _billing_post(self, path: str, *, key: str, json: dict | None = None) -> None:
        token = await self._tokens.get("billing")
        await internal_request(
            self._http,
            "POST",
            f"{settings.billing_service_url}{path}",
            token=token,
            service="billing",
            idempotency_key=key,
            json=json,
        )

    # --- cancel ---------------------------------------------------------------

    @activity.defn(name=ACT_CANCEL_APPOINTMENT)
    async def cancel_appointment(self, inp: CancelInput) -> None:
        """Appointment `confirmed -> cancelled`, slot -> `available`, one
        transaction. A repeat is a no-op; any other state is `NotCancellable`."""
        async with self._session_factory() as db:
            outcome = await appointment_repo.cancel_appointment(
                db, appointment_id=uuid.UUID(inp.appointment_id), actor=inp.actor
            )
            if outcome == "not_cancellable":
                await db.rollback()
                log.info("change.cancel_appointment", appointment_id=inp.appointment_id, outcome=outcome)
                raise _business_error("NotCancellable", "appointment not cancellable")
            await db.commit()
        log.info("change.cancel_appointment", appointment_id=inp.appointment_id, outcome=outcome)
        await self._audit(
            action="appointment.cancel",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="cancel_appointment",
            changed_fields=["status"],
        )

    @activity.defn(name=ACT_BILLING_CANCEL)
    async def billing_cancel(self, inp: CancelInput) -> None:
        """Drive the billing record by the refund policy (D-13, BILL-02):
        `refund` -> `refund_pending` then `refunded` (two state-targeted calls,
        settlement simulated by Billing); `no_refund` -> `no_refund`. No record
        is a no-op; a record Billing cannot move that way is `BillingConflict`."""
        path = f"/internal/billing/records/{inp.appointment_id}/transitions"
        targets = ["refund_pending", "refunded"] if inp.refund_policy == REFUND else ["no_refund"]
        try:
            for target in targets:
                await self._billing_post(
                    path,
                    key=f"{_workflow_id()}:{target}",
                    json={"target": target, "reason_code": CANCEL_REASON_CODE},
                )
        except InternalNotFound:
            log.info("change.billing_cancel", appointment_id=inp.appointment_id, outcome="no_record")
            return
        except (InternalConflict, InternalRejected) as exc:
            log.warning(
                "change.billing_cancel",
                appointment_id=inp.appointment_id,
                outcome="refused",
                error=type(exc).__name__,
            )
            raise _business_error("BillingConflict", "billing refused the cancel transition") from exc
        log.info(
            "change.billing_cancel",
            appointment_id=inp.appointment_id,
            outcome="applied",
            policy=inp.refund_policy,
        )
        await self._audit(
            action="billing.refund" if inp.refund_policy == REFUND else "billing.no_refund",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="billing_cancel",
            changed_fields=["billing_status"],
        )

    # --- reschedule -----------------------------------------------------------

    @activity.defn(name=ACT_HOLD_NEW)
    async def hold_new_slot(self, inp: RescheduleInput) -> None:
        """Hold the new slot for this appointment first (D-14 reserve). A slot
        held/booked by another appointment, withdrawn or missing is
        `SlotUnavailable`; a repeat by this appointment succeeds."""
        async with self._session_factory() as db:
            held = await slot_repo.hold_slot(
                db,
                slot_id=uuid.UUID(inp.new_slot_id),
                appointment_id=uuid.UUID(inp.appointment_id),
                ttl_minutes=self._hold_ttl_minutes,
            )
            if not held:
                await db.rollback()
                log.info("change.hold_new_slot", appointment_id=inp.appointment_id, outcome="unavailable")
                raise _business_error("SlotUnavailable", "slot unavailable")
            await db.commit()
        log.info("change.hold_new_slot", appointment_id=inp.appointment_id, outcome="held")
        await self._audit(
            action="slot.hold",
            entity_type="slot",
            entity_id=inp.new_slot_id,
            clinic_id=inp.clinic_id,
            step="hold_new_slot",
            changed_fields=["status", "held_until", "held_by_appointment_id"],
        )

    @activity.defn(name=ACT_SWAP)
    async def swap_slot(self, inp: RescheduleInput) -> None:
        """One local transaction: new slot booked, appointment repointed, old
        slot available (D-14). A refusal is `SwapRejected` and rolls back, so
        the original booking is untouched."""
        async with self._session_factory() as db:
            outcome = await appointment_repo.swap_slot(
                db,
                appointment_id=uuid.UUID(inp.appointment_id),
                old_slot_id=uuid.UUID(inp.old_slot_id),
                new_slot_id=uuid.UUID(inp.new_slot_id),
                actor=inp.actor,
            )
            if outcome == "rejected":
                await db.rollback()
                log.info("change.swap_slot", appointment_id=inp.appointment_id, outcome=outcome)
                raise _business_error("SwapRejected", "reschedule swap rejected")
            await db.commit()
        log.info("change.swap_slot", appointment_id=inp.appointment_id, outcome=outcome)
        await self._audit(
            action="appointment.reschedule",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="swap_slot",
            changed_fields=["slot_id", "slot_start", "slot_end"],
        )

    @activity.defn(name=ACT_RELEASE_NEW)
    async def release_new_hold(self, inp: RescheduleInput) -> None:
        """Compensation: return the new slot to `available` if this appointment
        holds it. Never touches a slot booked by it (a swap that landed) or
        held/booked by another appointment."""
        async with self._session_factory() as db:
            released = await slot_repo.release_hold(
                db, slot_id=uuid.UUID(inp.new_slot_id), appointment_id=uuid.UUID(inp.appointment_id)
            )
            await db.commit()
        log.info(
            "change.release_new_hold",
            appointment_id=inp.appointment_id,
            outcome="released" if released else "not_ours",
        )
        if released:
            await self._audit(
                action="slot.release",
                entity_type="slot",
                entity_id=inp.new_slot_id,
                clinic_id=inp.clinic_id,
                step="release_new_hold",
                changed_fields=["status", "held_until", "held_by_appointment_id"],
            )

    @activity.defn(name=ACT_BILLING_RELINK)
    async def billing_relink(self, inp: RescheduleInput) -> None:
        """Re-link billing to the moved appointment (D-14): one history row,
        no refund. No record is a no-op; a record Billing will not re-link is
        `BillingConflict`."""
        try:
            await self._billing_post(
                f"/internal/billing/records/{inp.appointment_id}/relink",
                key=f"{_workflow_id()}:relink",
            )
        except InternalNotFound:
            log.info("change.billing_relink", appointment_id=inp.appointment_id, outcome="no_record")
            return
        except (InternalConflict, InternalRejected) as exc:
            log.warning(
                "change.billing_relink",
                appointment_id=inp.appointment_id,
                outcome="refused",
                error=type(exc).__name__,
            )
            raise _business_error("BillingConflict", "billing refused the re-link") from exc
        log.info("change.billing_relink", appointment_id=inp.appointment_id, outcome="relinked")
        await self._audit(
            action="billing.relink",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="billing_relink",
            changed_fields=["billing_status_history"],
        )

    # --- shared ---------------------------------------------------------------

    @activity.defn(name=ACT_RECORD_OUTCOME)
    async def record_change_outcome(self, inp: ChangeOutcomeInput) -> None:
        """Write a change outcome that left no transition (a failed reschedule,
        an incomplete billing step) into the appointment's history as a
        `from_status == to_status` note, then audit it. Idempotent per
        workflow (the workflow id is the note's actor)."""
        async with self._session_factory() as db:
            written = await appointment_repo.record_history_note(
                db,
                appointment_id=uuid.UUID(inp.appointment_id),
                reason=inp.note[:64],
                actor=_workflow_actor(),
                bump_version=bool(inp.bump_version),
            )
            await db.commit()
        log.info(
            "change.record_outcome",
            appointment_id=inp.appointment_id,
            note=inp.note,
            outcome="written" if written else "already",
        )
        await self._audit(
            action=inp.audit_action,
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="record_outcome",
            changed_fields=["status_history"],
            outcome="failure",
        )

    @activity.defn(name=ACT_NOTIFY_CHANGE)
    async def schedule_change_notification(self, inp: NotifyInput) -> None:
        """Record the cancel/reschedule notification INTENT (D-11). A separate
        activity from the booking one, so BookingWorkflow is untouched; the
        seam Phase 3 replaces with a real send. Ids and the type only."""
        log.info(
            "notification.requested",
            appointment_id=inp.appointment_id,
            notification_type=inp.notification_type,
        )
        await self._audit(
            action="notification.requested",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="schedule_notification",
            changed_fields=["notification_type"],
        )
