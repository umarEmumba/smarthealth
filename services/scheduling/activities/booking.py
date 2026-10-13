"""Booking saga activities (plan 02-06; D-06, D-09, D-11, research Pattern 4).

`SchedulingActivities` receives its dependencies through the constructor (a
session factory for scheduling_db, an httpx client, Scheduling's
`ServiceTokenProvider` and a Mongo client), so the worker wires production
clients and the test harness wires the in-process mesh. Each bound method is
registered under its `ACT_*` name; the workflow calls them by name.

Rules every activity follows:

* Idempotent: each one is a conditional, state-targeted step (or a call to an
  idempotent Billing endpoint keyed by `{workflow_id}:{step}`), so a Temporal
  retry, a duplicate delivery or a re-run after a worker crash is harmless.
* Business outcomes are NON-retryable `ApplicationError`s whose `type` is the
  machine-readable reason (`ReferenceInvalid`, `SlotUnavailable`,
  `BillingRejected`, `ConfirmLostHold`); messages carry no values (they land in
  workflow history). Infrastructure trouble (`UpstreamServiceUnavailable`,
  `ServiceTokenUnavailable`, database errors) propagates and Temporal retries it
  within the bounded policy.
* State-changing steps commit first and write their audit entry LAST with
  `dedupe_key=f"{workflow_id}:{step}"` (one entry per step despite retries;
  review WR-04).
* No PHI anywhere: the coverage reference is read from the appointment row
  inside `billing_precheck` and sent only in the Billing request body; logs and
  audit carry ids, steps and outcomes.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import httpx
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.exceptions import ApplicationError

from libs.audit.writer import write_audit
from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalConflict, InternalNotFound, InternalRejected, internal_request
from libs.observability.logging import get_logger
from services.scheduling import references
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_BILLING,
    ACT_CONFIRM,
    ACT_MARK_FAILED,
    ACT_NOTIFY,
    ACT_RELEASE,
    ACT_RESERVE,
    ACT_VALIDATE,
    ACT_VOID,
    BookingInput,
    MarkFailedInput,
)
from services.scheduling.repository import appointment_repo, slot_repo
from services.scheduling.repository.models import Appointment

log = get_logger(__name__)

ACTOR = "system:booking"
AUDIT_ACTOR_ID = "service:scheduling"
AUDIT_ACTOR_ROLE = "service"
VOID_REASON_CODE = "booking_compensated"
NOTIFICATION_TYPE = "booking_confirmed"


def _business_error(kind: str, message: str) -> ApplicationError:
    """A non-retryable business outcome; `message` must carry no values."""
    return ApplicationError(message, type=kind, non_retryable=True)


def _workflow_id() -> str:
    return activity.info().workflow_id or "unknown"


class SchedulingActivities:
    """The booking saga's steps, bound to injected clients."""

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
            self.validate,
            self.reserve_slot,
            self.billing_precheck,
            self.schedule_notification,
            self.confirm,
            self.release_slot,
            self.void_billing,
            self.mark_failed,
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
    ) -> None:
        await write_audit(
            self._mongo,
            actor_id=AUDIT_ACTOR_ID,
            actor_role=AUDIT_ACTOR_ROLE,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            clinic_id=clinic_id,
            outcome="success",
            changed_fields=changed_fields,
            dedupe_key=f"{_workflow_id()}:{step}",
        )

    # --- forward steps -----------------------------------------------------

    @activity.defn(name=ACT_VALIDATE)
    async def validate(self, inp: BookingInput) -> None:
        """Re-validate every reference at booking time (D-09 step 1, research
        Pitfall 14): active patient in this clinic, active provider whose
        department belongs to this clinic, and the slot in this clinic for this
        provider. Read-only, so it needs no compensation and no audit."""
        clinic_id = uuid.UUID(inp.clinic_id)
        invalid = _business_error("ReferenceInvalid", "reference invalid")

        patient = await references.get_patient_ref(
            self._http, self._tokens, uuid.UUID(inp.patient_id), clinic_id
        )
        if patient is None or not patient.get("is_active", False):
            log.info("booking.validate", appointment_id=inp.appointment_id, outcome="patient_invalid")
            raise invalid

        provider = await references.get_provider_ref(
            self._http, self._tokens, uuid.UUID(inp.provider_id)
        )
        if provider is None or not provider.get("is_active", False):
            log.info("booking.validate", appointment_id=inp.appointment_id, outcome="provider_invalid")
            raise invalid

        department = await references.get_department_ref(
            self._http, self._tokens, uuid.UUID(str(provider["department_id"]))
        )
        if (
            department is None
            or str(department.get("clinic_id")) != inp.clinic_id
            or not department.get("is_active", True)
        ):
            log.info("booking.validate", appointment_id=inp.appointment_id, outcome="department_invalid")
            raise invalid

        async with self._session_factory() as db:
            slot = await db.scalar(
                text(
                    "SELECT 1 FROM provider_slots "
                    "WHERE id = :s AND clinic_id = :c AND provider_id = :p"
                ),
                {"s": uuid.UUID(inp.slot_id), "c": clinic_id, "p": uuid.UUID(inp.provider_id)},
            )
        if slot is None:
            log.info("booking.validate", appointment_id=inp.appointment_id, outcome="slot_invalid")
            raise invalid
        log.info("booking.validate", appointment_id=inp.appointment_id, outcome="valid")

    @activity.defn(name=ACT_RESERVE)
    async def reserve_slot(self, inp: BookingInput) -> None:
        """Hold the slot for this appointment for `hold_ttl_minutes` (D-03).
        A repeat by the same appointment succeeds; a slot held/booked by
        another appointment, withdrawn or missing is `SlotUnavailable`."""
        async with self._session_factory() as db:
            held = await slot_repo.hold_slot(
                db,
                slot_id=uuid.UUID(inp.slot_id),
                appointment_id=uuid.UUID(inp.appointment_id),
                ttl_minutes=self._hold_ttl_minutes,
            )
            if not held:
                await db.rollback()
                log.info("booking.reserve_slot", appointment_id=inp.appointment_id, outcome="unavailable")
                raise _business_error("SlotUnavailable", "slot unavailable")
            await db.commit()
        log.info("booking.reserve_slot", appointment_id=inp.appointment_id, outcome="held")
        await self._audit(
            action="slot.hold",
            entity_type="slot",
            entity_id=inp.slot_id,
            clinic_id=inp.clinic_id,
            step="reserve_slot",
            changed_fields=["status", "held_until", "held_by_appointment_id"],
        )

    @activity.defn(name=ACT_BILLING)
    async def billing_precheck(self, inp: BookingInput) -> None:
        """Run Billing's deterministic pre-check (BILL-01). The coverage
        reference is loaded HERE from the appointment row, never carried in
        workflow input. Billing is idempotent per appointment and key; a
        `failed` (or otherwise non-verified) record is `BillingRejected`."""
        async with self._session_factory() as db:
            appointment = await db.get(Appointment, uuid.UUID(inp.appointment_id))
            if appointment is None:
                raise _business_error("ReferenceInvalid", "reference invalid")
            coverage_ref = appointment.coverage_ref

        token = await self._tokens.get("billing")
        try:
            record = await internal_request(
                self._http,
                "POST",
                f"{settings.billing_service_url}/internal/billing/prechecks",
                token=token,
                service="billing",
                idempotency_key=f"{_workflow_id()}:billing_precheck",
                json={
                    "appointment_id": inp.appointment_id,
                    "clinic_id": inp.clinic_id,
                    "coverage_ref": coverage_ref,
                },
            )
        except (InternalConflict, InternalRejected, InternalNotFound) as exc:
            log.info(
                "booking.billing_precheck",
                appointment_id=inp.appointment_id,
                outcome="refused",
                error=type(exc).__name__,
            )
            raise _business_error("BillingRejected", "billing pre-check rejected") from exc

        billing_status = (record or {}).get("status")
        log.info("booking.billing_precheck", appointment_id=inp.appointment_id, status=billing_status)
        if billing_status != "verified":
            raise _business_error("BillingRejected", "billing pre-check rejected")

    @activity.defn(name=ACT_NOTIFY)
    async def schedule_notification(self, inp: BookingInput) -> None:
        """Record the booking-confirmation notification INTENT (D-11).

        The single seam Phase 3 replaces with a real send (Celery). Ids and the
        notification type only."""
        log.info(
            "notification.requested",
            appointment_id=inp.appointment_id,
            notification_type=NOTIFICATION_TYPE,
        )
        await self._audit(
            action="notification.requested",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="schedule_notification",
            changed_fields=["notification_type"],
        )

    @activity.defn(name=ACT_CONFIRM)
    async def confirm(self, inp: BookingInput) -> None:
        """Slot held -> booked and appointment pending -> confirmed in one
        transaction. A lost hold (reaped or taken) or an appointment that is no
        longer pending is `ConfirmLostHold` and the saga compensates."""
        async with self._session_factory() as db:
            confirmed = await appointment_repo.confirm_booking(
                db,
                appointment_id=uuid.UUID(inp.appointment_id),
                slot_id=uuid.UUID(inp.slot_id),
                actor=ACTOR,
            )
            if not confirmed:
                await db.rollback()
                log.info("booking.confirm", appointment_id=inp.appointment_id, outcome="lost_hold")
                raise _business_error("ConfirmLostHold", "slot hold lost before confirm")
            await db.commit()
        log.info("booking.confirm", appointment_id=inp.appointment_id, outcome="confirmed")
        await self._audit(
            action="appointment.confirm",
            entity_type="appointment",
            entity_id=inp.appointment_id,
            clinic_id=inp.clinic_id,
            step="confirm",
            changed_fields=["status"],
        )

    # --- compensations (forward transitions, idempotent) ---------------------

    @activity.defn(name=ACT_RELEASE)
    async def release_slot(self, inp: BookingInput) -> None:
        """Return the slot this appointment holds to `available`. A hold that
        is already gone (never taken, reaped, repeat) is a no-op; a slot held
        or booked by another appointment is never touched."""
        async with self._session_factory() as db:
            released = await slot_repo.release_hold(
                db, slot_id=uuid.UUID(inp.slot_id), appointment_id=uuid.UUID(inp.appointment_id)
            )
            await db.commit()
        log.info(
            "booking.release_slot",
            appointment_id=inp.appointment_id,
            outcome="released" if released else "not_ours",
        )
        if released:
            await self._audit(
                action="slot.release",
                entity_type="slot",
                entity_id=inp.slot_id,
                clinic_id=inp.clinic_id,
                step="release_slot",
                changed_fields=["status", "held_until", "held_by_appointment_id"],
            )

    @activity.defn(name=ACT_VOID)
    async def void_billing(self, inp: BookingInput) -> None:
        """Void the appointment's billing record. No record (the pre-check never
        ran) is a no-op; Billing treats a repeat as a no-op; a record Billing
        cannot void any more is logged and left alone (nothing to undo)."""
        token = await self._tokens.get("billing")
        try:
            await internal_request(
                self._http,
                "POST",
                f"{settings.billing_service_url}/internal/billing/records/{inp.appointment_id}/transitions",
                token=token,
                service="billing",
                idempotency_key=f"{_workflow_id()}:void_billing",
                json={"target": "voided", "reason_code": VOID_REASON_CODE},
            )
        except InternalNotFound:
            log.info("booking.void_billing", appointment_id=inp.appointment_id, outcome="no_record")
            return
        except (InternalConflict, InternalRejected) as exc:
            log.warning(
                "booking.void_billing",
                appointment_id=inp.appointment_id,
                outcome="refused",
                error=type(exc).__name__,
            )
            return
        log.info("booking.void_billing", appointment_id=inp.appointment_id, outcome="voided")

    @activity.defn(name=ACT_MARK_FAILED)
    async def mark_failed(self, inp: MarkFailedInput) -> None:
        """Appointment pending -> failed with the machine-readable reason."""
        async with self._session_factory() as db:
            failed = await appointment_repo.mark_appointment_failed(
                db, appointment_id=uuid.UUID(inp.appointment_id), reason=inp.reason, actor=ACTOR
            )
            await db.commit()
            appointment = await db.get(Appointment, uuid.UUID(inp.appointment_id))
            clinic_id = str(appointment.clinic_id) if appointment is not None else None
        log.info(
            "booking.mark_failed",
            appointment_id=inp.appointment_id,
            reason=inp.reason,
            outcome="failed" if failed else "not_pending",
        )
        if failed:
            await self._audit(
                action="appointment.fail",
                entity_type="appointment",
                entity_id=inp.appointment_id,
                clinic_id=clinic_id,
                step="mark_failed",
                changed_fields=["status", "failure_reason"],
            )
