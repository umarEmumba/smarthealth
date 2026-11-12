"""Visit's downstream activities (plan 02-11; VISIT-04, BILL-02, D-06, D-11, D-16).

`VisitActivities` receives its dependencies through the constructor (a session
factory for visit_db, an httpx client, Visit's `ServiceTokenProvider`, a Mongo
client and a Temporal client for the sweep), so the worker wires production
clients and the test harness wires the in-process mesh. Each bound method is
registered under its `ACT_*` name; the workflows call them by name.

* `finalize_billing` - Billing's state-targeted transition `verified ->
  finalized` over an aud=billing service token minted for `service:visit`
  (Billing lets Visit target ONLY `finalized`), with Idempotency-Key
  `{workflow_id}:finalize`, so a retry or a re-run is a 200 no-op in Billing.
  No record -> "no_record" (nothing to finalize, e.g. a pre-Phase-2 booking);
  a record that cannot be finalized (voided, refunded, ...) is the
  non-retryable business outcome `BillingNotFinalizable`; transport trouble,
  401/403/5xx and `ServiceTokenUnavailable` propagate and Temporal retries.
* `complete_appointment` (plan 02.1-09, M1) - Scheduling's internal complete
  route over an aud=scheduling service token minted for `service:visit`
  (`checked_in -> completed`; a repeat is a 200 no-op there). Outcomes:
  `completed`, `no_appointment` (404), and the terminal business refusal
  `AppointmentNotCheckedIn` (409: a visit created before plan 02.1-06 whose
  appointment is still `confirmed`), which is never retried. Transport trouble
  and `ServiceTokenUnavailable` propagate and Temporal retries.
* Settlement markers (plan 02.1-09, S4 groundwork): `finalize_billing` and
  `complete_appointment` each stamp a timestamp + outcome on the visit row
  (conditional on the marker still being NULL, so a repeat never overwrites)
  once the step has landed OR reached a terminal business outcome. The reconciler
  (plan 02.1-10) reads the NULL ones.
* `reconcile_unsettled` (plan 02.1-10, S4) - the reconciler pass: completed visits
  whose billing or appointment marker is still empty after a grace period (a
  legacy `skipped` run, a terminated workflow, a lost trigger) are settled by
  the SAME step bodies the workflow uses (`_finalize_billing`,
  `_complete_appointment`), called with the explicit workflow id
  `visit-completed-{visit_id}`: Billing's Idempotency-Key and the audit dedupe
  key are therefore identical whether the workflow or the reconciler runs the
  step, and the two can race without a double effect.
* `record_followup_intent` / `record_analytics_intent` - Phase 2 stubs (D-11):
  a log line and an audit entry, ids only. They are the single Phase 3 seams
  (Celery follow-up job, Kafka `visit.completed` event).
* `sweep_unstarted` - the lost-trigger sweep (`services.visit.downstream`).

Audit is the LAST step of each activity, deduplicated by
`{workflow_id}:{step}` (one entry per step despite retries). Error messages and
logs carry no values.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import httpx
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from libs.audit.writer import write_audit
from libs.auth.service_tokens import ServiceTokenProvider
from libs.internal_client import InternalConflict, InternalNotFound, internal_request
from libs.observability.logging import get_logger
from services.visit import downstream, references
from services.visit.config import settings
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
    ACT_COMPLETE_APPOINTMENT,
    ACT_FINALIZE_BILLING,
    ACT_FOLLOWUP_INTENT,
    ACT_RECONCILE,
    ACT_SWEEP,
    APPOINTMENT_COMPLETED,
    APPOINTMENT_NO_APPOINTMENT,
    APPOINTMENT_NOT_CHECKED_IN,
    BILLING_FINALIZED,
    BILLING_NO_RECORD,
    BILLING_NOT_FINALIZABLE,
    INTENT_RECORDED,
    SweepResult,
    VisitCompletedInput,
    visit_completed_workflow_id,
)
from services.visit.repository import visit_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = "service:visit"
AUDIT_ACTOR_ROLE = "service"
ENTITY_TYPE = "visit"
FINALIZE_REASON_CODE = "visit_completed"


def _workflow_id() -> str:
    return activity.info().workflow_id or "unknown"


class VisitActivities:
    """The downstream fan-out's steps and the sweep, bound to injected clients."""

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        http: httpx.AsyncClient,
        tokens: ServiceTokenProvider,
        mongo: AsyncMongoClient,
        client: Client | None,
        *,
        sweeper_grace_seconds: int | None = None,
        reconcile_grace_minutes: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._http = http
        self._tokens = tokens
        self._mongo = mongo
        self._client = client
        self._grace = (
            settings.sweeper_grace_seconds if sweeper_grace_seconds is None else sweeper_grace_seconds
        )
        self._reconcile_grace = (
            settings.reconcile_grace_minutes
            if reconcile_grace_minutes is None
            else reconcile_grace_minutes
        )

    def all(self) -> list[Callable[..., Any]]:
        """Every activity, for `Worker(activities=...)`."""
        return [
            self.finalize_billing,
            self.complete_appointment,
            self.record_followup_intent,
            self.record_analytics_intent,
            self.sweep_unstarted,
            self.reconcile_unsettled,
        ]

    async def _audit(
        self, inp: VisitCompletedInput, *, action: str, step: str, workflow_id: str | None = None
    ) -> None:
        await write_audit(
            self._mongo,
            actor_id=AUDIT_ACTOR_ID,
            actor_role=AUDIT_ACTOR_ROLE,
            action=action,
            entity_type=ENTITY_TYPE,
            entity_id=inp.visit_id,
            clinic_id=inp.clinic_id,
            outcome="success",
            dedupe_key=f"{workflow_id or _workflow_id()}:{step}",
        )

    async def _settle(self, inp: VisitCompletedInput, *, step: str, outcome: str) -> None:
        """Stamp the step's settlement marker in its own short transaction.
        Conditional on the marker being NULL: a repeat keeps the first outcome."""
        mark = (
            visit_repo.mark_billing_settled
            if step == "billing"
            else visit_repo.mark_appointment_settled
        )
        async with self._session_factory() as db:
            await mark(db, visit_id=uuid.UUID(inp.visit_id), outcome=outcome)
            await db.commit()

    @activity.defn(name=ACT_FINALIZE_BILLING)
    async def finalize_billing(self, inp: VisitCompletedInput) -> str:
        return await self._finalize_billing(inp, _workflow_id())

    async def _finalize_billing(self, inp: VisitCompletedInput, workflow_id: str) -> str:
        """The billing step's body, shared by the workflow's activity and the
        reconciler. `workflow_id` is `visit-completed-{visit_id}` on both paths,
        so the Billing Idempotency-Key and the audit dedupe key do not depend on
        who runs it."""
        token = await self._tokens.get("billing")
        try:
            await internal_request(
                self._http,
                "POST",
                f"{settings.billing_service_url}/internal/billing/records/"
                f"{inp.appointment_id}/transitions",
                token=token,
                service="billing",
                idempotency_key=f"{workflow_id}:finalize",
                json={"target": "finalized", "reason_code": FINALIZE_REASON_CODE},
            )
        except InternalNotFound:
            log.info("visit.billing_finalize", visit_id=inp.visit_id, outcome=BILLING_NO_RECORD)
            await self._settle(inp, step="billing", outcome=BILLING_NO_RECORD)
            return BILLING_NO_RECORD
        except InternalConflict as exc:
            log.info(
                "visit.billing_finalize", visit_id=inp.visit_id, outcome=BILLING_NOT_FINALIZABLE
            )
            await self._settle(inp, step="billing", outcome=BILLING_NOT_FINALIZABLE)
            raise ApplicationError(
                "billing not finalizable", type="BillingNotFinalizable", non_retryable=True
            ) from exc
        log.info("visit.billing_finalize", visit_id=inp.visit_id, outcome=BILLING_FINALIZED)
        await self._settle(inp, step="billing", outcome=BILLING_FINALIZED)
        await self._audit(
            inp,
            action="visit.billing_finalize_requested",
            step="finalize",
            workflow_id=workflow_id,
        )
        return BILLING_FINALIZED

    @activity.defn(name=ACT_COMPLETE_APPOINTMENT)
    async def complete_appointment(self, inp: VisitCompletedInput) -> str:
        """Tell Scheduling the visit is over (M1, VISIT-04). The visit is already
        committed, so this catches up; Scheduling's route is idempotent, which
        makes a retry or a reconciler call harmless."""
        return await self._complete_appointment(inp, _workflow_id())

    async def _complete_appointment(self, inp: VisitCompletedInput, workflow_id: str) -> str:
        """The appointment step's body, shared with the reconciler (see
        `_finalize_billing` for why the workflow id is explicit)."""
        try:
            ref = await references.complete_appointment(
                self._http,
                self._tokens,
                appointment_id=uuid.UUID(inp.appointment_id),
                clinic_id=uuid.UUID(inp.clinic_id),
            )
        except references.CompleteRefused as exc:
            # A business refusal (appointment never checked in): terminal. The
            # marker records it so nothing loops, then the branch reports a skip.
            log.info(
                "visit.appointment_complete",
                visit_id=inp.visit_id,
                outcome=APPOINTMENT_NOT_CHECKED_IN,
            )
            await self._settle(inp, step="appointment", outcome=APPOINTMENT_NOT_CHECKED_IN)
            raise ApplicationError(
                "appointment not checked in", type="AppointmentNotCheckedIn", non_retryable=True
            ) from exc
        if ref is None:
            log.info(
                "visit.appointment_complete",
                visit_id=inp.visit_id,
                outcome=APPOINTMENT_NO_APPOINTMENT,
            )
            await self._settle(inp, step="appointment", outcome=APPOINTMENT_NO_APPOINTMENT)
            return APPOINTMENT_NO_APPOINTMENT
        log.info(
            "visit.appointment_complete", visit_id=inp.visit_id, outcome=APPOINTMENT_COMPLETED
        )
        await self._settle(inp, step="appointment", outcome=APPOINTMENT_COMPLETED)
        await self._audit(
            inp,
            action="visit.appointment_completed",
            step="complete_appointment",
            workflow_id=workflow_id,
        )
        return APPOINTMENT_COMPLETED

    @activity.defn(name=ACT_FOLLOWUP_INTENT)
    async def record_followup_intent(self, inp: VisitCompletedInput) -> str:
        # Phase 3 seam: a Celery follow-up job replaces this stub (D-11).
        log.info("followup.requested", visit_id=inp.visit_id, appointment_id=inp.appointment_id)
        await self._audit(inp, action="followup.requested", step="followup")
        return INTENT_RECORDED

    @activity.defn(name=ACT_ANALYTICS_INTENT)
    async def record_analytics_intent(self, inp: VisitCompletedInput) -> str:
        # Phase 3 seam: a Kafka `visit.completed` event replaces this stub (D-11).
        log.info(
            "analytics.visit_completed", visit_id=inp.visit_id, appointment_id=inp.appointment_id
        )
        await self._audit(inp, action="analytics.intent_recorded", step="analytics")
        return INTENT_RECORDED

    @activity.defn(name=ACT_SWEEP)
    async def sweep_unstarted(self) -> SweepResult:
        if self._client is None:  # pragma: no cover - wiring error
            raise RuntimeError("VisitActivities needs a Temporal client to sweep")
        processed = await downstream.sweep_unstarted(
            self._session_factory, self._client, grace_seconds=self._grace
        )
        return SweepResult(processed=processed)

    async def reconcile(self) -> int:
        """One reconciler pass (also run once by the worker at start-up): settle
        completed visits whose markers are still empty after the grace period.
        Returns how many visits ended with both markers set."""
        return await downstream.reconcile_unsettled(
            self._session_factory,
            finalize_billing=self._finalize_billing,
            complete_appointment=self._complete_appointment,
            grace_minutes=self._reconcile_grace,
        )

    @activity.defn(name=ACT_RECONCILE)
    async def reconcile_unsettled(self) -> SweepResult:
        return SweepResult(processed=await self.reconcile())
