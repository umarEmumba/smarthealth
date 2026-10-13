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
* `record_followup_intent` / `record_analytics_intent` - Phase 2 stubs (D-11):
  a log line and an audit entry, ids only. They are the single Phase 3 seams
  (Celery follow-up job, Kafka `visit.completed` event).
* `sweep_unstarted` - the lost-trigger sweep (`services.visit.downstream`).

Audit is the LAST step of each activity, deduplicated by
`{workflow_id}:{step}` (one entry per step despite retries). Error messages and
logs carry no values.
"""

from __future__ import annotations

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
from services.visit import downstream
from services.visit.config import settings
from services.visit.domain.contracts import (
    ACT_ANALYTICS_INTENT,
    ACT_FINALIZE_BILLING,
    ACT_FOLLOWUP_INTENT,
    ACT_SWEEP,
    BILLING_FINALIZED,
    BILLING_NO_RECORD,
    INTENT_RECORDED,
    SweepResult,
    VisitCompletedInput,
)

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
    ) -> None:
        self._session_factory = session_factory
        self._http = http
        self._tokens = tokens
        self._mongo = mongo
        self._client = client
        self._grace = (
            settings.sweeper_grace_seconds if sweeper_grace_seconds is None else sweeper_grace_seconds
        )

    def all(self) -> list[Callable[..., Any]]:
        """Every activity, for `Worker(activities=...)`."""
        return [
            self.finalize_billing,
            self.record_followup_intent,
            self.record_analytics_intent,
            self.sweep_unstarted,
        ]

    async def _audit(self, inp: VisitCompletedInput, *, action: str, step: str) -> None:
        await write_audit(
            self._mongo,
            actor_id=AUDIT_ACTOR_ID,
            actor_role=AUDIT_ACTOR_ROLE,
            action=action,
            entity_type=ENTITY_TYPE,
            entity_id=inp.visit_id,
            clinic_id=inp.clinic_id,
            outcome="success",
            dedupe_key=f"{_workflow_id()}:{step}",
        )

    @activity.defn(name=ACT_FINALIZE_BILLING)
    async def finalize_billing(self, inp: VisitCompletedInput) -> str:
        workflow_id = _workflow_id()
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
            return BILLING_NO_RECORD
        except InternalConflict as exc:
            log.info("visit.billing_finalize", visit_id=inp.visit_id, outcome="not_finalizable")
            raise ApplicationError(
                "billing not finalizable", type="BillingNotFinalizable", non_retryable=True
            ) from exc
        log.info("visit.billing_finalize", visit_id=inp.visit_id, outcome=BILLING_FINALIZED)
        await self._audit(inp, action="visit.billing_finalize_requested", step="finalize")
        return BILLING_FINALIZED

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
