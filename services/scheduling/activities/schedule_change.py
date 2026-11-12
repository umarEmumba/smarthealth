"""Schedule-change activities (plan 02.1-08; mentor item M2, Scheduling side).

Provider writes one durable event per change that can make already-generated
slots wrong (plan 02.1-05). Two activities consume that feed:

* `poll_schedule_changes` - run by `ScheduleChangePollerWorkflow` on a Temporal
  Schedule. Lists up to 100 pending events, starts ONE `ScheduleChangeWorkflow`
  per event under the deterministic id `schedule-change-{event_id}`
  (`start_once`) and ONLY THEN acknowledges the event, so a crash between the
  two re-delivers the event (at-least-once). The redelivery is harmless: the
  start is a no-op (the workflow exists or already closed) and the ack is
  idempotent. One failing event is logged and skipped; it stays pending and the
  next poll retries it, without blocking the others.

* `apply_schedule_change` - the step the workflow runs twice (immediately, and
  again after the hold TTL). It works by SET DIFFERENCE, never by tracking which
  template made which slot: the provider's CURRENT templates are re-expanded
  with the pure, DST-safe `expand_templates` over clinic-local yesterday ..
  the local date of the provider's latest slot, and every FUTURE slot whose
  exact (start, end) is not in that set is stale. An inactive or unknown
  provider yields the empty set. This is idempotent, needs no slot-to-template
  link and reuses the tested generator, so the candidate set cannot drift from
  what generation would produce.

  Only `available` stale slots are withdrawn, through the existing conditional
  `withdraw_slot` (`UPDATE ... WHERE status = 'available'`); `held` and `booked`
  slots are never withdrawn. A stale `booked` slot's `confirmed` or `checked_in`
  appointment is FLAGGED instead (`flag_schedule_change`: it stays confirmed, keeps
  its slot, and staff cancel or reschedule it; compensations never undo it). A
  stale `held` slot belongs to an in-flight booking and is skipped in pass 1; the
  workflow's second pass, after the hold TTL, finds it booked (flag it), available
  again (withdraw it) or cancelled (nothing to do). For an ACTIVE provider the same
  step then inserts the not-yet-existing candidates (`ON CONFLICT DO NOTHING`), so
  an edit replaces slots instead of only removing them; an existing slot is never
  touched and a withdrawn one is never resurrected. Withdrawals, flags and inserts
  commit in ONE transaction.

  Ambiguity withdraws NOTHING: an owner that cannot answer propagates as the
  retryable `UpstreamServiceUnavailable`; a missing department, clinic,
  availability list or an unreadable clinic timezone is the non-retryable
  business error `ReferenceInvalid` raised before any write.

After the commit, one best-effort notification INTENT is requested per newly
flagged appointment (a PHI-free `notification.requested` log line and audit entry,
type `appointment_schedule_changed`, dedupe key `{workflow_id}:notify:{appointment_id}`);
its failure is logged and never fails the activity. State changes commit first;
the audit entry is written LAST with
`dedupe_key=f"{workflow_id}:apply:{pass_number}"` and field names only, as a
best-effort write (the slots and flags are already committed and durable, and a
failed audit must not make the workflow fail before its second pass). Logs carry
ids and counts only, never a name or a clinical value.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from libs.audit.writer import write_audit
from libs.auth.service_tokens import ServiceTokenProvider
from libs.observability.logging import get_logger
from libs.temporal import start_once
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling import references
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_APPLY_SCHEDULE_CHANGE,
    ACT_POLL_SCHEDULE_CHANGES,
    NOTIFY_SCHEDULE_CHANGED,
    SCHEDULE_CHANGE_WORKFLOW,
    MaintenanceResult,
    ScheduleChangeInput,
    ScheduleChangeResult,
    schedule_change_workflow_id,
)
from services.scheduling.domain.enums import SlotStatus
from services.scheduling.domain.slot_generation import CandidateSlot, expand_templates
from services.scheduling.repository import appointment_repo, slot_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = "service:scheduling"
AUDIT_ACTOR_ROLE = "service"
CHANGE_ACTOR = "system:schedule-change"
POLL_LIMIT = 100
APPLY_CHANGED_FIELDS = ["provider_slots", "schedule_change_flagged_at"]


def _business_error(kind: str, message: str) -> ApplicationError:
    """A non-retryable business outcome; `message` must carry no values."""
    return ApplicationError(message, type=kind, non_retryable=True)


def _workflow_id() -> str:
    return activity.info().workflow_id or "unknown"


class _Candidates:
    """What the provider's current templates define: the exact (start, end)
    pairs that are valid, the generator's candidates (to insert), the clinic and
    its timezone. `tz` is None for an inactive or unknown provider (empty set)."""

    def __init__(
        self,
        *,
        tz: ZoneInfo | None,
        templates: list[dict[str, Any]],
        clinic_id: uuid.UUID | None,
    ) -> None:
        self.tz = tz
        self.templates = templates
        self.clinic_id = clinic_id

    @property
    def active(self) -> bool:
        return self.tz is not None


class ScheduleChangeActivities:
    """The schedule-change poller and apply steps, bound to injected clients.
    `client` is the worker's own Temporal client (the poller starts workflows
    with it)."""

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        http: httpx.AsyncClient,
        tokens: ServiceTokenProvider,
        mongo: AsyncMongoClient,
        client: Client | None,
    ) -> None:
        self._session_factory = session_factory
        self._http = http
        self._tokens = tokens
        self._mongo = mongo
        self._client = client

    def all(self) -> list[Callable[..., Any]]:
        """Every schedule-change activity, for `Worker(activities=...)`."""
        return [self.poll_schedule_changes, self.apply_schedule_change]

    # --- the poller ----------------------------------------------------------

    @activity.defn(name=ACT_POLL_SCHEDULE_CHANGES)
    async def poll_schedule_changes(self) -> MaintenanceResult:
        """Start one workflow per pending Provider event, then acknowledge it."""
        if self._client is None:  # pragma: no cover - wiring error
            raise RuntimeError("ScheduleChangeActivities needs a Temporal client to start workflows")
        events = await references.list_schedule_changes(self._http, self._tokens, limit=POLL_LIMIT)

        processed = 0
        for event in events:
            event_id = str(event.get("id"))
            try:
                template_id = event.get("template_id")
                inp = ScheduleChangeInput(
                    event_id=event_id,
                    provider_id=str(event["provider_id"]),
                    change_type=str(event["change_type"]),
                    template_id=str(template_id) if template_id else None,
                    # The second pass runs one minute after the hold TTL, so a
                    # booking that held the slot just before the change has
                    # either confirmed or been reaped by then.
                    second_pass_after_minutes=settings.hold_ttl_minutes + 1,
                )
                fresh = await start_once(
                    self._client,
                    SCHEDULE_CHANGE_WORKFLOW,
                    inp,
                    workflow_id=schedule_change_workflow_id(event_id),
                    task_queue=SCHEDULING_TASK_QUEUE,
                )
                # Acknowledge only after the workflow is durably started (or
                # already ran): a crash before this line re-delivers the event.
                await references.ack_schedule_change(self._http, self._tokens, event_id)
            except Exception as exc:  # noqa: BLE001 - one event never blocks the rest
                log.warning(
                    "schedule_change.poll_event_failed",
                    event_id=event_id,
                    error=type(exc).__name__,
                )
                continue
            processed += 1
            log.info(
                "schedule_change.event_started",
                event_id=event_id,
                outcome="started" if fresh else "already_started",
            )
        log.info("maintenance.poll_schedule_changes", listed=len(events), processed=processed)
        return MaintenanceResult(processed=processed)

    # --- the apply step -------------------------------------------------------

    async def _resolve_candidates(self, provider_id: uuid.UUID) -> _Candidates:
        """The provider's current candidate definition, or an empty one.

        An unknown or inactive provider has no valid slots (empty set). An
        ACTIVE provider needs its department, clinic timezone and templates:
        an owner that cannot answer raises the retryable
        `UpstreamServiceUnavailable`; an answer that says "does not exist" (or
        an unreadable timezone) is `ReferenceInvalid` and nothing is withdrawn,
        because the candidate set would be a guess.
        """
        provider = await references.get_provider_ref(self._http, self._tokens, provider_id)
        if provider is None or not provider.get("is_active", False):
            return _Candidates(tz=None, templates=[], clinic_id=None)

        department = await references.get_department_ref(
            self._http, self._tokens, uuid.UUID(str(provider["department_id"]))
        )
        if department is None:
            log.info("schedule_change.apply", provider_id=str(provider_id), outcome="department_missing")
            raise _business_error("ReferenceInvalid", "reference invalid")
        clinic_id = uuid.UUID(str(department["clinic_id"]))
        clinic = await references.get_clinic_ref(self._http, self._tokens, clinic_id)
        if clinic is None:
            log.info("schedule_change.apply", provider_id=str(provider_id), outcome="clinic_missing")
            raise _business_error("ReferenceInvalid", "reference invalid")
        try:
            tz = ZoneInfo(str(clinic["timezone"]))
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            log.info("schedule_change.apply", provider_id=str(provider_id), outcome="timezone_invalid")
            raise _business_error("ReferenceInvalid", "reference invalid") from exc
        templates = await references.get_provider_availability(self._http, self._tokens, provider_id)
        if templates is None:
            log.info("schedule_change.apply", provider_id=str(provider_id), outcome="templates_missing")
            raise _business_error("ReferenceInvalid", "reference invalid")
        return _Candidates(tz=tz, templates=templates, clinic_id=clinic_id)

    @staticmethod
    def _candidate_slots(
        resolved: _Candidates, *, latest_start: datetime | None
    ) -> list[CandidateSlot]:
        """Expand the templates over clinic-local yesterday .. the local date of
        the provider's latest slot. Empty for an inactive provider or one with
        no slots yet."""
        if resolved.tz is None or latest_start is None:
            return []
        now_local = datetime.now(timezone.utc).astimezone(resolved.tz).date()
        from_date = now_local - timedelta(days=1)
        to_date = latest_start.astimezone(resolved.tz).date()
        if to_date < from_date:
            return []
        return expand_templates(resolved.templates, resolved.tz, from_date, to_date)

    @activity.defn(name=ACT_APPLY_SCHEDULE_CHANGE)
    async def apply_schedule_change(self, inp: ScheduleChangeInput) -> ScheduleChangeResult:
        """Withdraw the provider's stale available future slots (see the module
        docstring). Idempotent: a repeat finds nothing left to change."""
        provider_id = uuid.UUID(inp.provider_id)
        resolved = await self._resolve_candidates(provider_id)  # raises before any write

        withdrawn = 0
        newly_flagged: list[tuple[uuid.UUID, uuid.UUID]] = []  # (appointment, clinic)
        clinic_id: uuid.UUID | None = resolved.clinic_id
        async with self._session_factory() as db:
            latest = await slot_repo.latest_generated_start(db, provider_id=provider_id)
            candidates = self._candidate_slots(resolved, latest_start=latest)
            valid = {(c.start, c.end) for c in candidates}
            future = await slot_repo.list_future_slots(db, provider_id=provider_id)

            for slot in future:
                if clinic_id is None:
                    clinic_id = slot.clinic_id
                if (slot.start_time, slot.end_time) in valid:
                    continue
                if slot.status == SlotStatus.AVAILABLE.value:
                    outcome = await slot_repo.withdraw_slot(db, slot_id=slot.id)
                    withdrawn += int(outcome == "withdrawn")
                elif slot.status == SlotStatus.BOOKED.value:
                    appointment_id = await appointment_repo.active_appointment_on_slot(
                        db, slot_id=slot.id
                    )
                    if appointment_id is not None and await appointment_repo.flag_schedule_change(
                        db, appointment_id=appointment_id, reason=inp.change_type
                    ):
                        newly_flagged.append((appointment_id, slot.clinic_id))
                # `held`: an in-flight booking owns the slot; pass 2 (after the
                # hold TTL) sees it booked, available again or cancelled.

            # Replace what was withdrawn: insert the candidates that do not
            # exist yet (after the withdrawals, so a withdrawn slot's window is
            # free again). Never an existing slot; never a slot already started.
            if resolved.active and resolved.clinic_id is not None:
                now = datetime.now(timezone.utc)
                fresh = [c for c in candidates if c.start > now]
                await slot_repo.insert_generated_slots(
                    db, provider_id=provider_id, clinic_id=resolved.clinic_id, candidates=fresh
                )
            await db.commit()

        log.info(
            "schedule_change.apply",
            event_id=inp.event_id,
            provider_id=inp.provider_id,
            change_type=inp.change_type,
            pass_number=inp.pass_number,
            withdrawn=withdrawn,
            flagged=len(newly_flagged),
            outcome="applied",
        )
        for appointment_id, appointment_clinic in newly_flagged:
            await self._request_notification(appointment_id, appointment_clinic)
        await self._audit_best_effort(inp, clinic_id=clinic_id)
        return ScheduleChangeResult(withdrawn=withdrawn, flagged=len(newly_flagged))

    async def _request_notification(self, appointment_id: uuid.UUID, clinic_id: uuid.UUID) -> None:
        """Record the "your appointment may be affected" notification INTENT
        (D-11): a PHI-free log line and audit entry; the seam Phase 3 replaces
        with a real send. Best effort: the flag is committed, so a failure here
        is logged (ids and exception type) and never fails the activity."""
        log.info(
            "notification.requested",
            appointment_id=str(appointment_id),
            notification_type=NOTIFY_SCHEDULE_CHANGED,
        )
        try:
            await write_audit(
                self._mongo,
                actor_id=AUDIT_ACTOR_ID,
                actor_role=AUDIT_ACTOR_ROLE,
                action="notification.requested",
                entity_type="appointment",
                entity_id=str(appointment_id),
                clinic_id=str(clinic_id),
                outcome="success",
                changed_fields=["notification_type"],
                dedupe_key=f"{_workflow_id()}:notify:{appointment_id}",
            )
        except Exception as exc:  # noqa: BLE001 - deliberately non-fatal
            log.error(
                "audit.write_failed",
                action="notification.requested",
                entity_type="appointment",
                entity_id=str(appointment_id),
                error=type(exc).__name__,
            )

    async def _audit_best_effort(
        self, inp: ScheduleChangeInput, *, clinic_id: uuid.UUID | None
    ) -> None:
        """The apply pass's audit entry, LAST, deduplicated per workflow and
        pass. Best effort: the state is already committed, and a Mongo outage
        must not stop the workflow before its second pass."""
        try:
            await write_audit(
                self._mongo,
                actor_id=AUDIT_ACTOR_ID,
                actor_role=AUDIT_ACTOR_ROLE,
                action="schedule_change.apply",
                entity_type="provider",
                entity_id=inp.provider_id,
                clinic_id=str(clinic_id) if clinic_id is not None else None,
                outcome="success",
                changed_fields=APPLY_CHANGED_FIELDS,
                dedupe_key=f"{_workflow_id()}:apply:{inp.pass_number}",
            )
        except Exception as exc:  # noqa: BLE001 - deliberately non-fatal
            log.error(
                "audit.write_failed",
                action="schedule_change.apply",
                entity_type="provider",
                entity_id=inp.provider_id,
                error=type(exc).__name__,
            )
