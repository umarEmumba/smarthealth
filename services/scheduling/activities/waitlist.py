"""Waitlist-advance activities (plan 02-09; SCHED-07, D-12, research Pattern 7).

`WaitlistActivities` follows the other Scheduling activities' rules
(`activities/booking.py`): constructor-injected clients, idempotent
state-targeted steps, commit first and audit LAST with a dedupe key, ids and
statuses only in logs, audit and workflow I/O (the entry's coverage reference
is copied database-to-database onto the appointment and never leaves
scheduling_db).

* `claim_next` (forward step) - ONE transaction: re-read the freed slot (gone,
  no longer `available` or already started -> `slot_gone`), claim the oldest
  matching `waiting` entry with `FOR UPDATE SKIP LOCKED` (none -> `no_match`)
  and create the `pending` appointment for that entry's patient with
  `accept_booking(..., workflow_started=True)` (so the pending-booking starter
  never races the parent's child start). The idempotency key is
  `waitlist-{appointment_id}`, the caller and `created_by` are the entry's
  creator. Database arbiters decide the races (research Pitfall 9):
  `SlotTaken` -> roll back, the entry stays `waiting` (`slot_taken`);
  `PatientOverlap` -> roll back, then a second transaction marks the entry
  `failed`/`PatientOverlap` (`entry_failed`). A retry finds the entry already
  matched to the same appointment id and reports `claimed` again without a
  second insert.
* `resolve_entry` (compensation budget: it must land) - settle the matched
  entry from the child booking saga's result: confirmed -> `booked`; a
  lost-race reason (`LOST_RACE_REASONS`) -> back to `waiting`; any other
  reason -> `failed` with that reason (never retried).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity

from libs.audit.writer import Outcome, write_audit
from libs.observability.logging import get_logger
from services.scheduling.domain.contracts import (
    ACT_WAITLIST_CLAIM,
    ACT_WAITLIST_RESOLVE,
    CLAIM_CLAIMED,
    CLAIM_ENTRY_FAILED,
    CLAIM_NO_MATCH,
    CLAIM_SLOT_GONE,
    CLAIM_SLOT_TAKEN,
    LOST_RACE_REASONS,
    OUTCOME_CONFIRMED,
    ClaimResult,
    WaitlistClaimInput,
    WaitlistResolveInput,
)
from services.scheduling.domain.enums import SlotStatus
from services.scheduling.domain.errors import PatientOverlap, SlotTaken
from services.scheduling.repository import appointment_repo, slot_repo, waitlist_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = "service:scheduling"
AUDIT_ACTOR_ROLE = "service"
PATIENT_OVERLAP = "PatientOverlap"


def _workflow_id() -> str:
    return activity.info().workflow_id or "unknown"


def waitlist_request_hash(slot_id: uuid.UUID, patient_id: uuid.UUID, coverage_ref: str | None) -> str:
    """The same canonical booking-body hash as `api.appointments.request_hash`
    (D-08), over `{slot_id, patient_id, coverage_ref}`. Only the hash is stored."""
    canonical = json.dumps(
        {"slot_id": str(slot_id), "patient_id": str(patient_id), "coverage_ref": coverage_ref},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def waitlist_idempotency_key(appointment_id: str) -> str:
    return f"waitlist-{appointment_id}"


class WaitlistActivities:
    """The waitlist advance's steps, bound to injected clients."""

    def __init__(
        self, session_factory: Callable[[], AsyncSession], mongo: AsyncMongoClient
    ) -> None:
        self._session_factory = session_factory
        self._mongo = mongo

    def all(self) -> list[Callable[..., Any]]:
        """Every activity, for `Worker(activities=...)`."""
        return [self.claim_next, self.resolve_entry]

    async def _audit(
        self,
        *,
        action: str,
        entry_id: str,
        clinic_id: str | None,
        dedupe: str,
        changed_fields: list[str],
        outcome: Outcome = "success",
    ) -> None:
        await write_audit(
            self._mongo,
            actor_id=AUDIT_ACTOR_ID,
            actor_role=AUDIT_ACTOR_ROLE,
            action=action,
            entity_type="waitlist_entry",
            entity_id=entry_id,
            clinic_id=clinic_id,
            outcome=outcome,
            changed_fields=changed_fields,
            dedupe_key=f"{_workflow_id()}:{dedupe}",
        )

    @activity.defn(name=ACT_WAITLIST_CLAIM)
    async def claim_next(self, inp: WaitlistClaimInput) -> ClaimResult:
        appointment_id = uuid.UUID(inp.appointment_id)
        slot_id = uuid.UUID(inp.slot_id)
        async with self._session_factory() as db:
            # A retry of a claim that already committed: report it again.
            existing = await waitlist_repo.find_matched_entry(db, appointment_id=appointment_id)
            if existing is not None:
                log.info("waitlist.claim", appointment_id=inp.appointment_id, outcome="replayed")
                return ClaimResult(
                    outcome=CLAIM_CLAIMED,
                    entry_id=str(existing.id),
                    patient_id=str(existing.patient_id),
                    provider_id=str(existing.provider_id),
                )

            slot = await slot_repo.get_slot(db, slot_id=slot_id)
            now = datetime.now(timezone.utc)
            if (
                slot is None
                or str(slot.clinic_id) != inp.clinic_id
                or slot.status != SlotStatus.AVAILABLE.value
                or slot.start_time <= now
            ):
                await db.rollback()
                log.info("waitlist.claim", slot_id=inp.slot_id, outcome=CLAIM_SLOT_GONE)
                return ClaimResult(outcome=CLAIM_SLOT_GONE)
            provider_id, slot_start, slot_end = slot.provider_id, slot.start_time, slot.end_time
            clinic_id = slot.clinic_id

            entry = await waitlist_repo.claim_next_entry(
                db,
                provider_id=provider_id,
                slot_start=slot_start,
                slot_end=slot_end,
                appointment_id=appointment_id,
            )
            if entry is None:
                await db.rollback()
                log.info("waitlist.claim", slot_id=inp.slot_id, outcome=CLAIM_NO_MATCH)
                return ClaimResult(outcome=CLAIM_NO_MATCH)
            entry_id, patient_id = entry.id, entry.patient_id
            created_by, coverage_ref = entry.created_by, entry.coverage_ref

            try:
                await appointment_repo.accept_booking(
                    db,
                    appointment_id=appointment_id,
                    caller_user_id=created_by,
                    key=waitlist_idempotency_key(inp.appointment_id),
                    request_hash=waitlist_request_hash(slot_id, patient_id, coverage_ref),
                    clinic_id=clinic_id,
                    patient_id=patient_id,
                    provider_id=provider_id,
                    slot_id=slot_id,
                    slot_start=slot_start,
                    slot_end=slot_end,
                    coverage_ref=coverage_ref,
                    created_by=created_by,
                    workflow_started=True,
                )
            except SlotTaken:
                # Lost the slot race at the arbiter: the entry stays waiting
                # (the claim is rolled back with the insert).
                await db.rollback()
                log.info("waitlist.claim", slot_id=inp.slot_id, outcome=CLAIM_SLOT_TAKEN)
                return ClaimResult(outcome=CLAIM_SLOT_TAKEN)
            except PatientOverlap:
                await db.rollback()
                overlap = True
            else:
                overlap = False
                await db.commit()

        if overlap:
            async with self._session_factory() as db:
                await waitlist_repo.fail_waiting_entry(db, entry_id=entry_id, reason=PATIENT_OVERLAP)
                await db.commit()
            log.info(
                "waitlist.claim",
                entry_id=str(entry_id),
                outcome=CLAIM_ENTRY_FAILED,
                reason=PATIENT_OVERLAP,
            )
            await self._audit(
                action="waitlist.failed",
                entry_id=str(entry_id),
                clinic_id=str(clinic_id),
                dedupe=f"claim:{inp.appointment_id}",
                changed_fields=["status", "failure_reason"],
                outcome="failure",
            )
            return ClaimResult(
                outcome=CLAIM_ENTRY_FAILED, entry_id=str(entry_id), reason=PATIENT_OVERLAP
            )

        log.info(
            "waitlist.claim",
            entry_id=str(entry_id),
            appointment_id=inp.appointment_id,
            outcome=CLAIM_CLAIMED,
        )
        await self._audit(
            action="waitlist.matched",
            entry_id=str(entry_id),
            clinic_id=str(clinic_id),
            dedupe=f"claim:{inp.appointment_id}",
            changed_fields=["status", "matched_appointment_id"],
        )
        return ClaimResult(
            outcome=CLAIM_CLAIMED,
            entry_id=str(entry_id),
            patient_id=str(patient_id),
            provider_id=str(provider_id),
        )

    @activity.defn(name=ACT_WAITLIST_RESOLVE)
    async def resolve_entry(self, inp: WaitlistResolveInput) -> str:
        """Settle the matched entry; returns the resolution ("booked",
        "returned" or "failed"). A repeat changes nothing."""
        if inp.booking_outcome == OUTCOME_CONFIRMED:
            resolution = "booked"
        elif inp.reason in LOST_RACE_REASONS:
            resolution = "returned"
        else:
            resolution = "failed"
        async with self._session_factory() as db:
            applied = await waitlist_repo.resolve_entry(
                db,
                entry_id=uuid.UUID(inp.entry_id),
                appointment_id=uuid.UUID(inp.appointment_id),
                outcome=resolution,  # type: ignore[arg-type]
                reason=inp.reason,
            )
            entry = await waitlist_repo.get_entry(db, uuid.UUID(inp.entry_id))
            clinic_id = str(entry.clinic_id) if entry is not None else None
            await db.commit()
        log.info(
            "waitlist.resolve",
            entry_id=inp.entry_id,
            appointment_id=inp.appointment_id,
            resolution=resolution,
            reason=inp.reason,
            applied=applied,
        )
        await self._audit(
            action=f"waitlist.{resolution}",
            entry_id=inp.entry_id,
            clinic_id=clinic_id,
            dedupe=f"resolve:{inp.appointment_id}",
            changed_fields=["status"],
            outcome="success" if resolution == "booked" else "failure",
        )
        return resolution
