"""Maintenance activities run by Temporal Schedules (plan 02-07; D-03,
research G7 / Pattern 8, Pitfalls 4 and 5).

* `expire_holds` - the hold reaper. Releases slots whose hold outlived its
  saga (for example a worker that died past all retries) and fails the owning
  `pending` appointment with reason `hold_expired`, in one transaction
  (`slot_repo.expire_holds`, `FOR UPDATE SKIP LOCKED`, batch of 100). Audit is
  written after the commit, one entry per released pair, deduplicated by
  `reaper:{appointment_id}`.

  Racing a live saga is safe (Pitfall 5): the saga's `confirm` is conditional
  on `status = 'held' AND held_by_appointment_id = :a`, so a reaped hold makes
  it raise `ConfirmLostHold` and the saga compensates; the appointment stays
  failed and is never confirmed. In normal operation the race cannot happen:
  the hold TTL (10 min) exceeds the saga's bounded worst case (about 4.5 min of
  forward `schedule_to_close` budgets).

* `start_pending_bookings` - the lost-trigger starter. The API commits the
  `pending` row and then starts `booking-{id}`; a crash or a Temporal outage
  between the two leaves a pending appointment with `workflow_started_at IS
  NULL`. This activity starts those (older than the grace window, at most 100
  per run) with `start_once` and then sets the marker. A duplicate start is a
  no-op: a running workflow is attached to (USE_EXISTING), a closed one is
  refused (REJECT_DUPLICATE), and the marker update is conditional.

Both are idempotent, so a Schedule run that overlaps a retry, or two workers
registering the same Schedules, cannot double-process anything.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.client import Client

from libs.audit.writer import write_audit
from libs.observability.logging import get_logger
from libs.temporal import start_once
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    ACT_EXPIRE_HOLDS,
    ACT_START_PENDING,
    BOOKING_WORKFLOW,
    BookingInput,
    MaintenanceResult,
    booking_workflow_id,
)
from services.scheduling.repository import appointment_repo, slot_repo

log = get_logger(__name__)

AUDIT_ACTOR_ID = "system:reaper"
AUDIT_ACTOR_ROLE = "service"
BATCH_LIMIT = 100


class MaintenanceActivities:
    """The reaper and the starter, bound to injected clients. `client` is the
    worker's own Temporal client (the starter starts workflows with it)."""

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        client: Client | None,
        mongo: AsyncMongoClient,
        *,
        starter_grace_seconds: int | None = None,
        batch_limit: int = BATCH_LIMIT,
    ) -> None:
        self._session_factory = session_factory
        self._client = client
        self._mongo = mongo
        self._grace = starter_grace_seconds or settings.starter_grace_seconds
        self._limit = batch_limit

    def all(self) -> list[Callable[..., Any]]:
        """Every maintenance activity, for `Worker(activities=...)`."""
        return [self.expire_holds, self.start_pending_bookings]

    @activity.defn(name=ACT_EXPIRE_HOLDS)
    async def expire_holds(self) -> MaintenanceResult:
        async with self._session_factory() as db:
            released = await slot_repo.expire_holds(db, limit=self._limit)
            clinics: dict[uuid.UUID, str] = {}
            if released:
                rows = await db.execute(
                    text("SELECT id, clinic_id FROM provider_slots WHERE id = ANY(:ids)"),
                    {"ids": [slot_id for slot_id, _ in released]},
                )
                clinics = {row.id: str(row.clinic_id) for row in rows}
            await db.commit()

        for slot_id, appointment_id in released:
            log.info(
                "slot.hold_expired",
                slot_id=str(slot_id),
                appointment_id=str(appointment_id) if appointment_id else None,
            )
            await write_audit(
                self._mongo,
                actor_id=AUDIT_ACTOR_ID,
                actor_role=AUDIT_ACTOR_ROLE,
                action="slot.hold_expired",
                entity_type="slot",
                entity_id=str(slot_id),
                clinic_id=clinics.get(slot_id),
                outcome="success",
                changed_fields=["status", "held_until", "held_by_appointment_id"],
                # A hold always names its appointment; without one there is no
                # stable key, so the (rare) entry is written plainly.
                dedupe_key=f"reaper:{appointment_id}" if appointment_id else None,
            )
        log.info("maintenance.expire_holds", processed=len(released))
        return MaintenanceResult(processed=len(released))

    @activity.defn(name=ACT_START_PENDING)
    async def start_pending_bookings(self) -> MaintenanceResult:
        if self._client is None:  # pragma: no cover - wiring error
            raise RuntimeError("MaintenanceActivities needs a Temporal client to start workflows")
        async with self._session_factory() as db:
            orphans = await appointment_repo.list_unstarted_pending(
                db, older_than_seconds=self._grace, limit=self._limit
            )
            inputs = [
                BookingInput(
                    appointment_id=str(a.id),
                    clinic_id=str(a.clinic_id),
                    patient_id=str(a.patient_id),
                    provider_id=str(a.provider_id),
                    slot_id=str(a.slot_id),
                )
                for a in orphans
            ]

        started = 0
        for inp in inputs:
            fresh = await start_once(
                self._client,
                BOOKING_WORKFLOW,
                inp,
                workflow_id=booking_workflow_id(inp.appointment_id),
                task_queue=SCHEDULING_TASK_QUEUE,
            )
            async with self._session_factory() as db:
                await appointment_repo.mark_workflow_started(
                    db, appointment_id=uuid.UUID(inp.appointment_id)
                )
                await db.commit()
            log.info(
                "booking.workflow_start_recovered",
                appointment_id=inp.appointment_id,
                outcome="started" if fresh else "already_closed",
            )
            started += int(fresh)
        log.info("maintenance.start_pending_bookings", scanned=len(inputs), started=started)
        return MaintenanceResult(processed=started)
