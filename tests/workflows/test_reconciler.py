"""The Scheduling reconciler (plan 02.1-11; S4, S5, M1, BILL-02).

`ReconcilerActivities` fails pending appointments whose booking workflow is gone,
resolves waitlist entries stuck `matched`, voids billing orphans, marks no-shows
and (through the hold reaper) offers every freed slot to the waitlist. The tests
run the activities in isolation through `ActivityEnvironment` against the real
scheduling_db, Billing and audit sink, with a real `Worker` on a time-skipping
Temporal test server (`scheduling_worker`) where a workflow has to run.

Isolation from the Compose stack: its scheduling worker runs a live reconciler
(thresholds 10 and 15 minutes, no-show grace 30 minutes) against the SAME
databases, but on a different Temporal server. These tests therefore seed FRESH
rows (never aged by SQL) and reconcile them with an explicit threshold of 0
(`pending_after_minutes=0`, `no_show_grace_minutes=0`), so only the call under
test can select them. A no-show row is a slot that ended a minute ago, which the
live 30-minute grace does not reach. Rows are seeded on throwaway providers or in
the booking world and removed by provider id. The dev database may hold rows of
other runs; a pass with threshold 0 settles those too, so every assertion is about
rows a test created and counts are lower bounds.
"""

from __future__ import annotations

import asyncio
import itertools
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from libs.audit.writer import AUDIT_COLLECTION_NAME
from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from libs.config import shared_settings
from libs.db.postgres import get_session_factory
from libs.temporal import start_once
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.activities.reconcile import ReconcilerActivities
from services.scheduling.config import settings
from services.scheduling.domain.contracts import (
    BOOKING_WORKFLOW,
    BookingInput,
    ReconcilePendingResult,
    WaitlistAdvanceResult,
    booking_workflow_id,
    waitlist_advance_workflow_id,
)
from tests.scheduling.conftest import cleanup_scheduling_rows, seed_slot
from tests.scheduling.seed import World, auth
from tests.scheduling.test_waitlist import add_patient, get_entry, joined

pytestmark = pytest.mark.integration

# A batch limit well above any leftover rows in the dev database, so a pass with
# threshold 0 always reaches the rows a test created.
BIG_LIMIT = 500


# --- helpers -------------------------------------------------------------------------


@dataclass
class Seeded:
    appointment_id: uuid.UUID
    slot_id: uuid.UUID
    provider_id: uuid.UUID
    clinic_id: uuid.UUID
    patient_id: uuid.UUID


class Scope:
    """Throwaway provider ids whose Scheduling rows the test removes."""

    def __init__(self, engine: AsyncEngine, world: World) -> None:
        self.engine = engine
        self.world = world
        self.provider_ids: list[uuid.UUID] = []
        self._starts = itertools.count()

    def new_provider(self) -> uuid.UUID:
        provider_id = uuid.uuid4()
        self.provider_ids.append(provider_id)
        return provider_id

    def future_start(self) -> datetime:
        """A start well beyond the generated slots, unique within the test."""
        base = datetime.combine(
            self.world.last_day + timedelta(days=8), time(0), tzinfo=timezone.utc
        )
        return base + timedelta(minutes=30 * next(self._starts))


@pytest_asyncio.fixture
async def scope(
    committed_scheduling_engine: AsyncEngine, booking_world: World
) -> AsyncIterator[Scope]:
    scope = Scope(committed_scheduling_engine, booking_world)
    try:
        yield scope
    finally:
        await cleanup_scheduling_rows(committed_scheduling_engine, scope.provider_ids)


async def seed_appointment(
    scope: Scope,
    *,
    status: str = "pending",
    provider_id: uuid.UUID | None = None,
    patient_id: uuid.UUID | None = None,
    start: datetime | None = None,
    workflow_started: bool = True,
    slot_status: str | None = None,
    failure_reason: str | None = None,
) -> Seeded:
    """Insert (and COMMIT) one appointment in `status` on a fresh slot. The slot
    mirrors reality: `held` for pending, `booked` for the committed statuses,
    `available` for failed or cancelled. Defaults to a throwaway provider and a
    random patient (no cross-service FK), so rows never collide with the world's
    own."""
    engine = scope.engine
    world = scope.world
    provider_id = provider_id or scope.new_provider()
    patient_id = patient_id or uuid.uuid4()
    start = start if start is not None else scope.future_start()
    default_slot = {
        "pending": "held",
        "confirmed": "booked",
        "checked_in": "booked",
        "completed": "booked",
        "no_show": "booked",
    }.get(status, "available")
    slot_id = await seed_slot(
        engine,
        provider_id=provider_id,
        start=start,
        clinic_id=world.clinic_id,
        department_id=world.department_id,
        status=slot_status or default_slot,
    )
    appointment_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointments (id, clinic_id, patient_id, provider_id, slot_id, "
                "slot_start, slot_end, status, failure_reason, version, created_by, "
                "workflow_started_at) "
                "VALUES (:id, :c, :pt, :pr, :s, :st, :en, :status, :fr, 1, :by, "
                "CASE WHEN :ws THEN now() END)"
            ),
            {
                "id": appointment_id,
                "c": world.clinic_id,
                "pt": patient_id,
                "pr": provider_id,
                "s": slot_id,
                "st": start,
                "en": start + timedelta(minutes=30),
                "status": status,
                "fr": failure_reason,
                "by": uuid.uuid4(),
                "ws": workflow_started,
            },
        )
        await conn.execute(
            text(
                "INSERT INTO appointment_status_history (appointment_id, from_status, to_status, actor) "
                "VALUES (:a, NULL, :st, 'test')"
            ),
            {"a": appointment_id, "st": status},
        )
        if (slot_status or default_slot) == "held":
            await conn.execute(
                text(
                    "UPDATE provider_slots SET held_by_appointment_id = :a, "
                    "held_until = now() + interval '10 minutes' WHERE id = :s"
                ),
                {"a": appointment_id, "s": slot_id},
            )
        elif (slot_status or default_slot) == "booked":
            await conn.execute(
                text("UPDATE provider_slots SET held_by_appointment_id = :a WHERE id = :s"),
                {"a": appointment_id, "s": slot_id},
            )
    return Seeded(appointment_id, slot_id, provider_id, world.clinic_id, patient_id)


async def seed_billing(mesh: httpx.AsyncClient, seeded: Seeded) -> None:
    """The `verified` (self-pay) Billing record a saga leaves behind once its
    pre-check ran, created through Billing's internal pre-check with a service
    token minted for Scheduling (the only allowed caller)."""
    from tests.billing.conftest import precheck, service_token  # noqa: PLC0415

    token = await service_token(mesh, "scheduling")
    resp = await precheck(
        mesh, token, str(seeded.clinic_id), appointment_id=str(seeded.appointment_id)
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "verified"


async def billing_record(
    client: AsyncClient, world: World, appointment_id: uuid.UUID | str
) -> dict[str, Any]:
    resp = await client.get(
        "/api/billing/records",
        params={"appointment_id": str(appointment_id)},
        headers=auth(world.front_desk),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def billing_status(
    client: AsyncClient, world: World, appointment_id: uuid.UUID | str
) -> str:
    return (await billing_record(client, world, appointment_id))["status"]


async def appointment_row(engine: AsyncEngine, appointment_id: uuid.UUID | str) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, failure_reason, version, workflow_started_at "
                    "FROM appointments WHERE id = :a"
                ),
                {"a": uuid.UUID(str(appointment_id))},
            )
        ).one()


async def slot_row(engine: AsyncEngine, slot_id: uuid.UUID | str) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text("SELECT status, held_by_appointment_id FROM provider_slots WHERE id = :s"),
                {"s": uuid.UUID(str(slot_id))},
            )
        ).one()


async def history(engine: AsyncEngine, appointment_id: uuid.UUID | str) -> list[tuple]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT from_status, to_status, reason, actor FROM appointment_status_history "
                "WHERE appointment_id = :a "
                # Rows written in one transaction share created_at: a transition
                # (from != to) precedes a note (from == to) written after it.
                "ORDER BY created_at, (from_status IS NOT DISTINCT FROM to_status), id"
            ),
            {"a": uuid.UUID(str(appointment_id))},
        )
        return [tuple(r) for r in rows]


async def audit_count(mongo: AsyncMongoClient, dedupe_key: str) -> int:
    return await mongo[shared_settings.mongo_db_name][AUDIT_COLLECTION_NAME].count_documents(
        {"dedupe_key": dedupe_key}
    )


def reconciler(
    client: Any,
    http: httpx.AsyncClient,
    mongo: AsyncMongoClient,
    **overrides: Any,
) -> ReconcilerActivities:
    """`ReconcilerActivities` wired like the worker's, over `http` (the mesh or a
    degraded one), with test thresholds of 0 unless overridden."""
    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    options: dict[str, Any] = {
        "pending_after_minutes": 0,
        "matched_after_minutes": 0,
        "no_show_grace_minutes": 0,
        "batch_limit": BIG_LIMIT,
    }
    options.update(overrides)
    return ReconcilerActivities(
        get_session_factory(settings.database_url), client, mongo, http, tokens, **options
    )


async def run_pending(acts: ReconcilerActivities) -> ReconcilePendingResult:
    return await ActivityEnvironment().run(acts.reconcile_stuck_pending)


async def book_attempt(client: AsyncClient, world: World, slot_id: uuid.UUID, patient_id: str):
    return await client.post(
        "/api/appointments",
        json={"slot_id": str(slot_id), "patient_id": patient_id},
        headers=auth(world.front_desk) | {"Idempotency-Key": f"rec-{uuid.uuid4().hex}"},
    )


async def advance_result(env: WorkflowEnvironment, workflow_id: str) -> WaitlistAdvanceResult:
    handle = env.client.get_workflow_handle(workflow_id, result_type=WaitlistAdvanceResult)
    async with env.time_skipping_unlocked():
        return await handle.result()


# --- Task 1 tracer ---------------------------------------------------------------------


async def test_tracer_dead_workflow_appointment_is_failed_and_slot_reoffered(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """A pending appointment whose booking workflow no longer exists blocks its
    slot (a second booking is refused). One reconcile pass fails it
    `workflow_lost`, voids its billing, frees the slot, and the waiting patient
    is booked on that slot by the advance the reconciler started."""
    world = booking_world
    engine = scope.engine
    waiting_patient, waiting_tokens = await add_patient(client, world)
    entry = await joined(client, waiting_tokens, {"provider_id": str(world.provider_id)})
    seeded = await seed_appointment(
        scope, provider_id=world.provider_id, patient_id=world.patient_id
    )
    await seed_billing(mesh, seeded)
    assert await billing_status(client, world, seeded.appointment_id) == "verified"
    # No booking-{id} workflow exists on the test server.
    with pytest.raises(RPCError) as missing:
        await temporal_env.client.get_workflow_handle(
            booking_workflow_id(str(seeded.appointment_id))
        ).describe()
    assert missing.value.status == RPCStatusCode.NOT_FOUND

    blocked = await book_attempt(client, world, seeded.slot_id, waiting_patient)
    assert blocked.status_code == 409, blocked.text

    result = await run_pending(reconciler(temporal_env.client, mesh, mongo_client))

    assert result.failed >= 1
    row = await appointment_row(engine, seeded.appointment_id)
    assert (row.status, row.failure_reason) == ("failed", "workflow_lost")
    assert await billing_status(client, world, seeded.appointment_id) == "voided"
    assert [(h[0], h[1], h[2], h[3]) for h in await history(engine, seeded.appointment_id)] == [
        (None, "pending", None, "test"),
        ("pending", "failed", "workflow_lost", "system:reconciler"),
        ("failed", "failed", "billing_void_checked", "system:reconciler"),
    ]
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:fail") == 1

    # The slot went back to the waitlist: the waiting patient is booked on it
    # by the advance under the id the booking workflow itself would have used.
    advance = await advance_result(
        temporal_env,
        waitlist_advance_workflow_id(booking_workflow_id(str(seeded.appointment_id))),
    )
    assert advance.outcome == "booked"
    after = await get_entry(client, world, entry["id"])
    assert after["status"] == "booked"
    booked = await client.get(
        f"/api/appointments/{after['booked_appointment_id']}", headers=auth(world.front_desk)
    )
    assert booked.json()["status"] == "confirmed"
    assert booked.json()["slot_id"] == str(seeded.slot_id)
    assert booked.json()["patient_id"] == waiting_patient
    assert (await slot_row(engine, seeded.slot_id)).status == "booked"


async def test_a_running_workflow_leaves_its_appointment_pending(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The liveness seam says `running`: nothing is touched (no void, no state
    change, no hold release, no audit)."""
    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    async def running(_workflow_id: str) -> str:
        return "running"

    monkeypatch.setattr(acts, "_workflow_status", running)

    await run_pending(acts)

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"
    slot = await slot_row(scope.engine, seeded.slot_id)
    assert (slot.status, slot.held_by_appointment_id) == ("held", seeded.appointment_id)
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:fail") == 0


async def test_a_really_running_booking_workflow_is_never_touched(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
) -> None:
    """Without the seam patched: an open `booking-{id}` on the (workerless) test
    server is RUNNING, so the appointment stays pending; once that workflow is
    terminated it is gone and the next pass fails the appointment."""
    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)
    wf_id = booking_workflow_id(str(seeded.appointment_id))
    await start_once(
        temporal_env.client,
        BOOKING_WORKFLOW,
        BookingInput(
            appointment_id=str(seeded.appointment_id),
            clinic_id=str(seeded.clinic_id),
            patient_id=str(seeded.patient_id),
            provider_id=str(seeded.provider_id),
            slot_id=str(seeded.slot_id),
        ),
        workflow_id=wf_id,
        task_queue=SCHEDULING_TASK_QUEUE,
    )
    handle = temporal_env.client.get_workflow_handle(wf_id)
    assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    await run_pending(acts)

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"

    await handle.terminate(reason="test: the workflow died")
    await run_pending(acts)

    row = await appointment_row(scope.engine, seeded.appointment_id)
    assert (row.status, row.failure_reason) == ("failed", "workflow_lost")
    assert await billing_status(client, scope.world, seeded.appointment_id) == "voided"


async def test_a_young_pending_appointment_is_left_alone(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """Younger than the threshold: untouched (positive control: threshold 0 fails it)."""
    seeded = await seed_appointment(scope)

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client, pending_after_minutes=5))
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "failed"


async def test_an_unreachable_temporal_skips_the_step_and_fails_nothing(
    mesh: httpx.AsyncClient,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
) -> None:
    """Never fail a booking because the control plane is unreachable: the
    activity raises (Temporal retries it later) and nothing changed."""

    class _DownTemporal:
        def get_workflow_handle(self, *_args: Any, **_kwargs: Any) -> "_DownTemporal":
            return self

        async def describe(self) -> None:
            raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")

    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)

    with pytest.raises(RPCError):
        await run_pending(reconciler(_DownTemporal(), mesh, mongo_client))

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"
    slot = await slot_row(scope.engine, seeded.slot_id)
    assert slot.status == "held"


async def test_a_never_started_workflow_is_left_to_the_starter(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """`workflow_started_at IS NULL` is the pending-booking starter's case: the
    reconciler never fails it."""
    seeded = await seed_appointment(scope, workflow_started=False)

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"


async def test_an_appointment_that_confirmed_meanwhile_is_not_touched(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The workflow finished (confirmed) between the selection and the
    liveness answer: the fresh re-read sees it, so billing is NOT voided and the
    appointment is not failed."""
    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    async def gone_after_confirming(_workflow_id: str) -> str:
        async with scope.engine.begin() as conn:
            await conn.execute(
                text("UPDATE appointments SET status = 'confirmed' WHERE id = :a"),
                {"a": seeded.appointment_id},
            )
            await conn.execute(
                text("UPDATE provider_slots SET status = 'booked' WHERE id = :s"),
                {"s": seeded.slot_id},
            )
        return "gone"

    monkeypatch.setattr(acts, "_workflow_status", gone_after_confirming)

    await run_pending(acts)

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "confirmed"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"
    assert (await slot_row(scope.engine, seeded.slot_id)).status == "booked"


async def test_a_billing_outage_keeps_the_appointment_pending_for_the_next_run(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    client: AsyncClient,
) -> None:
    """Void-then-fail: Billing unreachable -> the appointment stays pending and
    the slot held (so the pair cannot diverge); a later pass completes both."""
    from tests.conftest import degraded_mesh  # noqa: PLC0415

    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)

    async with degraded_mesh("billing") as degraded:
        result = await run_pending(reconciler(temporal_env.client, degraded, mongo_client))
    assert isinstance(result, ReconcilePendingResult)

    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "pending"
    assert (await slot_row(scope.engine, seeded.slot_id)).status == "held"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "failed"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "voided"


async def test_the_pass_is_idempotent_and_writes_one_history_and_audit(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    await run_pending(acts)
    first = await history(scope.engine, seeded.appointment_id)
    await run_pending(acts)

    assert await history(scope.engine, seeded.appointment_id) == first
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:fail") == 1
    row = await appointment_row(scope.engine, seeded.appointment_id)
    assert (row.status, row.version) == ("failed", 2)
    # No record exists for this appointment: Billing's 404 is a resolved case.
    assert [h[2] for h in first] == [None, "workflow_lost", "billing_void_checked"]



# --- Task 2: stuck waitlist entries ------------------------------------------------------


async def seed_matched_entry(
    scope: Scope, appointment_id: uuid.UUID | None, *, provider_id: uuid.UUID
) -> uuid.UUID:
    """A waitlist entry stuck `matched` to `appointment_id` (no FK: it can also
    name an appointment that does not exist)."""
    entry_id = uuid.uuid4()
    async with scope.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO waitlist_entries (id, clinic_id, provider_id, patient_id, status, "
                "matched_appointment_id, attempts, created_by) "
                "VALUES (:id, :c, :pr, gen_random_uuid(), 'matched', :a, 1, gen_random_uuid())"
            ),
            {"id": entry_id, "c": scope.world.clinic_id, "pr": provider_id, "a": appointment_id},
        )
    return entry_id


async def entry_row(engine: AsyncEngine, entry_id: uuid.UUID) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT status, failure_reason, matched_appointment_id, booked_appointment_id "
                    "FROM waitlist_entries WHERE id = :e"
                ),
                {"e": entry_id},
            )
        ).one()


async def run_waitlist(acts: ReconcilerActivities) -> int:
    return (await ActivityEnvironment().run(acts.reconcile_stuck_waitlist)).processed


@pytest.mark.parametrize("status", ["confirmed", "checked_in", "completed", "no_show", "cancelled"])
async def test_a_matched_entry_whose_appointment_was_booked_is_resolved_booked(
    status: str,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status=status)
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)

    assert await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client)) >= 1

    row = await entry_row(scope.engine, entry)
    assert (row.status, row.booked_appointment_id) == ("booked", seeded.appointment_id)
    assert await audit_count(mongo_client, f"reconciler:{entry}:resolve") == 1


@pytest.mark.parametrize(
    "reason", ["SlotUnavailable", "ConfirmLostHold", "hold_expired", "workflow_lost"]
)
async def test_a_matched_entry_whose_booking_lost_the_slot_returns_to_waiting(
    reason: str,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="failed", failure_reason=reason)
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)

    await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client))

    row = await entry_row(scope.engine, entry)
    assert (row.status, row.matched_appointment_id, row.failure_reason) == ("waiting", None, None)


async def test_a_matched_entry_whose_booking_failed_otherwise_is_failed_with_the_reason(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="failed", failure_reason="BillingRejected")
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)

    await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client))

    row = await entry_row(scope.engine, entry)
    assert (row.status, row.failure_reason) == ("failed", "BillingRejected")


async def test_a_matched_entry_with_a_pending_appointment_is_untouched(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="pending")
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)

    await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client))

    row = await entry_row(scope.engine, entry)
    assert (row.status, row.matched_appointment_id) == ("matched", seeded.appointment_id)
    assert await audit_count(mongo_client, f"reconciler:{entry}:resolve") == 0


async def test_a_matched_entry_with_a_missing_appointment_returns_to_waiting(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    entry = await seed_matched_entry(scope, uuid.uuid4(), provider_id=scope.new_provider())

    await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client))

    row = await entry_row(scope.engine, entry)
    assert (row.status, row.matched_appointment_id) == ("waiting", None)


async def test_a_young_matched_entry_is_left_alone_and_a_repeat_changes_nothing(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="confirmed")
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)

    await run_waitlist(reconciler(temporal_env.client, mesh, mongo_client, matched_after_minutes=5))
    assert (await entry_row(scope.engine, entry)).status == "matched"

    acts = reconciler(temporal_env.client, mesh, mongo_client)
    await run_waitlist(acts)
    await run_waitlist(acts)
    assert (await entry_row(scope.engine, entry)).status == "booked"
    assert await audit_count(mongo_client, f"reconciler:{entry}:resolve") == 1


async def test_a_pending_appointment_failed_by_the_reconciler_frees_its_matched_entry_next_pass(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """The two sweeps compose: the entry waits while its booking is pending,
    and once the stuck pending sweep failed it `workflow_lost` the entry goes
    back to waiting."""
    seeded = await seed_appointment(scope, status="pending")
    entry = await seed_matched_entry(scope, seeded.appointment_id, provider_id=seeded.provider_id)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    await run_waitlist(acts)
    assert (await entry_row(scope.engine, entry)).status == "matched"
    await run_pending(acts)
    await run_waitlist(acts)

    assert (await entry_row(scope.engine, entry)).status == "waiting"


# --- Task 2: the billing orphan lookback ---------------------------------------------------


async def marker_rows(engine: AsyncEngine, appointment_id: uuid.UUID) -> int:
    async with engine.connect() as conn:
        return await conn.scalar(
            text(
                "SELECT count(*) FROM appointment_status_history "
                "WHERE appointment_id = :a AND reason = 'billing_void_checked'"
            ),
            {"a": appointment_id},
        )


@pytest.mark.parametrize("reason", ["hold_expired", "workflow_lost"])
async def test_the_lookback_voids_an_orphaned_billing_record_exactly_once(
    reason: str,
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="failed", failure_reason=reason)
    await seed_billing(mesh, seeded)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    first = await run_pending(acts)
    assert first.billing_voids >= 1
    assert await billing_status(client, scope.world, seeded.appointment_id) == "voided"
    assert await marker_rows(scope.engine, seeded.appointment_id) == 1
    record = await billing_record(client, scope.world, seeded.appointment_id)
    assert [h["to_status"] for h in record["history"]].count("voided") == 1

    await run_pending(acts)

    assert await marker_rows(scope.engine, seeded.appointment_id) == 1
    record = await billing_record(client, scope.world, seeded.appointment_id)
    assert [h["to_status"] for h in record["history"]].count("voided") == 1
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:void") == 1


async def test_the_lookback_covers_appointments_failed_by_the_real_reaper(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """The reaper fails the appointment and frees the slot but never reaches
    Billing; the lookback closes the orphan."""
    from services.scheduling.activities.maintenance import MaintenanceActivities  # noqa: PLC0415

    seeded = await seed_appointment(scope)
    await seed_billing(mesh, seeded)
    async with scope.engine.begin() as conn:
        await conn.execute(
            text("UPDATE provider_slots SET held_until = now() - interval '1 second' WHERE id = :s"),
            {"s": seeded.slot_id},
        )
    reaper = MaintenanceActivities(get_session_factory(settings.database_url), None, mongo_client)
    await ActivityEnvironment().run(reaper.expire_holds)
    row = await appointment_row(scope.engine, seeded.appointment_id)
    assert (row.status, row.failure_reason) == ("failed", "hold_expired")
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))

    assert await billing_status(client, scope.world, seeded.appointment_id) == "voided"
    assert await marker_rows(scope.engine, seeded.appointment_id) == 1


async def test_the_lookback_ignores_other_failure_reasons_and_rows_outside_the_window(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    other = await seed_appointment(scope, status="failed", failure_reason="BillingRejected")
    await seed_billing(mesh, other)
    inside = await seed_appointment(scope, status="failed", failure_reason="hold_expired")
    await seed_billing(mesh, inside)

    # Window of about a third of a second: nothing seeded a moment ago is in it
    # (no SQL ageing: a live reconciler uses the real 24 hours).
    await asyncio.sleep(0.5)
    narrow = reconciler(temporal_env.client, mesh, mongo_client, billing_lookback_hours=0.0001)
    await run_pending(narrow)
    assert await billing_status(client, scope.world, inside.appointment_id) == "verified"
    assert await marker_rows(scope.engine, inside.appointment_id) == 0

    # Positive control with the real window: the hold_expired row is voided, the
    # BillingRejected one is not covered at all.
    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))
    assert await billing_status(client, scope.world, inside.appointment_id) == "voided"
    assert await billing_status(client, scope.world, other.appointment_id) == "verified"
    assert await marker_rows(scope.engine, other.appointment_id) == 0


async def test_the_lookback_leaves_no_marker_when_billing_is_unreachable(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    from tests.conftest import degraded_mesh  # noqa: PLC0415

    seeded = await seed_appointment(scope, status="failed", failure_reason="workflow_lost")
    await seed_billing(mesh, seeded)

    async with degraded_mesh("billing") as degraded:
        await run_pending(reconciler(temporal_env.client, degraded, mongo_client))
    assert await marker_rows(scope.engine, seeded.appointment_id) == 0
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))
    assert await marker_rows(scope.engine, seeded.appointment_id) == 1
    assert await billing_status(client, scope.world, seeded.appointment_id) == "voided"


async def test_the_lookback_marks_an_appointment_with_no_billing_record_as_checked(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """The saga never reached the billing step: Billing's 404 is a resolved
    case, so the marker is written (and nothing is voided or audited)."""
    seeded = await seed_appointment(scope, status="failed", failure_reason="hold_expired")

    await run_pending(reconciler(temporal_env.client, mesh, mongo_client))

    assert await marker_rows(scope.engine, seeded.appointment_id) == 1
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:void") == 0


# --- Task 2: the no-show sweep ---------------------------------------------------------------


def just_ended() -> datetime:
    """A 30-minute slot that ended a minute ago: past a grace of 0 and far
    inside the live reconciler's 30-minute grace."""
    return datetime.now(timezone.utc) - timedelta(minutes=31)


async def run_no_shows(acts: ReconcilerActivities) -> int:
    return (await ActivityEnvironment().run(acts.reconcile_no_shows)).processed


async def test_a_confirmed_appointment_past_the_grace_becomes_no_show_after_billing_no_refund(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="confirmed", start=just_ended())
    await seed_billing(mesh, seeded)

    marked = await run_no_shows(reconciler(temporal_env.client, mesh, mongo_client))

    assert marked >= 1
    row = await appointment_row(scope.engine, seeded.appointment_id)
    assert (row.status, row.version) == ("no_show", 2)
    record = await billing_record(client, scope.world, seeded.appointment_id)
    assert record["status"] == "no_refund"
    assert [h["to_status"] for h in record["history"]].count("no_refund") == 1
    # The slot stays reserved for the no-show.
    assert (await slot_row(scope.engine, seeded.slot_id)).status == "booked"
    assert [(h[0], h[1], h[3]) for h in await history(scope.engine, seeded.appointment_id)] == [
        (None, "confirmed", "test"),
        ("confirmed", "no_show", "system:reconciler"),
    ]
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:no_show") == 1


async def test_within_the_grace_checked_in_and_already_no_show_are_untouched(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    recent = await seed_appointment(scope, status="confirmed", start=just_ended())
    await seed_billing(mesh, recent)
    checked_in = await seed_appointment(scope, status="checked_in", start=just_ended())
    done = await seed_appointment(scope, status="no_show", start=just_ended())

    await run_no_shows(reconciler(temporal_env.client, mesh, mongo_client, no_show_grace_minutes=5))
    assert (await appointment_row(scope.engine, recent.appointment_id)).status == "confirmed"
    assert await billing_status(client, scope.world, recent.appointment_id) == "verified"

    await run_no_shows(reconciler(temporal_env.client, mesh, mongo_client))
    assert (await appointment_row(scope.engine, recent.appointment_id)).status == "no_show"
    assert (await appointment_row(scope.engine, checked_in.appointment_id)).status == "checked_in"
    assert (await appointment_row(scope.engine, done.appointment_id)).version == 1
    assert await audit_count(mongo_client, f"reconciler:{done.appointment_id}:no_show") == 0
    assert await audit_count(mongo_client, f"reconciler:{checked_in.appointment_id}:no_show") == 0


async def test_a_billing_outage_keeps_the_appointment_confirmed_and_the_next_run_completes_both(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    from tests.conftest import degraded_mesh  # noqa: PLC0415

    seeded = await seed_appointment(scope, status="confirmed", start=just_ended())
    await seed_billing(mesh, seeded)

    async with degraded_mesh("billing") as degraded:
        await run_no_shows(reconciler(temporal_env.client, degraded, mongo_client))
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "confirmed"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "verified"

    await run_no_shows(reconciler(temporal_env.client, mesh, mongo_client))
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "no_show"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "no_refund"


async def test_a_failed_state_change_after_billing_converges_on_the_next_run(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Billing went to no_refund and then the state change failed: the
    appointment stays confirmed, and the next run repeats both (Billing answers
    the repeat 200) and lands the state."""
    from services.scheduling.repository import appointment_repo  # noqa: PLC0415

    seeded = await seed_appointment(scope, status="confirmed", start=just_ended())
    await seed_billing(mesh, seeded)
    real = appointment_repo.mark_no_show
    calls = {"n": 0}

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if kwargs.get("appointment_id") == seeded.appointment_id and calls["n"] == 1:
            raise RuntimeError("database went away")
        return await real(*args, **kwargs)

    monkeypatch.setattr(appointment_repo, "mark_no_show", flaky)
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    await run_no_shows(acts)
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "confirmed"
    assert await billing_status(client, scope.world, seeded.appointment_id) == "no_refund"

    await run_no_shows(acts)
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "no_show"
    record = await billing_record(client, scope.world, seeded.appointment_id)
    assert [h["to_status"] for h in record["history"]].count("no_refund") == 1


async def test_a_billing_refusal_does_not_block_the_no_show(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    """The record is already settled (voided), so Billing answers 409 to
    no_refund: logged, and the appointment still becomes no_show. No record at
    all (404) behaves the same."""
    from tests.billing.conftest import service_token, transition  # noqa: PLC0415

    settled = await seed_appointment(scope, status="confirmed", start=just_ended())
    await seed_billing(mesh, settled)
    token = await service_token(mesh, "scheduling")
    assert (await transition(mesh, token, str(settled.appointment_id), "voided")).status_code == 200
    without_record = await seed_appointment(scope, status="confirmed", start=just_ended())

    await run_no_shows(reconciler(temporal_env.client, mesh, mongo_client))

    assert (await appointment_row(scope.engine, settled.appointment_id)).status == "no_show"
    assert await billing_status(client, scope.world, settled.appointment_id) == "voided"
    assert (await appointment_row(scope.engine, without_record.appointment_id)).status == "no_show"


async def test_the_no_show_sweep_is_idempotent(
    mesh: httpx.AsyncClient,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    seeded = await seed_appointment(scope, status="confirmed", start=just_ended())
    acts = reconciler(temporal_env.client, mesh, mongo_client)

    await run_no_shows(acts)
    await run_no_shows(acts)

    assert len(await history(scope.engine, seeded.appointment_id)) == 2
    assert await audit_count(mongo_client, f"reconciler:{seeded.appointment_id}:no_show") == 1
    assert (await appointment_row(scope.engine, seeded.appointment_id)).version == 2


# --- Task 2: the reaper offers the slot it frees to the waitlist ---------------------------------


async def expire_now(scope: Scope, slot_id: uuid.UUID) -> None:
    async with scope.engine.begin() as conn:
        await conn.execute(
            text("UPDATE provider_slots SET held_until = now() - interval '1 second' WHERE id = :s"),
            {"s": slot_id},
        )


async def test_the_reaper_starts_a_waitlist_advance_that_books_the_waiting_patient(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    booking_world: World,
    scheduling_worker: object,
    temporal_env: WorkflowEnvironment,
    mongo_client: AsyncMongoClient,
    scope: Scope,
) -> None:
    from services.scheduling.activities.maintenance import MaintenanceActivities  # noqa: PLC0415

    world = booking_world
    waiting_patient, waiting_tokens = await add_patient(client, world)
    entry = await joined(client, waiting_tokens, {"provider_id": str(world.provider_id)})
    seeded = await seed_appointment(
        scope, provider_id=world.provider_id, patient_id=world.patient_id
    )
    await expire_now(scope, seeded.slot_id)
    reaper = MaintenanceActivities(
        get_session_factory(settings.database_url), temporal_env.client, mongo_client
    )

    result = await ActivityEnvironment().run(reaper.expire_holds)

    assert result.processed >= 1
    row = await appointment_row(scope.engine, seeded.appointment_id)
    assert (row.status, row.failure_reason) == ("failed", "hold_expired")
    advance = await advance_result(
        temporal_env, waitlist_advance_workflow_id(f"reaper-{seeded.appointment_id}")
    )
    assert advance.outcome == "booked"
    after = await get_entry(client, world, entry["id"])
    assert after["status"] == "booked"
    booked = await client.get(
        f"/api/appointments/{after['booked_appointment_id']}", headers=auth(world.front_desk)
    )
    assert (booked.json()["status"], booked.json()["patient_id"]) == ("confirmed", waiting_patient)
    assert booked.json()["slot_id"] == str(seeded.slot_id)


async def test_the_reaper_without_a_temporal_client_starts_nothing_and_does_not_fail(
    mongo_client: AsyncMongoClient, scope: Scope
) -> None:
    from services.scheduling.activities.maintenance import MaintenanceActivities  # noqa: PLC0415

    seeded = await seed_appointment(scope)
    await expire_now(scope, seeded.slot_id)
    reaper = MaintenanceActivities(get_session_factory(settings.database_url), None, mongo_client)

    result = await ActivityEnvironment().run(reaper.expire_holds)

    assert result.processed >= 0
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "failed"


async def test_a_failing_waitlist_start_is_logged_and_never_fails_the_reaper(
    mongo_client: AsyncMongoClient,
    scope: Scope,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json  # noqa: PLC0415

    import structlog  # noqa: PLC0415

    from libs.observability.logging import configure_logging  # noqa: PLC0415
    from services.scheduling.activities import waitlist as waitlist_module  # noqa: PLC0415
    from services.scheduling.activities.maintenance import MaintenanceActivities  # noqa: PLC0415

    configure_logging()
    monkeypatch.setattr(waitlist_module, "log", structlog.get_logger("test-reaper-offer"))

    class _DownTemporal:
        async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
            raise RPCError("temporal unavailable", RPCStatusCode.UNAVAILABLE, b"")

    seeded = await seed_appointment(scope)
    await expire_now(scope, seeded.slot_id)
    capsys.readouterr()
    reaper = MaintenanceActivities(
        get_session_factory(settings.database_url), _DownTemporal(), mongo_client
    )

    result = await ActivityEnvironment().run(reaper.expire_holds)

    assert result.processed >= 0
    assert (await appointment_row(scope.engine, seeded.appointment_id)).status == "failed"
    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    mine = [
        e
        for e in events
        if e.get("event") == "reaper.waitlist_offer_failed" and e.get("slot_id") == str(seeded.slot_id)
    ]
    assert len(mine) == 1
    assert mine[0]["error"] == "RPCError"
    assert set(mine[0]) <= {"event", "level", "timestamp", "slot_id", "source", "error", "logger"}
