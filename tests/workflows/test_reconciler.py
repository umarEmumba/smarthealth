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
                "WHERE appointment_id = :a ORDER BY created_at, id"
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

