"""The visit-downstream consumer's handler (plan 03-09; EVENT-03, D-03, D-08 refined,
T-3-27, T-3-28).

`handle_visit_completed` turns one decoded `visit.completed` envelope into the existing
`VisitCompletedWorkflow` start plus the conditional `downstream_started_at` marker. The
harness runs no Kafka consumer, so these tests decode the visit's real outbox rows with
`deliver_visit_events` and inject the test Temporal client (or a failing double).

* Idempotency: the same envelope handled twice starts ONE execution (deterministic id
  `visit-completed-{visit_id}`, USE_EXISTING + REJECT_DUPLICATE) and sets the marker once.
* Poison: an event for an unknown visit, or for a visit that is not completed, raises
  `NonRetryableEventError` (the base dead-letters it).
* Outage: an unavailable Temporal (or database) raises `TransientEventError` and leaves
  the marker NULL, so the base keeps the event and retries; a later delivery succeeds.

The live stack's own `visit-consumer` must not be running while these tests complete
harness visits (it would race them for the marker); the plan 03-09 SUMMARY records how
the suite was run.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import httpx
import pytest
import sqlalchemy.exc
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment

from libs.events.catalog import DLQ_TOPICS, EventType, topic_for
from libs.events.consumer import EventEnvelope, NonRetryableEventError, TransientEventError
from services.visit import downstream as downstream_module
from services.visit.config import settings as visit_settings
from services.visit.consumer import GROUP_ID, build_consumer, handle_visit_completed
from services.visit.domain.contracts import VisitCompletedResult, visit_completed_workflow_id
from tests.scheduling.seed import auth
from tests.visit.conftest import (
    VisitWorld,
    _DownTemporal,
    check_in,
    confirmed_appointment,
    deliver_visit_events,
    outbox_rows,
)

pytestmark = pytest.mark.integration


async def _complete(
    client: AsyncClient, mesh: httpx.AsyncClient, vw: VisitWorld, *, with_billing: bool = True
) -> tuple[str, str]:
    """(visit id, appointment id) of a visit checked in, started and completed."""
    appointment_id = await confirmed_appointment(vw, with_billing=with_billing, mesh=mesh)
    resp = await check_in(client, vw.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit_id = resp.json()["id"]
    for step in ("start", "complete"):
        moved = await client.post(f"/api/visits/{visit_id}/{step}", headers=auth(vw.world.provider))
        assert moved.status_code == 200, moved.text
    return visit_id, appointment_id


async def _marker(visit_id: str) -> Any:
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()


def _envelope(visit_id: str) -> EventEnvelope:
    """A hand-built `visit.completed` envelope (for events with no outbox row)."""
    now = datetime.datetime.now(datetime.UTC)
    return EventEnvelope(
        event_id=uuid.uuid4(),
        event_type=EventType.VISIT_COMPLETED,
        aggregate_id=visit_id,
        occurred_at=now,
        schema_version=1,
        payload={"visit_id": visit_id},
        topic=topic_for(EventType.VISIT_COMPLETED),
        partition=0,
        offset=0,
    )


class _RecordingStartOnce:
    """Wraps `libs.temporal.start_once` as imported by the downstream module and
    records each return value (True: started or attached; False: already closed)."""

    def __init__(self, real: Any) -> None:
        self._real = real
        self.results: list[bool] = []

    async def __call__(self, *args: Any, **kwargs: Any) -> bool:
        result = await self._real(*args, **kwargs)
        self.results.append(result)
        return result


async def test_handler_twice_starts_one_execution_and_sets_the_marker_once(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    visit_worker: object,
    temporal_env: WorkflowEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _RecordingStartOnce(downstream_module.start_once)
    monkeypatch.setattr(downstream_module, "start_once", recorder)
    visit_id, appointment_id = await _complete(client, mesh, visit_world)
    assert await _marker(visit_id) is None  # the API starts nothing

    assert await deliver_visit_events(None, visit_id, client=temporal_env.client) == 1
    marker = await _marker(visit_id)
    assert marker is not None
    handle = temporal_env.client.get_workflow_handle(
        visit_completed_workflow_id(visit_id), result_type=VisitCompletedResult
    )
    async with temporal_env.time_skipping_unlocked():
        result = await handle.result()
    assert result.billing == "finalized"
    first_run = (await handle.describe()).run_id

    # The same event again (a redelivery): the closed id is refused, nothing changes.
    assert await deliver_visit_events(None, visit_id, client=temporal_env.client) == 1

    assert recorder.results == [True, False]
    assert await _marker(visit_id) == marker
    assert (await handle.describe()).run_id == first_run
    assert len(await outbox_rows(visit_id)) == 1


async def test_a_redelivery_while_the_workflow_runs_attaches_to_the_same_run(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """No worker polls visit-tq, so the execution is still RUNNING at the second
    delivery: USE_EXISTING attaches to it instead of starting another."""
    visit_id, _ = await _complete(client, mesh, visit_world)

    await deliver_visit_events(None, visit_id, client=temporal_env.client)
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    first_run = (await handle.describe()).run_id
    await deliver_visit_events(None, visit_id, client=temporal_env.client)

    assert (await handle.describe()).run_id == first_run
    assert await _marker(visit_id) is not None
    await handle.terminate(reason="test clean-up")


async def test_unknown_visit_is_dead_lettered_as_non_retryable(
    temporal_env: WorkflowEnvironment,
) -> None:
    with pytest.raises(NonRetryableEventError, match="VisitNotFound"):
        await handle_visit_completed(_envelope(str(uuid.uuid4())), client=temporal_env.client)


async def test_a_visit_that_is_not_completed_is_non_retryable(
    client: AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """A forged or replayed event naming a real but not-completed visit starts nothing."""
    appointment_id = await confirmed_appointment(visit_world)
    resp = await check_in(client, visit_world.world.front_desk, appointment_id)
    assert resp.status_code == 201, resp.text
    visit_id = resp.json()["id"]

    with pytest.raises(NonRetryableEventError, match="VisitNotCompleted"):
        await handle_visit_completed(_envelope(visit_id), client=temporal_env.client)

    assert await _marker(visit_id) is None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    with pytest.raises(RPCError) as exc_info:
        await handle.describe()
    assert exc_info.value.status == RPCStatusCode.NOT_FOUND


async def test_a_malformed_visit_id_is_non_retryable(temporal_env: WorkflowEnvironment) -> None:
    envelope = _envelope("not-a-uuid")
    with pytest.raises(NonRetryableEventError, match="InvalidVisitId"):
        await handle_visit_completed(envelope, client=temporal_env.client)


async def test_a_temporal_outage_is_transient_and_leaves_the_marker_null(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
) -> None:
    visit_id, _ = await _complete(client, mesh, visit_world)

    with pytest.raises(TransientEventError):
        await deliver_visit_events(None, visit_id, client=_DownTemporal())

    assert await _marker(visit_id) is None


async def test_a_lost_connection_runtime_error_is_transient(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
) -> None:
    class _LostConnection:
        async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("Client is closed")

    visit_id, _ = await _complete(client, mesh, visit_world)

    with pytest.raises(TransientEventError):
        await deliver_visit_events(None, visit_id, client=_LostConnection())

    assert await _marker(visit_id) is None


async def test_the_event_succeeds_once_temporal_is_back(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
    temporal_env: WorkflowEnvironment,
) -> None:
    """The outage delays the trigger; the retried event then does the work, once."""
    visit_id, _ = await _complete(client, mesh, visit_world)
    with pytest.raises(TransientEventError):
        await deliver_visit_events(None, visit_id, client=_DownTemporal())
    assert await _marker(visit_id) is None

    await deliver_visit_events(None, visit_id, client=temporal_env.client)

    assert await _marker(visit_id) is not None
    handle = temporal_env.client.get_workflow_handle(visit_completed_workflow_id(visit_id))
    assert (await handle.describe()).run_id
    await handle.terminate(reason="test clean-up")


async def test_a_request_error_is_not_classified_as_an_outage(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    visit_world: VisitWorld,
) -> None:
    """INVALID_ARGUMENT can never succeed by waiting: it surfaces unchanged, so the
    base's bounded retries and the DLQ apply instead of an endless transient loop."""

    class _Rejecting:
        async def start_workflow(self, *args: Any, **kwargs: Any) -> None:
            raise RPCError("bad request", RPCStatusCode.INVALID_ARGUMENT, b"")

    visit_id, _ = await _complete(client, mesh, visit_world)

    with pytest.raises(RPCError):
        await deliver_visit_events(None, visit_id, client=_Rejecting())

    assert await _marker(visit_id) is None


async def test_a_database_outage_is_transient(temporal_env: WorkflowEnvironment) -> None:
    class _BrokenSession:
        async def __aenter__(self) -> Any:
            raise sqlalchemy.exc.OperationalError("SELECT 1", {}, ConnectionRefusedError("down"))

        async def __aexit__(self, *exc: object) -> None:
            return None

    with pytest.raises(TransientEventError):
        await handle_visit_completed(
            _envelope(str(uuid.uuid4())),
            client=temporal_env.client,
            session_factory=lambda: _BrokenSession(),  # type: ignore[arg-type,return-value]
        )


def test_build_consumer_is_wired_for_the_visit_downstream_group() -> None:
    consumer = build_consumer(metrics_port=None, heartbeat_path=None)
    try:
        assert GROUP_ID == consumer.group_id == "visit-downstream"
        assert consumer.topics == ["smarthealth.visit.events"]
        assert consumer.dlq_topic == DLQ_TOPICS["visit-downstream"] == "smarthealth.dlq.visit-downstream"
        assert set(consumer._handlers) == {"visit.completed"}  # noqa: SLF001
    finally:
        consumer.close()
