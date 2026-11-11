"""Visit completion -> outbox -> Debezium -> Kafka -> visit-downstream consumer ->
Temporal, on the deployed stack (plan 03-09; EVENT-01, EVENT-02, EVENT-03, ANLY-03,
ANLY-05; D-03).

A visit is checked in, started and completed through the gateway. The completion commits
a `visit.completed` outbox row in visit_db; the `visit-outbox` connector relays it to
`smarthealth.visit.events` keyed by the visit id; the `visit-consumer` container starts
`visit-completed-{visit_id}` and sets `downstream_started_at`. Both effects are asserted
within 15 s of the completion, which is shorter than the still-running sweeper's 30 s
grace (D-03: the sweeper is retired in plan 03-12), so they prove the consumer path.

Plan 03-11 adds the kill-between-commit-and-publish and outage proofs (the D-03 gate
before the sweeper is retired in plan 03-12) below the "Plan 03-11" marker. They kill or
stop `visit`, `visit-consumer` and `connect`, restore each in `finally`, wait for health
(Connect: every connector task RUNNING) and need a quiet stack. Each Kafka read uses a
fresh consumer group.
Run with the stack up:
    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_visit_downstream.py -m e2e
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import Client

from libs.events.catalog import topic_for
from libs.events.outbox import event_id_for
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.config import settings as visit_settings
from services.visit.domain.contracts import visit_completed_workflow_id
from tests.e2e import phase3_stack as stack
from tests.e2e.phase2_stack import (
    E2EWorld,
    appointment_in,
    billing_in,
    book,
    book_slot_inside_checkin_window,
    gateway_url,
    require_stack,
    seed_phase2_world,
)

pytestmark = pytest.mark.e2e

VISIT_TOPIC = topic_for("visit.completed")
DOWNSTREAM_DEADLINE_SECONDS = 15.0
TEMPORAL_LOCAL_ADDRESS = "localhost:7233"


@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()
    stack.wait_connectors_running(["visit-outbox"])


@pytest_asyncio.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=gateway_url(), timeout=20.0) as http:
        await require_stack(http)
        yield http


@pytest_asyncio.fixture
async def ew(gateway: httpx.AsyncClient) -> E2EWorld:
    return await seed_phase2_world(gateway, patients=1)


@pytest_asyncio.fixture
async def temporal() -> Client:
    return await Client.connect(TEMPORAL_LOCAL_ADDRESS)


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)


async def _downstream_marker(visit_id: str) -> datetime.datetime | None:
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT downstream_started_at FROM visits WHERE id = :v"), {"v": visit_id}
            )
    finally:
        await engine.dispose()


async def _complete_visit_through_gateway(
    gateway: httpx.AsyncClient, ew: E2EWorld
) -> tuple[str, str, float]:
    """(visit id, appointment id, monotonic time just after the completion response)."""
    slot_id, _ = await book_slot_inside_checkin_window(gateway, ew)
    appointment_id = await book(gateway, ew, slot_id=slot_id, patient_id=ew.patient_ids[0])
    assert (await appointment_in(gateway, ew, appointment_id, {"confirmed", "failed"}))[
        "status"
    ] == "confirmed"

    checked_in = await gateway.post(
        "/api/visits", json={"appointment_id": appointment_id}, headers=ew.front_desk
    )
    assert checked_in.status_code == 201, checked_in.text
    visit_id = checked_in.json()["id"]
    started = await gateway.post(f"/api/visits/{visit_id}/start", headers=ew.front_desk)
    assert started.status_code == 200, started.text
    completed = await gateway.post(f"/api/visits/{visit_id}/complete", headers=ew.front_desk)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "completed"
    return visit_id, appointment_id, time.monotonic()


async def _wait_for(check: Any, deadline: float) -> Any:
    """Await `check()` until truthy or the monotonic `deadline`; the last value."""
    value = await check()
    while not value and time.monotonic() < deadline:
        await asyncio.sleep(0.25)
        value = await check()
    return value


# --- Task 1: tracer ------------------------------------------------------------------


async def test_tracer_completed_visit_starts_downstream_through_the_outbox_and_consumer(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    visit_id, appointment_id, completed_at_mono = await _complete_visit_through_gateway(gateway, ew)
    deadline = completed_at_mono + DOWNSTREAM_DEADLINE_SECONDS
    workflow_id = visit_completed_workflow_id(visit_id)

    # 1. the consumer set the marker ...
    marker = await _wait_for(lambda: _downstream_marker(visit_id), deadline)
    assert marker is not None, (
        f"downstream_started_at still NULL {DOWNSTREAM_DEADLINE_SECONDS:.0f} s after the "
        "completion: the visit-downstream consumer did not act on visit.completed"
    )

    # 2. ... and a Temporal execution with the deterministic id exists, on the visit queue.
    async def _describe() -> Any:
        try:
            return await temporal.get_workflow_handle(workflow_id).describe()
        except Exception:  # noqa: BLE001 - not found yet
            return None

    description = await _wait_for(_describe, deadline)
    assert description is not None, f"no Temporal execution {workflow_id} within the deadline"
    assert description.task_queue == VISIT_TASK_QUEUE
    assert time.monotonic() <= deadline + 1.0

    # 3. exactly one visit.completed message, keyed by the visit id, with the timestamps.
    found = await asyncio.to_thread(
        stack.consume_matching,
        VISIT_TOPIC,
        lambda m: m["key"] == visit_id and m["headers"].get("eventType") == "visit.completed",
        60,
        1,
    )
    assert found, f"no visit.completed message for {visit_id} within 60 s"
    # Debezium is at-least-once: count distinct event ids, not messages.
    assert {m["headers"]["id"] for m in found} == {
        str(event_id_for(visit_id, "visit.completed", "completed"))
    }
    value = found[0]["value"]
    assert found[0]["key"] == visit_id
    assert str(value["visit_id"]) == visit_id
    assert str(value["appointment_id"]) == appointment_id
    assert str(value["clinic_id"]) == ew.clinic_id
    for name in ("checked_in_at", "started_at", "completed_at"):
        assert isinstance(value[name], datetime.datetime), name
    assert (
        _aware(value["checked_in_at"])
        <= _aware(value["started_at"])
        <= _aware(value["completed_at"])
    )

    # The downstream fan-out the consumer started still finishes the visit's work.
    await billing_in(gateway, ew, appointment_id, "finalized")
    assert (await appointment_in(gateway, ew, appointment_id, {"completed"}))["status"] == "completed"


# --- Plan 03-11: the D-03 kill-between-commit-and-publish gate ------------------------
#
# Three scenarios, each in a fresh clinic. The completion is committed together with its
# `visit.completed` outbox row; the downstream trigger must survive a kill of the committing
# API, a consumer outage and a Connect outage with exactly one `visit-completed-{id}`
# execution and the marker set. The still-running sweeper (30 s grace, interval 60 s) is a
# second path to the same end state, so each test also reads the visit-consumer's own log
# line `visit.downstream_started` (outcome=started) for the visit id: that proves the
# OUTBOX path started the workflow, not the sweeper.

OUTAGE_SECONDS = 15
SETTLE_SECONDS = 5


@contextmanager
def _restored(service: str, *, connect: bool = False) -> Iterator[None]:
    """Whatever the body does to `service` (kill, stop), start it again afterwards and wait
    until it is healthy; for Connect also until every connector task is RUNNING."""
    try:
        yield
    finally:
        stack.start_service(service)
        stack.wait_service_healthy(service)
        if connect:
            stack.wait_connectors_running()


async def _executions(temporal: Client, visit_id: str) -> int:
    """Number of Temporal executions (any status) with the visit's deterministic id."""
    query = f"WorkflowId = '{visit_completed_workflow_id(visit_id)}'"
    return len([wf async for wf in temporal.list_workflows(query)])


async def _wait_one_execution(temporal: Client, visit_id: str, *, timeout: float = 60) -> None:
    """Wait until visibility shows the execution, let it settle, then require exactly one."""
    deadline = time.monotonic() + timeout
    count = await _executions(temporal, visit_id)
    while count == 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        count = await _executions(temporal, visit_id)
    await asyncio.sleep(SETTLE_SECONDS)
    count = await _executions(temporal, visit_id)
    assert count == 1, f"expected exactly one execution of the visit's downstream workflow, saw {count}"


def _consumer_started_it(visit_id: str, since: datetime.datetime) -> bool:
    """True when the visit-consumer logged `visit.downstream_started` with outcome=started
    for this visit id since `since` (the outbox path, not the sweeper, started it)."""
    out = stack.compose(
        "logs", "--no-color", "--since", since.strftime("%Y-%m-%dT%H:%M:%SZ"), "visit-consumer"
    ).stdout
    for line in out.splitlines():
        _, _, payload = line.partition("| ")
        try:
            record = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if (
            record.get("event") == "visit.downstream_started"
            and record.get("visit_id") == visit_id
            and record.get("outcome") == "started"
        ):
            return True
    return False


async def _downstream_finishes(gateway: httpx.AsyncClient, ew: E2EWorld, appointment_id: str) -> None:
    await billing_in(gateway, ew, appointment_id, "finalized")
    assert (await appointment_in(gateway, ew, appointment_id, {"completed"}))["status"] == "completed"


async def _start_window() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=5)


async def test_visit_trigger_survives_api_kill_after_commit(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    """SIGKILL the Visit API the moment the completion response arrives: the committed
    outbox row still reaches the consumer, which starts one execution and sets the marker
    within 15 s of the completion (inside the sweeper's 30 s grace)."""
    since = await _start_window()
    with _restored("visit"):
        visit_id, appointment_id, completed_at_mono = await _complete_visit_through_gateway(gateway, ew)
        await asyncio.to_thread(stack.kill_service, "visit")

        marker = await _wait_for(
            lambda: _downstream_marker(visit_id), completed_at_mono + DOWNSTREAM_DEADLINE_SECONDS
        )
        assert marker is not None, "marker still NULL 15 s after the API was killed after the commit"
        assert time.monotonic() <= completed_at_mono + DOWNSTREAM_DEADLINE_SECONDS + 1.0
        await _wait_one_execution(temporal, visit_id)
        assert _consumer_started_it(visit_id, since), "the outbox consumer did not start the workflow"

    await _downstream_finishes(gateway, ew, appointment_id)
    fetched = await gateway.get(f"/api/visits/{visit_id}", headers=ew.front_desk)
    assert fetched.status_code == 200 and fetched.json()["status"] == "completed", fetched.text


async def test_visit_trigger_catches_up_after_consumer_outage(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    """With the consumer stopped the completion leaves no workflow and a NULL marker for
    15 s; once it is started the unread event is consumed from the committed offset and
    the workflow starts once, the marker set."""
    since = await _start_window()
    with _restored("visit-consumer"):
        await asyncio.to_thread(stack.stop_service, "visit-consumer")
        visit_id, appointment_id, completed_at_mono = await _complete_visit_through_gateway(gateway, ew)

        await asyncio.sleep(max(0.0, completed_at_mono + OUTAGE_SECONDS - time.monotonic()))
        assert await _downstream_marker(visit_id) is None, "marker set while the consumer was stopped"
        assert await _executions(temporal, visit_id) == 0, "a workflow started while the consumer was stopped"

        await asyncio.to_thread(stack.start_service, "visit-consumer")
        await asyncio.to_thread(stack.wait_service_healthy, "visit-consumer")
        marker = await _wait_for(lambda: _downstream_marker(visit_id), time.monotonic() + 60)
        assert marker is not None, "marker still NULL after the consumer was started again"
        await _wait_one_execution(temporal, visit_id)
        assert _consumer_started_it(visit_id, since), "the outbox consumer did not start the workflow"

    await _downstream_finishes(gateway, ew, appointment_id)


async def test_visit_trigger_survives_connect_outage(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    """With Connect stopped the completion commits but nothing is relayed: no workflow and
    a NULL marker for 15 s. Once Connect is started and every connector task is RUNNING,
    the event is relayed and downstream starts once."""
    since = await _start_window()
    with _restored(stack.CONNECT_SERVICE, connect=True):
        await asyncio.to_thread(stack.stop_service, stack.CONNECT_SERVICE)
        visit_id, appointment_id, completed_at_mono = await _complete_visit_through_gateway(gateway, ew)

        await asyncio.sleep(max(0.0, completed_at_mono + OUTAGE_SECONDS - time.monotonic()))
        assert await _downstream_marker(visit_id) is None, "marker set while Connect was stopped"
        assert await _executions(temporal, visit_id) == 0, "a workflow started while Connect was stopped"

        await asyncio.to_thread(stack.start_service, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_service_healthy, stack.CONNECT_SERVICE)
        await asyncio.to_thread(stack.wait_connectors_running)
        marker = await _wait_for(lambda: _downstream_marker(visit_id), time.monotonic() + 90)
        assert marker is not None, "marker still NULL after Connect was started again"
        await _wait_one_execution(temporal, visit_id)
        assert _consumer_started_it(visit_id, since), "the outbox consumer did not start the workflow"

    await _downstream_finishes(gateway, ew, appointment_id)


# --- Plan 03-12: the visit downstream sweeper is retired (D-03) -----------------------------
# Read-only: the test never deletes, pauses or creates anything. It describes the Schedules
# the rebuilt visit worker leaves on the live Temporal server.

RETIRED_SCHEDULE = "visit-downstream-sweeper"
SAFETY_NET_SCHEDULES = (
    "visit-reconciler",
    "scheduling-hold-reaper",
    "scheduling-pending-starter",
    "scheduling-reconciler",
)
PAUSED_POLLER = "scheduling-schedule-change-poller"


async def _schedule_paused(temporal: Client, schedule_id: str) -> bool | None:
    """True/False for an existing Schedule's paused flag, None when it does not exist."""
    from temporalio.service import RPCError, RPCStatusCode  # noqa: PLC0415

    try:
        description = await temporal.get_schedule_handle(schedule_id).describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            return None
        raise
    return description.schedule.state.paused


async def test_sweeper_schedule_retired_on_live_server(temporal: Client) -> None:
    """D-03, T-3-38: the sweeper Schedule is gone, every other safety net is still
    registered and running, the user-paused poller is still paused, and no sweeper run is
    in flight (an in-flight run gets 60 s to finish)."""
    assert await _schedule_paused(temporal, RETIRED_SCHEDULE) is None, (
        "the visit-downstream-sweeper Schedule still exists on the live server"
    )
    for schedule_id in SAFETY_NET_SCHEDULES:
        assert await _schedule_paused(temporal, schedule_id) is False, (
            f"{schedule_id} must exist and not be paused"
        )
    assert await _schedule_paused(temporal, PAUSED_POLLER) is True, (
        "scheduling-schedule-change-poller must still exist and stay paused"
    )

    query = "WorkflowType = 'VisitDownstreamSweeperWorkflow' AND ExecutionStatus = 'Running'"
    deadline = time.monotonic() + 60
    running = [wf async for wf in temporal.list_workflows(query)]
    while running and time.monotonic() < deadline:
        await asyncio.sleep(2)
        running = [wf async for wf in temporal.list_workflows(query)]
    assert not running, "a VisitDownstreamSweeperWorkflow execution is still running"


# --- Plan 03-22: a lost visit.completed trigger is recovered by the visit-reconciler (CR-01) --
# The consumer is stopped, a visit is completed, and the `visit-downstream` group's offset on
# the event's partition is moved ONE past the event (what the consumer does after a
# dead-letter): the event is consumed and dropped, the marker stays NULL. After the visit is
# backdated beyond the reconcile grace, the live `visit-reconciler` Schedule must restart the
# downstream workflow: one `visit-completed-{id}` execution, the marker set, billing finalized,
# the appointment completed, and the visit-worker (not the consumer) logged the restart.

VISIT_DOWNSTREAM_GROUP = "visit-downstream"
RECONCILER_SCHEDULE = "visit-reconciler"
RECOVERY_DEADLINE_SECONDS = 120.0
LOST_TRIGGER_QUIET_SECONDS = 10


def _kafka_groups(*args: str, retry_seconds: float = 90) -> str:
    """`kafka-consumer-groups ...` inside the broker container, retried while the (just
    stopped) group is still being expired; returns stdout."""
    argv = [
        "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:29092", *args,
    ]  # fmt: skip
    deadline = time.monotonic() + retry_seconds
    last = ""
    while time.monotonic() < deadline:
        result = stack.compose(*argv, check=False, timeout=60)
        last = (result.stdout + result.stderr).strip()
        if result.returncode == 0 and "Error" not in last:
            return result.stdout
        time.sleep(3)
    raise AssertionError(f"kafka-consumer-groups {' '.join(args[:2])} failed: {last[-300:]}")


def _committed_offset(group: str, topic: str, partition: int) -> int | None:
    out = _kafka_groups("--describe", "--group", group)
    for line in out.splitlines():
        parts = line.split()
        if (
            len(parts) >= 4
            and parts[0] == group
            and parts[1] == topic
            and parts[2] == str(partition)
            and parts[3].isdigit()
        ):
            return int(parts[3])
    return None


def _service_log_lines(
    service: str, since: datetime.datetime, event: str, visit_id: str
) -> list[dict[str, Any]]:
    """The service's structured log records named `event` for `visit_id` since `since`."""
    out = stack.compose(
        "logs", "--no-color", "--since", since.strftime("%Y-%m-%dT%H:%M:%SZ"), service
    ).stdout
    found: list[dict[str, Any]] = []
    for line in out.splitlines():
        _, _, payload = line.partition("| ")
        try:
            record = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if record.get("event") == event and record.get("visit_id") == visit_id:
            found.append(record)
    return found


async def _age_visit(visit_id: str, minutes: int) -> None:
    """Move the test's OWN visit's three timestamps `minutes` into the past (one UPDATE)."""
    engine = create_async_engine(visit_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE visits SET checked_in_at = checked_in_at - make_interval(mins => :m),"
                    " started_at = started_at - make_interval(mins => :m),"
                    " completed_at = completed_at - make_interval(mins => :m) WHERE id = :v"
                ),
                {"m": minutes, "v": visit_id},
            )
    finally:
        await engine.dispose()


async def test_lost_visit_trigger_is_recovered_by_the_reconciler(
    gateway: httpx.AsyncClient, ew: E2EWorld, temporal: Client
) -> None:
    since = await _start_window()
    with _restored("visit-consumer"):
        await asyncio.to_thread(stack.stop_service, "visit-consumer")
        visit_id, appointment_id, _ = await _complete_visit_through_gateway(gateway, ew)

        found = await asyncio.to_thread(
            stack.consume_matching,
            VISIT_TOPIC,
            lambda m: m["key"] == visit_id and m["headers"].get("eventType") == "visit.completed",
            60,
            1,
            decode=False,
        )
        assert found, f"no visit.completed message for {visit_id} within 60 s"
        partition, offset = found[0]["partition"], found[0]["offset"]

        # Only move the group's offset when it sits exactly on this event: never skip others.
        committed = await asyncio.to_thread(_committed_offset, VISIT_DOWNSTREAM_GROUP, VISIT_TOPIC, partition)
        assert committed == offset, (
            f"{VISIT_DOWNSTREAM_GROUP} committed offset on {VISIT_TOPIC}:{partition} is "
            f"{committed}, expected {offset}: refusing to skip other events"
        )
        await asyncio.to_thread(
            _kafka_groups,
            "--group", VISIT_DOWNSTREAM_GROUP,
            "--topic", f"{VISIT_TOPIC}:{partition}",
            "--reset-offsets", "--to-offset", str(offset + 1), "--execute",
        )  # fmt: skip

        await asyncio.to_thread(stack.start_service, "visit-consumer")
        await asyncio.to_thread(stack.wait_service_healthy, "visit-consumer")

    # The event was consumed and dropped: nothing starts the visit.
    await asyncio.sleep(LOST_TRIGGER_QUIET_SECONDS)
    assert await _downstream_marker(visit_id) is None, "the consumer started a dropped trigger"
    assert await _executions(temporal, visit_id) == 0, "a workflow exists for a dropped trigger"

    # Recovery: past the reconcile grace, the live visit-reconciler Schedule restarts it.
    await _age_visit(visit_id, 30)
    await temporal.get_schedule_handle(RECONCILER_SCHEDULE).trigger()
    marker = await _wait_for(
        lambda: _downstream_marker(visit_id), time.monotonic() + RECOVERY_DEADLINE_SECONDS
    )
    assert marker is not None, (
        f"marker still NULL {RECOVERY_DEADLINE_SECONDS:.0f} s after the visit-reconciler "
        "Schedule was triggered: the lost trigger was not restarted"
    )
    await _wait_one_execution(temporal, visit_id)

    restarted = _service_log_lines(
        "visit-worker", since, "visit.lost_trigger_restarted", visit_id
    )
    assert restarted, "the visit-worker did not log visit.lost_trigger_restarted for the visit"
    assert not _service_log_lines("visit-consumer", since, "visit.downstream_started", visit_id), (
        "the consumer started the visit: it was not the reconciler"
    )
    await _downstream_finishes(gateway, ew, appointment_id)
