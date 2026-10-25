"""Shared Temporal test fixtures (plan 02-06), registered from the root
`tests/conftest.py` through `pytest_plugins` so that BOTH `tests/workflows/`
and `tests/scheduling/` (and later `tests/visit/`) can use them: pytest does
not share a sibling folder's conftest fixtures.

* `temporal_env`       - a fresh time-skipping Temporal test server per test
                         (`WorkflowEnvironment.start_time_skipping()`). Time
                         skips only while a test awaits a workflow result; the
                         database's `now()` never skips (research Pitfall 8).
* `scheduling_worker`  - a real `Worker` on `scheduling-tq` running every
                         Scheduling workflow (`services.scheduling.worker.
                         WORKFLOWS`) with `SchedulingActivities` (yielded),
                         `AppointmentChangeActivities` (plan 02-08),
                         `WaitlistActivities` (plan 02-09) and
                         `MaintenanceActivities` and `ScheduleChangeActivities`
                         (plan 02.1-08) wired to
                         the in-process mesh (HTTP + service-token minting), the
                         real scheduling_db and the test Mongo client. While it
                         runs, the Scheduling app's `state.temporal_client` is
                         the test server's client, so `POST /api/appointments`
                         starts sagas this worker executes.
* `schedule_changes`   - (plan 02.1-08) a `ScheduleChangeActivities` bound to the
                         test server for direct `poll_schedule_changes` /
                         `apply_schedule_change` calls.
* `booking_world`      - `seed_bookable_world` plus clean-up of the Scheduling
                         rows it (and the bookings made in it) created.

* `visit_worker`       - (plan 02-11) a real `Worker` on `visit-tq` running
                         `services.visit.worker.WORKFLOWS` with `VisitActivities`
                         (yielded) wired to the mesh, visit_db, the test Mongo
                         client and the test server's client (for the sweep).
                         While it runs, the Visit app's `state.temporal_client`
                         is the test server's client, so `POST /api/visits/{id}/
                         complete` starts the downstream workflow it executes.

`running_scheduling_worker(client, http, mongo)` is the same wiring as an async
context manager, for tests that need a differently built client (the trace
test wraps the test server's client with `TracingInterceptor`).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest_asyncio
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy.ext.asyncio import create_async_engine
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from tests.conftest import harness_apps


@pytest_asyncio.fixture
async def temporal_env() -> AsyncIterator[WorkflowEnvironment]:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()


def build_schedule_change_activities(
    client: Client, http: httpx.AsyncClient, mongo: AsyncMongoClient
):
    """A `ScheduleChangeActivities` wired like the worker's (plan 02.1-08): the
    real scheduling_db, the in-process mesh with real service tokens, the test
    Mongo client and `client` for starting workflows. Tests call its methods
    directly (an awaited call or `ActivityEnvironment`)."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415
    from libs.db.postgres import get_session_factory  # noqa: PLC0415
    from services.scheduling.activities.schedule_change import ScheduleChangeActivities  # noqa: PLC0415
    from services.scheduling.config import settings  # noqa: PLC0415

    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    return ScheduleChangeActivities(
        get_session_factory(settings.database_url), http, tokens, mongo, client
    )


@asynccontextmanager
async def running_scheduling_worker(
    client: Client, http: httpx.AsyncClient, mongo: AsyncMongoClient
) -> AsyncIterator[object]:
    """Run Scheduling's worker on `client` for the block (see module docstring)
    and point the Scheduling app's `state.temporal_client` at `client`."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415
    from libs.db.postgres import get_session_factory  # noqa: PLC0415
    from libs.temporal_policies import SCHEDULING_TASK_QUEUE  # noqa: PLC0415
    from services.scheduling.activities.booking import SchedulingActivities  # noqa: PLC0415
    from services.scheduling.activities.changes import AppointmentChangeActivities  # noqa: PLC0415
    from services.scheduling.activities.maintenance import MaintenanceActivities  # noqa: PLC0415
    from services.scheduling.activities.schedule_change import ScheduleChangeActivities  # noqa: PLC0415
    from services.scheduling.activities.waitlist import WaitlistActivities  # noqa: PLC0415
    from services.scheduling.config import settings  # noqa: PLC0415
    from services.scheduling.worker import WORKFLOWS  # noqa: PLC0415

    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    session_factory = get_session_factory(settings.database_url)
    activities = SchedulingActivities(session_factory, http, tokens, mongo)
    changes = AppointmentChangeActivities(session_factory, http, tokens, mongo)
    maintenance = MaintenanceActivities(session_factory, client, mongo)
    waitlist = WaitlistActivities(session_factory, mongo)
    schedule_changes = ScheduleChangeActivities(session_factory, http, tokens, mongo, client)

    app = harness_apps()["scheduling"]
    previous = getattr(app.state, "temporal_client", None)
    app.state.temporal_client = client
    try:
        async with Worker(
            client,
            task_queue=SCHEDULING_TASK_QUEUE,
            workflows=WORKFLOWS,
            activities=[
                *activities.all(),
                *changes.all(),
                *waitlist.all(),
                *maintenance.all(),
                *schedule_changes.all(),
            ],
        ):
            yield activities
    finally:
        app.state.temporal_client = previous


@pytest_asyncio.fixture
async def scheduling_worker(
    temporal_env: WorkflowEnvironment, mesh: httpx.AsyncClient, mongo_client: AsyncMongoClient
) -> AsyncIterator[object]:
    async with running_scheduling_worker(temporal_env.client, mesh, mongo_client) as activities:
        yield activities


@pytest_asyncio.fixture
async def schedule_changes(
    temporal_env: WorkflowEnvironment, mesh: httpx.AsyncClient, mongo_client: AsyncMongoClient
):
    """The schedule-change activity object (plan 02.1-08) bound to the test
    Temporal server, for direct calls to `poll_schedule_changes` and
    `apply_schedule_change`. Use together with `scheduling_worker`, which runs
    the workflows the poller starts."""
    return build_schedule_change_activities(temporal_env.client, mesh, mongo_client)


@pytest_asyncio.fixture
async def booking_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[object]:
    from services.scheduling.config import settings  # noqa: PLC0415
    from tests.scheduling.conftest import cleanup_scheduling_rows  # noqa: PLC0415
    from tests.scheduling.seed import seed_bookable_world  # noqa: PLC0415

    world = await seed_bookable_world(client, mesh)
    try:
        yield world
    finally:
        engine = create_async_engine(settings.database_url)
        try:
            await cleanup_scheduling_rows(engine, [world.provider_id])
        finally:
            await engine.dispose()


@asynccontextmanager
async def running_visit_worker(
    client: Client, http: httpx.AsyncClient, mongo: AsyncMongoClient
) -> AsyncIterator[object]:
    """Run Visit's worker on `client` for the block (plan 02-11) and point the
    Visit app's `state.temporal_client` at `client`."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415
    from libs.db.postgres import get_session_factory  # noqa: PLC0415
    from libs.temporal_policies import VISIT_TASK_QUEUE  # noqa: PLC0415
    from services.visit.activities.downstream import VisitActivities  # noqa: PLC0415
    from services.visit.config import settings  # noqa: PLC0415
    from services.visit.worker import WORKFLOWS  # noqa: PLC0415

    tokens = ServiceTokenProvider(
        "visit",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    activities = VisitActivities(
        get_session_factory(settings.database_url), http, tokens, mongo, client
    )
    app = harness_apps()["visit"]
    previous = getattr(app.state, "temporal_client", None)
    app.state.temporal_client = client
    try:
        async with Worker(
            client, task_queue=VISIT_TASK_QUEUE, workflows=WORKFLOWS, activities=activities.all()
        ):
            yield activities
    finally:
        app.state.temporal_client = previous


@pytest_asyncio.fixture
async def visit_worker(
    temporal_env: WorkflowEnvironment, mesh: httpx.AsyncClient, mongo_client: AsyncMongoClient
) -> AsyncIterator[object]:
    async with running_visit_worker(temporal_env.client, mesh, mongo_client) as activities:
        yield activities
