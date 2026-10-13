"""Shared Temporal test fixtures (plan 02-06), registered from the root
`tests/conftest.py` through `pytest_plugins` so that BOTH `tests/workflows/`
and `tests/scheduling/` (and later `tests/visit/`) can use them: pytest does
not share a sibling folder's conftest fixtures.

* `temporal_env`       - a fresh time-skipping Temporal test server per test
                         (`WorkflowEnvironment.start_time_skipping()`). Time
                         skips only while a test awaits a workflow result; the
                         database's `now()` never skips (research Pitfall 8).
* `scheduling_worker`  - a real `Worker` on `scheduling-tq` running
                         `BookingWorkflow` with `SchedulingActivities` wired to
                         the in-process mesh (HTTP + service-token minting), the
                         real scheduling_db and the test Mongo client. While it
                         runs, the Scheduling app's `state.temporal_client` is
                         the test server's client, so `POST /api/appointments`
                         starts sagas this worker executes.
* `booking_world`      - `seed_bookable_world` plus clean-up of the Scheduling
                         rows it (and the bookings made in it) created.

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
    from services.scheduling.config import settings  # noqa: PLC0415
    from services.scheduling.workflows.booking import BookingWorkflow  # noqa: PLC0415

    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    activities = SchedulingActivities(get_session_factory(settings.database_url), http, tokens, mongo)

    app = harness_apps()["scheduling"]
    previous = getattr(app.state, "temporal_client", None)
    app.state.temporal_client = client
    try:
        async with Worker(
            client,
            task_queue=SCHEDULING_TASK_QUEUE,
            workflows=[BookingWorkflow],
            activities=activities.all(),
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
