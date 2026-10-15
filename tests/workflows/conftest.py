"""Fixtures for Temporal workflow tests (plans 02-06, 02-07).

The Temporal fixtures this folder shares with `tests/scheduling/`
(`temporal_env`, `scheduling_worker`, `booking_world`) live in
`tests/temporal_fixtures.py`, registered from the root `tests/conftest.py` via
`pytest_plugins`, because pytest does not share a sibling folder's conftest
fixtures.

`scheduling_worker_degraded` (plan 02-07) is the `scheduling_worker` wiring
with Billing unreachable from the ACTIVITIES' HTTP client for the whole test
(every call is a refused connection, as from `degraded_mesh("billing")`). Only
the worker's client is degraded, so the gateway can still read Billing to
prove nothing was written there.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest_asyncio
from pymongo import AsyncMongoClient
from temporalio.testing import WorkflowEnvironment

from tests.conftest import _mesh_mounts
# Re-exported: compensation/maintenance tests inspect committed scheduling rows.
from tests.scheduling.conftest import committed_scheduling_engine  # noqa: F401
from tests.temporal_fixtures import running_scheduling_worker


@pytest_asyncio.fixture
async def scheduling_worker_degraded(
    temporal_env: WorkflowEnvironment, mesh: httpx.AsyncClient, mongo_client: AsyncMongoClient
) -> AsyncIterator[object]:
    async with httpx.AsyncClient(mounts=_mesh_mounts(("billing",))) as degraded:
        async with running_scheduling_worker(temporal_env.client, degraded, mongo_client) as acts:
            yield acts
