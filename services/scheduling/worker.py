"""Scheduling's Temporal worker: `python -m services.scheduling.worker` (D-05).

A separate process from the API (same image, command override in Compose,
plan 02-12). It polls `scheduling-tq`, runs `BookingWorkflow`, the maintenance
workflows (`HoldReaperWorkflow`, `PendingBookingStarterWorkflow`, plan 02-07),
the `SchedulingActivities` and `MaintenanceActivities`, and authenticates every outgoing `/internal/*` call
with Scheduling's aud-scoped service tokens (D-06).

Start-up: logging, audit indexes, then an EAGER Temporal connection retried
every 2 s for up to 60 s (Temporal's auto-setup can boot slower than this
process); after that the process exits non-zero and the orchestrator restarts
it. Then `ensure_schedules(client)` registers the reaper and starter Temporal
Schedules (idempotent: an existing Schedule is left alone) before polling. It never runs Alembic: the API container owns migrations (research
anti-pattern), and Compose starts the worker after the API is healthy.

Shutdown: SIGTERM/SIGINT set a stop event; the worker then stops polling and
gives running activities `graceful_shutdown_timeout` to finish (an interrupted
activity is retried elsewhere; every activity is idempotent).

Liveness: a heartbeat file (`HEARTBEAT_PATH`) is touched every 10 s while the
worker runs, for the Compose healthcheck (plan 02-12).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from libs.audit.mongo_indexes import ensure_audit_indexes
from libs.auth.service_tokens import ServiceTokenProvider, ServiceTokenUnavailable, load_client_secret
from libs.db.mongo import get_mongo_client
from libs.db.postgres import get_session_factory
from libs.http import _get_default_client
from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from libs.temporal import connect_client
from libs.temporal_policies import SCHEDULING_TASK_QUEUE
from services.scheduling.activities.booking import SchedulingActivities
from services.scheduling.activities.maintenance import MaintenanceActivities
from services.scheduling.config import settings
from services.scheduling.schedules import ensure_schedules
from services.scheduling.workflows.booking import BookingWorkflow
from services.scheduling.workflows.maintenance import (
    HoldReaperWorkflow,
    PendingBookingStarterWorkflow,
)

WORKFLOWS = [BookingWorkflow, HoldReaperWorkflow, PendingBookingStarterWorkflow]

log = get_logger(__name__)

HEARTBEAT_PATH = Path("/tmp/scheduling-worker.alive")
HEARTBEAT_INTERVAL_SECONDS = 10
CONNECT_RETRY_SECONDS = 2
CONNECT_DEADLINE_SECONDS = 60
GRACEFUL_SHUTDOWN = timedelta(seconds=20)


async def _connect_with_retry() -> Client:
    deadline = time.monotonic() + CONNECT_DEADLINE_SECONDS
    attempt = 0
    while True:
        attempt += 1
        try:
            return await connect_client(lazy=False)
        except Exception as exc:  # noqa: BLE001 - any connect failure is retried, then fatal
            if time.monotonic() >= deadline:
                log.error("worker.temporal_connect_failed", attempts=attempt, error=type(exc).__name__)
                raise
            log.warning("worker.temporal_connect_retry", attempt=attempt, error=type(exc).__name__)
            await asyncio.sleep(CONNECT_RETRY_SECONDS)


async def _heartbeat(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            HEARTBEAT_PATH.touch()
        except OSError as exc:
            log.warning("worker.heartbeat_failed", error=type(exc).__name__)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)


def build_activities() -> SchedulingActivities:
    """Production wiring: scheduling_db sessions, the process-default
    timeout-bounded HTTP client, Scheduling's service tokens and Mongo."""
    http = _get_default_client()
    tokens = ServiceTokenProvider(
        "scheduling",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    return SchedulingActivities(
        get_session_factory(settings.database_url),
        http,
        tokens,
        get_mongo_client(),
    )


async def main() -> int:
    configure_logging()
    configure_tracer_provider("smarthealth-scheduling-worker")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    try:
        client = await _connect_with_retry()
    except Exception:  # noqa: BLE001 - logged above
        return 1

    try:
        activities = build_activities()
    except ServiceTokenUnavailable:
        # The secret is never logged; only the fact that it could not be read.
        log.error("worker.client_secret_unavailable")
        return 1
    maintenance = MaintenanceActivities(
        get_session_factory(settings.database_url), client, get_mongo_client()
    )
    await ensure_audit_indexes(get_mongo_client())
    try:
        await ensure_schedules(client)
    except Exception as exc:  # noqa: BLE001 - recovery Schedules are required; restart
        log.error("worker.schedules_failed", error=type(exc).__name__)
        return 1

    heartbeat = asyncio.create_task(_heartbeat(stop))
    try:
        async with Worker(
            client,
            task_queue=SCHEDULING_TASK_QUEUE,
            workflows=WORKFLOWS,
            activities=[*activities.all(), *maintenance.all()],
            graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
        ):
            log.info("worker.started", task_queue=SCHEDULING_TASK_QUEUE)
            await stop.wait()
            log.info("worker.stopping")
    finally:
        stop.set()
        await heartbeat
    log.info("worker.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
