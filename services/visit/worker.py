"""Visit's Temporal worker: `python -m services.visit.worker` (plan 02-11, D-05, D-16).

A separate process from the Visit API (same image, command override in
Compose, plan 02-12). It polls `visit-tq`, runs `VisitCompletedWorkflow` (the
downstream fan-out after a completion), `VisitDownstreamSweeperWorkflow`
(lost-trigger recovery) and `VisitReconcilerWorkflow` (plan 02.1-10, S4: settles
completed visits whose billing or appointment marker stayed empty) with
`VisitActivities`, and authenticates every outgoing `/internal/*` call with
Visit's aud-scoped service tokens (D-06).

Start-up: logging, audit indexes, then an EAGER Temporal connection retried
every 2 s for up to 60 s; after that the process exits non-zero and the
orchestrator restarts it. Then `ensure_visit_schedules(client)` registers the
`visit-downstream-sweeper` and `visit-reconciler` Schedules (idempotent; a
failure exits 1), and one `sweep_unstarted` runs immediately so visits whose
start was lost while the worker was down are picked up at once, followed by one
reconcile pass (a failure of either is logged, never fatal: the Schedules
retry). It never runs database migrations: the API container owns them.

Shutdown: SIGTERM/SIGINT set a stop event; the worker stops polling and gives
running activities `graceful_shutdown_timeout` to finish (every activity is
idempotent, so an interrupted one is simply retried).

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
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.activities.downstream import VisitActivities
from services.visit.config import settings
from services.visit.downstream import sweep_unstarted
from services.visit.schedules import ensure_visit_schedules
from services.visit.workflows.reconciler import VisitReconcilerWorkflow
from services.visit.workflows.sweeper import VisitDownstreamSweeperWorkflow
from services.visit.workflows.visit_completed import VisitCompletedWorkflow

WORKFLOWS = [VisitCompletedWorkflow, VisitDownstreamSweeperWorkflow, VisitReconcilerWorkflow]

log = get_logger(__name__)

HEARTBEAT_PATH = Path("/tmp/visit-worker.alive")
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


def build_activities(client: Client) -> VisitActivities:
    """Production wiring: visit_db sessions, the process-default
    timeout-bounded HTTP client, Visit's service tokens, Mongo and the
    worker's own Temporal client (for the sweep)."""
    http = _get_default_client()
    tokens = ServiceTokenProvider(
        "visit",
        load_client_secret(settings.client_secret_path),
        settings.identity_service_url,
        http=http,
    )
    return VisitActivities(
        get_session_factory(settings.database_url), http, tokens, get_mongo_client(), client
    )


async def main() -> int:
    configure_logging()
    configure_tracer_provider("smarthealth-visit-worker")

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
        activities = build_activities(client)
    except ServiceTokenUnavailable:
        # The secret is never logged; only the fact that it could not be read.
        log.error("worker.client_secret_unavailable")
        return 1
    await ensure_audit_indexes(get_mongo_client())
    try:
        await ensure_visit_schedules(client)
    except Exception as exc:  # noqa: BLE001 - the recovery Schedule is required; restart
        log.error("worker.schedules_failed", error=type(exc).__name__)
        return 1

    # Run-on-start: recover triggers lost while no worker was running.
    try:
        await sweep_unstarted(
            get_session_factory(settings.database_url),
            client,
            grace_seconds=settings.sweeper_grace_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - the Schedule sweeps again; never fatal
        log.warning("visit.startup_sweep_failed", error=type(exc).__name__)
    try:
        await activities.reconcile()
    except Exception as exc:  # noqa: BLE001 - the Schedule reconciles again; never fatal
        log.warning("visit.startup_reconcile_failed", error=type(exc).__name__)

    heartbeat = asyncio.create_task(_heartbeat(stop))
    try:
        async with Worker(
            client,
            task_queue=VISIT_TASK_QUEUE,
            workflows=WORKFLOWS,
            activities=activities.all(),
            graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
        ):
            log.info("worker.started", task_queue=VISIT_TASK_QUEUE)
            await stop.wait()
            log.info("worker.stopping")
    finally:
        stop.set()
        await heartbeat
    log.info("worker.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
