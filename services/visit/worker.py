"""Visit's Temporal worker: `python -m services.visit.worker` (plan 02-11, D-05, D-16).

A separate process from the Visit API (same image, command override in
Compose, plan 02-12). It polls `visit-tq`, runs `VisitCompletedWorkflow` (the
downstream fan-out after a completion, started by the visit-downstream consumer),
`VisitDownstreamSweeperWorkflow` (a retired no-op tombstone, plan 03-12: kept
registered so a recorded history still replays) and `VisitReconcilerWorkflow`
(plan 02.1-10, S4: settles completed visits whose billing or appointment marker
stayed empty) with `VisitActivities`, and authenticates every outgoing
`/internal/*` call with Visit's aud-scoped service tokens (D-06).

Start-up: logging, audit indexes, then an EAGER Temporal connection retried
every 2 s for up to 60 s; after that the process exits non-zero and the
orchestrator restarts it. Then `ensure_visit_schedules(client)` registers the
`visit-reconciler` Schedule and `retire_sweeper_schedule(client)` deletes the retired
`visit-downstream-sweeper` Schedule (D-03; both idempotent; a failure exits 1),
followed by the heartbeat task and then one reconcile pass, which also restarts completed
visits whose `visit.completed` trigger was lost (marker still NULL after the grace, plan
03-22). The start-up pass runs as a BACKGROUND task created after the heartbeat task (gap
review WR-01): its failure is logged and never raised (the Schedule retries), it is
cancelled at shutdown if it is still running, and a slow, hanging or failing pass can
neither delay nor stop the heartbeat or keep the Worker from polling. It may overlap the
Schedule's first pass; both are idempotent. There is no start-up sweep: the outbox
consumer is the trigger and the reconcile pass is its safety net.
It never runs database migrations: the API container owns them.

Shutdown: SIGTERM/SIGINT set a stop event; the worker stops polling and gives
running activities `graceful_shutdown_timeout` to finish (every activity is
idempotent, so an interrupted one is simply retried).

Liveness: a heartbeat file (`HEARTBEAT_PATH`) is touched every 10 s while the
worker runs, for the Compose healthcheck (plan 02-12). It starts after the pre-Worker
steps (connection, audit indexes, Schedules), so a hang there still leaves it stale; the
background start-up reconcile pass cannot delay or stop it.
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
from libs.config import shared_settings
from libs.temporal import connect_client, metrics_runtime, start_worker_metrics_server
from libs.temporal_policies import VISIT_TASK_QUEUE
from services.visit.activities.downstream import VisitActivities
from services.visit.config import settings
from services.visit.schedules import ensure_visit_schedules, retire_sweeper_schedule
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
            return await connect_client(
                lazy=False, runtime=metrics_runtime(shared_settings.temporal_sdk_metrics_bind)
            )
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


async def _startup_reconcile(activities: VisitActivities) -> None:
    """The start-up reconcile pass, run as a background task: a failure is logged
    (class name only) and never raised; the Schedule reconciles again."""
    try:
        await activities.reconcile()
    except Exception as exc:  # noqa: BLE001 - the Schedule reconciles again; never fatal
        log.warning("visit.startup_reconcile_failed", error=type(exc).__name__)


def build_activities(client: Client) -> VisitActivities:
    """Production wiring: visit_db sessions, the process-default
    timeout-bounded HTTP client, Visit's service tokens, Mongo and the
    worker's own Temporal client (used by the reconciler's lost-trigger pass,
    plan 03-22)."""
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
    # Application metrics on WORKER_METRICS_PORT; the Temporal SDK's own metrics are
    # served by the client runtime on TEMPORAL_SDK_METRICS_BIND (plan 03-15).
    start_worker_metrics_server()

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
        retired = await retire_sweeper_schedule(client)
        log.info("visit.sweeper_schedule_retired", deleted=retired)
    except Exception as exc:  # noqa: BLE001 - the Schedules are required; restart
        log.error("worker.schedules_failed", error=type(exc).__name__)
        return 1

    # No start-up sweep any more (plan 03-12, D-03): the visit.completed outbox event,
    # consumed by the visit-downstream consumer, is the only downstream trigger.
    heartbeat = asyncio.create_task(_heartbeat(stop))
    startup: asyncio.Task[None] | None = None
    try:
        startup = asyncio.create_task(_startup_reconcile(activities))
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
        if startup is not None and not startup.done():
            startup.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await startup
        await heartbeat
    log.info("worker.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
