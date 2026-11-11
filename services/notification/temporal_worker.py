"""Notification's Temporal worker: `python -m services.notification.temporal_worker`
(plan 03-06; D-02, D-05, D-09). Its Compose container is added in plan 03-14.

A separate process from the Celery delivery worker. It polls `notification-tq`,
runs `ReminderWorkflow` and `FollowUpReminderWorkflow` with
`NotificationActivities`, whose `notification.enqueue` publishes the shared
Celery `notifications.send` task. Publishing is instrumented
(`instrument_publisher`), so the enqueue joins the workflow's trace and the Celery
worker's task span is its child.

Start-up mirrors `services/visit/worker.py`: logging, tracer provider, Celery
instrumentation, then an EAGER Temporal connection retried every 2 s for up to
60 s (after that the process exits non-zero and the orchestrator restarts it).
It owns no database and runs no migrations; the Celery app resolves its broker
URL from the password file only when the first message is published.

Shutdown: SIGTERM/SIGINT stop polling and give running activities
`GRACEFUL_SHUTDOWN` to finish (the enqueue is idempotent by dedupe key, so an
interrupted one is simply retried).

Liveness: a heartbeat file (`NOTIFICATION_TEMPORAL_HEARTBEAT_PATH`) is touched
every 10 s while the worker runs, for the Compose healthcheck.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
from datetime import timedelta
from pathlib import Path

from celery import Celery
from temporalio.client import Client
from temporalio.worker import Worker

from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from libs.config import shared_settings
from libs.temporal import connect_client, metrics_runtime, start_worker_metrics_server
from libs.temporal_policies import NOTIFICATION_TASK_QUEUE
from services.notification.activities import NotificationActivities
from services.notification.celery_app import app as default_celery_app
from services.notification.celery_app import instrument_publisher
from services.notification.config import settings
from services.notification.workflows.reminder import FollowUpReminderWorkflow, ReminderWorkflow

WORKFLOWS = [ReminderWorkflow, FollowUpReminderWorkflow]

log = get_logger(__name__)

HEARTBEAT_INTERVAL_SECONDS = 10
CONNECT_RETRY_SECONDS = 2
CONNECT_DEADLINE_SECONDS = 60
GRACEFUL_SHUTDOWN = timedelta(seconds=20)


def build_worker(client: Client, celery_app: Celery) -> Worker:
    """A worker on `notification-tq` with both workflows and `notification.enqueue`."""
    return Worker(
        client,
        task_queue=NOTIFICATION_TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=NotificationActivities(celery_app).all(),
        graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
    )


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


async def _heartbeat(stop: asyncio.Event, path: Path) -> None:
    while not stop.is_set():
        try:
            path.touch()
        except OSError as exc:
            log.warning("worker.heartbeat_failed", error=type(exc).__name__)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)


async def main() -> int:
    configure_logging()
    configure_tracer_provider("smarthealth-notification-temporal-worker")
    # Application metrics on WORKER_METRICS_PORT; the Temporal SDK's own metrics are
    # served by the client runtime on TEMPORAL_SDK_METRICS_BIND (plan 03-15).
    start_worker_metrics_server()
    instrument_publisher()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    try:
        client = await _connect_with_retry()
    except Exception:  # noqa: BLE001 - logged above
        return 1

    heartbeat = asyncio.create_task(_heartbeat(stop, Path(settings.temporal_heartbeat_path)))
    try:
        async with build_worker(client, default_celery_app):
            log.info("worker.started", task_queue=NOTIFICATION_TASK_QUEUE)
            await stop.wait()
            log.info("worker.stopping")
    finally:
        stop.set()
        await heartbeat
    log.info("worker.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
