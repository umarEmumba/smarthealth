"""Temporal client factory and exactly-once workflow start (plan 02-06).

`connect_client` builds a client for `shared_settings.temporal_address` /
`temporal_namespace` with OpenTelemetry's `TracingInterceptor`, so a workflow
started inside an HTTP request span, and the activities it runs on a worker
whose client carries the same interceptor, join that request's trace (D-18).
API processes connect lazily (`lazy=True`): the app boots and serves reads while
Temporal is down, and the first RPC connects (research Pitfall 4). Workers
connect eagerly.

Workers also pass `runtime=metrics_runtime(bind)` (plan 03-15, D-11): a Temporal
SDK `Runtime` whose telemetry config serves the SDK's own metrics (task-poll
latency, activity and workflow task execution, sticky cache) in Prometheus
format on `bind`. A process may have only one such server per bind address, so
the runtime is created once and cached.

`start_once` starts a workflow under a deterministic ID with
`USE_EXISTING` (a start while it runs attaches to the running execution) and
`REJECT_DUPLICATE` (a start after it closed is refused), treating
`WorkflowAlreadyStartedError` as "already started" (research Pattern 2). The
database arbiters, not the workflow ID, are the real idempotency guarantee:
workflow-ID uniqueness lasts only as long as the namespace retention
(research Pitfall 3).

No run/execution timeout is ever passed when starting a saga: a run timeout
terminates the workflow WITHOUT running its compensations, whereas bounded
activities let the saga reach compensation on its own (research Pattern 8).

This module reads `libs.config`, so workflow modules must never import it
(sandbox rule); they import `libs.temporal_policies` instead.
"""

from __future__ import annotations

import threading
from datetime import timedelta
from typing import Any

from prometheus_client import start_http_server
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig

from libs.config import shared_settings
from libs.observability.logging import get_logger

__all__ = ["connect_client", "metrics_runtime", "start_once", "start_worker_metrics_server"]

# Bound on one start RPC, so a request never hangs on an unreachable server.
START_RPC_TIMEOUT = timedelta(seconds=5)


log = get_logger(__name__)

_runtimes: dict[str, Runtime] = {}
_runtimes_lock = threading.Lock()
_app_metrics_started = False


def metrics_runtime(bind_address: str) -> Runtime:
    """The process's Temporal SDK `Runtime`, serving SDK metrics in Prometheus
    format on `bind_address` (e.g. `0.0.0.0:9464`). Cached per address: calling
    it again (a connect retry) returns the same runtime instead of trying to
    bind the port a second time."""
    with _runtimes_lock:
        runtime = _runtimes.get(bind_address)
        if runtime is None:
            runtime = Runtime(
                telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=bind_address))
            )
            _runtimes[bind_address] = runtime
        return runtime


def start_worker_metrics_server() -> None:
    """Serve this worker process's application metrics (the default
    prometheus_client registry: `booking_saga_seconds`, outbox and notification
    metrics ...) on `worker_metrics_port`, once per process. An address that is
    already bound is logged and skipped (the worker keeps running; the target
    shows as down rather than the worker crash-looping)."""
    global _app_metrics_started  # noqa: PLW0603
    with _runtimes_lock:
        if _app_metrics_started:
            return
        try:
            start_http_server(shared_settings.worker_metrics_port)
        except OSError:
            log.warning("worker.metrics_server_unavailable", port=shared_settings.worker_metrics_port)
        _app_metrics_started = True


async def connect_client(*, lazy: bool = True, runtime: Runtime | None = None) -> Client:
    """A Temporal client for the configured address and namespace, with
    trace propagation. `lazy=True` defers the connection to the first call.
    `runtime` (workers only) exposes the SDK's metrics; see `metrics_runtime`."""
    return await Client.connect(
        shared_settings.temporal_address,
        namespace=shared_settings.temporal_namespace,
        lazy=lazy,
        interceptors=[TracingInterceptor()],
        runtime=runtime,
    )


async def start_once(
    client: Client,
    workflow: Any,
    arg: Any,
    *,
    workflow_id: str,
    task_queue: str,
) -> bool:
    """Start `workflow(arg)` as `workflow_id` unless it already ran or runs.

    True when this call started it (or attached to the running execution),
    False when a run with this ID already closed. RPC/connection errors
    propagate; callers decide whether to defer (the API returns 202 anyway and
    the starter sweeper retries).
    """
    try:
        await client.start_workflow(
            workflow,
            arg,
            id=workflow_id,
            task_queue=task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            rpc_timeout=START_RPC_TIMEOUT,
        )
    except WorkflowAlreadyStartedError:
        return False
    return True
