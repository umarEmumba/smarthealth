"""Temporal client factory and exactly-once workflow start (plan 02-06).

`connect_client` builds a client for `shared_settings.temporal_address` /
`temporal_namespace` with OpenTelemetry's `TracingInterceptor`, so a workflow
started inside an HTTP request span, and the activities it runs on a worker
whose client carries the same interceptor, join that request's trace (D-18).
API processes connect lazily (`lazy=True`): the app boots and serves reads while
Temporal is down, and the first RPC connects (research Pitfall 4). Workers
connect eagerly.

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

from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.exceptions import WorkflowAlreadyStartedError

from libs.config import shared_settings

__all__ = ["connect_client", "start_once"]

# Bound on one start RPC, so a request never hangs on an unreachable server.
START_RPC_TIMEOUT = timedelta(seconds=5)


async def connect_client(*, lazy: bool = True) -> Client:
    """A Temporal client for the configured address and namespace, with
    trace propagation. `lazy=True` defers the connection to the first call."""
    return await Client.connect(
        shared_settings.temporal_address,
        namespace=shared_settings.temporal_namespace,
        lazy=lazy,
        interceptors=[TracingInterceptor()],
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
