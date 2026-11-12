"""Temporal task queues and activity option presets (plan 02-06, D-05, D-06).

Sandbox-safe on purpose: this module imports only `datetime` and
`temporalio.common`, so workflow modules may import it (inside
`workflow.unsafe.imports_passed_through()`) without dragging settings objects,
database drivers or HTTP clients into the deterministic workflow sandbox.
Anything that reads configuration (`libs.temporal`) stays out of workflows.

One task queue per service (D-05): Scheduling's workflows and activities run on
`SCHEDULING_TASK_QUEUE`, Visit's on `VISIT_TASK_QUEUE`. The names live in
workflow histories and in every starter, so renaming one is a migration.

Every activity is bounded (research Pitfall 6): the SDK's default retry policy
never gives up, which would stall a saga against a down dependency instead of
letting it compensate. Business outcomes are raised as non-retryable
`ApplicationError`s whose `type` is listed in `NON_RETRYABLE`, so Temporal does
not retry them even if a raiser forgets `non_retryable=True`.

The presets are dicts of `workflow.execute_activity` keyword arguments:
`await workflow.execute_activity(name, arg, **FORWARD_ACTIVITY)`.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy

SCHEDULING_TASK_QUEUE = "scheduling-tq"
VISIT_TASK_QUEUE = "visit-tq"

# ApplicationError types that are business outcomes, never retried.
NON_RETRYABLE = ["SlotUnavailable", "BillingRejected", "ReferenceInvalid", "ConfirmLostHold"]

# Forward saga steps: a few quick retries for transport trouble, then the saga
# compensates (schedule_to_close caps the total time across attempts).
FORWARD_ACTIVITY: dict = {
    "start_to_close_timeout": timedelta(seconds=10),
    "schedule_to_close_timeout": timedelta(seconds=60),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=10),
        maximum_attempts=5,
        non_retryable_error_types=NON_RETRYABLE,
    ),
}

# Best-effort notification intent (D-09, D-11): short budget; a failure is
# logged by the workflow and never fails or compensates the booking.
NOTIFY_ACTIVITY: dict = {
    "start_to_close_timeout": timedelta(seconds=10),
    "schedule_to_close_timeout": timedelta(seconds=30),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=10),
        maximum_attempts=3,
        non_retryable_error_types=NON_RETRYABLE,
    ),
}

# Maintenance-style activities (plan 02.1-08, reused by 02.1-11): idempotent
# sweeps over durable state (the schedule-change apply, the feed poller). They
# touch many rows or several services in one run, so the single attempt is
# allowed longer than a saga step, and a transient outage of an owning service
# is retried for a few minutes before the workflow fails (the next poll or the
# reconciler then picks the work up again). Business outcomes stay non-retryable.
MAINTENANCE_ACTIVITY: dict = {
    "start_to_close_timeout": timedelta(seconds=60),
    "schedule_to_close_timeout": timedelta(minutes=5),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=30),
        maximum_attempts=5,
        non_retryable_error_types=NON_RETRYABLE,
    ),
}

# Compensations must eventually land, so they retry far longer than forward
# steps; each one is idempotent, so retrying is always safe.
COMPENSATION_ACTIVITY: dict = {
    "start_to_close_timeout": timedelta(seconds=10),
    "schedule_to_close_timeout": timedelta(minutes=10),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=30),
        maximum_attempts=20,
        non_retryable_error_types=NON_RETRYABLE,
    ),
}
