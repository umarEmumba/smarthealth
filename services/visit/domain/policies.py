"""Visit-local Temporal activity presets (plan 02.1-10; S4, BILL-02).

`BILLING_FINALIZE_ACTIVITY` is the option preset for the billing branch of
`VisitCompletedWorkflow`. The visit is already committed as `completed`, so
Billing must catch up; giving up after a bounded budget (the old
`COMPENSATION_ACTIVITY`: 20 attempts, 10 minutes) left the branch reporting
`skipped:<reason>` with the Billing record stuck at `verified` and nobody
retrying. This preset never gives up on a transient failure:

* an attempt cap of zero means "unlimited" and there is no `schedule_to_close_timeout`,
  so a Billing outage of any length is waited out;
* retries add no workflow history events (the attempt counter lives on the
  server, not in history), so an open-ended retry does not grow history;
* the backoff interval is capped at 5 minutes, which bounds the load a retrying
  workflow puts on an unhealthy Billing (2 s, 4 s, 8 s, ... then every 5 min);
* each attempt is bounded by `start_to_close_timeout` (30 s);
* `BillingNotFinalizable` (the record is voided or refunded: a terminal
  business answer from Billing) is the only outcome that ends the step early.

Sandbox-safe on purpose: this module imports only `datetime` and
`temporalio.common`, nothing from the service, so workflow code may load it
inside `workflow.unsafe.imports_passed_through()`. It lives in Visit (not in
the shared `libs.temporal_policies`) so it cannot collide with the Scheduling
plans' use of that module.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy

BILLING_FINALIZE_ACTIVITY: dict = {
    "start_to_close_timeout": timedelta(seconds=30),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=2),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(minutes=5),
        maximum_attempts=0,
        non_retryable_error_types=["BillingNotFinalizable"],
    ),
}
