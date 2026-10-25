"""Visit's Temporal contracts (plan 02-11; D-05, D-06, D-16).

Pure on purpose: only dataclasses and typing, so the workflow sandbox can load
this module. Every field is a plain `str` (ids, outcomes) or `int` (counts):
workflow histories are durable and operator-visible, so nothing here may carry
PHI, coverage data or credentials, and no str-subclass Enum (it decodes as a
list of characters).

The task queue (`visit-tq`, `libs.temporal_policies.VISIT_TASK_QUEUE`), the
workflow type names and the id scheme (`visit_completed_workflow_id`) live in
histories: renaming any of them is a migration.
"""

from __future__ import annotations

from dataclasses import dataclass

VISIT_COMPLETED_WORKFLOW = "VisitCompletedWorkflow"
SWEEPER_WORKFLOW = "VisitDownstreamSweeperWorkflow"

ACT_FINALIZE_BILLING = "visit.finalize_billing"
ACT_FOLLOWUP_INTENT = "visit.record_followup_intent"
ACT_ANALYTICS_INTENT = "visit.record_analytics_intent"
ACT_COMPLETE_APPOINTMENT = "visit.complete_appointment"
ACT_SWEEP = "visit.sweep_unstarted"

# workflow.patched id (plan 02.1-09, M1): VisitCompletedWorkflow gains a fourth
# concurrent branch that completes the appointment in Scheduling. Histories
# recorded before it have no marker and replay the original three branches.
VISIT_COMPLETE_APPOINTMENT_PATCH_ID = "visit-complete-appointment-v1"

# workflow.patched id (plan 02.1-10, S4): the billing branch uses the open-ended
# BILLING_FINALIZE_ACTIVITY preset instead of the bounded COMPENSATION_ACTIVITY.
# Histories recorded before it keep the bounded options (the options are part of
# the scheduled-activity command, so they must replay unchanged).
BILLING_RETRY_PATCH_ID = "visit-billing-retry-v1"

OUTCOME_COMPLETED = "completed"
BILLING_FINALIZED = "finalized"
BILLING_NO_RECORD = "no_record"
BILLING_NOT_FINALIZABLE = "not_finalizable"
APPOINTMENT_COMPLETED = "completed"
APPOINTMENT_NO_APPOINTMENT = "no_appointment"
APPOINTMENT_NOT_CHECKED_IN = "not_checked_in"
INTENT_RECORDED = "recorded"
SKIPPED_PREFIX = "skipped:"


@dataclass
class VisitCompletedInput:
    visit_id: str
    appointment_id: str
    clinic_id: str


@dataclass
class VisitCompletedResult:
    """`outcome` is always "completed"; each branch is "finalized" /
    "no_record" (billing), "recorded" (intents), "completed" /
    "no_appointment" (appointment, plan 02.1-09) or "skipped:<ErrorType>".
    `appointment` defaults to "" so results recorded before the fourth branch
    still decode."""

    outcome: str
    billing: str
    followup: str
    analytics: str
    appointment: str = ""


@dataclass
class SweepResult:
    processed: int


def visit_completed_workflow_id(visit_id: str) -> str:
    """Deterministic per visit (D-06): a second start is a no-op."""
    return f"visit-completed-{visit_id}"
