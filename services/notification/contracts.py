"""Pure contracts of the reminder workflows (plan 03-06).

Sandbox-safe on purpose: plain dataclasses of strings and integers, no settings,
no I/O, so a workflow module can import this inside
`workflow.unsafe.imports_passed_through()`. Everything here is ids, ISO
timestamps and integers: no PHI ever enters Temporal history (T-3-31).

The workflow, activity and signal names are stable forever: they are written
into recorded histories and every starter, so renaming one is a migration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

REMINDER_WORKFLOW = "ReminderWorkflow"
FOLLOWUP_WORKFLOW = "FollowUpReminderWorkflow"
ACT_ENQUEUE = "notification.enqueue"
SIGNAL_CANCEL = "cancel"
SIGNAL_RESCHEDULE = "reschedule"

TYPE_REMINDER = "appointment.reminder"
TYPE_FOLLOWUP = "visit.followup"

STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"
STATUS_SKIPPED_PAST = "skipped_past"


@dataclass
class ReminderInput:
    appointment_id: str
    patient_id: str
    slot_start_iso: str
    lead_minutes: list[int] = field(default_factory=list)


@dataclass
class FollowUpInput:
    visit_id: str
    appointment_id: str
    patient_id: str
    completed_at_iso: str
    delay_minutes: int


@dataclass
class EnqueueRequest:
    notification_type: str
    dedupe_key: str
    patient_id: str
    appointment_id: str
    visit_id: str | None
    requested_at_iso: str


@dataclass
class ReminderResult:
    status: str
    sent_leads: list[int] = field(default_factory=list)


def reminder_workflow_id(appointment_id: str) -> str:
    return f"reminder-{appointment_id}"


def followup_workflow_id(visit_id: str) -> str:
    return f"followup-{visit_id}"


def parse_instant(value: str) -> datetime:
    """An ISO timestamp as an aware UTC datetime (a naive value is read as UTC)."""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def slot_epoch(slot_start_iso: str) -> int:
    """The slot start in whole epoch seconds: part of the reminder dedupe key, so a
    rescheduled slot gets its own reminders while a repeat for the same slot dedupes."""
    return int(parse_instant(slot_start_iso).timestamp())


def reminder_dedupe_key(appointment_id: str, slot_start_iso: str, lead_minutes: int) -> str:
    return f"{TYPE_REMINDER}:{appointment_id}:{slot_epoch(slot_start_iso)}:{lead_minutes}"


def followup_dedupe_key(visit_id: str) -> str:
    return f"{TYPE_FOLLOWUP}:{visit_id}"
