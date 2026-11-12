"""Appointment states checked_in, completed and no_show (plan 02.1-01; M1).

Revision ID: scheduling_0004
Revises: scheduling_0003
Create Date: 2026-10-24

Phase 2 deliberately kept `completed` out of the appointment vocabulary: the
active-slot unique index and the patient-overlap exclusion constraint covered
`pending`/`confirmed` only, so any status outside that predicate would free
the slot for rebooking (research Open Question 4). M1 adds the states by
WIDENING those predicates instead, so the guarantee holds at the database level
for every code path, including a raw UPDATE:

  * `ck_appointments_status_valid` accepts pending, confirmed, checked_in,
    completed, no_show, cancelled, failed.
  * `uq_appointments_active_slot` (partial UNIQUE on slot_id) now covers
    pending, confirmed, checked_in, completed, no_show: a checked-in, finished
    or missed appointment keeps its slot reserved forever, so cancel and
    reschedule can never rebook it.
  * `ex_appointments_patient_no_overlap` (EXCLUDE gist) has the same widened
    WHERE: a patient with a checked-in appointment cannot hold an overlapping one.

The widened predicates are supersets of the old ones, so no existing row can
violate them. The rebuilds take a table lock while they run (acceptable on the
local stack; a production rollout would build the new unique index CONCURRENTLY
under a temporary name first, then swap).

Downgrade (rollback): rows in checked_in / completed / no_show are first
rewritten to `confirmed` (LOSSY by design: the narrow predicates cannot express
the new states; `appointment_status_history` rows are never touched and keep
the true path), then the narrow CHECK, index and EXCLUDE are restored.
The rewrite cannot make two rows collide: the widened unique index and EXCLUDE
already allowed at most one slot-holding row per slot and no overlapping rows
per patient.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "scheduling_0004"
down_revision: Union[str, None] = "scheduling_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_NARROW_STATUSES = "status IN ('pending','confirmed','cancelled','failed')"
_NARROW_ACTIVE = "status IN ('pending','confirmed')"

_WIDE_STATUSES = (
    "status IN ('pending','confirmed','checked_in','completed','no_show','cancelled','failed')"
)
_WIDE_ACTIVE = "status IN ('pending','confirmed','checked_in','completed','no_show')"


def _rebuild(statuses_check: str, active_predicate: str) -> None:
    """Drop and recreate the status CHECK, the active-slot index and the
    patient-overlap EXCLUDE with the given predicates."""
    op.drop_constraint("ck_appointments_status_valid", "appointments", type_="check")
    op.create_check_constraint("ck_appointments_status_valid", "appointments", statuses_check)

    op.drop_index("uq_appointments_active_slot", table_name="appointments")
    op.create_index(
        "uq_appointments_active_slot",
        "appointments",
        ["slot_id"],
        unique=True,
        postgresql_where=sa.text(active_predicate),
    )

    op.execute(
        "ALTER TABLE appointments DROP CONSTRAINT IF EXISTS ex_appointments_patient_no_overlap"
    )
    op.execute(
        f"""
        ALTER TABLE appointments
        ADD CONSTRAINT ex_appointments_patient_no_overlap
        EXCLUDE USING gist (
            patient_id WITH =,
            tstzrange(slot_start, slot_end) WITH &&
        ) WHERE ({active_predicate})
        """
    )


def upgrade() -> None:
    _rebuild(_WIDE_STATUSES, _WIDE_ACTIVE)


def downgrade() -> None:
    # Lossy by design: the narrow predicates have no place for the new states.
    # The status history keeps the real path (checked_in / completed / no_show).
    op.execute(
        "UPDATE appointments SET status = 'confirmed' "
        "WHERE status IN ('checked_in','completed','no_show')"
    )
    _rebuild(_NARROW_STATUSES, _NARROW_ACTIVE)
