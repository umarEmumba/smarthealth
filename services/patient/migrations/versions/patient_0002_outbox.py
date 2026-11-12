"""Transactional outbox for Patient events (plan 03-08; D-01, D-06, D-07, EVENT-02).

Revision ID: patient_0002
Revises: patient_0001
Create Date: 2026-11-11

Adds patient_db's `outbox` table, the logical-replication publication
`dbz_outbox` that Debezium reads (one connector per database, slot
`dbz_patient`) and, when the `debezium` role exists (created by the Compose
`db-init` job), `GRANT SELECT ON outbox TO debezium`. All three come from
`libs.events.migration.create_outbox`, shared by every producing service.

Patient is the only service holding PHI. The publication covers `outbox` ONLY and
the debezium role has no privilege on `patients`, so the replication slot on this
database can never expose a PHI column (T-3-58). Events written here carry ids and
timestamps only (T-3-24).

Downgrade drops the publication and the table. It is LOSSY: every retained,
possibly unconsumed event is discarded, and the replication slot (if any) must be
dropped by an operator.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from libs.events.migration import create_outbox, drop_outbox

# revision identifiers, used by Alembic.
revision: str = "patient_0002"
down_revision: Union[str, None] = "patient_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
