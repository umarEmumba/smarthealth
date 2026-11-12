"""Transactional outbox for Scheduling events (plan 03-05; D-01, D-06, EVENT-02).

Revision ID: scheduling_0006
Revises: scheduling_0005
Create Date: 2026-11-11

Adds the service's `outbox` table, the logical-replication publication
`dbz_outbox` that Debezium reads (one connector per database, slot
`dbz_scheduling`), and, when the `debezium` role exists (created by the Compose
`db-init` job), `GRANT SELECT ON outbox TO debezium`. All three come from
`libs.events.migration.create_outbox`, shared by every producing service.

A producer inserts an outbox row in the same transaction as its state change;
Debezium relays committed rows to Kafka, so there is no dual write and no relay
code in the service. The table is insert-only and kept: it is the replay source.

Downgrade drops the publication and the table. It is LOSSY: every retained,
possibly unconsumed event is discarded, and the replication slot (if any) must be
dropped by an operator.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from libs.events.migration import create_outbox, drop_outbox

# revision identifiers, used by Alembic.
revision: str = "scheduling_0006"
down_revision: Union[str, None] = "scheduling_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
