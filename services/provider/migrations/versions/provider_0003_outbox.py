"""Transactional outbox for Provider events (plan 03-08; D-01, D-06, D-07, EVENT-02).

Revision ID: provider_0003
Revises: provider_0002
Create Date: 2026-11-11

Adds provider_db's `outbox` table, the logical-replication publication
`dbz_outbox` that Debezium reads (one connector per database, slot
`dbz_provider`) and, when the `debezium` role exists (created by the Compose
`db-init` job), `GRANT SELECT ON outbox TO debezium`. All three come from
`libs.events.migration.create_outbox`, shared by every producing service.

`schedule_change_events` (the Phase 2.1 pull feed) is left exactly as it is: each
schedule change now also commits an outbox row in the same transaction, so the feed
and the event stream describe the same changes.

Downgrade drops the publication and the table. It is LOSSY: every retained,
possibly unconsumed event is discarded, and the replication slot (if any) must be
dropped by an operator.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from libs.events.migration import create_outbox, drop_outbox

# revision identifiers, used by Alembic.
revision: str = "provider_0003"
down_revision: Union[str, None] = "provider_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
