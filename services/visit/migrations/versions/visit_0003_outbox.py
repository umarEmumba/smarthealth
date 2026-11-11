"""Transactional outbox for Visit events (plan 03-09; D-01, D-03, D-06, D-07, EVENT-02).

Revision ID: visit_0003
Revises: visit_0002
Create Date: 2026-11-11

Adds visit_db's `outbox` table, the logical-replication publication
`dbz_outbox` that Debezium reads (one connector per database, slot
`dbz_visit`) and, when the `debezium` role exists (created by the Compose
`db-init` job), `GRANT SELECT ON outbox TO debezium`. All three come from
`libs.events.migration.create_outbox`, shared by every producing service.

From this revision a completion commits one `visit.completed` outbox row with the
status change and history row; that event is the trigger of the downstream work
(plan 03-09 moves the trigger off the API, D-03). `visits` and its history are
left exactly as they are, and the connector captures `public.outbox` only.

Downgrade drops the publication and the table. It is LOSSY: every retained,
possibly unconsumed event is discarded, and the replication slot (if any) must be
dropped by an operator.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from libs.events.migration import create_outbox, drop_outbox

# revision identifiers, used by Alembic.
revision: str = "visit_0003"
down_revision: Union[str, None] = "visit_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
