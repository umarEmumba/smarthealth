"""Alembic helpers that create and drop a service's outbox (plan 03-05, D-01, D-06).

Every producing service (scheduling, patient, provider, visit) owns exactly one
`outbox` table in its own database. Debezium reads it through the logical
replication publication `dbz_outbox` (one connector per database), so each
service's migration calls `create_outbox` and nothing else is needed to make its
rows reach Kafka. The publication is created here, by the table owner, because
the connector runs with `publication.autocreate.mode=disabled` and the least
privileged `debezium` role (REPLICATION + SELECT on the outbox only).

The `debezium` role is created by the Compose `db-init` job, which runs before
any service migration. The SELECT grant is guarded by role existence so that the
same migration still applies on a developer database that has no such role (for
example the unit-test database before `db-init` ever ran); the grant is then
simply absent and the connector would fail its snapshot loudly, which the
registration script and the e2e tests detect (task state, not connector state).

Usage inside a migration:

    from libs.events.migration import create_outbox, drop_outbox

    def upgrade() -> None:
        create_outbox(op)

    def downgrade() -> None:
        drop_outbox(op)
"""

from __future__ import annotations

from typing import Any

from libs.events.outbox import outbox_table_ddl

PUBLICATION_NAME = "dbz_outbox"
DEBEZIUM_ROLE = "debezium"

_PUBLICATION_DDL = f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE outbox"

_GUARDED_GRANT = f"""
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{DEBEZIUM_ROLE}') THEN
        GRANT SELECT ON outbox TO {DEBEZIUM_ROLE};
    END IF;
END
$$
"""


def create_outbox(op: Any) -> None:
    """Create the `outbox` table, the `dbz_outbox` publication and the guarded grant.

    Must run as the table owner (the service role): only the owner of a table may
    add it to a publication. `outbox` is insert-only for the service and is the
    replay source for consumers (D-10), so no UPDATE/DELETE path exists.
    """
    op.execute(outbox_table_ddl())
    op.execute(_PUBLICATION_DDL)
    op.execute(_GUARDED_GRANT)


def drop_outbox(op: Any) -> None:
    """Drop the publication and the table.

    LOSSY: the outbox is the replay source for events that consumers may not have
    processed yet. Downgrading discards every retained event; the Debezium
    replication slot, if any, must be dropped separately by an operator.
    """
    op.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION_NAME}")
    op.execute("DROP TABLE IF EXISTS outbox")
