"""Case-insensitive email uniqueness: unique index on lower(users.email).

Revision ID: identity_0002
Revises: identity_0001
Create Date: 2026-10-24

Carry-over C3: `users.email` is UNIQUE but case-sensitive, so `Alice@x` and
`alice@x` could be two accounts. This adds the unique expression index
`uq_users_email_lower` on `lower(email)`; the application normalizes input to
lowercase and looks accounts up through `lower(email)` so the index is used.

No row is rewritten. Existing mixed-case addresses keep their stored casing and
are matched through `lower(email)`; that is what keeps the migration fully
reversible (the downgrade just drops the index).

Preflight: if rows already collide case-insensitively the index cannot be built
and nobody should guess which account wins, so the upgrade refuses BEFORE
touching the schema. The error reports only the NUMBER of duplicate groups,
never an email address (emails are personal data and must not reach migration
logs). Resolve the duplicates (merge or delete the extra accounts), then re-run.

Production rollout: `CREATE UNIQUE INDEX` takes a write lock on `users` while it
builds. For a large table build it with `CREATE UNIQUE INDEX CONCURRENTLY`
outside a transaction (`op.get_context().autocommit_block()`); the table is small
enough here that the plain form is the simpler, safer choice.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "identity_0002"
down_revision: Union[str, None] = "identity_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEX_NAME = "uq_users_email_lower"


def _count_case_insensitive_duplicate_groups(connection: sa.Connection) -> int:
    return int(
        connection.execute(
            sa.text(
                "SELECT count(*) FROM ("
                "SELECT lower(email) FROM users GROUP BY lower(email) HAVING count(*) > 1"
                ") d"
            )
        ).scalar_one()
    )


def upgrade() -> None:
    groups = _count_case_insensitive_duplicate_groups(op.get_bind())
    if groups > 0:
        raise RuntimeError(
            f"identity_0002 refused: {groups} group(s) of users share an email address that "
            "differs only in letter case, so the case-insensitive unique index cannot be "
            "built. Resolve the duplicate accounts first (merge or delete the extras), then "
            "re-run the upgrade. No addresses are printed here on purpose."
        )
    op.create_index(INDEX_NAME, "users", [sa.text("lower(email)")], unique=True)


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="users")
