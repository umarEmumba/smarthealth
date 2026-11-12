"""Same-transaction idempotency for database effects (plan 03-04, D-08 refined, EVENT-03).

Kafka delivery is at-least-once (Debezium re-emits after a hard kill, a consumer
can crash between its effect and its offset commit), so every consumer must make
a redelivered event a no-op. THIS table is only for consumers whose effect is a
DATABASE write, e.g. the analytics projection: the event id is claimed in the
SAME transaction as the effect, so either both commit or neither does, and a
redelivered event finds the claim and changes nothing.

Do not use it for other effects (D-08 refined):

* Temporal effects use deterministic workflow ids with the reject-duplicate and
  use-existing policies;
* Celery effects are absorbed at the send by the unique dedupe key
  `(notification_type, appointment_id, event_id)`.

`claim_event` runs on the CALLER'S session and never commits (the repository
contract: the caller owns the transaction).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_CLAIM_SQL = text(
    """
    INSERT INTO processed_events (event_id, consumer)
    VALUES (:event_id, :consumer)
    ON CONFLICT DO NOTHING
    RETURNING event_id
    """
)


def processed_events_table_ddl(temporary: bool = False) -> str:
    """DDL for `processed_events` (a TEMP table in tests when `temporary`)."""
    create = "CREATE TEMP TABLE" if temporary else "CREATE TABLE"
    return f"""
    {create} processed_events (
        event_id     uuid        NOT NULL,
        consumer     text        NOT NULL,
        processed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (event_id, consumer)
    )
    """


def create_processed_events_table(op: Any) -> None:
    """Alembic helper: `create_processed_events_table(op)` inside `upgrade()`."""
    op.execute(processed_events_table_ddl())


async def claim_event(db: AsyncSession, *, event_id: uuid.UUID | str, consumer: str) -> bool:
    """Claim `event_id` for `consumer` in the caller's transaction.

    True the first time (apply the effect in the same transaction), False when
    this consumer already processed the event (do nothing). Different consumers
    claim the same event independently. A concurrent claimant of the same key
    waits for the first transaction: it sees False if that one commits and True
    if it rolls back, so a lost race never raises.
    """
    result = await db.execute(
        _CLAIM_SQL, {"event_id": uuid.UUID(str(event_id)), "consumer": consumer}
    )
    return result.first() is not None
