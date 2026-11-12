"""Read-only metric queries over the analytics views (plan 03-13; ANLY-01..06).

Every number is computed by a view in analytics_db (plan 03-10, with the written
definition in its `COMMENT ON VIEW`); these functions only filter by clinic and window
and aggregate the view rows, with SQLAlchemy Core constructs over lightweight table
declarations of the views (no raw SQL text). Nothing here reads an OLTP database or any
relation other than the views (ANLY-06).

Windows are inclusive UTC calendar days `[start, end]`. `total_patients` is a current
count (patients registered and not deactivated), not a windowed one.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Literal

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession

__all__ = ["METRICS", "Metric", "summary", "timeseries"]

Metric = Literal["appointments_booked", "completed_visits"]
METRICS: tuple[str, ...] = ("appointments_booked", "completed_visits")

_UUID = PG_UUID(as_uuid=True)

v_active_patients = sa.table(
    "v_active_patients", sa.column("clinic_id", _UUID), sa.column("active_patients", sa.BigInteger)
)
v_appointments_booked_daily = sa.table(
    "v_appointments_booked_daily",
    sa.column("clinic_id", _UUID),
    sa.column("day", sa.Date),
    sa.column("appointments_booked", sa.BigInteger),
)
v_completed_visits_daily = sa.table(
    "v_completed_visits_daily",
    sa.column("clinic_id", _UUID),
    sa.column("day", sa.Date),
    sa.column("completed_visits", sa.BigInteger),
)
v_cancellation = sa.table(
    "v_cancellation",
    sa.column("clinic_id", _UUID),
    sa.column("day", sa.Date),
    sa.column("booked", sa.BigInteger),
    sa.column("cancelled", sa.BigInteger),
)
v_visit_wait = sa.table(
    "v_visit_wait",
    sa.column("clinic_id", _UUID),
    sa.column("completed_at", sa.DateTime(timezone=True)),
    sa.column("wait_seconds", sa.Float),
)


def _scoped(stmt: sa.Select[Any], view: sa.TableClause, clinic_id: uuid.UUID | None) -> sa.Select[Any]:
    return stmt if clinic_id is None else stmt.where(view.c.clinic_id == clinic_id)


def _days(stmt: sa.Select[Any], view: sa.TableClause, start: datetime.date, end: datetime.date) -> sa.Select[Any]:
    return stmt.where(view.c.day >= start, view.c.day <= end)


def _total(view: sa.TableClause, column: str) -> sa.ColumnElement[Any]:
    return sa.func.coalesce(sa.func.sum(view.c[column]), 0)


async def summary(
    db: AsyncSession, clinic_id: uuid.UUID | None, start: datetime.date, end: datetime.date
) -> dict[str, Any]:
    """The five metrics for one clinic (or all clinics when `clinic_id` is None).

    `cancellation_rate` = cancelled / booked over the appointments booked in the
    window, None when nothing was booked. `average_wait_seconds` = mean of check-in to
    visit start over the visits completed in the window, None when there is no sample.
    """
    start_ts = datetime.datetime.combine(start, datetime.time.min, tzinfo=datetime.UTC)
    end_ts = datetime.datetime.combine(
        end + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.UTC
    )
    total_patients = await db.scalar(
        _scoped(sa.select(_total(v_active_patients, "active_patients")), v_active_patients, clinic_id)
    )
    booked = await db.scalar(
        _scoped(
            _days(
                sa.select(_total(v_appointments_booked_daily, "appointments_booked")),
                v_appointments_booked_daily,
                start,
                end,
            ),
            v_appointments_booked_daily,
            clinic_id,
        )
    )
    completed = await db.scalar(
        _scoped(
            _days(
                sa.select(_total(v_completed_visits_daily, "completed_visits")),
                v_completed_visits_daily,
                start,
                end,
            ),
            v_completed_visits_daily,
            clinic_id,
        )
    )
    cancellation = (
        await db.execute(
            _scoped(
                _days(
                    sa.select(
                        _total(v_cancellation, "booked").label("booked"),
                        _total(v_cancellation, "cancelled").label("cancelled"),
                    ),
                    v_cancellation,
                    start,
                    end,
                ),
                v_cancellation,
                clinic_id,
            )
        )
    ).one()
    wait = await db.scalar(
        _scoped(
            sa.select(sa.func.avg(v_visit_wait.c.wait_seconds)).where(
                v_visit_wait.c.completed_at >= start_ts, v_visit_wait.c.completed_at < end_ts
            ),
            v_visit_wait,
            clinic_id,
        )
    )
    cancel_booked, cancel_cancelled = int(cancellation.booked), int(cancellation.cancelled)
    return {
        "total_patients": int(total_patients or 0),
        "appointments_booked": int(booked or 0),
        "completed_visits": int(completed or 0),
        "cancellation_rate": (cancel_cancelled / cancel_booked) if cancel_booked else None,
        "average_wait_seconds": float(wait) if wait is not None else None,
    }


async def timeseries(
    db: AsyncSession,
    metric: Metric,
    clinic_id: uuid.UUID | None,
    start: datetime.date,
    end: datetime.date,
) -> list[dict[str, Any]]:
    """One `{day, value}` per UTC day of `[start, end]`, zero where nothing happened."""
    view = v_appointments_booked_daily if metric == "appointments_booked" else v_completed_visits_daily
    stmt = _scoped(
        _days(sa.select(view.c.day, _total(view, metric).label("value")), view, start, end),
        view,
        clinic_id,
    ).group_by(view.c.day)
    by_day = {row.day: int(row.value) for row in (await db.execute(stmt)).all()}
    return [
        {"day": day, "value": by_day.get(day, 0)}
        for day in (start + datetime.timedelta(days=n) for n in range((end - start).days + 1))
    ]
