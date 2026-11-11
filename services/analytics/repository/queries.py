"""Read-only metric queries over the analytics views (plan 03-13; ANLY-01..06).

Every number is computed by a view in analytics_db (plan 03-10, with the written
definition in its `COMMENT ON VIEW`); these functions only filter by clinic and window
and aggregate the view rows. Nothing here reads an OLTP database or any table other
than the views (ANLY-06). Statements are static text with bound parameters: the only
variable fragment is one of two constant clinic filters.

Windows are inclusive UTC calendar days `[start, end]`. `total_patients` is a current
count (patients registered and not deactivated), not a windowed one.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

__all__ = ["METRICS", "Metric", "summary", "timeseries"]

Metric = Literal["appointments_booked", "completed_visits"]
METRICS: tuple[str, ...] = ("appointments_booked", "completed_visits")

_CLINIC = " AND clinic_id = :clinic_id"

_TOTAL_PATIENTS = "SELECT coalesce(sum(active_patients), 0) FROM v_active_patients WHERE true"
_BOOKED = (
    "SELECT coalesce(sum(appointments_booked), 0) FROM v_appointments_booked_daily "
    "WHERE day BETWEEN :start AND :end"
)
_COMPLETED = (
    "SELECT coalesce(sum(completed_visits), 0) FROM v_completed_visits_daily "
    "WHERE day BETWEEN :start AND :end"
)
_CANCELLATION = (
    "SELECT coalesce(sum(booked), 0) AS booked, coalesce(sum(cancelled), 0) AS cancelled "
    "FROM v_cancellation WHERE day BETWEEN :start AND :end"
)
_WAIT = (
    "SELECT avg(wait_seconds) FROM v_visit_wait "
    "WHERE completed_at >= :start_ts AND completed_at < :end_ts"
)
_SERIES = {
    "appointments_booked": (
        "SELECT day, sum(appointments_booked) AS value FROM v_appointments_booked_daily "
        "WHERE day BETWEEN :start AND :end"
    ),
    "completed_visits": (
        "SELECT day, sum(completed_visits) AS value FROM v_completed_visits_daily "
        "WHERE day BETWEEN :start AND :end"
    ),
}


def _clinic(clinic_id: uuid.UUID | None) -> tuple[str, dict[str, Any]]:
    """The constant clinic fragment (or "") and its bound parameter."""
    if clinic_id is None:
        return "", {}
    return _CLINIC, {"clinic_id": clinic_id}


def _day_bounds(start: datetime.date, end: datetime.date) -> dict[str, Any]:
    return {"start": start, "end": end}


def _ts_bounds(start: datetime.date, end: datetime.date) -> dict[str, Any]:
    return {
        "start_ts": datetime.datetime.combine(start, datetime.time.min, tzinfo=datetime.UTC),
        "end_ts": datetime.datetime.combine(
            end + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.UTC
        ),
    }


async def summary(
    db: AsyncSession, clinic_id: uuid.UUID | None, start: datetime.date, end: datetime.date
) -> dict[str, Any]:
    """The five metrics for one clinic (or all clinics when `clinic_id` is None).

    `cancellation_rate` = cancelled / booked over the appointments booked in the
    window, None when nothing was booked. `average_wait_seconds` = mean of check-in to
    visit start over the visits completed in the window, None when there is no sample.
    """
    clinic_sql, clinic_params = _clinic(clinic_id)
    days = {**_day_bounds(start, end), **clinic_params}

    total_patients = (await db.execute(text(_TOTAL_PATIENTS + clinic_sql), clinic_params)).scalar_one()
    booked = (await db.execute(text(_BOOKED + clinic_sql), days)).scalar_one()
    completed = (await db.execute(text(_COMPLETED + clinic_sql), days)).scalar_one()
    wait = (
        await db.execute(text(_WAIT + clinic_sql), {**_ts_bounds(start, end), **clinic_params})
    ).scalar_one()
    row = (await db.execute(text(_CANCELLATION + clinic_sql), days)).one()
    cancel_booked, cancel_cancelled = int(row.booked), int(row.cancelled)

    return {
        "total_patients": int(total_patients),
        "appointments_booked": int(booked),
        "completed_visits": int(completed),
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
    clinic_sql, clinic_params = _clinic(clinic_id)
    sql = _SERIES[metric] + clinic_sql + " GROUP BY day"
    rows = (await db.execute(text(sql), {**_day_bounds(start, end), **clinic_params})).all()
    by_day = {row.day: int(row.value) for row in rows}
    days = (end - start).days + 1
    return [
        {"day": day, "value": by_day.get(day, 0)}
        for day in (start + datetime.timedelta(days=offset) for offset in range(days))
    ]
