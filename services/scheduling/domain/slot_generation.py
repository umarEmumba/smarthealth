"""Pure, DST-safe expansion of weekly availability templates into slots (D-01).

Provider's availability templates are weekly wall-clock windows ("Monday
09:00-12:00, 30-minute slots") with no timezone of their own: they are read in
the CLINIC's IANA timezone (from Organization) and the concrete slots are
stored as UTC instants (research Pattern 6).

Rules:

* `local_to_utc` converts one wall-clock time with a round-trip check. A wall
  time that does not exist (the spring-forward gap) returns None; an ambiguous
  one (the fall-back fold) resolves to its FIRST occurrence (`fold=0`).
* A template's window is converted once at each end; slot k then starts at
  `start_utc + k * duration` in ELAPSED (UTC) time, so a DST jump inside the
  window neither overlaps two slots nor leaves a hole: a 01:00-05:00 window on
  the spring-forward day is three real hours (three 60-minute slots) and a
  00:00-04:00 window on the fall-back day is five.
* A window whose START falls in a gap starts at the next template grid point
  that exists (no slot is ever generated for a nonexistent wall time); an END
  in a gap is taken at its earlier reading, so a window never grows.
* `expand_templates` honours `is_active`, `day_of_week` (0 = Monday, the
  Provider convention and Python's `date.weekday()`), and the inclusive
  `effective_from`/`effective_to` dates. Overlapping templates for the same
  provider are reduced so that no two returned candidates overlap; the database
  exclusion constraint remains the real guard.

This module is deliberately pure (stdlib + uuid only): no database, no HTTP,
no settings, so it is unit-testable and safe to reuse anywhere.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

__all__ = ["AvailabilityTemplate", "CandidateSlot", "expand_templates", "local_to_utc"]


@dataclass(frozen=True, slots=True)
class CandidateSlot:
    """One slot to insert: UTC start/end plus the template's department."""

    start: datetime
    end: datetime
    department_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class AvailabilityTemplate:
    """The fields of Provider's `AvailabilityRef` that generation needs."""

    department_id: uuid.UUID
    day_of_week: int
    start_time: time
    end_time: time
    slot_duration_minutes: int
    effective_from: date
    effective_to: date | None = None
    is_active: bool = True

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> AvailabilityTemplate:
        """Build from Provider's JSON (ISO strings) or already-parsed values."""

        def _parse(value: Any, kind: type) -> Any:
            if value is None or isinstance(value, kind):
                return value
            return kind.fromisoformat(str(value))

        return cls(
            department_id=uuid.UUID(str(data["department_id"])),
            day_of_week=int(data["day_of_week"]),
            start_time=_parse(data["start_time"], time),
            end_time=_parse(data["end_time"], time),
            slot_duration_minutes=int(data["slot_duration_minutes"]),
            effective_from=_parse(data["effective_from"], date),
            effective_to=_parse(data.get("effective_to"), date),
            is_active=bool(data.get("is_active", True)),
        )

    def applies_on(self, day: date) -> bool:
        if not self.is_active or day.weekday() != self.day_of_week:
            return False
        if day < self.effective_from:
            return False
        return self.effective_to is None or day <= self.effective_to


def local_to_utc(day: date, t: time, tz: ZoneInfo) -> datetime | None:
    """`day` + `t` as a wall-clock time in `tz`, converted to UTC.

    None when that wall time does not exist (spring-forward gap). Ambiguous
    times (fall-back fold) resolve to the first occurrence (`fold=0`).
    """
    naive = datetime.combine(day, t)
    aware = naive.replace(tzinfo=tz)  # fold=0 -> first occurrence when ambiguous
    if aware.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != naive:
        return None  # nonexistent wall time (spring-forward gap)
    return aware.astimezone(timezone.utc)


def _window_start(day: date, template: AvailabilityTemplate, tz: ZoneInfo) -> datetime | None:
    """UTC start of the template's window on `day`: the first grid point
    (start_time + k * duration, wall clock) that exists and is before end."""
    step = timedelta(minutes=template.slot_duration_minutes)
    wall = datetime.combine(day, template.start_time)
    wall_end = datetime.combine(day, template.end_time)
    while wall < wall_end:
        converted = local_to_utc(wall.date(), wall.time(), tz)
        if converted is not None:
            return converted
        wall += step
    return None


def _window_end(day: date, template: AvailabilityTemplate, tz: ZoneInfo) -> datetime:
    """UTC end of the template's window on `day`; a gap end takes the earlier
    of its two readings so the window never grows."""
    converted = local_to_utc(day, template.end_time, tz)
    if converted is not None:
        return converted
    naive = datetime.combine(day, template.end_time)
    return min(
        naive.replace(tzinfo=tz, fold=0).astimezone(timezone.utc),
        naive.replace(tzinfo=tz, fold=1).astimezone(timezone.utc),
    )


def expand_templates(
    templates: Iterable[AvailabilityTemplate | Mapping[str, Any]],
    tz: ZoneInfo,
    from_date: date,
    to_date: date,
) -> list[CandidateSlot]:
    """Every slot the templates define on the local dates `from_date..to_date`
    (inclusive), ordered by start, with no two candidates overlapping."""
    parsed = [
        t if isinstance(t, AvailabilityTemplate) else AvailabilityTemplate.from_mapping(t)
        for t in templates
    ]
    candidates: list[CandidateSlot] = []
    day = from_date
    while day <= to_date:
        for template in parsed:
            if template.slot_duration_minutes <= 0 or not template.applies_on(day):
                continue
            start = _window_start(day, template, tz)
            if start is None:
                continue
            end_limit = _window_end(day, template, tz)
            step = timedelta(minutes=template.slot_duration_minutes)
            slot_start = start
            while slot_start + step <= end_limit:
                candidates.append(
                    CandidateSlot(
                        start=slot_start, end=slot_start + step, department_id=template.department_id
                    )
                )
                slot_start += step
        day += timedelta(days=1)

    # Overlapping templates: keep the earliest-starting candidate, drop any
    # later one that would overlap it (the exclusion constraint would anyway).
    candidates.sort(key=lambda c: (c.start, c.end))
    result: list[CandidateSlot] = []
    for candidate in candidates:
        if result and candidate.start < result[-1].end:
            continue
        result.append(candidate)
    return result
