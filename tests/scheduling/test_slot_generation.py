"""Pure unit tests for DST-safe slot generation (plan 02-04, research Pattern 6).

No infrastructure: `domain.slot_generation` imports only the standard library.
The DST dates are America/New_York's 2026 transitions: spring forward on
2026-03-08 (02:00 EST -> 03:00 EDT) and fall back on 2026-11-01 (02:00 EDT ->
01:00 EST).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from services.scheduling.domain.slot_generation import (
    AvailabilityTemplate,
    CandidateSlot,
    expand_templates,
    local_to_utc,
)

NY = ZoneInfo("America/New_York")
KARACHI = ZoneInfo("Asia/Karachi")
SPRING_FORWARD = date(2026, 3, 8)  # a Sunday
FALL_BACK = date(2026, 11, 1)  # a Sunday
DEPT = uuid.UUID("00000000-0000-4000-8000-000000000001")


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _template(day: date, start: time, end: time, minutes: int = 60, **overrides) -> dict:
    data = {
        "department_id": str(DEPT),
        "day_of_week": day.weekday(),
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "slot_duration_minutes": minutes,
        "effective_from": "2026-01-01",
        "effective_to": None,
        "is_active": True,
    }
    data.update(overrides)
    return data


def _assert_no_overlap(slots: list[CandidateSlot]) -> None:
    ordered = sorted(slots, key=lambda s: s.start)
    for earlier, later in zip(ordered, ordered[1:]):
        assert earlier.end <= later.start, (earlier, later)


# --- local_to_utc ------------------------------------------------------------


def test_spring_forward_gap_wall_time_does_not_exist() -> None:
    assert local_to_utc(SPRING_FORWARD, time(2, 30), NY) is None


def test_wall_time_after_the_gap_is_edt() -> None:
    assert local_to_utc(SPRING_FORWARD, time(3, 30), NY) == _utc(2026, 3, 8, 7, 30)


def test_ambiguous_fall_back_time_resolves_to_first_occurrence() -> None:
    # 01:30 happens twice; fold=0 is the first (EDT, UTC-4) reading.
    assert local_to_utc(FALL_BACK, time(1, 30), NY) == _utc(2026, 11, 1, 5, 30)


def test_karachi_has_a_fixed_plus_five_offset() -> None:
    assert local_to_utc(date(2026, 10, 13), time(9, 0), KARACHI) == _utc(2026, 10, 13, 4, 0)


# --- expand_templates --------------------------------------------------------


def test_spring_forward_window_yields_three_real_hours() -> None:
    slots = expand_templates([_template(SPRING_FORWARD, time(1), time(5))], NY, SPRING_FORWARD, SPRING_FORWARD)

    assert [s.start for s in slots] == [_utc(2026, 3, 8, 6), _utc(2026, 3, 8, 7), _utc(2026, 3, 8, 8)]
    assert all(s.end - s.start == timedelta(hours=1) for s in slots)
    assert all(s.department_id == DEPT for s in slots)
    _assert_no_overlap(slots)
    # No slot starts at a nonexistent wall time.
    assert all(local_to_utc(SPRING_FORWARD, s.start.astimezone(NY).time(), NY) == s.start for s in slots)


def test_fall_back_window_yields_five_real_hours_without_overlap() -> None:
    slots = expand_templates([_template(FALL_BACK, time(0), time(4))], NY, FALL_BACK, FALL_BACK)

    assert len(slots) == 5
    assert slots[0].start == _utc(2026, 11, 1, 4)
    assert slots[-1].end == _utc(2026, 11, 1, 9)
    _assert_no_overlap(slots)


def test_window_starting_inside_the_gap_starts_at_the_next_existing_grid_point() -> None:
    slots = expand_templates(
        [_template(SPRING_FORWARD, time(2, 0), time(5, 0), minutes=30)], NY, SPRING_FORWARD, SPRING_FORWARD
    )

    # 02:00 and 02:30 do not exist; 03:00 EDT (07:00Z) is the first slot.
    assert slots[0].start == _utc(2026, 3, 8, 7, 0)
    assert slots[-1].end == _utc(2026, 3, 8, 9, 0)
    assert len(slots) == 4
    _assert_no_overlap(slots)


def test_window_ending_inside_the_gap_never_grows() -> None:
    slots = expand_templates(
        [_template(SPRING_FORWARD, time(0, 0), time(2, 30), minutes=30)], NY, SPRING_FORWARD, SPRING_FORWARD
    )

    # 00:00-02:00 EST are the only real minutes before the gap.
    assert slots[-1].end <= _utc(2026, 3, 8, 7, 0)
    assert len(slots) == 4
    _assert_no_overlap(slots)


def test_template_outside_its_effective_window_yields_nothing() -> None:
    day = date(2026, 6, 1)
    before = _template(day, time(9), time(12), effective_from="2026-06-02")
    after = _template(day, time(9), time(12), effective_from="2026-01-01", effective_to="2026-05-31")

    assert expand_templates([before, after], NY, day, day) == []


def test_inactive_template_yields_nothing() -> None:
    day = date(2026, 6, 1)

    assert expand_templates([_template(day, time(9), time(12), is_active=False)], NY, day, day) == []


def test_only_matching_weekdays_inside_the_range_are_expanded() -> None:
    monday = date(2026, 6, 1)
    template = AvailabilityTemplate(
        department_id=DEPT,
        day_of_week=0,
        start_time=time(9),
        end_time=time(12),
        slot_duration_minutes=30,
        effective_from=date(2026, 1, 1),
    )

    slots = expand_templates([template], KARACHI, monday, monday + timedelta(days=13))

    assert len(slots) == 12  # two Mondays x six slots
    assert {s.start.date() for s in slots} == {monday, monday + timedelta(days=7)}


def test_overlapping_templates_never_produce_overlapping_candidates() -> None:
    day = date(2026, 6, 1)
    templates = [
        _template(day, time(9), time(12), minutes=30),
        _template(day, time(9, 15), time(13), minutes=45),
    ]

    slots = expand_templates(templates, NY, day, day)

    _assert_no_overlap(slots)
    assert slots[0].start == local_to_utc(day, time(9), NY)
