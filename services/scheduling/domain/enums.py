"""Scheduling's domain enums.

`SlotStatus` backs the `provider_slots` exclusion constraint's WHERE clause
(`status <> 'cancelled'`) — see `services/scheduling/repository/models.py`.
"""

from __future__ import annotations

from enum import Enum


class SlotStatus(str, Enum):
    """Lifecycle states for a `provider_slots` row.

    The table exists, empty, from this phase on; Phase 2's booking saga is what
    transitions slots through this lifecycle.
    """

    AVAILABLE = "available"
    HELD = "held"
    BOOKED = "booked"
    CANCELLED = "cancelled"
