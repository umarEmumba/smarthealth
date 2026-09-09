"""Shared domain enums for the identity service.

`Role` backs IAM-01/IAM-02 RBAC — every user row and every JWT `role` claim
uses one of these four values. `SlotStatus` backs the (currently empty)
`provider_slots` table's exclusion-constraint WHERE clause (`status <>
'cancelled'`) — see services/identity/repository/models.py.
"""

from __future__ import annotations

from enum import Enum


class Role(str, Enum):
    """The four RBAC roles this platform supports (IAM-01/IAM-02)."""

    PATIENT = "patient"
    PROVIDER = "provider"
    FRONT_DESK = "front_desk"
    ADMIN = "admin"


class SlotStatus(str, Enum):
    """Lifecycle states for a `provider_slots` row.

    Phase 1 creates the table empty (no rows in any status yet) — Phase 2's
    booking saga is what actually transitions slots through this lifecycle.
    """

    AVAILABLE = "available"
    HELD = "held"
    BOOKED = "booked"
    CANCELLED = "cancelled"
