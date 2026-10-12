"""The four RBAC roles (IAM-01/IAM-02).

Lives in `libs/auth` because every service verifies a token whose `role`
claim is one of these values and applies `require_role(...)` locally — it is
part of the shared token contract, not Identity's private domain. Identity
stores the same string in `users.role`.
"""

from __future__ import annotations

from enum import Enum


class Role(str, Enum):
    PATIENT = "patient"
    PROVIDER = "provider"
    FRONT_DESK = "front_desk"
    ADMIN = "admin"
