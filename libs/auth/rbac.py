"""`require_role(*roles)` — server-side RBAC dependency guard (IAM-02).

Raises 403 when the authenticated user's role is not in the allowed set. A
FastAPI dependency layered on `get_current_user` (which itself raises 401 for
missing/invalid/revoked tokens) — RBAC is enforced here, server-side, on every
route that declares it, in every service; never client-side and never merely
"documented" at router registration.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, HTTPException, status

from libs.auth.current_user import AuthenticatedUser, get_current_user
from libs.auth.roles import Role


def require_role(*allowed: Role) -> Callable[..., AuthenticatedUser]:
    def _guard(
        user: AuthenticatedUser = Depends(get_current_user),
    ) -> AuthenticatedUser:
        if user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="insufficient role for this operation",
            )
        return user

    return _guard
